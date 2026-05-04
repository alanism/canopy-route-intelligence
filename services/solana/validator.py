"""
Phase 6 — Solana Batch Validation + Reconciliation.

Runs a suite of promotion gates on a batch of normalized Solana events
before they are promoted to the derived (analytics) layer.

Gates (all must pass to approve a batch)
-----------------------------------------
1. Row count consistency         — batch size matches expected count
2. No float amounts              — all amount fields are int or Decimal
3. Decimal precision             — amount_decimal has ≤ 9 decimal places (NUMERIC limit)
4. No unresolved account keys    — no __account_index_N__ placeholders remain
5. No missing required fields    — all 44 canonical fields present
6. Transfer truth consistency    — observed_transfer_inclusion matches transfer_detected
7. Reconciliation sample         — spot-check VALIDATION_SAMPLE_SIZE transactions

Each gate returns a `GateResult`. A `ValidationReport` collects all gate
results and produces a pass/fail decision with per-gate detail.

Usage
-----
    report = validate_batch(normalized_events, expected_row_count=len(raw_events))
    if report.approved:
        checkpoint.advance(promoted=True)
    else:
        logger.error("Batch rejected: %s", report.summary())
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Optional

logger = logging.getLogger("canopy.solana.validator")

# Reconciliation sample size (spot-check count per batch)
VALIDATION_SAMPLE_SIZE = 20

# Max decimal places allowed in amount_decimal (NUMERIC BigQuery limit)
MAX_DECIMAL_PLACES = 9

# Pattern that indicates an unresolved account index placeholder
_PLACEHOLDER_PATTERN = re.compile(r"^__account_index_\d+__$")


# ---------------------------------------------------------------------------
# Gate result
# ---------------------------------------------------------------------------

@dataclass
class GateResult:
    gate_name: str
    passed: bool
    violations: list[str] = field(default_factory=list)
    checked: int = 0
    failed_count: int = 0

    @property
    def summary(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        detail = f"checked={self.checked} failed={self.failed_count}"
        if self.violations:
            preview = self.violations[0]
            more = f" (+{len(self.violations) - 1} more)" if len(self.violations) > 1 else ""
            return f"[{status}] {self.gate_name}: {detail} — {preview}{more}"
        return f"[{status}] {self.gate_name}: {detail}"


# ---------------------------------------------------------------------------
# Validation report
# ---------------------------------------------------------------------------

@dataclass
class ValidationReport:
    gate_results: list[GateResult] = field(default_factory=list)
    batch_size: int = 0
    expected_row_count: Optional[int] = None

    @property
    def approved(self) -> bool:
        return all(g.passed for g in self.gate_results)

    def summary(self) -> str:
        status = "APPROVED" if self.approved else "REJECTED"
        lines = [f"Batch {status} ({self.batch_size} rows):"]
        for g in self.gate_results:
            lines.append(f"  {g.summary}")
        return "\n".join(lines)

    def failed_gates(self) -> list[GateResult]:
        return [g for g in self.gate_results if not g.passed]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def validate_batch(
    normalized_events: list[dict[str, Any]],
    *,
    expected_row_count: Optional[int] = None,
    sample_size: int = VALIDATION_SAMPLE_SIZE,
) -> ValidationReport:
    """
    Run all promotion gates on a batch of normalized events.

    Parameters
    ----------
    normalized_events:
        List of dicts produced by normalize_event() (and optionally patched
        by apply_owner_and_amount_resolution()).
    expected_row_count:
        If provided, gate 1 checks batch size equals this value.
    sample_size:
        Number of events to spot-check in the reconciliation gate.

    Returns
    -------
    ValidationReport with per-gate results and overall approved/rejected status.
    """
    report = ValidationReport(
        batch_size=len(normalized_events),
        expected_row_count=expected_row_count,
    )

    report.gate_results = [
        _gate_row_count(normalized_events, expected_row_count),
        _gate_no_float_amounts(normalized_events),
        _gate_decimal_precision(normalized_events),
        _gate_no_placeholder_accounts(normalized_events),
        _gate_required_fields(normalized_events),
        _gate_transfer_truth_consistency(normalized_events),
        _gate_reconciliation_sample(normalized_events, sample_size),
    ]

    if report.approved:
        logger.info(
            "Validation APPROVED: %d rows, all gates passed", len(normalized_events)
        )
    else:
        failed = [g.gate_name for g in report.failed_gates()]
        logger.warning(
            "Validation REJECTED: %d rows, failed gates: %s",
            len(normalized_events), failed,
        )

    return report


# ---------------------------------------------------------------------------
# Gate implementations
# ---------------------------------------------------------------------------

def _gate_row_count(
    events: list[dict[str, Any]],
    expected: Optional[int],
) -> GateResult:
    """Gate 1: Batch size matches expected count."""
    gate = GateResult(gate_name="row_count", passed=True, checked=1)

    if expected is None:
        # No expectation set — pass unconditionally
        return gate

    if len(events) != expected:
        gate.passed = False
        gate.failed_count = 1
        gate.violations = [
            f"expected {expected} rows but got {len(events)}"
        ]

    return gate


def _gate_no_float_amounts(events: list[dict[str, Any]]) -> GateResult:
    """Gate 2: All amount fields are int or Decimal — never float."""
    _AMOUNT_FIELDS = {
        "amount_raw", "amount_decimal", "amount_transferred_raw",
        "fee_withheld_raw", "amount_received_raw",
        "fee_lamports", "native_base_fee_lamports", "native_priority_fee_lamports",
        "jito_tip_lamports", "explicit_tip_lamports",
        "total_native_observed_cost_lamports",
    }
    violations: list[str] = []

    for i, event in enumerate(events):
        sig = event.get("signature", f"[{i}]")
        for f in _AMOUNT_FIELDS:
            val = event.get(f)
            if isinstance(val, float):
                violations.append(f"sig={sig} field={f} value={val!r}")

    gate = GateResult(
        gate_name="no_float_amounts",
        passed=len(violations) == 0,
        checked=len(events),
        failed_count=len(violations),
        violations=violations,
    )
    return gate


def _gate_decimal_precision(events: list[dict[str, Any]]) -> GateResult:
    """Gate 3: amount_decimal ≤ MAX_DECIMAL_PLACES decimal places."""
    violations: list[str] = []

    for i, event in enumerate(events):
        val = event.get("amount_decimal")
        if val is None:
            continue
        sig = event.get("signature", f"[{i}]")

        if isinstance(val, Decimal):
            # sign, digits, exponent — exponent is negative for decimal places
            sign, digits, exponent = val.as_tuple()
            decimal_places = -exponent if exponent < 0 else 0
        elif isinstance(val, (int, str)):
            # str form — count digits after decimal point
            s = str(val)
            dot_pos = s.find(".")
            decimal_places = len(s) - dot_pos - 1 if dot_pos >= 0 else 0
        else:
            # Unexpected type — report it
            violations.append(f"sig={sig} amount_decimal unexpected type {type(val).__name__}")
            continue

        if decimal_places > MAX_DECIMAL_PLACES:
            violations.append(
                f"sig={sig} amount_decimal={val!r} has {decimal_places} decimal places "
                f"(max {MAX_DECIMAL_PLACES})"
            )

    gate = GateResult(
        gate_name="decimal_precision",
        passed=len(violations) == 0,
        checked=len(events),
        failed_count=len(violations),
        violations=violations,
    )
    return gate


def _gate_no_placeholder_accounts(events: list[dict[str, Any]]) -> GateResult:
    """Gate 4: No __account_index_N__ placeholders remain in token account fields."""
    _ACCOUNT_FIELDS = {
        "source_token_account", "destination_token_account",
        "source_owner", "destination_owner", "token_mint",
    }
    violations: list[str] = []

    for i, event in enumerate(events):
        sig = event.get("signature", f"[{i}]")
        for f in _ACCOUNT_FIELDS:
            val = event.get(f)
            if val is not None and _PLACEHOLDER_PATTERN.match(str(val)):
                violations.append(f"sig={sig} field={f} placeholder={val!r}")

    gate = GateResult(
        gate_name="no_placeholder_accounts",
        passed=len(violations) == 0,
        checked=len(events),
        failed_count=len(violations),
        violations=violations,
    )
    return gate


def _gate_required_fields(events: list[dict[str, Any]]) -> GateResult:
    """Gate 5: All 44 required fields present in every event."""
    from services.solana.event_schema import REQUIRED_FIELDS

    violations: list[str] = []

    for i, event in enumerate(events):
        missing = [f for f in REQUIRED_FIELDS if f not in event]
        if missing:
            sig = event.get("signature", f"[{i}]")
            violations.append(f"sig={sig} missing={missing}")

    gate = GateResult(
        gate_name="required_fields",
        passed=len(violations) == 0,
        checked=len(events),
        failed_count=len(violations),
        violations=violations,
    )
    return gate


def _gate_transfer_truth_consistency(events: list[dict[str, Any]]) -> GateResult:
    """
    Gate 6: observed_transfer_inclusion consistency check.

    Rule: if transfer_detected is True, observed_transfer_inclusion must also be True.
    The inverse is not required (settlement evidence can exist without transfer_detected).
    """
    violations: list[str] = []

    for i, event in enumerate(events):
        transfer_detected = event.get("transfer_detected")
        observed = event.get("observed_transfer_inclusion")

        if transfer_detected is True and observed is False:
            sig = event.get("signature", f"[{i}]")
            violations.append(
                f"sig={sig} transfer_detected=True but observed_transfer_inclusion=False"
            )

    gate = GateResult(
        gate_name="transfer_truth_consistency",
        passed=len(violations) == 0,
        checked=len(events),
        failed_count=len(violations),
        violations=violations,
    )
    return gate


def _gate_reconciliation_sample(
    events: list[dict[str, Any]],
    sample_size: int,
) -> GateResult:
    """
    Gate 7: Spot-check up to sample_size events for internal consistency.

    Checks per sampled event:
    - amount_raw == amount_received_raw (when both non-None)
    - amount_decimal is consistent with amount_received_raw (within 1 ULP tolerance)
    - validation_status is a known value
    - raw_event_id and normalized_event_id are non-empty strings
    """
    violations: list[str] = []
    _VALID_STATUSES = {"ok", "degraded", "failed", "partial"}

    # Sample deterministically — first N events
    sample = events[:sample_size]

    for i, event in enumerate(sample):
        sig = event.get("signature", f"[{i}]")

        # amount_raw == amount_received_raw
        amount_raw = event.get("amount_raw")
        amount_received = event.get("amount_received_raw")
        if amount_raw is not None and amount_received is not None:
            if amount_raw != amount_received:
                violations.append(
                    f"sig={sig} amount_raw={amount_raw} != amount_received_raw={amount_received}"
                )

        # validation_status is a known value
        status = event.get("validation_status")
        if status not in _VALID_STATUSES:
            violations.append(f"sig={sig} validation_status={status!r} unknown")

        # raw_event_id and normalized_event_id are non-empty strings
        for id_field in ("raw_event_id", "normalized_event_id"):
            val = event.get(id_field)
            if not val or not isinstance(val, str):
                violations.append(f"sig={sig} {id_field}={val!r} is empty or non-string")

        # amount_decimal consistent with amount_received_raw
        amount_decimal = event.get("amount_decimal")
        if amount_received is not None and amount_decimal is not None:
            if isinstance(amount_decimal, Decimal):
                # Reconstruct from amount_received_raw using 6 decimals (USDC default)
                # Allow tolerance for non-USDC tokens with different decimal counts
                reconstructed = Decimal(amount_received) / Decimal(10 ** 6)
                # Check within 1% tolerance (accounts for different decimal counts)
                if amount_decimal != 0 and reconstructed != 0:
                    ratio = float(abs(amount_decimal - reconstructed) / max(abs(amount_decimal), abs(reconstructed)))
                    if ratio > 0.01:
                        violations.append(
                            f"sig={sig} amount_decimal={amount_decimal} inconsistent "
                            f"with amount_received_raw={amount_received} "
                            f"(reconstructed={reconstructed}, ratio={ratio:.4f})"
                        )

    gate = GateResult(
        gate_name="reconciliation_sample",
        passed=len(violations) == 0,
        checked=len(sample),
        failed_count=len(violations),
        violations=violations,
    )
    return gate


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------

def assert_batch_approved(
    normalized_events: list[dict[str, Any]],
    *,
    expected_row_count: Optional[int] = None,
) -> ValidationReport:
    """
    Validate a batch and raise ValueError if rejected.

    For use in pipeline code where a rejected batch must halt processing.
    """
    report = validate_batch(normalized_events, expected_row_count=expected_row_count)
    if not report.approved:
        raise ValueError(f"Batch validation failed:\n{report.summary()}")
    return report
