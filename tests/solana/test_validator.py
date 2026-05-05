"""
Phase 6 — Solana Batch Validation + Reconciliation Tests.

Tests all 7 promotion gates and the ValidationReport / GateResult structures.
"""

from __future__ import annotations

import pytest
from decimal import Decimal
from unittest.mock import patch

from services.solana.validator import (
    validate_batch,
    assert_batch_approved,
    ValidationReport,
    GateResult,
    VALIDATION_SAMPLE_SIZE,
    MAX_DECIMAL_PLACES,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_event(**overrides) -> dict:
    """Return a minimal valid normalized event. Override any field as needed."""
    base = {
        # Identity
        "chain": "solana",
        "signature": "SIG1",
        "slot": 300_000_000,
        "block_time": 1_700_000_000,
        # Token accounts
        "token_mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "watched_address": "WatchedAddr1111111111111111111111111111111111",
        "source_token_account": "SRC_ATA",
        "destination_token_account": "DST_ATA",
        "source_owner": "WALLET_A",
        "destination_owner": "WALLET_B",
        # Instruction position
        "instruction_index": 0,
        "inner_instruction_index": -1,
        "transfer_ordinal": 0,
        "program_id": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
        # Amounts
        "amount_raw": 1_000_000,
        "amount_decimal": Decimal("1.000000"),
        "amount_transferred_raw": 1_000_000,
        "fee_withheld_raw": 0,
        "amount_received_raw": 1_000_000,
        # Cost
        "fee_lamports": 5000,
        "native_base_fee_lamports": 5000,
        "native_priority_fee_lamports": 0,
        "jito_tip_lamports": 0,
        "explicit_tip_lamports": 0,
        "total_native_observed_cost_lamports": 5000,
        # Transfer truth
        "transaction_success": True,
        "transfer_detected": True,
        "balance_delta_detected": True,
        "observed_transfer_inclusion": True,
        "settlement_evidence_type": "balance_delta",
        # Metadata
        "decode_version": "1",
        "validation_status": "ok",
        "cost_detection_status": "ok",
        "tip_detection_status": "ok",
        "provider": "helius",
        "provider_mode": "primary",
        # Canonical keys
        "raw_event_id": "solana:SIG1:0:-1",
        "normalized_event_id": "solana:SIG1:0:-1:abcdef01",
        "event_fingerprint": "abcdef0123456789",
        "collision_detected": False,
        # Resolution statuses
        "alt_resolution_status": "not_required",
        "owner_resolution_status": "ok",
        "amount_resolution_status": "ok",
        # Ingestion timestamp
        "ingested_at": "2026-05-04T00:00:00+00:00",
    }
    base.update(overrides)
    return base


def _batch(n: int = 3, **overrides) -> list[dict]:
    events = []
    for i in range(n):
        e = _make_event(signature=f"SIG{i+1}", raw_event_id=f"solana:SIG{i+1}:0:-1", **overrides)
        e["normalized_event_id"] = f"solana:SIG{i+1}:0:-1:abcdef01"
        events.append(e)
    return events


# ---------------------------------------------------------------------------
# ValidationReport structure
# ---------------------------------------------------------------------------

class TestValidationReport:
    def test_approved_when_all_gates_pass(self):
        report = validate_batch(_batch())
        assert report.approved is True

    def test_rejected_when_any_gate_fails(self):
        events = _batch()
        events[0]["amount_raw"] = 1.5  # float — gate 2 failure
        report = validate_batch(events)
        assert report.approved is False

    def test_batch_size_recorded(self):
        events = _batch(5)
        report = validate_batch(events)
        assert report.batch_size == 5

    def test_summary_contains_approved(self):
        report = validate_batch(_batch())
        assert "APPROVED" in report.summary()

    def test_summary_contains_rejected(self):
        events = _batch()
        events[0]["amount_raw"] = 3.14
        report = validate_batch(events)
        assert "REJECTED" in report.summary()

    def test_failed_gates_lists_failing_gates(self):
        events = _batch()
        events[0]["amount_raw"] = 3.14
        report = validate_batch(events)
        failed = [g.gate_name for g in report.failed_gates()]
        assert "no_float_amounts" in failed

    def test_all_seven_gates_present(self):
        report = validate_batch(_batch())
        gate_names = {g.gate_name for g in report.gate_results}
        expected = {
            "row_count", "no_float_amounts", "decimal_precision",
            "no_placeholder_accounts", "required_fields",
            "transfer_truth_consistency", "reconciliation_sample",
        }
        assert gate_names == expected


# ---------------------------------------------------------------------------
# Gate 1 — Row count
# ---------------------------------------------------------------------------

class TestGateRowCount:
    def test_passes_when_count_matches(self):
        events = _batch(3)
        report = validate_batch(events, expected_row_count=3)
        gate = next(g for g in report.gate_results if g.gate_name == "row_count")
        assert gate.passed is True

    def test_fails_when_count_mismatches(self):
        events = _batch(3)
        report = validate_batch(events, expected_row_count=5)
        gate = next(g for g in report.gate_results if g.gate_name == "row_count")
        assert gate.passed is False
        assert "expected 5 rows but got 3" in gate.violations[0]

    def test_passes_when_no_expected_count(self):
        events = _batch(3)
        report = validate_batch(events, expected_row_count=None)
        gate = next(g for g in report.gate_results if g.gate_name == "row_count")
        assert gate.passed is True


# ---------------------------------------------------------------------------
# Gate 2 — No float amounts
# ---------------------------------------------------------------------------

class TestGateNoFloatAmounts:
    def test_passes_with_int_amounts(self):
        events = _batch()
        report = validate_batch(events)
        gate = next(g for g in report.gate_results if g.gate_name == "no_float_amounts")
        assert gate.passed is True

    def test_passes_with_decimal_amount_decimal(self):
        events = _batch(amount_decimal=Decimal("1.000000"))
        report = validate_batch(events)
        gate = next(g for g in report.gate_results if g.gate_name == "no_float_amounts")
        assert gate.passed is True

    def test_fails_on_float_amount_raw(self):
        events = _batch()
        events[0]["amount_raw"] = 1.5
        report = validate_batch(events)
        gate = next(g for g in report.gate_results if g.gate_name == "no_float_amounts")
        assert gate.passed is False
        assert "amount_raw" in gate.violations[0]

    def test_fails_on_float_fee_lamports(self):
        events = _batch()
        events[0]["fee_lamports"] = 5000.0
        report = validate_batch(events)
        gate = next(g for g in report.gate_results if g.gate_name == "no_float_amounts")
        assert gate.passed is False

    def test_none_amounts_pass(self):
        events = _batch(amount_transferred_raw=None, fee_withheld_raw=None)
        report = validate_batch(events)
        gate = next(g for g in report.gate_results if g.gate_name == "no_float_amounts")
        assert gate.passed is True


# ---------------------------------------------------------------------------
# Gate 3 — Decimal precision
# ---------------------------------------------------------------------------

class TestGateDecimalPrecision:
    def test_passes_with_6_decimal_places(self):
        events = _batch(amount_decimal=Decimal("1.000000"))
        report = validate_batch(events)
        gate = next(g for g in report.gate_results if g.gate_name == "decimal_precision")
        assert gate.passed is True

    def test_passes_with_none_amount_decimal(self):
        events = _batch(amount_decimal=None)
        report = validate_batch(events)
        gate = next(g for g in report.gate_results if g.gate_name == "decimal_precision")
        assert gate.passed is True

    def test_fails_with_too_many_decimal_places(self):
        # 10 decimal places > MAX_DECIMAL_PLACES (9)
        events = _batch(amount_decimal=Decimal("0.0000000001"))
        report = validate_batch(events)
        gate = next(g for g in report.gate_results if g.gate_name == "decimal_precision")
        assert gate.passed is False

    def test_passes_at_exactly_max_decimal_places(self):
        # 9 decimal places == MAX_DECIMAL_PLACES
        events = _batch(amount_decimal=Decimal("0.000000001"))
        report = validate_batch(events)
        gate = next(g for g in report.gate_results if g.gate_name == "decimal_precision")
        assert gate.passed is True


# ---------------------------------------------------------------------------
# Gate 4 — No placeholder accounts
# ---------------------------------------------------------------------------

class TestGateNoPlaceholderAccounts:
    def test_passes_with_real_pubkeys(self):
        events = _batch()
        report = validate_batch(events)
        gate = next(g for g in report.gate_results if g.gate_name == "no_placeholder_accounts")
        assert gate.passed is True

    def test_fails_on_placeholder_source_token_account(self):
        events = _batch(source_token_account="__account_index_2__")
        report = validate_batch(events)
        gate = next(g for g in report.gate_results if g.gate_name == "no_placeholder_accounts")
        assert gate.passed is False
        assert "__account_index_2__" in gate.violations[0]

    def test_fails_on_placeholder_destination_owner(self):
        events = _batch(destination_owner="__account_index_5__")
        report = validate_batch(events)
        gate = next(g for g in report.gate_results if g.gate_name == "no_placeholder_accounts")
        assert gate.passed is False

    def test_passes_with_none_owner_fields(self):
        # None is allowed (unresolved but not a placeholder string)
        events = _batch(source_owner=None, destination_owner=None)
        report = validate_batch(events)
        gate = next(g for g in report.gate_results if g.gate_name == "no_placeholder_accounts")
        assert gate.passed is True


# ---------------------------------------------------------------------------
# Gate 5 — Required fields
# ---------------------------------------------------------------------------

class TestGateRequiredFields:
    def test_passes_with_all_44_fields(self):
        events = _batch()
        report = validate_batch(events)
        gate = next(g for g in report.gate_results if g.gate_name == "required_fields")
        assert gate.passed is True

    def test_fails_when_field_missing(self):
        events = _batch()
        del events[0]["chain"]
        report = validate_batch(events)
        gate = next(g for g in report.gate_results if g.gate_name == "required_fields")
        assert gate.passed is False
        assert "chain" in gate.violations[0]

    def test_fails_when_multiple_fields_missing(self):
        events = _batch()
        del events[0]["chain"]
        del events[0]["signature"]
        report = validate_batch(events)
        gate = next(g for g in report.gate_results if g.gate_name == "required_fields")
        assert gate.passed is False


# ---------------------------------------------------------------------------
# Gate 6 — Transfer truth consistency
# ---------------------------------------------------------------------------

class TestGateTransferTruthConsistency:
    def test_passes_when_both_true(self):
        events = _batch(transfer_detected=True, observed_transfer_inclusion=True)
        report = validate_batch(events)
        gate = next(g for g in report.gate_results if g.gate_name == "transfer_truth_consistency")
        assert gate.passed is True

    def test_passes_when_both_false(self):
        events = _batch(transfer_detected=False, observed_transfer_inclusion=False)
        report = validate_batch(events)
        gate = next(g for g in report.gate_results if g.gate_name == "transfer_truth_consistency")
        assert gate.passed is True

    def test_passes_when_transfer_detected_false_but_observed_true(self):
        # Settlement evidence can exist without instruction-level detection
        events = _batch(transfer_detected=False, observed_transfer_inclusion=True)
        report = validate_batch(events)
        gate = next(g for g in report.gate_results if g.gate_name == "transfer_truth_consistency")
        assert gate.passed is True

    def test_fails_when_transfer_detected_true_but_observed_false(self):
        events = _batch(transfer_detected=True, observed_transfer_inclusion=False)
        report = validate_batch(events)
        gate = next(g for g in report.gate_results if g.gate_name == "transfer_truth_consistency")
        assert gate.passed is False
        assert "observed_transfer_inclusion=False" in gate.violations[0]

    def test_passes_when_none_values(self):
        # None means unknown — not a violation
        events = _batch(transfer_detected=None, observed_transfer_inclusion=None)
        report = validate_batch(events)
        gate = next(g for g in report.gate_results if g.gate_name == "transfer_truth_consistency")
        assert gate.passed is True


# ---------------------------------------------------------------------------
# Gate 7 — Reconciliation sample
# ---------------------------------------------------------------------------

class TestGateReconciliationSample:
    def test_passes_with_consistent_amounts(self):
        events = _batch(amount_raw=1_000_000, amount_received_raw=1_000_000)
        report = validate_batch(events)
        gate = next(g for g in report.gate_results if g.gate_name == "reconciliation_sample")
        assert gate.passed is True

    def test_fails_when_amount_raw_mismatches_received(self):
        events = _batch()
        events[0]["amount_raw"] = 999_999
        events[0]["amount_received_raw"] = 1_000_000
        report = validate_batch(events)
        gate = next(g for g in report.gate_results if g.gate_name == "reconciliation_sample")
        assert gate.passed is False
        assert "amount_raw" in gate.violations[0]

    def test_fails_on_unknown_validation_status(self):
        events = _batch(validation_status="unknown_status")
        report = validate_batch(events)
        gate = next(g for g in report.gate_results if g.gate_name == "reconciliation_sample")
        assert gate.passed is False
        assert "unknown_status" in gate.violations[0]

    def test_passes_with_known_statuses(self):
        for status in ("ok", "degraded", "failed", "partial"):
            events = _batch(validation_status=status)
            report = validate_batch(events)
            gate = next(g for g in report.gate_results if g.gate_name == "reconciliation_sample")
            assert gate.passed is True, f"Status {status!r} should pass"

    def test_fails_on_empty_raw_event_id(self):
        events = _batch()
        events[0]["raw_event_id"] = ""
        report = validate_batch(events)
        gate = next(g for g in report.gate_results if g.gate_name == "reconciliation_sample")
        assert gate.passed is False
        assert "raw_event_id" in gate.violations[0]

    def test_only_checks_up_to_sample_size(self):
        # 30 events; events 21+ have an invalid status — should NOT be checked
        events = _batch(VALIDATION_SAMPLE_SIZE + 10)
        for e in events[VALIDATION_SAMPLE_SIZE:]:
            e["validation_status"] = "totally_bogus"
        report = validate_batch(events, sample_size=VALIDATION_SAMPLE_SIZE)
        gate = next(g for g in report.gate_results if g.gate_name == "reconciliation_sample")
        assert gate.passed is True
        assert gate.checked == VALIDATION_SAMPLE_SIZE

    def test_empty_batch_passes_reconciliation(self):
        report = validate_batch([])
        gate = next(g for g in report.gate_results if g.gate_name == "reconciliation_sample")
        assert gate.passed is True
        assert gate.checked == 0


# ---------------------------------------------------------------------------
# assert_batch_approved
# ---------------------------------------------------------------------------

class TestAssertBatchApproved:
    def test_returns_report_when_approved(self):
        events = _batch()
        report = assert_batch_approved(events)
        assert isinstance(report, ValidationReport)
        assert report.approved is True

    def test_raises_when_rejected(self):
        events = _batch()
        events[0]["amount_raw"] = 9.99  # float
        with pytest.raises(ValueError, match="Batch validation failed"):
            assert_batch_approved(events)


# ---------------------------------------------------------------------------
# GateResult.summary
# ---------------------------------------------------------------------------

class TestGateResultSummary:
    def test_pass_summary_format(self):
        gate = GateResult(gate_name="my_gate", passed=True, checked=10, failed_count=0)
        assert "[PASS]" in gate.summary
        assert "my_gate" in gate.summary

    def test_fail_summary_includes_first_violation(self):
        gate = GateResult(
            gate_name="my_gate",
            passed=False,
            checked=10,
            failed_count=2,
            violations=["violation one", "violation two"],
        )
        assert "[FAIL]" in gate.summary
        assert "violation one" in gate.summary
        assert "+1 more" in gate.summary
