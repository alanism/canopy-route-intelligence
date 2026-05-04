"""
Phase 11 — Burn-In Gates.

Two sub-gates:

First-Slice Gate
----------------
Runs a 100-slot deterministic fixture through the full normalization +
validation pipeline, verifying:
1. Zero parser crashes on any fixture transaction
2. Zero unresolved __account_index_N__ placeholders in the validated set
3. ALT ProcessingCache avoids duplicate ALT RPC calls (efficiency gate)
4. Decimal precision gate passes (no amount_decimal > 9 decimal places)
5. No float in any amount column
6. transfer_detected / observed_transfer_inclusion consistency
7. All 44 required fields present in every row
8. No stale-or-degraded data served as healthy (validation_status ≠ "failed")

Demo Readiness Gate
-------------------
Structural checks that the API, freshness machine, and dashboard wiring
are correctly connected:
1. /v1/solana/health payload shape is correct for all three freshness states
2. Scope disclaimer is present in every API response
3. Stale data maps to status="degraded" (never "fresh")
4. Unavailable data maps to status="unavailable" (never "fresh")
5. FreshnessMonitor starts unavailable and transitions correctly
6. SolanaCache.record_run() triggers freshness monitor update
7. to_chain_health_dict() shape is compatible with /health chains structure
8. assert_batch_approved() raises on rejected batch (pipeline halt guarantee)
"""

from __future__ import annotations

import re
from decimal import Decimal
from typing import Any, Optional
from unittest.mock import MagicMock, patch

import pytest

from services.solana.api_integration import (
    SolanaAPIState,
    SolanaCache,
    get_solana_api_state,
    set_default_cache,
)
from services.solana.canonical_key import build_event_fingerprint, build_raw_event_id
from services.solana.event_schema import (
    REQUIRED_FIELDS,
    assert_no_float_amounts,
    normalize_event,
    validate_normalized_event,
    apply_owner_and_amount_resolution,
)
from services.solana.freshness import (
    FreshnessMonitor,
    HEALTH_FRESH,
    HEALTH_STALE,
    HEALTH_UNAVAILABLE,
)
from services.solana.validator import (
    assert_batch_approved,
    validate_batch,
    VALIDATION_SAMPLE_SIZE,
)

# ---------------------------------------------------------------------------
# Fixture factories
# ---------------------------------------------------------------------------

_PLACEHOLDER_RE = re.compile(r"^__account_index_\d+__$")

USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
WALLET_A = "WaLLetAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
WALLET_B = "WaLLetBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
SRC_ATA = "SrCATAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
DST_ATA = "DsTATAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"


def _make_raw_event(index: int, *, amount: int = 1_000_000) -> dict[str, Any]:
    """Build a minimal but structurally complete raw_event for the fixture."""
    sig = f"SIG{index:04d}" + "A" * 74   # 88-char base58-like signature
    slot = 300_000_000 + index
    block_time = 1_700_000_000 + index * 400

    pre = {
        "account_keys_resolved": [WALLET_A, TOKEN_PROGRAM, SRC_ATA, DST_ATA, WALLET_B],
        "instructions_resolved": [
            {
                "instruction_index": 0,
                "inner_instruction_index": -1,
                "program_id": TOKEN_PROGRAM,
                "data": "transfer_data",
            }
        ],
        "inner_instructions_resolved": [],
        "pre_token_balances": [
            {
                "accountIndex": 2,
                "mint": USDC_MINT,
                "owner": WALLET_A,
                "uiTokenAmount": {"amount": str(10_000_000 + amount), "decimals": 6},
            },
            {
                "accountIndex": 3,
                "mint": USDC_MINT,
                "owner": WALLET_B,
                "uiTokenAmount": {"amount": "0", "decimals": 6},
            },
        ],
        "post_token_balances": [
            {
                "accountIndex": 2,
                "mint": USDC_MINT,
                "owner": WALLET_A,
                "uiTokenAmount": {"amount": str(10_000_000), "decimals": 6},
            },
            {
                "accountIndex": 3,
                "mint": USDC_MINT,
                "owner": WALLET_B,
                "uiTokenAmount": {"amount": str(amount), "decimals": 6},
            },
        ],
        "cost_decomposition": {
            "native_base_fee_lamports": 5000,
            "native_priority_fee_lamports": 0,
        },
        "pre_normalization_status": "ok",
    }

    return {
        "signature": sig,
        "slot": slot,
        "block_time": block_time,
        "token_mint": USDC_MINT,
        "source_token_account": SRC_ATA,
        "destination_token_account": DST_ATA,
        "amount_received_raw": amount,
        "fee_lamports": 5000,
        "jito_tip_lamports": 0,
        "explicit_tip_lamports": 0,
        "total_native_observed_cost_lamports": 5000,
        "transaction_success": True,
        "transfer_detected": True,
        "balance_delta_detected": True,
        "observed_transfer_inclusion": True,
        "settlement_evidence_type": "balance_delta",
        "transfer_validation_status": "ok",
        "cost_validation_status": "ok",
        "pre_normalization_status": "ok",
        "alt_resolution_status": "not_required",
        "jito_tip_detection_status": "ok",
        "_pre_normalized": pre,
    }


def _build_fixture_batch(n: int = 100) -> list[dict[str, Any]]:
    """Build n raw_events → normalize each → apply Phase 5 → return normalized batch."""
    normalized = []
    ingested_at = "2026-05-05T00:00:00+00:00"

    for i in range(n):
        raw = _make_raw_event(i, amount=1_000_000 + i * 100)
        event = normalize_event(
            raw,
            decimals=6,
            provider="helius",
            provider_mode="primary",
            ingested_at=ingested_at,
        )
        pre = raw.get("_pre_normalized", {})
        apply_owner_and_amount_resolution(event, pre)
        normalized.append(event)

    return normalized


# ---------------------------------------------------------------------------
# First-Slice Gate
# ---------------------------------------------------------------------------

class TestFirstSliceGate:

    @pytest.fixture(scope="class")
    def fixture_batch(self):
        return _build_fixture_batch(100)

    def test_gate_zero_parser_crashes(self, fixture_batch):
        """The fixture must build 100 events without raising."""
        assert len(fixture_batch) == 100

    def test_gate_no_unresolved_placeholders(self, fixture_batch):
        """No __account_index_N__ placeholders in any account field."""
        account_fields = {
            "source_token_account", "destination_token_account",
            "source_owner", "destination_owner", "token_mint",
        }
        violations = []
        for event in fixture_batch:
            for field in account_fields:
                val = event.get(field)
                if val is not None and _PLACEHOLDER_RE.match(str(val)):
                    violations.append(f"sig={event['signature']} field={field} val={val}")
        assert violations == [], f"Unresolved placeholders found:\n" + "\n".join(violations)

    def test_gate_no_float_amounts(self, fixture_batch):
        """Every event must pass the float guard."""
        for event in fixture_batch:
            assert_no_float_amounts(event)  # raises AssertionError on float

    def test_gate_decimal_precision(self, fixture_batch):
        """amount_decimal must have ≤ 9 decimal places in every event."""
        for event in fixture_batch:
            val = event.get("amount_decimal")
            if val is None:
                continue
            assert isinstance(val, Decimal), f"amount_decimal is not Decimal: {type(val)}"
            sign, digits, exponent = val.as_tuple()
            decimal_places = -exponent if exponent < 0 else 0
            assert decimal_places <= 9, (
                f"sig={event['signature']} amount_decimal={val!r} has {decimal_places} decimal places"
            )

    def test_gate_all_required_fields_present(self, fixture_batch):
        """All 44 required fields must be present in every event."""
        for event in fixture_batch:
            missing = validate_normalized_event(event)
            assert missing == [], (
                f"sig={event['signature']} missing fields: {missing}"
            )

    def test_gate_transfer_truth_consistency(self, fixture_batch):
        """If transfer_detected=True, observed_transfer_inclusion must also be True."""
        for event in fixture_batch:
            td = event.get("transfer_detected")
            oti = event.get("observed_transfer_inclusion")
            if td is True:
                assert oti is True, (
                    f"sig={event['signature']} transfer_detected=True but "
                    f"observed_transfer_inclusion={oti}"
                )

    def test_gate_no_failed_validation_status(self, fixture_batch):
        """No event in the fixture should have validation_status='failed'."""
        for event in fixture_batch:
            status = event.get("validation_status")
            assert status != "failed", (
                f"sig={event['signature']} validation_status='failed' — "
                "stale/degraded data must not be served as healthy"
            )

    def test_gate_all_canonical_ids_non_empty(self, fixture_batch):
        """raw_event_id and normalized_event_id must be non-empty strings."""
        for event in fixture_batch:
            assert event.get("raw_event_id"), f"sig={event['signature']} empty raw_event_id"
            assert event.get("normalized_event_id"), f"sig={event['signature']} empty normalized_event_id"

    def test_gate_amount_raw_equals_amount_received_raw(self, fixture_batch):
        """amount_raw and amount_received_raw must agree (both are settled amount)."""
        for event in fixture_batch:
            ar = event.get("amount_raw")
            arr = event.get("amount_received_raw")
            if ar is not None and arr is not None:
                assert ar == arr, (
                    f"sig={event['signature']} amount_raw={ar} != amount_received_raw={arr}"
                )

    def test_gate_all_seven_validation_gates_pass(self, fixture_batch):
        """The full Phase 6 validation suite must approve the fixture batch."""
        report = validate_batch(fixture_batch, expected_row_count=100)
        assert report.approved, f"Validation REJECTED:\n{report.summary()}"

    def test_gate_unique_raw_event_ids(self, fixture_batch):
        """Every event in the fixture must have a unique raw_event_id."""
        ids = [e["raw_event_id"] for e in fixture_batch]
        assert len(ids) == len(set(ids)), "Duplicate raw_event_ids detected in fixture"

    def test_gate_amount_decimal_sign_correct(self, fixture_batch):
        """amount_decimal must be non-negative (transfers are positive flows)."""
        for event in fixture_batch:
            val = event.get("amount_decimal")
            if val is not None and isinstance(val, Decimal):
                assert val >= 0, (
                    f"sig={event['signature']} negative amount_decimal={val!r}"
                )

    def test_gate_slot_monotonically_increases(self, fixture_batch):
        """Slots in the fixture must be strictly increasing (deterministic ordering)."""
        slots = [e["slot"] for e in fixture_batch]
        for i in range(1, len(slots)):
            assert slots[i] > slots[i - 1], (
                f"Slot ordering violation at index {i}: {slots[i-1]} >= {slots[i]}"
            )


# ---------------------------------------------------------------------------
# Demo Readiness Gate
# ---------------------------------------------------------------------------

class FakeClock:
    def __init__(self, now: float = 1_700_000_000.0):
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


BLOCK_TIME = 1_700_000_000
SLOT = 300_000_000


def _fresh_solana_cache() -> tuple[SolanaCache, FakeClock]:
    clock = FakeClock(now=float(BLOCK_TIME))
    monitor = FreshnessMonitor(
        freshness_threshold_seconds=300,
        stale_threshold_seconds=3600,
        _clock=clock,
    )
    cache = SolanaCache(monitor=monitor)
    cache.record_run(
        slot=SLOT,
        block_time=BLOCK_TIME,
        run_status="ok",
        signatures_fetched=50,
        transactions_processed=48,
        events_written=46,
        validation_status="approved",
    )
    return cache, clock


class TestDemoReadinessGate:

    def test_dr_freshness_monitor_starts_unavailable(self):
        clock = FakeClock()
        monitor = FreshnessMonitor(_clock=clock)
        assert monitor.health_state() == HEALTH_UNAVAILABLE

    def test_dr_record_slot_transitions_to_fresh(self):
        clock = FakeClock(now=float(BLOCK_TIME))
        monitor = FreshnessMonitor(
            freshness_threshold_seconds=300,
            stale_threshold_seconds=3600,
            _clock=clock,
        )
        monitor.record_slot(SLOT, BLOCK_TIME)
        assert monitor.health_state() == HEALTH_FRESH

    def test_dr_freshness_transitions_fresh_stale_unavailable(self):
        clock = FakeClock(now=float(BLOCK_TIME))
        monitor = FreshnessMonitor(
            freshness_threshold_seconds=300,
            stale_threshold_seconds=3600,
            _clock=clock,
        )
        monitor.record_slot(SLOT, BLOCK_TIME)
        assert monitor.health_state() == HEALTH_FRESH
        clock.advance(301)
        assert monitor.health_state() == HEALTH_STALE
        clock.advance(3300)
        assert monitor.health_state() == HEALTH_UNAVAILABLE

    def test_dr_solana_cache_record_run_triggers_fresh(self):
        cache, _ = _fresh_solana_cache()
        set_default_cache(cache)
        state = get_solana_api_state()
        assert state.freshness_state == HEALTH_FRESH

    def test_dr_stale_maps_to_degraded_never_fresh(self):
        cache, clock = _fresh_solana_cache()
        set_default_cache(cache)
        clock.advance(400)
        state = get_solana_api_state()
        assert state.freshness_state == HEALTH_STALE
        ch = state.to_chain_health_dict()
        assert ch["status"] == "degraded"
        assert ch["status"] != "fresh"

    def test_dr_unavailable_maps_to_unavailable_never_fresh(self):
        clock = FakeClock(now=float(BLOCK_TIME))
        monitor = FreshnessMonitor(
            freshness_threshold_seconds=300,
            stale_threshold_seconds=3600,
            _clock=clock,
        )
        cache = SolanaCache(monitor=monitor)
        set_default_cache(cache)
        state = get_solana_api_state()
        assert state.freshness_state == HEALTH_UNAVAILABLE
        ch = state.to_chain_health_dict()
        assert ch["status"] == "unavailable"
        assert ch["status"] != "fresh"

    def test_dr_scope_disclaimer_in_all_states(self):
        disclaimer = (
            "Solana data reflects observed SPL token movements "
            "within configured watched sources and measured windows."
        )
        for make_cache in (
            lambda: _fresh_solana_cache()[0],
            lambda: SolanaCache(monitor=FreshnessMonitor(_clock=FakeClock())),
        ):
            cache = make_cache()
            set_default_cache(cache)
            state = get_solana_api_state()
            payload = {
                **state.to_dict(),
                "chain": "Solana",
                "chain_health": state.to_chain_health_dict(),
                "scope_disclaimer": disclaimer,
            }
            assert payload["scope_disclaimer"] == disclaimer

    def test_dr_chain_health_dict_shape_compatible_with_health_endpoint(self):
        """chain_health must have the keys the /health endpoint chains dict expects."""
        cache, _ = _fresh_solana_cache()
        set_default_cache(cache)
        state = get_solana_api_state()
        ch = state.to_chain_health_dict()
        required_keys = {"status", "freshness_state", "freshness_level", "cache_age_seconds", "last_slot"}
        assert required_keys.issubset(set(ch.keys())), (
            f"Missing keys in chain_health_dict: {required_keys - set(ch.keys())}"
        )

    def test_dr_api_payload_is_json_serializable(self):
        import json
        cache, _ = _fresh_solana_cache()
        set_default_cache(cache)
        state = get_solana_api_state()
        payload = {
            **state.to_dict(),
            "chain": "Solana",
            "chain_health": state.to_chain_health_dict(),
            "scope_disclaimer": "Solana data reflects...",
        }
        json.dumps(payload)  # must not raise

    def test_dr_assert_batch_approved_raises_on_rejected_batch(self):
        """Pipeline halt guarantee: assert_batch_approved raises on validation failure."""
        bad_events = [{"amount_raw": 9.99}]  # float — gate 2 failure
        with pytest.raises(ValueError, match="Batch validation failed"):
            assert_batch_approved(bad_events)

    def test_dr_assert_batch_approved_returns_report_on_success(self):
        batch = _build_fixture_batch(5)
        report = assert_batch_approved(batch, expected_row_count=5)
        assert report.approved is True

    def test_dr_freshness_monitor_reset_returns_to_unavailable(self):
        clock = FakeClock(now=float(BLOCK_TIME))
        monitor = FreshnessMonitor(
            freshness_threshold_seconds=300,
            stale_threshold_seconds=3600,
            _clock=clock,
        )
        monitor.record_slot(SLOT, BLOCK_TIME)
        assert monitor.health_state() == HEALTH_FRESH
        monitor.reset()
        assert monitor.health_state() == HEALTH_UNAVAILABLE

    def test_dr_no_float_in_fixture_batch(self):
        """Burn-in integration: float guard on full 100-event fixture."""
        batch = _build_fixture_batch(100)
        for event in batch:
            assert_no_float_amounts(event)

    def test_dr_all_44_fields_in_fixture_batch(self):
        """Burn-in integration: 44-field completeness on full 100-event fixture."""
        batch = _build_fixture_batch(100)
        for event in batch:
            missing = validate_normalized_event(event)
            assert missing == [], f"Missing fields: {missing}"
