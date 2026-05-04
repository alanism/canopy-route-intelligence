"""
Phase 4 — Normalized Event Schema + BigQuery Writer Tests.

Covers:
event_schema.normalize_event():
- All 44 required fields present in output
- No float in any amount field
- amount_decimal is Decimal, not float
- amount_decimal = Decimal(amount_raw) / Decimal(10 ** decimals) exactly
- raw_event_id uses solana: prefix format
- normalized_event_id is raw_event_id + fingerprint suffix
- validation_status is "degraded" when any sub-phase is degraded
- validation_status is "ok" when all sub-phases are ok
- owner fields are None (Phase 5 placeholder)
- owner_resolution_status = "pending"
- missing raw amount → amount_decimal = None, not crash
- ingested_at is populated

validate_normalized_event():
- returns empty list for complete event
- returns missing field names for incomplete event

assert_no_float_amounts():
- passes on clean event
- raises AssertionError when float found in amount field

bigquery_writer._serialize_for_bq():
- BIGNUMERIC fields serialized as str
- None values preserved as None
- private '_' keys stripped
- float in BIGNUMERIC field raises TypeError
- Decimal values serialized as str

SolanaEventWriter.write_batch():
- empty batch returns WriteResult with 0 rows
- fallback path used when no BQ client
- fallback writes valid JSONL
- write_batch returns WriteResult with rows_inserted count
- BQ errors captured in WriteResult.errors
"""

from __future__ import annotations

import json
import os
from decimal import Decimal
from typing import Any, Optional

import pytest

from services.solana.bigquery_writer import (
    SolanaEventWriter,
    WriteResult,
    _serialize_for_bq,
)
from services.solana.constants import SPL_TOKEN_PROGRAM, USDC_MINT
from services.solana.event_schema import (
    REQUIRED_FIELDS,
    assert_no_float_amounts,
    normalize_event,
    validate_normalized_event,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

WALLET_A = "WalletAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
WALLET_B = "WalletBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
ATA_A = "AtaAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
ATA_B = "AtaBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
SIG_1 = "Sig1" + "1" * 84


def _make_raw_event(
    *,
    signature: str = SIG_1,
    amount_received_raw: int = 1_000_000,
    transaction_success: bool = True,
    observed_transfer_inclusion: bool = True,
    transfer_validation_status: str = "ok",
    cost_validation_status: str = "ok",
    pre_normalization_status: str = "ok",
    alt_resolution_status: str = "not_required",
    fee_lamports: int = 5000,
    jito_tip_lamports: int = 0,
    explicit_tip_lamports: int = 0,
    total_native_observed_cost_lamports: int = 5000,
    token_mint: str = USDC_MINT,
    source_token_account: Optional[str] = ATA_A,
    destination_token_account: Optional[str] = ATA_B,
) -> dict[str, Any]:
    """Build a minimal raw_event as produced by SolanaIngestionAdapter."""
    pre_normalized = {
        "pre_normalization_status": pre_normalization_status,
        "alt_resolution_status": alt_resolution_status,
        "instructions_resolved": [
            {
                "program_id": SPL_TOKEN_PROGRAM,
                "instruction_index": 0,
                "inner_instruction_index": -1,
                "accounts": [ATA_A, ATA_B, WALLET_A],
                "data": "3Bxs3zr3hH7HgAGB",
            }
        ],
        "inner_instructions_resolved": [],
    }
    return {
        "signature": signature,
        "slot": 999_000,
        "block_time": 1_700_000_000,
        "chain": "solana",
        "pre_normalization_status": pre_normalization_status,
        "alt_resolution_status": alt_resolution_status,
        "transaction_version": "legacy",
        "transaction_success": transaction_success,
        "observed_transfer_inclusion": observed_transfer_inclusion,
        "transfer_detected": True,
        "balance_delta_detected": True,
        "settlement_evidence_type": "balance_delta",
        "amount_received_raw": amount_received_raw,
        "source_token_account": source_token_account,
        "destination_token_account": destination_token_account,
        "token_mint": token_mint,
        "transfer_validation_status": transfer_validation_status,
        "fee_lamports": fee_lamports,
        "jito_tip_lamports": jito_tip_lamports,
        "explicit_tip_lamports": explicit_tip_lamports,
        "total_native_observed_cost_lamports": total_native_observed_cost_lamports,
        "cost_validation_status": cost_validation_status,
        "jito_tip_detection_status": "ok",
        "_pre_normalized": pre_normalized,
    }


# ---------------------------------------------------------------------------
# normalize_event — field completeness
# ---------------------------------------------------------------------------

class TestNormalizeEventCompleteness:

    def test_all_required_fields_present(self):
        event = normalize_event(_make_raw_event())
        missing = validate_normalized_event(event)
        assert missing == [], f"Missing required fields: {missing}"

    def test_chain_is_solana(self):
        event = normalize_event(_make_raw_event())
        assert event["chain"] == "solana"

    def test_signature_preserved(self):
        event = normalize_event(_make_raw_event(signature=SIG_1))
        assert event["signature"] == SIG_1

    def test_slot_preserved(self):
        event = normalize_event(_make_raw_event())
        assert event["slot"] == 999_000

    def test_token_mint_preserved(self):
        event = normalize_event(_make_raw_event())
        assert event["token_mint"] == USDC_MINT

    def test_decode_version_is_string(self):
        event = normalize_event(_make_raw_event())
        assert isinstance(event["decode_version"], str)
        assert event["decode_version"] != ""

    def test_ingested_at_populated(self):
        event = normalize_event(_make_raw_event())
        assert event["ingested_at"] is not None

    def test_ingested_at_injectable(self):
        event = normalize_event(_make_raw_event(), ingested_at="2026-05-04T00:00:00+00:00")
        assert event["ingested_at"] == "2026-05-04T00:00:00+00:00"


# ---------------------------------------------------------------------------
# normalize_event — owner fields (Phase 5 placeholders)
# ---------------------------------------------------------------------------

class TestOwnerPlaceholders:

    def test_source_owner_is_none(self):
        event = normalize_event(_make_raw_event())
        assert event["source_owner"] is None

    def test_destination_owner_is_none(self):
        event = normalize_event(_make_raw_event())
        assert event["destination_owner"] is None

    def test_owner_resolution_status_is_pending(self):
        event = normalize_event(_make_raw_event())
        assert event["owner_resolution_status"] == "pending"


# ---------------------------------------------------------------------------
# normalize_event — amount precision
# ---------------------------------------------------------------------------

class TestAmountPrecision:

    def test_amount_decimal_is_decimal_not_float(self):
        event = normalize_event(_make_raw_event(amount_received_raw=1_000_000))
        assert isinstance(event["amount_decimal"], Decimal)
        assert not isinstance(event["amount_decimal"], float)

    def test_amount_decimal_exact_6_decimal_places(self):
        event = normalize_event(_make_raw_event(amount_received_raw=1_000_000))
        assert event["amount_decimal"] == Decimal("1.000000")

    def test_amount_decimal_one_lamport_usdc(self):
        event = normalize_event(_make_raw_event(amount_received_raw=1))
        assert event["amount_decimal"] == Decimal("0.000001")

    def test_large_usdc_amount_exact_precision(self):
        # 9,999,999.999999 USDC
        raw = 9_999_999_999_999
        event = normalize_event(_make_raw_event(amount_received_raw=raw))
        assert event["amount_decimal"] == Decimal("9999999.999999")

    def test_zero_amount_decimal_is_zero(self):
        event = normalize_event(_make_raw_event(amount_received_raw=0))
        assert event["amount_decimal"] == Decimal("0")

    def test_none_amount_raw_gives_none_decimal(self):
        raw = _make_raw_event()
        raw["amount_received_raw"] = None
        event = normalize_event(raw)
        assert event["amount_decimal"] is None
        assert event["amount_raw"] is None

    def test_no_float_in_any_amount_field(self):
        event = normalize_event(_make_raw_event(amount_received_raw=500_000))
        assert_no_float_amounts(event)  # raises if float found


# ---------------------------------------------------------------------------
# normalize_event — canonical keys
# ---------------------------------------------------------------------------

class TestCanonicalKeys:

    def test_raw_event_id_has_solana_prefix(self):
        event = normalize_event(_make_raw_event())
        assert event["raw_event_id"].startswith("solana:")

    def test_raw_event_id_contains_signature(self):
        event = normalize_event(_make_raw_event(signature=SIG_1))
        assert SIG_1 in event["raw_event_id"]

    def test_normalized_event_id_starts_with_raw_event_id(self):
        event = normalize_event(_make_raw_event())
        assert event["normalized_event_id"].startswith(event["raw_event_id"])

    def test_event_fingerprint_is_string(self):
        event = normalize_event(_make_raw_event())
        assert isinstance(event["event_fingerprint"], str)
        assert len(event["event_fingerprint"]) > 0

    def test_different_amounts_give_different_fingerprints(self):
        e1 = normalize_event(_make_raw_event(amount_received_raw=1_000_000))
        e2 = normalize_event(_make_raw_event(amount_received_raw=2_000_000))
        assert e1["event_fingerprint"] != e2["event_fingerprint"]


# ---------------------------------------------------------------------------
# normalize_event — validation status
# ---------------------------------------------------------------------------

class TestValidationStatus:

    def test_ok_when_all_sub_phases_ok(self):
        event = normalize_event(_make_raw_event(
            transfer_validation_status="ok",
            cost_validation_status="ok",
            pre_normalization_status="ok",
        ))
        assert event["validation_status"] == "ok"

    def test_degraded_when_transfer_validation_degraded(self):
        event = normalize_event(_make_raw_event(transfer_validation_status="degraded"))
        assert event["validation_status"] == "degraded"

    def test_degraded_when_cost_validation_degraded(self):
        event = normalize_event(_make_raw_event(cost_validation_status="degraded"))
        assert event["validation_status"] == "degraded"

    def test_degraded_when_pre_normalization_failed(self):
        event = normalize_event(_make_raw_event(pre_normalization_status="failed"))
        assert event["validation_status"] == "degraded"


# ---------------------------------------------------------------------------
# validate_normalized_event
# ---------------------------------------------------------------------------

class TestValidateNormalizedEvent:

    def test_returns_empty_list_for_complete_event(self):
        event = normalize_event(_make_raw_event())
        assert validate_normalized_event(event) == []

    def test_returns_missing_field_names(self):
        event = normalize_event(_make_raw_event())
        del event["signature"]
        missing = validate_normalized_event(event)
        assert "signature" in missing

    def test_multiple_missing_fields_all_reported(self):
        event = normalize_event(_make_raw_event())
        del event["chain"]
        del event["slot"]
        missing = validate_normalized_event(event)
        assert "chain" in missing
        assert "slot" in missing


# ---------------------------------------------------------------------------
# assert_no_float_amounts
# ---------------------------------------------------------------------------

class TestAssertNoFloatAmounts:

    def test_passes_on_clean_event(self):
        event = normalize_event(_make_raw_event())
        assert_no_float_amounts(event)  # no exception

    def test_raises_on_float_in_fee_lamports(self):
        event = normalize_event(_make_raw_event())
        event["fee_lamports"] = 5000.0  # inject float
        with pytest.raises(AssertionError, match="Float found"):
            assert_no_float_amounts(event)

    def test_raises_on_float_in_amount_decimal(self):
        event = normalize_event(_make_raw_event())
        event["amount_decimal"] = 1.0  # inject float
        with pytest.raises(AssertionError, match="Float found"):
            assert_no_float_amounts(event)


# ---------------------------------------------------------------------------
# _serialize_for_bq
# ---------------------------------------------------------------------------

class TestSerializeForBQ:

    def test_bignumeric_fields_serialized_as_str(self):
        event = normalize_event(_make_raw_event(amount_received_raw=1_000_000))
        row = _serialize_for_bq(event)
        assert isinstance(row["amount_raw"], str)
        assert isinstance(row["fee_lamports"], str)

    def test_decimal_amount_serialized_as_str(self):
        event = normalize_event(_make_raw_event(amount_received_raw=1_000_000))
        row = _serialize_for_bq(event)
        assert isinstance(row["amount_decimal"], str)
        assert row["amount_decimal"] == "1.000000"

    def test_none_values_preserved_as_none(self):
        event = normalize_event(_make_raw_event())
        event["source_owner"] = None
        row = _serialize_for_bq(event)
        assert row["source_owner"] is None

    def test_private_keys_stripped(self):
        event = normalize_event(_make_raw_event())
        event["_pre_normalized"] = {"some": "data"}
        row = _serialize_for_bq(event)
        assert "_pre_normalized" not in row

    def test_float_in_bignumeric_raises_type_error(self):
        event = normalize_event(_make_raw_event())
        event["fee_lamports"] = 5000.0  # inject float
        with pytest.raises(TypeError, match="Float found in BIGNUMERIC"):
            _serialize_for_bq(event)

    def test_non_amount_string_fields_unchanged(self):
        event = normalize_event(_make_raw_event())
        row = _serialize_for_bq(event)
        assert row["chain"] == "solana"
        assert row["signature"] == SIG_1


# ---------------------------------------------------------------------------
# SolanaEventWriter
# ---------------------------------------------------------------------------

class TestSolanaEventWriter:

    def test_empty_batch_returns_zero_rows(self, tmp_path):
        writer = SolanaEventWriter(fallback_path=str(tmp_path / "buf.jsonl"))
        result = writer.write_batch([])
        assert result.rows_attempted == 0
        assert result.rows_inserted == 0

    def test_fallback_used_when_no_bq_client(self, tmp_path):
        buf_path = str(tmp_path / "buf.jsonl")
        writer = SolanaEventWriter(fallback_path=buf_path)
        events = [normalize_event(_make_raw_event())]
        result = writer.write_batch(events)
        assert result.fallback_used is True
        assert result.rows_inserted == 1
        assert result.success is True

    def test_fallback_writes_valid_jsonl(self, tmp_path):
        buf_path = str(tmp_path / "buf.jsonl")
        writer = SolanaEventWriter(fallback_path=buf_path)
        events = [
            normalize_event(_make_raw_event(signature=SIG_1)),
            normalize_event(_make_raw_event(signature="Sig2" + "2" * 84)),
        ]
        writer.write_batch(events)
        lines = open(buf_path).read().strip().split("\n")
        assert len(lines) == 2
        for line in lines:
            parsed = json.loads(line)
            assert "chain" in parsed
            assert "signature" in parsed

    def test_fallback_appends_across_batches(self, tmp_path):
        buf_path = str(tmp_path / "buf.jsonl")
        writer = SolanaEventWriter(fallback_path=buf_path)
        writer.write_batch([normalize_event(_make_raw_event())])
        writer.write_batch([normalize_event(_make_raw_event())])
        lines = open(buf_path).read().strip().split("\n")
        assert len(lines) == 2

    def test_write_result_success_true_on_clean_write(self, tmp_path):
        writer = SolanaEventWriter(fallback_path=str(tmp_path / "buf.jsonl"))
        result = writer.write_batch([normalize_event(_make_raw_event())])
        assert result.success is True
        assert result.errors == []

    def test_mock_bq_client_used_when_injected(self, tmp_path):
        """Injected BQ client is called — no fallback."""
        inserted_rows = []

        class MockBQClient:
            def dataset(self, name):
                return self
            def table(self, name):
                return self
            def insert_rows_json(self, table_ref, rows):
                inserted_rows.extend(rows)
                return []  # empty = no errors

        writer = SolanaEventWriter(
            bq_client=MockBQClient(),
            fallback_path=str(tmp_path / "buf.jsonl"),
        )
        events = [normalize_event(_make_raw_event())]
        result = writer.write_batch(events)
        assert result.rows_inserted == 1
        assert result.fallback_used is False
        assert len(inserted_rows) == 1

    def test_bq_errors_captured_in_result(self, tmp_path):
        class ErrorBQClient:
            def dataset(self, name): return self
            def table(self, name): return self
            def insert_rows_json(self, table_ref, rows):
                return [{"index": 0, "errors": [{"reason": "invalid"}]}]

        writer = SolanaEventWriter(
            bq_client=ErrorBQClient(),
            fallback_path=str(tmp_path / "buf.jsonl"),
        )
        result = writer.write_batch([normalize_event(_make_raw_event())])
        assert result.errors != []
        assert result.success is False
