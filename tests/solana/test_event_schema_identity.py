from __future__ import annotations

from decimal import Decimal
from typing import Any

from services.solana.constants import SPL_TOKEN_PROGRAM, USDC_MINT
from services.solana.event_schema import (
    assign_identity_and_dedupe_batch,
    normalize_event,
)


def _make_raw_event(
    *,
    signature: str,
    instruction_index: int = 0,
    inner_instruction_index: int = -1,
    amount_received_raw: int = 1_000_000,
    source_token_account: str = "AtaAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    destination_token_account: str = "AtaBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
) -> dict[str, Any]:
    return {
        "signature": signature,
        "slot": 999_000,
        "block_time": 1_700_000_000,
        "chain": "solana",
        "pre_normalization_status": "ok",
        "alt_resolution_status": "not_required",
        "transaction_version": "legacy",
        "transaction_success": True,
        "observed_transfer_inclusion": True,
        "transfer_detected": True,
        "balance_delta_detected": True,
        "settlement_evidence_type": "balance_delta",
        "amount_received_raw": amount_received_raw,
        "source_token_account": source_token_account,
        "destination_token_account": destination_token_account,
        "token_mint": USDC_MINT,
        "transfer_validation_status": "ok",
        "fee_lamports": 5000,
        "jito_tip_lamports": 0,
        "explicit_tip_lamports": 0,
        "total_native_observed_cost_lamports": 5000,
        "cost_validation_status": "ok",
        "jito_tip_detection_status": "ok",
        "_pre_normalized": {
            "pre_normalization_status": "ok",
            "alt_resolution_status": "not_required",
            "instructions_resolved": [
                {
                    "program_id": SPL_TOKEN_PROGRAM,
                    "instruction_index": instruction_index,
                    "inner_instruction_index": inner_instruction_index,
                    "data": "3Bxs3zr3hH7HgAGB",
                }
            ],
            "inner_instructions_resolved": [],
        },
    }


def _normalize(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [normalize_event(e) for e in events]


def test_no_collision_batch():
    events = _normalize(
        [
            _make_raw_event(signature="SigA" + "a" * 84),
            _make_raw_event(signature="SigB" + "b" * 84),
            _make_raw_event(signature="SigC" + "c" * 84),
            _make_raw_event(signature="SigD" + "d" * 84),
            _make_raw_event(signature="SigE" + "e" * 84),
        ]
    )
    out = assign_identity_and_dedupe_batch(events)
    assert len(out) == 5
    assert all(e["transfer_ordinal"] == 0 for e in out)
    assert all(e["collision_detected"] is False for e in out)


def test_single_collision():
    sig = "SigCOLLIDE" + "x" * 79
    events = _normalize(
        [
            _make_raw_event(signature=sig, amount_received_raw=1_000_000),
            _make_raw_event(signature=sig, amount_received_raw=2_000_000),
        ]
    )
    out = assign_identity_and_dedupe_batch(events)
    assert [e["transfer_ordinal"] for e in out] == [0, 1]
    assert all(e["collision_detected"] is True for e in out)


def test_multiple_collisions_same_key():
    sig = "SigMULTI" + "m" * 81
    events = _normalize(
        [
            _make_raw_event(signature=sig, amount_received_raw=1),
            _make_raw_event(signature=sig, amount_received_raw=2),
            _make_raw_event(signature=sig, amount_received_raw=3),
        ]
    )
    out = assign_identity_and_dedupe_batch(events)
    assert [e["transfer_ordinal"] for e in out] == [0, 1, 2]
    assert all(e["collision_detected"] is True for e in out)


def test_collisions_across_keys():
    sig_a = "SigAKEY" + "a" * 82
    sig_b = "SigBKEY" + "b" * 82
    events = _normalize(
        [
            _make_raw_event(signature=sig_a, amount_received_raw=1),
            _make_raw_event(signature=sig_a, amount_received_raw=2),
            _make_raw_event(signature=sig_b, amount_received_raw=3),
            _make_raw_event(signature=sig_b, amount_received_raw=4),
            _make_raw_event(signature="SigCLEAN" + "c" * 80, amount_received_raw=5),
        ]
    )
    out = assign_identity_and_dedupe_batch(events)
    by_sig = {}
    for e in out:
        by_sig.setdefault(e["signature"], []).append(e)
    assert sorted(e["transfer_ordinal"] for e in by_sig[sig_a]) == [0, 1]
    assert sorted(e["transfer_ordinal"] for e in by_sig[sig_b]) == [0, 1]
    assert by_sig["SigCLEAN" + "c" * 80][0]["collision_detected"] is False


def test_determinism_shuffled_input():
    sig = "SigDET" + "d" * 83
    base = _normalize(
        [
            _make_raw_event(signature=sig, amount_received_raw=3),
            _make_raw_event(signature=sig, amount_received_raw=1),
            _make_raw_event(signature=sig, amount_received_raw=2),
            _make_raw_event(signature="SigUNIQ" + "u" * 80, amount_received_raw=7),
            _make_raw_event(signature="SigUNIQ2" + "v" * 79, amount_received_raw=8),
        ]
    )
    shuffled = [base[2], base[4], base[0], base[3], base[1]]
    out_a = assign_identity_and_dedupe_batch(list(base))
    out_b = assign_identity_and_dedupe_batch(list(shuffled))
    ids_a = sorted((e["raw_event_id"], e["normalized_event_id"], e["transfer_ordinal"]) for e in out_a)
    ids_b = sorted((e["raw_event_id"], e["normalized_event_id"], e["transfer_ordinal"]) for e in out_b)
    assert ids_a == ids_b


def test_distinct_same_coordinate():
    sig = "SigCOORD" + "q" * 81
    events = _normalize(
        [
            _make_raw_event(signature=sig, amount_received_raw=100),
            _make_raw_event(signature=sig, amount_received_raw=101),
        ]
    )
    out = assign_identity_and_dedupe_batch(events)
    assert len(out) == 2
    assert out[0]["normalized_event_id"] != out[1]["normalized_event_id"]
    assert sorted(e["transfer_ordinal"] for e in out) == [0, 1]
    assert all(e["collision_detected"] is True for e in out)


def test_duplicate_exact_replay():
    sig = "SigDUP" + "p" * 83
    event = normalize_event(_make_raw_event(signature=sig, amount_received_raw=123))
    out = assign_identity_and_dedupe_batch([event.copy(), event.copy()])
    assert len(out) == 1
    assert out[0]["transfer_ordinal"] == 0


def test_cross_transaction_non_collision():
    shared_ix = 5
    shared_inner = -1
    events = _normalize(
        [
            _make_raw_event(
                signature="SigTX1" + "1" * 82,
                instruction_index=shared_ix,
                inner_instruction_index=shared_inner,
            ),
            _make_raw_event(
                signature="SigTX2" + "2" * 82,
                instruction_index=shared_ix,
                inner_instruction_index=shared_inner,
            ),
        ]
    )
    out = assign_identity_and_dedupe_batch(events)
    assert len(out) == 2
    assert all(e["transfer_ordinal"] == 0 for e in out)
    assert all(e["collision_detected"] is False for e in out)


def test_stable_serialization():
    raw = _make_raw_event(signature="SigDEC" + "z" * 83, amount_received_raw=1_000_000)
    e1 = normalize_event(raw)
    e2 = normalize_event(raw)
    assert e1["amount_decimal"] == Decimal("1")
    assert e1["event_fingerprint"] == e2["event_fingerprint"]
