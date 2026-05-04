"""
Phase 4 — Normalized Solana Event Schema.

Converts a raw_event dict (output of SolanaIngestionAdapter._process_transaction)
into the fully-normalized Solana event dict required for BigQuery insertion.

All 44 canonical fields are present in every output row — missing source data
produces None, never KeyError. Unknown/missing fields produce `validation_status`
= "degraded", not a crash.

Amount rules
------------
- Raw amounts: int (u64-compatible)
- Decimal amounts: decimal.Decimal — NEVER float
- `amount_decimal` = Decimal(amount_raw) / Decimal(10 ** decimals)
- BigQuery target type: BIGNUMERIC (set in bigquery_writer.py schema)

Field sources
-------------
Most fields are lifted directly from the raw_event dict (built in Phase 1).
Canonical key fields (raw_event_id, normalized_event_id, event_fingerprint,
collision_detected) are built here via canonical_key module.
Owner fields (source_owner, destination_owner) are Phase 5 — set to None here.
"""

from __future__ import annotations

import hashlib
import logging
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from services.solana.canonical_key import (
    build_event_fingerprint,
    build_raw_event_id,
)
from services.solana.constants import USDC_DECIMALS

logger = logging.getLogger("canopy.solana.event_schema")

# Increment when the schema changes in a backward-incompatible way
DECODE_VERSION = "1"

# Required field names — used for completeness validation
REQUIRED_FIELDS: frozenset[str] = frozenset({
    "chain", "signature", "slot", "block_time",
    "token_mint", "source_token_account", "destination_token_account",
    "source_owner", "destination_owner",
    "instruction_index", "inner_instruction_index", "transfer_ordinal",
    "program_id",
    "amount_raw", "amount_decimal",
    "amount_transferred_raw", "fee_withheld_raw", "amount_received_raw",
    "fee_lamports", "native_base_fee_lamports", "native_priority_fee_lamports",
    "jito_tip_lamports", "explicit_tip_lamports", "total_native_observed_cost_lamports",
    "transaction_success", "transfer_detected", "balance_delta_detected",
    "observed_transfer_inclusion", "settlement_evidence_type",
    "decode_version", "validation_status",
    "cost_detection_status", "tip_detection_status",
    "provider", "provider_mode",
    "raw_event_id", "normalized_event_id", "event_fingerprint", "collision_detected",
    "alt_resolution_status", "owner_resolution_status", "amount_resolution_status",
    "ingested_at",
})


def normalize_event(
    raw_event: dict[str, Any],
    *,
    decimals: int = USDC_DECIMALS,
    provider: str = "primary",
    provider_mode: str = "primary",
    ingested_at: Optional[str] = None,
) -> dict[str, Any]:
    """
    Normalize a raw ingestion event into the canonical 44-field schema.

    Parameters
    ----------
    raw_event:
        Output of SolanaIngestionAdapter._process_transaction(). Must include
        '_pre_normalized' for instruction-level field resolution.
    decimals:
        Token decimal places for amount_decimal computation (default: 6 for USDC).
    provider:
        RPC provider name (e.g. "helius", "triton", "public").
    provider_mode:
        "primary" or "fallback".
    ingested_at:
        ISO8601 UTC timestamp for when the row is written. Injected for testability.

    Returns
    -------
    Fully normalized event dict. All required fields present. No float amounts.
    """
    from datetime import datetime, timezone
    if ingested_at is None:
        ingested_at = datetime.now(timezone.utc).isoformat()

    pre = raw_event.get("_pre_normalized") or {}
    instructions = pre.get("instructions_resolved") or []
    inner_instructions = pre.get("inner_instructions_resolved") or []

    # ------------------------------------------------------------------
    # Instruction-level fields — pick the first transfer instruction found
    # ------------------------------------------------------------------
    ix_fields = _extract_instruction_fields(instructions, inner_instructions)

    # ------------------------------------------------------------------
    # Amount fields — raw int → Decimal
    # ------------------------------------------------------------------
    amount_received_raw: Optional[int] = _safe_int(raw_event.get("amount_received_raw"))
    amount_transferred_raw: Optional[int] = _safe_int(
        raw_event.get("_pre_normalized", {}) and None  # Phase 5 resolves this
    )
    fee_withheld_raw: Optional[int] = None  # Phase 5 / Token-2022

    amount_decimal: Optional[Decimal] = _to_decimal(amount_received_raw, decimals)

    # ------------------------------------------------------------------
    # Cost fields
    # ------------------------------------------------------------------
    fee_lamports = _safe_int(raw_event.get("fee_lamports"))
    jito_tip_lamports = _safe_int(raw_event.get("jito_tip_lamports")) or 0
    explicit_tip_lamports = _safe_int(raw_event.get("explicit_tip_lamports")) or 0
    total_cost = _safe_int(raw_event.get("total_native_observed_cost_lamports"))

    # Base fee estimate = 5000 * sig count (already computed in cost_decomposition)
    native_base_fee_lamports = _safe_int(
        (pre.get("cost_decomposition") or {}).get("native_base_fee_lamports")
    )
    native_priority_fee_lamports = _safe_int(
        (pre.get("cost_decomposition") or {}).get("native_priority_fee_lamports")
    )

    # ------------------------------------------------------------------
    # Canonical key construction
    # ------------------------------------------------------------------
    signature = raw_event.get("signature") or ""
    instruction_index = ix_fields["instruction_index"]
    inner_instruction_index = ix_fields["inner_instruction_index"]

    raw_event_id = build_raw_event_id(signature, instruction_index, inner_instruction_index)

    program_id = ix_fields["program_id"] or ""
    token_mint = raw_event.get("token_mint") or ""
    source_token_account = raw_event.get("source_token_account") or ""
    destination_token_account = raw_event.get("destination_token_account") or ""

    event_fingerprint = build_event_fingerprint(
        program_id=program_id,
        token_mint=token_mint,
        source_token_account=source_token_account,
        destination_token_account=destination_token_account,
        amount_raw=amount_received_raw or 0,
        instruction_data_hash=ix_fields.get("data_hash") or "",
    )

    # normalized_event_id: fingerprint-scoped canonical key (collision-safe)
    normalized_event_id = _build_normalized_event_id(raw_event_id, event_fingerprint)

    # ------------------------------------------------------------------
    # Validation status — aggregate across all sub-phases
    # ------------------------------------------------------------------
    sub_statuses = [
        raw_event.get("transfer_validation_status") or "ok",
        raw_event.get("cost_validation_status") or "ok",
        raw_event.get("pre_normalization_status") or "ok",
        pre.get("pre_normalization_status") or "ok",
    ]
    validation_status = (
        "degraded"
        if any(s in ("degraded", "failed", "partial") for s in sub_statuses)
        else "ok"
    )

    # ------------------------------------------------------------------
    # Assemble final normalized event
    # ------------------------------------------------------------------
    return {
        # Identity
        "chain": "solana",
        "signature": signature,
        "slot": raw_event.get("slot"),
        "block_time": raw_event.get("block_time"),

        # Token accounts (owner resolution in Phase 5)
        "token_mint": token_mint or None,
        "source_token_account": source_token_account or None,
        "destination_token_account": destination_token_account or None,
        "source_owner": None,           # Phase 5
        "destination_owner": None,      # Phase 5

        # Instruction position
        "instruction_index": instruction_index,
        "inner_instruction_index": inner_instruction_index,
        "transfer_ordinal": 0,          # Phase 5 / collision defense assigns ordinal
        "program_id": program_id or None,

        # Amounts — all int or Decimal, never float
        "amount_raw": amount_received_raw,
        "amount_decimal": amount_decimal,
        "amount_transferred_raw": amount_transferred_raw,
        "fee_withheld_raw": fee_withheld_raw,
        "amount_received_raw": amount_received_raw,

        # Cost — all int lamports
        "fee_lamports": fee_lamports,
        "native_base_fee_lamports": native_base_fee_lamports,
        "native_priority_fee_lamports": native_priority_fee_lamports,
        "jito_tip_lamports": jito_tip_lamports,
        "explicit_tip_lamports": explicit_tip_lamports,
        "total_native_observed_cost_lamports": total_cost,

        # Transfer truth flags
        "transaction_success": raw_event.get("transaction_success"),
        "transfer_detected": raw_event.get("transfer_detected"),
        "balance_delta_detected": raw_event.get("balance_delta_detected"),
        "observed_transfer_inclusion": raw_event.get("observed_transfer_inclusion"),
        "settlement_evidence_type": raw_event.get("settlement_evidence_type"),

        # Metadata
        "decode_version": DECODE_VERSION,
        "validation_status": validation_status,
        "cost_detection_status": raw_event.get("cost_validation_status") or "ok",
        "tip_detection_status": raw_event.get("jito_tip_detection_status") or "ok",
        "provider": provider,
        "provider_mode": provider_mode,

        # Canonical keys
        "raw_event_id": raw_event_id,
        "normalized_event_id": normalized_event_id,
        "event_fingerprint": event_fingerprint,
        "collision_detected": False,    # Phase 5 / collision defense

        # Resolution statuses
        "alt_resolution_status": raw_event.get("alt_resolution_status") or "not_required",
        "owner_resolution_status": "pending",   # Phase 5
        "amount_resolution_status": (
            "ok" if amount_received_raw is not None else "pending"
        ),

        # Ingestion timestamp
        "ingested_at": ingested_at,
    }


def apply_owner_and_amount_resolution(
    normalized_event: dict[str, Any],
    pre_normalized: dict[str, Any],
    *,
    owner_resolver=None,
    decimals: int = USDC_DECIMALS,
) -> dict[str, Any]:
    """
    Phase 5 patch: resolve owners and fill amount fields in a normalized event.

    Returns the event dict with updated fields (mutated in-place AND returned).
    Call after normalize_event(). Safe to call with owner_resolver=None
    (skips owner resolution, amounts still resolved from balances).

    Parameters
    ----------
    normalized_event:
        Output of normalize_event(). Modified in-place.
    pre_normalized:
        The _pre_normalized dict from the raw_event (contains token balances,
        account_keys_resolved).
    owner_resolver:
        Optional OwnerResolver instance. If None, owner fields stay as-is.
    decimals:
        Token decimal places for amount conversion.
    """
    from services.solana.owner_resolver import resolve_amounts

    # --- Amount resolution ---
    amount_result = resolve_amounts(pre_normalized, decimals=decimals)
    patch = amount_result.to_dict()

    # Only overwrite amount fields if resolution succeeded — don't clobber
    # a good Phase 4 value with a degraded None
    if amount_result.resolution_status == "ok":
        normalized_event["amount_transferred_raw"] = patch["amount_transferred_raw"]
        normalized_event["fee_withheld_raw"] = patch["fee_withheld_raw"]
        # amount_received_raw and amount_decimal already set in Phase 4;
        # update only if Phase 5 found a more authoritative value
        if patch["amount_received_raw"] is not None:
            normalized_event["amount_received_raw"] = patch["amount_received_raw"]
            normalized_event["amount_raw"] = patch["amount_received_raw"]
        if patch["amount_decimal"] is not None:
            normalized_event["amount_decimal"] = patch["amount_decimal"]

    normalized_event["amount_resolution_method"] = patch["amount_resolution_method"]
    normalized_event["amount_resolution_status"] = patch["amount_resolution_status"]

    # --- Owner resolution ---
    if owner_resolver is not None:
        owner_patch = owner_resolver.resolve_event_owners(normalized_event, pre_normalized)
        normalized_event.update(owner_patch)

    # --- Re-aggregate validation status ---
    statuses = [
        normalized_event.get("amount_resolution_status", "ok"),
        normalized_event.get("owner_resolution_status", "ok"),
        normalized_event.get("validation_status", "ok"),
    ]
    if any(s in ("degraded", "failed") for s in statuses):
        normalized_event["validation_status"] = "degraded"

    return normalized_event


def validate_normalized_event(event: dict[str, Any]) -> list[str]:
    """
    Check that all required fields are present in a normalized event.

    Returns a list of missing field names. Empty list = valid.
    Does not check field types — that is BigQuery's job at insert time.
    """
    return [f for f in REQUIRED_FIELDS if f not in event]


def assert_no_float_amounts(event: dict[str, Any]) -> None:
    """
    Raise AssertionError if any amount field contains a float.

    Used in tests and the first-slice gate.
    """
    amount_fields = {
        "amount_raw", "amount_decimal", "amount_transferred_raw",
        "fee_withheld_raw", "amount_received_raw",
        "fee_lamports", "native_base_fee_lamports", "native_priority_fee_lamports",
        "jito_tip_lamports", "explicit_tip_lamports", "total_native_observed_cost_lamports",
    }
    for field in amount_fields:
        val = event.get(field)
        if isinstance(val, float):
            raise AssertionError(
                f"Float found in amount field '{field}': {val!r}. "
                "All Solana amounts must be int or decimal.Decimal."
            )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_instruction_fields(
    instructions: list[dict],
    inner_instructions: list[dict],
) -> dict[str, Any]:
    """
    Find the first SPL token transfer instruction and return its position fields.

    Falls back to the first instruction if no token transfer is found.
    Returns safe defaults (index=0, inner=-1) if no instructions exist.
    """
    from services.solana.constants import TOKEN_PROGRAMS

    all_ix = list(instructions) + list(inner_instructions)

    for ix in all_ix:
        if ix.get("program_id") in TOKEN_PROGRAMS:
            return {
                "instruction_index": ix.get("instruction_index", 0),
                "inner_instruction_index": ix.get("inner_instruction_index", -1),
                "program_id": ix.get("program_id"),
                "data_hash": _hash_data(ix.get("data", "")),
            }

    # No token instruction found — use first instruction or defaults
    if all_ix:
        ix = all_ix[0]
        return {
            "instruction_index": ix.get("instruction_index", 0),
            "inner_instruction_index": ix.get("inner_instruction_index", -1),
            "program_id": ix.get("program_id"),
            "data_hash": _hash_data(ix.get("data", "")),
        }

    return {
        "instruction_index": 0,
        "inner_instruction_index": -1,
        "program_id": None,
        "data_hash": "",
    }


def _safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _to_decimal(raw: Optional[int], decimals: int) -> Optional[Decimal]:
    """Convert a raw integer token amount to Decimal. Returns None on failure."""
    if raw is None:
        return None
    try:
        return Decimal(raw) / Decimal(10 ** decimals)
    except (InvalidOperation, OverflowError, ValueError):
        return None


def _hash_data(data: str) -> str:
    """Short SHA-256 of instruction data — used in fingerprint."""
    if not data:
        return ""
    return hashlib.sha256(data.encode()).hexdigest()[:16]


def _build_normalized_event_id(raw_event_id: str, fingerprint: str) -> str:
    """
    Collision-safe event ID: raw_event_id + first 8 chars of fingerprint.

    If two events share the same raw_event_id but have different fingerprints,
    their normalized_event_ids will differ.
    """
    return f"{raw_event_id}:{fingerprint[:8]}"
