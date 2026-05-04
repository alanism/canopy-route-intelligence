"""
Phase 0A — Transaction Pre-Normalizer.

Converts a raw Solana RPC getTransaction response (encoding="json") into a
resolved transaction object that downstream parsers can safely consume.

The business parser must never operate on unresolved numeric account indexes.

ALT integration point
---------------------
For v0 transactions with addressTableLookups, this module accepts an optional
`resolved_loaded_addresses` dict provided by ALTManager (Phase 0B, built tomorrow).
If not provided, alt_resolution_status is set to "pending_alt_manager" and
canonical metrics must not be promoted as healthy.
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any, Optional

logger = logging.getLogger("canopy.solana.pre_normalizer")

# Sentinel for top-level instructions — never use 0 for this.
TOP_LEVEL_INNER_INDEX = -1


def _sha256_hex(data: str) -> str:
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def _resolve_accounts(
    account_indexes: list[int],
    account_keys: list[str],
    instruction_index: int,
    context: str,
) -> tuple[list[str], bool]:
    """
    Resolve a list of account indexes to public key strings.

    Returns (resolved_list, all_resolved).
    """
    resolved: list[str] = []
    all_resolved = True
    for pos, idx in enumerate(account_indexes):
        if idx < len(account_keys):
            key = account_keys[idx]
            if key is None or key == "":
                logger.warning(
                    "Null account key at index %d in %s instruction %d position %d",
                    idx, context, instruction_index, pos,
                )
                resolved.append("")
                all_resolved = False
            else:
                resolved.append(key)
        else:
            logger.warning(
                "Account index %d out of range (len=%d) in %s instruction %d position %d",
                idx, len(account_keys), context, instruction_index, pos,
            )
            resolved.append("")
            all_resolved = False
    return resolved, all_resolved


def normalize_transaction(
    raw_tx: dict[str, Any],
    *,
    resolved_loaded_addresses: Optional[dict[str, list[str]]] = None,
) -> dict[str, Any]:
    """
    Normalize a raw getTransaction RPC response.

    Parameters
    ----------
    raw_tx:
        Raw dict from the Solana RPC getTransaction call (encoding="json").
        Top-level keys: slot, blockTime, meta, transaction, version.
    resolved_loaded_addresses:
        Provided by ALTManager (Phase 0B) for v0 transactions. Must be a dict
        with keys "writable" and "readonly", each a list of base58 public key
        strings in index order. If None and the transaction is v0 with ALTs,
        alt_resolution_status will be "pending_alt_manager".

    Returns
    -------
    dict with the pre-normalized transaction. Consumers must check
    pre_normalization_status and alt_resolution_status before trusting
    resolved account keys.
    """
    if not raw_tx:
        return _failed_result("empty_input", None, None)

    slot = raw_tx.get("slot")
    block_time = raw_tx.get("blockTime")

    transaction = raw_tx.get("transaction", {})
    meta = raw_tx.get("meta", {})
    version = raw_tx.get("version", "legacy")

    signatures = transaction.get("signatures", [])
    signature = signatures[0] if signatures else None

    if not signature:
        logger.error("Transaction missing signature at slot %s", slot)
        return _failed_result("missing_signature", slot, block_time)

    message = transaction.get("message", {})

    # ------------------------------------------------------------------
    # Determine transaction version and static account keys
    # ------------------------------------------------------------------
    is_v0 = version == 0 or version == "0"
    transaction_version = "0" if is_v0 else "legacy"

    # Static account keys — always present for both versions
    static_keys: list[str] = message.get("accountKeys", [])

    # ------------------------------------------------------------------
    # Resolve loaded addresses (v0 only)
    # ------------------------------------------------------------------
    alt_resolution_status: str
    loaded_addresses_resolved: bool
    address_table_lookups: list[dict] = message.get("addressTableLookups", [])
    has_alt = bool(address_table_lookups)

    if not is_v0 or not has_alt:
        # Legacy transaction or v0 with no ALT references
        loaded_writable: list[str] = []
        loaded_readonly: list[str] = []
        alt_resolution_status = "not_required"
        loaded_addresses_resolved = True
    elif resolved_loaded_addresses is not None:
        # ALTManager has provided self-resolved addresses
        loaded_writable = resolved_loaded_addresses.get("writable", [])
        loaded_readonly = resolved_loaded_addresses.get("readonly", [])
        alt_resolution_status = "ok"
        loaded_addresses_resolved = True
    else:
        # v0 with ALTs but ALTManager not yet available (Phase 0B pending)
        # Fall back to provider-supplied loadedAddresses for structural
        # completeness only; these are NOT canonical until ALTManager verifies.
        provider_loaded = meta.get("loadedAddresses", {}) or {}
        loaded_writable = provider_loaded.get("writable", [])
        loaded_readonly = provider_loaded.get("readonly", [])
        alt_resolution_status = "pending_alt_manager"
        loaded_addresses_resolved = False
        logger.warning(
            "v0 transaction %s has ALTs but ALTManager not provided; "
            "using provider-loaded addresses as structural scaffold only. "
            "Do not promote canonical metrics until ALTManager resolves.",
            signature,
        )

    # Full account key list in Solana-specified order:
    # [static keys] + [loaded writable] + [loaded readonly]
    account_keys: list[str] = static_keys + loaded_writable + loaded_readonly

    if not account_keys:
        logger.error("Transaction %s has no resolvable account keys", signature)
        return _failed_result("no_account_keys", slot, block_time, signature=signature)

    # Check for null keys
    null_key_count = sum(1 for k in account_keys if not k)
    if null_key_count > 0:
        logger.warning(
            "Transaction %s has %d null account keys", signature, null_key_count
        )

    # ------------------------------------------------------------------
    # Resolve top-level instructions
    # ------------------------------------------------------------------
    raw_instructions: list[dict] = message.get("instructions", [])
    instructions_resolved: list[dict] = []
    all_instructions_resolved = True

    for ix_idx, raw_ix in enumerate(raw_instructions):
        program_id_index = raw_ix.get("programIdIndex", -1)
        if program_id_index < 0 or program_id_index >= len(account_keys):
            logger.warning(
                "Transaction %s instruction %d has invalid programIdIndex %d",
                signature, ix_idx, program_id_index,
            )
            program_id = ""
            all_instructions_resolved = False
        else:
            program_id = account_keys[program_id_index]

        account_index_list: list[int] = raw_ix.get("accounts", [])
        resolved_accounts, ok = _resolve_accounts(
            account_index_list, account_keys, ix_idx, f"tx={signature}"
        )
        if not ok:
            all_instructions_resolved = False

        data_raw = raw_ix.get("data", "")
        data_hash = _sha256_hex(data_raw) if data_raw else ""

        instructions_resolved.append({
            "instruction_index": ix_idx,
            "inner_instruction_index": TOP_LEVEL_INNER_INDEX,
            "program_id": program_id,
            "accounts": resolved_accounts,
            "data": data_raw,
            "data_hash": data_hash,
        })

    # ------------------------------------------------------------------
    # Resolve inner instructions
    # ------------------------------------------------------------------
    raw_inner_groups: list[dict] = meta.get("innerInstructions", []) or []
    inner_instructions_resolved: list[dict] = []
    all_inner_resolved = True

    for group in raw_inner_groups:
        outer_index = group.get("index", -1)
        inner_list: list[dict] = group.get("instructions", [])

        for inner_idx, raw_inner in enumerate(inner_list):
            program_id_index = raw_inner.get("programIdIndex", -1)
            if program_id_index < 0 or program_id_index >= len(account_keys):
                logger.warning(
                    "Transaction %s inner instruction outer=%d inner=%d "
                    "has invalid programIdIndex %d",
                    signature, outer_index, inner_idx, program_id_index,
                )
                program_id = ""
                all_inner_resolved = False
            else:
                program_id = account_keys[program_id_index]

            account_index_list = raw_inner.get("accounts", [])
            resolved_accounts, ok = _resolve_accounts(
                account_index_list,
                account_keys,
                outer_index,
                f"tx={signature} inner={inner_idx}",
            )
            if not ok:
                all_inner_resolved = False

            data_raw = raw_inner.get("data", "")
            data_hash = _sha256_hex(data_raw) if data_raw else ""

            inner_instructions_resolved.append({
                "instruction_index": outer_index,
                "inner_instruction_index": inner_idx,
                "program_id": program_id,
                "accounts": resolved_accounts,
                "data": data_raw,
                "data_hash": data_hash,
            })

    # ------------------------------------------------------------------
    # Overall pre-normalization status
    # ------------------------------------------------------------------
    resolution_ok = (
        all_instructions_resolved
        and all_inner_resolved
        and null_key_count == 0
    )

    if alt_resolution_status == "pending_alt_manager":
        pre_normalization_status = "partial"
    elif resolution_ok:
        pre_normalization_status = "ok"
    else:
        pre_normalization_status = "partial"

    # ------------------------------------------------------------------
    # Preserve metadata needed by downstream parsers
    # ------------------------------------------------------------------
    transaction_success = meta.get("err") is None
    fee_lamports = meta.get("fee", 0) or 0
    pre_token_balances = meta.get("preTokenBalances", []) or []
    post_token_balances = meta.get("postTokenBalances", []) or []
    pre_balances = meta.get("preBalances", []) or []
    post_balances = meta.get("postBalances", []) or []
    log_messages = meta.get("logMessages", []) or []

    return {
        "signature": signature,
        "slot": slot,
        "block_time": block_time,
        "transaction_version": transaction_version,
        "account_keys_resolved": account_keys,
        "instructions_resolved": instructions_resolved,
        "inner_instructions_resolved": inner_instructions_resolved,
        "loaded_addresses_resolved": loaded_addresses_resolved,
        "alt_resolution_status": alt_resolution_status,
        "pre_normalization_status": pre_normalization_status,
        # Metadata for downstream parsers
        "transaction_success": transaction_success,
        "fee_lamports": fee_lamports,
        "pre_token_balances": pre_token_balances,
        "post_token_balances": post_token_balances,
        "pre_balances": pre_balances,
        "post_balances": post_balances,
        "log_messages": log_messages,
        "address_table_lookups": address_table_lookups,
        "null_account_key_count": null_key_count,
    }


def _failed_result(
    reason: str,
    slot: Any,
    block_time: Any,
    *,
    signature: Optional[str] = None,
) -> dict[str, Any]:
    logger.error("Pre-normalization failed: %s (slot=%s sig=%s)", reason, slot, signature)
    return {
        "signature": signature,
        "slot": slot,
        "block_time": block_time,
        "transaction_version": "unknown",
        "account_keys_resolved": [],
        "instructions_resolved": [],
        "inner_instructions_resolved": [],
        "loaded_addresses_resolved": False,
        "alt_resolution_status": "failed",
        "pre_normalization_status": "failed",
        "transaction_success": False,
        "fee_lamports": 0,
        "pre_token_balances": [],
        "post_token_balances": [],
        "pre_balances": [],
        "post_balances": [],
        "log_messages": [],
        "address_table_lookups": [],
        "null_account_key_count": 0,
        "_failure_reason": reason,
    }
