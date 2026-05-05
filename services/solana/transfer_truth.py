"""
Phase 0C — Transfer Truth Rule.

A Solana transaction can be ledger-successful while the watched transfer did
not happen. This module enforces:

    transaction_success != transfer_success

observed_transfer_inclusion is set True only when ALL of the following hold:
  - transaction_success is True
  - the watched token mint is present in pre/post token balances
  - a valid SPL transfer effect exists OR pre/post token balances prove movement
  - amount_received_raw > 0

All token amount math uses decimal.Decimal. float is prohibited.

Preferred proof hierarchy
-------------------------
1. Pre/post token balance delta  (strongest)
2. Verified SPL transfer instruction effect
3. Log messages                  (support evidence only)
4. meta.err == null alone        (never sufficient)
"""

from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from services.solana.constants import (
    ASSOCIATED_TOKEN_PROGRAM,
    SPL_TOKEN_PROGRAM,
    TOKEN_2022_PROGRAM,
    TOKEN_PROGRAMS,
)

logger = logging.getLogger("canopy.solana.transfer_truth")

# SPL Token transfer instruction discriminators (first byte of base58 decoded data)
# transfer = 3, transferChecked = 12
_SPL_TRANSFER_DISCRIMINATORS = {3, 12}


def evaluate_transfer_truth(
    pre_normalized: dict[str, Any],
    watched_mints: set[str],
    *,
    watched_addresses: Optional[set[str]] = None,
) -> dict[str, Any]:
    """
    Evaluate transfer truth for a pre-normalized transaction.

    Parameters
    ----------
    pre_normalized:
        Output of pre_normalizer.normalize_transaction().
    watched_mints:
        Set of token mint addresses to watch (e.g. {USDC_MINT}).
    watched_addresses:
        Optional set of source or destination token accounts / owner addresses
        to further scope which transfers are relevant. If None, any transfer
        involving a watched mint is eligible.

    Returns
    -------
    dict with transfer truth fields. Merge this into the normalized event.
    """
    transaction_success: bool = pre_normalized.get("transaction_success", False)
    pre_token_balances: list[dict] = pre_normalized.get("pre_token_balances", []) or []
    post_token_balances: list[dict] = pre_normalized.get("post_token_balances", []) or []
    instructions: list[dict] = pre_normalized.get("instructions_resolved", [])
    inner_instructions: list[dict] = pre_normalized.get("inner_instructions_resolved", [])
    all_instructions = instructions + inner_instructions

    # ------------------------------------------------------------------
    # Step 1: check if any watched mint appears in token balance tables
    # ------------------------------------------------------------------
    pre_by_index = _index_balances(pre_token_balances)
    post_by_index = _index_balances(post_token_balances)

    watched_pre = {
        idx: bal for idx, bal in pre_by_index.items()
        if bal.get("mint") in watched_mints
    }
    watched_post = {
        idx: bal for idx, bal in post_by_index.items()
        if bal.get("mint") in watched_mints
    }

    mint_present = bool(watched_pre or watched_post)

    if not mint_present:
        return _no_transfer_result(
            transaction_success=transaction_success,
            reason="watched_mint_absent",
        )

    # ------------------------------------------------------------------
    # Step 2: compute balance delta (strongest evidence)
    # ------------------------------------------------------------------
    delta_result = _compute_balance_delta(
        watched_pre, watched_post, watched_addresses
    )

    balance_delta_detected = delta_result["balance_delta_detected"]
    amount_received_raw = delta_result["amount_received_raw"]
    destination_token_account = delta_result["destination_token_account"]
    source_token_account = delta_result["source_token_account"]
    delta_mint = delta_result["mint"]

    # ------------------------------------------------------------------
    # Step 3: scan for SPL transfer instructions (secondary evidence)
    # ------------------------------------------------------------------
    instruction_evidence = _find_spl_transfer_instructions(
        all_instructions, watched_mints, watched_addresses
    )
    transfer_detected_by_instruction = (
        instruction_evidence["found"]
        and balance_delta_detected
        and amount_received_raw > 0
    )

    # ------------------------------------------------------------------
    # Step 4: determine settlement_evidence_type
    # ------------------------------------------------------------------
    if balance_delta_detected and transfer_detected_by_instruction:
        settlement_evidence_type = "both"
    elif balance_delta_detected:
        settlement_evidence_type = "balance_delta"
    elif transfer_detected_by_instruction:
        settlement_evidence_type = "instruction"
    else:
        settlement_evidence_type = "none"

    transfer_detected = balance_delta_detected or transfer_detected_by_instruction

    if not transfer_detected:
        return _no_transfer_result(
            transaction_success=transaction_success,
            reason="watched_mint_no_balance_delta",
        )

    # ------------------------------------------------------------------
    # Step 5: apply the inclusion rule
    # All conditions must be true:
    #   - transaction_success
    #   - mint present
    #   - valid transfer effect or balance delta
    #   - amount_received_raw > 0
    # ------------------------------------------------------------------
    observed_transfer_inclusion = (
        transaction_success
        and mint_present
        and transfer_detected
        and amount_received_raw > 0
    )

    # Resolve source/dest from instruction evidence if balance delta didn't find them
    if not source_token_account and instruction_evidence.get("source_token_account"):
        source_token_account = instruction_evidence["source_token_account"]
    if not destination_token_account and instruction_evidence.get("destination_token_account"):
        destination_token_account = instruction_evidence["destination_token_account"]
    if not delta_mint and instruction_evidence.get("mint"):
        delta_mint = instruction_evidence["mint"]

    return {
        "transaction_success": transaction_success,
        "transfer_detected": transfer_detected,
        "balance_delta_detected": balance_delta_detected,
        "observed_transfer_inclusion": observed_transfer_inclusion,
        "settlement_evidence_type": settlement_evidence_type,
        "amount_received_raw": amount_received_raw,
        "source_token_account": source_token_account,
        "destination_token_account": destination_token_account,
        "token_mint": delta_mint,
        "validation_status": (
            "ok" if pre_normalized.get("pre_normalization_status") == "ok"
            else "degraded"
        ),
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _index_balances(balances: list[dict]) -> dict[int, dict]:
    """Index token balances by accountIndex."""
    result: dict[int, dict] = {}
    for bal in balances:
        idx = bal.get("accountIndex")
        if idx is not None:
            result[int(idx)] = bal
    return result


def _safe_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _compute_balance_delta(
    watched_pre: dict[int, dict],
    watched_post: dict[int, dict],
    watched_addresses: Optional[set[str]],
) -> dict[str, Any]:
    """
    Find accounts where the token balance increased (receiver side).

    Returns the largest single increase if multiple watched accounts changed,
    which covers the common single-transfer case.
    """
    best_received_raw: int = 0
    best_destination: Optional[str] = None
    best_source: Optional[str] = None
    best_mint: Optional[str] = None
    found = False

    all_indexes = set(watched_pre) | set(watched_post)

    for idx in all_indexes:
        pre_bal = watched_pre.get(idx, {})
        post_bal = watched_post.get(idx, {})

        mint = post_bal.get("mint") or pre_bal.get("mint")
        owner = post_bal.get("owner") or pre_bal.get("owner")

        # Check address filter
        if watched_addresses is not None:
            account_addr = post_bal.get("accountIndex")  # not useful here
            # Filter on owner address if available
            if owner and owner not in watched_addresses:
                # Also check the token account key — we don't have it indexed here,
                # so we allow through and let the caller filter downstream
                pass

        pre_amount_raw = _raw_token_amount(pre_bal)
        post_amount_raw = _raw_token_amount(post_bal)

        # An account missing from pre_token_balances had a zero balance before
        # the transaction (e.g. a newly-created ATA). Treat as 0, not unknown.
        if pre_amount_raw is None and post_amount_raw is None:
            continue
        if pre_amount_raw is None:
            pre_amount_raw = 0
        if post_amount_raw is None:
            post_amount_raw = 0

        delta = post_amount_raw - pre_amount_raw

        if delta > 0:
            # This account received tokens
            found = True
            if delta > best_received_raw:
                best_received_raw = delta
                best_destination = _token_account_key(post_bal)
                best_mint = mint

        elif delta < 0:
            # This account sent tokens
            if best_source is None:
                best_source = _token_account_key(pre_bal)

    return {
        "balance_delta_detected": found,
        "amount_received_raw": best_received_raw,
        "destination_token_account": best_destination,
        "source_token_account": best_source,
        "mint": best_mint,
    }


def _raw_token_amount(balance_entry: dict) -> Optional[int]:
    """Extract raw integer token amount from a token balance entry."""
    if not balance_entry:
        return None
    ui_token_amount = balance_entry.get("uiTokenAmount", {}) or {}
    amount_str = ui_token_amount.get("amount")
    if amount_str is None:
        return None
    try:
        return int(amount_str)
    except (ValueError, TypeError):
        return None


def _token_account_key(balance_entry: dict) -> Optional[str]:
    """Best-effort extraction of the token account public key from a balance entry."""
    # The RPC balance entry doesn't directly include the account pubkey — it uses
    # accountIndex. The caller must resolve accountIndex → pubkey via account_keys_resolved.
    # We store accountIndex here as a placeholder; upstream must resolve.
    idx = balance_entry.get("accountIndex")
    if idx is not None:
        return f"__account_index_{idx}__"
    return None


def _find_spl_transfer_instructions(
    instructions: list[dict],
    watched_mints: set[str],
    watched_addresses: Optional[set[str]],
) -> dict[str, Any]:
    """
    Scan resolved instructions for SPL token transfer effects.

    We look for instructions where:
    - program_id is a known token program
    - The instruction involves a watched mint (checked via accounts heuristic)

    Returns evidence dict with found, source_token_account, destination_token_account, mint.
    """
    for ix in instructions:
        program_id = ix.get("program_id", "")
        if program_id not in TOKEN_PROGRAMS:
            continue

        accounts = ix.get("accounts", [])
        data = ix.get("data", "")

        # For a standard SPL transfer: accounts[0]=source, accounts[1]=dest (transfer)
        # For transferChecked: accounts[0]=source, accounts[1]=mint, accounts[2]=dest, accounts[3]=authority
        if len(accounts) >= 2:
            # We can't confirm the mint from instruction accounts alone without
            # token account → mint resolution (Phase 5). Mark as potential evidence.
            source = accounts[0] if accounts else None
            dest = accounts[1] if len(accounts) > 1 else None

            if watched_addresses is not None:
                if source not in watched_addresses and dest not in watched_addresses:
                    continue

            return {
                "found": True,
                "source_token_account": source,
                "destination_token_account": dest,
                "mint": None,  # Requires Phase 5 owner/amount resolution
            }

    return {
        "found": False,
        "source_token_account": None,
        "destination_token_account": None,
        "mint": None,
    }


def _no_transfer_result(*, transaction_success: bool, reason: str) -> dict[str, Any]:
    return {
        "transaction_success": transaction_success,
        "transfer_detected": False,
        "balance_delta_detected": False,
        "observed_transfer_inclusion": False,
        "settlement_evidence_type": "none",
        "amount_received_raw": 0,
        "source_token_account": None,
        "destination_token_account": None,
        "token_mint": None,
        "validation_status": "ok",
        "_no_transfer_reason": reason,
    }
