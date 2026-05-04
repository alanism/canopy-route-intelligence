"""
Phase 0G — Token-2022 / Transfer Fee Handling.

Classifies SPL token transfer instructions and extracts amounts safely.
Does not crash on Token-2022 complexity — marks unknown as degraded.

Hackathon v1 ingests USDC (SPL Token) only. PYUSD and USDT are reference
fixtures only and must not appear in dashboard claims unless actually ingested.

All token amount math uses decimal.Decimal. float is prohibited.
"""

from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from services.solana.constants import SPL_TOKEN_PROGRAM, TOKEN_2022_PROGRAM

logger = logging.getLogger("canopy.solana.token_program")

# Transfer type classifications
VANILLA_TRANSFER = "vanilla_transfer"
TRANSFER_CHECKED = "transfer_checked"
TRANSFER_CHECKED_WITH_FEE = "transfer_checked_with_fee"
HOOK_NON_MONETARY = "hook_non_monetary"
HOOK_UNKNOWN = "hook_unknown"

TRANSFER_TYPES = frozenset({
    VANILLA_TRANSFER,
    TRANSFER_CHECKED,
    TRANSFER_CHECKED_WITH_FEE,
    HOOK_NON_MONETARY,
    HOOK_UNKNOWN,
})


def classify_transfer_instruction(
    instruction: dict[str, Any],
) -> str:
    """
    Classify a resolved token program instruction by transfer type.

    Classification is based on the program_id and account count heuristic
    since we self-parse raw data (not provider-parsed jsonParsed format).

    SPL Token (classic):
        transfer        -> 2 accounts (source, dest) + authority; data[0] == 3
        transferChecked -> 4 accounts (source, mint, dest, authority); data[0] == 12

    Token-2022 extensions:
        transferCheckedWithFee -> 4+ accounts; data[0] == 26
        Hook instructions    -> extra inner CPIs with unknown accounts

    When the classification is ambiguous, returns HOOK_UNKNOWN (marks degraded,
    does not crash).
    """
    program_id = instruction.get("program_id", "")
    accounts = instruction.get("accounts", [])
    data = instruction.get("data", "")

    if program_id not in (SPL_TOKEN_PROGRAM, TOKEN_2022_PROGRAM):
        return HOOK_UNKNOWN

    discriminator = _first_data_byte(data)

    if program_id == SPL_TOKEN_PROGRAM:
        if discriminator == 3:
            return VANILLA_TRANSFER
        if discriminator == 12:
            return TRANSFER_CHECKED
        return HOOK_UNKNOWN

    # Token-2022
    if discriminator == 12:
        return TRANSFER_CHECKED
    if discriminator == 26:
        return TRANSFER_CHECKED_WITH_FEE
    if discriminator in _TOKEN_2022_NON_MONETARY_DISCRIMINATORS:
        return HOOK_NON_MONETARY
    return HOOK_UNKNOWN


def extract_transfer_amounts(
    instruction: dict[str, Any],
    transfer_type: str,
    pre_token_balances: list[dict],
    post_token_balances: list[dict],
    account_keys: list[str],
) -> dict[str, Any]:
    """
    Extract transfer amounts for a classified token instruction.

    Resolution hierarchy (per build plan):
    1. Pre/post token balance delta (strongest)
    2. Parsed SPL transfer amount from instruction data (secondary)
    3. Token-2022 transfer fee data from instruction (if available)
    4. None + degraded status

    Returns dict with amount fields using decimal.Decimal — never float.
    """
    accounts = instruction.get("accounts", [])

    # Resolve the destination token account
    if transfer_type == TRANSFER_CHECKED or transfer_type == TRANSFER_CHECKED_WITH_FEE:
        # accounts: [source, mint, dest, authority]
        dest_account = accounts[2] if len(accounts) > 2 else None
        source_account = accounts[0] if accounts else None
    else:
        # accounts: [source, dest, authority]
        dest_account = accounts[1] if len(accounts) > 1 else None
        source_account = accounts[0] if accounts else None

    # ------------------------------------------------------------------
    # Attempt balance delta resolution (preferred)
    # ------------------------------------------------------------------
    delta_result = _resolve_via_balance_delta(
        source_account, dest_account, pre_token_balances, post_token_balances, account_keys
    )

    amount_transferred_raw = delta_result.get("amount_transferred_raw", 0)
    fee_withheld_raw = 0  # Token-2022 withheld fee; extracted below if available
    amount_received_raw = delta_result.get("amount_received_raw", 0)
    amount_resolution_method = delta_result.get("method", "unknown")

    if not delta_result.get("resolved"):
        # Balance delta failed — mark degraded
        logger.debug(
            "Balance delta resolution failed for %s instruction; "
            "transfer amounts set to null",
            transfer_type,
        )
        return _degraded_amount_result(transfer_type)

    # ------------------------------------------------------------------
    # Token-2022 fee extraction (if transferCheckedWithFee)
    # ------------------------------------------------------------------
    if transfer_type == TRANSFER_CHECKED_WITH_FEE:
        fee_withheld_raw = _extract_token_2022_fee(
            amount_transferred_raw, amount_received_raw
        )

    # ------------------------------------------------------------------
    # Decimal conversion — always via Decimal(), never float
    # ------------------------------------------------------------------
    decimals = delta_result.get("decimals", 6)  # Default USDC decimals
    try:
        amount_decimal = Decimal(amount_received_raw) / Decimal(10 ** decimals)
    except (InvalidOperation, ValueError, OverflowError):
        logger.warning("Decimal conversion failed for amount_received_raw=%d", amount_received_raw)
        return _degraded_amount_result(transfer_type)

    token_program_version = (
        "token_2022" if instruction.get("program_id") == TOKEN_2022_PROGRAM
        else "spl_token"
    )

    token_extension_status = (
        "active" if transfer_type in (TRANSFER_CHECKED_WITH_FEE, HOOK_NON_MONETARY, HOOK_UNKNOWN)
        else "none"
    )

    return {
        "amount_transferred_raw": amount_transferred_raw,
        "fee_withheld_raw": fee_withheld_raw,
        "amount_received_raw": amount_received_raw,
        "amount_decimal": amount_decimal,
        "amount_resolution_method": amount_resolution_method,
        "token_program_version": token_program_version,
        "token_extension_status": token_extension_status,
        "transfer_type": transfer_type,
        "validation_status": "ok",
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

# Token-2022 instruction discriminators for non-monetary operations
_TOKEN_2022_NON_MONETARY_DISCRIMINATORS: frozenset[int] = frozenset({
    # Freeze, thaw, approve, revoke, etc. (non-exhaustive; extend as needed)
    4,   # approve
    5,   # revoke
    6,   # setAuthority
    7,   # mintTo
    8,   # burn
    9,   # closeAccount
    10,  # freezeAccount
    11,  # thawAccount
})


def _first_data_byte(data: str) -> Optional[int]:
    """
    Decode the first byte of base58-encoded instruction data.

    For SPL Token instructions, the first byte is the discriminator.
    Returns None if data is empty or decoding fails.
    """
    if not data:
        return None
    try:
        import base58  # type: ignore
        decoded = base58.b58decode(data)
        return decoded[0] if decoded else None
    except Exception:
        # base58 not installed or decode error — fall back to None
        # The caller will classify as HOOK_UNKNOWN, which is safe.
        return None


def _resolve_via_balance_delta(
    source_account: Optional[str],
    dest_account: Optional[str],
    pre_token_balances: list[dict],
    post_token_balances: list[dict],
    account_keys: list[str],
) -> dict[str, Any]:
    """
    Resolve transfer amounts from pre/post token balance delta.
    """
    if not account_keys:
        return {"resolved": False, "method": "no_account_keys"}

    # Build index → pubkey mapping for quick lookup
    key_to_idx = {k: i for i, k in enumerate(account_keys) if k}

    dest_idx = key_to_idx.get(dest_account) if dest_account else None
    source_idx = key_to_idx.get(source_account) if source_account else None

    pre_by_idx = {b.get("accountIndex"): b for b in pre_token_balances if b.get("accountIndex") is not None}
    post_by_idx = {b.get("accountIndex"): b for b in post_token_balances if b.get("accountIndex") is not None}

    amount_transferred_raw = 0
    amount_received_raw = 0
    decimals = 6  # Default USDC; updated from balance entry

    dest_resolved = False
    if dest_idx is not None:
        pre_dest = pre_by_idx.get(dest_idx, {})
        post_dest = post_by_idx.get(dest_idx, {})
        pre_amt = _raw_amount(pre_dest)
        post_amt = _raw_amount(post_dest)
        decimals = _decimals(post_dest) or _decimals(pre_dest) or 6
        if pre_amt is not None and post_amt is not None:
            delta = post_amt - pre_amt
            if delta >= 0:
                amount_received_raw = delta
                dest_resolved = True

    if source_idx is not None:
        pre_src = pre_by_idx.get(source_idx, {})
        post_src = post_by_idx.get(source_idx, {})
        pre_amt = _raw_amount(pre_src)
        post_amt = _raw_amount(post_src)
        if pre_amt is not None and post_amt is not None:
            delta = pre_amt - post_amt
            if delta >= 0:
                amount_transferred_raw = delta

    if not dest_resolved:
        return {"resolved": False, "method": "balance_delta_failed"}

    return {
        "resolved": True,
        "method": "balance_delta",
        "amount_transferred_raw": amount_transferred_raw,
        "amount_received_raw": amount_received_raw,
        "decimals": decimals,
    }


def _raw_amount(balance_entry: dict) -> Optional[int]:
    if not balance_entry:
        return None
    ui = balance_entry.get("uiTokenAmount", {}) or {}
    amount_str = ui.get("amount")
    if amount_str is None:
        return None
    try:
        return int(amount_str)
    except (ValueError, TypeError):
        return None


def _decimals(balance_entry: dict) -> Optional[int]:
    if not balance_entry:
        return None
    ui = balance_entry.get("uiTokenAmount", {}) or {}
    d = ui.get("decimals")
    if d is None:
        return None
    try:
        return int(d)
    except (ValueError, TypeError):
        return None


def _extract_token_2022_fee(amount_transferred_raw: int, amount_received_raw: int) -> int:
    """Infer withheld fee as the difference between sent and received amounts."""
    delta = amount_transferred_raw - amount_received_raw
    return max(delta, 0)


def _degraded_amount_result(transfer_type: str) -> dict[str, Any]:
    return {
        "amount_transferred_raw": None,
        "fee_withheld_raw": None,
        "amount_received_raw": None,
        "amount_decimal": None,
        "amount_resolution_method": "failed",
        "token_program_version": "unknown",
        "token_extension_status": "unknown",
        "transfer_type": transfer_type,
        "validation_status": "degraded",
    }
