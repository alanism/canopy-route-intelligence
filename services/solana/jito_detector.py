"""
Phase 0D — Jito Tip Detector.

Detects explicit SOL transfers to Jito tip accounts so that Observed Route Cost
does not undercount execution cost.

fee_lamports captures native ledger fees. Jito tips are separate SOL transfers
to designated tip accounts sent via the System Program. If missed, high-performance
routes appear artificially cheap.

Scans both top-level and inner System Program SOL transfers.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from services.solana.constants import JITO_TIP_ACCOUNTS, SYSTEM_PROGRAM, get_jito_tip_accounts

logger = logging.getLogger("canopy.solana.jito_detector")

# System Program transfer instruction: first byte of decoded data == 2
# In base58: "3Bxs..." patterns appear for SOL transfers, but we identify
# by program_id == SYSTEM_PROGRAM and 2 accounts (from, to).
_SYSTEM_TRANSFER_ACCOUNT_COUNT = 2


def detect_jito_tips(
    pre_normalized: dict[str, Any],
    *,
    tip_accounts: Optional[frozenset[str]] = None,
) -> dict[str, Any]:
    """
    Scan a pre-normalized transaction for SOL transfers to Jito tip accounts.

    Parameters
    ----------
    pre_normalized:
        Output of pre_normalizer.normalize_transaction().
    tip_accounts:
        Override the default tip account set. If None, uses constants.JITO_TIP_ACCOUNTS.
        Pass an empty frozenset to test the "unavailable" status path.

    Returns
    -------
    dict with Jito tip detection fields.
    """
    active_tip_accounts = tip_accounts if tip_accounts is not None else get_jito_tip_accounts()

    if not active_tip_accounts:
        logger.warning("Jito tip account list is empty; tip detection unavailable")
        return _unavailable_result()

    instructions: list[dict] = pre_normalized.get("instructions_resolved", [])
    inner_instructions: list[dict] = pre_normalized.get("inner_instructions_resolved", [])
    pre_balances: list[int] = pre_normalized.get("pre_balances", []) or []
    post_balances: list[int] = pre_normalized.get("post_balances", []) or []
    account_keys: list[str] = pre_normalized.get("account_keys_resolved", [])

    all_instructions = instructions + inner_instructions

    jito_tip_lamports: int = 0
    tip_account_match_count: int = 0
    tip_detection_evidence: list[dict] = []
    anomalous = False

    for ix in all_instructions:
        program_id = ix.get("program_id", "")
        if program_id != SYSTEM_PROGRAM:
            continue

        accounts = ix.get("accounts", [])

        # System Program SOL transfer requires exactly 2 accounts: [from, to]
        if len(accounts) < 2:
            continue

        destination = accounts[1]
        if not destination:
            continue

        if destination not in active_tip_accounts:
            continue

        # Matched a Jito tip account — measure the lamports transferred
        lamports = _measure_sol_transfer_lamports(
            accounts, account_keys, pre_balances, post_balances
        )

        if lamports is None:
            logger.warning(
                "Jito tip transfer to %s detected but lamports could not be measured "
                "(instruction_index=%d inner=%d)",
                destination,
                ix.get("instruction_index", -1),
                ix.get("inner_instruction_index", -1),
            )
            anomalous = True
            lamports = 0

        jito_tip_lamports += lamports
        tip_account_match_count += 1
        tip_detection_evidence.append({
            "instruction_index": ix.get("instruction_index"),
            "inner_instruction_index": ix.get("inner_instruction_index"),
            "destination": destination,
            "lamports": lamports,
        })

    if anomalous and jito_tip_lamports == 0:
        tip_detection_status = "anomalous"
    elif anomalous:
        tip_detection_status = "anomalous"
    elif tip_account_match_count > 0:
        tip_detection_status = "ok"
    else:
        tip_detection_status = "ok"  # no tips found is a valid ok state

    return {
        "jito_tip_lamports": jito_tip_lamports,
        "explicit_tip_lamports": 0,  # Non-Jito explicit tips — Phase 0E computes this
        "tip_detection_status": tip_detection_status,
        "tip_account_match_count": tip_account_match_count,
        "tip_detection_evidence": tip_detection_evidence,
    }


def _measure_sol_transfer_lamports(
    accounts: list[str],
    account_keys: list[str],
    pre_balances: list[int],
    post_balances: list[int],
) -> Optional[int]:
    """
    Measure lamports transferred by checking the balance delta on the destination account.

    Falls back to None if balances are unavailable or unresolvable.
    """
    if len(accounts) < 2:
        return None

    destination = accounts[1]
    if not destination or not account_keys:
        return None

    try:
        dest_idx = account_keys.index(destination)
    except ValueError:
        return None

    if dest_idx >= len(pre_balances) or dest_idx >= len(post_balances):
        return None

    pre = pre_balances[dest_idx]
    post = post_balances[dest_idx]

    if pre is None or post is None:
        return None

    delta = post - pre
    if delta < 0:
        # Destination balance decreased — not a tip receipt, skip
        return None

    return delta


def _unavailable_result() -> dict[str, Any]:
    return {
        "jito_tip_lamports": 0,
        "explicit_tip_lamports": 0,
        "tip_detection_status": "unavailable",
        "tip_account_match_count": 0,
        "tip_detection_evidence": [],
    }
