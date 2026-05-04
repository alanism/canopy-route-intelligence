"""
Phase 0E — Cost Decomposition.

Avoids the fee_lamports trap and avoids priority-fee double counting.

Key rule: fee_lamports from transaction metadata may already include native
priority fees. Therefore:

    total_native_observed_cost_lamports =
        fee_lamports
        + jito_tip_lamports
        + explicit_tip_lamports

Do NOT add priority_fee_lamports to fee_lamports — that double-counts.
Use decomposition (base + priority ≈ fee_lamports) for reporting only.

Token-denominated fees remain separate until explicitly converted; never mix
SOL lamports and USDC raw units in the same numerator without conversion.

All token amount conversion math uses decimal.Decimal.
"""

from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from services.solana.constants import SYSTEM_PROGRAM, TOKEN_PROGRAMS

logger = logging.getLogger("canopy.solana.cost_decomposition")

# Solana base fee per signature in lamports (as of 2024, 5000 lamports/signature)
_BASE_FEE_PER_SIGNATURE_LAMPORTS = 5000


def decompose_cost(
    pre_normalized: dict[str, Any],
    jito_result: dict[str, Any],
    *,
    token_transfer_fee_raw: int = 0,
    token_transfer_fee_mint: Optional[str] = None,
    sol_price_usd: Optional[Decimal] = None,
) -> dict[str, Any]:
    """
    Produce the full cost decomposition for a pre-normalized transaction.

    Parameters
    ----------
    pre_normalized:
        Output of pre_normalizer.normalize_transaction().
    jito_result:
        Output of jito_detector.detect_jito_tips().
    token_transfer_fee_raw:
        Raw withheld token fee (e.g. Token-2022 transfer fee). Integer u64-compatible.
    token_transfer_fee_mint:
        Mint of the token_transfer_fee_raw, if any.
    sol_price_usd:
        Optional SOL/USD price as Decimal for USD conversion. If None, USD fields
        are None rather than estimated.

    Returns
    -------
    dict with all cost decomposition fields.
    """
    fee_lamports: int = pre_normalized.get("fee_lamports", 0) or 0
    signatures = _count_signatures(pre_normalized)

    # ------------------------------------------------------------------
    # Decompose fee_lamports into base + priority (reporting only)
    # ------------------------------------------------------------------
    native_base_fee_lamports = _BASE_FEE_PER_SIGNATURE_LAMPORTS * signatures
    native_priority_fee_lamports = max(fee_lamports - native_base_fee_lamports, 0)

    # ------------------------------------------------------------------
    # Jito tips from detector
    # ------------------------------------------------------------------
    jito_tip_lamports: int = jito_result.get("jito_tip_lamports", 0) or 0
    explicit_tip_lamports: int = jito_result.get("explicit_tip_lamports", 0) or 0
    tip_detection_status: str = jito_result.get("tip_detection_status", "unavailable")

    # ------------------------------------------------------------------
    # Total observed native cost
    # Primary formula:
    #   total = fee_lamports + jito_tip_lamports + explicit_tip_lamports
    # (fee_lamports already includes base + priority; do NOT add priority again)
    # ------------------------------------------------------------------
    total_native_observed_cost_lamports = (
        fee_lamports + jito_tip_lamports + explicit_tip_lamports
    )

    # ------------------------------------------------------------------
    # Token-denominated fees (Token-2022 withheld fees, etc.)
    # Kept separate — never mix with lamports before conversion.
    # ------------------------------------------------------------------
    token_fee_usd: Optional[Decimal] = None
    if token_transfer_fee_raw > 0 and token_transfer_fee_mint:
        # For USDC (6 decimals) the USD value equals the token amount directly
        # (stablecoin peg assumption). For other mints, leave as None until
        # a price oracle is connected.
        if token_transfer_fee_mint in _STABLECOIN_MINTS:
            try:
                token_fee_usd = Decimal(token_transfer_fee_raw) / Decimal(10 ** 6)
            except (InvalidOperation, ValueError):
                token_fee_usd = None

    # ------------------------------------------------------------------
    # USD conversion for native cost (optional, requires price input)
    # ------------------------------------------------------------------
    total_native_observed_cost_usd: Optional[Decimal] = None
    if sol_price_usd is not None:
        try:
            sol_price = Decimal(str(sol_price_usd))
            lamports_decimal = Decimal(total_native_observed_cost_lamports)
            total_native_observed_cost_usd = (
                lamports_decimal / Decimal("1_000_000_000") * sol_price
            )
        except (InvalidOperation, ValueError):
            total_native_observed_cost_usd = None

    # ------------------------------------------------------------------
    # Cost detection status
    # ------------------------------------------------------------------
    cost_detection_status = _resolve_cost_status(tip_detection_status)

    return {
        "fee_lamports": fee_lamports,
        "native_base_fee_lamports": native_base_fee_lamports,
        "native_priority_fee_lamports": native_priority_fee_lamports,
        "jito_tip_lamports": jito_tip_lamports,
        "explicit_tip_lamports": explicit_tip_lamports,
        "token_transfer_fee_raw": token_transfer_fee_raw,
        "token_transfer_fee_mint": token_transfer_fee_mint,
        "token_transfer_fee_usd": token_fee_usd,
        "total_native_observed_cost_lamports": total_native_observed_cost_lamports,
        "total_native_observed_cost_usd": total_native_observed_cost_usd,
        "cost_detection_status": cost_detection_status,
        # Decomposition check fields (for reporting, not double-counting)
        "_decomposition_check": {
            "base_fee_lamports": native_base_fee_lamports,
            "priority_fee_lamports": native_priority_fee_lamports,
            "sum_check": native_base_fee_lamports + native_priority_fee_lamports,
            "fee_lamports_reported": fee_lamports,
            "note": "sum_check should approximately equal fee_lamports_reported",
        },
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

# Mints treated as stablecoins for token fee USD conversion
_STABLECOIN_MINTS: frozenset[str] = frozenset({
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
})


def _count_signatures(pre_normalized: dict[str, Any]) -> int:
    """Count the number of signatures (used for base fee estimation)."""
    # The raw transaction has one signature per required signer.
    # Without re-parsing the full header, we assume 1 for simplicity.
    # Phase 0B/ALTManager may refine this.
    return 1


def _resolve_cost_status(tip_detection_status: str) -> str:
    """
    Determine overall cost_detection_status based on tip detection outcome.

    If tip detection is unavailable or anomalous, cost may be understated.
    """
    if tip_detection_status == "ok":
        return "ok"
    if tip_detection_status == "unavailable":
        return "partial"
    if tip_detection_status == "anomalous":
        return "partial"
    if tip_detection_status == "degraded":
        return "degraded"
    return "partial"
