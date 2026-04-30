"""Liquidity and trust scoring helpers for Canopy v4."""

from __future__ import annotations


def clamp_score(value: float) -> float:
    return max(0.0, min(round(value, 4), 1.0))


def calculate_liquidity_score(depth: float, solvency: float, integrity: float) -> float:
    return clamp_score((0.5 * depth) + (0.3 * solvency) + (0.2 * integrity))


def depth_score_from_usd(depth_usd: float, transfer_amount_usdc: float) -> float:
    target_depth = max(float(transfer_amount_usdc) * 25, 250_000)
    return clamp_score(depth_usd / target_depth)


def calculate_trust_score(
    *,
    integrity: float,
    solvency: float,
    concentration_risk: float,
    freshness: float,
) -> float:
    inverse_concentration = 1 - clamp_score(concentration_risk)
    return clamp_score(
        (integrity * 0.35)
        + (solvency * 0.3)
        + (inverse_concentration * 0.2)
        + (freshness * 0.15)
    )


def cost_score_from_fee(estimated_fee_usd: float, fee_floor: float, fee_ceiling: float) -> float:
    spread = max(fee_ceiling - fee_floor, 0.0001)
    return clamp_score(1 - ((estimated_fee_usd - fee_floor) / spread))
