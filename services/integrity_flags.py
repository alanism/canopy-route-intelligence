"""Integrity flag helpers for the dual-layer decision model."""

from __future__ import annotations

from typing import Dict, List

from services.score_normalization import normalize_unit_score

LOW_INTEGRITY_THRESHOLD = 0.32
LOW_VOLUME_DIVERSITY_THRESHOLD = 0.28
BRIDGE_SOLVENCY_THRESHOLD = 0.999


def build_integrity_flags(
    *,
    mode: str,
    integrity_score: float,
    volume_diversity_score: float,
    solvency_ratio: float,
    estimated_fee_usd: float,
    fee_floor: float,
    fee_ceiling: float,
) -> List[str]:
    if mode != "live_measured":
        return []

    flags: List[str] = []
    if normalize_unit_score(integrity_score) < LOW_INTEGRITY_THRESHOLD:
        flags.append("LOW_INTEGRITY")
    if normalize_unit_score(volume_diversity_score) < LOW_VOLUME_DIVERSITY_THRESHOLD:
        flags.append("LOW_VOLUME_DIVERSITY")
    if float(solvency_ratio) < BRIDGE_SOLVENCY_THRESHOLD:
        flags.append("BRIDGE_SOLVENCY_RISK")

    spread = max(float(fee_ceiling) - float(fee_floor), 0.0001)
    slippage_score = (float(estimated_fee_usd) - float(fee_floor)) / spread
    if slippage_score >= 0.9:
        flags.append("HIGH_SLIPPAGE")
    return flags


def apply_flag_penalties(
    *,
    liquidity_score: float,
    trust_score: float,
    flags: List[str],
) -> Dict[str, float]:
    liquidity_penalty = 1.0
    trust_penalty = 1.0

    if "LOW_INTEGRITY" in flags:
        liquidity_penalty *= 0.25
        trust_penalty *= 0.70
    if "LOW_VOLUME_DIVERSITY" in flags:
        liquidity_penalty *= 0.65
        trust_penalty *= 0.90
    if "BRIDGE_SOLVENCY_RISK" in flags:
        liquidity_penalty *= 0.85
        trust_penalty *= 0.72
    if "HIGH_SLIPPAGE" in flags:
        trust_penalty *= 0.92

    penalized_liquidity = normalize_unit_score(liquidity_score * liquidity_penalty)
    penalized_trust = normalize_unit_score(trust_score * trust_penalty)
    return {
        "liquidity_score": penalized_liquidity,
        "trust_score": penalized_trust,
        "liquidity_penalty_factor": round(liquidity_penalty, 4),
        "trust_penalty_factor": round(trust_penalty, 4),
    }
