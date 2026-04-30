"""Modeled strategy scoring for Canopy corridor recommendations."""

from __future__ import annotations

from typing import List

from services.integrity_flags import apply_flag_penalties
from services.score_normalization import normalize_unit_score, score_label

CRITICAL_RISK_FLAGS = {"LOW_INTEGRITY", "BRIDGE_SOLVENCY_RISK"}
ELEVATED_RISK_COMBINATIONS = (
    {"LOW_VOLUME_DIVERSITY", "HIGH_SLIPPAGE"},
)


def build_strategy_assessment(
    *,
    cost_score: float,
    liquidity_score: float,
    trust_score: float,
    flags: List[str],
) -> dict:
    normalized_cost = normalize_unit_score(cost_score)
    raw_liquidity = normalize_unit_score(liquidity_score)
    raw_trust = normalize_unit_score(trust_score)
    penalties = apply_flag_penalties(
        liquidity_score=raw_liquidity,
        trust_score=raw_trust,
        flags=flags,
    )
    penalized_liquidity = penalties["liquidity_score"]
    penalized_trust = penalties["trust_score"]
    strategy_score = normalize_unit_score(
        (0.4 * normalized_cost)
        + (0.4 * penalized_liquidity)
        + (0.2 * penalized_trust)
    )
    risk_gate_status = "OPEN"
    risk_gate_cap = None
    if CRITICAL_RISK_FLAGS.intersection(flags):
        risk_gate_status = "CRITICAL_CAP"
        risk_gate_cap = 0.39
    elif any(combo.issubset(set(flags)) for combo in ELEVATED_RISK_COMBINATIONS):
        risk_gate_status = "ELEVATED_CAP"
        risk_gate_cap = 0.52
    if risk_gate_cap is not None:
        strategy_score = min(strategy_score, risk_gate_cap)

    return {
        "cost_score": normalized_cost,
        "liquidity_score": penalized_liquidity,
        "trust_score": penalized_trust,
        "raw_liquidity_score": raw_liquidity,
        "raw_trust_score": raw_trust,
        "strategy_score": strategy_score,
        "strategy_score_label": score_label(strategy_score),
        "evidence_confidence": penalized_trust,
        "evidence_confidence_label": score_label(penalized_trust),
        "liquidity_penalty_factor": penalties["liquidity_penalty_factor"],
        "trust_penalty_factor": penalties["trust_penalty_factor"],
        "risk_gate_status": risk_gate_status,
        "risk_gate_cap": risk_gate_cap,
        "trace": {
            "formula": "0.4 * cost_score + 0.4 * liquidity_score + 0.2 * trust_score",
            "cost_term": round(0.4 * normalized_cost, 4),
            "liquidity_term": round(0.4 * penalized_liquidity, 4),
            "trust_term": round(0.2 * penalized_trust, 4),
        },
        "provenance": {
            "cost_score": "MODELED",
            "liquidity_score": "MODELED",
            "trust_score": "MODELED",
            "strategy_score": "MODELED",
            "evidence_confidence": "MODELED",
            "risk_gate_status": "MODELED",
        },
    }
