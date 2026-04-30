"""Deterministic flow classification for context graph snapshots."""

from __future__ import annotations

from typing import Dict

from services.score_normalization import normalize_unit_score


def classify_signals(signals: Dict[str, float]) -> str:
    counterparty_entropy = float(signals.get("counterparty_entropy", 0.0) or 0.0)
    liquidity_gap = float(signals.get("liquidity_gap", 0.0) or 0.0)
    protocol_noise_ratio = float(signals.get("protocol_noise_ratio", 0.0) or 0.0)
    bridge_usage_rate = float(signals.get("bridge_usage_rate", 0.0) or 0.0)

    if counterparty_entropy > 50 and liquidity_gap < 60:
        return "arbitrage_bot"
    if bridge_usage_rate >= 0.12 and protocol_noise_ratio <= 0.35 and liquidity_gap >= 900:
        return "payment_corridor"
    if protocol_noise_ratio >= 0.45:
        return "defi_activity"
    return "mixed"


def score_confidence(
    *,
    total_transactions: int,
    node_count: int,
    freshness_seconds: float,
    evidence_edge_count: int,
) -> float:
    tx_score = min(total_transactions / 250.0, 1.0)
    node_score = min(node_count / 40.0, 1.0)
    evidence_score = min(evidence_edge_count / 25.0, 1.0)
    freshness_score = max(0.0, 1.0 - (freshness_seconds / 21600.0))
    return normalize_unit_score(
        (0.4 * tx_score)
        + (0.2 * node_score)
        + (0.15 * evidence_score)
        + (0.25 * freshness_score)
    )

