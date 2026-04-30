"""Liquidity shock and slippage models for Canopy corridor forecasting."""

from __future__ import annotations

import math
from typing import List

import pandas as pd

from forecasting.features import clamp_probability


def _mean(values: List[float]) -> float:
    return sum(values) / max(len(values), 1)


def calculate_liquidity_metrics(feature_bundle: dict, volatility_result: dict) -> dict:
    frame: pd.DataFrame = feature_bundle["feature_frame"]
    current_state = feature_bundle["current_state"]

    abs_returns = [abs(float(value)) for value in frame["volume_log_return"].tolist()]
    turnover = [
        float(volume) / max(float(depth), 1.0)
        for volume, depth in zip(frame["volume_usd"].tolist(), frame["dex_liquidity_usd"].tolist())
    ]
    amihud_series = [
        abs_return / max(turnover_value, 1e-9)
        for abs_return, turnover_value in zip(abs_returns, turnover)
    ]
    amihud_baseline = _mean(amihud_series[-30:] or amihud_series)
    amihud_current = amihud_series[-1] if amihud_series else 0.0
    amihud_shock_index = amihud_current / max(amihud_baseline, 1e-9)

    turnover_adjusted_amihud = amihud_current / max(turnover[-1] if turnover else 1.0, 1e-9)

    price_impact_samples = [
        abs_return / max(float(volume), 1.0)
        for abs_return, volume in zip(abs_returns[-20:], frame["volume_usd"].tail(20).tolist())
    ]
    hasbrouck_price_impact = _mean(price_impact_samples) * 1_000_000

    order_size = float(current_state["transfer_amount_usdc"])
    adv = max(_mean(frame["volume_usd"].tail(7).tolist()), 1.0)
    corridor_volatility = float(volatility_result["predicted_corridor_volatility"])
    temporary_impact = corridor_volatility * math.sqrt(order_size / adv)
    permanent_impact = (hasbrouck_price_impact / 1_000_000) * (order_size / adv)

    depth = max(_mean(frame["dex_liquidity_usd"].tail(7).tolist()), 1.0)
    spread_widening = max(corridor_volatility * 4.5, 0.0)
    lvar = min(
        1.0,
        (spread_widening * 0.4)
        + min(order_size / depth, 1.0) * 0.35
        + min(amihud_shock_index / 4.0, 1.0) * 0.25,
    )

    solvency_risk = clamp_probability(1.0 - min(float(current_state["solvency_ratio"]), 1.0))
    liquidity_shock_risk = clamp_probability(
        (min(amihud_shock_index / 3.0, 1.0) * 0.45)
        + (min(lvar, 1.0) * 0.3)
        + (solvency_risk * 0.15)
        + (max(0.0, 0.5 - float(current_state["integrity_score"])) * 0.2)
    )
    slippage_probability = clamp_probability(
        min((temporary_impact * 8.0) + (permanent_impact * 12.0) + (lvar * 0.45), 1.0)
    )

    return {
        "amihud_ratio": round(amihud_current, 6),
        "amihud_baseline": round(amihud_baseline, 6),
        "amihud_shock_index": round(amihud_shock_index, 4),
        "turnover_adjusted_amihud": round(turnover_adjusted_amihud, 6),
        "hasbrouck_price_impact": round(hasbrouck_price_impact, 6),
        "temporary_impact": round(temporary_impact, 6),
        "permanent_impact": round(permanent_impact, 6),
        "lvar": round(lvar, 6),
        "liquidity_shock_risk": liquidity_shock_risk,
        "transfer_slippage_probability": slippage_probability,
    }
