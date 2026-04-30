"""Weighted scoring for Canopy corridor forecasting."""

from __future__ import annotations


def _growth_to_score(growth_rate: float) -> float:
    bounded = max(min(growth_rate, 25.0), -25.0)
    return (bounded + 25.0) / 50.0


def compute_corridor_health_score(signals: dict) -> dict:
    stability = float(signals["corridor_stability_probability"])
    liquidity_shock = float(signals["liquidity_shock_risk"])
    demand_growth = float(signals["demand_growth_forecast"])
    fx_volatility = float(signals["fx_volatility_signal"])
    regulatory_risk = float(signals["regulatory_risk_index"])

    growth_score = _growth_to_score(demand_growth)
    raw_health_score = 100.0 * (
        (stability * 0.30)
        + ((1.0 - liquidity_shock) * 0.25)
        + (growth_score * 0.15)
        + ((1.0 - fx_volatility) * 0.15)
        + ((1.0 - regulatory_risk) * 0.15)
    )

    return {
        "corridor_health_score": round(raw_health_score, 2),
        "score_breakdown": {
            "stability_component": round(stability * 30.0, 2),
            "liquidity_component": round((1.0 - liquidity_shock) * 25.0, 2),
            "demand_component": round(growth_score * 15.0, 2),
            "fx_component": round((1.0 - fx_volatility) * 15.0, 2),
            "regulatory_component": round((1.0 - regulatory_risk) * 15.0, 2),
        },
    }
