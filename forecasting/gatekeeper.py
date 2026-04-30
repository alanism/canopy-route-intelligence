"""Kill-switch and risk gatekeeping for Canopy corridor forecasts."""

from __future__ import annotations


def apply_risk_gatekeeper(feature_bundle: dict, signals: dict) -> dict:
    current_state = feature_bundle["current_state"]
    kill_switches = []

    if float(signals["amihud_shock_index"]) > 2.5:
        kill_switches.append("AMIHUD_LIQUIDITY_SHOCK")
    if float(signals["volatility_sigma_multiplier"]) > 3.0:
        kill_switches.append("GARCH_VOLATILITY_3SIGMA")
    if float(signals["regulatory_risk_index"]) > 0.8:
        kill_switches.append("REGULATORY_RISK_CRITICAL")
    if float(current_state["solvency_ratio"]) < 0.999:
        kill_switches.append("BRIDGE_SOLVENCY_RISK")
    if float(current_state["integrity_score"]) < 0.32:
        kill_switches.append("WASH_TRADING_DETECTION")

    score = float(signals["corridor_health_score"])
    if kill_switches:
        score = min(score, 39.0)

    status = "HEALTHY"
    if kill_switches:
        status = "KILL_SWITCH_TRIGGERED"
    elif score < 55:
        status = "WATCHLIST"

    alerts = []
    if float(signals["transfer_slippage_probability"]) > 0.65:
        alerts.append("SLIPPAGE_RISK_ELEVATED")
    if float(signals["high_volatility_regime_probability"]) > 0.55:
        alerts.append("VOLATILITY_REGIME_ELEVATED")
    if float(signals["liquidity_shock_risk"]) > 0.6:
        alerts.append("LIQUIDITY_SHOCK_RISK_ELEVATED")

    return {
        "corridor_health_score": round(score, 2),
        "kill_switch_triggered": bool(kill_switches),
        "kill_switches": kill_switches,
        "status": status,
        "alerts": alerts,
    }
