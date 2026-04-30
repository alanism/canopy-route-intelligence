"""Risk evaluation for Canopy execution simulation."""

from __future__ import annotations


def evaluate_risk_profile(*, snapshot: dict, route_result: dict, slippage_tolerance: float) -> dict:
    alerts = []
    flags = []

    max_trade_share = float(route_result.get("max_trade_share", 0.0))
    if max_trade_share > 0.10:
        alerts.append("HIGH_SLIPPAGE_WARNING")

    for bridge_step in route_result.get("bridge_steps", []):
        amount_in = float(bridge_step.get("amount_in", 0.0))
        vault_balance = float(bridge_step.get("vault_balance", 0.0))
        if amount_in > vault_balance > 0:
            alerts.append("BRIDGE_CAPACITY_EXCEEDED")

        current_velocity = float(bridge_step.get("current_velocity_per_min", 0.0))
        historical_velocity = float(bridge_step.get("historical_velocity_per_min", 0.0))
        if historical_velocity > 0 and current_velocity > (historical_velocity * 3):
            alerts.append("LIQUIDITY_VELOCITY_CRITICAL")

    if route_result.get("gas_spike_detected"):
        alerts.append("GAS_SPIKE_DETECTION")

    for warning in snapshot.get("warnings", []):
        if warning not in alerts:
            alerts.append(warning)

    if not alerts:
        flags.append("LIQUIDITY_STABLE")
    else:
        flags.append("RISK_CHECK_REQUIRED")

    realized_slippage = float(route_result.get("realized_slippage", 0.0))
    if realized_slippage <= slippage_tolerance:
        flags.append("SLIPPAGE_WITHIN_TOLERANCE")

    liquidity_score = max(0.0, min(float(route_result.get("liquidity_score", 0.0)), 1.0))
    safety_score = max(0.0, min(float(route_result.get("safety_score", 0.0)), 1.0))

    confidence = 0.96
    confidence -= min(realized_slippage / max(slippage_tolerance, 0.0001), 3.0) * 0.03
    confidence -= len(set(alerts)) * 0.08
    confidence -= max(0.0, 0.35 - liquidity_score) * 0.12
    confidence -= max(0.0, 0.55 - safety_score) * 0.16
    confidence = max(0.15, min(round(confidence, 4), 0.99))

    return {
        "confidence_score": confidence,
        "liquidity_score": round(liquidity_score, 4),
        "safety_score": round(safety_score, 4),
        "flags": flags,
        "alerts": sorted(set(alerts)),
    }
