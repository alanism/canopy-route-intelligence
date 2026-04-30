"""Corridor analytics payload builders for Canopy v4."""

from __future__ import annotations

from typing import Dict, Optional

from services.corridor_analytics import build_rail_corridor_metrics


def build_corridor_analytics_payload(route_result: dict, *, time_range: str = "24h") -> dict:
    rails = route_result.get("rails", [])
    fee_values = [
        float(rail.get("estimated_fee_usd", 0.0))
        for rail in rails
        if rail.get("estimated_fee_usd") is not None
    ]
    fee_floor = min(fee_values) if fee_values else 0.0
    fee_ceiling = max(fee_values) if fee_values else 1.0
    corridor_key = route_result.get("corridor_key", "UNKNOWN")

    enriched_rails = []
    for rail in rails:
        analytics = build_rail_corridor_metrics(
            corridor_key,
            rail,
            transfer_amount_usdc=float(route_result.get("amount_usdc", 0.0) or 0.0),
            fee_floor=fee_floor,
            fee_ceiling=fee_ceiling,
            token=route_result.get("token", "USDC"),
            time_range=time_range,
        )
        enriched_rails.append({**rail, **analytics})

    live_rails = [rail for rail in enriched_rails if rail.get("mode") == "live_measured"]
    recommended_rail_name = route_result.get("recommended_rail")
    selected = next(
        (rail for rail in enriched_rails if rail["rail"] == recommended_rail_name),
        live_rails[0] if live_rails else (enriched_rails[0] if enriched_rails else None),
    )

    corridor_summary = {
        "corridor": route_result.get("corridor"),
        "corridor_key": corridor_key,
        "token": route_result.get("token", "USDC"),
        "time_range": time_range,
        "best_route": selected["rail"] if selected else None,
        "canopy_recommendation": selected["rail"] if selected else None,
        "status": selected["status"] if selected else "NO_DATA",
        "volume_24h": selected["volume_24h"] if selected else 0,
        "volume_7d": selected["volume_7d"] if selected else 0,
        "tx_count": selected["tx_count"] if selected else 0,
        "unique_senders": selected["unique_senders"] if selected else 0,
        "unique_receivers": selected["unique_receivers"] if selected else 0,
        "integrity_score": selected["integrity_score"] if selected else 0,
        "liquidity_score": selected["liquidity_score_v4"] if selected else 0,
        "trust_score": selected["trust_score_v4"] if selected else 0,
        "evidence_confidence": selected["evidence_confidence"] if selected else 0,
        "strategy_score": selected["strategy_score"] if selected else 0,
        "bridge_volume": selected["bridge_volume"] if selected else 0,
        "bridge_transactions": selected["bridge_transactions"] if selected else 0,
        "bridge_share": selected["bridge_share"] if selected else 0,
        "volume_series_7d": selected["volume_series_7d"] if selected else [],
        "whale_flows": selected["top_whale_flows"] if selected else [],
        "flags": selected["adversarial_flags"] if selected else [],
        "rails": enriched_rails,
    }
    return corridor_summary


def build_corridor_analytics_response(route_result: dict, *, time_range: str = "24h") -> dict:
    analytics = build_corridor_analytics_payload(route_result, time_range=time_range)
    recommended_rail = next(
        (rail for rail in analytics["rails"] if rail["rail"] == analytics["best_route"]),
        analytics["rails"][0] if analytics["rails"] else None,
    )
    response = dict(analytics)
    if recommended_rail:
        response["best_route_detail"] = {
            "rail": recommended_rail["rail"],
            "route_score": recommended_rail["route_score"],
            "strategy_score": recommended_rail["strategy_score"],
            "cost_score": recommended_rail["cost_score_v4"],
            "liquidity_score": recommended_rail["liquidity_score_v4"],
            "trust_score": recommended_rail["trust_score_v4"],
            "evidence_confidence": recommended_rail["evidence_confidence"],
            "solvency_ratio": recommended_rail["solvency_ratio"],
            "integrity_score": recommended_rail["integrity_score"],
            "status": recommended_rail["status"],
        }
    return response


def attach_corridor_analytics(route_result: dict, *, time_range: str = "24h") -> dict:
    analytics = build_corridor_analytics_response(route_result, time_range=time_range)
    recommended = next(
        (rail for rail in analytics["rails"] if rail["rail"] == route_result.get("recommended_rail")),
        None,
    )
    route_result["corridor_analytics"] = analytics
    route_result["rails"] = analytics["rails"]
    if recommended:
        route_result["status"] = recommended["status"]
        route_result["route_score"] = recommended["route_score"]
        route_result["strategy_score"] = recommended["strategy_score"]
        route_result["strategy_score_label"] = recommended["strategy_score_label"]
        route_result["liquidity_score"] = recommended["liquidity_score_v4"]
        route_result["trust_score_v4"] = recommended["trust_score_v4"]
        route_result["evidence_confidence"] = recommended["evidence_confidence"]
        route_result["evidence_confidence_label"] = recommended["evidence_confidence_label"]
        route_result["integrity_score"] = recommended["integrity_score"]
        route_result["solvency_ratio"] = recommended["solvency_ratio"]
        route_result["adversarial_flags"] = recommended["adversarial_flags"]
    else:
        route_result["status"] = "NO_DATA"
        route_result["route_score"] = 0.0
        route_result["strategy_score"] = 0.0
        route_result["strategy_score_label"] = "0 / 100"
        route_result["liquidity_score"] = 0.0
        route_result["trust_score_v4"] = 0.0
        route_result["evidence_confidence"] = 0.0
        route_result["evidence_confidence_label"] = "0 / 100"
        route_result["integrity_score"] = 0.0
        route_result["solvency_ratio"] = 0.0
        route_result["adversarial_flags"] = []
    return route_result
