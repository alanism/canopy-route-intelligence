"""Helpers for converting graph snapshots into API responses."""

from __future__ import annotations

from typing import Optional


def resolve_chain(chain: str, route_result: dict) -> str:
    requested = chain.strip()
    if requested and requested.lower() != "auto":
        return requested.title()

    recommended = str(route_result.get("recommended_rail", "")).strip()
    if recommended in {"Ethereum", "Polygon"}:
        return recommended

    live_rails = [
        rail
        for rail in route_result.get("rails", [])
        if rail.get("rail") in {"Ethereum", "Polygon"}
    ]
    if live_rails:
        ranked = sorted(
            live_rails,
            key=lambda item: float(item.get("route_score", 0.0) or 0.0),
            reverse=True,
        )
        return ranked[0]["rail"]
    return "Polygon"


def build_response_payload(
    *,
    corridor_id: str,
    route_result: dict,
    chain: str,
    token: str,
    requested_time_range: str,
    resolved_time_range: Optional[str],
    snapshot: Optional[dict],
    graph_cache_status: str,
    graph_cache_age_seconds: Optional[int],
) -> dict:
    corridor = route_result.get("corridor")
    corridor_key = route_result.get("corridor_key", corridor_id)
    if snapshot is None:
        return {
            "corridor_id": corridor_id,
            "corridor": corridor,
            "corridor_key": corridor_key,
            "chain": chain,
            "token": token,
            "time_range": resolved_time_range or requested_time_range,
            "requested_time_range": requested_time_range,
            "status": "unavailable",
            "graph_generated_at": None,
            "graph_cache_status": graph_cache_status,
            "graph_cache_age_seconds": graph_cache_age_seconds,
            "data_layer": "derived",
            "serving_path": "in_memory_snapshot",
            "query_layer_status": "mixed_transitional",
            "query_mode": None,
            "topology": "Wallet -> Wallet",
            "topology_classification": "mixed",
            "liquidity_hubs": [],
            "nodes": [],
            "edges": [],
            "signals": [],
            "flow_density": 0.0,
            "protocol_noise_ratio": 0.0,
            "bridge_usage_rate": 0.0,
            "counterparty_entropy": 0.0,
            "liquidity_gap": 0.0,
            "confidence_score": 0.0,
            "evidence_stack": [],
        }

    return {
        "corridor_id": corridor_id,
        "corridor": corridor,
        "corridor_key": corridor_key,
        "chain": chain,
        "token": token,
        "time_range": resolved_time_range or requested_time_range,
        "requested_time_range": requested_time_range,
        "status": snapshot.get("status", graph_cache_status),
        "graph_generated_at": snapshot.get("generated_at"),
        "graph_cache_status": graph_cache_status,
        "graph_cache_age_seconds": graph_cache_age_seconds,
        "data_layer": snapshot.get("data_layer"),
        "serving_path": snapshot.get("serving_path"),
        "query_layer_status": snapshot.get("query_layer_status"),
        "query_mode": snapshot.get("query_mode"),
        "topology": snapshot.get("topology", "Wallet -> Wallet"),
        "topology_classification": snapshot.get("topology_classification", "mixed"),
        "liquidity_hubs": snapshot.get("liquidity_hubs", []),
        "nodes": snapshot.get("nodes", []),
        "edges": snapshot.get("edges", []),
        "signals": snapshot.get("signals", []),
        "flow_density": snapshot.get("flow_density", 0.0),
        "protocol_noise_ratio": snapshot.get("protocol_noise_ratio", 0.0),
        "bridge_usage_rate": snapshot.get("bridge_usage_rate", 0.0),
        "counterparty_entropy": snapshot.get("counterparty_entropy", 0.0),
        "liquidity_gap": snapshot.get("liquidity_gap", 0.0),
        "confidence_score": snapshot.get("confidence_score", 0.0),
        "evidence_stack": snapshot.get("evidence_stack", []),
    }
