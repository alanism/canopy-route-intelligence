"""Assemble context graph snapshots from deterministic edge aggregates."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Dict, List

import networkx as nx
import pandas as pd

from services.context_graph.classifier import classify_signals, score_confidence
from services.context_graph.registries import match_bridge_address, match_protocol_address


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        if value is None:
            return fallback
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _safe_int(value: Any, fallback: int = 0) -> int:
    try:
        if value is None:
            return fallback
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _short_label(node_id: str, node_type: str) -> str:
    if node_type == "wallet" and node_id.startswith("0x") and len(node_id) > 12:
        return f"{node_id[:6]}...{node_id[-4:]}"
    return node_id


def _resolve_entity(chain: str, node_id: str, node_type: str) -> tuple[str, str]:
    if node_type != "contract":
        return node_id, node_type
    protocol_match = match_protocol_address(chain, node_id)
    if protocol_match is not None:
        return (
            str(protocol_match.get("protocol_name") or node_id),
            str(protocol_match.get("protocol_type") or "protocol"),
        )
    bridge_match = match_bridge_address(chain, node_id)
    if bridge_match is not None:
        return (
            str(bridge_match.get("bridge_name") or node_id),
            "bridge",
        )
    return node_id, "contract"


def _resolve_edge_type(source_type: str, destination_type: str, fallback: str) -> str:
    if source_type == "wallet" and destination_type not in {"wallet", "contract"}:
        return f"wallet_{destination_type}"
    if source_type not in {"wallet", "contract"} and destination_type not in {"wallet", "contract"}:
        return "protocol_protocol"
    if source_type == "wallet" and destination_type == "contract":
        return "wallet_contract"
    if source_type == "contract" and destination_type == "contract":
        return "contract_contract"
    return fallback


def _compute_counterparty_entropy(edge_records: List[Dict[str, Any]]) -> float:
    counterparty_counts: Dict[str, int] = {}
    for edge in edge_records:
        if edge["source_type"] != "wallet":
            continue
        counterparty = edge["destination_node"]
        counterparty_counts[counterparty] = counterparty_counts.get(counterparty, 0) + int(
            edge["transaction_count"]
        )

    total = sum(counterparty_counts.values())
    if total <= 0:
        return 0.0

    entropy = 0.0
    for count in counterparty_counts.values():
        probability = count / total
        entropy -= probability * math.log2(probability)
    return round(2 ** entropy, 2)


def _infer_topology(edge_records: List[Dict[str, Any]]) -> str:
    node_types = {
        edge["source_type"]
        for edge in edge_records
    } | {
        edge["destination_type"]
        for edge in edge_records
    }

    if "exchange" in node_types and "bridge" in node_types:
        return "Exchange -> Bridge -> Exchange"
    if "bridge" in node_types and "protocol" in node_types:
        return "Wallet -> Bridge -> Protocol"
    if "bridge" in node_types:
        return "Wallet -> Bridge -> Wallet"
    if "protocol" in node_types or "liquidity_pool" in node_types or "dex" in node_types:
        return "Wallet -> Protocol -> Wallet"
    return "Wallet -> Wallet"


def _build_hubs(graph: nx.DiGraph) -> List[Dict[str, Any]]:
    hubs = []
    for node_id, attrs in graph.nodes(data=True):
        incident = list(graph.in_edges(node_id, data=True)) + list(graph.out_edges(node_id, data=True))
        total_volume = round(sum(_safe_float(edge[2].get("total_volume")) for edge in incident), 2)
        transaction_count = sum(_safe_int(edge[2].get("transaction_count")) for edge in incident)
        hubs.append(
            {
                "node_id": node_id,
                "label": attrs["label"],
                "node_type": attrs["node_type"],
                "total_volume": total_volume,
                "transaction_count": transaction_count,
                "degree": graph.degree(node_id),
            }
        )
    return sorted(
        hubs,
        key=lambda item: (-item["total_volume"], -item["transaction_count"], item["label"]),
    )[:8]


def _aggregate_edge_facts(edge_frame: pd.DataFrame, *, token: str) -> List[Dict[str, Any]]:
    aggregates: Dict[tuple[str, str, str, str, str, str], Dict[str, Any]] = {}
    for row in edge_frame.to_dict(orient="records"):
        source_node = str(row.get("source_node", ""))
        destination_node = str(row.get("destination_node", ""))
        if not source_node or not destination_node:
            continue

        source_type = str(row.get("source_type", "wallet"))
        destination_type = str(row.get("destination_type", "wallet"))
        edge_type = str(row.get("edge_type", "wallet_wallet"))
        edge_token = str(row.get("token", token))
        key = (source_node, destination_node, source_type, destination_type, edge_type, edge_token)
        entry = aggregates.setdefault(
            key,
            {
                "source_node": source_node,
                "destination_node": destination_node,
                "source_type": source_type,
                "destination_type": destination_type,
                "edge_type": edge_type,
                "token": edge_token,
                "total_volume": 0.0,
                "transaction_hashes": set(),
                "transaction_count_sum": 0,
                "last_seen": "",
                "gas_fee_sum": 0.0,
                "gas_fee_count": 0,
                "sample_transaction_hash": "",
                "evidence_types": set(),
            },
        )
        transaction_hash = str(row.get("transaction_hash", ""))
        if transaction_hash:
            entry["transaction_hashes"].add(transaction_hash)
            if not entry["sample_transaction_hash"]:
                entry["sample_transaction_hash"] = transaction_hash
        else:
            entry["transaction_count_sum"] += _safe_int(row.get("transaction_count"))
            sample_hash = str(row.get("sample_transaction_hash", ""))
            if sample_hash and not entry["sample_transaction_hash"]:
                entry["sample_transaction_hash"] = sample_hash
        entry["total_volume"] += _safe_float(row.get("fact_volume", row.get("total_volume")))
        last_seen = str(row.get("last_seen", row.get("block_timestamp", "")))
        if last_seen and last_seen > entry["last_seen"]:
            entry["last_seen"] = last_seen
        gas_fee = _safe_float(row.get("gas_fee_native", row.get("avg_gas_fee")))
        entry["gas_fee_sum"] += gas_fee
        entry["gas_fee_count"] += 1
        evidence_value = str(row.get("evidence_type", "transfer"))
        for item in evidence_value.split(","):
            item = item.strip()
            if item:
                entry["evidence_types"].add(item)

    aggregated_records: List[Dict[str, Any]] = []
    for entry in aggregates.values():
        aggregated_records.append(
            {
                "source_node": entry["source_node"],
                "destination_node": entry["destination_node"],
                "source_type": entry["source_type"],
                "destination_type": entry["destination_type"],
                "edge_type": entry["edge_type"],
                "token": entry["token"],
                "total_volume": round(entry["total_volume"], 2),
                "transaction_count": len(entry["transaction_hashes"]) or entry["transaction_count_sum"],
                "last_seen": entry["last_seen"],
                "avg_gas_fee": round(
                    entry["gas_fee_sum"] / max(entry["gas_fee_count"], 1),
                    8,
                ),
                "sample_transaction_hash": entry["sample_transaction_hash"],
                "evidence_type": ",".join(sorted(entry["evidence_types"])) or "transfer",
            }
        )
    return aggregated_records


def build_graph_snapshot(
    edge_frame: pd.DataFrame,
    *,
    chain: str,
    token: str,
    time_range: str,
    gap_seconds: float = 0.0,
    generated_at: str | None = None,
) -> Dict[str, Any]:
    generated_at = generated_at or datetime.now(timezone.utc).isoformat()
    graph = nx.DiGraph(chain=chain, token=token, time_range=time_range)

    if edge_frame.empty:
        return {
            "status": "no_data",
            "chain": chain,
            "token": token,
            "time_range": time_range,
            "generated_at": generated_at,
            "data_layer": "derived",
            "serving_path": "in_memory_snapshot",
            "query_layer_status": "mixed_transitional",
            "topology": "Wallet -> Wallet",
            "topology_classification": "mixed",
            "nodes": [],
            "edges": [],
            "liquidity_hubs": [],
            "signals": [],
            "flow_density": 0.0,
            "protocol_noise_ratio": 0.0,
            "bridge_usage_rate": 0.0,
            "counterparty_entropy": 0.0,
            "liquidity_gap": round(_safe_float(gap_seconds), 2),
            "confidence_score": 0.0,
            "evidence_stack": [],
            "total_transactions": 0,
        }

    edge_records = _aggregate_edge_facts(edge_frame, token=token)
    resolved_edge_records: List[Dict[str, Any]] = []
    for row in edge_records:
        source_node = str(row.get("source_node", ""))
        destination_node = str(row.get("destination_node", ""))
        if not source_node or not destination_node:
            continue

        raw_source_type = str(row.get("source_type", "wallet"))
        raw_destination_type = str(row.get("destination_type", "wallet"))
        resolved_source_node, source_type = _resolve_entity(chain, source_node, raw_source_type)
        resolved_destination_node, destination_type = _resolve_entity(
            chain,
            destination_node,
            raw_destination_type,
        )
        resolved_row = {
            **row,
            "source_node": resolved_source_node,
            "destination_node": resolved_destination_node,
            "source_type": source_type,
            "destination_type": destination_type,
            "edge_type": _resolve_edge_type(source_type, destination_type, str(row.get("edge_type", ""))),
        }
        resolved_edge_records.append(resolved_row)
        graph.add_node(
            resolved_source_node,
            label=_short_label(resolved_source_node, source_type),
            node_type=source_type,
        )
        graph.add_node(
            resolved_destination_node,
            label=_short_label(resolved_destination_node, destination_type),
            node_type=destination_type,
        )
        graph.add_edge(resolved_source_node, resolved_destination_node, **resolved_row)

    total_transactions = sum(edge["transaction_count"] for edge in resolved_edge_records)
    node_count = graph.number_of_nodes()
    protocol_transactions = sum(
        edge["transaction_count"]
        for edge in resolved_edge_records
        if edge["destination_type"] in {"protocol", "dex", "liquidity_pool", "lending"}
        or edge["source_type"] in {"protocol", "dex", "liquidity_pool", "lending"}
    )
    bridge_transactions = sum(
        edge["transaction_count"]
        for edge in resolved_edge_records
        if edge["destination_type"] == "bridge" or edge["source_type"] == "bridge"
    )
    flow_density = round(total_transactions / max(node_count, 1), 2)
    protocol_noise_ratio = round(protocol_transactions / max(total_transactions, 1), 4)
    bridge_usage_rate = round(bridge_transactions / max(total_transactions, 1), 4)
    counterparty_entropy = _compute_counterparty_entropy(resolved_edge_records)
    liquidity_gap = round(_safe_float(gap_seconds), 2)
    topology = _infer_topology(resolved_edge_records)
    topology_classification = classify_signals(
        {
            "counterparty_entropy": counterparty_entropy,
            "liquidity_gap": liquidity_gap,
            "protocol_noise_ratio": protocol_noise_ratio,
            "bridge_usage_rate": bridge_usage_rate,
        }
    )

    generated_dt = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    freshness_seconds = max(
        0.0,
        (datetime.now(timezone.utc) - generated_dt.astimezone(timezone.utc)).total_seconds(),
    )
    confidence_score = score_confidence(
        total_transactions=total_transactions,
        node_count=node_count,
        freshness_seconds=freshness_seconds,
        evidence_edge_count=len(resolved_edge_records),
    )

    liquidity_hubs = _build_hubs(graph)
    top_edges = sorted(
        resolved_edge_records,
        key=lambda item: (-item["transaction_count"], -item["total_volume"], item["edge_type"]),
    )[:20]
    visible_node_ids = {
        node["node_id"] for node in liquidity_hubs
    }
    for edge in top_edges:
        visible_node_ids.add(edge["source_node"])
        visible_node_ids.add(edge["destination_node"])

    nodes = []
    for node_id in visible_node_ids:
        attrs = graph.nodes[node_id]
        incident = list(graph.in_edges(node_id, data=True)) + list(graph.out_edges(node_id, data=True))
        nodes.append(
            {
                "node_id": node_id,
                "label": attrs["label"],
                "node_type": attrs["node_type"],
                "total_volume": round(sum(_safe_float(edge[2].get("total_volume")) for edge in incident), 2),
                "transaction_count": sum(_safe_int(edge[2].get("transaction_count")) for edge in incident),
                "degree": graph.degree(node_id),
            }
        )
    nodes.sort(key=lambda item: (-item["total_volume"], item["label"]))

    signals = [
        {"name": "flow_density", "value": flow_density, "label": "Txs per node"},
        {"name": "protocol_noise_ratio", "value": protocol_noise_ratio, "label": "Protocol-linked tx share"},
        {"name": "bridge_usage_rate", "value": bridge_usage_rate, "label": "Bridge-linked tx share"},
        {"name": "counterparty_entropy", "value": counterparty_entropy, "label": "Effective counterparties"},
        {"name": "liquidity_gap", "value": liquidity_gap, "label": "Avg seconds between transfers"},
    ]
    evidence_stack = [
        {
            "kind": edge["edge_type"],
            "source": edge["source_node"],
            "destination": edge["destination_node"],
            "transaction_count": edge["transaction_count"],
            "total_volume": edge["total_volume"],
            "last_seen": edge["last_seen"],
            "sample_transaction_hash": edge["sample_transaction_hash"],
            "evidence_type": edge["evidence_type"],
        }
        for edge in top_edges[:8]
    ]

    return {
        "status": "ok",
        "chain": chain,
        "token": token,
        "time_range": time_range,
        "generated_at": generated_at,
        "data_layer": "derived",
        "serving_path": "in_memory_snapshot",
        "query_layer_status": "mixed_transitional",
        "topology": topology,
        "topology_classification": topology_classification,
        "nodes": nodes[:20],
        "edges": top_edges,
        "liquidity_hubs": liquidity_hubs,
        "signals": signals,
        "flow_density": flow_density,
        "protocol_noise_ratio": protocol_noise_ratio,
        "bridge_usage_rate": bridge_usage_rate,
        "counterparty_entropy": counterparty_entropy,
        "liquidity_gap": liquidity_gap,
        "confidence_score": confidence_score,
        "evidence_stack": evidence_stack,
        "total_transactions": total_transactions,
    }
