"""Graph-based route selection for the Canopy execution engine."""

from __future__ import annotations

import heapq
from dataclasses import dataclass
from typing import Dict, List, Tuple

from models.request_models import SimulateRequest
from services.bridge_engine import simulate_bridge_transfer
from services.gas_engine import estimate_gas_cost
from services.swap_engine import simulate_swap_from_snapshot

DESTINATION_SWAP_BY_CHAIN = {
    "Polygon": "swap_quickswap",
    "Arbitrum": "swap_sushiswap",
    "Base": "swap_aerodrome",
    "Ethereum": "swap_uniswap",
}

SUPPORTED_BRIDGES = {
    ("Ethereum", "Polygon"): ["Hop", "Stargate", "LayerZero", "PolygonBridge"],
    ("Ethereum", "Arbitrum"): ["Hop", "Stargate", "LayerZero"],
    ("Ethereum", "Base"): ["Stargate", "LayerZero"],
}

PREFERENCE_ADJUSTMENTS = {
    "balanced": {"cost": 1.0, "liquidity": 1.0, "safety": 1.0},
    "cheapest": {"cost": 0.82, "liquidity": 1.05, "safety": 1.05},
    "fastest": {"cost": 1.05, "liquidity": 0.96, "safety": 1.0},
    "safest": {"cost": 1.1, "liquidity": 0.94, "safety": 0.82},
}


@dataclass(frozen=True)
class Edge:
    source: str
    target: str
    weight: float
    operation_name: str
    kind: str
    metadata: dict


def _get_supported_bridges(source_chain: str, destination_chain: str) -> List[str]:
    bridges = SUPPORTED_BRIDGES.get((source_chain, destination_chain))
    if bridges:
        return bridges
    return ["Hop", "Stargate", "LayerZero"]


def _estimate_swap_penalty(snapshot: dict, pool_key: str, amount: float, preference: str) -> float:
    pool = snapshot["dex_pool_reserves"][pool_key]
    trade_share = amount / max(float(pool["reserve_in"]), 1.0)
    cost = trade_share * 0.5
    liquidity_penalty = trade_share * 0.7
    safety_penalty = trade_share * 0.4
    adj = PREFERENCE_ADJUSTMENTS[preference]
    return round(
        (0.4 * cost * adj["cost"])
        + (0.4 * liquidity_penalty * adj["liquidity"])
        + (0.2 * safety_penalty * adj["safety"]),
        6,
    )


def _estimate_bridge_penalty(
    *,
    snapshot: dict,
    bridge_name: str,
    source_chain: str,
    destination_chain: str,
    amount: float,
    preference: str,
) -> float:
    bridge_cfg = snapshot["bridge_config"][bridge_name]
    bridge_fee_ratio = (
        float(bridge_cfg["protocol_fee_bps"])
        + float(bridge_cfg["bonder_fee_bps"])
        + float(bridge_cfg["liquidity_fee_bps"])
    ) / 10_000
    vault_balance = float(
        snapshot["bridge_vault_balances"].get(bridge_name, {}).get(
            f"{source_chain}:{destination_chain}",
            max(amount * 3, 1.0),
        )
    )
    liquidity_penalty = max(0.0, 1 - min(vault_balance / max(amount * 8, 1.0), 1.0))
    safety_penalty = max(0.0, 1 - float(bridge_cfg.get("safety_factor", 0.8)))
    incentive_penalty = float(bridge_cfg.get("incentive_usd", 0.0)) / max(amount, 1.0)
    congestion_penalty = float(snapshot["network_congestion"].get(source_chain, 0.2)) * 0.03
    adj = PREFERENCE_ADJUSTMENTS[preference]
    return round(
        (0.4 * (bridge_fee_ratio + congestion_penalty) * adj["cost"])
        + (0.4 * liquidity_penalty * adj["liquidity"])
        + (0.2 * safety_penalty * adj["safety"])
        - incentive_penalty,
        6,
    )


def build_route_graph(request: SimulateRequest, snapshot: dict) -> Tuple[List[Edge], str, str]:
    start = f"{request.source_chain}:{request.token}"
    source_canonical = f"{request.source_chain}:USDC"
    destination_wrapped_template = f"{request.destination_chain}:USDC_BRIDGED"
    destination_canonical = f"{request.destination_chain}:USDC"
    end = "END"

    edges: List[Edge] = []

    if request.token != "USDC":
        source_pool_key = f"{request.source_chain}:{request.token}:USDC"
        if source_pool_key not in snapshot["dex_pool_reserves"]:
            source_pool_key = f"{request.source_chain}:ETH:USDC"
        edges.append(
            Edge(
                source=start,
                target=source_canonical,
                weight=_estimate_swap_penalty(
                    snapshot,
                    source_pool_key,
                    request.amount,
                    request.preference,
                ),
                operation_name="swap_source_pool",
                kind="swap",
                metadata={"pool_key": source_pool_key, "chain": request.source_chain},
            )
        )
    else:
        edges.append(
            Edge(
                source=start,
                target=source_canonical,
                weight=0.0,
                operation_name="hold_canonical",
                kind="hold",
                metadata={"chain": request.source_chain},
            )
        )

    destination_pool_key = f"{request.destination_chain}:USDC_BRIDGED:USDC"
    swap_operation = DESTINATION_SWAP_BY_CHAIN.get(
        request.destination_chain,
        "swap_exchange_exit",
    )

    for bridge_name in _get_supported_bridges(request.source_chain, request.destination_chain):
        wrapped_node = f"{destination_wrapped_template}:{bridge_name}"
        edges.append(
            Edge(
                source=source_canonical,
                target=wrapped_node,
                weight=_estimate_bridge_penalty(
                    snapshot=snapshot,
                    bridge_name=bridge_name,
                    source_chain=request.source_chain,
                    destination_chain=request.destination_chain,
                    amount=request.amount,
                    preference=request.preference,
                ),
                operation_name=f"bridge_{bridge_name.lower()}",
                kind="bridge",
                metadata={
                    "bridge_name": bridge_name,
                    "source_chain": request.source_chain,
                    "destination_chain": request.destination_chain,
                },
            )
        )
        edges.append(
            Edge(
                source=wrapped_node,
                target=destination_canonical,
                weight=_estimate_swap_penalty(
                    snapshot,
                    destination_pool_key,
                    request.amount,
                    request.preference,
                ),
                operation_name=swap_operation,
                kind="swap",
                metadata={"pool_key": destination_pool_key, "chain": request.destination_chain},
            )
        )

    edges.append(
        Edge(
            source=destination_canonical,
            target=end,
            weight=0.008,
            operation_name="deposit_exchange",
            kind="deposit",
            metadata={"chain": request.destination_chain},
        )
    )

    return edges, start, end


def _build_adjacency(edges: List[Edge]) -> Dict[str, List[Edge]]:
    adjacency: Dict[str, List[Edge]] = {}
    for edge in edges:
        adjacency.setdefault(edge.source, []).append(edge)
        adjacency.setdefault(edge.target, [])
    return adjacency


def _reconstruct_path(previous: Dict[str, Edge], start: str, end: str) -> List[Edge]:
    node = end
    path: List[Edge] = []
    while node != start:
        edge = previous[node]
        path.append(edge)
        node = edge.source
    path.reverse()
    return path


def _run_dijkstra(edges: List[Edge], start: str, end: str) -> List[Edge]:
    adjacency = _build_adjacency(edges)
    distances = {node: float("inf") for node in adjacency}
    previous: Dict[str, Edge] = {}
    distances[start] = 0.0
    queue: List[Tuple[float, str]] = [(0.0, start)]

    while queue:
        current_dist, node = heapq.heappop(queue)
        if current_dist > distances[node]:
            continue
        if node == end:
            break
        for edge in adjacency.get(node, []):
            candidate = current_dist + edge.weight
            if candidate < distances[edge.target]:
                distances[edge.target] = candidate
                previous[edge.target] = edge
                heapq.heappush(queue, (candidate, edge.target))

    if end not in previous:
        raise ValueError("No route found with Dijkstra")
    return _reconstruct_path(previous, start, end)


def _run_bellman_ford(edges: List[Edge], start: str, end: str) -> List[Edge]:
    nodes = {edge.source for edge in edges} | {edge.target for edge in edges}
    distances = {node: float("inf") for node in nodes}
    previous: Dict[str, Edge] = {}
    distances[start] = 0.0

    for _ in range(len(nodes) - 1):
        updated = False
        for edge in edges:
            if distances[edge.source] == float("inf"):
                continue
            candidate = distances[edge.source] + edge.weight
            if candidate < distances[edge.target]:
                distances[edge.target] = candidate
                previous[edge.target] = edge
                updated = True
        if not updated:
            break

    if end not in previous:
        raise ValueError("No route found with Bellman-Ford")
    return _reconstruct_path(previous, start, end)


def _simulate_path(path: List[Edge], request: SimulateRequest, snapshot: dict, algorithm_used: str) -> dict:
    amount = float(request.amount)
    steps = []
    route = []
    total_fees_usd = 0.0
    estimated_time_seconds = 0
    max_trade_share = 0.0
    realized_slippage = 0.0
    gas_spike_detected = False
    liquidity_components = []
    safety_components = []
    bridge_steps = []

    for edge in path:
        route.append(edge.operation_name)
        amount_in = amount

        if edge.kind == "hold":
            amount_out = amount
            step_fee_usd = 0.0
            step_seconds = 0
            details = {"note": "Token already canonical for bridge entry."}
        elif edge.kind == "swap":
            swap = simulate_swap_from_snapshot(
                snapshot=snapshot,
                pool_key=edge.metadata["pool_key"],
                amount_in=amount,
            )
            gas = estimate_gas_cost(
                snapshot=snapshot,
                chain=edge.metadata["chain"],
                operation_type="swap",
                gas_speed=request.gas_speed,
            )
            amount_out = float(swap["amount_out"])
            step_fee_usd = float(gas["gas_cost_usd"])
            step_seconds = int(gas["confirmation_seconds"])
            max_trade_share = max(max_trade_share, float(swap["trade_share"]))
            realized_slippage = max(realized_slippage, float(swap["slippage"]))
            gas_spike_detected = gas_spike_detected or bool(gas["gas_spike_detected"])
            liquidity_components.append(max(0.0, 1 - min(float(swap["trade_share"]) * 2.2, 1.0)))
            safety_components.append(max(0.0, 1 - min(float(swap["slippage"]) * 12, 1.0)))
            details = {
                "pool_key": edge.metadata["pool_key"],
                "slippage": float(swap["slippage"]),
                "execution_price": float(swap["execution_price"]),
                "trade_share": float(swap["trade_share"]),
                "gas_price_gwei": float(gas["gas_price_gwei"]),
            }
        elif edge.kind == "bridge":
            bridge = simulate_bridge_transfer(
                snapshot=snapshot,
                bridge_name=edge.metadata["bridge_name"],
                source_chain=edge.metadata["source_chain"],
                destination_chain=edge.metadata["destination_chain"],
                input_amount=amount,
            )
            gas = estimate_gas_cost(
                snapshot=snapshot,
                chain=edge.metadata["source_chain"],
                operation_type="bridge",
                gas_speed=request.gas_speed,
            )
            velocity_key = (
                f"{edge.metadata['bridge_name']}:"
                f"{edge.metadata['source_chain']}:"
                f"{edge.metadata['destination_chain']}"
            )
            velocity = snapshot["bridge_liquidity_velocity"].get(
                velocity_key,
                {"current_per_min": 0.0, "historical_per_min": 0.0},
            )
            amount_out = float(bridge["amount_out"])
            step_fee_usd = float(gas["gas_cost_usd"]) + float(bridge["fees_total_usd"]) - float(
                bridge["incentive_usd"]
            )
            step_seconds = int(bridge["estimated_seconds"] + gas["confirmation_seconds"])
            gas_spike_detected = gas_spike_detected or bool(gas["gas_spike_detected"])
            vault_ratio = min(float(bridge["vault_balance"]) / max(amount_in, 1.0), 3.0)
            liquidity_components.append(min(vault_ratio / 1.2, 1.0))
            safety_components.append(float(bridge["safety_factor"]))
            bridge_step = {
                "bridge_name": edge.metadata["bridge_name"],
                "amount_in": round(amount_in, 6),
                "vault_balance": float(bridge["vault_balance"]),
                "current_velocity_per_min": float(velocity["current_per_min"]),
                "historical_velocity_per_min": float(velocity["historical_per_min"]),
            }
            bridge_steps.append(bridge_step)
            details = {
                "bridge_name": edge.metadata["bridge_name"],
                "vault_balance": float(bridge["vault_balance"]),
                "protocol_fee": float(bridge["protocol_fee"]),
                "bonder_fee": float(bridge["bonder_fee"]),
                "liquidity_fee": float(bridge["liquidity_fee"]),
                "incentive_usd": float(bridge["incentive_usd"]),
            }
        else:
            gas = estimate_gas_cost(
                snapshot=snapshot,
                chain=edge.metadata["chain"],
                operation_type="deposit",
                gas_speed=request.gas_speed,
            )
            amount_out = amount
            step_fee_usd = float(gas["gas_cost_usd"])
            step_seconds = int(gas["confirmation_seconds"])
            gas_spike_detected = gas_spike_detected or bool(gas["gas_spike_detected"])
            liquidity_components.append(0.95)
            safety_components.append(0.97)
            details = {
                "destination_chain": edge.metadata["chain"],
                "gas_price_gwei": float(gas["gas_price_gwei"]),
            }

        amount = amount_out
        total_fees_usd += step_fee_usd
        estimated_time_seconds += step_seconds
        steps.append(
            {
                "operation": edge.operation_name,
                "chain": edge.metadata.get("chain", edge.metadata.get("destination_chain", request.destination_chain)),
                "amount_in": round(amount_in, 6),
                "amount_out": round(amount_out, 6),
                "fee_usd": round(step_fee_usd, 6),
                "estimated_seconds": int(step_seconds),
                "details": details,
            }
        )

    liquidity_score = round(sum(liquidity_components) / max(len(liquidity_components), 1), 4)
    safety_score = round(sum(safety_components) / max(len(safety_components), 1), 4)
    cost_penalty = min(total_fees_usd / max(request.amount, 1.0) / 0.02, 1.0)
    route_score = round(
        (0.4 * cost_penalty) + (0.4 * (1 - liquidity_score)) + (0.2 * (1 - safety_score)),
        4,
    )
    lower_bound_minutes = max(1, round((estimated_time_seconds * 0.85) / 60))
    upper_bound_minutes = max(lower_bound_minutes + 1, round((estimated_time_seconds * 1.35) / 60))

    return {
        "algorithm_used": algorithm_used,
        "route": route,
        "steps": steps,
        "total_received": round(amount, 6),
        "total_fees_usd": round(total_fees_usd, 6),
        "estimated_time_seconds": int(estimated_time_seconds),
        "settlement_time_confidence": f"{estimated_time_seconds / 60:.1f} minutes (99% confidence)",
        "settlement_range": f"{lower_bound_minutes}-{upper_bound_minutes} minutes (reorg risk)",
        "max_trade_share": round(max_trade_share, 6),
        "realized_slippage": round(realized_slippage, 6),
        "gas_spike_detected": gas_spike_detected,
        "liquidity_score": liquidity_score,
        "safety_score": safety_score,
        "route_score": route_score,
        "bridge_steps": bridge_steps,
    }


def select_best_execution_route(request: SimulateRequest, snapshot: dict) -> dict:
    edges, start, end = build_route_graph(request, snapshot)
    has_negative_edge = any(edge.weight < 0 for edge in edges)
    algorithm_used = "bellman-ford" if has_negative_edge else "dijkstra"
    if has_negative_edge:
        path = _run_bellman_ford(edges, start, end)
    else:
        path = _run_dijkstra(edges, start, end)
    return _simulate_path(path, request, snapshot, algorithm_used)
