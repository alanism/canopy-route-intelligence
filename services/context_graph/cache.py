"""Background cache for deterministic context graph snapshots."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from services.bigquery_client import DEFAULT_MAX_BYTES_BILLED, dry_run_sql, execute_sql
from services.context_graph.graph_builder import build_graph_snapshot
from services.context_graph.queries import (
    SUPPORTED_TIME_RANGES,
    build_context_graph_query,
    build_liquidity_gap_query,
    resolve_budget_safe_time_range,
)
from services.context_graph.registries import get_bridge_registry, get_protocol_registry
from services.context_graph.schema import discover_supported_schemas
from services.bigquery_client import get_client
from services.logging_utils import log_event
from services.query_metrics import get_query_metrics_snapshot
from services.summary_store import get_context_graph_summary, upsert_context_graph_summary
from services.runtime_mode import get_runtime_mode_label, is_demo_mode

logger = logging.getLogger("sci-agent.context-graph")

CANOPY_CONTEXT_GRAPH_ENABLED = os.getenv("CANOPY_CONTEXT_GRAPH_ENABLED", "true").lower() == "true"
SUPPORTED_CHAINS = ("Ethereum", "Polygon")
GRAPH_POLL_INTERVAL = 900
GRAPH_BACKOFF_INTERVAL = 60
TOKEN = "USDC"
DEFAULT_POLL_TIME_RANGES = tuple(
    item.strip().lower()
    for item in os.getenv("CANOPY_CONTEXT_GRAPH_TIME_RANGES", "1h").split(",")
    if item.strip()
)
MAX_BYTES_PER_QUERY = int(
    os.getenv("CANOPY_CONTEXT_GRAPH_MAX_BYTES_PER_QUERY", str(DEFAULT_MAX_BYTES_BILLED))
)
TOKEN_CONTRACTS = {
    "Ethereum": os.getenv(
        "USDC_ETH_CONTRACT",
        "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    ),
    "Polygon": os.getenv(
        "USDC_POLYGON_CONTRACT",
        "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
    ),
}
CHAIN_QUERY_MODES = {
    "Ethereum": os.getenv("CANOPY_CONTEXT_GRAPH_ETHEREUM_MODE", "transfer_only").lower(),
    "Polygon": os.getenv("CANOPY_CONTEXT_GRAPH_POLYGON_MODE", "transfer_only").lower(),
}
EDGE_MAX_BYTES_PER_QUERY = int(
    os.getenv("CANOPY_CONTEXT_GRAPH_EDGE_MAX_BYTES_PER_QUERY", str(DEFAULT_MAX_BYTES_BILLED))
)
GAP_MAX_BYTES_PER_QUERY = int(
    os.getenv("CANOPY_CONTEXT_GRAPH_GAP_MAX_BYTES_PER_QUERY", str(MAX_BYTES_PER_QUERY))
)

BOOTSTRAP = {
    "status": "initializing",
    "snapshots": {},
    "schemas": {},
    "last_updated": None,
    "poll_count": 0,
    "last_error": None,
    "is_bootstrap": True,
}

_graph_cache: dict = {**BOOTSTRAP}


def _snapshot_key(chain: str, token: str, time_range: str) -> str:
    return f"{chain}:{token}:{time_range}"


async def start_poller() -> None:
    if is_demo_mode():
        seed_demo_cache()
        return
    while True:
        try:
            await asyncio.to_thread(refresh_snapshots)
            await asyncio.sleep(GRAPH_POLL_INTERVAL)
        except asyncio.CancelledError:
            logger.info("Context graph poller cancelled")
            break
        except Exception as exc:
            logger.error("Context graph poll cycle failed: %s", exc, exc_info=True)
            await asyncio.sleep(GRAPH_BACKOFF_INTERVAL)


def seed_demo_cache() -> dict:
    global _graph_cache
    _graph_cache = {
        "status": "demo",
        "snapshots": {},
        "schemas": {},
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "poll_count": 1,
        "last_error": None,
        "is_bootstrap": False,
        "mode": get_runtime_mode_label(),
    }
    return _graph_cache


def refresh_snapshots() -> None:
    global _graph_cache

    poll_count = _graph_cache.get("poll_count", 0) + 1
    client = get_client()
    if not CANOPY_CONTEXT_GRAPH_ENABLED:
        _graph_cache = {
            **_graph_cache,
            "status": "disabled",
            "poll_count": poll_count,
            "last_error": None,
            "is_bootstrap": False,
        }
        return

    snapshots = {}
    errors = []

    try:
        schemas = discover_supported_schemas(SUPPORTED_CHAINS, client=client)
    except Exception as exc:
        errors.append(str(exc))
        schemas = {}

    generated_at = datetime.now(timezone.utc).isoformat()
    for chain, schema in schemas.items():
        protocol_registry = get_protocol_registry(chain)
        bridge_registry = get_bridge_registry(chain)
        token_contract = TOKEN_CONTRACTS[chain]

        query_mode = CHAIN_QUERY_MODES.get(chain, "full")
        for time_range in DEFAULT_POLL_TIME_RANGES:
            resolved_time_range = resolve_budget_safe_time_range(
                chain,
                time_range,
                mode=query_mode,
            )
            try:
                edge_sql = build_context_graph_query(
                    schema,
                    token_contract=token_contract,
                    protocol_registry=protocol_registry,
                    bridge_registry=bridge_registry,
                    time_range=resolved_time_range,
                    mode=query_mode,
                )
                gap_sql = build_liquidity_gap_query(
                    schema,
                    token_contract=token_contract,
                    time_range=resolved_time_range,
                )
                for label, sql, max_bytes in (
                    ("edges", edge_sql, EDGE_MAX_BYTES_PER_QUERY),
                    ("gap", gap_sql, GAP_MAX_BYTES_PER_QUERY),
                ):
                    bytes_processed = dry_run_sql(
                        sql,
                        query_name=f"context_graph_{label}_{chain.lower()}_{time_range}",
                        query_family=f"context_graph_{label}",
                        maximum_bytes_billed=max_bytes,
                        query_classification="derived",
                    )
                    if bytes_processed > max_bytes:
                        raise ValueError(
                            f"{label} query over budget ({bytes_processed} bytes > {max_bytes})"
                        )
                edge_frame = execute_sql(
                    edge_sql,
                    ttl_seconds=0,
                    use_cache=False,
                    query_name=f"context_graph_edges_{chain.lower()}_{time_range}",
                    query_family="context_graph_edges",
                    maximum_bytes_billed=EDGE_MAX_BYTES_PER_QUERY,
                    query_classification="derived",
                )
                gap_frame = execute_sql(
                    gap_sql,
                    ttl_seconds=0,
                    use_cache=False,
                    query_name=f"context_graph_gap_{chain.lower()}_{time_range}",
                    query_family="context_graph_gap",
                    maximum_bytes_billed=GAP_MAX_BYTES_PER_QUERY,
                    query_classification="derived",
                )
                gap_seconds = 0.0
                if not gap_frame.empty:
                    gap_seconds = float(gap_frame.iloc[0].get("avg_gap_seconds") or 0.0)
                snapshot = build_graph_snapshot(
                    edge_frame,
                    chain=chain,
                    token=TOKEN,
                    time_range=resolved_time_range,
                    gap_seconds=gap_seconds,
                    generated_at=generated_at,
                )
                snapshot["query_mode"] = query_mode
                snapshot["requested_time_range"] = time_range
                snapshot["resolved_time_range"] = resolved_time_range
                snapshots[_snapshot_key(chain, TOKEN, time_range)] = snapshot
            except Exception as exc:
                logger.error(
                    "Context graph refresh failed for %s %s: %s",
                    chain,
                    time_range,
                    exc,
                    exc_info=True,
                )
                errors.append(f"{chain}:{time_range}: {exc}")

    if snapshots:
        upsert_context_graph_summary(
            [
                {
                    "chain": snapshot["chain"],
                    "token": snapshot["token"],
                    "time_range": snapshot["time_range"],
                    "snapshot_json": json.dumps(snapshot, sort_keys=True),
                    "materialized_at": generated_at,
                }
                for snapshot in snapshots.values()
            ]
        )
        _graph_cache = {
            "status": "ok",
            "snapshots": snapshots,
            "schemas": {chain: schema.to_dict() for chain, schema in schemas.items()},
            "last_updated": generated_at,
            "poll_count": poll_count,
            "last_error": None if not errors else " | ".join(errors),
            "is_bootstrap": False,
        }
        metrics = get_query_metrics_snapshot()
        log_event(
            logger,
            "bigquery.metrics.digest",
            source="context_graph_poller",
            poll_count=poll_count,
            execution_count=metrics["overall"]["execution_count"],
            execution_bytes=metrics["overall"]["execution_bytes"],
            dry_run_count=metrics["overall"]["dry_run_count"],
            dry_run_bytes=metrics["overall"]["dry_run_bytes"],
            families={
                family: {
                    "query_count": item["query_count"],
                    "avg_bytes_per_query": item["avg_bytes_per_query"],
                    "max_budget_utilization": item["max_budget_utilization"],
                }
                for family, item in metrics["families"].items()
                if family.startswith("context_graph")
            },
        )
        return

    _graph_cache = {
        **_graph_cache,
        "status": "stale" if _graph_cache.get("status") != "initializing" else "initializing",
        "poll_count": poll_count,
        "last_error": " | ".join(errors) if errors else "No context graph snapshots generated",
    }


def get_cache() -> dict:
    return _graph_cache


def get_cache_age_seconds() -> Optional[int]:
    last = _graph_cache.get("last_updated")
    if last is None:
        return None
    try:
        updated = datetime.fromisoformat(last)
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - updated
        return int(delta.total_seconds())
    except (TypeError, ValueError):
        return None


def get_snapshot(chain: str, *, token: str = TOKEN, time_range: str = "24h") -> Optional[dict]:
    snapshot = _graph_cache.get("snapshots", {}).get(_snapshot_key(chain, token, time_range))
    if snapshot is not None:
        return snapshot
    return get_context_graph_summary(chain, token=token, time_range=time_range)


def get_best_snapshot(chain: str, *, token: str = TOKEN, requested_time_range: str = "24h") -> tuple[Optional[dict], Optional[str]]:
    preferred = [requested_time_range]
    if requested_time_range == "7d":
        preferred.extend(["24h", "6h", "2h", "1h"])
    elif requested_time_range == "24h":
        preferred.extend(["6h", "2h", "1h"])
    elif requested_time_range == "6h":
        preferred.extend(["2h", "1h"])

    for candidate in preferred:
        snapshot = get_snapshot(chain, token=token, time_range=candidate)
        if snapshot is not None:
            return snapshot, candidate
    return None, None
