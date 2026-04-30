"""Helpers for running non-request-path BigQuery audit parity checks."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd

from api import cache as route_cache
from services.bigquery_client import execute_sql, get_client
from services.context_graph import cache as context_graph_cache
from services.context_graph.cache import (
    CHAIN_QUERY_MODES,
    EDGE_MAX_BYTES_PER_QUERY,
    SUPPORTED_CHAINS,
    TOKEN,
    TOKEN_CONTRACTS,
)
from services.context_graph.graph_builder import _aggregate_edge_facts, build_graph_snapshot
from services.context_graph.queries import build_context_graph_query, resolve_budget_safe_time_range
from services.context_graph.registries import get_bridge_registry, get_protocol_registry
from services.context_graph.schema import discover_supported_schemas
from services.corridor_analytics import build_corridor_base_summary
from services.corridor_config import get_corridors
from services.summary_store import get_context_graph_summary, get_corridor_summary, init_summary_store
from services.token_registry import DEFAULT_TOKEN

AUDIT_DIRNAME = "audit"
CORRIDOR_FIELDS = (
    "volume_24h",
    "volume_7d",
    "tx_count",
    "unique_senders",
    "unique_receivers",
    "velocity_unique_capital",
    "concentration_score",
    "bridge_name",
    "bridge_share",
    "bridge_volume",
    "bridge_transactions",
    "whale_threshold_usd",
    "whale_activity_score",
    "net_flow_7d",
    "top_whale_flows",
    "source",
    "data_layer",
    "serving_path",
)
CONTEXT_GRAPH_FIELDS = (
    "topology",
    "topology_classification",
    "flow_density",
    "protocol_noise_ratio",
    "bridge_usage_rate",
    "counterparty_entropy",
    "liquidity_gap",
    "total_transactions",
    "edges",
    "evidence_stack",
)


@dataclass(frozen=True)
class AuditCheckResult:
    scope: dict[str, Any]
    fields_compared: list[str]
    mismatches: list[dict[str, Any]]
    intentional_deltas: list[str]
    status: str
    notes: list[str]


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_output_dir(output_dir: Optional[Path]) -> Path:
    target = output_dir or Path.cwd() / AUDIT_DIRNAME
    target.mkdir(parents=True, exist_ok=True)
    return target


def _normalize_value(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 8)
    if isinstance(value, list):
        return [_normalize_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _normalize_value(value[key]) for key in sorted(value.keys())}
    return value


def _compare_payloads(left: dict[str, Any], right: dict[str, Any], *, fields: Iterable[str]) -> list[dict[str, Any]]:
    mismatches = []
    for field in fields:
        left_value = _normalize_value(left.get(field))
        right_value = _normalize_value(right.get(field))
        if left_value != right_value:
            mismatches.append(
                {
                    "field": field,
                    "expected": left_value,
                    "actual": right_value,
                }
            )
    return mismatches


def run_corridor_live_parity() -> dict[str, Any]:
    init_summary_store()
    cache_payload = route_cache.get_cache()
    route_cache._materialize_corridor_summaries(
        cache_payload=cache_payload,
        materialized_at=_utcnow_iso(),
    )
    sample_corridors = [corridor["key"] for corridor in get_corridors()[:2]]
    sample_rails = ("Ethereum", "Polygon")
    results = []
    for corridor_id in sample_corridors:
        for rail in sample_rails:
            scope = {
                "corridor_id": corridor_id,
                "rail": rail,
                "token": DEFAULT_TOKEN,
                "time_range": "24h",
            }
            try:
                materialized = get_corridor_summary(corridor_id, rail, token=DEFAULT_TOKEN, time_range="24h")
                recomputed = build_corridor_base_summary(
                    corridor_id,
                    route_cache._rail_seed_from_cache(cache_payload, rail, DEFAULT_TOKEN),
                    rail=rail,
                    token=DEFAULT_TOKEN,
                    time_range="24h",
                    allow_live_bigquery=False,
                )
            except Exception as exc:
                results.append(
                    asdict(
                        AuditCheckResult(
                            scope=scope,
                            fields_compared=list(CORRIDOR_FIELDS),
                            mismatches=[{"field": "runtime_error", "expected": "none", "actual": str(exc)}],
                            intentional_deltas=[],
                            status="fail",
                            notes=["Corridor live parity could not complete for sampled scope."],
                        )
                    )
                )
                continue
            if materialized is None:
                results.append(
                    asdict(
                        AuditCheckResult(
                            scope=scope,
                            fields_compared=list(CORRIDOR_FIELDS),
                            mismatches=[{"field": "materialized_summary", "expected": "present", "actual": None}],
                            intentional_deltas=[],
                            status="fail",
                            notes=["Materialized corridor summary missing for sampled scope."],
                        )
                    )
                )
                continue
            mismatches = _compare_payloads(materialized, recomputed, fields=CORRIDOR_FIELDS)
            results.append(
                asdict(
                    AuditCheckResult(
                        scope=scope,
                        fields_compared=list(CORRIDOR_FIELDS),
                        mismatches=mismatches,
                        intentional_deltas=[],
                        status="pass" if not mismatches else "fail",
                        notes=["Materialized corridor summary compared against fresh recomputation from cache poll inputs."],
                    )
                )
            )
    passed = all(item["status"] == "pass" for item in results)
    return {
        "status": "pass" if passed else "fail",
        "sampled_entities": results,
        "notes": [
            "Corridor parity uses materialized summaries versus fresh recomputation from current batch poll inputs.",
            "No request-path BigQuery execution occurs during this audit check.",
        ],
    }


def run_context_graph_live_parity() -> dict[str, Any]:
    init_summary_store()
    schemas = discover_supported_schemas(SUPPORTED_CHAINS, client=get_client())
    results = []
    for chain in SUPPORTED_CHAINS:
        schema = schemas.get(chain)
        if schema is None:
            results.append(
                asdict(
                    AuditCheckResult(
                        scope={"chain": chain, "token": TOKEN, "time_ranges": ["1h", "24h"]},
                        fields_compared=list(CONTEXT_GRAPH_FIELDS),
                        mismatches=[{"field": "schema", "expected": "supported", "actual": None}],
                        intentional_deltas=[],
                        status="fail",
                        notes=["No supported chain schema discovered for context-graph audit run."],
                    )
                )
            )
            continue
        for time_range in ("1h", "24h"):
            resolved_time_range = resolve_budget_safe_time_range(
                chain,
                time_range,
                mode=CHAIN_QUERY_MODES.get(chain, "transfer_only"),
            )
            scope = {
                "chain": chain,
                "token": TOKEN,
                "requested_time_range": time_range,
                "resolved_time_range": resolved_time_range,
            }
            try:
                edge_sql = build_context_graph_query(
                    schema,
                    token_contract=TOKEN_CONTRACTS[chain],
                    protocol_registry=get_protocol_registry(chain),
                    bridge_registry=get_bridge_registry(chain),
                    time_range=resolved_time_range,
                    mode=CHAIN_QUERY_MODES.get(chain, "transfer_only"),
                )
                edge_frame = execute_sql(
                    edge_sql,
                    ttl_seconds=0,
                    use_cache=False,
                    query_name=f"audit_context_graph_live_parity_{chain.lower()}_{resolved_time_range}",
                    query_family="audit_context_graph_parity",
                    maximum_bytes_billed=EDGE_MAX_BYTES_PER_QUERY,
                    query_classification="dev_only",
                    enforce_validation=False,
                )
                current_snapshot = build_graph_snapshot(
                    edge_frame,
                    chain=chain,
                    token=TOKEN,
                    time_range=resolved_time_range,
                )
                legacy_grouped_frame = pd.DataFrame(_aggregate_edge_facts(edge_frame, token=TOKEN))
                legacy_snapshot = build_graph_snapshot(
                    legacy_grouped_frame,
                    chain=chain,
                    token=TOKEN,
                    time_range=resolved_time_range,
                )
                mismatches = _compare_payloads(current_snapshot, legacy_snapshot, fields=CONTEXT_GRAPH_FIELDS)
                notes = [
                    "Current snapshot built from live raw edge facts queried from BigQuery.",
                    "Legacy-equivalent snapshot reconstructed by grouping those facts before graph assembly.",
                ]
                if resolved_time_range != time_range:
                    notes.append(
                        f"Budget-safe fallback resolved requested {time_range} to {resolved_time_range} for {chain}."
                    )
                materialized = get_context_graph_summary(chain, token=TOKEN, time_range=resolved_time_range)
                if materialized is not None:
                    notes.append("Materialized snapshot exists for this sampled scope.")
                results.append(
                    asdict(
                        AuditCheckResult(
                            scope=scope,
                            fields_compared=list(CONTEXT_GRAPH_FIELDS),
                            mismatches=mismatches,
                            intentional_deltas=[
                                "Registry matching and entity labeling now occur in Python rather than SQL.",
                            ],
                            status="pass" if not mismatches else "fail",
                            notes=notes,
                        )
                    )
                )
            except Exception as exc:
                results.append(
                    asdict(
                        AuditCheckResult(
                            scope=scope,
                            fields_compared=list(CONTEXT_GRAPH_FIELDS),
                            mismatches=[{"field": "runtime_error", "expected": "none", "actual": str(exc)}],
                            intentional_deltas=[
                                "Registry matching and entity labeling now occur in Python rather than SQL.",
                            ],
                            status="fail",
                            notes=[
                                "Context-graph live parity could not complete for this sampled scope.",
                            ],
                        )
                    )
                )
    passed = all(item["status"] == "pass" for item in results)
    return {
        "status": "pass" if passed else "fail",
        "sampled_entities": results,
        "notes": [
            "Context-graph parity uses live BigQuery edge extraction in non-request-path audit mode.",
            "The legacy comparator reconstructs grouped-edge semantics from the same raw fact rows.",
        ],
    }


def build_audit_report() -> dict[str, Any]:
    generated_at = _utcnow_iso()
    corridor = run_corridor_live_parity()
    context_graph = run_context_graph_live_parity()
    overall_status = "pass" if corridor["status"] == "pass" and context_graph["status"] == "pass" else "fail"
    return {
        "generated_at": generated_at,
        "status": overall_status,
        "checks": {
            "corridor_live_parity": corridor,
            "context_graph_live_parity": context_graph,
        },
    }


def _markdown_for_check(name: str, payload: dict[str, Any]) -> list[str]:
    lines = [f"## {name}", "", f"Status: **{payload['status'].upper()}**", ""]
    for note in payload.get("notes", []):
        lines.append(f"- {note}")
    if payload.get("notes"):
        lines.append("")
    for item in payload.get("sampled_entities", []):
        lines.append(f"### Scope: `{json.dumps(item['scope'], sort_keys=True)}`")
        lines.append(f"- Status: `{item['status']}`")
        lines.append(f"- Fields compared: `{', '.join(item['fields_compared'])}`")
        if item.get("intentional_deltas"):
            lines.append(f"- Intentional deltas: `{'; '.join(item['intentional_deltas'])}`")
        if item.get("notes"):
            lines.append(f"- Notes: `{'; '.join(item['notes'])}`")
        if item["mismatches"]:
            lines.append("- Mismatches:")
            for mismatch in item["mismatches"]:
                lines.append(
                    f"  - `{mismatch['field']}` expected `{json.dumps(mismatch['expected'], sort_keys=True)}` "
                    f"got `{json.dumps(mismatch['actual'], sort_keys=True)}`"
                )
        else:
            lines.append("- Mismatches: none")
        lines.append("")
    return lines


def write_audit_report(report: dict[str, Any], *, output_dir: Optional[Path] = None) -> dict[str, str]:
    target_dir = _ensure_output_dir(output_dir)
    json_path = target_dir / "bigquery_live_audit_report.json"
    markdown_path = target_dir / "bigquery_live_audit_report.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")

    lines = [
        "# BigQuery Live Audit Report",
        "",
        f"Generated at: `{report['generated_at']}`",
        "",
        f"Overall status: **{report['status'].upper()}**",
        "",
    ]
    for name, payload in report["checks"].items():
        lines.extend(_markdown_for_check(name, payload))
    markdown_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return {"json_path": str(json_path), "markdown_path": str(markdown_path)}


def run_and_write_audit_report(*, output_dir: Optional[Path] = None) -> dict[str, Any]:
    report = build_audit_report()
    paths = write_audit_report(report, output_dir=output_dir)
    return {
        "report": report,
        "artifacts": paths,
    }
