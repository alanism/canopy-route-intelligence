"""In-memory BigQuery query metrics for drift monitoring."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Optional

_lock = Lock()
_started_at = datetime.now(timezone.utc).isoformat()
_overall: dict[str, Any] = {
    "dry_run_count": 0,
    "execution_count": 0,
    "dry_run_bytes": 0,
    "execution_bytes": 0,
}
_families: dict[str, dict[str, Any]] = defaultdict(
    lambda: {
        "query_count": 0,
        "dry_run_count": 0,
        "execution_count": 0,
        "total_bytes": 0,
        "dry_run_bytes": 0,
        "execution_bytes": 0,
        "max_bytes": 0,
        "total_execution_time": 0.0,
        "avg_execution_time": 0.0,
        "last_query_name": None,
        "last_classification": None,
        "last_seen": None,
        "max_budget_utilization": 0.0,
    }
)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _infer_family(query_name: str, query_classification: str) -> str:
    name = str(query_name or "unnamed_query").lower()
    if name.startswith(("measured_fee_extraction_", "fee_activity_")):
        return "fee_activity"
    if name.startswith("measured_corridor_extraction_"):
        return "corridor_volume"
    if name.startswith("context_graph_edges_") or "context_graph_edges" in name:
        return "context_graph_edges"
    if name.startswith("context_graph_gap_") or "context_graph_gap" in name:
        return "context_graph_gap"
    if name.startswith("context_graph_"):
        return "context_graph"
    if query_classification == "measured":
        return "measured_other"
    if query_classification == "derived":
        return "derived_other"
    return "dev_only"


def record_query_metric(
    *,
    phase: str,
    query_name: str,
    query_family: Optional[str] = None,
    query_classification: str,
    bytes_processed: int,
    maximum_bytes_billed: Optional[int] = None,
    execution_time: Optional[float] = None,
) -> None:
    family = query_family or _infer_family(query_name, query_classification)
    budget_utilization = 0.0
    if maximum_bytes_billed and maximum_bytes_billed > 0:
        budget_utilization = round(bytes_processed / maximum_bytes_billed, 6)

    with _lock:
        family_metrics = _families[family]
        family_metrics["query_count"] += 1
        family_metrics["total_bytes"] += int(bytes_processed or 0)
        family_metrics["max_bytes"] = max(family_metrics["max_bytes"], int(bytes_processed or 0))
        family_metrics["last_query_name"] = query_name
        family_metrics["last_classification"] = query_classification
        family_metrics["last_seen"] = _utcnow_iso()
        family_metrics["max_budget_utilization"] = max(
            family_metrics["max_budget_utilization"],
            budget_utilization,
        )

        if phase == "dry_run":
            _overall["dry_run_count"] += 1
            _overall["dry_run_bytes"] += int(bytes_processed or 0)
            family_metrics["dry_run_count"] += 1
            family_metrics["dry_run_bytes"] += int(bytes_processed or 0)
        else:
            _overall["execution_count"] += 1
            _overall["execution_bytes"] += int(bytes_processed or 0)
            family_metrics["execution_count"] += 1
            family_metrics["execution_bytes"] += int(bytes_processed or 0)
            if execution_time is not None:
                family_metrics["total_execution_time"] += float(execution_time)
                family_metrics["avg_execution_time"] = round(
                    family_metrics["total_execution_time"] / max(family_metrics["execution_count"], 1),
                    3,
                )


def get_query_metrics_snapshot() -> dict[str, Any]:
    with _lock:
        families = {}
        for family, metrics in _families.items():
            families[family] = {
                **metrics,
                "avg_bytes_per_query": round(
                    metrics["total_bytes"] / max(metrics["query_count"], 1),
                    2,
                ),
            }
        return {
            "status": "ok",
            "started_at": _started_at,
            "overall": {
                **_overall,
                "family_count": len(families),
            },
            "families": dict(sorted(families.items())),
        }


def reset_query_metrics() -> None:
    global _started_at
    with _lock:
        _overall["dry_run_count"] = 0
        _overall["execution_count"] = 0
        _overall["dry_run_bytes"] = 0
        _overall["execution_bytes"] = 0
        _families.clear()
        _started_at = _utcnow_iso()
