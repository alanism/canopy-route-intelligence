#!/usr/bin/env python3
"""Run Phase 16.5 internal-only shadow S3 validation queries."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.query_validator import validate_query
from services.solana.shadow_validation import (
    SHADOW_MAX_BYTES_BILLED,
    SHADOW_QUERY_FAMILY,
    SHADOW_QUERY_NAMES,
    ShadowQueryDefinition,
    build_slot_bounds_sql,
    cold_start_window_status,
    resolve_shadow_target,
    shadow_missing_fields,
    shadow_query_definitions,
)


def _gcloud_value(key: str) -> str:
    try:
        completed = subprocess.run(
            ["gcloud", "config", "get-value", key],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return "unavailable"
    value = (completed.stdout or completed.stderr).strip()
    if not value:
        return "unset"
    return value


def _run_json_command(command: list[str]) -> Any:
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        details = stderr or stdout or f"exit code {completed.returncode}"
        raise RuntimeError(f"Command failed: {' '.join(command)} :: {details}")
    return json.loads(completed.stdout)


def _table_schema_fields(table_path: str) -> set[str]:
    project_id, dataset, table = table_path.split(".", 2)
    bq_table_ref = f"{project_id}:{dataset}.{table}"
    payload = _run_json_command(["bq", "show", "--schema", "--format=prettyjson", bq_table_ref])
    return {field["name"] for field in payload}


def _dry_run_bytes(sql: str) -> int:
    payload = _run_json_command(
        [
            "bq",
            "query",
            "--use_legacy_sql=false",
            "--dry_run",
            "--format=prettyjson",
            f"--maximum_bytes_billed={SHADOW_MAX_BYTES_BILLED}",
            sql,
        ]
    )
    stats = payload.get("statistics", {})
    query_stats = stats.get("query", {})
    total = query_stats.get("totalBytesProcessed", payload.get("totalBytesProcessed", 0))
    return int(total or 0)


def _execute_rows(sql: str) -> list[dict[str, Any]]:
    payload = _run_json_command(
        [
            "bq",
            "query",
            "--use_legacy_sql=false",
            "--format=prettyjson",
            f"--maximum_bytes_billed={SHADOW_MAX_BYTES_BILLED}",
            sql,
        ]
    )
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and "rows" in payload:
        return payload["rows"]
    return [payload]


def _validate_shadow_query(definition: ShadowQueryDefinition) -> None:
    issues = validate_query(
        definition.sql,
        classification=definition.classification,
        query_name=definition.name,
        query_family=definition.query_family,
        maximum_bytes_billed=definition.maximum_bytes_billed,
        request_scoped=False,
    )
    if issues:
        details = "; ".join(issue.message for issue in issues)
        raise ValueError(f"Shadow query validation failed for {definition.name}: {details}")


def _print_text_report(report: dict[str, Any]) -> None:
    header = report["header"]
    print(f"Active gcloud account: {header['active_account']}")
    print(f"Active gcloud project: {header['active_project']}")
    print(f"Target dataset.table: {header['target_table']}")
    print(f"Slot bounds: {header['slot_min']}..{header['slot_max']}")
    print(f"7-day success purity unavailable due to cold start: {header['seven_day_unavailable_due_to_cold_start']}")
    print(f"Missing fields: {', '.join(header['missing_fields']) if header['missing_fields'] else 'None'}")
    print("")
    for query in report["queries"]:
        print(f"Query: {query['name']}")
        print(f"Maximum bytes billed: {query['maximum_bytes_billed']}")
        print(f"Dry-run bytes: {query['dry_run_bytes']}")
        print("Exact SQL:")
        print(query["sql"])
        print("Result rows:")
        print(json.dumps(query["rows"], default=str, indent=2, sort_keys=True))
        print("")


def run_shadow_validation(
    *,
    project_id: str | None = None,
    dataset: str | None = None,
    table: str | None = None,
    slot_min: int | None = None,
    slot_max: int | None = None,
) -> dict[str, Any]:
    target = resolve_shadow_target(project_id=project_id, dataset=dataset, table=table)
    schema_fields = _table_schema_fields(target.table_path)
    missing_fields = shadow_missing_fields(schema_fields)

    bounds_sql = build_slot_bounds_sql(target)
    bounds_dry_run_bytes = _dry_run_bytes(bounds_sql)
    bounds_rows = _execute_rows(bounds_sql)
    bounds_row = bounds_rows[0] if bounds_rows else {}
    derived_slot_min = slot_min if slot_min is not None else bounds_row.get("slot_min")
    derived_slot_max = slot_max if slot_max is not None else bounds_row.get("slot_max")
    if derived_slot_min is None or derived_slot_max is None:
        raise ValueError("Unable to resolve slot bounds from the sandbox table.")
    unavailable_due_to_cold_start, cold_start_window_seconds = cold_start_window_status(bounds_row)

    definitions = shadow_query_definitions(
        target=target,
        slot_min=int(derived_slot_min),
        slot_max=int(derived_slot_max),
        unavailable_due_to_cold_start=unavailable_due_to_cold_start,
        cold_start_window_seconds=cold_start_window_seconds,
        present_fields=schema_fields,
    )
    queries: list[dict[str, Any]] = []
    for definition in definitions:
        _validate_shadow_query(definition)
        dry_run_bytes = _dry_run_bytes(definition.sql)
        rows = _execute_rows(definition.sql)
        queries.append(
            {
                "name": definition.name,
                "sql": definition.sql,
                "dry_run_bytes": dry_run_bytes,
                "rows": rows,
                "maximum_bytes_billed": definition.maximum_bytes_billed,
            }
        )

    return {
        "header": {
            "active_account": _gcloud_value("core/account"),
            "active_project": _gcloud_value("core/project"),
            "target_table": target.table_path,
            "slot_min": int(derived_slot_min),
            "slot_max": int(derived_slot_max),
            "seven_day_unavailable_due_to_cold_start": unavailable_due_to_cold_start,
            "cold_start_window_seconds": cold_start_window_seconds,
            "missing_fields": missing_fields,
            "bounds_sql": bounds_sql,
            "bounds_dry_run_bytes": bounds_dry_run_bytes,
            "bounds_rows": bounds_rows,
            "query_family": SHADOW_QUERY_FAMILY,
        },
        "queries": queries,
        "query_names": list(SHADOW_QUERY_NAMES),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Phase 16.5 shadow S3 validation queries.")
    parser.add_argument("--project-id", default=None)
    parser.add_argument("--dataset", default=None)
    parser.add_argument("--table", default=None)
    parser.add_argument("--slot-min", type=int, default=None)
    parser.add_argument("--slot-max", type=int, default=None)
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output.")
    args = parser.parse_args()

    report = run_shadow_validation(
        project_id=args.project_id,
        dataset=args.dataset,
        table=args.table,
        slot_min=args.slot_min,
        slot_max=args.slot_max,
    )
    if args.json:
        print(json.dumps(report, default=str, indent=2, sort_keys=True))
    else:
        _print_text_report(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
