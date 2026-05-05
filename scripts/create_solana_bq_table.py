#!/usr/bin/env python3
"""Create the Solana measured BigQuery table from services.solana.bigquery_writer.BQ_SCHEMA."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.solana.bigquery_writer import (
    BQ_SCHEMA,
    CLUSTERING_FIELDS,
    DATASET,
    TABLE,
    build_create_table_ddl,
    s3_readiness_field_report,
    validate_bq_schema_contract,
)


def _project_id(explicit: str | None = None) -> str:
    project = explicit or os.environ.get("GCP_PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project:
        raise ValueError("Set GCP_PROJECT_ID or pass --project-id before creating Solana BigQuery table.")
    return project


def build_summary(*, project_id: str, dataset: str, table: str) -> dict:
    report = s3_readiness_field_report()
    return {
        "table": f"{project_id}.{dataset}.{table}",
        "schema_source": "services.solana.bigquery_writer.BQ_SCHEMA",
        "field_count": len(BQ_SCHEMA),
        "partitioning": "RANGE_BUCKET(slot, GENERATE_ARRAY(...))",
        "clustering": list(CLUSTERING_FIELDS),
        "schema_contract_violations": validate_bq_schema_contract(),
        "s3_readiness_present_count": len(report["present"]),
        "s3_readiness_missing": report["missing"],
    }


def create_table(*, project_id: str, dataset: str, table: str, dry_run: bool = True) -> dict:
    violations = validate_bq_schema_contract()
    if violations:
        raise ValueError("Solana BQ schema contract violations: " + "; ".join(violations))

    ddl = build_create_table_ddl(project_id=project_id, dataset=dataset, table=table)
    summary = build_summary(project_id=project_id, dataset=dataset, table=table)
    summary["ddl"] = ddl

    if dry_run:
        summary["executed"] = False
        return summary

    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    job = client.query(ddl)
    job.result()
    summary["executed"] = True
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Create Solana measured BigQuery table from BQ_SCHEMA.")
    parser.add_argument("--project-id", default=None, help="GCP project id. Defaults to GCP_PROJECT_ID.")
    parser.add_argument("--dataset", default=os.environ.get("SOLANA_BQ_DATASET", DATASET))
    parser.add_argument("--table", default=os.environ.get("SOLANA_BQ_TABLE", TABLE))
    parser.add_argument("--execute", action="store_true", help="Execute DDL against BigQuery. Default is dry-run summary only.")
    parser.add_argument("--print-ddl", action="store_true", help="Print DDL after the JSON summary.")
    args = parser.parse_args()

    summary = create_table(
        project_id=_project_id(args.project_id),
        dataset=args.dataset,
        table=args.table,
        dry_run=not args.execute,
    )
    ddl = summary.pop("ddl")
    print(json.dumps(summary, sort_keys=True))
    if args.print_ddl:
        print(ddl)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
