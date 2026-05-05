"""Phase 16 — idempotent BigQuery promotion and table tooling tests."""

from __future__ import annotations

import importlib.util
from decimal import Decimal
from pathlib import Path

from services.solana.bigquery_writer import (
    BQ_SCHEMA,
    CLUSTERING_FIELDS,
    SolanaEventWriter,
    build_create_table_ddl,
    s3_readiness_field_report,
    validate_bq_schema_contract,
)
from services.solana.event_schema import normalize_event
from tests.solana.test_event_schema import _make_raw_event

ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = ROOT / "scripts" / "create_solana_bq_table.py"


def _load_create_script():
    spec = importlib.util.spec_from_file_location("create_solana_bq_table", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class TestCreateSolanaBQTableDDL:
    def test_script_uses_writer_bq_schema_object(self):
        script = _load_create_script()
        assert script.BQ_SCHEMA is BQ_SCHEMA

    def test_ddl_uses_range_bucket_slot_partitioning(self):
        ddl = build_create_table_ddl(project_id="demo-project")
        assert "PARTITION BY RANGE_BUCKET(`slot`, GENERATE_ARRAY(" in ddl
        assert "`slot` INTEGER" in ddl

    def test_ddl_clusters_by_required_fields(self):
        ddl = build_create_table_ddl(project_id="demo-project")
        assert CLUSTERING_FIELDS == ("token_mint", "watched_address", "raw_event_id")
        assert "CLUSTER BY `token_mint`, `watched_address`, `raw_event_id`" in ddl

    def test_create_table_dry_run_summary_does_not_execute_bq(self):
        script = _load_create_script()
        summary = script.create_table(
            project_id="demo-project",
            dataset="solana_measured",
            table="solana_transfers",
            dry_run=True,
        )
        assert summary["executed"] is False
        assert summary["schema_source"] == "services.solana.bigquery_writer.BQ_SCHEMA"
        assert summary["schema_contract_violations"] == []
        assert "CREATE TABLE IF NOT EXISTS `demo-project.solana_measured.solana_transfers`" in summary["ddl"]


class TestBQSchemaContract:
    def _schema(self):
        return {field["name"]: field["type"] for field in BQ_SCHEMA}

    def test_amount_fields_enforce_bignumeric_and_numeric(self):
        schema = self._schema()
        bignumeric_fields = {
            "amount_raw", "amount_transferred_raw", "fee_withheld_raw",
            "amount_received_raw", "fee_lamports", "native_base_fee_lamports",
            "native_priority_fee_lamports", "jito_tip_lamports", "explicit_tip_lamports",
            "total_native_observed_cost_lamports",
        }
        for field in bignumeric_fields:
            assert schema[field] == "BIGNUMERIC"
        assert schema["amount_decimal"] == "NUMERIC"
        assert "FLOAT64" not in set(schema.values())

    def test_schema_contract_has_no_violations(self):
        assert validate_bq_schema_contract() == []

    def test_s3_readiness_fields_present(self):
        report = s3_readiness_field_report()
        assert report["missing"] == []
        assert "watched_address" in report["present"]
        assert "normalized_event_id" in report["present"]

    def test_env_example_has_phase16_solana_bq_vars(self):
        env_text = (ROOT / ".env.example").read_text(encoding="utf-8")
        for key in (
            "SOLANA_BQ_DATASET",
            "SOLANA_BQ_TABLE",
            "SOLANA_CHECKPOINT_BACKEND",
            "SOLANA_COMMITMENT",
            "SOLANA_TOKEN_MINT",
        ):
            assert f"{key}=" in env_text


class TestMergeIdempotency:
    def test_merge_sql_keyed_only_on_normalized_event_id(self, tmp_path):
        client = InMemoryMergeBQClient()
        writer = SolanaEventWriter(bq_client=client, fallback_path=str(tmp_path / "buf.jsonl"))
        event = normalize_event(_make_raw_event(), ingested_at="2026-05-05T00:00:00+00:00")

        result = writer.write_batch([event])

        assert result.success is True
        assert len(client.queries) == 1
        sql = client.queries[0]
        assert "ON target.normalized_event_id = source.normalized_event_id" in sql
        on_line = next(line.strip() for line in sql.splitlines() if line.strip().startswith("ON "))
        assert on_line == "ON target.normalized_event_id = source.normalized_event_id"

    def test_same_batch_twice_produces_no_duplicate_rows(self, tmp_path):
        client = InMemoryMergeBQClient()
        writer = SolanaEventWriter(bq_client=client, fallback_path=str(tmp_path / "buf.jsonl"))
        event = normalize_event(_make_raw_event(), ingested_at="2026-05-05T00:00:00+00:00")
        event["amount_decimal"] = Decimal("1.000000")

        first = writer.write_batch([event])
        second = writer.write_batch([event])

        assert first.success is True
        assert second.success is True
        assert len(client.target_rows) == 1
        assert event["normalized_event_id"] in client.target_rows


class _DoneJob:
    def result(self):
        return None


class InMemoryMergeBQClient:
    def __init__(self):
        self.loaded_staging_rows = []
        self.queries = []
        self.deleted = []
        self.target_rows = {}

    def load_table_from_json(self, rows, table_ref, job_config=None):
        self.loaded_staging_rows = list(rows)
        self.last_staging_table = table_ref
        return _DoneJob()

    def query(self, sql):
        self.queries.append(sql)
        for row in self.loaded_staging_rows:
            self.target_rows[row["normalized_event_id"]] = row
        return _DoneJob()

    def delete_table(self, table_ref, not_found_ok=True):
        self.deleted.append((table_ref, not_found_ok))
