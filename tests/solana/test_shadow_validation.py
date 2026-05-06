"""Phase 16.5 shadow S3 validation tests."""

from __future__ import annotations

import importlib.util
from datetime import datetime, timezone
from pathlib import Path

import pytest

from services.query_validator import validate_query
from services.solana.bigquery_writer import S3_READINESS_FIELDS
from services.solana.shadow_validation import (
    SHADOW_DATASET,
    SHADOW_PROJECT_ID,
    SHADOW_QUERY_NAMES,
    SHADOW_TABLE,
    ShadowTarget,
    ShadowValidationTargetError,
    cold_start_window_status,
    resolve_shadow_target,
    shadow_missing_fields,
    shadow_query_definitions,
)

ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = ROOT / "scripts" / "run_solana_shadow_validation.py"


def _load_script():
    spec = importlib.util.spec_from_file_location("run_solana_shadow_validation", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class TestShadowQueryDefinitions:
    def test_exact_shadow_query_names_are_defined(self):
        definitions = shadow_query_definitions(
            target=resolve_shadow_target(),
            slot_min=100,
            slot_max=200,
            unavailable_due_to_cold_start=True,
            cold_start_window_seconds=3600,
            present_fields=set(S3_READINESS_FIELDS),
        )

        assert tuple(definition.name for definition in definitions) == SHADOW_QUERY_NAMES
        assert all(definition.name.startswith("shadow_") for definition in definitions)

    def test_every_shadow_query_is_slot_bounded_and_exact_table_only(self):
        target = resolve_shadow_target()
        definitions = shadow_query_definitions(
            target=target,
            slot_min=111,
            slot_max=222,
            unavailable_due_to_cold_start=False,
            cold_start_window_seconds=900000,
            present_fields=set(S3_READINESS_FIELDS),
        )

        for definition in definitions:
            assert "slot BETWEEN 111 AND 222" in definition.sql
            assert target.table_path in definition.sql
            assert "*. " not in definition.sql
            assert "INFORMATION_SCHEMA" not in definition.sql
            assert definition.classification == "dev_only"

    def test_shadow_queries_validate_as_dev_only_non_request_scoped(self):
        definitions = shadow_query_definitions(
            target=resolve_shadow_target(),
            slot_min=1,
            slot_max=2,
            unavailable_due_to_cold_start=True,
            cold_start_window_seconds=None,
            present_fields=set(S3_READINESS_FIELDS),
        )

        for definition in definitions:
            issues = validate_query(
                definition.sql,
                classification=definition.classification,
                query_name=definition.name,
                query_family=definition.query_family,
                maximum_bytes_billed=definition.maximum_bytes_billed,
                request_scoped=False,
            )
            assert issues == []

    def test_shadow_missing_field_report_includes_every_s3_field(self):
        definitions = shadow_query_definitions(
            target=resolve_shadow_target(),
            slot_min=5,
            slot_max=6,
            unavailable_due_to_cold_start=True,
            cold_start_window_seconds=100,
            present_fields=set(S3_READINESS_FIELDS),
        )
        missing_report = next(definition for definition in definitions if definition.name == "shadow_missing_field_report")
        for field_name in sorted(S3_READINESS_FIELDS):
            assert f"'{field_name}' AS field_name" in missing_report.sql


class TestShadowTargetGuard:
    def test_resolve_shadow_target_defaults_to_exact_sandbox(self):
        target = resolve_shadow_target()
        assert target == ShadowTarget(
            project_id=SHADOW_PROJECT_ID,
            dataset=SHADOW_DATASET,
            table=SHADOW_TABLE,
        )

    @pytest.mark.parametrize("kwargs", [
        {"project_id": "other-project"},
        {"dataset": "other_dataset"},
        {"table": "other_table"},
    ])
    def test_resolve_shadow_target_rejects_non_sandbox_resources(self, kwargs):
        with pytest.raises(ShadowValidationTargetError):
            resolve_shadow_target(**kwargs)

    def test_shadow_missing_fields_reports_absent_schema_members(self):
        missing = shadow_missing_fields({"chain", "signature"})
        assert "normalized_event_id" in missing
        assert "chain" not in missing


class TestColdStartStatus:
    def test_cold_start_true_when_window_shorter_than_seven_days(self):
        unavailable, seconds = cold_start_window_status(
            {
                "min_block_time": datetime(2026, 5, 1, tzinfo=timezone.utc),
                "max_block_time": datetime(2026, 5, 2, tzinfo=timezone.utc),
            }
        )
        assert unavailable is True
        assert seconds == 86400

    def test_cold_start_false_when_window_is_seven_days_or_more(self):
        unavailable, seconds = cold_start_window_status(
            {
                "min_block_time": datetime(2026, 5, 1, tzinfo=timezone.utc),
                "max_block_time": datetime(2026, 5, 9, tzinfo=timezone.utc),
            }
        )
        assert unavailable is False
        assert seconds == 691200


class TestShadowRunner:
    def test_json_output_contains_expected_header_fields(self, monkeypatch):
        script = _load_script()

        monkeypatch.setattr(script, "_gcloud_value", lambda key: {"core/account": "alan@canopysystems.xyz", "core/project": "canopy-main"}[key])
        monkeypatch.setattr(script, "_table_schema_fields", lambda table_path: set(S3_READINESS_FIELDS))
        monkeypatch.setattr(script, "_dry_run_bytes", lambda sql: 1234)
        monkeypatch.setattr(
            script,
            "_execute_rows",
            lambda sql: (
                [{
                    "slot_min": 10,
                    "slot_max": 20,
                    "total_rows": 3,
                    "min_block_time": datetime(2026, 5, 5, tzinfo=timezone.utc),
                    "max_block_time": datetime(2026, 5, 5, 0, 5, tzinfo=timezone.utc),
                }]
                if "MIN(slot)" in sql else
                [{"shadow_query_name": "ok"}]
            ),
        )

        report = script.run_shadow_validation()

        assert report["header"]["active_account"] == "alan@canopysystems.xyz"
        assert report["header"]["active_project"] == "canopy-main"
        assert report["header"]["target_table"] == "canopy-main.solana_measured_sandbox.solana_transfers_phase16_test"
        assert report["header"]["seven_day_unavailable_due_to_cold_start"] is True
        assert report["header"]["missing_fields"] == []
        assert report["query_names"] == list(SHADOW_QUERY_NAMES)
