"""Phase 17 Solana corridor intelligence product-layer tests."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

from services.solana.corridor_intelligence import (
    CorridorIntelligenceError,
    build_corridor_intelligence_from_shadow_report,
    empty_corridor_intelligence,
    load_corridor_intelligence,
    write_corridor_intelligence,
)
from services.solana.shadow_validation import SHADOW_QUERY_NAMES

ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = ROOT / "scripts" / "materialize_solana_corridor_intelligence.py"


def _load_script():
    spec = importlib.util.spec_from_file_location("materialize_solana_corridor_intelligence", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _shadow_report(*, cold_start: bool = True, missing_fields: list[str] | None = None) -> dict:
    return {
        "header": {
            "target_table": "canopy-main.solana_measured_sandbox.solana_transfers_phase16_test",
            "slot_min": 417663784,
            "slot_max": 417663784,
            "cold_start_window_seconds": 0 if cold_start else 700000,
            "seven_day_unavailable_due_to_cold_start": cold_start,
            "missing_fields": missing_fields or [],
        },
        "query_names": list(SHADOW_QUERY_NAMES),
        "queries": [
            {
                "name": "shadow_success_purity",
                "rows": [{
                    "total_rows": "1",
                    "qualifying_rows": "1",
                    "success_rows": "1",
                    "success_purity_rate": "1.0",
                }],
            },
            {
                "name": "shadow_mev_protection_rate",
                "rows": [{
                    "protected_proxy_rows": "1",
                    "jito_tip_positive_rows": "1",
                    "mev_protection_proxy_rate": "1.0",
                }],
            },
            {
                "name": "shadow_settlement_velocity",
                "rows": [{
                    "latency_rows": "1",
                    "avg_latency_seconds": "1513.0",
                    "p50_latency_seconds": "1513",
                    "p95_latency_seconds": "1513",
                }],
            },
            {
                "name": "shadow_fee_efficiency",
                "rows": [{
                    "qualified_rows": "1",
                    "avg_transfer_per_lamport": "94273.0514",
                    "avg_total_native_observed_cost_lamports": "44362",
                }],
            },
            {
                "name": "shadow_missing_field_report",
                "rows": [
                    {"field_name": "source_owner", "null_count": "1", "null_rate": "1.0"},
                    {"field_name": "watched_address", "null_count": "1", "null_rate": "1.0"},
                ],
            },
        ],
    }


class TestCorridorIntelligenceBuild:
    def test_cold_start_shadow_report_becomes_evidence_limited_product_payload(self):
        payload = build_corridor_intelligence_from_shadow_report(_shadow_report(cold_start=True))

        assert payload["chain"] == "Solana"
        assert payload["status"] == "degraded"
        assert payload["signal_state"] == "cold_start"
        assert payload["claim_level"] == "evidence_limited"
        assert payload["quality_gates"]["request_path_bigquery_free"] is True
        assert payload["signals"]["success_purity"]["value"] == 1.0
        assert payload["signals"]["success_purity"]["seven_day_unavailable_due_to_cold_start"] is True
        assert "Seven-day success purity is unavailable" in payload["open_risks"][0]

    def test_ready_shadow_report_becomes_production_candidate(self):
        payload = build_corridor_intelligence_from_shadow_report(_shadow_report(cold_start=False))

        assert payload["status"] == "ready"
        assert payload["signal_state"] == "ready"
        assert payload["claim_level"] == "production_candidate"
        assert payload["open_risks"] == []

    def test_missing_s3_fields_block_ready_state(self):
        payload = build_corridor_intelligence_from_shadow_report(
            _shadow_report(cold_start=False, missing_fields=["watched_address"])
        )

        assert payload["signal_state"] == "schema_gap"
        assert payload["quality_gates"]["missing_fields_clear"] is False
        assert payload["missing_fields"] == ["watched_address"]

    def test_bad_shadow_report_is_rejected(self):
        report = _shadow_report()
        report["query_names"] = ["shadow_success_purity"]

        with pytest.raises(CorridorIntelligenceError):
            build_corridor_intelligence_from_shadow_report(report)

    def test_null_rate_findings_are_preserved(self):
        payload = build_corridor_intelligence_from_shadow_report(_shadow_report())

        fields = {finding["field_name"] for finding in payload["null_rate_findings"]}
        assert fields == {"source_owner", "watched_address"}


class TestCorridorIntelligenceArtifact:
    def test_missing_artifact_returns_unavailable_without_bigquery(self, tmp_path):
        payload = load_corridor_intelligence(path=tmp_path / "missing.json")

        assert payload == empty_corridor_intelligence()
        assert payload["quality_gates"]["request_path_bigquery_free"] is True

    def test_write_then_load_artifact_round_trips(self, tmp_path):
        payload = build_corridor_intelligence_from_shadow_report(_shadow_report())
        path = write_corridor_intelligence(payload, path=tmp_path / "artifact.json")

        loaded = load_corridor_intelligence(path=path)

        assert loaded["signal_state"] == payload["signal_state"]
        assert loaded["signals"]["fee_efficiency"]["qualified_rows"] == 1

    def test_materializer_script_can_use_saved_shadow_report(self, tmp_path, monkeypatch, capsys):
        script = _load_script()
        report_path = tmp_path / "shadow.json"
        output_path = tmp_path / "solana_corridor_intelligence.json"
        report_path.write_text(json.dumps(_shadow_report()), encoding="utf-8")
        monkeypatch.setattr(
            "sys.argv",
            [
                "materialize_solana_corridor_intelligence.py",
                "--from-shadow-json",
                str(report_path),
                "--output",
                str(output_path),
            ],
        )

        code = script.main()
        summary = json.loads(capsys.readouterr().out)

        assert code == 0
        assert output_path.exists()
        assert summary["signal_state"] == "cold_start"
        assert summary["output_path"] == str(output_path)
