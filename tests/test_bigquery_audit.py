import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from services.bigquery_audit import (
    build_audit_report,
    run_context_graph_live_parity,
    run_corridor_live_parity,
    write_audit_report,
)


class BigQueryAuditTests(unittest.TestCase):
    @patch("services.bigquery_audit.build_corridor_base_summary")
    @patch("services.bigquery_audit.get_corridor_summary")
    @patch("services.bigquery_audit.get_corridors")
    @patch("services.bigquery_audit.route_cache.get_cache")
    @patch("services.bigquery_audit.route_cache._materialize_corridor_summaries")
    @patch("services.bigquery_audit.route_cache._rail_seed_from_cache")
    def test_corridor_live_parity_report_passes_for_matching_values(
        self,
        seed_mock,
        materialize_mock,
        cache_mock,
        corridors_mock,
        summary_mock,
        recompute_mock,
    ):
        cache_mock.return_value = {"chains": {}}
        seed_mock.return_value = {"rail": "Ethereum"}
        corridors_mock.return_value = [{"key": "US-PH"}, {"key": "US-BR"}]
        matching = {
            "volume_24h": 100.0,
            "volume_7d": 700.0,
            "tx_count": 40,
            "unique_senders": 10,
            "unique_receivers": 8,
            "velocity_unique_capital": 0.5,
            "concentration_score": 0.2,
            "bridge_name": "Circle CCTP",
            "bridge_share": 0.3,
            "bridge_volume": 30.0,
            "bridge_transactions": 12,
            "whale_threshold_usd": 5000,
            "whale_activity_score": 0.4,
            "net_flow_7d": 50.0,
            "top_whale_flows": [],
            "source": "derived_deterministic_profile",
            "data_layer": "derived",
            "serving_path": "deterministic_fallback",
        }
        summary_mock.return_value = dict(matching)
        recompute_mock.return_value = dict(matching)

        result = run_corridor_live_parity()

        self.assertEqual(result["status"], "pass")
        self.assertTrue(all(item["status"] == "pass" for item in result["sampled_entities"]))

    @patch("services.bigquery_audit.get_context_graph_summary")
    @patch("services.bigquery_audit.execute_sql")
    @patch("services.bigquery_audit.discover_supported_schemas")
    def test_context_graph_live_parity_report_passes_for_matching_snapshots(
        self,
        schemas_mock,
        execute_sql_mock,
        materialized_mock,
    ):
        schemas_mock.return_value = {
            "Ethereum": SimpleNamespace(),
            "Polygon": SimpleNamespace(),
        }
        execute_sql_mock.return_value = pd.DataFrame(
            [
                {
                    "source_node": "0x111",
                    "destination_node": "0x222",
                    "source_type": "wallet",
                    "destination_type": "contract",
                    "edge_type": "wallet_contract",
                    "token": "USDC",
                    "transaction_hash": "0xtx1",
                    "fact_volume": 10.0,
                    "last_seen": "2026-03-20T00:00:00+00:00",
                    "gas_fee_native": 0.001,
                    "evidence_type": "address",
                }
            ]
        )
        materialized_mock.return_value = {"status": "ok"}

        with patch("services.bigquery_audit.build_context_graph_query", return_value="SELECT 1"), patch(
            "services.bigquery_audit.get_protocol_registry",
            return_value=[],
        ), patch(
            "services.bigquery_audit.get_bridge_registry",
            return_value=[],
        ), patch(
            "services.bigquery_audit.resolve_budget_safe_time_range",
            side_effect=lambda chain, time_range, mode="transfer_only": "1h" if chain == "Polygon" and time_range == "24h" else time_range,
        ):
            result = run_context_graph_live_parity()

        self.assertEqual(result["status"], "pass")
        self.assertTrue(all(item["status"] == "pass" for item in result["sampled_entities"]))
        polygon_24h = next(
            item for item in result["sampled_entities"]
            if item["scope"]["chain"] == "Polygon" and item["scope"]["requested_time_range"] == "24h"
        )
        self.assertEqual(polygon_24h["scope"]["resolved_time_range"], "1h")

    def test_write_audit_report_writes_json_and_markdown(self):
        report = {
            "generated_at": "2026-03-21T00:00:00+00:00",
            "status": "pass",
            "checks": {
                "corridor_live_parity": {
                    "status": "pass",
                    "notes": ["ok"],
                    "sampled_entities": [
                        {
                            "scope": {"corridor_id": "US-PH"},
                            "fields_compared": ["volume_24h"],
                            "mismatches": [],
                            "intentional_deltas": [],
                            "status": "pass",
                            "notes": ["match"],
                        }
                    ],
                }
            },
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = write_audit_report(report, output_dir=Path(tmpdir))
            self.assertTrue(Path(paths["json_path"]).exists())
            self.assertTrue(Path(paths["markdown_path"]).exists())

    @patch("services.bigquery_audit.run_corridor_live_parity")
    @patch("services.bigquery_audit.run_context_graph_live_parity")
    def test_build_audit_report_fails_when_a_subcheck_fails(self, context_mock, corridor_mock):
        corridor_mock.return_value = {"status": "pass", "sampled_entities": [], "notes": []}
        context_mock.return_value = {"status": "fail", "sampled_entities": [], "notes": []}

        report = build_audit_report()

        self.assertEqual(report["status"], "fail")


if __name__ == "__main__":
    unittest.main()
