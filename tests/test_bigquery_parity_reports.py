import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from services.context_graph.graph_builder import build_graph_snapshot
from services.corridor_analytics import build_corridor_base_summary
from services import summary_store


class CorridorParityTests(unittest.TestCase):
    def test_materialized_corridor_summary_preserves_business_metrics(self):
        with tempfile.TemporaryDirectory() as tmpdir, patch.object(
            summary_store,
            "DEFAULT_DB_PATH",
            Path(tmpdir) / "canopy_summary.db",
        ), patch(
            "services.corridor_analytics.check_bridge_solvency",
            return_value={
                "bridge_name": "Circle CCTP",
                "solvency_ratio": 0.98,
                "buffer_usd": 2_500_000.0,
                "alert_level": "normal",
            },
        ):
            summary_store.init_summary_store()
            summary = build_corridor_base_summary(
                "US-PH",
                {
                    "rail": "Ethereum",
                    "mode": "live_measured",
                    "adjusted_volume_usdc": 125_000.0,
                    "volume_usdc": 125_000.0,
                    "adjusted_transfer_count": 840,
                    "transfer_count": 840,
                    "confidence": 0.88,
                    "freshness_score": 0.9,
                },
                rail="Ethereum",
                token="USDC",
                time_range="24h",
                allow_live_bigquery=False,
            )

            summary_store.upsert_corridor_summary(
                [{**summary, "materialized_at": "2026-03-21T00:00:00+00:00"}]
            )
            stored = summary_store.get_corridor_summary(
                "US-PH",
                "Ethereum",
                token="USDC",
                time_range="24h",
            )

        self.assertIsNotNone(stored)
        parity_keys = [
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
        ]
        self.assertEqual(
            {key: stored[key] for key in parity_keys},
            {key: summary[key] for key in parity_keys},
        )
        self.assertEqual(stored["data_layer"], "derived")
        self.assertEqual(stored["serving_path"], "deterministic_fallback")


class ContextGraphParityTests(unittest.TestCase):
    def test_raw_fact_aggregation_matches_legacy_grouped_edge_snapshot(self):
        legacy_grouped_edges = pd.DataFrame(
            [
                {
                    "source_node": "Coinbase",
                    "destination_node": "Circle CCTP",
                    "source_type": "exchange",
                    "destination_type": "bridge",
                    "edge_type": "protocol_protocol",
                    "token": "USDC",
                    "total_volume": 4200.0,
                    "transaction_count": 2,
                    "last_seen": "2026-03-15T00:02:00+00:00",
                    "avg_gas_fee": 0.002,
                    "sample_transaction_hash": "0xtx1",
                    "evidence_type": "log,trace",
                },
                {
                    "source_node": "Circle CCTP",
                    "destination_node": "Coins.ph",
                    "source_type": "bridge",
                    "destination_type": "exchange",
                    "edge_type": "protocol_protocol",
                    "token": "USDC",
                    "total_volume": 3900.0,
                    "transaction_count": 1,
                    "last_seen": "2026-03-15T00:03:00+00:00",
                    "avg_gas_fee": 0.0015,
                    "sample_transaction_hash": "0xtx3",
                    "evidence_type": "log,trace",
                },
            ]
        )
        raw_edge_facts = pd.DataFrame(
            [
                {
                    "source_node": "Coinbase",
                    "destination_node": "Circle CCTP",
                    "source_type": "exchange",
                    "destination_type": "bridge",
                    "edge_type": "protocol_protocol",
                    "token": "USDC",
                    "transaction_hash": "0xtx1",
                    "fact_volume": 2000.0,
                    "block_timestamp": "2026-03-15T00:00:00+00:00",
                    "gas_fee_native": 0.002,
                    "evidence_type": "log",
                },
                {
                    "source_node": "Coinbase",
                    "destination_node": "Circle CCTP",
                    "source_type": "exchange",
                    "destination_type": "bridge",
                    "edge_type": "protocol_protocol",
                    "token": "USDC",
                    "transaction_hash": "0xtx2",
                    "fact_volume": 2200.0,
                    "block_timestamp": "2026-03-15T00:02:00+00:00",
                    "gas_fee_native": 0.002,
                    "evidence_type": "trace",
                },
                {
                    "source_node": "Circle CCTP",
                    "destination_node": "Coins.ph",
                    "source_type": "bridge",
                    "destination_type": "exchange",
                    "edge_type": "protocol_protocol",
                    "token": "USDC",
                    "transaction_hash": "0xtx3",
                    "fact_volume": 3900.0,
                    "block_timestamp": "2026-03-15T00:03:00+00:00",
                    "gas_fee_native": 0.0015,
                    "evidence_type": "log,trace",
                },
            ]
        )

        legacy_snapshot = build_graph_snapshot(
            legacy_grouped_edges,
            chain="Ethereum",
            token="USDC",
            time_range="24h",
            gap_seconds=4200,
            generated_at="2026-03-15T00:10:00+00:00",
        )
        current_snapshot = build_graph_snapshot(
            raw_edge_facts,
            chain="Ethereum",
            token="USDC",
            time_range="24h",
            gap_seconds=4200,
            generated_at="2026-03-15T00:10:00+00:00",
        )

        parity_keys = [
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
        ]
        self.assertEqual(
            {key: current_snapshot[key] for key in parity_keys},
            {key: legacy_snapshot[key] for key in parity_keys},
        )


if __name__ == "__main__":
    unittest.main()
