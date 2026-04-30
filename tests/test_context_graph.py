import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from fastapi import HTTPException

from api import main as main_api
from services.context_graph.classifier import classify_signals
from services.context_graph.graph_builder import build_graph_snapshot
from services.context_graph.queries import build_context_graph_query, resolve_budget_safe_time_range
from services.context_graph.registries import match_bridge_address, match_protocol_address
from services.context_graph.schema import discover_chain_schema
from services.summary_store import get_context_graph_summary, init_summary_store, upsert_context_graph_summary


class SchemaDiscoveryTests(unittest.TestCase):
    def test_discover_chain_schema_handles_ethereum_receipt_fields(self):
        schemas = {
            "bigquery-public-data.goog_blockchain_ethereum_mainnet_us.transactions": {
                "hash",
                "receipt_effective_gas_price",
                "receipt_gas_used",
            },
            "bigquery-public-data.goog_blockchain_ethereum_mainnet_us.logs": {
                "transaction_hash",
                "address",
            },
            "bigquery-public-data.goog_blockchain_ethereum_mainnet_us.traces": {
                "transaction_hash",
                "to_address",
                "from_address",
            },
            "bigquery-public-data.goog_blockchain_ethereum_mainnet_us.token_transfers": {
                "transaction_hash",
            },
        }

        class FakeClient:
            def get_table(self, table_id):
                return SimpleNamespace(
                    schema=[SimpleNamespace(name=name) for name in schemas[table_id]]
                )

        schema = discover_chain_schema("Ethereum", client=FakeClient())
        self.assertEqual(schema.transfer_source, "token_transfers")
        self.assertEqual(schema.transfer_contract_field, "address")
        self.assertEqual(schema.transfer_value_field, "quantity")
        self.assertEqual(schema.receipt_gas_price_field, "effective_gas_price")
        self.assertEqual(schema.gas_used_field, "gas_used")
        self.assertEqual(schema.logs_address_field, "address")

    def test_discover_chain_schema_handles_polygon_gas_used(self):
        schemas = {
            "bigquery-public-data.goog_blockchain_polygon_mainnet_us.transactions": {
                "hash",
                "gas_price",
                "gas_used",
            },
            "bigquery-public-data.goog_blockchain_polygon_mainnet_us.logs": {
                "transaction_hash",
                "contract_address",
            },
            "bigquery-public-data.goog_blockchain_polygon_mainnet_us.traces": {
                "transaction_hash",
                "to_address",
                "from_address",
            },
            "bigquery-public-data.goog_blockchain_polygon_mainnet_us.token_transfers": {
                "transaction_hash",
            },
        }

        class FakeClient:
            def get_table(self, table_id):
                return SimpleNamespace(
                    schema=[SimpleNamespace(name=name) for name in schemas[table_id]]
                )

        schema = discover_chain_schema("Polygon", client=FakeClient())
        self.assertEqual(schema.transfer_source, "decoded_events")
        self.assertEqual(schema.transfer_contract_field, "address")
        self.assertEqual(schema.transfer_value_field, "args")
        self.assertEqual(schema.receipt_gas_price_field, "effective_gas_price")
        self.assertEqual(schema.gas_used_field, "gas_used")
        self.assertEqual(schema.logs_address_field, "address")


class QueryBuilderTests(unittest.TestCase):
    def test_polygon_transfer_only_resolves_to_budget_safe_window(self):
        self.assertEqual(
            resolve_budget_safe_time_range("Polygon", "24h", mode="transfer_only"),
            "1h",
        )
        self.assertEqual(
            resolve_budget_safe_time_range("Ethereum", "24h", mode="transfer_only"),
            "24h",
        )

    def test_context_graph_query_uses_filtered_ctes_before_joins(self):
        schema = SimpleNamespace(
            token_transfers_table="dataset.token_transfers",
            transfer_table="dataset.token_transfers",
            transfer_source="token_transfers",
            transactions_table="dataset.transactions",
            receipts_table="dataset.receipts",
            logs_table="dataset.logs",
            traces_table="dataset.traces",
            transfer_contract_field="address",
            transfer_value_field="quantity",
            transfer_transaction_hash_field="transaction_hash",
            transactions_hash_field="transaction_hash",
            receipts_transaction_hash_field="transaction_hash",
            receipt_gas_price_field="effective_gas_price",
            gas_used_field="gas_used",
            logs_address_field="address",
            logs_transaction_hash_field="transaction_hash",
            traces_to_address_field="to_address",
            traces_from_address_field="from_address",
            traces_transaction_hash_field="transaction_hash",
        )
        query = build_context_graph_query(
            schema,
            token_contract="0xabc",
            protocol_registry=[
                {
                    "contract_address": "0x111",
                    "protocol_name": "Uniswap V2 Router",
                    "protocol_type": "dex",
                }
            ],
            bridge_registry=[
                {
                    "contract_address": "0x222",
                    "bridge_name": "Wormhole Token Bridge",
                    "bridge_type": "bridge",
                }
            ],
            time_range="24h",
        )

        self.assertIn("filtered_transfers AS", query)
        self.assertIn("tracked_transfer_hashes AS", query)
        self.assertIn("filtered_transactions AS", query)
        self.assertIn("DATE(block_timestamp) >=", query)
        self.assertIn("SELECT transaction_hash FROM tracked_transfer_hashes", query)
        self.assertIn("JOIN filtered_transactions", query)
        self.assertIn("wallet_wallet_facts", query)
        self.assertIn("wallet_protocol_facts", query)
        self.assertNotIn("entity_name", query)
        self.assertNotIn("protocol_name", query)

    def test_transfer_only_mode_does_not_scan_traces_table(self):
        schema = SimpleNamespace(
            token_transfers_table="dataset.token_transfers",
            transfer_table="dataset.token_transfers",
            transfer_source="token_transfers",
            transactions_table="dataset.transactions",
            receipts_table="dataset.receipts",
            logs_table="dataset.logs",
            traces_table="dataset.traces",
            transfer_contract_field="address",
            transfer_value_field="quantity",
            transfer_transaction_hash_field="transaction_hash",
            transactions_hash_field="transaction_hash",
            receipts_transaction_hash_field="transaction_hash",
            receipt_gas_price_field="effective_gas_price",
            gas_used_field="gas_used",
            logs_address_field="address",
            logs_transaction_hash_field="transaction_hash",
            traces_to_address_field="to_address",
            traces_from_address_field="from_address",
            traces_transaction_hash_field="transaction_hash",
        )

        query = build_context_graph_query(
            schema,
            token_contract="0xabc",
            protocol_registry=[],
            bridge_registry=[],
            time_range="24h",
            mode="transfer_only",
        )

        self.assertIn("filtered_traces AS", query)
        self.assertIn("WHERE FALSE", query)
        self.assertNotIn("FROM `dataset.traces`", query)


class RegistryTests(unittest.TestCase):
    def test_protocol_registry_matches_case_insensitive_address(self):
        match = match_protocol_address(
            "Ethereum",
            "0x7A250D5630B4CF539739DF2C5DACAB4C659F2488",
        )
        self.assertIsNotNone(match)
        self.assertEqual(match["protocol_name"], "Uniswap V2 Router")

    def test_bridge_registry_matches_case_insensitive_address(self):
        match = match_bridge_address(
            "Polygon",
            "0x3C2269811836AF69497E5F486A85D7316753CF62",
        )
        self.assertIsNotNone(match)
        self.assertEqual(match["bridge_name"], "LayerZero Endpoint")


class ClassifierTests(unittest.TestCase):
    def test_classifier_detects_arbitrage_bots(self):
        self.assertEqual(
            classify_signals(
                {
                    "counterparty_entropy": 80,
                    "liquidity_gap": 15,
                    "protocol_noise_ratio": 0.10,
                    "bridge_usage_rate": 0.05,
                }
            ),
            "arbitrage_bot",
        )

    def test_classifier_detects_payment_corridors(self):
        self.assertEqual(
            classify_signals(
                {
                    "counterparty_entropy": 4,
                    "liquidity_gap": 3600,
                    "protocol_noise_ratio": 0.12,
                    "bridge_usage_rate": 0.42,
                }
            ),
            "payment_corridor",
        )

    def test_classifier_detects_defi_activity(self):
        self.assertEqual(
            classify_signals(
                {
                    "counterparty_entropy": 12,
                    "liquidity_gap": 120,
                    "protocol_noise_ratio": 0.78,
                    "bridge_usage_rate": 0.05,
                }
            ),
            "defi_activity",
        )


class GraphBuilderTests(unittest.TestCase):
    def test_graph_snapshot_resolves_registry_labels_in_python(self):
        frame = pd.DataFrame(
            [
                {
                    "source_node": "0xwallet1",
                    "destination_node": "0x7a250d5630b4cf539739df2c5dacab4c659f2488",
                    "source_type": "wallet",
                    "destination_type": "contract",
                    "edge_type": "wallet_contract",
                    "token": "USDC",
                    "transaction_hash": "0xtx1",
                    "fact_volume": 1250.0,
                    "last_seen": "2026-03-15T00:00:00+00:00",
                    "gas_fee_native": 0.0011,
                    "evidence_type": "address",
                }
            ]
        )

        snapshot = build_graph_snapshot(
            frame,
            chain="Ethereum",
            token="USDC",
            time_range="24h",
            gap_seconds=1200,
            generated_at="2026-03-15T00:10:00+00:00",
        )

        self.assertEqual(snapshot["edges"][0]["destination_node"], "Uniswap V2 Router")
        self.assertEqual(snapshot["edges"][0]["destination_type"], "dex")
        self.assertEqual(snapshot["edges"][0]["edge_type"], "wallet_dex")

    def test_graph_snapshot_infers_exchange_bridge_exchange_topology(self):
        frame = pd.DataFrame(
            [
                {
                    "source_node": "Coinbase",
                    "destination_node": "Circle CCTP",
                    "source_type": "exchange",
                    "destination_type": "bridge",
                    "edge_type": "protocol_protocol",
                    "token": "USDC",
                    "total_volume": 4_200_000,
                    "transaction_count": 148,
                    "last_seen": "2026-03-15T00:00:00+00:00",
                    "avg_gas_fee": 0.0021,
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
                    "total_volume": 3_900_000,
                    "transaction_count": 128,
                    "last_seen": "2026-03-15T00:05:00+00:00",
                    "avg_gas_fee": 0.0018,
                    "sample_transaction_hash": "0xtx2",
                    "evidence_type": "log,trace",
                },
            ]
        )

        snapshot = build_graph_snapshot(
            frame,
            chain="Ethereum",
            token="USDC",
            time_range="24h",
            gap_seconds=4200,
            generated_at="2026-03-15T00:10:00+00:00",
        )

        self.assertEqual(snapshot["topology"], "Exchange -> Bridge -> Exchange")
        self.assertEqual(snapshot["topology_classification"], "payment_corridor")
        self.assertGreater(snapshot["bridge_usage_rate"], 0)
        self.assertTrue(snapshot["liquidity_hubs"])
        self.assertEqual(snapshot["data_layer"], "derived")
        self.assertEqual(snapshot["query_layer_status"], "mixed_transitional")


class ContextGraphApiTests(unittest.TestCase):
    def test_corridor_graph_returns_snapshot_for_auto_chain(self):
        snapshot = {
            "status": "ok",
            "generated_at": "2026-03-15T00:15:00+00:00",
            "data_layer": "derived",
            "serving_path": "summary_store",
            "query_layer_status": "mixed_transitional",
            "topology": "Wallet -> Bridge -> Wallet",
            "topology_classification": "payment_corridor",
            "liquidity_hubs": [
                {
                    "node_id": "Circle CCTP",
                    "label": "Circle CCTP",
                    "node_type": "bridge",
                    "total_volume": 1200000.0,
                    "transaction_count": 42,
                    "degree": 2,
                }
            ],
            "nodes": [],
            "edges": [],
            "signals": [
                {"name": "bridge_usage_rate", "value": 0.64, "label": "Bridge-linked tx share"}
            ],
            "flow_density": 3.5,
            "protocol_noise_ratio": 0.12,
            "bridge_usage_rate": 0.64,
            "counterparty_entropy": 4.2,
            "liquidity_gap": 4100.0,
            "confidence_score": 0.94,
            "evidence_stack": [],
        }
        route_result = {
            "corridor": "US -> Philippines",
            "corridor_key": "US-PH",
            "recommended_rail": "Ethereum",
            "rails": [
                {"rail": "Ethereum", "route_score": 0.92},
                {"rail": "Polygon", "route_score": 0.88},
            ],
        }

        with patch("api.main.get_route", return_value=route_result), patch(
            "api.main.context_graph_cache.get_best_snapshot", return_value=(snapshot, "1h")
        ), patch(
            "api.main.context_graph_cache.get_cache",
            return_value={"status": "ok"},
        ), patch(
            "api.main.context_graph_cache.get_cache_age_seconds",
            return_value=120,
        ):
            payload = asyncio.run(main_api.corridor_graph("us-philippines"))

        self.assertEqual(payload["chain"], "Ethereum")
        self.assertEqual(payload["time_range"], "1h")
        self.assertEqual(payload["requested_time_range"], "24h")
        self.assertEqual(payload["topology_classification"], "payment_corridor")
        self.assertEqual(payload["graph_cache_status"], "ok")
        self.assertEqual(payload["data_layer"], "derived")
        self.assertEqual(payload["query_layer_status"], "mixed_transitional")

    def test_corridor_graph_returns_unavailable_when_snapshot_missing(self):
        route_result = {
            "corridor": "US -> Brazil",
            "corridor_key": "US-BR",
            "recommended_rail": "Polygon",
            "rails": [{"rail": "Polygon", "route_score": 0.9}],
        }

        with patch("api.main.get_route", return_value=route_result), patch(
            "api.main.context_graph_cache.get_best_snapshot", return_value=(None, None)
        ), patch(
            "api.main.context_graph_cache.get_cache",
            return_value={"status": "stale"},
        ), patch(
            "api.main.context_graph_cache.get_cache_age_seconds",
            return_value=901,
        ):
            payload = asyncio.run(main_api.corridor_graph("us-brazil"))

        self.assertEqual(payload["status"], "unavailable")
        self.assertEqual(payload["graph_cache_status"], "stale")

    def test_corridor_graph_rejects_unknown_corridor(self):
        with self.assertRaises(HTTPException) as exc:
            asyncio.run(main_api.corridor_graph("not-a-real-corridor"))
        self.assertEqual(exc.exception.status_code, 404)


class ContextGraphMaterializationTests(unittest.TestCase):
    def test_context_graph_summary_store_round_trip(self):
        init_summary_store()
        snapshot = {
            "status": "ok",
            "chain": "Ethereum",
            "token": "USDC",
            "time_range": "1h",
            "generated_at": "2026-03-20T16:00:00+00:00",
            "data_layer": "derived",
            "serving_path": "in_memory_snapshot",
            "query_layer_status": "mixed_transitional",
            "topology": "Wallet -> Bridge -> Wallet",
            "topology_classification": "payment_corridor",
            "nodes": [],
            "edges": [],
            "liquidity_hubs": [],
            "signals": [],
            "flow_density": 0.0,
            "protocol_noise_ratio": 0.0,
            "bridge_usage_rate": 0.0,
            "counterparty_entropy": 0.0,
            "liquidity_gap": 0.0,
            "confidence_score": 0.0,
            "evidence_stack": [],
            "total_transactions": 0,
        }
        upsert_context_graph_summary(
            [
                {
                    "chain": "Ethereum",
                    "token": "USDC",
                    "time_range": "1h",
                    "snapshot_json": json.dumps(snapshot, sort_keys=True),
                    "materialized_at": "2026-03-20T16:00:00+00:00",
                }
            ]
        )

        stored = get_context_graph_summary("Ethereum", token="USDC", time_range="1h")

        self.assertIsNotNone(stored)
        self.assertEqual(stored["topology_classification"], "payment_corridor")
        self.assertEqual(stored["materialized_at"], "2026-03-20T16:00:00+00:00")


if __name__ == "__main__":
    unittest.main()
