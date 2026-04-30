import unittest
from unittest.mock import patch

from data.query import CHAIN_CONFIGS, FEE_QUERY_MAX_BYTES_BILLED, _build_measured_fee_extraction_query, run_chain_token_query
from services.derived_fee_metrics import derive_fee_metrics


class FeeLayerSplitTests(unittest.TestCase):
    def test_measured_fee_query_emits_raw_fields_only(self):
        query = _build_measured_fee_extraction_query(
            CHAIN_CONFIGS["Ethereum"],
            token_contract="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            decimals=6,
            hours=24,
        )

        self.assertIn("from_address", query)
        self.assertIn("to_address", query)
        self.assertIn("transfer_value_token", query)
        self.assertNotIn("avg_fee_usd", query)
        self.assertNotIn("payment_like", query)
        self.assertNotIn("PERCENTILE_CONT", query)

    def test_derived_fee_metrics_matches_expected_parity_fixture(self):
        measured_rows = [
            {
                "transaction_hash": "0xtx1",
                "block_timestamp": "2026-03-20T00:00:00+00:00",
                "from_address": "0xfrom1",
                "to_address": "0xto1",
                "token_address": "0xtoken",
                "transfer_value_token": 10.0,
                "tx_to_address": "0xtoken",
                "status": 1,
                "gas_used": 21000,
                "effective_gas_price": 100000000000,
            },
            {
                "transaction_hash": "0xtx1",
                "block_timestamp": "2026-03-20T00:00:10+00:00",
                "from_address": "0xfrom1",
                "to_address": "0xto2",
                "token_address": "0xtoken",
                "transfer_value_token": 5.0,
                "tx_to_address": "0xtoken",
                "status": 1,
                "gas_used": 21000,
                "effective_gas_price": 100000000000,
            },
            {
                "transaction_hash": "0xtx2",
                "block_timestamp": "2026-03-20T00:05:00+00:00",
                "from_address": "0xfrom2",
                "to_address": "0xto3",
                "token_address": "0xtoken",
                "transfer_value_token": 0.5,
                "tx_to_address": "0xtoken",
                "status": 1,
                "gas_used": 30000,
                "effective_gas_price": 50000000000,
            },
            {
                "transaction_hash": "0xtx3",
                "block_timestamp": "2026-03-20T00:10:00+00:00",
                "from_address": "0xfrom3",
                "to_address": "0xto4",
                "token_address": "0xtoken",
                "transfer_value_token": 100.0,
                "tx_to_address": "0xmerchant",
                "status": 1,
                "gas_used": 40000,
                "effective_gas_price": 20000000000,
            },
        ]

        derived = derive_fee_metrics(
            measured_rows,
            chain="Ethereum",
            token="USDC",
            token_contract="0xtoken",
            native_price_usd=3000.0,
            window_label="24h",
            queried_at=None,
        )

        self.assertIsNotNone(derived)
        self.assertAlmostEqual(derived["avg_fee_usd"], 3.3, places=6)
        self.assertAlmostEqual(derived["median_fee_usd"], 3.15, places=6)
        self.assertAlmostEqual(derived["p90_fee_usd"], 4.23, places=6)
        self.assertEqual(derived["transfer_count"], 4)
        self.assertAlmostEqual(derived["volume_usdc"], 115.5, places=6)
        self.assertEqual(derived["adjusted_transaction_count"], 1)
        self.assertEqual(derived["adjusted_transfer_count"], 2)
        self.assertAlmostEqual(derived["adjusted_volume_usdc"], 15.0, places=6)
        self.assertEqual(derived["window_used"], "24h")

    @patch("data.query.run_query")
    @patch("data.query.derive_fee_metrics")
    def test_measured_fee_path_uses_family_byte_cap(self, derive_metrics_mock, run_query_mock):
        derive_metrics_mock.return_value = {"transfer_count": 1, "window_used": "24h"}
        run_query_mock.return_value = (None, [])

        result = run_chain_token_query(
            CHAIN_CONFIGS["Ethereum"],
            native_price_usd=3000.0,
            token="USDC",
        )

        self.assertEqual(result["transfer_count"], 1)
        self.assertEqual(run_query_mock.call_args.kwargs["maximum_bytes_billed"], FEE_QUERY_MAX_BYTES_BILLED)


if __name__ == "__main__":
    unittest.main()
