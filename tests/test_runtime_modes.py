import os
import unittest
from unittest.mock import patch

from api import cache
from api.eth_price import get_native_prices
from services.context_graph import cache as context_graph_cache
from services.bigquery_client import _validate_query
from services.runtime_mode import (
    get_runtime_mode,
    get_runtime_mode_label,
    get_runtime_mode_note,
    is_demo_mode,
)


class RuntimeModeTests(unittest.TestCase):
    def test_runtime_mode_defaults_to_real(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(get_runtime_mode(), "real")
            self.assertFalse(is_demo_mode())

    def test_demo_mode_prices_use_fallbacks_without_coinbase(self):
        with patch.dict(
            os.environ,
            {
                "CANOPY_RUNTIME_MODE": "demo",
                "ETH_PRICE_FALLBACK": "3210",
                "POLYGON_PRICE_FALLBACK": "0.11",
            },
            clear=False,
        ), patch("api.eth_price._fetch_coinbase_price", side_effect=AssertionError("should not fetch")):
            prices, is_live = get_native_prices()

        self.assertEqual(prices, {"ethereum": 3210.0, "polygon": 0.11})
        self.assertFalse(is_live)

    def test_demo_mode_blocks_bigquery_execution(self):
        with patch.dict(os.environ, {"CANOPY_RUNTIME_MODE": "demo"}, clear=False):
            with self.assertRaises(RuntimeError):
                _validate_query(
                    "SELECT 1",
                    query_name="demo_block",
                    query_family="test",
                    maximum_bytes_billed=1000,
                    query_classification="derived",
                    enforce_validation=False,
                    allow_request_scoped=False,
                )

    def test_demo_cache_is_seeded_and_fresh(self):
        with patch.dict(os.environ, {"CANOPY_RUNTIME_MODE": "demo", "CANOPY_ACTIVE_TOKENS": "USDC"}, clear=False):
            payload = cache.seed_demo_cache()

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["chains"]["Polygon"]["status"], "fresh")
        self.assertEqual(payload["chains"]["Ethereum"]["status"], "fresh")
        self.assertEqual(payload["poll_count"], 1)

    def test_context_graph_demo_cache_is_seeded(self):
        with patch.dict(os.environ, {"CANOPY_RUNTIME_MODE": "demo"}, clear=False):
            payload = context_graph_cache.seed_demo_cache()
            self.assertEqual(get_runtime_mode_label(), "Demo Mode")
            self.assertIn("No BigQuery or Coinbase", get_runtime_mode_note())

        self.assertEqual(payload["status"], "demo")


if __name__ == "__main__":
    unittest.main()
