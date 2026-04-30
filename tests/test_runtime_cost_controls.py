import os
import unittest
from unittest.mock import patch

from data.query import _query_windows_hours
from services.token_registry import get_active_combinations, iter_active_tokens_for_chain


class RuntimeCostControlTests(unittest.TestCase):
    def test_runtime_active_tokens_can_limit_poller_to_usdc(self):
        with patch.dict(os.environ, {"CANOPY_ACTIVE_TOKENS": "USDC"}, clear=False):
            combos = get_active_combinations()
            ethereum_tokens = list(iter_active_tokens_for_chain("Ethereum"))

        self.assertEqual(combos, [("Ethereum", "USDC"), ("Polygon", "USDC")])
        self.assertEqual(ethereum_tokens, ["USDC"])

    def test_query_windows_hours_uses_configured_values(self):
        with patch.dict(os.environ, {"CANOPY_MEASURED_QUERY_WINDOWS_HOURS": "6,12"}, clear=False):
            windows = _query_windows_hours()

        self.assertEqual(windows, [6, 12])


if __name__ == "__main__":
    unittest.main()
