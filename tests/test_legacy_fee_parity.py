import unittest
from unittest.mock import patch

from data.legacy_fee_parity import (
    build_legacy_fee_query,
    run_legacy_chain_token_query,
)
from data.query import CHAIN_CONFIGS, FEE_QUERY_MAX_BYTES_BILLED


class LegacyFeeParityTests(unittest.TestCase):
    def test_legacy_fee_query_remains_mixed_for_parity_reference(self):
        query = build_legacy_fee_query(
            CHAIN_CONFIGS["Ethereum"],
            token_contract="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            decimals=6,
            native_price_usd=3000.0,
            hours=24,
        )

        self.assertIn("avg_fee_usd", query)
        self.assertIn("payment_like_transfer_logs", query)
        self.assertIn("PERCENTILE_CONT", query)

    @patch("data.legacy_fee_parity.LEGACY_FEE_PARITY_ENABLED", False)
    def test_legacy_fee_runtime_is_quarantined_by_default(self):
        with self.assertRaises(RuntimeError):
            run_legacy_chain_token_query(
                CHAIN_CONFIGS["Ethereum"],
                native_price_usd=3000.0,
                token="USDC",
                maximum_bytes_billed=FEE_QUERY_MAX_BYTES_BILLED,
            )


if __name__ == "__main__":
    unittest.main()
