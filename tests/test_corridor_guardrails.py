import unittest
from unittest.mock import patch

import pandas as pd

from services.request_context import reset_request_id, set_request_id
from services.corridor_analytics import (
    CORRIDOR_QUERY_MAX_BYTES_BILLED,
    get_corridor_volume,
)


class CorridorGuardrailTests(unittest.TestCase):
    @patch("services.corridor_analytics.LIVE_CORRIDOR_BIGQUERY", True)
    @patch("services.corridor_analytics.execute_sql")
    @patch("services.corridor_analytics.get_token_contract")
    def test_live_corridor_bigquery_uses_family_byte_cap(
        self,
        get_token_contract_mock,
        execute_sql_mock,
    ):
        get_token_contract_mock.return_value = "0xtoken"
        execute_sql_mock.return_value = pd.DataFrame(
            [
                {
                    "block_timestamp": "2026-03-20T00:00:00+00:00",
                    "transfer_value_token": 10.0,
                }
            ]
        )

        result = get_corridor_volume(
            "US-VN",
            "Ethereum",
            rail_data={},
            token="USDC",
            time_range="24h",
            allow_live_bigquery=True,
        )

        self.assertEqual(result["source"], "derived_from_measured_batch")
        self.assertEqual(result["data_layer"], "derived")
        self.assertEqual(
            execute_sql_mock.call_args.kwargs["maximum_bytes_billed"],
            CORRIDOR_QUERY_MAX_BYTES_BILLED,
        )

    @patch("services.corridor_analytics.LIVE_CORRIDOR_BIGQUERY", True)
    def test_request_context_blocks_live_corridor_bigquery(self):
        token = set_request_id("req_audit_guard")
        try:
            with self.assertRaises(RuntimeError):
                get_corridor_volume(
                    "US-VN",
                    "Ethereum",
                    rail_data={},
                    token="USDC",
                    time_range="24h",
                    allow_live_bigquery=True,
                )
        finally:
            reset_request_id(token)


if __name__ == "__main__":
    unittest.main()
