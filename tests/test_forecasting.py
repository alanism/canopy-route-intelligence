import unittest
from unittest.mock import patch

from forecasting.api import run_corridor_forecast


def build_cache(
    *,
    polygon_adjusted_transfer_count=1200,
    polygon_avg_gap=3.0,
    polygon_minutes_since=8,
    polygon_avg_fee=0.01,
    eth_adjusted_transfer_count=800,
    eth_avg_gap=4.0,
    eth_minutes_since=12,
    eth_avg_fee=3.2,
):
    return {
        "chains": {
            "Polygon": {
                "transfer_count": 2000,
                "volume_usdc": 2_500_000,
                "avg_fee_usd": polygon_avg_fee,
                "p90_fee_usd": polygon_avg_fee * 3,
                "adjusted_transfer_count": polygon_adjusted_transfer_count,
                "adjusted_volume_usdc": 12_000_000,
                "avg_minutes_between_adjusted_transfers": polygon_avg_gap,
                "minutes_since_last_adjusted_transfer": polygon_minutes_since,
                "adjusted_freshness_timestamp": "2026-03-14T07:24:43+00:00",
                "freshness_timestamp": "2026-03-14T07:24:43+00:00",
                "activity_filter_method": (
                    "Direct USDC contract calls with transfer value >= $1; excludes obvious "
                    "router-mediated flows, zero-value logs, and dust-like activity."
                ),
            },
            "Ethereum": {
                "transfer_count": 1800,
                "volume_usdc": 4_500_000,
                "avg_fee_usd": eth_avg_fee,
                "p90_fee_usd": eth_avg_fee * 2,
                "adjusted_transfer_count": eth_adjusted_transfer_count,
                "adjusted_volume_usdc": 24_000_000,
                "avg_minutes_between_adjusted_transfers": eth_avg_gap,
                "minutes_since_last_adjusted_transfer": eth_minutes_since,
                "adjusted_freshness_timestamp": "2026-03-14T07:18:47+00:00",
                "freshness_timestamp": "2026-03-14T07:18:47+00:00",
                "activity_filter_method": (
                    "Direct USDC contract calls with transfer value >= $1; excludes obvious "
                    "router-mediated flows, zero-value logs, and dust-like activity."
                ),
            },
        },
        "native_prices_live": True,
        "eth_price_live": True,
        "eth_price_usd": 2080.26,
        "polygon_price_usd": 0.0959,
        "is_bootstrap": False,
    }


class ForecastingTests(unittest.TestCase):
    def test_corridor_forecast_returns_probabilistic_signals(self):
        with patch("api.router.get_cache", return_value=build_cache()), patch(
            "api.router.get_cache_age_seconds", return_value=300
        ):
            response = run_corridor_forecast("us-brazil")

        payload = response.model_dump()
        self.assertEqual(payload["corridor_id"], "US-BR")
        self.assertIn("corridor_stability_probability", payload)
        self.assertIn("liquidity_shock_risk", payload)
        self.assertIn("transfer_slippage_probability", payload)
        self.assertIn("fx_volatility_signal", payload)
        self.assertIn("regulatory_risk_index", payload)
        self.assertIn("demand_growth_forecast", payload)
        self.assertIn("corridor_health_score", payload)
        self.assertFalse(payload["model_metadata"].get("forecast_layer") is None)

    def test_low_integrity_corridor_triggers_kill_switch(self):
        stressed_cache = build_cache(
            polygon_adjusted_transfer_count=12,
            polygon_avg_gap=180.0,
            polygon_minutes_since=480,
            eth_adjusted_transfer_count=9,
            eth_avg_gap=220.0,
            eth_minutes_since=560,
        )
        with patch("api.router.get_cache", return_value=stressed_cache), patch(
            "api.router.get_cache_age_seconds", return_value=7200
        ):
            response = run_corridor_forecast("US-VN")

        payload = response.model_dump()
        self.assertTrue(payload["kill_switch_triggered"])
        self.assertLess(payload["corridor_health_score"], 40)
        self.assertIn("WASH_TRADING_DETECTION", payload["kill_switches"])

    def test_south_africa_corridor_is_registered_for_forecast(self):
        with patch("api.router.get_cache", return_value=build_cache()), patch(
            "api.router.get_cache_age_seconds", return_value=300
        ):
            response = run_corridor_forecast("US-ZA")

        payload = response.model_dump()
        self.assertEqual(payload["corridor_id"], "US-ZA")
        self.assertEqual(payload["corridor_label"], "US -> South Africa")


if __name__ == "__main__":
    unittest.main()
