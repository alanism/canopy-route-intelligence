import unittest
from unittest.mock import patch

from api import router


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


def build_structured_cache(
    *,
    polygon_status="fresh",
    polygon_age_seconds=300,
    polygon_error=None,
    ethereum_status="fresh",
    ethereum_age_seconds=300,
    ethereum_error=None,
):
    cache = build_cache()
    ethereum_data = cache["chains"]["Ethereum"]
    return {
        "chains": {
            "Polygon": {
                "data": cache["chains"]["Polygon"],
                "tokens": {
                    "USDC": {
                        "data": {**cache["chains"]["Polygon"], "token": "USDC"},
                        "last_success_at": "2026-03-14T07:24:43+00:00",
                        "last_attempt_at": "2026-03-14T07:25:00+00:00",
                        "status": polygon_status,
                        "last_error": polygon_error,
                        "poll_count": 4,
                        "age_seconds": polygon_age_seconds,
                        "freshness_level": "critical" if polygon_age_seconds > 3600 else "stale" if polygon_age_seconds > 900 else "fresh",
                        "using_bootstrap_data": False,
                    }
                },
                "last_success_at": "2026-03-14T07:24:43+00:00",
                "last_attempt_at": "2026-03-14T07:25:00+00:00",
                "status": polygon_status,
                "last_error": polygon_error,
                "poll_count": 4,
                "age_seconds": polygon_age_seconds,
                "freshness_level": "critical" if polygon_age_seconds > 3600 else "stale" if polygon_age_seconds > 900 else "fresh",
                "using_bootstrap_data": False,
            },
            "Ethereum": {
                "data": cache["chains"]["Ethereum"],
                "tokens": {
                    "USDC": {
                        "data": {**cache["chains"]["Ethereum"], "token": "USDC"},
                        "last_success_at": "2026-03-14T07:18:47+00:00",
                        "last_attempt_at": "2026-03-14T07:25:00+00:00",
                        "status": ethereum_status,
                        "last_error": ethereum_error,
                        "poll_count": 4,
                        "age_seconds": ethereum_age_seconds,
                        "freshness_level": "critical" if ethereum_age_seconds > 3600 else "stale" if ethereum_age_seconds > 900 else "fresh",
                        "using_bootstrap_data": False,
                    },
                    "USDT": {
                        "data": {**ethereum_data, "token": "USDT", "avg_fee_usd": ethereum_data["avg_fee_usd"] + 0.4},
                        "last_success_at": "2026-03-14T07:18:47+00:00",
                        "last_attempt_at": "2026-03-14T07:25:00+00:00",
                        "status": ethereum_status,
                        "last_error": ethereum_error,
                        "poll_count": 4,
                        "age_seconds": ethereum_age_seconds,
                        "freshness_level": "critical" if ethereum_age_seconds > 3600 else "stale" if ethereum_age_seconds > 900 else "fresh",
                        "using_bootstrap_data": False,
                    },
                    "PYUSD": {
                        "data": {**ethereum_data, "token": "PYUSD", "avg_fee_usd": ethereum_data["avg_fee_usd"] + 0.8},
                        "last_success_at": "2026-03-14T07:18:47+00:00",
                        "last_attempt_at": "2026-03-14T07:25:00+00:00",
                        "status": ethereum_status,
                        "last_error": ethereum_error,
                        "poll_count": 4,
                        "age_seconds": ethereum_age_seconds,
                        "freshness_level": "critical" if ethereum_age_seconds > 3600 else "stale" if ethereum_age_seconds > 900 else "fresh",
                        "using_bootstrap_data": False,
                    },
                },
                "last_success_at": "2026-03-14T07:18:47+00:00",
                "last_attempt_at": "2026-03-14T07:25:00+00:00",
                "status": ethereum_status,
                "last_error": ethereum_error,
                "poll_count": 4,
                "age_seconds": ethereum_age_seconds,
                "freshness_level": "critical" if ethereum_age_seconds > 3600 else "stale" if ethereum_age_seconds > 900 else "fresh",
                "using_bootstrap_data": False,
            },
        },
        "native_prices_live": True,
        "eth_price_live": True,
        "eth_price_usd": 2080.26,
        "polygon_price_usd": 0.0959,
        "is_bootstrap": False,
        "status": (
            "ok"
            if polygon_status == "fresh" and ethereum_status == "fresh"
            else "degraded"
            if "fresh" in {polygon_status, ethereum_status}
            else "error"
        ),
        "cache_age_seconds": max(polygon_age_seconds, ethereum_age_seconds),
    }


class RouterLensTests(unittest.TestCase):
    def test_strategy_lens_returns_expected_workspace_shape(self):
        with patch("api.router.get_cache", return_value=build_cache()), patch(
            "api.router.get_cache_age_seconds", return_value=300
        ):
            result = router.get_route(origin="US", destination="BR", lens="strategy")

        self.assertEqual(result["lens"], "strategy")
        self.assertEqual(result["active_lens"]["label"], "Strategy")
        self.assertEqual(result["section_titles"]["why_title"], "Why this rail leads the launch decision")
        self.assertEqual(result["api_workflow_note"], router.API_WORKFLOW_NOTE)
        self.assertEqual(len(result["lens_highlights"]), 4)
        self.assertTrue(result["committee_summary"].startswith("Decision lens: Strategy"))
        self.assertIn("Scenario assumptions", result["committee_summary"])
        self.assertIn("Rail comparison", result["committee_summary"])
        self.assertIn("Unresolved risks", result["committee_summary"])

    def test_expansion_lens_returns_ranked_corridor_comparison(self):
        with patch("api.router.get_cache", return_value=build_cache()), patch(
            "api.router.get_cache_age_seconds", return_value=300
        ):
            result = router.get_route(origin="US", destination="BR", lens="expansion")

        self.assertEqual(result["lens"], "expansion")
        self.assertEqual(result["section_titles"]["corridor_title"], "Which corridors should move next")
        self.assertEqual(len(result["corridor_rankings"]), 9)
        self.assertEqual(result["corridor_rankings"][0]["rank"], 1)
        self.assertEqual(result["corridor_rankings"][0]["corridor"], "US -> Mexico")

    def test_route_uses_first_class_singapore_vietnam_corridor(self):
        with patch("api.router.get_cache", return_value=build_cache()), patch(
            "api.router.get_cache_age_seconds", return_value=300
        ):
            result = router.get_route(origin="SG", destination="VN", lens="treasury")

        self.assertEqual(result["corridor"], "Singapore -> Vietnam")
        self.assertEqual(result["corridor_key"], "SG-VN")
        self.assertEqual(result["corridor_slug"], "singapore-vietnam")
        self.assertEqual(result["source_country"], "Singapore")
        self.assertEqual(result["destination_country"], "Vietnam")

    def test_v5_route_scopes_to_selected_token_support(self):
        cache = build_structured_cache()
        with patch("api.router.get_cache", return_value=cache), patch(
            "api.router.get_cache_age_seconds", return_value=300
        ):
            result = router.get_route(origin="US", destination="BR", lens="risk", token="USDT")

        self.assertEqual(result["token"], "USDT")
        self.assertEqual(result["coverage_state"], "ACTIVE_COVERAGE")
        self.assertEqual([rail["rail"] for rail in result["rails"]], ["Ethereum"])
        self.assertEqual(result["recommended_rail"], "Ethereum")

    def test_confidence_degrades_for_stale_and_small_samples(self):
        cache = build_cache(
            polygon_adjusted_transfer_count=20,
            polygon_avg_gap=90.0,
            polygon_minutes_since=320,
            eth_adjusted_transfer_count=15,
            eth_avg_gap=120.0,
            eth_minutes_since=420,
        )
        with patch("api.router.get_cache", return_value=cache), patch(
            "api.router.get_cache_age_seconds", return_value=7200
        ):
            result = router.get_route(origin="US", destination="VN", lens="treasury")

        polygon = next(rail for rail in result["rails"] if rail["rail"] == "Polygon")
        self.assertLess(polygon["confidence"], 0.7)
        self.assertIn("directional only", result["caveat"].lower())
        self.assertIn("over an hour old", result["caveat"].lower())

    def test_route_includes_corridor_analytics_and_status(self):
        with patch("api.router.get_cache", return_value=build_cache()), patch(
            "api.router.get_cache_age_seconds", return_value=300
        ):
            result = router.get_route(origin="US", destination="BR", lens="strategy")

        self.assertIn("corridor_analytics", result)
        self.assertIn("status", result)
        self.assertIn("route_score", result)
        self.assertIn("liquidity_score", result)
        self.assertIn("trust_score_v4", result)
        self.assertIn("integrity_score", result)
        self.assertIn("solvency_ratio", result)
        self.assertIsInstance(result["corridor_analytics"]["rails"], list)
        polygon = next(rail for rail in result["rails"] if rail["rail"] == "Polygon")
        self.assertIn("route_score", polygon)
        self.assertIn("integrity_score", polygon)
        self.assertIn("solvency_ratio", polygon)
        self.assertIn("liquidity_score_v4", polygon)
        self.assertIn("trust_score_v4", polygon)

    def test_low_integrity_corridor_is_flagged(self):
        cache = build_cache(
            polygon_adjusted_transfer_count=15,
            polygon_avg_gap=120.0,
            polygon_minutes_since=320,
            eth_adjusted_transfer_count=12,
            eth_avg_gap=160.0,
            eth_minutes_since=420,
        )
        with patch("api.router.get_cache", return_value=cache), patch(
            "api.router.get_cache_age_seconds", return_value=7200
        ):
            result = router.get_route(origin="US", destination="VN", lens="risk")

        self.assertIn("Directional only", result["caveat"])
        self.assertLess(result["confidence"], 0.4)
        self.assertTrue(
            any(rail["status"].startswith("FLAGGED_") for rail in result["rails"])
            or any(rail["adversarial_flags"] for rail in result["rails"])
        )

    def test_route_surfaces_per_chain_health_when_one_chain_degrades(self):
        cache = build_structured_cache(
            polygon_status="fresh",
            polygon_age_seconds=180,
            ethereum_status="error",
            ethereum_age_seconds=2100,
            ethereum_error="BigQuery timeout",
        )
        with patch("api.router.get_cache", return_value=cache), patch(
            "api.router.get_cache_age_seconds", return_value=2100
        ):
            result = router.get_route(origin="US", destination="BR", lens="strategy")

        self.assertEqual(result["global_data_status"], "degraded")
        self.assertEqual(result["data_health_summary"]["chains"]["Polygon"]["status"], "fresh")
        self.assertEqual(result["data_health_summary"]["chains"]["Ethereum"]["status"], "error")
        self.assertIn("single-live-rail", result["caveat"].lower())

    def test_landscape_returns_three_token_tiles_and_corridor_best(self):
        cache = build_structured_cache()
        with patch("api.router.get_cache", return_value=cache), patch(
            "api.router.get_cache_age_seconds", return_value=300
        ):
            result = router.get_landscape(origin="US", destination="BR", amount_usdc=50000, lens="strategy")

        self.assertEqual(result["default_token"], "USDC")
        self.assertEqual(len(result["tiles"]), 3)
        self.assertIn("token", result["corridor_best_supported"])
        self.assertIn("rail", result["corridor_best_supported"])

    def test_route_warns_when_recommendation_is_based_on_stale_signal(self):
        cache = build_structured_cache(
            polygon_status="error",
            polygon_age_seconds=5400,
            polygon_error="Polygon query failed",
            ethereum_status="error",
            ethereum_age_seconds=4800,
            ethereum_error="Ethereum query failed",
        )
        with patch("api.router.get_cache", return_value=cache), patch(
            "api.router.get_cache_age_seconds", return_value=5400
        ):
            result = router.get_route(origin="US", destination="VN", lens="strategy")

        self.assertEqual(result["global_data_status"], "error")
        self.assertIsNotNone(result["degraded_recommendation_warning"])
        recommended = next(rail for rail in result["rails"] if rail["rail"] == result["recommended_rail"])
        self.assertIn(recommended["data_status"], {"error", "stale"})


if __name__ == "__main__":
    unittest.main()
