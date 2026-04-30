import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.responses import JSONResponse

from api import main
from services import corridor_config
from services.request_context import get_request_id


class DummyRequest:
    def __init__(self, headers=None, path="/v1/route", query="", method="POST"):
        self.headers = headers or {}
        self.method = method
        self.url = SimpleNamespace(path=path, query=query)


class DemoHardeningTests(unittest.TestCase):
    def test_request_id_middleware_echoes_header_and_sets_context(self):
        captured = {}

        async def call_next(_request):
            captured["request_id"] = get_request_id()
            return JSONResponse(content={"ok": True})

        response = asyncio.run(
            main.request_id_middleware(
                DummyRequest(headers={"X-Request-Id": "req_user_supplied"}),
                call_next,
            )
        )

        self.assertEqual(captured["request_id"], "req_user_supplied")
        self.assertEqual(response.headers["X-Request-Id"], "req_user_supplied")

    def test_health_reports_chain_and_config_health(self):
        cache_payload = {
            "status": "degraded",
            "cache_age_seconds": 1800,
            "eth_price_usd": 2100,
            "polygon_price_usd": 0.11,
            "native_prices_live": True,
            "eth_price_live": True,
            "last_error": "Ethereum timeout",
            "poll_count": 9,
            "chains": {
                "Polygon": {
                    "data": {"transfer_count": 1200},
                    "status": "fresh",
                    "freshness_level": "fresh",
                    "age_seconds": 120,
                    "last_success_at": "2026-03-15T10:00:00+00:00",
                    "last_attempt_at": "2026-03-15T10:05:00+00:00",
                    "last_error": None,
                },
                "Ethereum": {
                    "data": {"transfer_count": 900},
                    "status": "error",
                    "freshness_level": "stale",
                    "age_seconds": 1800,
                    "last_success_at": "2026-03-15T09:35:00+00:00",
                    "last_attempt_at": "2026-03-15T10:05:00+00:00",
                    "last_error": "Ethereum timeout",
                },
            },
        }
        with patch("api.main.cache.get_cache", return_value=cache_payload), patch(
            "api.main.cache.get_cache_age_seconds", return_value=1800
        ), patch(
            "api.main.context_graph_cache.get_cache", return_value={"status": "ok"}
        ), patch(
            "api.main.context_graph_cache.get_cache_age_seconds", return_value=90
        ), patch(
            "api.main.get_config_health",
            return_value={
                "status": "ok",
                "source": "gs://bucket/canopy/corridors.v1.json",
                "last_loaded_at": 123,
                "last_error": None,
                "refresh_seconds": 60,
            },
        ):
            response = asyncio.run(main.health())

        payload = json.loads(response.body.decode("utf-8"))
        self.assertEqual(payload["status"], "degraded")
        self.assertEqual(payload["chains"]["Polygon"]["status"], "fresh")
        self.assertEqual(payload["chains"]["Ethereum"]["status"], "error")
        self.assertEqual(payload["corridor_config_status"], "ok")

    def test_external_config_can_add_corridor_without_code_change(self):
        base_config = corridor_config.load_corridor_config(force=True)
        payload = json.loads(json.dumps({k: v for k, v in base_config.items() if not k.startswith("_")}))
        payload["corridors"].append(
            {
                **payload["default_corridor"],
                "key": "AU-SG",
                "origin": "AU",
                "destination": "SG",
                "label": "Australia -> Singapore",
                "corridor_slug": "australia-singapore",
                "source_country": "Australia",
                "destination_country": "Singapore",
                "destination_city": "Singapore",
            }
        )
        with patch.dict("os.environ", {"CANOPY_CORRIDOR_CONFIG_URI": "gs://demo/corridors.v1.json"}), patch(
            "services.corridor_config._load_from_gcs", return_value=payload
        ):
            config = corridor_config.load_corridor_config(force=True)

        keys = {corridor["key"] for corridor in config["corridors"]}
        self.assertIn("AU-SG", keys)

    def test_invalid_external_config_keeps_last_known_good(self):
        corridor_config.load_corridor_config(force=True)
        with patch.dict("os.environ", {"CANOPY_CORRIDOR_CONFIG_URI": "gs://demo/corridors.v1.json"}), patch(
            "services.corridor_config._load_from_gcs", side_effect=ValueError("bad config")
        ):
            config = corridor_config.load_corridor_config(force=True)
            health = corridor_config.get_config_health()

        keys = {corridor["key"] for corridor in config["corridors"]}
        self.assertIn("US-BR", keys)
        self.assertEqual(health["status"], "degraded")
        self.assertEqual(health["last_error"], "bad config")

    def test_singapore_vietnam_corridor_resolves_by_pair_key_and_slug(self):
        corridor_config.load_corridor_config(force=True)

        by_pair = corridor_config.get_corridor("SG", "VN")
        by_key = corridor_config.get_corridor_by_key("SG-VN")
        by_slug = corridor_config.get_corridor_by_slug("singapore-vietnam")

        self.assertIsNotNone(by_pair)
        self.assertEqual(by_pair["label"], "Singapore -> Vietnam")
        self.assertEqual(by_key["destination_city"], "Ho Chi Minh City")
        self.assertEqual(by_slug["origin"], "SG")

    def test_bigquery_metrics_endpoint_returns_snapshot(self):
        snapshot = {
            "status": "ok",
            "started_at": "2026-03-20T15:00:00+00:00",
            "overall": {"execution_count": 3, "dry_run_count": 3, "family_count": 2},
            "families": {
                "fee_activity": {"query_count": 2},
                "context_graph_edges": {"query_count": 1},
            },
        }
        with patch("api.main.get_query_metrics_snapshot", return_value=snapshot):
            response = asyncio.run(main.bigquery_metrics())

        payload = json.loads(response.body.decode("utf-8"))
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["overall"]["execution_count"], 3)
        self.assertIn("fee_activity", payload["families"])


if __name__ == "__main__":
    unittest.main()
