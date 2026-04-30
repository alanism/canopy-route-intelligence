import asyncio
import tempfile
import unittest
from pathlib import Path

from fastapi import HTTPException

from api import demo_store, simulate
from models.request_models import SimulateRequest
from services.export_receipt import export_decision_receipt
from services.strategy_engine import build_strategy_assessment


class V5ScenarioTests(unittest.TestCase):
    def test_scenario_lifecycle_persists_and_reviews(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "scenario.sqlite3"
            original_db_path = demo_store.DB_PATH
            try:
                demo_store.DB_PATH = db_path
                saved = demo_store.create_scenario(
                    corridor_key="US-BR",
                    corridor_label="US -> Brazil",
                    token="USDT",
                    recommended_rail="Ethereum",
                    scenario_payload={"token": "USDT", "amount_usdc": 50000},
                    route_payload={"recommended_rail": "Ethereum", "token": "USDT"},
                    follow_up_requested=True,
                )
                loaded = demo_store.get_scenario(saved["id"])
                reviewed = demo_store.review_scenario(
                    saved["id"],
                    review_state="accepted",
                    reviewer="tester",
                    review_notes="Looks good",
                )
            finally:
                demo_store.DB_PATH = original_db_path

        self.assertIsNotNone(loaded)
        self.assertEqual(loaded["token"], "USDT")
        self.assertTrue(loaded["follow_up_requested"])
        self.assertEqual(reviewed["review_state"], "accepted")
        self.assertEqual(reviewed["reviewer"], "tester")

    def test_scenario_listing_and_discovery_events(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "scenario.sqlite3"
            original_db_path = demo_store.DB_PATH
            try:
                demo_store.DB_PATH = db_path
                saved = demo_store.create_scenario(
                    corridor_key="US-BR",
                    corridor_label="US -> Brazil",
                    token="USDC",
                    recommended_rail="Polygon",
                    scenario_payload={"token": "USDC", "amount_usdc": 10000},
                    route_payload={"recommended_rail": "Polygon", "token": "USDC"},
                )
                demo_store.save_discovery_event(
                    event_name="token_selected",
                    corridor_key="US-BR",
                    corridor_label="US -> Brazil",
                    token="USDC",
                    lens="strategy",
                    metadata={"source": "test"},
                )
                scenarios = demo_store.list_scenarios(corridor_key="US-BR")
                events = demo_store.list_discovery_events(corridor_key="US-BR")
                summary = demo_store.get_discovery_summary(corridor_key="US-BR")
            finally:
                demo_store.DB_PATH = original_db_path

        self.assertEqual(scenarios[0]["id"], saved["id"])
        self.assertEqual(events[0]["event_name"], "token_selected")
        self.assertEqual(events[0]["metadata"]["source"], "test")
        self.assertEqual(summary["total_events"], 1)
        self.assertEqual(summary["total_scenarios"], 1)
        self.assertEqual(summary["token_counts"][0]["token"], "USDC")

    def test_strategy_assessment_caps_critical_risk(self):
        assessment = build_strategy_assessment(
            cost_score=0.92,
            liquidity_score=0.91,
            trust_score=0.88,
            flags=["LOW_INTEGRITY"],
        )

        self.assertEqual(assessment["risk_gate_status"], "CRITICAL_CAP")
        self.assertLessEqual(assessment["strategy_score"], 0.39)

    def test_receipt_includes_scenario_review_metadata(self):
        route_result = {
            "timestamp": "2026-03-16T07:14:54+00:00",
            "request_id": "req_123",
            "decision_id": "decision_123",
            "token": "USDC",
            "coverage_state": "ACTIVE_COVERAGE",
            "global_data_status": "ok",
            "corridor_best_supported": {"token": "USDC", "rail": "Polygon", "is_selected_route": True},
            "scenario": {"current_rail_fee_pct": 1.2},
            "rails": [
                {
                    "rail": "Polygon",
                    "mode": "live_measured",
                    "data_status": "fresh",
                    "freshness_level": "fresh",
                    "cache_age_seconds": 120,
                    "adversarial_flags": [],
                    "transfer_math": {
                        "amount_usdc": 10000,
                        "network_fee_usd": 0.01,
                        "routing_bps": 0.0014,
                        "routing_fixed_fee_usd": 0.2,
                        "routing_min_fee_usd": 1.25,
                        "routing_fee_usd": 14.2,
                        "total_fee_usd": 14.21,
                        "landed_amount_usd": 9985.79,
                        "provenance": {"landed_amount_usd": "CALCULATED"},
                    },
                    "strategy_assessment": {
                        "strategy_score": 0.9,
                        "cost_score": 0.95,
                        "liquidity_score": 0.88,
                        "trust_score": 0.72,
                        "liquidity_penalty_factor": 1.0,
                        "trust_penalty_factor": 1.0,
                        "strategy_score_label": "90 / 100",
                        "evidence_confidence_label": "Strong",
                        "provenance": {
                            "strategy_score": "MODELED",
                            "evidence_confidence": "MODELED",
                        },
                    },
                },
                {
                    "rail": "Ethereum",
                    "mode": "live_measured",
                    "data_status": "fresh",
                    "freshness_level": "fresh",
                    "cache_age_seconds": 120,
                    "adversarial_flags": ["HIGH_SLIPPAGE"],
                    "transfer_math": {
                        "amount_usdc": 10000,
                        "network_fee_usd": 3.2,
                        "routing_bps": 0.0011,
                        "routing_fixed_fee_usd": 0.35,
                        "routing_min_fee_usd": 1.75,
                        "routing_fee_usd": 11.35,
                        "total_fee_usd": 14.55,
                        "landed_amount_usd": 9985.45,
                        "provenance": {"landed_amount_usd": "CALCULATED"},
                    },
                    "strategy_assessment": {
                        "strategy_score": 0.4,
                        "cost_score": 0.6,
                        "liquidity_score": 0.3,
                        "trust_score": 0.4,
                        "liquidity_penalty_factor": 0.8,
                        "trust_penalty_factor": 0.9,
                        "strategy_score_label": "40 / 100",
                        "evidence_confidence_label": "Moderate",
                        "provenance": {
                            "strategy_score": "MODELED",
                            "evidence_confidence": "MODELED",
                        },
                    },
                },
            ],
            "transfer_winner": "Polygon",
            "canopy_recommendation": "Polygon",
            "evidence_packet": {
                "expected_fee_usd": {
                    "data_source": "bigquery_cache",
                    "last_updated_at": "2026-03-16T07:14:54+00:00",
                }
            },
        }
        scenario_artifact = {
            "review_state": "accepted",
            "reviewer": "tester",
            "review_notes": "Use this in the next treasury call.",
        }

        receipt = export_decision_receipt(
            corridor="US -> Brazil",
            lens="Strategy",
            route_result=route_result,
            scenario_artifact=scenario_artifact,
        )

        self.assertIn("Scenario Review State: accepted", receipt)
        self.assertIn("Scenario Reviewer: tester", receipt)
        self.assertIn("REVIEW NOTES", receipt)

    def test_simulation_rejects_non_usdc(self):
        request = SimulateRequest(
            amount=1000,
            token="USDT",
            source_chain="Ethereum",
            destination_chain="Polygon",
        )
        with self.assertRaises(HTTPException) as error:
            asyncio.run(simulate.simulate_transfer(request))

        self.assertEqual(error.exception.status_code, 400)
        self.assertIn("USDC", error.exception.detail)


if __name__ == "__main__":
    unittest.main()
