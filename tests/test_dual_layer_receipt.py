import unittest

from services.export_receipt import export_decision_receipt
from services.strategy_engine import build_strategy_assessment
from services.transfer_math import build_transfer_math


class DualLayerModelTests(unittest.TestCase):
    def test_transfer_math_matches_expected_example(self):
        result = build_transfer_math(
            rail="Ethereum",
            amount_usdc=10_000,
            network_fee_usd=0.0649,
            measured_fee_available=True,
        )

        self.assertAlmostEqual(result["routing_fee_usd"], 11.35, places=2)
        self.assertAlmostEqual(result["total_fee_usd"], 11.4149, places=4)
        self.assertAlmostEqual(result["landed_amount_usd"], 9988.5851, places=4)
        self.assertEqual(result["provenance"]["network_fee_usd"], "MEASURED")
        self.assertEqual(result["provenance"]["routing_fee_usd"], "CALCULATED")

    def test_strategy_assessment_matches_expected_formula(self):
        polygon = build_strategy_assessment(
            cost_score=0.95,
            liquidity_score=1.0,
            trust_score=0.91,
            flags=[],
        )
        ethereum = build_strategy_assessment(
            cost_score=1.0,
            liquidity_score=0.2,
            trust_score=0.59,
            flags=[],
        )

        self.assertAlmostEqual(polygon["strategy_score"], 0.962, places=3)
        self.assertAlmostEqual(ethereum["strategy_score"], 0.598, places=3)
        self.assertEqual(polygon["strategy_score_label"], "96 / 100")
        self.assertEqual(ethereum["evidence_confidence_label"], "59 / 100")

    def test_decision_receipt_separates_transfer_winner_and_recommendation(self):
        route_result = {
            "request_id": "req_demo_123",
            "decision_id": "decision_demo_456",
            "timestamp": "2026-03-15T11:00:00+00:00",
            "global_data_status": "degraded",
            "amount_usdc": 10_000,
            "scenario": {
                "current_rail_fee_pct": 0.9,
            },
            "transfer_winner": "Ethereum",
            "canopy_recommendation": "Polygon",
            "rails": [
                {
                    "rail": "Ethereum",
                    "mode": "live_measured",
                    "adversarial_flags": ["LOW_INTEGRITY"],
                    "data_status": "error",
                    "freshness_level": "critical",
                    "cache_age_seconds": 5400,
                    "transfer_math": {
                        "amount_usdc": 10_000,
                        "network_fee_usd": 0.0649,
                        "routing_bps": 0.0011,
                        "routing_fixed_fee_usd": 0.35,
                        "routing_min_fee_usd": 1.75,
                        "routing_fee_usd": 11.35,
                        "total_fee_usd": 11.4149,
                        "landed_amount_usd": 9988.5851,
                        "provenance": {
                            "landed_amount_usd": "CALCULATED",
                        },
                    },
                    "strategy_assessment": {
                        "cost_score": 1.0,
                        "liquidity_score": 0.2,
                        "trust_score": 0.59,
                        "strategy_score": 0.598,
                        "strategy_score_label": "60 / 100",
                        "evidence_confidence_label": "59 / 100",
                        "liquidity_penalty_factor": 1.0,
                        "trust_penalty_factor": 1.0,
                        "provenance": {
                            "strategy_score": "MODELED",
                            "evidence_confidence": "MODELED",
                        },
                    },
                },
                {
                    "rail": "Polygon",
                    "mode": "live_measured",
                    "adversarial_flags": [],
                    "data_status": "fresh",
                    "freshness_level": "fresh",
                    "cache_age_seconds": 120,
                    "transfer_math": {
                        "amount_usdc": 10_000,
                        "network_fee_usd": 0.01,
                        "routing_bps": 0.0014,
                        "routing_fixed_fee_usd": 0.20,
                        "routing_min_fee_usd": 1.25,
                        "routing_fee_usd": 14.2,
                        "total_fee_usd": 14.21,
                        "landed_amount_usd": 9985.79,
                        "provenance": {
                            "landed_amount_usd": "CALCULATED",
                        },
                    },
                    "strategy_assessment": {
                        "cost_score": 0.95,
                        "liquidity_score": 1.0,
                        "trust_score": 0.91,
                        "strategy_score": 0.962,
                        "strategy_score_label": "96 / 100",
                        "evidence_confidence_label": "91 / 100",
                        "liquidity_penalty_factor": 1.0,
                        "trust_penalty_factor": 1.0,
                        "provenance": {
                            "strategy_score": "MODELED",
                            "evidence_confidence": "MODELED",
                        },
                    },
                },
            ],
        }

        receipt = export_decision_receipt(
            corridor="US -> Mexico",
            lens="Product",
            route_result=route_result,
        )

        self.assertIn("Transfer Winner: Ethereum [CALCULATED]", receipt)
        self.assertIn("Canopy Recommendation: Polygon [MODELED]", receipt)
        self.assertIn("Request ID: req_demo_123", receipt)
        self.assertIn("Decision ID: decision_demo_456", receipt)
        self.assertIn("Global Data Status: degraded", receipt)
        self.assertIn("routing_fee = max(10000 * 0.0011 + 0.35, 1.75) = 11.35", receipt)
        self.assertIn("0.4(0.95) + 0.4(1) + 0.2(0.91)", receipt)
        self.assertIn("Polygon recommended despite a lower landed amount", receipt)
        self.assertIn("Ethereum\nLOW_INTEGRITY", receipt)


if __name__ == "__main__":
    unittest.main()
