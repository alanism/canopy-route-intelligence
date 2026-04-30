import unittest
from unittest.mock import patch

from services import payroll_demo
from services.export_receipt import export_decision_receipt
from tests.test_payroll_demo import (
    FakeForecastResponse,
    build_forecast_payload,
    build_query_metrics,
    build_route_payload,
)


def render_payroll_receipt(run_id: str = "ph-2026-04-04") -> str:
    with patch(
        "services.payroll_demo.get_route",
        return_value=build_route_payload(recommended_rail="Polygon", liquidity_score=0.72),
    ), patch(
        "services.payroll_demo.run_corridor_forecast",
        return_value=FakeForecastResponse(build_forecast_payload()),
    ), patch("services.payroll_demo.get_query_metrics_snapshot", return_value=build_query_metrics()):
        detail = payroll_demo.get_payroll_run_detail(run_id)
        return export_decision_receipt(
            corridor=detail["corridor"],
            lens="Payroll Readiness",
            route_result=detail["route_recommendation"]["route_payload"],
            payroll_context=payroll_demo.build_receipt_context(run_id),
        )


def section_position(receipt: str, heading: str) -> int:
    position = receipt.find(heading)
    if position == -1:
        raise AssertionError(f"Heading not found: {heading}")
    return position


class ReceiptContractTests(unittest.TestCase):
    def test_structure_stability(self):
        receipt = render_payroll_receipt()

        original_sections = [
            "PAYROLL READINESS",
            "SYSTEM STATUS",
            "SYSTEM STATE",
            "DECISION RULE",
            "Evidence Ladder",
            "Measured Input Posture",
            "Policy Results",
            "ROUTE COMPARISON",
            "CAPITAL IMPACT",
            "Forecast Advisory",
            "HANDOFF RECORD",
            "DECISION LOG",
            "RECEIPT CONTRACT",
        ]
        positions = [section_position(receipt, section) for section in original_sections]
        self.assertEqual(positions, sorted(positions))

        self.assertLess(section_position(receipt, "SYSTEM STATE"), section_position(receipt, "OPERATIONAL CONTEXT"))
        self.assertLess(section_position(receipt, "OPERATIONAL CONTEXT"), section_position(receipt, "DECISION RULE"))
        self.assertLess(section_position(receipt, "Policy Results"), section_position(receipt, "DECISION FLIP CONDITIONS"))
        self.assertLess(section_position(receipt, "DECISION FLIP CONDITIONS"), section_position(receipt, "ROUTE COMPARISON"))
        self.assertLess(section_position(receipt, "Forecast Advisory"), section_position(receipt, "ALTERNATIVE PATHS"))
        self.assertLess(section_position(receipt, "ALTERNATIVE PATHS"), section_position(receipt, "HANDOFF RECORD"))

    def test_backward_compatibility_old_style_context(self):
        route_result = {
            "request_id": "req_legacy_receipt",
            "decision_id": "decision_legacy_receipt",
            "timestamp": "2026-03-15T11:00:00+00:00",
            "global_data_status": "degraded",
            "token": "USDC",
            "coverage_state": "ACTIVE_COVERAGE",
            "corridor_best_supported": {"token": "USDC", "rail": "Polygon", "is_selected_route": True},
            "transfer_winner": "Ethereum",
            "canopy_recommendation": "Polygon",
            "evidence_packet": {"expected_fee_usd": {"data_source": "BigQuery cache", "last_updated_at": None}},
            "rails": [
                {
                    "rail": "Ethereum",
                    "mode": "live_measured",
                    "adversarial_flags": [],
                    "transfer_math": {
                        "amount_usdc": 10000,
                        "network_fee_usd": 0.0649,
                        "routing_bps": 0.0011,
                        "routing_fixed_fee_usd": 0.35,
                        "routing_min_fee_usd": 1.75,
                        "routing_fee_usd": 11.35,
                        "total_fee_usd": 11.4149,
                        "landed_amount_usd": 9988.5851,
                        "provenance": {"landed_amount_usd": "CALCULATED"},
                    },
                    "strategy_assessment": {
                        "strategy_score_label": "60 / 100",
                        "evidence_confidence_label": "59 / 100",
                        "cost_score": 1.0,
                        "liquidity_score": 0.2,
                        "trust_score": 0.59,
                        "strategy_score": 0.598,
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
                    "transfer_math": {
                        "amount_usdc": 10000,
                        "network_fee_usd": 0.01,
                        "routing_bps": 0.0014,
                        "routing_fixed_fee_usd": 0.20,
                        "routing_min_fee_usd": 1.25,
                        "routing_fee_usd": 14.2,
                        "total_fee_usd": 14.21,
                        "landed_amount_usd": 9985.79,
                        "provenance": {"landed_amount_usd": "CALCULATED"},
                    },
                    "strategy_assessment": {
                        "strategy_score_label": "96 / 100",
                        "evidence_confidence_label": "91 / 100",
                        "cost_score": 0.95,
                        "liquidity_score": 1.0,
                        "trust_score": 0.91,
                        "strategy_score": 0.962,
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
        payroll_context = {
            "payroll_run_id": "legacy-run",
            "client_name": "Sample Legacy Settlement",
            "payroll_date": "2026-04-01",
            "readiness_state": "Hold",
            "risk_level": "High",
            "recommended_action": "Hold",
            "last_evaluation_at": "2026-03-15T11:05:00+00:00",
            "decision_context": {
                "transfer_amount_usd": 100000,
                "payroll_currency": "USD",
                "required_arrival_at": None,
                "effective_deadline_at": "2026-04-01T09:00:00-06:00",
                "override_buffer_percent": None,
            },
            "evaluation_log_summary": {
                "outputs": {
                    "buffer_range_min": 0.13,
                    "buffer_range_max": 0.18,
                    "selected_rail": "Polygon",
                    "readiness_state": "HOLD",
                }
            },
            "system_status": {
                "operating_mode": "Real",
                "measured_data_source": "BigQuery",
                "last_measured_refresh": None,
                "cache_age_seconds": None,
                "poll_interval_minutes": 5,
                "query_status": "Delayed",
                "bigquery_budget_posture": "Active",
                "kill_switch_status": "Active",
            },
            "system_state": {
                "measured_data": "Not measured",
                "forecast_engine": "Paused",
                "kill_switch": "Active",
                "system_health": "Degraded",
            },
            "top_blockers": ["Liquidity confidence is below threshold"],
            "evidence_ladder": [],
            "measured_snapshot": {"freshness_timestamp": None, "measured_fee_source": "BigQuery cache", "data_status": "initializing", "freshness_level": "unknown"},
            "query_posture": {"families": [], "request_path_note": "Request handlers served cached/materialized state only; no raw BigQuery queries ran on the request path."},
            "policy_checks": [],
            "route_comparison": [],
            "capital_impact": {"current_buffer_percent": 0.09, "new_buffer_percent": 0.16, "capital_released": 0.0},
            "forecast_advisory": {"status": "KILL_SWITCH_TRIGGERED"},
            "forecast_action_path": {},
            "handoff_record": {},
            "decision_log": [],
            "decision_rule": {"title": "Payroll readiness release rule", "condition": "legacy condition", "logic": "legacy logic", "result": "Hold"},
            "approval_boundary_note": "Canopy records the decision. Execution stays outside the product.",
        }

        receipt = export_decision_receipt(
            corridor="US -> Mexico",
            lens="Payroll Readiness",
            route_result=route_result,
            payroll_context=payroll_context,
        )

        self.assertIn("Receipt Contract Version: 1.1", receipt)
        self.assertIn("Readiness State: Hold", receipt)
        self.assertIn("Recommended Action: Hold", receipt)
        self.assertIn("OPERATIONAL CONTEXT", receipt)
        self.assertIn("Display Status: Hold", receipt)
        self.assertIn("Decision Confidence: —", receipt)

    def test_determinism(self):
        with patch(
            "services.payroll_demo.get_route",
            return_value=build_route_payload(recommended_rail="Polygon", liquidity_score=0.72),
        ), patch(
            "services.payroll_demo.run_corridor_forecast",
            return_value=FakeForecastResponse(build_forecast_payload()),
        ), patch("services.payroll_demo.get_query_metrics_snapshot", return_value=build_query_metrics()):
            detail = payroll_demo.get_payroll_run_detail("ph-2026-04-04")
            payroll_context = payroll_demo.build_receipt_context("ph-2026-04-04")
        payroll_context["last_evaluation_at"] = "2026-03-30T15:05:00+00:00"

        receipt_one = export_decision_receipt(
            corridor=detail["corridor"],
            lens="Payroll Readiness",
            route_result=detail["route_recommendation"]["route_payload"],
            payroll_context=payroll_context,
        )
        receipt_two = export_decision_receipt(
            corridor=detail["corridor"],
            lens="Payroll Readiness",
            route_result=detail["route_recommendation"]["route_payload"],
            payroll_context=payroll_context,
        )
        self.assertEqual(receipt_one, receipt_two)

    def test_decision_integrity(self):
        with patch(
            "services.payroll_demo.get_route",
            return_value=build_route_payload(recommended_rail="Polygon", liquidity_score=0.52),
        ), patch(
            "services.payroll_demo.run_corridor_forecast",
            return_value=FakeForecastResponse(build_forecast_payload()),
        ), patch("services.payroll_demo.get_query_metrics_snapshot", return_value=build_query_metrics()):
            detail = payroll_demo.get_payroll_run_detail("ng-2026-03-31")
            receipt = export_decision_receipt(
                corridor=detail["corridor"],
                lens="Payroll Readiness",
                route_result=detail["route_recommendation"]["route_payload"],
                payroll_context=payroll_demo.build_receipt_context("ng-2026-03-31"),
            )

        self.assertIn(f"Readiness State: {detail['readiness_label']}", receipt)
        self.assertIn(f"Risk Level: {detail['risk_label']}", receipt)
        self.assertIn(f"Recommended Action: {detail['recommended_action_label']}", receipt)
        self.assertIn(f"Result: {detail['decision_rule']['result']}", receipt)

    def test_operational_context_rendering(self):
        receipt = render_payroll_receipt()
        self.assertIn("OPERATIONAL CONTEXT", receipt)
        self.assertIn("Display Status: Evidence-Limited Hold", receipt)
        self.assertIn("System State: Measured evidence delayed", receipt)
        self.assertIn("Corridor State: Approve", receipt)
        self.assertIn("Immediate Next Step: Refresh measured evidence and rerun readiness before approving.", receipt)
        self.assertIn("Decision Confidence: Moderate", receipt)

    def test_capital_impact_expansion(self):
        receipt = render_payroll_receipt()
        self.assertIn("Current Buffer: 10.0%", receipt)
        self.assertIn("Recommended Buffer: 8.0%", receipt)
        self.assertIn("Capital Released: $2,000.00", receipt)
        self.assertIn("Selected Buffer Percent: 8.0%", receipt)
        self.assertIn("Selected Buffer Amount: $8,000.00", receipt)
        self.assertIn("Capital Direction: Released", receipt)
        self.assertIn("Additional Prefunding Required: $0.00", receipt)
        self.assertIn("Yield Opportunity (Illustrative): $90.00 annually at Illustrative 4.5% annual T-bill yield — Illustrative, not live market data", receipt)
        self.assertIn("Effective Deadline: 2026-04-03T18:30:00+08:00", receipt)
        self.assertIn("Time Until Cutoff: 465 minutes", receipt)
        self.assertIn("Safe Operating Range: 6.0% - 11.0%", receipt)

    def test_decision_flip_conditions_and_alternative_paths(self):
        receipt = render_payroll_receipt()
        self.assertIn("DECISION FLIP CONDITIONS", receipt)
        self.assertIn("Measured route freshness returns to current", receipt)
        self.assertIn("Current State: Measured evidence delayed", receipt)
        self.assertIn("Target State: Current measured evidence", receipt)
        self.assertIn("ALTERNATIVE PATHS", receipt)
        self.assertIn("Path 1", receipt)
        self.assertIn("Action: Wait for the next measured refresh and rerun readiness", receipt)
        self.assertIn("Path 2", receipt)
        self.assertIn("Action: Keep Ethereum ready as the fallback rail", receipt)

    def test_decision_log_event_type_compatibility(self):
        route_result = {
            "request_id": "req_events_123",
            "decision_id": "decision_events_456",
            "timestamp": "2026-03-15T11:00:00+00:00",
            "global_data_status": "ok",
            "token": "USDC",
            "coverage_state": "ACTIVE_COVERAGE",
            "corridor_best_supported": {"token": "USDC", "rail": "Polygon", "is_selected_route": True},
            "transfer_winner": "Polygon",
            "canopy_recommendation": "Polygon",
            "evidence_packet": {"expected_fee_usd": {"data_source": "BigQuery cache", "last_updated_at": "2026-03-15T11:00:00+00:00"}},
            "rails": [
                {
                    "rail": "Polygon",
                    "mode": "live_measured",
                    "adversarial_flags": [],
                    "transfer_math": {
                        "amount_usdc": 10000,
                        "network_fee_usd": 0.01,
                        "routing_bps": 0.0014,
                        "routing_fixed_fee_usd": 0.20,
                        "routing_min_fee_usd": 1.25,
                        "routing_fee_usd": 14.2,
                        "total_fee_usd": 14.21,
                        "landed_amount_usd": 9985.79,
                        "provenance": {"landed_amount_usd": "CALCULATED"},
                    },
                    "strategy_assessment": {
                        "strategy_score_label": "96 / 100",
                        "evidence_confidence_label": "91 / 100",
                        "cost_score": 0.95,
                        "liquidity_score": 1.0,
                        "trust_score": 0.91,
                        "strategy_score": 0.962,
                        "liquidity_penalty_factor": 1.0,
                        "trust_penalty_factor": 1.0,
                        "provenance": {
                            "strategy_score": "MODELED",
                            "evidence_confidence": "MODELED",
                        },
                    },
                }
            ],
        }
        payroll_context = {
            "payroll_run_id": "event-run",
            "client_name": "Sample Event Settlement",
            "payroll_date": "2026-04-01",
            "readiness_state": "Hold",
            "risk_level": "High",
            "recommended_action": "Hold",
            "decision_context": {"transfer_amount_usd": 100000, "payroll_currency": "USD", "effective_deadline_at": "2026-04-01T09:00:00-06:00"},
            "evaluation_log_summary": {"outputs": {"buffer_range_min": 0.13, "buffer_range_max": 0.18, "selected_rail": "Polygon", "readiness_state": "HOLD"}},
            "system_status": {},
            "system_state": {},
            "top_blockers": [],
            "evidence_ladder": [],
            "measured_snapshot": {},
            "query_posture": {"families": [], "request_path_note": "cached only"},
            "policy_checks": [],
            "decision_flip_conditions": [],
            "route_comparison": [],
            "capital_impact": {},
            "forecast_advisory": {},
            "forecast_action_path": {},
            "alternative_paths": [],
            "handoff_record": {},
            "decision_rule": {"title": "rule", "condition": "condition", "logic": "logic", "result": "Hold"},
            "decision_log": [
                {
                    "entry_type": "decision",
                    "decision_timestamp": "2026-03-26T17:39:00+00:00",
                    "decision_action": "HOLD",
                    "decision_reason": "Liquidity below threshold",
                    "decision_owner": "Operations",
                    "decision_rule": "Liquidity score below minimum threshold",
                },
                {
                    "entry_type": "payroll_data_event",
                    "decision_timestamp": "2026-03-26T11:29:00+00:00",
                    "event_name": "Payroll file loaded",
                    "record_count": 142,
                    "beneficiary_change_count": 3,
                    "source_type_label": "BigQuery",
                    "file_name": "payroll.csv",
                },
                {
                    "entry_type": "refresh_event",
                    "decision_timestamp": "2026-03-26T12:00:00+00:00",
                    "event_name": "Measured refresh completed",
                    "detail": "Cache age returned to current range.",
                },
            ],
            "approval_boundary_note": "Canopy records the decision. Execution stays outside the product.",
        }

        receipt = export_decision_receipt(
            corridor="US -> Mexico",
            lens="Payroll Readiness",
            route_result=route_result,
            payroll_context=payroll_context,
        )

        self.assertIn("Event Type: Decision", receipt)
        self.assertIn("Action: HOLD", receipt)
        self.assertIn("Event Type: Payroll Data Update", receipt)
        self.assertIn("Records Loaded: 142", receipt)
        self.assertIn("Source: BigQuery", receipt)
        self.assertIn("Event Type: Measured Refresh", receipt)
        self.assertIn("Detail: Cache age returned to current range.", receipt)


if __name__ == "__main__":
    unittest.main()
