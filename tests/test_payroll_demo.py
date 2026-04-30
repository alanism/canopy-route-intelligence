import base64
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from api import cache
from api import demo_store
from services.export_receipt import export_decision_receipt
from services import payroll_demo
from services.request_context import reset_request_id, set_request_id


class FakeForecastResponse:
    def __init__(self, payload):
        self._payload = payload

    def model_dump(self):
        return dict(self._payload)


def build_route_payload(
    *,
    recommended_rail="Polygon",
    alternative_rail="Ethereum",
    liquidity_score=0.52,
    freshness_level="fresh",
    data_status="fresh",
    timestamp="2026-03-30T15:00:00+00:00",
    amount_usdc=2_450_000,
):
    rails = [
        {
            "rail": "Polygon",
            "mode": "live_measured",
            "data_status": data_status if recommended_rail == "Polygon" else "fresh",
            "freshness_level": freshness_level if recommended_rail == "Polygon" else "fresh",
            "cache_age_seconds": 300,
            "adversarial_flags": [],
            "estimated_fee_usd": 0.12,
            "liquidity_proxy_label": "Liquidity Proxy",
            "liquidity_proxy_detail": "Measured transfer activity is acceptable but not comfortably above the payroll threshold.",
            "liquidity_score_v4": liquidity_score if recommended_rail == "Polygon" else 0.83,
            "evidence_confidence_label": "Moderate",
            "strategy_assessment": {
                "strategy_score": 0.61,
                "cost_score": 0.88,
                "liquidity_score": 0.52,
                "trust_score": 0.72,
                "liquidity_penalty_factor": 1.0,
                "trust_penalty_factor": 1.0,
                "strategy_score_label": "61 / 100",
                "evidence_confidence_label": "Moderate",
                "provenance": {
                    "strategy_score": "MODELED",
                    "evidence_confidence": "MODELED",
                },
            },
            "transfer_math": {
                "amount_usdc": amount_usdc,
                "network_fee_usd": 0.12,
                "routing_bps": 0.0014,
                "routing_fixed_fee_usd": 0.2,
                "routing_min_fee_usd": 1.25,
                "routing_fee_usd": round(amount_usdc * 0.0014 + 0.2, 2),
                "total_fee_usd": round(amount_usdc * 0.0014 + 0.32, 2),
                "landed_amount_usd": round(amount_usdc - (amount_usdc * 0.0014 + 0.32), 2),
                "provenance": {"landed_amount_usd": "CALCULATED"},
            },
        },
        {
            "rail": "Ethereum",
            "mode": "live_measured",
            "data_status": data_status if recommended_rail == "Ethereum" else "fresh",
            "freshness_level": freshness_level if recommended_rail == "Ethereum" else "fresh",
            "cache_age_seconds": 300,
            "adversarial_flags": [],
            "estimated_fee_usd": 7.8,
            "liquidity_proxy_label": "Liquidity Proxy",
            "liquidity_proxy_detail": "Measured activity is more expensive but deeper and easier to defend operationally.",
            "liquidity_score_v4": liquidity_score if recommended_rail == "Ethereum" else 0.83,
            "evidence_confidence_label": "Strong",
            "strategy_assessment": {
                "strategy_score": 0.74,
                "cost_score": 0.43,
                "liquidity_score": 0.83,
                "trust_score": 0.86,
                "liquidity_penalty_factor": 1.0,
                "trust_penalty_factor": 1.0,
                "strategy_score_label": "74 / 100",
                "evidence_confidence_label": "Strong",
                "provenance": {
                    "strategy_score": "MODELED",
                    "evidence_confidence": "MODELED",
                },
            },
            "transfer_math": {
                "amount_usdc": amount_usdc,
                "network_fee_usd": 7.8,
                "routing_bps": 0.0011,
                "routing_fixed_fee_usd": 0.35,
                "routing_min_fee_usd": 1.75,
                "routing_fee_usd": round(amount_usdc * 0.0011 + 0.35, 2),
                "total_fee_usd": round(amount_usdc * 0.0011 + 8.15, 2),
                "landed_amount_usd": round(amount_usdc - (amount_usdc * 0.0011 + 8.15), 2),
                "provenance": {"landed_amount_usd": "CALCULATED"},
            },
        },
    ]
    return {
        "timestamp": timestamp,
        "request_id": "req_payroll_123",
        "decision_id": "decision_payroll_123",
        "token": "USDC",
        "coverage_state": "ACTIVE_COVERAGE",
        "global_data_status": "ok" if freshness_level == "fresh" and data_status == "fresh" else "degraded",
        "corridor_best_supported": {
            "token": "USDC",
            "rail": recommended_rail,
            "is_selected_route": True,
        },
        "scenario": {"current_rail_fee_pct": 1.38},
        "transfer_winner": alternative_rail if recommended_rail == "Ethereum" else recommended_rail,
        "canopy_recommendation": recommended_rail,
        "recommended_rail": recommended_rail,
        "alternative_rail": alternative_rail,
        "expected_landed_amount_label": f"${round(amount_usdc - (amount_usdc * 0.0014 + 0.32), 2):,.2f}",
        "strategy_score_label": "74 / 100" if recommended_rail == "Ethereum" else "61 / 100",
        "evidence_confidence_label": "Strong" if recommended_rail == "Ethereum" else "Moderate",
        "why_this_route": [
            "Ethereum is the fallback recommendation because measured liquidity confidence is stronger than the cheaper path."
            if recommended_rail == "Ethereum"
            else "Polygon remains the modeled recommendation because cost and measured freshness still clear the threshold."
        ],
        "evidence_packet": {
            "expected_fee_usd": {
                "data_source": "BigQuery cache",
                "last_updated_at": timestamp,
            }
        },
        "rails": rails,
    }


def build_forecast_payload(*, kill_switch_triggered=False, status="Advisory only"):
    return {
        "corridor_id": "US-NG",
        "corridor_label": "US -> Nigeria",
        "corridor_stability_probability": 0.62,
        "liquidity_shock_risk": 0.37,
        "transfer_slippage_probability": 0.29,
        "fx_volatility_signal": 0.54,
        "regulatory_risk_index": 0.73,
        "demand_growth_forecast": 6.1,
        "corridor_health_score": 48.0,
        "kill_switch_triggered": kill_switch_triggered,
        "kill_switches": ["WASH_TRADING_DETECTION"] if kill_switch_triggered else [],
        "alerts": ["Advisory only"],
        "status": status,
        "forecast_freshness": {"generated_at": "2026-03-30T15:02:00+00:00"},
    }


def build_query_metrics():
    return {
        "overall": {"execution_bytes": 210000000},
        "families": {
            "fee_activity": {
                "query_count": 4,
                "execution_bytes": 180000000,
                "max_budget_utilization": 0.72,
                "last_seen": "2026-03-30T14:58:00+00:00",
            },
            "corridor_volume": {
                "query_count": 2,
                "execution_bytes": 30000000,
                "max_budget_utilization": 0.18,
                "last_seen": "2026-03-30T14:57:00+00:00",
            },
        },
    }


def build_ready_refresh_state(timestamp="2026-03-30T16:00:00+00:00"):
    return {
        "status": "ready",
        "label": "Current",
        "indicator": "green",
        "last_measured_refresh": timestamp,
        "last_error": None,
        "is_querying": False,
    }


def build_ready_cache_payload(timestamp="2026-03-30T16:00:00+00:00"):
    return {
        "status": "ok",
        "last_updated": timestamp,
    }


class PayrollDemoTests(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self._original_db_path = demo_store.DB_PATH
        demo_store.DB_PATH = Path(self._tmpdir.name) / "payroll.sqlite3"

    def tearDown(self):
        demo_store.DB_PATH = self._original_db_path
        self._tmpdir.cleanup()

    def test_overview_sorts_nigeria_first(self):
        def route_side_effect(**kwargs):
            if kwargs["destination"] == "NG":
                return build_route_payload(recommended_rail="Polygon", liquidity_score=0.52)
            return build_route_payload(
                recommended_rail="Polygon",
                liquidity_score=0.88,
                timestamp="2026-04-01T11:00:00+00:00",
            )

        with patch("services.payroll_demo.get_route", side_effect=route_side_effect), patch(
            "services.payroll_demo.run_corridor_forecast",
            side_effect=lambda corridor_key: FakeForecastResponse(build_forecast_payload()),
        ), patch("services.payroll_demo.get_query_metrics_snapshot", return_value=build_query_metrics()):
            overview = payroll_demo.get_overview()

        self.assertEqual(overview["top_line_run"]["id"], "ng-2026-03-31")
        self.assertIn("Nigeria", overview["top_line_answer"])

    def test_nigeria_run_is_hold_with_blockers(self):
        with patch(
            "services.payroll_demo.get_route",
            return_value=build_route_payload(recommended_rail="Polygon", liquidity_score=0.52),
        ), patch(
            "services.payroll_demo.run_corridor_forecast",
            return_value=FakeForecastResponse(build_forecast_payload()),
        ), patch("services.payroll_demo.get_query_metrics_snapshot", return_value=build_query_metrics()):
            detail = payroll_demo.get_payroll_run_detail("ng-2026-03-31")

        self.assertEqual(detail["readiness_state"], "HOLD")
        blocker_labels = [item["label"] for item in detail["blockers"]]
        self.assertIn("Funding arrived inside the cutoff risk window", blocker_labels)
        self.assertIn("Beneficiary ownership changes require manual review", blocker_labels)

    def test_stale_measured_route_downgrades_ready_run(self):
        with patch(
            "services.payroll_demo.get_route",
            return_value=build_route_payload(
                recommended_rail="Polygon",
                liquidity_score=0.88,
                freshness_level="critical",
                data_status="error",
            ),
        ), patch(
            "services.payroll_demo.run_corridor_forecast",
            return_value=FakeForecastResponse(build_forecast_payload()),
        ), patch("services.payroll_demo.get_query_metrics_snapshot", return_value=build_query_metrics()):
            detail = payroll_demo.get_payroll_run_detail("br-2026-04-02")

        self.assertEqual(detail["readiness_state"], "HOLD")
        self.assertIn("Measured route evidence is stale or degraded", [item["label"] for item in detail["blockers"]])

    def test_evidence_limited_hold_is_additive_to_base_readiness_state(self):
        with patch(
            "services.payroll_demo.get_route",
            return_value=build_route_payload(recommended_rail="Polygon", liquidity_score=0.87),
        ), patch(
            "services.payroll_demo.run_corridor_forecast",
            return_value=FakeForecastResponse(build_forecast_payload()),
        ), patch(
            "services.payroll_demo.get_query_metrics_snapshot",
            return_value=build_query_metrics(),
        ), patch(
            "services.payroll_demo.runtime_cache.get_refresh_state",
            return_value={
                "status": "idle",
                "label": "Not measured",
                "indicator": "gray",
                "last_measured_refresh": None,
                "last_error": None,
                "is_querying": False,
            },
        ), patch(
            "services.payroll_demo.runtime_cache.get_cache",
            return_value={"status": "degraded", "last_updated": None},
        ), patch(
            "services.payroll_demo.runtime_cache.get_cache_age_seconds",
            return_value=None,
        ):
            detail = payroll_demo.get_payroll_run_detail("mx-2026-04-01")

        self.assertEqual(detail["readiness_state"], "READY_FOR_APPROVAL")
        self.assertEqual(detail["decision_surface"]["display_decision_label"], "Evidence-Limited Hold")
        self.assertEqual(detail["decision_surface"]["system_state_code"], "evidence_delayed")
        self.assertTrue(detail["decision_surface"]["is_evidence_limited"])

    def test_healthy_system_uses_corridor_specific_decision_surface_labels(self):
        fresh_timestamp = "2026-03-30T16:00:00+00:00"

        with patch(
            "services.payroll_demo.get_route",
            return_value=build_route_payload(
                recommended_rail="Polygon",
                liquidity_score=0.52,
                freshness_level="fresh",
                data_status="fresh",
                timestamp=fresh_timestamp,
            ),
        ), patch(
            "services.payroll_demo.run_corridor_forecast",
            return_value=FakeForecastResponse(build_forecast_payload()),
        ), patch(
            "services.payroll_demo.get_query_metrics_snapshot",
            return_value=build_query_metrics(),
        ), patch(
            "services.payroll_demo.runtime_cache.get_refresh_state",
            return_value=build_ready_refresh_state(fresh_timestamp),
        ), patch(
            "services.payroll_demo.runtime_cache.get_cache",
            return_value=build_ready_cache_payload(fresh_timestamp),
        ), patch(
            "services.payroll_demo.runtime_cache.get_cache_age_seconds",
            return_value=60,
        ):
            detail = payroll_demo.get_payroll_run_detail("ng-2026-03-31")

        self.assertEqual(detail["readiness_state"], "HOLD")
        self.assertEqual(detail["decision_surface"]["system_state_code"], "healthy")
        self.assertEqual(detail["decision_surface"]["corridor_state_label"], "Conditional hold")
        self.assertEqual(detail["decision_surface"]["display_decision_label"], "Conditional Hold")
        self.assertFalse(detail["decision_surface"]["is_evidence_limited"])

    def test_capital_impact_reports_required_and_released_capital(self):
        fresh_timestamp = "2026-03-30T16:00:00+00:00"

        with patch(
            "services.payroll_demo.runtime_cache.get_refresh_state",
            return_value=build_ready_refresh_state(fresh_timestamp),
        ), patch(
            "services.payroll_demo.runtime_cache.get_cache",
            return_value=build_ready_cache_payload(fresh_timestamp),
        ), patch(
            "services.payroll_demo.runtime_cache.get_cache_age_seconds",
            return_value=60,
        ), patch(
            "services.payroll_demo.run_corridor_forecast",
            return_value=FakeForecastResponse(build_forecast_payload()),
        ), patch(
            "services.payroll_demo.get_query_metrics_snapshot",
            return_value=build_query_metrics(),
        ):
            with patch(
                "services.payroll_demo.get_route",
                return_value=build_route_payload(
                    recommended_rail="Polygon",
                    liquidity_score=0.52,
                    freshness_level="fresh",
                    data_status="fresh",
                    timestamp=fresh_timestamp,
                ),
            ):
                required_detail = payroll_demo.get_payroll_run_detail("mx-2026-04-01")

            with patch(
                "services.payroll_demo.get_route",
                return_value=build_route_payload(
                    recommended_rail="Polygon",
                    liquidity_score=0.87,
                    freshness_level="fresh",
                    data_status="fresh",
                    timestamp=fresh_timestamp,
                ),
            ):
                released_detail = payroll_demo.get_payroll_run_detail("ph-2026-04-04")

        self.assertEqual(required_detail["capital_impact"]["capital_delta_direction"], "required")
        self.assertGreater(required_detail["capital_impact"]["additional_prefunding_required"], 0)
        self.assertEqual(required_detail["capital_impact"]["recommended_buffer_amount"], 16000.0)
        self.assertEqual(released_detail["capital_impact"]["capital_delta_direction"], "released")
        self.assertGreater(released_detail["capital_impact"]["capital_released"], 0)
        self.assertGreater(released_detail["capital_impact"]["yield_opportunity_estimate_annual"], 0)

    def test_non_approve_runs_have_flip_conditions_and_alternative_paths(self):
        fresh_timestamp = "2026-03-30T16:00:00+00:00"

        with patch(
            "services.payroll_demo.get_route",
            return_value=build_route_payload(
                recommended_rail="Polygon",
                liquidity_score=0.52,
                freshness_level="fresh",
                data_status="fresh",
                timestamp=fresh_timestamp,
            ),
        ), patch(
            "services.payroll_demo.run_corridor_forecast",
            return_value=FakeForecastResponse(build_forecast_payload()),
        ), patch(
            "services.payroll_demo.get_query_metrics_snapshot",
            return_value=build_query_metrics(),
        ), patch(
            "services.payroll_demo.runtime_cache.get_refresh_state",
            return_value=build_ready_refresh_state(fresh_timestamp),
        ), patch(
            "services.payroll_demo.runtime_cache.get_cache",
            return_value=build_ready_cache_payload(fresh_timestamp),
        ), patch(
            "services.payroll_demo.runtime_cache.get_cache_age_seconds",
            return_value=60,
        ):
            detail = payroll_demo.get_payroll_run_detail("ng-2026-03-31")

        flip_labels = {item["label"] for item in detail["decision_flip_conditions"]}
        alternative_keys = {item["key"] for item in detail["alternative_paths"]}
        self.assertGreaterEqual(len(detail["alternative_paths"]), 2)
        self.assertIn("Beneficiary review clears", flip_labels)
        self.assertIn("Liquidity threshold rises above minimum", flip_labels)
        self.assertIn("Cutoff buffer returns above minimum", flip_labels)
        self.assertIn("clear_beneficiary", alternative_keys)
        self.assertIn("fallback_rail", alternative_keys)

    def test_south_africa_run_is_seeded_for_simulation(self):
        with patch(
            "services.payroll_demo.get_route",
            return_value=build_route_payload(recommended_rail="Ethereum", liquidity_score=0.83, amount_usdc=5_600_000),
        ), patch(
            "services.payroll_demo.run_corridor_forecast",
            return_value=FakeForecastResponse(build_forecast_payload()),
        ), patch("services.payroll_demo.get_query_metrics_snapshot", return_value=build_query_metrics()):
            detail = payroll_demo.get_payroll_run_detail("za-2026-04-03")

        self.assertEqual(detail["currency"], "USD")
        self.assertEqual(detail["decision_context"]["transfer_amount_usd"], 100_000.0)
        self.assertEqual(detail["decision_context"]["payroll_currency"], "USD")
        self.assertTrue(detail["route_comparison"])

    def test_new_seeded_runs_appear_in_payroll_run_list(self):
        with patch(
            "services.payroll_demo.get_route",
            return_value=build_route_payload(recommended_rail="Polygon", liquidity_score=0.83),
        ), patch(
            "services.payroll_demo.run_corridor_forecast",
            return_value=FakeForecastResponse(build_forecast_payload()),
        ), patch("services.payroll_demo.get_query_metrics_snapshot", return_value=build_query_metrics()):
            items = payroll_demo.list_payroll_runs()

        run_ids = {item["id"] for item in items}
        self.assertIn("mx-2026-04-01", run_ids)
        self.assertIn("ph-2026-04-04", run_ids)
        self.assertIn("sg-vn-2026-04-07", run_ids)

    def test_singapore_vietnam_run_uses_first_class_corridor_and_supports_vnd(self):
        captured = []

        def route_side_effect(**kwargs):
            captured.append(kwargs)
            return build_route_payload(
                recommended_rail="Ethereum",
                alternative_rail="Polygon",
                liquidity_score=0.77,
                amount_usdc=kwargs["amount_usdc"],
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            original_db_path = demo_store.DB_PATH
            try:
                demo_store.DB_PATH = Path(tmpdir) / "payroll.sqlite3"
                with patch("services.payroll_demo.get_route", side_effect=route_side_effect), patch(
                    "services.payroll_demo.run_corridor_forecast",
                    return_value=FakeForecastResponse(build_forecast_payload()),
                ), patch("services.payroll_demo.get_query_metrics_snapshot", return_value=build_query_metrics()):
                    detail = payroll_demo.get_payroll_run_detail("sg-vn-2026-04-07")
                    evaluated = payroll_demo.evaluate_payroll_run(
                        "sg-vn-2026-04-07",
                        transfer_amount_usd=1_480_000,
                        required_arrival_at="2026-04-06T14:00:00+07:00",
                        payroll_currency="VND",
                    )
            finally:
                demo_store.DB_PATH = original_db_path

        self.assertEqual(detail["corridor"], "Singapore -> Vietnam")
        self.assertEqual(detail["corridor_key"], "SG-VN")
        self.assertEqual(captured[0]["origin"], "SG")
        self.assertEqual(captured[0]["destination"], "VN")
        self.assertEqual(evaluated["run"]["decision_context"]["payroll_currency"], "VND")

    def test_mexico_run_detail_is_ready_for_approval(self):
        with patch(
            "services.payroll_demo.get_route",
            return_value=build_route_payload(recommended_rail="Polygon", liquidity_score=0.87),
        ), patch(
            "services.payroll_demo.run_corridor_forecast",
            return_value=FakeForecastResponse(build_forecast_payload()),
        ), patch("services.payroll_demo.get_query_metrics_snapshot", return_value=build_query_metrics()):
            detail = payroll_demo.get_payroll_run_detail("mx-2026-04-01")

        self.assertEqual(detail["corridor"], "US -> Mexico")
        self.assertEqual(detail["readiness_state"], "READY_FOR_APPROVAL")

    def test_evaluate_payroll_run_persists_latest_state_and_receipt_inputs(self):
        seen_requests = []

        def route_side_effect(**kwargs):
            seen_requests.append(kwargs)
            return build_route_payload(
                recommended_rail="Ethereum",
                alternative_rail="Polygon",
                liquidity_score=0.79,
                amount_usdc=kwargs["amount_usdc"],
                timestamp="2026-04-02T08:15:00+00:00",
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            original_db_path = demo_store.DB_PATH
            try:
                demo_store.DB_PATH = Path(tmpdir) / "payroll.sqlite3"
                with patch("services.payroll_demo.get_route", side_effect=route_side_effect), patch(
                    "services.payroll_demo.run_corridor_forecast",
                    return_value=FakeForecastResponse(build_forecast_payload()),
                ), patch("services.payroll_demo.get_query_metrics_snapshot", return_value=build_query_metrics()):
                    evaluated = payroll_demo.evaluate_payroll_run(
                        "za-2026-04-03",
                        transfer_amount_usd=10_000_000,
                        required_arrival_at="2026-04-03T09:00:00+02:00",
                        payroll_currency="ZAR",
                    )
                    receipt = export_decision_receipt(
                        corridor=evaluated["run"]["corridor"],
                        lens="Payroll Readiness",
                        route_result=evaluated["run"]["route_recommendation"]["route_payload"],
                        payroll_context=payroll_demo.build_receipt_context("za-2026-04-03"),
                    )
                    evaluations = demo_store.list_payroll_evaluations(payroll_run_id="za-2026-04-03")
            finally:
                demo_store.DB_PATH = original_db_path

        self.assertEqual(seen_requests[-1]["amount_usdc"], 10_000_000)
        self.assertEqual(seen_requests[-1]["time_sensitivity"], "standard")
        self.assertEqual(evaluated["run"]["decision_context"]["payroll_currency"], "ZAR")
        self.assertEqual(evaluations[0]["transfer_amount_usd"], 10_000_000)
        self.assertEqual(evaluated["run"]["route_comparison"][0]["rail"], "Ethereum")
        self.assertGreater(evaluated["run"]["capital_impact"]["capital_released"], 0)
        self.assertIn("LATEST EVALUATION", receipt)
        self.assertIn("Transfer Amount: $10,000,000.00", receipt)
        self.assertIn("Selected Rail: Ethereum", receipt)

    def test_blank_deadline_uses_default_cutoff_and_urgent_deadline_changes_arrival_estimates(self):
        captured = []

        def route_side_effect(**kwargs):
            captured.append(kwargs)
            return build_route_payload(
                recommended_rail="Polygon",
                alternative_rail="Ethereum",
                liquidity_score=0.67,
                amount_usdc=kwargs["amount_usdc"],
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            original_db_path = demo_store.DB_PATH
            try:
                demo_store.DB_PATH = Path(tmpdir) / "payroll.sqlite3"
                with patch("services.payroll_demo.get_route", side_effect=route_side_effect), patch(
                    "services.payroll_demo.run_corridor_forecast",
                    return_value=FakeForecastResponse(build_forecast_payload()),
                ), patch("services.payroll_demo.get_query_metrics_snapshot", return_value=build_query_metrics()):
                    default_eval = payroll_demo.evaluate_payroll_run(
                        "za-2026-04-03",
                        transfer_amount_usd=5_600_000,
                        required_arrival_at=None,
                        payroll_currency="ZAR",
                    )
                    urgent_eval = payroll_demo.evaluate_payroll_run(
                        "za-2026-04-03",
                        transfer_amount_usd=5_600_000,
                        required_arrival_at="2026-04-02T13:00:00+02:00",
                        payroll_currency="ZAR",
                    )
                    evaluations = demo_store.list_payroll_evaluations(payroll_run_id="za-2026-04-03")
            finally:
                demo_store.DB_PATH = original_db_path

        self.assertEqual(captured[0]["time_sensitivity"], "standard")
        self.assertEqual(default_eval["run"]["decision_context"]["effective_deadline_at"], "2026-04-03T12:00:00+02:00")
        self.assertEqual(captured[1]["time_sensitivity"], "urgent")
        self.assertEqual(len(evaluations), 2)
        self.assertEqual(urgent_eval["run"]["route_comparison"][0]["estimated_arrival_minutes"], 12)
        self.assertGreater(
            urgent_eval["run"]["buffer_recommendation"]["recommended_buffer_percent"],
            default_eval["run"]["buffer_recommendation"]["recommended_buffer_percent"],
        )

    def test_override_buffer_changes_capital_impact_and_warning(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            original_db_path = demo_store.DB_PATH
            try:
                demo_store.DB_PATH = Path(tmpdir) / "payroll.sqlite3"
                with patch(
                    "services.payroll_demo.get_route",
                    return_value=build_route_payload(recommended_rail="Ethereum", liquidity_score=0.79, amount_usdc=10_000_000),
                ), patch(
                    "services.payroll_demo.run_corridor_forecast",
                    return_value=FakeForecastResponse(build_forecast_payload(kill_switch_triggered=True)),
                ), patch("services.payroll_demo.get_query_metrics_snapshot", return_value=build_query_metrics()):
                    evaluated = payroll_demo.evaluate_payroll_run(
                        "za-2026-04-03",
                        transfer_amount_usd=10_000_000,
                        required_arrival_at="2026-04-03T09:00:00+02:00",
                        payroll_currency="ZAR",
                        override_buffer_percent=16,
                    )
            finally:
                demo_store.DB_PATH = original_db_path

        self.assertEqual(evaluated["run"]["buffer_recommendation"]["override_buffer_percent"], 0.16)
        self.assertEqual(evaluated["run"]["buffer_recommendation"]["override_warning"], "Manual override increases settlement risk.")
        self.assertAlmostEqual(evaluated["run"]["capital_impact"]["new_buffer_percent"], 0.16)

    def test_ingest_payroll_file_updates_provenance_state_and_decision_log(self):
        baseline_snapshot = [
            {
                "beneficiary_id": "BEN-001",
                "name": "Alice Example",
                "account_number": "12345678",
                "routing_code": "0440001",
                "currency": "NGN",
            },
            {
                "beneficiary_id": "BEN-002",
                "name": "Bob Example",
                "account_number": "87654321",
                "routing_code": "0440002",
                "currency": "NGN",
            },
        ]
        uploaded_csv = "\n".join(
            [
                "beneficiary_id,name,account_number,routing_code,currency",
                "BEN-001,Alice Example,12345678,0440001,NGN",
                "BEN-002,Bob Example,99999999,0440002,NGN",
            ]
        )

        demo_store.record_payroll_data_snapshot(
            payroll_run_id="ng-2026-03-31",
            source_type="upload",
            source_label="CSV upload",
            snapshot_format="csv",
            file_name="baseline.csv",
            last_loaded_timestamp="2026-03-26T10:00:00+00:00",
            record_count=2,
            beneficiary_change_count=0,
            verification_status="verified",
            data_status="ready",
            lineage_label="Payroll dataset snapshot -> BigQuery -> Decision Engine",
            snapshot=baseline_snapshot,
            validation_errors=[],
        )

        with patch(
            "services.payroll_demo.get_route",
            return_value=build_route_payload(recommended_rail="Polygon", liquidity_score=0.52),
        ), patch(
            "services.payroll_demo.run_corridor_forecast",
            return_value=FakeForecastResponse(build_forecast_payload()),
        ), patch("services.payroll_demo.get_query_metrics_snapshot", return_value=build_query_metrics()):
            ingested = payroll_demo.ingest_payroll_file(
                "ng-2026-03-31",
                source_type="upload",
                file_name="payroll_update.csv",
                content_base64=base64.b64encode(uploaded_csv.encode("utf-8")).decode("ascii"),
            )

        payroll_data_state = ingested["run"]["payroll_data_state"]
        self.assertEqual(payroll_data_state["source_type"], "upload")
        self.assertEqual(payroll_data_state["record_count"], 2)
        self.assertEqual(payroll_data_state["beneficiary_change_count"], 1)
        self.assertEqual(payroll_data_state["verification_status"], "review_required")
        self.assertEqual(ingested["run"]["decision_log"][0]["entry_type"], "payroll_data_event")
        self.assertEqual(ingested["run"]["decision_log"][0]["event_name"], "Payroll file loaded")
        self.assertEqual(ingested["run"]["decision_log"][0]["source_type_label"], "CSV upload")
        self.assertTrue(
            any(item["title"] == "Payroll Data Source" for item in ingested["run"]["evidence_ladder"])
        )

    def test_ingest_payroll_file_rejects_invalid_records(self):
        invalid_csv = "\n".join(
            [
                "beneficiary_id,name,account_number,routing_code,currency",
                "BEN-001,Alice Example,!!,0440001,NGN",
                "BEN-001,,12345678,,NGN",
            ]
        )

        with patch(
            "services.payroll_demo.get_route",
            return_value=build_route_payload(recommended_rail="Polygon", liquidity_score=0.52),
        ), patch(
            "services.payroll_demo.run_corridor_forecast",
            return_value=FakeForecastResponse(build_forecast_payload()),
        ), patch("services.payroll_demo.get_query_metrics_snapshot", return_value=build_query_metrics()):
            ingested = payroll_demo.ingest_payroll_file(
                "ng-2026-03-31",
                source_type="upload",
                file_name="invalid_payroll.csv",
                content_base64=base64.b64encode(invalid_csv.encode("utf-8")).decode("ascii"),
            )

        payroll_data_state = ingested["run"]["payroll_data_state"]
        self.assertEqual(payroll_data_state["verification_status"], "failed")
        self.assertEqual(payroll_data_state["data_status"], "missing_data")
        self.assertGreaterEqual(len(payroll_data_state["validation_errors"]), 3)
        self.assertTrue(
            any("duplicate beneficiary_id" in item for item in payroll_data_state["validation_errors"])
        )
        self.assertTrue(
            any("invalid account format" in item for item in payroll_data_state["validation_errors"])
        )

    def test_evaluate_run_stays_off_bigquery_on_request_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            original_db_path = demo_store.DB_PATH
            demo_store.DB_PATH = Path(tmpdir) / "payroll.sqlite3"
            cache.seed_demo_cache()
            token = set_request_id("req_payroll_eval_guard")
            try:
                with patch(
                    "services.corridor_analytics.execute_sql",
                    side_effect=AssertionError("live corridor BigQuery called"),
                ), patch(
                    "services.bigquery_client.run_query",
                    side_effect=AssertionError("raw BigQuery called"),
                ):
                    evaluated = payroll_demo.evaluate_payroll_run(
                        "br-2026-04-02",
                        transfer_amount_usd=1_900_000,
                        required_arrival_at="2026-04-01T17:00:00-03:00",
                        payroll_currency="BRL",
                    )
            finally:
                reset_request_id(token)
                demo_store.DB_PATH = original_db_path

        self.assertEqual(evaluated["status"], "evaluated")
        self.assertEqual(
            evaluated["run"]["query_posture"]["request_path_note"],
            "Request handlers served cached/materialized state only; no raw BigQuery queries ran on the request path.",
        )

    def test_record_run_decision_persists_and_receipt_has_payroll_sections(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            original_db_path = demo_store.DB_PATH
            try:
                demo_store.DB_PATH = Path(tmpdir) / "payroll.sqlite3"
                with patch(
                    "services.payroll_demo.get_route",
                    return_value=build_route_payload(recommended_rail="Ethereum", liquidity_score=0.79),
                ), patch(
                    "services.payroll_demo.run_corridor_forecast",
                    return_value=FakeForecastResponse(build_forecast_payload()),
                ), patch("services.payroll_demo.get_query_metrics_snapshot", return_value=build_query_metrics()):
                    recorded = payroll_demo.record_run_decision(
                        "ng-2026-03-31",
                        action="HOLD",
                        approver="Ops Lead",
                        decision_reason="Beneficiary mismatch",
                    )
                    receipt = export_decision_receipt(
                        corridor=recorded["run"]["corridor"],
                        lens="Payroll Readiness",
                        route_result=recorded["run"]["route_recommendation"]["route_payload"],
                        payroll_context=payroll_demo.build_receipt_context("ng-2026-03-31"),
                    )
            finally:
                demo_store.DB_PATH = original_db_path

        self.assertEqual(recorded["decision"]["action"], "HOLD")
        self.assertEqual(recorded["decision"]["decision_reason"], "Beneficiary mismatch")
        self.assertIn("PAYROLL READINESS", receipt)
        self.assertIn("SYSTEM STATUS", receipt)
        self.assertIn("DECISION RULE", receipt)
        self.assertIn("Policy Threshold:", receipt)
        self.assertIn("DATA LINEAGE", receipt)
        self.assertIn("Measured Input Posture", receipt)
        self.assertIn("Forecasted = advisory only; does not override readiness.", receipt)
        self.assertIn("Request-path note: Request handlers served cached/materialized state only", receipt)

    def test_receipt_context_fills_missing_evaluation_timestamp(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            original_db_path = demo_store.DB_PATH
            try:
                demo_store.DB_PATH = Path(tmpdir) / "payroll.sqlite3"
                with patch(
                    "services.payroll_demo.get_route",
                    return_value=build_route_payload(recommended_rail="Ethereum", liquidity_score=0.79),
                ), patch(
                    "services.payroll_demo.run_corridor_forecast",
                    return_value=FakeForecastResponse(build_forecast_payload()),
                ), patch("services.payroll_demo.get_query_metrics_snapshot", return_value=build_query_metrics()):
                    context = payroll_demo.build_receipt_context("ng-2026-03-31")
                    receipt = export_decision_receipt(
                        corridor="US -> Nigeria",
                        lens="Payroll Readiness",
                        route_result=build_route_payload(recommended_rail="Ethereum", liquidity_score=0.79),
                        payroll_context=context,
                    )
            finally:
                demo_store.DB_PATH = original_db_path

        self.assertIsNotNone(context["last_evaluation_at"])
        self.assertNotIn("Evaluation Timestamp: —", receipt)

    def test_approval_creates_handoff_and_receipt_includes_handoff_record(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            original_db_path = demo_store.DB_PATH
            try:
                demo_store.DB_PATH = Path(tmpdir) / "payroll.sqlite3"
                with patch(
                    "services.payroll_demo.get_route",
                    return_value=build_route_payload(recommended_rail="Ethereum", liquidity_score=0.79),
                ), patch(
                    "services.payroll_demo.run_corridor_forecast",
                    return_value=FakeForecastResponse(build_forecast_payload()),
                ), patch("services.payroll_demo.get_query_metrics_snapshot", return_value=build_query_metrics()):
                    recorded = payroll_demo.record_run_decision(
                        "br-2026-04-02",
                        action="APPROVE",
                        approver="Ops Lead",
                        decision_reason="Manual override",
                    )
                    self.assertEqual(recorded["run"]["handoff_record"]["status"], "Queued")
                    handoff = payroll_demo.trigger_run_handoff("br-2026-04-02")
                    receipt = export_decision_receipt(
                        corridor=handoff["run"]["corridor"],
                        lens="Payroll Readiness",
                        route_result=handoff["run"]["route_recommendation"]["route_payload"],
                        payroll_context=payroll_demo.build_receipt_context("br-2026-04-02"),
                    )
            finally:
                demo_store.DB_PATH = original_db_path

        self.assertEqual(handoff["handoff"]["status"], "Acknowledged")
        self.assertIn("HANDOFF RECORD", receipt)
        self.assertIn("Execution system: Airwallex", receipt)
        self.assertIn("Status: Acknowledged", receipt)


if __name__ == "__main__":
    unittest.main()
