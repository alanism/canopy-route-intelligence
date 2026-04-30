"""Plain-text decision receipt for the Canopy dual-layer model."""

from __future__ import annotations

from typing import List, Optional


RECEIPT_CONTRACT_VERSION = "1.1"


def _fmt_usd(value: float) -> str:
    return f"${value:,.2f}"


def _fmt_pct(value: float) -> str:
    return f"{value * 100:.1f}%"


def _fmt_ratio(value: float) -> str:
    return f"{value:.3f}".rstrip("0").rstrip(".")


def _fmt_minutes(value: object) -> str:
    if value is None or value == "—":
        return "—"
    try:
        return f"{int(float(value))} minutes"
    except (TypeError, ValueError):
        return str(value)


def _fmt_pct_or_dash(value: object) -> str:
    if value is None or value == "—":
        return "—"
    try:
        return _fmt_pct(float(value))
    except (TypeError, ValueError):
        return str(value)


def _fmt_usd_or_dash(value: object) -> str:
    if value is None or value == "—":
        return "—"
    try:
        return _fmt_usd(float(value))
    except (TypeError, ValueError):
        return str(value)


def _humanize_capital_direction(value: object) -> str:
    mapping = {
        "released": "Released",
        "required": "Required",
        "neutral": "Neutral",
    }
    return mapping.get(str(value or "").lower(), str(value or "—"))


def _safe_operating_range_label(capital_impact: dict) -> str:
    safe_range = capital_impact.get("safe_operating_range") or {}
    minimum = safe_range.get("min_percent")
    maximum = safe_range.get("max_percent")
    if minimum is None or maximum is None:
        return "—"
    return f"{_fmt_pct(float(minimum))} - {_fmt_pct(float(maximum))}"


def _yield_opportunity_label(capital_impact: dict) -> str:
    amount = capital_impact.get("yield_opportunity_estimate_annual")
    assumption = capital_impact.get("yield_assumption_label") or "Illustrative — Not live market data"
    if amount is None:
        return "—"
    return f"{_fmt_usd(float(amount))} annually at {assumption} — Illustrative, not live market data"


def _decision_log_event_label(item: dict) -> str:
    event_type = item.get("event_type") or item.get("entry_type") or "decision"
    normalized = str(event_type).lower()
    if normalized in {"decision"}:
        return "Decision"
    if normalized in {"payroll_data_event", "data_load"}:
        return "Payroll Data Update"
    if normalized == "refresh_event":
        return "Measured Refresh"
    if normalized == "system_state_change":
        return "System State Change"
    return str(event_type).replace("_", " ").title()


def export_decision_receipt(
    corridor: str,
    lens: str,
    route_result: dict,
    scenario_artifact: Optional[dict] = None,
    payroll_context: Optional[dict] = None,
) -> str:
    rails = route_result.get("rails", [])
    transfer_winner = next(
        (rail for rail in rails if rail.get("rail") == route_result.get("transfer_winner")),
        None,
    )
    canopy_recommendation = next(
        (rail for rail in rails if rail.get("rail") == route_result.get("canopy_recommendation")),
        None,
    )
    if transfer_winner is None or canopy_recommendation is None:
        raise ValueError("Route payload is missing transfer winner or canopy recommendation.")

    token = route_result.get("token", "USDC")
    evidence_packet = route_result.get("evidence_packet", {})
    corridor_best_supported = route_result.get("corridor_best_supported") or {}
    review_state = scenario_artifact.get("review_state") if scenario_artifact else None
    reviewer = scenario_artifact.get("reviewer") if scenario_artifact else None
    review_notes = scenario_artifact.get("review_notes") if scenario_artifact else None
    follow_up_requested = scenario_artifact.get("follow_up_requested") if scenario_artifact else False
    artifact_type = "Saved scenario artifact" if scenario_artifact else "Live recommendation snapshot"
    lines: List[str] = [
        "CANOPY DECISION RECEIPT",
        "",
        f"Generated: {route_result.get('timestamp', '—')}",
        f"Request ID: {route_result.get('request_id', '—')}",
        f"Decision ID: {route_result.get('decision_id', '—')}",
        f"Receipt Contract Version: {RECEIPT_CONTRACT_VERSION}",
        "",
        f"Corridor: {corridor}",
        f"Lens: {lens}",
        f"Stablecoin: {token}",
        f"Coverage State: {route_result.get('coverage_state', '—')}",
        f"Artifact Type: {artifact_type}",
        f"Scenario Review State: {review_state or 'Not saved'}",
        f"Scenario Reviewer: {reviewer or '—'}",
        f"Follow-up Requested: {'Yes' if follow_up_requested else 'No'}",
        "",
        "LEGEND",
        "Measured = observed chain or freshness data in the current Canopy read",
        "Calculated = fixed math from the current screen inputs",
        "Modeled = weighted strategy logic using cost, liquidity, trust, and integrity penalties",
    ]
    if payroll_context:
        lines.append("Forecasted = advisory corridor-risk signals that do not override the readiness decision")
    lines.extend(
        [
            "",
            "--------------------------------",
            "",
            "SUMMARY",
            "",
            f"Corridor Best Among Supported Routes: {corridor_best_supported.get('token', '—')} on {corridor_best_supported.get('rail', '—')}",
            f"Recommendation Scope: best rail for {token} among Canopy's currently supported routes",
            f"Transfer Winner: {transfer_winner['rail']} [{transfer_winner['transfer_math']['provenance']['landed_amount_usd']}]",
            f"Canopy Recommendation: {canopy_recommendation['rail']} [{canopy_recommendation['strategy_assessment']['provenance']['strategy_score']}]",
            f"Global Data Status: {route_result.get('global_data_status', '—')}",
            f"Selected route is corridor best among supported routes: {corridor_best_supported.get('is_selected_route', False)}",
            "",
            f"Expected Landed Amount: {_fmt_usd(transfer_winner['transfer_math']['landed_amount_usd'])} [{transfer_winner['transfer_math']['provenance']['landed_amount_usd']}]",
            f"Strategy Score: {canopy_recommendation['strategy_assessment']['strategy_score_label']} [{canopy_recommendation['strategy_assessment']['provenance']['strategy_score']}]",
            f"Evidence Confidence: {canopy_recommendation['strategy_assessment']['evidence_confidence_label']} [{canopy_recommendation['strategy_assessment']['provenance']['evidence_confidence']}]",
            f"Evidence Snapshot Source: {evidence_packet.get('expected_fee_usd', {}).get('data_source', '—')}",
            f"Evidence Snapshot Time: {evidence_packet.get('expected_fee_usd', {}).get('last_updated_at', '—')}",
            "",
            "--------------------------------",
            "",
        ]
    )

    if payroll_context:
        decision_surface = payroll_context.get("decision_surface") or {}
        display_status = decision_surface.get("display_decision_label") or payroll_context.get("readiness_state", "—")
        operational_system_state = decision_surface.get("system_state_label") or payroll_context.get("system_state", {}).get("measured_data", "—")
        corridor_state = decision_surface.get("corridor_state_label") or payroll_context.get("recommended_action") or payroll_context.get("readiness_state", "—")
        immediate_next_step = decision_surface.get("next_step") or payroll_context.get("recommended_next_action") or "—"
        why_it_matters = decision_surface.get("why_it_matters") or payroll_context.get("top_line_reason") or "—"
        decision_confidence = payroll_context.get("decision_confidence") or "—"
        lines.extend(
            [
                "PAYROLL READINESS",
                "",
                f"Payroll Run ID: {payroll_context.get('payroll_run_id', '—')}",
                f"Client Name: {payroll_context.get('client_name', '—')}",
                f"Payroll Date: {payroll_context.get('payroll_date', '—')}",
                f"Readiness State: {payroll_context.get('readiness_state', '—')}",
                f"Risk Level: {payroll_context.get('risk_level', '—')}",
                f"Recommended Action: {payroll_context.get('recommended_action', '—')}",
                f"Recorded Operator Action: {payroll_context.get('operator_action', '—')}",
                f"Recorded By: {payroll_context.get('operator_approver', '—')}",
                f"Decision Reason: {payroll_context.get('operator_reason', '—')}",
                f"Decision Reason Detail: {payroll_context.get('operator_reason_other', '—')}",
                "",
                "LATEST EVALUATION",
                "",
                f"Evaluation Timestamp: {payroll_context.get('last_evaluation_at', '—')}",
                f"Transfer Amount: {_fmt_usd(float(payroll_context.get('decision_context', {}).get('transfer_amount_usd', 0.0) or 0.0))}",
                f"Payroll Currency: {payroll_context.get('decision_context', {}).get('payroll_currency', '—')}",
                f"Required Arrival Time: {payroll_context.get('decision_context', {}).get('required_arrival_at', 'Default payroll cutoff') or 'Default payroll cutoff'}",
                f"Effective Deadline: {payroll_context.get('decision_context', {}).get('effective_deadline_at', '—')}",
                f"Override Buffer: {(_fmt_pct(float(payroll_context.get('decision_context', {}).get('override_buffer_percent', 0.0))) if payroll_context.get('decision_context', {}).get('override_buffer_percent') is not None else '—')}",
                f"Evaluated Buffer Range: {_fmt_pct(float(payroll_context.get('evaluation_log_summary', {}).get('outputs', {}).get('buffer_range_min', 0.0) or 0.0))} - {_fmt_pct(float(payroll_context.get('evaluation_log_summary', {}).get('outputs', {}).get('buffer_range_max', 0.0) or 0.0))}",
                f"Selected Rail: {payroll_context.get('evaluation_log_summary', {}).get('outputs', {}).get('selected_rail', '—')}",
                f"Readiness Result: {payroll_context.get('evaluation_log_summary', {}).get('outputs', {}).get('readiness_state', '—')}",
                "",
                "SYSTEM STATUS",
                "",
                f"Operating Mode: {payroll_context.get('system_status', {}).get('operating_mode', '—')}",
                f"Measured Data Source: {payroll_context.get('system_status', {}).get('measured_data_source', '—')}",
                f"Last Measured Refresh: {payroll_context.get('system_status', {}).get('last_measured_refresh', '—')}",
                f"Cache Age: {payroll_context.get('system_status', {}).get('cache_age_seconds', '—')} seconds",
                f"Poll Interval: {payroll_context.get('system_status', {}).get('poll_interval_minutes', '—')} minutes",
                f"Query Status: {payroll_context.get('system_status', {}).get('query_status', '—')}",
                f"BigQuery Budget Posture: {payroll_context.get('system_status', {}).get('bigquery_budget_posture', '—')}",
                f"Kill Switch Status: {payroll_context.get('system_status', {}).get('kill_switch_status', '—')}",
                "",
                "SYSTEM STATE",
                "",
                f"Measured Data: {payroll_context.get('system_state', {}).get('measured_data', '—')}",
                f"Forecast Engine: {payroll_context.get('system_state', {}).get('forecast_engine', '—')}",
                f"Kill Switch: {payroll_context.get('system_state', {}).get('kill_switch', '—')}",
                f"System Health: {payroll_context.get('system_state', {}).get('system_health', '—')}",
                "",
                "OPERATIONAL CONTEXT",
                "",
                f"Display Status: {display_status}",
                f"System State: {operational_system_state}",
                f"Corridor State: {corridor_state}",
                f"Immediate Next Step: {immediate_next_step}",
                f"Why It Matters: {why_it_matters}",
                f"Decision Confidence: {decision_confidence}",
                "",
                "DECISION RULE",
                "",
                f"Decision Rule: {payroll_context.get('decision_rule', {}).get('title', '—')}",
                f"Condition: {payroll_context.get('decision_rule', {}).get('condition', '—')}",
                f"Rule Logic: {payroll_context.get('decision_rule', {}).get('logic', '—')}",
                f"Result: {payroll_context.get('decision_rule', {}).get('result', '—')}",
                "",
                "Top Blockers",
            ]
        )
        blockers = payroll_context.get("top_blockers") or []
        if blockers:
            lines.extend(f"- {item}" for item in blockers)
        else:
            lines.append("- None")
        lines.extend(["", "Evidence Ladder"])
        for item in payroll_context.get("evidence_ladder") or []:
            lines.append(
                f"- {item.get('title', '—')}: {item.get('value', '—')} [{item.get('evidence_type', '—')}]"
            )
            lines.append(f"  {item.get('detail', '—')}")
            provenance = item.get("provenance", {})
            lines.append(f"  Source: {provenance.get('source', '—')}")
            lines.append(f"  Timestamp: {provenance.get('timestamp', '—')}")
            lines.append(f"  Age: {provenance.get('age_label', 'Initializing')}")
            lines.append(f"  Confidence: {provenance.get('confidence', '—')}")
            lines.append(f"  Status: {provenance.get('status', '—')}")
        lines.extend(
            [
                "",
                "Measured Input Posture",
                "",
                f"Measured snapshot time: {payroll_context.get('measured_snapshot', {}).get('freshness_timestamp', '—')}",
                f"Measured source: {payroll_context.get('measured_snapshot', {}).get('measured_fee_source', '—')}",
                f"Measured data status: {payroll_context.get('measured_snapshot', {}).get('data_status', '—')} / {payroll_context.get('measured_snapshot', {}).get('freshness_level', '—')}",
            ]
        )
        for family in payroll_context.get("query_posture", {}).get("families") or []:
            lines.append(f"- {family.get('family', '—')}: {family.get('summary', '—')}")
        lines.extend(
            [
                f"Request-path note: {payroll_context.get('query_posture', {}).get('request_path_note', '—')}",
                "",
                "Policy Results",
            ]
        )
        for check in payroll_context.get("policy_checks") or []:
            lines.append(
                f"- {check.get('label', '—')}: {check.get('status', '—')} [{check.get('evidence_type', '—')}]"
            )
            lines.append(f"  {check.get('detail', '—')}")
            lines.append(f"  Policy Threshold: {check.get('policy_threshold_label', '—')}")
            lines.append(f"  Actual Value: {check.get('actual_value_label', '—')}")
            lines.append(f"  Decision Trigger: {check.get('decision_trigger', '—')}")
        lines.extend(["", "DECISION FLIP CONDITIONS", ""])
        flip_conditions = payroll_context.get("decision_flip_conditions") or []
        if flip_conditions:
            for item in flip_conditions:
                lines.append(f"- {item.get('label', '—')}")
                lines.append(f"  Current State: {item.get('current_state', '—')}")
                lines.append(f"  Target State: {item.get('target_state', '—')}")
        else:
            lines.append("- None")
        forecast_advisory = payroll_context.get("forecast_advisory") or {}
        capital_impact = payroll_context.get("capital_impact") or {}
        lines.extend(
            [
                "",
                "ROUTE COMPARISON",
                "",
            ]
        )
        for route in payroll_context.get("route_comparison") or []:
            lines.extend(
                [
                    f"Rail: {route.get('rail', '—')}",
                    f"Network Fee: {_fmt_usd(float(route.get('network_fee_usd', 0.0) or 0.0))}",
                    f"Routing Fee: {_fmt_usd(float(route.get('routing_fee_usd', 0.0) or 0.0))}",
                    f"Total Fee: {_fmt_usd(float(route.get('total_fee_usd', 0.0) or 0.0))}",
                    f"Estimated Arrival: {route.get('estimated_arrival_label', '—')}",
                    f"Confidence Score: {route.get('confidence_score_label', '—')}",
                    "",
                ]
            )
        lines.extend(
            [
                "CAPITAL IMPACT",
                "",
                f"Current Buffer: {_fmt_pct(float(capital_impact.get('current_buffer_percent', 0.0) or 0.0))}",
                f"Recommended Buffer: {_fmt_pct(float(capital_impact.get('new_buffer_percent', 0.0) or 0.0))}",
                f"Capital Released: {_fmt_usd(float(capital_impact.get('capital_released', 0.0) or 0.0))}",
                f"Selected Buffer Percent: {_fmt_pct_or_dash(capital_impact.get('selected_buffer_percent'))}",
                f"Selected Buffer Amount: {_fmt_usd_or_dash(capital_impact.get('selected_buffer_amount'))}",
                f"Capital Direction: {_humanize_capital_direction(capital_impact.get('capital_delta_direction'))}",
                f"Additional Prefunding Required: {_fmt_usd_or_dash(capital_impact.get('additional_prefunding_required'))}",
                f"Yield Opportunity (Illustrative): {_yield_opportunity_label(capital_impact)}",
                f"Effective Deadline: {capital_impact.get('effective_deadline_at') or payroll_context.get('decision_context', {}).get('effective_deadline_at', '—')}",
                f"Time Until Cutoff: {_fmt_minutes(capital_impact.get('time_until_cutoff_minutes'))}",
                f"Safe Operating Range: {_safe_operating_range_label(capital_impact)}",
                "",
                "Forecast Advisory",
                "",
                f"Status: {forecast_advisory.get('status', '—')}",
                (
                    f"Stability probability: {_fmt_pct(float(forecast_advisory.get('corridor_stability_probability', 0.0) or 0.0))}"
                    if forecast_advisory.get("corridor_stability_probability") is not None
                    else "Stability probability: —"
                ),
                (
                    f"Liquidity shock risk: {_fmt_pct(float(forecast_advisory.get('liquidity_shock_risk', 0.0) or 0.0))}"
                    if forecast_advisory.get("liquidity_shock_risk") is not None
                    else "Liquidity shock risk: —"
                ),
                f"Forecast Scenario: {payroll_context.get('forecast_action_path', {}).get('forecast_scenario', '—')}",
                f"Next Expected State: {payroll_context.get('forecast_action_path', {}).get('next_expected_state', '—')}",
                f"Trigger Condition: {payroll_context.get('forecast_action_path', {}).get('trigger_condition', '—')}",
                f"Escalation Condition: {payroll_context.get('forecast_action_path', {}).get('escalation_condition', '—')}",
                "Forecasted = advisory only; does not override readiness.",
                "",
                "ALTERNATIVE PATHS",
                "",
            ]
        )
        alternative_paths = payroll_context.get("alternative_paths") or []
        if alternative_paths:
            for index, item in enumerate(alternative_paths, start=1):
                lines.extend(
                    [
                        f"Path {index}",
                        f"Action: {item.get('action', '—')}",
                        f"Likely Outcome: {item.get('likely_outcome', '—')}",
                        f"Timing Impact: {item.get('timing_impact', '—')}",
                        f"Capital Impact: {item.get('capital_impact', '—')}",
                        f"Decision Impact: {item.get('decision_effect', '—')}",
                        "",
                    ]
                )
        else:
            lines.extend(["- None", ""])
        lines.extend(
            [
                "HANDOFF RECORD",
                "",
                f"Decision approved: {payroll_context.get('handoff_record', {}).get('decision_approved_at', '—')}",
                f"Handoff triggered: {payroll_context.get('handoff_record', {}).get('handoff_triggered_at', '—')}",
                f"Execution system: {payroll_context.get('handoff_record', {}).get('execution_system', '—')}",
                f"Status: {payroll_context.get('handoff_record', {}).get('status', '—')}",
                f"Execution acknowledgement: {payroll_context.get('handoff_record', {}).get('execution_acknowledged_at', '—')}",
                "",
                "DECISION LOG",
                "",
            ]
        )
        decision_log = payroll_context.get("decision_log") or []
        if decision_log:
            for item in decision_log:
                event_label = _decision_log_event_label(item)
                lines.append(f"Event Type: {event_label}")
                lines.append(f"Timestamp: {item.get('decision_timestamp', '—')}")
                if event_label == "Decision":
                    lines.extend(
                        [
                            f"Action: {item.get('decision_action', '—')}",
                            f"Reason: {item.get('decision_reason', '—')}",
                            f"Recorded By: {item.get('decision_owner', '—')}",
                            f"Rule: {item.get('decision_rule', '—')}",
                        ]
                    )
                elif event_label == "Payroll Data Update":
                    lines.extend(
                        [
                            f"Event: {item.get('event_name', '—')}",
                            f"Records Loaded: {item.get('record_count', '—')}",
                            f"Changes Detected: {item.get('beneficiary_change_count', '—')}",
                            f"Source: {item.get('source_type_label') or item.get('source_type', '—')}",
                            f"File: {item.get('file_name', '—')}",
                        ]
                    )
                else:
                    if item.get("event_name"):
                        lines.append(f"Event: {item.get('event_name')}")
                    if item.get("label"):
                        lines.append(f"Label: {item.get('label')}")
                    if item.get("detail"):
                        lines.append(f"Detail: {item.get('detail')}")
                    if item.get("summary"):
                        lines.append(f"Summary: {item.get('summary')}")
                lines.append("")
        else:
            lines.extend(["No recorded decisions.", ""])
        lines.extend(
            [
                "RECEIPT CONTRACT",
                "",
                f"Timestamp: {route_result.get('timestamp', '—')}",
                f"Inputs: measured + calculated + modeled + forecasted evidence captured for {payroll_context.get('payroll_run_id', '—')}",
                f"Policy Results: {len(payroll_context.get('policy_checks') or [])} checks evaluated",
                f"Evidence: {len(payroll_context.get('evidence_ladder') or [])} signals with source attribution",
                f"Decision: {payroll_context.get('operator_action', payroll_context.get('recommended_action', '—'))}",
                f"Approver: {payroll_context.get('operator_approver', '—')}",
                f"Execution Boundary: {payroll_context.get('approval_boundary_note', '—')}",
                "",
                "--------------------------------",
                "",
            ]
        )

    lines.extend(
        [
            "TRANSFER ECONOMICS",
            "",
            "Formula",
            "landed = amount - (network_fee + routing_fee)",
            "routing_fee = max(amount * routing_bps + routing_fixed_fee, routing_min_fee)",
            "",
            "Trace",
            (
                f"routing_fee = max({transfer_winner['transfer_math']['amount_usdc']:.0f} * "
                f"{transfer_winner['transfer_math']['routing_bps']:.4f} + "
                f"{transfer_winner['transfer_math']['routing_fixed_fee_usd']:.2f}, "
                f"{transfer_winner['transfer_math']['routing_min_fee_usd']:.2f}) = "
                f"{transfer_winner['transfer_math']['routing_fee_usd']:.2f}"
            ),
            (
                f"total_fee = {transfer_winner['transfer_math']['network_fee_usd']:.4f} + "
                f"{transfer_winner['transfer_math']['routing_fee_usd']:.2f} = "
                f"{transfer_winner['transfer_math']['total_fee_usd']:.2f}"
            ),
            (
                f"{transfer_winner['transfer_math']['amount_usdc']:.0f} - "
                f"{transfer_winner['transfer_math']['total_fee_usd']:.2f} = "
                f"{transfer_winner['transfer_math']['landed_amount_usd']:.2f}"
            ),
            "",
            "--------------------------------",
            "",
            "STRATEGY MODEL",
            "",
            "Formula",
            "strategy_score =",
            "0.4 cost + 0.4 liquidity + 0.2 trust",
            "critical risk flags can cap the final strategy score",
            "",
        ]
    )

    for rail in [rail for rail in rails if rail.get("mode") == "live_measured"]:
        assessment = rail["strategy_assessment"]
        lines.extend(
            [
                rail["rail"],
                (
                    f"0.4({_fmt_ratio(assessment['cost_score'])}) + "
                    f"0.4({_fmt_ratio(assessment['liquidity_score'])}) + "
                    f"0.2({_fmt_ratio(assessment['trust_score'])})"
                ),
                f"= {_fmt_ratio(assessment['strategy_score'])}",
                f"Evidence Confidence = {_fmt_ratio(assessment['trust_score'])} ({assessment['evidence_confidence_label']})",
                (
                    f"Penalty factors -> liquidity x{_fmt_ratio(assessment['liquidity_penalty_factor'])}, "
                    f"trust x{_fmt_ratio(assessment['trust_penalty_factor'])}"
                ),
                f"Risk gate -> {assessment.get('risk_gate_status', 'OPEN')}",
                "",
            ]
        )

    if transfer_winner["rail"] != canopy_recommendation["rail"]:
        flag_note = ", ".join(canopy_recommendation.get("adversarial_flags", [])) or "no active flags"
        explanation = (
            f"{canopy_recommendation['rail']} recommended despite a lower landed amount because "
            f"its modeled liquidity integrity and evidence confidence outweigh the transfer-cost edge "
            f"({flag_note})."
        )
    else:
        explanation = f"{canopy_recommendation['rail']} wins both transfer economics and corridor strategy."

    lines.extend(
        [
            "Result",
            explanation,
            "",
            "--------------------------------",
            "",
            "ASSUMPTIONS",
            "",
            f"ticket_size = {route_result.get('amount_usdc', 0):.0f}",
            f"routing_bps = {transfer_winner['transfer_math']['routing_bps']}",
            f"routing_fixed_fee = {transfer_winner['transfer_math']['routing_fixed_fee_usd']}",
            f"routing_min_fee = {transfer_winner['transfer_math']['routing_min_fee_usd']}",
            f"recommended_rail_data_status = {canopy_recommendation.get('data_status', '—')}",
            f"recommended_rail_freshness = {canopy_recommendation.get('freshness_level', '—')}",
            f"recommended_rail_cache_age_seconds = {canopy_recommendation.get('cache_age_seconds', '—')}",
            f"corridor_best_selected = {corridor_best_supported.get('is_selected_route', False)}",
            f"scenario_review_state = {review_state or 'unsaved'}",
            f"artifact_type = {artifact_type}",
            "",
            f"rail_fee_assumption = {_fmt_pct((route_result.get('scenario', {}).get('current_rail_fee_pct', 0.0) or 0.0) / 100)}",
            "",
            "--------------------------------",
            "",
            "WORKFLOW STATE",
            "",
            f"Live route timestamp: {route_result.get('timestamp', '—')}",
            f"Saved scenario available: {bool(scenario_artifact)}",
            f"Review status: {review_state or 'unsaved'}",
            f"Follow-up requested: {'yes' if follow_up_requested else 'no'}",
            "",
            "--------------------------------",
            "",
            "DATA LINEAGE",
            "",
        ]
    )

    if payroll_context and payroll_context.get("data_lineage"):
        lines.extend(payroll_context["data_lineage"])
    else:
        lines.append("BigQuery -> background refresh -> materialized summary -> decision engine -> decision receipt")

    lines.extend(
        [
            "",
            "--------------------------------",
            "",
            "INTEGRITY FLAGS",
            "",
        ]
    )

    for rail in rails:
        lines.append(rail["rail"])
        lines.append(", ".join(rail.get("adversarial_flags", [])) or "NONE")
        lines.append("")

    if review_notes:
        lines.extend(
            [
                "REVIEW NOTES",
                "",
                review_notes,
                "",
            ]
        )

    if payroll_context and payroll_context.get("operator_reason_other"):
        lines.extend(
            [
                "OPERATOR NOTES",
                "",
                str(payroll_context.get("operator_reason_other")),
                "",
                "BOUNDARY NOTE",
                "",
                str(payroll_context.get("approval_boundary_note", "Execution occurs outside this product.")),
                "",
            ]
        )

    return "\n".join(lines).rstrip() + "\n"
