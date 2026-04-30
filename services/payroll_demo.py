"""Payroll-readiness demo layer built on top of the existing Canopy route stack."""

from __future__ import annotations

import base64
import csv
import io
import json
import re
import zipfile
from datetime import datetime, timezone
from typing import Any, Dict, List
from uuid import uuid4
from xml.etree import ElementTree as ET

from api import cache as runtime_cache
from api.demo_store import (
    get_latest_payroll_evaluation,
    get_latest_payroll_decision,
    get_latest_payroll_data_snapshot,
    get_latest_payroll_handoff,
    get_last_verified_payroll_data_snapshot,
    list_payroll_data_events,
    record_payroll_evaluation,
    record_payroll_data_event,
    record_payroll_data_snapshot,
    record_payroll_decision,
    record_payroll_handoff,
    transition_payroll_handoff,
)
from api.router import get_route
from forecasting.api import run_corridor_forecast
from services.runtime_mode import get_runtime_mode, is_real_mode
from services.query_metrics import get_query_metrics_snapshot

READINESS_ORDER = {
    "BLOCKED": 4,
    "HOLD": 3,
    "REVIEW_REQUIRED": 2,
    "READY_FOR_APPROVAL": 1,
}

RISK_ORDER = {
    "CRITICAL": 4,
    "HIGH": 3,
    "MODERATE": 2,
    "LOW": 1,
}

ACTION_LABELS = {
    "APPROVE": "Approve",
    "HOLD": "Hold",
    "ESCALATE": "Escalate",
}

DECISION_REASON_OPTIONS = [
    "Liquidity below threshold",
    "Beneficiary mismatch",
    "Funding inside cutoff window",
    "Compliance flag",
    "Manual override",
    "Operational delay",
    "Other",
]

PAYROLL_SOURCE_LABELS = {
    "demo": "Demo dataset",
    "upload": "CSV upload",
    "api": "Sample merchant API",
    "sftp": "SFTP batch",
    "manual": "Manual entry",
}

ILLUSTRATIVE_YIELD_RATE = 0.045

REQUIRED_PAYROLL_COLUMNS = [
    "beneficiary_id",
    "name",
    "account_number",
    "routing_code",
    "currency",
]

DEMO_PAYROLL_DATA = {
    "ng-2026-03-31": {
        "source_type": "demo",
        "source_label": "Demo dataset",
        "last_loaded_timestamp": "2026-03-26T11:29:00+00:00",
        "record_count": 142,
        "beneficiary_change_count": 14,
        "verification_status": "review_required",
        "data_status": "pending_review",
        "lineage_label": "Payroll dataset snapshot -> BigQuery -> Decision Engine",
        "upload_status": "Not required",
        "demo_mode_label": "Demo dataset active",
    },
    "mx-2026-04-01": {
        "source_type": "demo",
        "source_label": "Demo dataset",
        "last_loaded_timestamp": "2026-03-26T11:21:00+00:00",
        "record_count": 96,
        "beneficiary_change_count": 0,
        "verification_status": "verified",
        "data_status": "ready",
        "lineage_label": "Payroll dataset snapshot -> BigQuery -> Decision Engine",
        "upload_status": "Not required",
        "demo_mode_label": "Demo dataset active",
    },
    "br-2026-04-02": {
        "source_type": "demo",
        "source_label": "Demo dataset",
        "last_loaded_timestamp": "2026-03-26T11:24:00+00:00",
        "record_count": 118,
        "beneficiary_change_count": 0,
        "verification_status": "verified",
        "data_status": "ready",
        "lineage_label": "Payroll dataset snapshot -> BigQuery -> Decision Engine",
        "upload_status": "Not required",
        "demo_mode_label": "Demo dataset active",
    },
    "ph-2026-04-04": {
        "source_type": "demo",
        "source_label": "Demo dataset",
        "last_loaded_timestamp": "2026-03-26T11:27:00+00:00",
        "record_count": 164,
        "beneficiary_change_count": 3,
        "verification_status": "review_required",
        "data_status": "pending_review",
        "lineage_label": "Payroll dataset snapshot -> BigQuery -> Decision Engine",
        "upload_status": "Not required",
        "demo_mode_label": "Demo dataset active",
    },
    "sg-vn-2026-04-07": {
        "source_type": "demo",
        "source_label": "Demo dataset",
        "last_loaded_timestamp": "2026-03-26T11:31:00+00:00",
        "record_count": 84,
        "beneficiary_change_count": 1,
        "verification_status": "review_required",
        "data_status": "pending_review",
        "lineage_label": "Payroll dataset snapshot -> BigQuery -> Decision Engine",
        "upload_status": "Not required",
        "demo_mode_label": "Demo dataset active",
    },
    "za-2026-04-03": {
        "source_type": "demo",
        "source_label": "Demo dataset",
        "last_loaded_timestamp": "2026-03-26T11:29:00+00:00",
        "record_count": 142,
        "beneficiary_change_count": 0,
        "verification_status": "verified",
        "data_status": "ready",
        "lineage_label": "Payroll dataset snapshot -> BigQuery -> Decision Engine",
        "upload_status": "Not required",
        "demo_mode_label": "Demo dataset active",
    },
}

DEMO_RUNS: Dict[str, Dict[str, Any]] = {
    "ng-2026-03-31": {
        "id": "ng-2026-03-31",
        "client_name": "Sample Nigeria Settlement",
        "origin": "US",
        "destination": "NG",
        "corridor_key": "US-NG",
        "corridor_slug": "us-nigeria",
        "corridor_label": "US -> Nigeria",
        "currency": "NGN",
        "payroll_date": "2026-03-31",
        "expected_payroll_amount": 2_450_000.0,
        "prefunding_amount": 2_494_100.0,
        "prefunding_received_at": "2026-03-30T16:42:00+01:00",
        "cutoff_at": "2026-03-30T18:00:00+01:00",
        "monthly_volume_usdc": 9_800_000.0,
        "current_rail_fee_pct": 1.38,
        "current_rail_settlement_hours": 40.0,
        "current_setup": "USD prefunding via treasury bank, local NGN payout counterparty, manual beneficiary review",
        "compliance_sensitivity": "high",
        "route_lens": "treasury",
        "top_line_reason": "Funding landed inside the cutoff window, beneficiary ownership changes spiked, and the measured liquidity proxy is below the release threshold.",
        "recommended_next_action": "Hold final approval, clear the flagged beneficiaries, and confirm counterparty capacity before release.",
        "policy_thresholds": {
            "minimum_cutoff_buffer_minutes": 180,
            "minimum_liquidity_score": 0.68,
            "minimum_freshness_level": "fresh",
        },
        "changes_since_last_run": [
            {
                "id": "ng-change-1",
                "change_type": "Prefunding timing",
                "summary": "Prefunding arrived 17 hours later than the previous payroll cycle.",
                "old_value": "2026-02-28 23:18 WAT",
                "new_value": "2026-03-30 16:42 WAT",
                "materiality": "HIGH",
                "detected_at": "2026-03-30T16:43:00+01:00",
            },
            {
                "id": "ng-change-2",
                "change_type": "Beneficiary updates",
                "summary": "14 beneficiary ownership changes were detected versus 2 last run.",
                "old_value": "2 ownership changes",
                "new_value": "14 ownership changes",
                "materiality": "HIGH",
                "detected_at": "2026-03-30T16:44:00+01:00",
            },
            {
                "id": "ng-change-3",
                "change_type": "counterparty capacity",
                "summary": "counterparty daylight capacity dropped from 3.2M to 2.6M for the release window.",
                "old_value": "$3.2M capacity",
                "new_value": "$2.6M capacity",
                "materiality": "MODERATE",
                "detected_at": "2026-03-30T16:50:00+01:00",
            },
        ],
        "exceptions": [
            {
                "id": "ng-ex-1",
                "exception_type": "late_funding",
                "severity": "HIGH",
                "entity_type": "payroll_run",
                "entity_reference": "ng-2026-03-31",
                "summary": "Funding arrived 78 minutes before cutoff.",
                "why_it_matters": "Late prefunding compresses review time and raises the chance of missing the payout release window.",
                "recommended_next_step": "Pause release until treasury confirms there is still enough time for final controls.",
                "owner": "Treasury Ops",
                "sla_due_at": "2026-03-30T17:15:00+01:00",
                "evidence_type": "CALCULATED",
                "evidence_state": "CURRENT",
                "status": "OPEN",
                "created_at": "2026-03-30T16:42:00+01:00",
                "updated_at": "2026-03-30T16:42:00+01:00",
            },
            {
                "id": "ng-ex-2",
                "exception_type": "beneficiary_identity_mismatch",
                "severity": "HIGH",
                "entity_type": "sub_batch",
                "entity_reference": "14 beneficiaries",
                "summary": "Ownership mismatches detected across 14 beneficiary records.",
                "why_it_matters": "The run should not be approved until the beneficiary changes are verified against the latest payroll file.",
                "recommended_next_step": "Review the flagged beneficiaries and clear or remove the affected sub-batch.",
                "owner": "Compliance Review",
                "sla_due_at": "2026-03-30T17:05:00+01:00",
                "evidence_type": "MEASURED",
                "evidence_state": "SEEDED_DEMO_DATA",
                "status": "OPEN",
                "created_at": "2026-03-30T16:44:00+01:00",
                "updated_at": "2026-03-30T16:44:00+01:00",
            },
            {
                "id": "ng-ex-3",
                "exception_type": "liquidity_shortage",
                "severity": "MODERATE",
                "entity_type": "counterparty",
                "entity_reference": "Nigeria payout counterparty",
                "summary": "counterparty capacity is below the preferred buffer for today's run.",
                "why_it_matters": "The route still exists, but the operator should confirm capacity before final approval.",
                "recommended_next_step": "Confirm counterparty daylight capacity and keep the fallback route ready.",
                "owner": "Corridor Ops",
                "sla_due_at": "2026-03-30T17:20:00+01:00",
                "evidence_type": "MODELED",
                "evidence_state": "CURRENT",
                "status": "OPEN",
                "created_at": "2026-03-30T16:50:00+01:00",
                "updated_at": "2026-03-30T16:50:00+01:00",
            },
        ],
    },
    "mx-2026-04-01": {
        "id": "mx-2026-04-01",
        "client_name": "Sample Mexico Settlement",
        "origin": "US",
        "destination": "MX",
        "corridor_key": "US-MX",
        "corridor_slug": "us-mexico",
        "corridor_label": "US -> Mexico",
        "currency": "MXN",
        "payroll_date": "2026-04-01",
        "expected_payroll_amount": 1_180_000.0,
        "prefunding_amount": 1_286_200.0,
        "prefunding_received_at": "2026-03-31T08:20:00-06:00",
        "cutoff_at": "2026-04-01T09:00:00-06:00",
        "monthly_volume_usdc": 3_100_000.0,
        "current_rail_fee_pct": 0.95,
        "current_rail_settlement_hours": 12.0,
        "current_setup": "US funding counterparty, Mexico payout network, standard beneficiary controls",
        "compliance_sensitivity": "medium",
        "route_lens": "treasury",
        "top_line_reason": "Measured route evidence is fresh, prefunding landed early, and the run is clear for routine release checks.",
        "recommended_next_action": "Approve after the final pre-release confirmation with the payout counterparty.",
        "policy_thresholds": {
            "minimum_cutoff_buffer_minutes": 150,
            "minimum_liquidity_score": 0.6,
            "minimum_freshness_level": "fresh",
        },
        "changes_since_last_run": [
            {
                "id": "mx-change-1",
                "change_type": "Payroll mix",
                "summary": "Beneficiary mix shifted toward hourly contractors, but no verification mismatches were detected.",
                "old_value": "88 beneficiaries",
                "new_value": "96 beneficiaries",
                "materiality": "LOW",
                "detected_at": "2026-03-31T08:35:00-06:00",
            }
        ],
        "exceptions": [],
    },
    "br-2026-04-02": {
        "id": "br-2026-04-02",
        "client_name": "Sample Brazil Settlement",
        "origin": "US",
        "destination": "BR",
        "corridor_key": "US-BR",
        "corridor_slug": "us-brazil",
        "corridor_label": "US -> Brazil",
        "currency": "BRL",
        "payroll_date": "2026-04-02",
        "expected_payroll_amount": 1_760_000.0,
        "prefunding_amount": 1_936_000.0,
        "prefunding_received_at": "2026-04-01T09:15:00-03:00",
        "cutoff_at": "2026-04-01T17:30:00-03:00",
        "monthly_volume_usdc": 7_200_000.0,
        "current_rail_fee_pct": 1.12,
        "current_rail_settlement_hours": 24.0,
        "current_setup": "Treasury prefunding, PIX-connected payout counterparty, standard beneficiary review",
        "compliance_sensitivity": "medium",
        "route_lens": "treasury",
        "top_line_reason": "Funding is early, the measured route is fresh, and there are no open beneficiary or sanctions blockers.",
        "recommended_next_action": "Approve the run after the final pre-release check.",
        "policy_thresholds": {
            "minimum_cutoff_buffer_minutes": 180,
            "minimum_liquidity_score": 0.6,
            "minimum_freshness_level": "fresh",
        },
        "changes_since_last_run": [
            {
                "id": "br-change-1",
                "change_type": "Payout volume",
                "summary": "Payroll value increased by 4.2% versus the prior cycle.",
                "old_value": "$1.69M",
                "new_value": "$1.76M",
                "materiality": "LOW",
                "detected_at": "2026-04-01T09:30:00-03:00",
            }
        ],
        "exceptions": [],
    },
    "ph-2026-04-04": {
        "id": "ph-2026-04-04",
        "client_name": "Sample Philippines Settlement",
        "origin": "US",
        "destination": "PH",
        "corridor_key": "US-PH",
        "corridor_slug": "us-philippines",
        "corridor_label": "US -> Philippines",
        "currency": "PHP",
        "payroll_date": "2026-04-04",
        "expected_payroll_amount": 920_000.0,
        "prefunding_amount": 1_012_000.0,
        "prefunding_received_at": "2026-04-03T10:45:00+08:00",
        "cutoff_at": "2026-04-03T18:30:00+08:00",
        "monthly_volume_usdc": 2_300_000.0,
        "current_rail_fee_pct": 1.24,
        "current_rail_settlement_hours": 20.0,
        "current_setup": "US treasury prefunding, Philippines payout counterparty, beneficiary review on change only",
        "compliance_sensitivity": "medium",
        "route_lens": "treasury",
        "top_line_reason": "The corridor is usable, but beneficiary review is still open and the operator should clear the changed records before release.",
        "recommended_next_action": "Review the changed beneficiaries and release once the payout file is re-verified.",
        "policy_thresholds": {
            "minimum_cutoff_buffer_minutes": 180,
            "minimum_liquidity_score": 0.62,
            "minimum_freshness_level": "fresh",
        },
        "changes_since_last_run": [
            {
                "id": "ph-change-1",
                "change_type": "Beneficiary updates",
                "summary": "Three beneficiaries changed payout details versus the last verified payroll file.",
                "old_value": "0 changes",
                "new_value": "3 changes",
                "materiality": "MODERATE",
                "detected_at": "2026-04-03T10:58:00+08:00",
            }
        ],
        "exceptions": [
            {
                "id": "ph-ex-1",
                "exception_type": "beneficiary_change_review",
                "severity": "MODERATE",
                "entity_type": "sub_batch",
                "entity_reference": "3 beneficiaries",
                "summary": "Three beneficiary profile changes are awaiting review.",
                "why_it_matters": "The release is operationally fine, but the changed beneficiaries should be confirmed before approval.",
                "recommended_next_step": "Review the changed beneficiary details and verify the payout file.",
                "owner": "Payroll Operations",
                "sla_due_at": "2026-04-03T16:30:00+08:00",
                "evidence_type": "MEASURED",
                "evidence_state": "SEEDED_DEMO_DATA",
                "status": "OPEN",
                "created_at": "2026-04-03T10:58:00+08:00",
                "updated_at": "2026-04-03T10:58:00+08:00",
            }
        ],
    },
    "za-2026-04-03": {
        "id": "za-2026-04-03",
        "client_name": "Sample South Africa Settlement",
        "origin": "US",
        "destination": "ZA",
        "corridor_key": "US-ZA",
        "corridor_slug": "us-south-africa",
        "corridor_label": "US -> South Africa",
        "currency": "ZAR",
        "payroll_date": "2026-04-03",
        "expected_payroll_amount": 5_600_000.0,
        "prefunding_amount": 7_280_000.0,
        "prefunding_received_at": "2026-04-02T11:20:00+02:00",
        "cutoff_at": "2026-04-03T12:00:00+02:00",
        "monthly_volume_usdc": 11_400_000.0,
        "current_rail_fee_pct": 1.08,
        "current_rail_settlement_hours": 18.0,
        "current_setup": "USD prefunding with local ZAR payout counterparty and standard beneficiary controls",
        "compliance_sensitivity": "medium",
        "route_lens": "treasury",
        "top_line_reason": "Measured route evidence is current, liquidity is stable, and the run is ready to be simulated against transfer size and arrival timing.",
        "recommended_next_action": "Run a readiness check with the intended amount and deadline, then record approval or hold inside Canopy.",
        "policy_thresholds": {
            "minimum_cutoff_buffer_minutes": 180,
            "minimum_liquidity_score": 0.64,
            "minimum_freshness_level": "fresh",
        },
        "changes_since_last_run": [
            {
                "id": "za-change-1",
                "change_type": "Run scale",
                "summary": "The expected payroll amount increased versus the prior cycle, so buffer guidance should be re-evaluated before release.",
                "old_value": "$4.8M",
                "new_value": "$5.6M",
                "materiality": "MODERATE",
                "detected_at": "2026-04-02T11:35:00+02:00",
            }
        ],
        "exceptions": [],
    },
    "sg-vn-2026-04-07": {
        "id": "sg-vn-2026-04-07",
        "client_name": "Sample Singapore Vietnam Settlement",
        "origin": "SG",
        "destination": "VN",
        "corridor_key": "SG-VN",
        "corridor_slug": "singapore-vietnam",
        "corridor_label": "Singapore -> Vietnam",
        "currency": "VND",
        "payroll_date": "2026-04-07",
        "expected_payroll_amount": 1_480_000.0,
        "prefunding_amount": 1_657_600.0,
        "prefunding_received_at": "2026-04-06T10:05:00+08:00",
        "cutoff_at": "2026-04-06T15:00:00+07:00",
        "monthly_volume_usdc": 2_400_000.0,
        "current_rail_fee_pct": 1.18,
        "current_rail_settlement_hours": 18.0,
        "current_setup": "Singapore treasury account, Vietnam payout counterparty, manual release controls for changed beneficiaries",
        "compliance_sensitivity": "high",
        "route_lens": "treasury",
        "top_line_reason": "This corridor is viable for a controlled run, but counterparty capacity and one beneficiary change should be cleared before the release window closes.",
        "recommended_next_action": "Confirm counterparty release capacity, clear the changed beneficiary, and then re-run readiness before approval.",
        "policy_thresholds": {
            "minimum_cutoff_buffer_minutes": 210,
            "minimum_liquidity_score": 0.66,
            "minimum_freshness_level": "fresh",
        },
        "changes_since_last_run": [
            {
                "id": "sgvn-change-1",
                "change_type": "counterparty capacity",
                "summary": "Vietnam counterparty daylight capacity tightened for the planned release block.",
                "old_value": "$2.1M reserved",
                "new_value": "$1.7M reserved",
                "materiality": "MODERATE",
                "detected_at": "2026-04-06T10:12:00+08:00",
            },
            {
                "id": "sgvn-change-2",
                "change_type": "Beneficiary updates",
                "summary": "One executive payroll beneficiary changed local account details after the last verified file.",
                "old_value": "0 changes",
                "new_value": "1 change",
                "materiality": "MODERATE",
                "detected_at": "2026-04-06T10:14:00+08:00",
            },
        ],
        "exceptions": [
            {
                "id": "sgvn-ex-1",
                "exception_type": "counterparty_capacity_review",
                "severity": "MODERATE",
                "entity_type": "counterparty",
                "entity_reference": "Vietnam payout counterparty",
                "summary": "Reserved release capacity is below the preferred comfort level for this run.",
                "why_it_matters": "The run may still clear, but the operator should confirm daytime capacity before approving release.",
                "recommended_next_step": "Confirm reserved capacity with the payout counterparty and keep the fallback rail ready.",
                "owner": "Corridor Ops",
                "sla_due_at": "2026-04-06T12:00:00+07:00",
                "evidence_type": "MODELED",
                "evidence_state": "CURRENT",
                "status": "OPEN",
                "created_at": "2026-04-06T11:12:00+07:00",
                "updated_at": "2026-04-06T11:12:00+07:00",
            },
            {
                "id": "sgvn-ex-2",
                "exception_type": "beneficiary_change_review",
                "severity": "MODERATE",
                "entity_type": "sub_batch",
                "entity_reference": "1 beneficiary",
                "summary": "One beneficiary bank-detail change still needs manual verification.",
                "why_it_matters": "Approval should wait until the changed beneficiary record is cleared against the latest payroll file.",
                "recommended_next_step": "Review the changed beneficiary and re-verify the run before release.",
                "owner": "Compliance Review",
                "sla_due_at": "2026-04-06T13:00:00+07:00",
                "evidence_type": "MEASURED",
                "evidence_state": "SEEDED_DEMO_DATA",
                "status": "OPEN",
                "created_at": "2026-04-06T11:14:00+07:00",
                "updated_at": "2026-04-06T11:14:00+07:00",
            },
        ],
    },
}


def _parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _fmt_pct(value: float) -> str:
    return f"{value * 100:.1f}%"


def _fmt_usd(value: float | None) -> str:
    if value is None:
        return "-"
    return f"${value:,.2f}"


def _fmt_number(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:,.0f}"


def _titleize_status(value: str) -> str:
    return str(value or "").replace("_", " ").title()


def _normalize_beneficiary_record(record: Dict[str, Any]) -> Dict[str, str]:
    return {
        "beneficiary_id": str(record.get("beneficiary_id", "")).strip(),
        "name": str(record.get("name", "")).strip(),
        "account_number": str(record.get("account_number", "")).strip(),
        "routing_code": str(record.get("routing_code", "")).strip(),
        "currency": str(record.get("currency", "")).strip().upper(),
    }


def _valid_account_format(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9\-]{6,34}", value))


def _validate_payroll_records(records: List[Dict[str, Any]]) -> List[str]:
    errors: List[str] = []
    seen_ids = set()
    for index, raw_record in enumerate(records, start=1):
        record = _normalize_beneficiary_record(raw_record)
        missing = [column for column in REQUIRED_PAYROLL_COLUMNS if not record.get(column)]
        if missing:
            errors.append(f"Row {index}: missing required fields: {', '.join(missing)}")
        beneficiary_id = record.get("beneficiary_id")
        if beneficiary_id:
            if beneficiary_id in seen_ids:
                errors.append(f"Row {index}: duplicate beneficiary_id '{beneficiary_id}'")
            seen_ids.add(beneficiary_id)
        if record.get("account_number") and not _valid_account_format(record["account_number"]):
            errors.append(f"Row {index}: invalid account format for beneficiary_id '{beneficiary_id or 'unknown'}'")
    return errors


def _parse_csv_records(content: bytes) -> List[Dict[str, Any]]:
    text = content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    return [_normalize_beneficiary_record(row) for row in reader]


def _parse_json_records(content: bytes) -> List[Dict[str, Any]]:
    payload = json.loads(content.decode("utf-8"))
    if isinstance(payload, dict):
        payload = payload.get("records") or payload.get("items") or []
    if not isinstance(payload, list):
        raise ValueError("JSON payroll file must contain a list of beneficiary records.")
    return [_normalize_beneficiary_record(row) for row in payload if isinstance(row, dict)]


def _xlsx_cell_value(shared_strings: List[str], cell: ET.Element) -> str:
    value = cell.find("{*}v")
    if value is None or value.text is None:
        return ""
    if cell.get("t") == "s":
        index = int(value.text)
        return shared_strings[index] if 0 <= index < len(shared_strings) else ""
    return value.text


def _parse_xlsx_records(content: bytes) -> List[Dict[str, Any]]:
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        shared_strings: List[str] = []
        if "xl/sharedStrings.xml" in archive.namelist():
            root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
            shared_strings = [
                "".join(text_node.itertext())
                for text_node in root.findall(".//{*}si")
            ]
        workbook = ET.fromstring(archive.read("xl/workbook.xml"))
        sheet_path = "xl/worksheets/sheet1.xml"
        relationships = {}
        if "xl/_rels/workbook.xml.rels" in archive.namelist():
            rel_root = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
            relationships = {
                rel.get("Id"): rel.get("Target", "")
                for rel in rel_root.findall(".//{*}Relationship")
            }
        first_sheet = workbook.find(".//{*}sheet")
        if first_sheet is not None and first_sheet.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id") in relationships:
            target = relationships[first_sheet.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")]
            sheet_path = f"xl/{target.lstrip('/')}"
        sheet_root = ET.fromstring(archive.read(sheet_path))
        rows = sheet_root.findall(".//{*}row")
        if not rows:
            return []
        header_cells = rows[0].findall("{*}c")
        headers = [_xlsx_cell_value(shared_strings, cell).strip() for cell in header_cells]
        records: List[Dict[str, Any]] = []
        for row in rows[1:]:
            values = [_xlsx_cell_value(shared_strings, cell).strip() for cell in row.findall("{*}c")]
            record = {
                headers[index]: values[index] if index < len(values) else ""
                for index in range(len(headers))
                if headers[index]
            }
            if any(record.values()):
                records.append(_normalize_beneficiary_record(record))
        return records


def _parse_payroll_file(*, file_name: str, content_base64: str) -> tuple[List[Dict[str, Any]], str]:
    extension = file_name.rsplit(".", 1)[-1].lower() if "." in file_name else ""
    content = base64.b64decode(content_base64)
    if extension == "csv":
        return _parse_csv_records(content), "csv"
    if extension == "json":
        return _parse_json_records(content), "json"
    if extension == "xlsx":
        return _parse_xlsx_records(content), "xlsx"
    raise ValueError("Unsupported payroll file type. Use CSV, XLSX, or JSON.")


def _count_beneficiary_changes(current_records: List[Dict[str, Any]], verified_records: List[Dict[str, Any]]) -> int:
    current_index = {record["beneficiary_id"]: record for record in current_records if record.get("beneficiary_id")}
    verified_index = {record["beneficiary_id"]: record for record in verified_records if record.get("beneficiary_id")}
    all_ids = set(current_index) | set(verified_index)
    return sum(1 for beneficiary_id in all_ids if current_index.get(beneficiary_id) != verified_index.get(beneficiary_id))


def _seeded_payroll_data_state(run_id: str) -> Dict[str, Any]:
    seeded = DEMO_PAYROLL_DATA.get(run_id, {})
    last_loaded = seeded.get("last_loaded_timestamp")
    return {
        "source_type": seeded.get("source_type", "demo"),
        "source_type_label": seeded.get("source_label", PAYROLL_SOURCE_LABELS["demo"]),
        "last_loaded_timestamp": last_loaded,
        "record_count": seeded.get("record_count", 0),
        "record_count_label": f"{seeded.get('record_count', 0)} beneficiaries",
        "beneficiary_change_count": seeded.get("beneficiary_change_count", 0),
        "data_status": seeded.get("data_status", "initializing"),
        "data_status_label": _titleize_status(seeded.get("data_status", "initializing")),
        "verification_status": seeded.get("verification_status", "review_required"),
        "verification_status_label": _titleize_status(seeded.get("verification_status", "review_required")),
        "lineage_label": seeded.get("lineage_label", "Payroll dataset snapshot -> BigQuery -> Decision Engine"),
        "upload_status": seeded.get("upload_status", "Not required"),
        "demo_mode_label": seeded.get("demo_mode_label", "Demo dataset active"),
        "validation_errors": [],
        "file_name": None,
    }


def _build_payroll_data_state(run_id: str) -> Dict[str, Any]:
    latest_snapshot = get_latest_payroll_data_snapshot(run_id)
    if latest_snapshot is None:
        return _seeded_payroll_data_state(run_id)
    return {
        "source_type": latest_snapshot.get("source_type", "upload"),
        "source_type_label": latest_snapshot.get("source_label", PAYROLL_SOURCE_LABELS.get(latest_snapshot.get("source_type", "upload"), "CSV upload")),
        "last_loaded_timestamp": latest_snapshot.get("last_loaded_timestamp"),
        "record_count": latest_snapshot.get("record_count", 0),
        "record_count_label": f"{latest_snapshot.get('record_count', 0)} beneficiaries",
        "beneficiary_change_count": latest_snapshot.get("beneficiary_change_count", 0),
        "data_status": latest_snapshot.get("data_status", "initializing"),
        "data_status_label": _titleize_status(latest_snapshot.get("data_status", "initializing")),
        "verification_status": latest_snapshot.get("verification_status", "review_required"),
        "verification_status_label": _titleize_status(latest_snapshot.get("verification_status", "review_required")),
        "lineage_label": latest_snapshot.get("lineage_label", "Payroll dataset snapshot -> BigQuery -> Decision Engine"),
        "upload_status": "Received",
        "demo_mode_label": "Uploaded payroll data active",
        "validation_errors": latest_snapshot.get("validation_errors", []),
        "file_name": latest_snapshot.get("file_name"),
    }


def _fmt_bytes(value: int) -> str:
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f} GB"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f} MB"
    if value >= 1_000:
        return f"{value / 1_000:.1f} KB"
    return f"{value} B"


def _age_seconds(value: str | None) -> int | None:
    if not value:
        return None
    return int((datetime.now(timezone.utc) - _parse_dt(value).astimezone(timezone.utc)).total_seconds())


def _age_label(value: str | None) -> str:
    age_seconds = _age_seconds(value)
    if age_seconds is None:
        return "Initializing"
    if age_seconds < 60:
        return f"{age_seconds} seconds"
    if age_seconds < 3600:
        return f"{int(age_seconds / 60)} minutes"
    return f"{int(age_seconds / 3600)} hours"


def _confidence_label(value: float) -> str:
    if value >= 0.8:
        return "High"
    if value >= 0.6:
        return "Medium"
    return "Low"


def _signal_status(timestamp: str | None, *, preferred: str | None = None) -> str:
    if not timestamp:
        return "Initializing"
    if preferred:
        normalized = str(preferred).replace("_", " ").strip()
        return normalized.title() if normalized else "Current"
    return "Current"


def _source_label(evidence_type: str, *, measured_source: str | None = None) -> str:
    if evidence_type == "MEASURED":
        return measured_source or "BigQuery materialized summary"
    if evidence_type == "CALCULATED":
        return "Deterministic payroll policy engine"
    if evidence_type == "MODELED":
        return "Canopy readiness decision engine"
    if evidence_type == "FORECASTED":
        return "Canopy advisory forecast layer"
    return "Canopy decision layer"


def _build_signal_provenance(
    *,
    evidence_type: str,
    timestamp: str | None,
    source: str | None = None,
    confidence: str | None = None,
    status: str | None = None,
) -> dict:
    derived_status = "Initializing" if not timestamp else (status or "Current")
    return {
        "source": source or _source_label(evidence_type),
        "timestamp": timestamp,
        "age_seconds": _age_seconds(timestamp),
        "age_label": _age_label(timestamp),
        "confidence": confidence or ("Initializing" if not timestamp else "Medium"),
        "status": derived_status,
    }


def _readiness_label(value: str) -> str:
    return {
        "READY_FOR_APPROVAL": "Ready for Approval",
        "REVIEW_REQUIRED": "Review Required",
        "HOLD": "Hold",
        "BLOCKED": "Blocked",
    }.get(value, value.replace("_", " ").title())


def _get_run(run_id: str) -> Dict[str, Any]:
    run = DEMO_RUNS.get(run_id)
    if run is None:
        raise KeyError(run_id)
    return run


def _normalize_override_percent(value: float | None) -> float | None:
    if value is None:
        return None
    override = float(value)
    if override > 1:
        override = override / 100.0
    return max(0.0, min(override, 1.0))


def _resolve_effective_deadline(run: dict, latest_evaluation: dict | None = None) -> str:
    if latest_evaluation and latest_evaluation.get("effective_deadline_at"):
        return latest_evaluation["effective_deadline_at"]
    return run["cutoff_at"]


def _deadline_buffer_minutes(run: dict, *, effective_deadline_at: str | None = None) -> int:
    deadline = effective_deadline_at or run["cutoff_at"]
    return int((_parse_dt(deadline) - _parse_dt(run["prefunding_received_at"])).total_seconds() / 60)


def _time_sensitivity_for_deadline(run: dict, effective_deadline_at: str) -> str:
    deadline_buffer = _deadline_buffer_minutes(run, effective_deadline_at=effective_deadline_at)
    threshold = int(run["policy_thresholds"]["minimum_cutoff_buffer_minutes"])
    return "urgent" if deadline_buffer < threshold else "standard"


def _recommended_rail(route_result: dict) -> dict:
    recommended = route_result.get("recommended_rail")
    return next(
        (rail for rail in route_result.get("rails", []) if rail.get("rail") == recommended),
        route_result.get("rails", [{}])[0] if route_result.get("rails") else {},
    )


def _build_query_posture(metrics: dict, route_result: dict) -> dict:
    families = metrics.get("families", {})
    measured_families = []
    for family_name in ("fee_activity", "corridor_volume", "measured_other"):
        family = families.get(family_name)
        if family:
            measured_families.append(
                {
                    "family": family_name,
                    "query_count": family.get("query_count", 0),
                    "execution_bytes": family.get("execution_bytes", 0),
                    "max_budget_utilization": family.get("max_budget_utilization", 0.0),
                    "last_seen": family.get("last_seen"),
                    "summary": (
                        f"{family_name.replace('_', ' ')} used {_fmt_bytes(int(family.get('execution_bytes', 0) or 0))} "
                        f"with a max budget utilization of {_fmt_pct(float(family.get('max_budget_utilization', 0.0) or 0.0))}."
                    ),
                }
            )
    measured_snapshot_at = route_result.get("evidence_packet", {}).get("expected_fee_usd", {}).get("last_updated_at")
    budget_posture = "Active"
    utilization_values = [float(item.get("max_budget_utilization", 0.0) or 0.0) for item in measured_families]
    max_budget_utilization = max(utilization_values) if utilization_values else 0.0
    if not measured_families and not is_real_mode():
        budget_posture = "Paused"
    elif max_budget_utilization >= 1.0:
        budget_posture = "Exceeded"
    return {
        "status": "cache_backed_measured",
        "measured_snapshot_at": measured_snapshot_at,
        "request_path_note": "Request handlers served cached/materialized state only; no raw BigQuery queries ran on the request path.",
        "overall_execution_bytes": metrics.get("overall", {}).get("execution_bytes", 0),
        "overall_execution_bytes_label": _fmt_bytes(int(metrics.get("overall", {}).get("execution_bytes", 0) or 0)),
        "max_budget_utilization": round(max_budget_utilization, 4),
        "budget_posture": budget_posture,
        "families": measured_families,
    }


def _build_system_status(query_posture: dict, forecast_result: dict) -> dict:
    cache_payload = runtime_cache.get_cache()
    refresh_state = runtime_cache.get_refresh_state()
    cache_age = runtime_cache.get_cache_age_seconds()
    last_refresh = refresh_state.get("last_measured_refresh") or cache_payload.get("last_updated")
    cache_status = str(cache_payload.get("status", "initializing")).lower()
    refresh_status = str(refresh_state.get("status", "idle")).lower()
    query_status = "Idle"
    if refresh_status == "querying":
        query_status = "Querying"
    elif refresh_status == "failed" or cache_status == "error":
        query_status = "Failed"
    elif refresh_status == "ready":
        query_status = "Healthy"
    elif cache_status in {"degraded", "initializing"} or (cache_age is not None and cache_age > runtime_cache.POLL_INTERVAL * 2):
        query_status = "Delayed"

    return {
        "operating_mode": "Real" if is_real_mode() else "Demo",
        "operating_mode_runtime": get_runtime_mode(),
        "measured_data_source": "BigQuery" if is_real_mode() else "Seeded demo snapshot",
        "last_measured_refresh": last_refresh,
        "cache_age_seconds": cache_age,
        "poll_interval_minutes": int(runtime_cache.POLL_INTERVAL / 60),
        "query_status": query_status,
        "bigquery_budget_posture": query_posture.get("budget_posture", "Paused"),
        "kill_switch_status": "Active" if forecast_result.get("kill_switch_triggered") else "Inactive",
        "request_path_note": query_posture.get("request_path_note"),
        "refresh_control": refresh_state,
    }


def _build_system_state(route_result: dict, forecast_result: dict, system_status: dict) -> dict:
    refresh_state = system_status.get("refresh_control", {})
    recommended_rail = _recommended_rail(route_result)
    if refresh_state.get("status") == "idle":
        measured_data = "Measured evidence incomplete"
    elif refresh_state.get("status") == "querying":
        measured_data = "Measured evidence initializing"
    else:
        measured_status = _signal_status(
            route_result.get("evidence_packet", {}).get("expected_fee_usd", {}).get("last_updated_at"),
            preferred=recommended_rail.get("freshness_level"),
        )
        if str(recommended_rail.get("freshness_level", "")).lower() != "fresh":
            measured_data = "Using cached summary only"
        else:
            measured_data = measured_status
    system_health = "Healthy"
    if system_status.get("query_status") == "Failed":
        system_health = "Offline"
    elif system_status.get("query_status") in {"Delayed", "Querying"} or system_status.get("kill_switch_status") == "Active":
        system_health = "Degraded"
    elif system_status.get("query_status") == "Idle":
        system_health = "Idle"
    return {
        "measured_data": measured_data,
        "forecast_engine": "Paused" if forecast_result.get("kill_switch_triggered") else "Active",
        "kill_switch": system_status.get("kill_switch_status", "Inactive"),
        "system_health": system_health,
        "evidence_posture_label": (
            "Measured evidence incomplete"
            if refresh_state.get("status") == "idle"
            else "Measured evidence initializing"
            if refresh_state.get("status") == "querying"
            else "Using cached summary only"
            if str(recommended_rail.get("freshness_level", "")).lower() != "fresh"
            else "Current measured evidence"
        ),
        "evidence_posture_detail": (
            "No fresh measured route snapshot is available yet, so Canopy is not release-confident."
            if refresh_state.get("status") == "idle"
            else "A measured refresh is in progress. Treat this corridor view as incomplete until the refresh settles."
            if refresh_state.get("status") == "querying"
            else "Directional liquidity proxy only; this measured layer is stale and should not be treated as corridor truth."
            if str(recommended_rail.get("freshness_level", "")).lower() != "fresh"
            else "Fresh measured route evidence is available for release confidence."
        ),
        "kill_switch_explainer": [
            f"Kill switch {system_status.get('kill_switch_status', 'Inactive')}",
            f"Forecast engine {'paused' if forecast_result.get('kill_switch_triggered') else 'active'}",
            "Recommendations remain advisory",
            "Execution unaffected",
        ],
    }


def _build_handoff_record(run_id: str, latest_decision: dict | None, latest_handoff: dict | None) -> dict:
    decision_approved_at = None
    if latest_decision and latest_decision.get("action") == "APPROVE":
        decision_approved_at = latest_decision.get("updated_at") or latest_decision.get("created_at")
    if not decision_approved_at:
        latest_handoff = None
    status = latest_handoff.get("status") if latest_handoff else ("Queued" if decision_approved_at else "Await approval")
    return {
        "decision_approved_at": decision_approved_at,
        "handoff_triggered_at": latest_handoff.get("sent_at") if latest_handoff else None,
        "execution_acknowledged_at": latest_handoff.get("acknowledged_at") if latest_handoff else None,
        "execution_system": (latest_handoff or {}).get("execution_system", "Airwallex"),
        "status": status,
        "boundary_note": "This action sends the approved decision to the execution system. Execution remains outside Canopy.",
    }


def _build_journey_progress(run: dict, route_result: dict, latest_decision: dict | None, handoff_record: dict) -> List[dict]:
    current_step = "Readiness evaluated"
    if handoff_record.get("status") in {"Sent", "Acknowledged"}:
        current_step = "Handoff triggered"
    elif latest_decision and latest_decision.get("action") == "APPROVE":
        current_step = "Decision recorded"
    steps = [
        ("Funding received", bool(run.get("prefunding_received_at"))),
        ("Readiness evaluated", bool(route_result.get("timestamp"))),
        ("Decision recorded", bool(latest_decision)),
        ("Handoff triggered", handoff_record.get("status") in {"Queued", "Sent", "Acknowledged"}),
        ("Execution pending", handoff_record.get("status") in {"Queued", "Sent", "Acknowledged"}),
        ("Settlement confirmed", False),
    ]
    return [
        {
            "label": label,
            "state": "complete" if complete else ("current" if label == current_step else "pending"),
        }
        for label, complete in steps
    ]


def _arrival_minutes_for_rail(rail_name: str, *, time_sensitivity: str) -> int:
    if str(rail_name).lower() == "polygon":
        return 12 if time_sensitivity == "urgent" else 18
    return 30 if time_sensitivity == "urgent" else 42


def _build_decision_context(run: dict, latest_evaluation: dict | None) -> dict:
    evaluation_summary = (latest_evaluation or {}).get("evaluation_summary", {})
    transfer_amount = float((latest_evaluation or {}).get("transfer_amount_usd") or 100000)
    payroll_currency = (latest_evaluation or {}).get("payroll_currency") or "USD"
    override_percent = _normalize_override_percent((latest_evaluation or {}).get("override_buffer_percent"))
    required_arrival_at = (latest_evaluation or {}).get("required_arrival_at")
    effective_deadline = (latest_evaluation or {}).get("effective_deadline_at") or run["cutoff_at"]
    evaluation_timestamp = (latest_evaluation or {}).get("updated_at")
    return {
        "transfer_amount_usd": round(transfer_amount, 2),
        "transfer_amount_label": _fmt_number(transfer_amount),
        "required_arrival_at": required_arrival_at,
        "effective_deadline_at": effective_deadline,
        "payroll_currency": payroll_currency,
        "override_buffer_percent": override_percent,
        "override_buffer_percent_label": _fmt_pct(override_percent) if override_percent is not None else None,
        "last_evaluation_at": evaluation_timestamp,
        "time_sensitivity": (latest_evaluation or {}).get("time_sensitivity") or _time_sensitivity_for_deadline(run, effective_deadline),
        "minimum_cutoff_buffer_minutes": int(run["policy_thresholds"]["minimum_cutoff_buffer_minutes"]),
        "last_inputs": evaluation_summary.get("inputs", {}),
    }


def _build_buffer_posture(
    run: dict,
    route_result: dict,
    forecast_result: dict,
    exceptions: List[dict],
    *,
    transfer_amount_usd: float,
    effective_deadline_at: str,
    override_buffer_percent: float | None = None,
) -> dict:
    expected = float(run["expected_payroll_amount"])
    prefunding = float(run["prefunding_amount"])
    current_buffer_pct = max((prefunding - expected) / max(expected, 1.0), 0.0)
    recommended_buffer_pct = 0.06
    drivers = ["Base pre-release payroll buffer"]
    if any(item["severity"] == "HIGH" for item in exceptions):
        recommended_buffer_pct = 0.12
        drivers = ["High-severity exception posture increases the required release slack."]
    recommended_rail = _recommended_rail(route_result)
    liquidity_threshold = float(run["policy_thresholds"]["minimum_liquidity_score"])
    liquidity_score = float(recommended_rail.get("liquidity_score_v4", 0.0) or 0.0)
    if liquidity_score < liquidity_threshold:
        recommended_buffer_pct = max(recommended_buffer_pct, 0.14)
        drivers.append("Liquidity proxy is below threshold.")
    if transfer_amount_usd >= 10_000_000:
        recommended_buffer_pct += 0.04
        drivers.append("Higher transfer size increases liquidity risk.")
    elif transfer_amount_usd >= 5_000_000:
        recommended_buffer_pct += 0.02
        drivers.append("Higher transfer size increases liquidity risk.")
    deadline_buffer_minutes = _deadline_buffer_minutes(run, effective_deadline_at=effective_deadline_at)
    if deadline_buffer_minutes < int(run["policy_thresholds"]["minimum_cutoff_buffer_minutes"]):
        recommended_buffer_pct += 0.02
        drivers.append("Late arrival deadline reduces recovery time.")
    if (
        float(forecast_result.get("corridor_stability_probability", 0.0) or 0.0) < 0.65
        or forecast_result.get("kill_switch_triggered")
    ):
        recommended_buffer_pct += 0.02
        drivers.append("Forecast stability is weak, so more prefunding slack is required.")
    recommended_buffer_pct = max(0.06, min(recommended_buffer_pct, 0.18))
    effective_buffer_pct = override_buffer_percent if override_buffer_percent is not None else recommended_buffer_pct
    current_buffer_amount = transfer_amount_usd * current_buffer_pct
    effective_buffer_amount = transfer_amount_usd * effective_buffer_pct
    return {
        "current_prefunding_amount": prefunding,
        "current_buffer_percent": round(current_buffer_pct, 4),
        "current_buffer_amount": round(current_buffer_amount, 2),
        "recommended_buffer_percent": round(recommended_buffer_pct, 4),
        "safe_buffer_range_min": round(max(recommended_buffer_pct - 0.03, 0.06), 4),
        "safe_buffer_range_max": round(min(recommended_buffer_pct + 0.03, 0.18), 4),
        "confidence_score": 0.84 if recommended_buffer_pct >= 0.12 else 0.91,
        "top_buffer_drivers": drivers,
        "buffer_reduction_requirements": " ".join(drivers),
        "why_this_buffer": " ".join(drivers),
        "override_buffer_percent": round(override_buffer_percent, 4) if override_buffer_percent is not None else None,
        "override_warning": "Manual override increases settlement risk." if override_buffer_percent is not None else "",
        "effective_buffer_percent": round(effective_buffer_pct, 4),
        "effective_buffer_amount": round(effective_buffer_amount, 2),
        "deadline_buffer_minutes": deadline_buffer_minutes,
        "freshness_timestamp": route_result.get("timestamp"),
    }


def _build_route_comparison(route_result: dict, *, time_sensitivity: str) -> List[dict]:
    rows: List[dict] = []
    selected_rails = [
        route_result.get("recommended_rail"),
        route_result.get("alternative_rail"),
    ]
    seen: set[str] = set()
    for rail_name in selected_rails:
        if not rail_name or rail_name in seen:
            continue
        rail_payload = next(
            (item for item in route_result.get("rails", []) if item.get("rail") == rail_name),
            None,
        )
        if rail_payload is None:
            continue
        seen.add(rail_name)
        transfer_math = rail_payload.get("transfer_math", {})
        strategy = rail_payload.get("strategy_assessment", {})
        minutes = _arrival_minutes_for_rail(rail_name, time_sensitivity=time_sensitivity)
        timestamp = route_result.get("timestamp")
        rows.append(
            {
                "rail": rail_name,
                "network_fee_usd": round(float(transfer_math.get("network_fee_usd", 0.0) or 0.0), 2),
                "routing_fee_usd": round(float(transfer_math.get("routing_fee_usd", 0.0) or 0.0), 2),
                "total_fee_usd": round(float(transfer_math.get("total_fee_usd", 0.0) or 0.0), 2),
                "estimated_arrival_minutes": minutes,
                "estimated_arrival_label": f"{minutes} minutes",
                "confidence_score_label": strategy.get("strategy_score_label") or rail_payload.get("evidence_confidence_label") or "—",
                "timestamp": timestamp,
                "age_label": _age_label(timestamp),
                "status": _signal_status(timestamp, preferred=rail_payload.get("freshness_level")),
            }
        )
    return rows


def _build_capital_impact(
    run: dict,
    *,
    transfer_amount_usd: float,
    recommended_buffer_percent: float,
    override_buffer_percent: float | None,
    effective_deadline_at: str,
    safe_buffer_range_min: float,
    safe_buffer_range_max: float,
) -> dict:
    expected = float(run["expected_payroll_amount"])
    prefunding = float(run["prefunding_amount"])
    current_buffer_percent = max((prefunding - expected) / max(expected, 1.0), 0.0)
    current_buffer_amount = transfer_amount_usd * current_buffer_percent
    new_buffer_percent = override_buffer_percent if override_buffer_percent is not None else recommended_buffer_percent
    new_buffer_amount = transfer_amount_usd * new_buffer_percent
    recommended_buffer_amount = transfer_amount_usd * recommended_buffer_percent
    capital_delta = current_buffer_amount - new_buffer_amount
    capital_released = max(capital_delta, 0.0)
    additional_prefunding_required = max(new_buffer_amount - current_buffer_amount, 0.0)
    capital_delta_direction = "neutral"
    if capital_released > 0:
        capital_delta_direction = "released"
    elif additional_prefunding_required > 0:
        capital_delta_direction = "required"
    yield_opportunity = capital_released * ILLUSTRATIVE_YIELD_RATE if capital_released > 0 else 0.0
    time_until_cutoff_minutes = max(_deadline_buffer_minutes(run, effective_deadline_at=effective_deadline_at), 0)
    return {
        "current_buffer_percent": round(current_buffer_percent, 4),
        "current_buffer_amount": round(current_buffer_amount, 2),
        "recommended_buffer_percent": round(recommended_buffer_percent, 4),
        "recommended_buffer_amount": round(recommended_buffer_amount, 2),
        "new_buffer_percent": round(new_buffer_percent, 4),
        "new_buffer_amount": round(new_buffer_amount, 2),
        "selected_buffer_percent": round(new_buffer_percent, 4),
        "selected_buffer_amount": round(new_buffer_amount, 2),
        "capital_delta_amount": round(abs(capital_delta), 2),
        "capital_delta_direction": capital_delta_direction,
        "additional_prefunding_required": round(additional_prefunding_required, 2),
        "capital_released": round(capital_released, 2),
        "capital_released_label": _fmt_usd(capital_released),
        "yield_opportunity_estimate_annual": round(yield_opportunity, 2),
        "yield_assumption_label": "Illustrative 4.5% annual T-bill yield",
        "effective_deadline_at": effective_deadline_at,
        "time_until_cutoff_minutes": time_until_cutoff_minutes,
        "safe_operating_range": {
            "min_percent": round(safe_buffer_range_min, 4),
            "max_percent": round(safe_buffer_range_max, 4),
        },
    }


def _decision_surface_tone(display_decision_label: str) -> str:
    if display_decision_label == "Approve":
        return "green"
    if display_decision_label == "Escalate":
        return "yellow"
    return "red"


def _build_decision_surface(
    run: dict,
    readiness: dict,
    system_status: dict,
    system_state: dict,
    measured_snapshot: dict,
    policy_checks: List[dict],
    capital_impact: dict,
) -> dict:
    refresh_state = system_status.get("refresh_control", {})
    refresh_status = str(refresh_state.get("status", "")).lower()
    query_status = str(system_status.get("query_status", "")).lower()
    budget_posture = str(system_status.get("bigquery_budget_posture", "")).lower()
    kill_switch_active = str(system_status.get("kill_switch_status", "")).lower() == "active"
    measured_timestamp = measured_snapshot.get("freshness_timestamp")
    freshness_level = str(measured_snapshot.get("freshness_level", "")).lower()
    data_status = str(measured_snapshot.get("data_status", "")).lower()

    if kill_switch_active:
        system_state_code = "kill_switch_active"
        system_state_label = "Kill switch active"
    elif refresh_status == "querying" or (refresh_status == "idle" and not measured_timestamp):
        system_state_code = "evidence_initializing"
        system_state_label = "Measured evidence initializing"
    elif (
        query_status in {"delayed", "failed"}
        or freshness_level not in {"fresh", ""}
        or data_status not in {"fresh", "healthy", "current", ""}
    ):
        system_state_code = "evidence_delayed"
        system_state_label = "Measured evidence delayed"
    elif budget_posture == "exceeded":
        system_state_code = "budget_protection_active"
        system_state_label = "BigQuery budget protection active"
    elif str(system_state.get("system_health", "")).lower() == "degraded":
        system_state_code = "degraded_but_decisionable"
        system_state_label = "Degraded but decisionable"
    else:
        system_state_code = "healthy"
        system_state_label = "System healthy"

    check_map = {item["label"]: item for item in policy_checks}
    corridor_specific_failures = [
        check_map.get("Cutoff buffer", {}).get("status") == "fail",
        check_map.get("Liquidity threshold", {}).get("status") == "fail",
        check_map.get("Beneficiary review", {}).get("status") == "fail",
    ]
    beneficiary_open = check_map.get("Beneficiary review", {}).get("status") == "fail"
    corridor_blocked = any(corridor_specific_failures)

    if not corridor_blocked:
        corridor_state_code = "approve"
        corridor_state_label = "Approve"
    elif beneficiary_open:
        corridor_state_code = "conditional_hold"
        corridor_state_label = "Conditional hold"
    elif readiness.get("recommended_action") == "ESCALATE" or readiness.get("readiness_state") == "REVIEW_REQUIRED":
        corridor_state_code = "escalate"
        corridor_state_label = "Escalate"
    else:
        corridor_state_code = "hold"
        corridor_state_label = "Hold"

    is_evidence_limited = system_state_code in {
        "kill_switch_active",
        "evidence_initializing",
        "evidence_delayed",
        "budget_protection_active",
    }

    if readiness.get("readiness_state") == "READY_FOR_APPROVAL" and not is_evidence_limited:
        display_decision_label = "Approve"
    elif is_evidence_limited:
        display_decision_label = "Evidence-Limited Hold"
    elif corridor_state_code == "conditional_hold":
        display_decision_label = "Conditional Hold"
    elif corridor_state_code == "escalate":
        display_decision_label = "Escalate"
    else:
        display_decision_label = "Hold"

    top_blocker = (readiness.get("blockers") or [{}])[0]
    top_blocker_label = top_blocker.get("label", "")
    additional_prefunding_required = float(capital_impact.get("additional_prefunding_required", 0.0) or 0.0)

    if system_state_code == "kill_switch_active":
        next_step = "Confirm the kill switch posture, then rerun readiness before approving."
        why_it_matters = "A kill switch is active, so the system is intentionally withholding release confidence."
    elif system_state_code in {"evidence_initializing", "evidence_delayed", "budget_protection_active"}:
        if corridor_state_code == "conditional_hold":
            next_step = "Refresh measured evidence, clear the beneficiary review, and rerun readiness."
        elif corridor_state_code in {"hold", "escalate"} and top_blocker_label:
            next_step = f"Refresh measured evidence, clear '{top_blocker_label.lower()}', and rerun readiness."
        else:
            next_step = "Refresh measured evidence and rerun readiness before approving."
        why_it_matters = "Releasing now would rely on cached or incomplete measured evidence instead of a current corridor read."
    elif corridor_state_code == "conditional_hold":
        next_step = run.get("recommended_next_action") or "Clear the beneficiary review, then rerun readiness."
        why_it_matters = "Approval is blocked until the changed payroll records are verified and cleared for release."
    elif corridor_state_code == "escalate":
        next_step = run.get("recommended_next_action") or "Escalate the run for manual review and keep the fallback rail ready."
        why_it_matters = "The current posture is decisionable, but manual intervention is still required before release can be recorded."
    elif additional_prefunding_required > 0:
        next_step = "Add prefunding slack to the recommended level, then rerun readiness."
        why_it_matters = f"The current posture is under the recommended release buffer by {_fmt_usd(additional_prefunding_required)}."
    elif display_decision_label == "Approve":
        next_step = "Approve the run and send the recorded decision to handoff when operations are ready."
        why_it_matters = "System and corridor conditions currently support release within the operating window."
    else:
        next_step = run.get("recommended_next_action") or "Hold the run, clear the blocking conditions, and rerun readiness."
        why_it_matters = "The run still has corridor-specific blockers that would make release unsafe or premature."

    return {
        "display_decision_label": display_decision_label,
        "system_state_code": system_state_code,
        "system_state_label": system_state_label,
        "corridor_state_code": corridor_state_code,
        "corridor_state_label": corridor_state_label,
        "is_evidence_limited": is_evidence_limited,
        "next_step": next_step,
        "why_it_matters": why_it_matters,
        "banner_tone": _decision_surface_tone(display_decision_label),
    }


def _build_blocking_summary(
    system_status: dict,
    decision_surface: dict,
    policy_checks: List[dict],
) -> List[dict]:
    items: List[dict] = []

    def append_item(
        *,
        key: str,
        label: str,
        severity: str,
        scope: str,
        evidence_type: str,
        determinism: str,
        current_state: str,
        target_state: str,
        why_blocking: str,
    ) -> None:
        items.append(
            {
                "key": key,
                "label": label,
                "severity": severity,
                "scope": scope,
                "evidence_type": evidence_type,
                "determinism": determinism,
                "current_state": current_state,
                "target_state": target_state,
                "why_blocking": why_blocking,
            }
        )

    system_code = decision_surface.get("system_state_code")
    if system_code == "kill_switch_active":
        append_item(
            key="kill_switch",
            label="Kill switch active",
            severity="Critical",
            scope="system",
            evidence_type="FORECASTED",
            determinism="advisory",
            current_state="Kill switch active",
            target_state="Kill switch inactive",
            why_blocking="The forecast safety layer is actively withholding release confidence until the kill switch clears.",
        )
    elif system_code in {"evidence_initializing", "evidence_delayed", "budget_protection_active"}:
        append_item(
            key=system_code,
            label=decision_surface.get("system_state_label", "Measured evidence delayed"),
            severity="Critical",
            scope="system",
            evidence_type="MEASURED",
            determinism="measured",
            current_state=system_status.get("query_status", "Initializing"),
            target_state="Current measured evidence",
            why_blocking="The current recommendation is operating on incomplete or delayed measured evidence rather than a fresh corridor read.",
        )

    for check in policy_checks:
        if check.get("status") not in {"fail", "warn"}:
            continue
        if check["label"] == "Measured data freshness" and decision_surface.get("is_evidence_limited"):
            continue
        if check["label"] == "Forecast advisory" and system_code == "kill_switch_active":
            continue

        scope = "system" if check["label"] in {"Measured data freshness", "Forecast advisory"} else "corridor"
        determinism = {
            "CALCULATED": "deterministic",
            "MEASURED": "measured",
            "MODELED": "modeled",
            "FORECASTED": "advisory",
        }.get(check.get("evidence_type"), "measured")
        severity = "High"
        if check["label"] == "Forecast advisory" or check.get("status") == "warn":
            severity = "Medium"
        append_item(
            key=re.sub(r"[^a-z0-9]+", "_", check["label"].lower()).strip("_"),
            label=check["label"],
            severity=severity,
            scope=scope,
            evidence_type=check.get("evidence_type", "MEASURED"),
            determinism=determinism,
            current_state=check.get("actual_value_label") or check.get("actual_value") or "-",
            target_state=check.get("policy_threshold_label") or "-",
            why_blocking=check.get("detail") or "This condition must clear before approval is safe.",
        )

    severity_rank = {"Critical": 3, "High": 2, "Medium": 1}
    items.sort(key=lambda item: (-severity_rank.get(item["severity"], 0), item["label"]))
    return items


def _build_decision_flip_conditions(
    decision_surface: dict,
    policy_checks: List[dict],
) -> List[dict]:
    check_map = {item["label"]: item for item in policy_checks}
    conditions: List[dict] = []

    def add_condition(label: str, *, scope: str, current_state: str, target_state: str, why_it_flips: str) -> None:
        conditions.append(
            {
                "label": label,
                "scope": scope,
                "current_state": current_state,
                "target_state": target_state,
                "why_it_flips": why_it_flips,
            }
        )

    if decision_surface.get("system_state_code") in {"evidence_initializing", "evidence_delayed", "budget_protection_active"}:
        add_condition(
            "Measured route freshness returns to current",
            scope="system",
            current_state=decision_surface.get("system_state_label", "Measured evidence delayed"),
            target_state="Current measured evidence",
            why_it_flips="A fresh measured snapshot restores release confidence and separates system posture from corridor posture.",
        )
    if decision_surface.get("system_state_code") == "kill_switch_active":
        add_condition(
            "Kill switch clears",
            scope="system",
            current_state="Kill switch active",
            target_state="Kill switch inactive",
            why_it_flips="The advisory kill switch must clear before the system can move back to a release-confident state.",
        )
    if check_map.get("Beneficiary review", {}).get("status") == "fail":
        add_condition(
            "Beneficiary review clears",
            scope="corridor",
            current_state=check_map["Beneficiary review"].get("actual_value_label", "Open"),
            target_state=check_map["Beneficiary review"].get("policy_threshold_label", "Clear"),
            why_it_flips="The payroll file cannot be approved until the changed beneficiary records are verified.",
        )
    if check_map.get("Liquidity threshold", {}).get("status") == "fail":
        add_condition(
            "Liquidity threshold rises above minimum",
            scope="corridor",
            current_state=check_map["Liquidity threshold"].get("actual_value_label", "-"),
            target_state=check_map["Liquidity threshold"].get("policy_threshold_label", "-"),
            why_it_flips="Approval requires the corridor liquidity proxy to recover above the configured release threshold.",
        )
    if check_map.get("Cutoff buffer", {}).get("status") == "fail":
        add_condition(
            "Cutoff buffer returns above minimum",
            scope="corridor",
            current_state=check_map["Cutoff buffer"].get("actual_value_label", "-"),
            target_state=check_map["Cutoff buffer"].get("policy_threshold_label", "-"),
            why_it_flips="The run needs enough timing slack to complete controls before the payout deadline.",
        )
    if not conditions:
        add_condition(
            "No flip conditions required",
            scope="corridor",
            current_state="All blocking checks are clear",
            target_state="Approve remains available",
            why_it_flips="The run is already aligned with the current release posture.",
        )
    return conditions


def _build_alternative_paths(
    decision_surface: dict,
    policy_checks: List[dict],
    route_comparison: List[dict],
    buffer_recommendation: dict,
    capital_impact: dict,
) -> List[dict]:
    if decision_surface.get("display_decision_label") == "Approve":
        return []

    check_map = {item["label"]: item for item in policy_checks}
    paths: List[dict] = []

    def add_path(
        key: str,
        *,
        action: str,
        likely_outcome: str,
        timing_impact: str,
        capital_impact_detail: str,
        decision_effect: str,
    ) -> None:
        if any(item["key"] == key for item in paths):
            return
        paths.append(
            {
                "key": key,
                "action": action,
                "likely_outcome": likely_outcome,
                "timing_impact": timing_impact,
                "capital_impact": capital_impact_detail,
                "decision_effect": decision_effect,
            }
        )

    if decision_surface.get("is_evidence_limited"):
        add_path(
            "wait_refresh",
            action="Wait for the next measured refresh and rerun readiness",
            likely_outcome="System posture should move from evidence-limited to current once a fresh measured snapshot lands.",
            timing_impact="Best case: next 5-minute refresh window.",
            capital_impact_detail="No immediate prefunding change while you wait.",
            decision_effect="Can flip the decision only if corridor-specific blockers are already clear.",
        )

    if check_map.get("Beneficiary review", {}).get("status") == "fail":
        add_path(
            "clear_beneficiary",
            action="Clear the beneficiary review and rerun",
            likely_outcome="Removes the compliance blocker from the approval path.",
            timing_impact="Depends on review turnaround from compliance operations.",
            capital_impact_detail="No direct capital change.",
            decision_effect="Can flip the decision if system evidence is current and no other blockers remain.",
        )

    additional_prefunding_required = float(capital_impact.get("additional_prefunding_required", 0.0) or 0.0)
    if additional_prefunding_required > 0:
        add_path(
            "increase_buffer",
            action="Increase buffer toward the safe operating range",
            likely_outcome="Adds prefunding slack and reduces release fragility around timing and liquidity.",
            timing_impact="Can be actioned immediately once treasury confirms funding.",
            capital_impact_detail=f"Requires {_fmt_usd(additional_prefunding_required)} of additional prefunding.",
            decision_effect="May improve readiness, but does not override stale measured evidence on its own.",
        )

    fallback_rail = next((item for item in route_comparison[1:] if item.get("rail")), None)
    if fallback_rail:
        add_path(
            "fallback_rail",
            action=f"Keep {fallback_rail['rail']} ready as the fallback rail",
            likely_outcome="Preserves an alternative execution path once the run is release-ready.",
            timing_impact=f"Estimated arrival on fallback: {fallback_rail.get('estimated_arrival_label', '-')}.",
            capital_impact_detail="No direct prefunding change; this is an execution-readiness option.",
            decision_effect="Improves operator readiness and can shorten release time once blockers clear.",
        )

    while len(paths) < 2:
        add_path(
            "manual_escalation",
            action="Escalate to manual review with the current blocker pack",
            likely_outcome="Ensures the run is reviewed with the latest system and corridor context.",
            timing_impact="Adds manual review time before release can be recorded.",
            capital_impact_detail="No direct capital change.",
            decision_effect="Improves readiness, but does not automatically flip the decision.",
        )

    return paths[:3]


def _build_policy_checks(
    run: dict,
    route_result: dict,
    forecast_result: dict,
    exceptions: List[dict],
    *,
    effective_deadline_at: str,
) -> List[dict]:
    cutoff_minutes = _deadline_buffer_minutes(run, effective_deadline_at=effective_deadline_at)
    recommended_rail = _recommended_rail(route_result)
    liquidity_score = float(recommended_rail.get("liquidity_score_v4", 0.0) or 0.0)
    min_liquidity_score = float(run["policy_thresholds"]["minimum_liquidity_score"])
    freshness_level = str(recommended_rail.get("freshness_level", "unknown")).lower()
    beneficiary_blocker = any(item["exception_type"] == "beneficiary_identity_mismatch" for item in exceptions)
    measured_source = route_result.get("evidence_packet", {}).get("expected_fee_usd", {}).get("data_source")
    checks = [
        {
            "label": "Cutoff buffer",
            "status": "pass" if cutoff_minutes >= int(run["policy_thresholds"]["minimum_cutoff_buffer_minutes"]) else "fail",
            "detail": f"{cutoff_minutes} minutes available before the required arrival deadline.",
            "evidence_type": "CALCULATED",
            "policy_threshold": int(run["policy_thresholds"]["minimum_cutoff_buffer_minutes"]),
            "policy_threshold_label": f"{int(run['policy_thresholds']['minimum_cutoff_buffer_minutes'])} minutes",
            "actual_value": cutoff_minutes,
            "actual_value_label": f"{cutoff_minutes} minutes",
            "decision_trigger": "Funding inside cutoff window" if cutoff_minutes < int(run["policy_thresholds"]["minimum_cutoff_buffer_minutes"]) else "Threshold met",
            "freshness_timestamp": run["prefunding_received_at"],
        },
        {
            "label": "Liquidity threshold",
            "status": "pass" if liquidity_score >= min_liquidity_score else "fail",
            "detail": f"Measured liquidity score {liquidity_score:.2f} vs threshold {min_liquidity_score:.2f}.",
            "evidence_type": "MEASURED",
            "policy_threshold": min_liquidity_score,
            "policy_threshold_label": f"{min_liquidity_score:.2f}",
            "actual_value": liquidity_score,
            "actual_value_label": f"{liquidity_score:.2f}",
            "decision_trigger": "Liquidity below threshold" if liquidity_score < min_liquidity_score else "Threshold met",
            "freshness_timestamp": route_result.get("evidence_packet", {}).get("expected_fee_usd", {}).get("last_updated_at"),
        },
        {
            "label": "Beneficiary review",
            "status": "fail" if beneficiary_blocker else "pass",
            "detail": "Ownership mismatches remain open." if beneficiary_blocker else "No beneficiary blockers are open.",
            "evidence_type": "MEASURED",
            "policy_threshold": 0,
            "policy_threshold_label": "0 open mismatches",
            "actual_value": 1 if beneficiary_blocker else 0,
            "actual_value_label": "Open" if beneficiary_blocker else "Clear",
            "decision_trigger": "Beneficiary mismatch" if beneficiary_blocker else "No beneficiary mismatch",
            "freshness_timestamp": route_result.get("evidence_packet", {}).get("expected_fee_usd", {}).get("last_updated_at"),
        },
        {
            "label": "Measured data freshness",
            "status": "pass" if freshness_level == "fresh" else "fail",
            "detail": f"Recommended rail freshness is {recommended_rail.get('freshness_level', 'unknown')}.",
            "evidence_type": "MEASURED",
            "policy_threshold": run["policy_thresholds"]["minimum_freshness_level"],
            "policy_threshold_label": str(run["policy_thresholds"]["minimum_freshness_level"]).title(),
            "actual_value": freshness_level,
            "actual_value_label": str(recommended_rail.get("freshness_level", "unknown")).title(),
            "decision_trigger": "Measured data stale" if freshness_level != "fresh" else "Freshness met",
            "freshness_timestamp": route_result.get("evidence_packet", {}).get("expected_fee_usd", {}).get("last_updated_at"),
        },
        {
            "label": "Forecast advisory",
            "status": "warn" if forecast_result.get("kill_switch_triggered") else "pass",
            "detail": (
                "Advisory only. Forecast kill switch is raised."
                if forecast_result.get("kill_switch_triggered")
                else "Forecast layer is advisory only and does not block release on its own."
            ),
            "evidence_type": "FORECASTED",
            "policy_threshold": "Advisory only",
            "policy_threshold_label": "Advisory only",
            "actual_value": "Kill switch raised" if forecast_result.get("kill_switch_triggered") else "No kill switch",
            "actual_value_label": "Kill switch raised" if forecast_result.get("kill_switch_triggered") else "No kill switch",
            "decision_trigger": "Forecast kill switch active" if forecast_result.get("kill_switch_triggered") else "Forecast remains non-blocking",
            "freshness_timestamp": forecast_result.get("forecast_freshness", {}).get("generated_at"),
        },
    ]
    for item in checks:
        item["provenance"] = _build_signal_provenance(
            evidence_type=item["evidence_type"],
            timestamp=item.get("freshness_timestamp"),
            source=_source_label(item["evidence_type"], measured_source=measured_source),
            confidence="High" if item["evidence_type"] == "CALCULATED" else ("Medium" if item["evidence_type"] == "MEASURED" else "Low"),
            status=_signal_status(item.get("freshness_timestamp"), preferred=item["status"]),
        )
    return checks


def _build_decision_rule(
    run: dict,
    route_result: dict,
    forecast_result: dict,
    exceptions: List[dict],
    readiness: dict,
    *,
    effective_deadline_at: str,
) -> dict:
    cutoff_minutes = _deadline_buffer_minutes(run, effective_deadline_at=effective_deadline_at)
    recommended_rail = _recommended_rail(route_result)
    liquidity_score = float(recommended_rail.get("liquidity_score_v4", 0.0) or 0.0)
    liquidity_threshold = float(run["policy_thresholds"]["minimum_liquidity_score"])
    cutoff_threshold = int(run["policy_thresholds"]["minimum_cutoff_buffer_minutes"])
    beneficiary_open = any(item["exception_type"] == "beneficiary_identity_mismatch" for item in exceptions)
    freshness_level = str(recommended_rail.get("freshness_level", "unknown")).lower()
    data_status = str(recommended_rail.get("data_status", "unknown")).lower()

    conditions = [
        f"liquidity score {liquidity_score:.2f} < {liquidity_threshold:.2f}",
        f"beneficiary review open = {'true' if beneficiary_open else 'false'}",
        f"deadline buffer {cutoff_minutes} < {cutoff_threshold}",
        f"measured status = {data_status}/{freshness_level}",
        f"forecast kill switch = {'true' if forecast_result.get('kill_switch_triggered') else 'false'}",
    ]
    logic = (
        "If liquidity score < threshold OR beneficiary review is open OR deadline buffer is below minimum "
        "OR measured evidence is stale/degraded, then do not release. "
        "If those checks clear but the forecast kill switch is active, escalate for review. Otherwise approve."
    )
    return {
        "title": "Payroll readiness release rule",
        "condition": " | ".join(conditions),
        "logic": logic,
        "result": readiness["readiness_label"],
        "recommended_action": readiness["recommended_action_label"],
        "triggered_by": [item["label"] for item in readiness["blockers"]] or ["No blocking conditions triggered"],
    }


def _forecast_action_path(run: dict, forecast_result: dict, readiness: dict, *, effective_deadline_at: str) -> dict:
    cutoff_local = _parse_dt(effective_deadline_at)
    escalation_time = cutoff_local.replace(hour=17, minute=30).isoformat()
    stable = float(forecast_result.get("corridor_stability_probability", 0.0) or 0.0) >= 0.6
    scenario = "Liquidity recovery likely" if stable and not forecast_result.get("kill_switch_triggered") else "Liquidity recovery uncertain"
    next_state = "Release payroll" if readiness["recommended_action"] == "APPROVE" else readiness["recommended_action_label"]
    trigger_condition = (
        "counterparty capacity restored before cutoff and beneficiary review closed"
        if stable
        else "Measured blockers remain active into the release window"
    )
    escalation_condition = (
        f"Manual payout review required if unresolved by {escalation_time}"
        if readiness["recommended_action"] != "APPROVE" or forecast_result.get("kill_switch_triggered")
        else f"Escalate if corridor stability weakens before {escalation_time}"
    )
    timestamp = forecast_result.get("forecast_freshness", {}).get("generated_at")
    return {
        "forecast_scenario": scenario,
        "next_expected_state": next_state,
        "trigger_condition": trigger_condition,
        "escalation_condition": escalation_condition,
        "provenance": _build_signal_provenance(
            evidence_type="FORECASTED",
            timestamp=timestamp,
            source="Canopy advisory forecast layer",
            confidence="Medium" if timestamp else "Initializing",
            status=_signal_status(timestamp, preferred=forecast_result.get("status")),
        ),
    }


def _build_readiness(
    run: dict,
    route_result: dict,
    forecast_result: dict,
    exceptions: List[dict],
    buffer_posture: dict,
    *,
    effective_deadline_at: str,
) -> dict:
    cutoff_minutes = _deadline_buffer_minutes(run, effective_deadline_at=effective_deadline_at)
    recommended_rail = _recommended_rail(route_result)
    liquidity_score = float(recommended_rail.get("liquidity_score_v4", 0.0) or 0.0)
    freshness_level = str(recommended_rail.get("freshness_level", "unknown")).lower()
    status = str(recommended_rail.get("data_status", "unknown")).lower()
    high_exception_count = sum(1 for item in exceptions if item["severity"] == "HIGH")
    blockers: List[dict] = []

    if cutoff_minutes < int(run["policy_thresholds"]["minimum_cutoff_buffer_minutes"]):
        blockers.append(
            {
                "label": "Funding arrived inside the cutoff risk window",
                "detail": f"Only {cutoff_minutes} minutes remain before the required arrival deadline.",
                "evidence_type": "CALCULATED",
            }
        )
    if any(item["exception_type"] == "beneficiary_identity_mismatch" for item in exceptions):
        blockers.append(
            {
                "label": "Beneficiary ownership changes require manual review",
                "detail": "Open mismatches remain on the payroll file.",
                "evidence_type": "MEASURED",
            }
        )
    if liquidity_score < float(run["policy_thresholds"]["minimum_liquidity_score"]):
        blockers.append(
            {
                "label": "Liquidity confidence is below threshold",
                "detail": f"Measured liquidity score is {liquidity_score:.2f}.",
                "evidence_type": "MEASURED",
            }
        )
    if status != "fresh" or freshness_level != "fresh":
        blockers.append(
            {
                "label": "Measured route evidence is stale or degraded",
                "detail": f"Route status is {recommended_rail.get('data_status')} / {recommended_rail.get('freshness_level')}.",
                "evidence_type": "MEASURED",
            }
        )

    if blockers and (status != "fresh" or freshness_level != "fresh"):
        readiness_state = "HOLD"
        risk_level = "HIGH"
        recommended_action = "HOLD"
    elif high_exception_count >= 2 or len(blockers) >= 3:
        readiness_state = "HOLD"
        risk_level = "HIGH"
        recommended_action = "HOLD"
    elif blockers:
        readiness_state = "REVIEW_REQUIRED"
        risk_level = "MODERATE"
        recommended_action = "ESCALATE"
    else:
        readiness_state = "READY_FOR_APPROVAL"
        risk_level = "LOW"
        recommended_action = "APPROVE"

    if forecast_result.get("kill_switch_triggered") and readiness_state == "READY_FOR_APPROVAL":
        readiness_state = "REVIEW_REQUIRED"
        risk_level = "MODERATE"
        recommended_action = "ESCALATE"
        blockers.append(
            {
                "label": "Forecast advisory raised caution",
                "detail": "The forecast layer is advisory only, but it is pushing this run into manual review.",
                "evidence_type": "FORECASTED",
            }
        )

    return {
        "readiness_state": readiness_state,
        "readiness_label": _readiness_label(readiness_state),
        "risk_level": risk_level,
        "risk_label": risk_level.title(),
        "recommended_action": recommended_action,
        "recommended_action_label": ACTION_LABELS[recommended_action],
        "top_drivers": blockers[:3],
        "blockers": blockers,
        "buffer_posture": buffer_posture,
    }


def _build_evidence_ladder(
    run: dict,
    route_result: dict,
    forecast_result: dict,
    readiness: dict,
    query_posture: dict,
    *,
    payroll_data_state: dict,
    effective_deadline_at: str,
) -> List[dict]:
    cutoff_minutes = _deadline_buffer_minutes(run, effective_deadline_at=effective_deadline_at)
    recommended_rail = _recommended_rail(route_result)
    buffer_posture = readiness["buffer_posture"]
    measured_source = route_result.get("evidence_packet", {}).get("expected_fee_usd", {}).get("data_source")
    measured_freshness_level = str(recommended_rail.get("freshness_level", "")).lower()
    measured_detail = recommended_rail.get("liquidity_proxy_detail")
    if measured_freshness_level != "fresh":
        measured_detail = (
            "Using cached summary only. Directional liquidity proxy only; not sufficient for release confidence."
        )
    items = [
        {
            "key": "funding_timing",
            "title": "Funding arrival timing",
            "value": f"{cutoff_minutes} mins before cutoff",
            "detail": "Deterministic cutoff math from the seeded payroll schedule and prefunding timestamp.",
            "evidence_type": "CALCULATED",
            "freshness_timestamp": run["prefunding_received_at"],
            "confidence": "High",
            "status": "Current",
        },
        {
            "key": "liquidity_proxy",
            "title": "Liquidity proxy",
            "value": recommended_rail.get("liquidity_proxy_label", "Liquidity Proxy"),
            "detail": measured_detail,
            "evidence_type": "MEASURED",
            "freshness_timestamp": query_posture.get("measured_snapshot_at"),
            "confidence": recommended_rail.get("evidence_confidence_label") or "Medium",
            "status": _signal_status(query_posture.get("measured_snapshot_at"), preferred=recommended_rail.get("freshness_level")),
        },
        {
            "key": "payroll_data_source",
            "title": "Payroll Data Source",
            "value": payroll_data_state.get("source_type_label", "Payroll dataset snapshot"),
            "detail": payroll_data_state.get("lineage_label", "Payroll dataset snapshot -> BigQuery -> Decision Engine"),
            "evidence_type": "MEASURED",
            "freshness_timestamp": payroll_data_state.get("last_loaded_timestamp"),
            "confidence": "High" if payroll_data_state.get("verification_status") == "verified" else "Medium",
            "status": payroll_data_state.get("data_status_label", "Initializing"),
        },
        {
            "key": "buffer_posture",
            "title": "Buffer recommendation",
            "value": f"{buffer_posture['recommended_buffer_percent'] * 100:.0f}% target buffer",
            "detail": buffer_posture["buffer_reduction_requirements"],
            "evidence_type": "CALCULATED",
            "freshness_timestamp": buffer_posture["freshness_timestamp"],
            "confidence": _confidence_label(float(buffer_posture.get("confidence_score", 0.0) or 0.0)),
            "status": "Current",
        },
        {
            "key": "route_recommendation",
            "title": "Route recommendation",
            "value": f"{route_result.get('recommended_rail')} with {route_result.get('strategy_score_label')}",
            "detail": route_result.get("why_this_route", ["No recommendation narrative available."])[0],
            "evidence_type": "MODELED",
            "freshness_timestamp": route_result.get("timestamp"),
            "confidence": route_result.get("evidence_confidence_label") or "Medium",
            "status": "Current",
        },
        {
            "key": "delay_advisory",
            "title": "Delay / stability advisory",
            "value": f"{int(float(forecast_result.get('corridor_stability_probability', 0.0) or 0.0) * 100)}% stability probability",
            "detail": "Advisory only. This forecast does not override the readiness decision.",
            "evidence_type": "FORECASTED",
            "freshness_timestamp": forecast_result.get("forecast_freshness", {}).get("generated_at"),
            "confidence": "Medium",
            "status": _signal_status(
                forecast_result.get("forecast_freshness", {}).get("generated_at"),
                preferred=forecast_result.get("status"),
            ),
        },
    ]
    for item in items:
        item["provenance"] = _build_signal_provenance(
            evidence_type=item["evidence_type"],
            timestamp=item.get("freshness_timestamp"),
            source=_source_label(item["evidence_type"], measured_source=measured_source),
            confidence=item.get("confidence"),
            status=item.get("status"),
        )
    return items


def _build_measured_snapshot(route_result: dict, forecast_result: dict, query_posture: dict) -> dict:
    recommended_rail = _recommended_rail(route_result)
    evidence_packet = route_result.get("evidence_packet", {})
    freshness_level = str(recommended_rail.get("freshness_level", "")).lower()
    if not evidence_packet.get("expected_fee_usd", {}).get("last_updated_at"):
        evidence_posture_label = "Measured evidence incomplete"
        evidence_posture_detail = "No fresh measured snapshot is available yet."
    elif freshness_level != "fresh":
        evidence_posture_label = "Using cached summary only"
        evidence_posture_detail = "Directional liquidity proxy only; not sufficient for release confidence."
    else:
        evidence_posture_label = "Current measured evidence"
        evidence_posture_detail = "Fresh measured route evidence is available."
    return {
        "recommended_rail": recommended_rail.get("rail"),
        "data_status": recommended_rail.get("data_status"),
        "freshness_level": recommended_rail.get("freshness_level"),
        "freshness_timestamp": evidence_packet.get("expected_fee_usd", {}).get("last_updated_at"),
        "measured_fee_source": evidence_packet.get("expected_fee_usd", {}).get("data_source"),
        "liquidity_score": recommended_rail.get("liquidity_score_v4"),
        "evidence_confidence_label": recommended_rail.get("evidence_confidence_label"),
        "evidence_posture_label": evidence_posture_label,
        "evidence_posture_detail": evidence_posture_detail,
        "forecast_generated_at": forecast_result.get("forecast_freshness", {}).get("generated_at"),
        "query_posture": query_posture,
        "provenance": _build_signal_provenance(
            evidence_type="MEASURED",
            timestamp=evidence_packet.get("expected_fee_usd", {}).get("last_updated_at"),
            source=evidence_packet.get("expected_fee_usd", {}).get("data_source"),
            confidence=recommended_rail.get("evidence_confidence_label") or "Medium",
            status=_signal_status(
                evidence_packet.get("expected_fee_usd", {}).get("last_updated_at"),
                preferred=recommended_rail.get("freshness_level"),
            ),
        ),
    }


def _route_request_for_context(run: dict, *, transfer_amount_usd: float, effective_deadline_at: str) -> tuple[dict, dict, str]:
    time_sensitivity = _time_sensitivity_for_deadline(run, effective_deadline_at)
    route_result = get_route(
        origin=run["origin"],
        destination=run["destination"],
        amount_usdc=transfer_amount_usd,
        time_sensitivity=time_sensitivity,
        monthly_volume_usdc=run["monthly_volume_usdc"],
        current_rail_fee_pct=run["current_rail_fee_pct"],
        current_rail_settlement_hours=run["current_rail_settlement_hours"],
        current_setup=run["current_setup"],
        compliance_sensitivity=run["compliance_sensitivity"],
        lens=run["route_lens"],
        token="USDC",
    )
    forecast_result = run_corridor_forecast(run["corridor_key"]).model_dump()
    return route_result, forecast_result, time_sensitivity


def _build_run_payload(run: dict) -> dict:
    latest_evaluation = get_latest_payroll_evaluation(run["id"])
    decision_context = _build_decision_context(run, latest_evaluation)
    if latest_evaluation:
        route_result = latest_evaluation["route_payload"]
        forecast_result = latest_evaluation["forecast_payload"]
        time_sensitivity = latest_evaluation.get("time_sensitivity") or decision_context["time_sensitivity"]
    else:
        route_result, forecast_result, time_sensitivity = _route_request_for_context(
            run,
            transfer_amount_usd=float(decision_context["transfer_amount_usd"]),
            effective_deadline_at=decision_context["effective_deadline_at"],
        )
    query_posture = _build_query_posture(get_query_metrics_snapshot(), route_result)
    exceptions = [dict(item) for item in run["exceptions"]]
    buffer_posture = _build_buffer_posture(
        run,
        route_result,
        forecast_result,
        exceptions,
        transfer_amount_usd=float(decision_context["transfer_amount_usd"]),
        effective_deadline_at=decision_context["effective_deadline_at"],
        override_buffer_percent=decision_context.get("override_buffer_percent"),
    )
    readiness = _build_readiness(
        run,
        route_result,
        forecast_result,
        exceptions,
        buffer_posture,
        effective_deadline_at=decision_context["effective_deadline_at"],
    )
    latest_decision = get_latest_payroll_decision(run["id"])
    latest_handoff = get_latest_payroll_handoff(run["id"])
    payroll_data_state = _build_payroll_data_state(run["id"])
    policy_checks = _build_policy_checks(
        run,
        route_result,
        forecast_result,
        exceptions,
        effective_deadline_at=decision_context["effective_deadline_at"],
    )
    measured_snapshot = _build_measured_snapshot(route_result, forecast_result, query_posture)
    decision_rule = _build_decision_rule(
        run,
        route_result,
        forecast_result,
        exceptions,
        readiness,
        effective_deadline_at=decision_context["effective_deadline_at"],
    )
    system_status = _build_system_status(query_posture, forecast_result)
    system_state = _build_system_state(route_result, forecast_result, system_status)
    forecast_action_path = _forecast_action_path(
        run,
        forecast_result,
        readiness,
        effective_deadline_at=decision_context["effective_deadline_at"],
    )
    handoff_record = _build_handoff_record(run["id"], latest_decision, latest_handoff)
    journey_progress = _build_journey_progress(run, route_result, latest_decision, handoff_record)
    route_comparison = _build_route_comparison(route_result, time_sensitivity=time_sensitivity)
    capital_impact = _build_capital_impact(
        run,
        transfer_amount_usd=float(decision_context["transfer_amount_usd"]),
        recommended_buffer_percent=float(buffer_posture["recommended_buffer_percent"]),
        override_buffer_percent=decision_context.get("override_buffer_percent"),
        effective_deadline_at=decision_context["effective_deadline_at"],
        safe_buffer_range_min=float(buffer_posture["safe_buffer_range_min"]),
        safe_buffer_range_max=float(buffer_posture["safe_buffer_range_max"]),
    )
    decision_surface = _build_decision_surface(
        run,
        readiness,
        system_status,
        system_state,
        measured_snapshot,
        policy_checks,
        capital_impact,
    )
    blocking_summary = _build_blocking_summary(system_status, decision_surface, policy_checks)
    decision_flip_conditions = _build_decision_flip_conditions(decision_surface, policy_checks)
    alternative_paths = _build_alternative_paths(
        decision_surface,
        policy_checks,
        route_comparison,
        buffer_posture,
        capital_impact,
    )
    evaluation_log_summary = {
        "last_evaluation_at": decision_context.get("last_evaluation_at"),
        "inputs": {
            "transfer_amount_usd": float(decision_context["transfer_amount_usd"]),
            "required_arrival_at": decision_context.get("required_arrival_at"),
            "effective_deadline_at": decision_context.get("effective_deadline_at"),
            "payroll_currency": decision_context.get("payroll_currency"),
            "override_buffer_percent": decision_context.get("override_buffer_percent"),
            "time_sensitivity": time_sensitivity,
        },
        "outputs": {
            "buffer_range_min": buffer_posture.get("safe_buffer_range_min"),
            "buffer_range_max": buffer_posture.get("safe_buffer_range_max"),
            "recommended_buffer_percent": buffer_posture.get("recommended_buffer_percent"),
            "selected_rail": route_result.get("recommended_rail"),
            "readiness_state": readiness.get("readiness_state"),
        },
    }
    decision_log = []
    for event in list_payroll_data_events(payroll_run_id=run["id"], limit=5):
        decision_log.append(
            {
                "entry_type": "payroll_data_event",
                "decision_timestamp": event.get("created_at"),
                "event_name": event.get("event_name"),
                "record_count": event.get("record_count", 0),
                "beneficiary_change_count": event.get("beneficiary_change_count", 0),
                "source_type": event.get("source_type"),
                "source_type_label": PAYROLL_SOURCE_LABELS.get(event.get("source_type"), "Payroll dataset"),
                "file_name": (event.get("metadata") or {}).get("file_name"),
            }
        )
    if latest_decision:
        decision_log.append(
            {
                "entry_type": "decision",
                "decision_timestamp": latest_decision.get("updated_at") or latest_decision.get("created_at"),
                "decision_action": latest_decision.get("action"),
                "decision_reason": latest_decision.get("decision_reason") or "Unspecified",
                "decision_reason_other": latest_decision.get("decision_reason_other") or "",
                "decision_owner": latest_decision.get("approver"),
                "decision_rule": latest_decision.get("decision_rule") or decision_rule["logic"],
            }
        )
    decision_log.sort(key=lambda item: item.get("decision_timestamp") or "", reverse=True)

    return {
        "id": run["id"],
        "client_name": run["client_name"],
        "corridor": run["corridor_label"],
        "corridor_key": run["corridor_key"],
        "corridor_slug": run["corridor_slug"],
        "payroll_date": run["payroll_date"],
        "currency": decision_context["payroll_currency"],
        "expected_payroll_amount": run["expected_payroll_amount"],
        "prefunding_amount": run["prefunding_amount"],
        "prefunding_received_at": run["prefunding_received_at"],
        "cutoff_at": run["cutoff_at"],
        "last_evaluation_at": decision_context.get("last_evaluation_at"),
        "readiness_state": readiness["readiness_state"],
        "readiness_label": readiness["readiness_label"],
        "risk_level": readiness["risk_level"],
        "risk_label": readiness["risk_label"],
        "recommended_action": readiness["recommended_action"],
        "recommended_action_label": readiness["recommended_action_label"],
        "top_line_reason": run["top_line_reason"],
        "recommended_next_action": run["recommended_next_action"],
        "top_drivers": readiness["top_drivers"],
        "blockers": readiness["blockers"],
        "policy_checks": policy_checks,
        "route_recommendation": {
            "recommended_rail": route_result.get("recommended_rail"),
            "alternative_rail": route_result.get("alternative_rail"),
            "expected_landed_amount_label": route_result.get("expected_landed_amount_label"),
            "confidence_label": route_result.get("evidence_confidence_label"),
            "strategy_score_label": route_result.get("strategy_score_label"),
            "reason": route_result.get("why_this_route", ["No route rationale available."])[0],
            "evidence_type": "MODELED",
            "freshness_timestamp": route_result.get("timestamp"),
            "provenance": _build_signal_provenance(
                evidence_type="MODELED",
                timestamp=route_result.get("timestamp"),
                source="Canopy readiness decision engine",
                confidence=route_result.get("evidence_confidence_label") or "Medium",
                status="Current",
            ),
            "route_payload": route_result,
        },
        "decision_context": decision_context,
        "payroll_data_state": payroll_data_state,
        "route_comparison": route_comparison,
        "capital_impact": capital_impact,
        "decision_surface": decision_surface,
        "blocking_summary": blocking_summary,
        "decision_flip_conditions": decision_flip_conditions,
        "alternative_paths": alternative_paths,
        "evaluation_log_summary": evaluation_log_summary,
        "buffer_recommendation": {
            **buffer_posture,
            "evidence_type": "CALCULATED",
            "provenance": _build_signal_provenance(
                evidence_type="CALCULATED",
                timestamp=buffer_posture.get("freshness_timestamp"),
                source="Deterministic payroll policy engine",
                confidence=_confidence_label(float(buffer_posture.get("confidence_score", 0.0) or 0.0)),
                status="Current",
            ),
        },
        "forecast_advisory": {
            **forecast_result,
            "evidence_type": "FORECASTED",
            "detail": "Advisory only. Forecast signals do not override the readiness decision.",
            "provenance": forecast_action_path["provenance"],
        },
        "forecast_action_path": forecast_action_path,
        "measured_route_snapshot": measured_snapshot,
        "query_posture": query_posture,
        "system_status": system_status,
        "system_state": system_state,
        "decision_rule": decision_rule,
        "evidence_ladder": _build_evidence_ladder(
            run,
            route_result,
            forecast_result,
            readiness,
            query_posture,
            payroll_data_state=payroll_data_state,
            effective_deadline_at=decision_context["effective_deadline_at"],
        ),
        "exceptions": exceptions,
        "changes_since_last_run": run["changes_since_last_run"],
        "operator_notes": latest_decision.get("decision_reason_other") if latest_decision else "",
        "latest_decision": latest_decision,
        "latest_evaluation": latest_evaluation and {
            "id": latest_evaluation.get("id"),
            "evaluation_timestamp": latest_evaluation.get("updated_at"),
            "transfer_amount_usd": latest_evaluation.get("transfer_amount_usd"),
            "required_arrival_at": latest_evaluation.get("required_arrival_at"),
            "effective_deadline_at": latest_evaluation.get("effective_deadline_at"),
            "payroll_currency": latest_evaluation.get("payroll_currency"),
            "override_buffer_percent": latest_evaluation.get("override_buffer_percent"),
            "time_sensitivity": latest_evaluation.get("time_sensitivity"),
            "selected_rail": latest_evaluation.get("selected_rail"),
            "readiness_state": latest_evaluation.get("readiness_state"),
            "buffer_range_min": latest_evaluation.get("buffer_range_min"),
            "buffer_range_max": latest_evaluation.get("buffer_range_max"),
            "recommended_buffer_percent": latest_evaluation.get("recommended_buffer_percent"),
        },
        "latest_handoff": latest_handoff,
        "handoff_record": handoff_record,
        "journey_progress": journey_progress,
        "decision_log": decision_log,
        "decision_reason_options": DECISION_REASON_OPTIONS,
        "approval_boundary_note": "Canopy records the decision. Execution stays outside the product.",
        "powered_by": {
            "headline": "How this is powered",
            "items": [
                "BigQuery-derived measured inputs refresh in the background.",
                "Freshness and query-budget posture stay visible on the run.",
                "Request handlers serve cached/materialized state only.",
                "Canopy decides; the external execution layer still moves the money.",
            ],
        },
    }


def _summary_for_run(run_id: str) -> dict:
    payload = _build_run_payload(_get_run(run_id))
    return {
        "id": payload["id"],
        "client_name": payload["client_name"],
        "corridor": payload["corridor"],
        "corridor_key": payload["corridor_key"],
        "corridor_slug": payload["corridor_slug"],
        "payroll_date": payload["payroll_date"],
        "readiness_state": payload["readiness_state"],
        "readiness_label": payload["readiness_label"],
        "risk_level": payload["risk_level"],
        "risk_label": payload["risk_label"],
        "recommended_action": payload["recommended_action"],
        "recommended_action_label": payload["recommended_action_label"],
        "recommended_rail": payload["route_recommendation"]["recommended_rail"],
        "top_line_reason": payload["top_line_reason"],
        "last_updated_at": payload["route_recommendation"]["freshness_timestamp"],
    }


def list_payroll_runs() -> List[dict]:
    items = [_summary_for_run(run_id) for run_id in DEMO_RUNS]
    return sorted(
        items,
        key=lambda item: (
            -READINESS_ORDER.get(item["readiness_state"], 0),
            -RISK_ORDER.get(item["risk_level"], 0),
            item["payroll_date"],
        ),
    )


def get_overview() -> dict:
    runs = list_payroll_runs()
    top_run = runs[0]
    return {
        "top_line_run": top_run,
        "top_line_answer": f"{top_run['corridor']} payroll - {top_run['readiness_label']}",
        "top_line_reason": top_run["top_line_reason"],
        "action_queue": [
            {
                "id": item["id"],
                "title": f"{item['corridor']} payroll",
                "readiness_label": item["readiness_label"],
                "risk_label": item["risk_label"],
                "recommended_action_label": item["recommended_action_label"],
            }
            for item in runs
        ],
        "corridor_summary": [
            {
                "id": item["id"],
                "corridor": item["corridor"],
                "payroll_date": item["payroll_date"],
                "readiness_label": item["readiness_label"],
                "risk_label": item["risk_label"],
                "recommended_rail": item["recommended_rail"],
                "last_updated_at": item["last_updated_at"],
            }
            for item in runs
        ],
        "evidence_explainer": [
            {"label": "Measured", "detail": "BigQuery-derived route and freshness evidence, served from cached summaries."},
            {"label": "Calculated", "detail": "Deterministic payroll math such as cutoff timing, landed amount, and buffer posture."},
            {"label": "Modeled", "detail": "Readiness state, route recommendation, and next action."},
            {"label": "Forecasted", "detail": "Advisory corridor-risk signals that never override approval logic."},
        ],
    }


def get_payroll_run_detail(run_id: str) -> dict:
    return _build_run_payload(_get_run(run_id))


def evaluate_payroll_run(
    run_id: str,
    *,
    transfer_amount_usd: float,
    payroll_currency: str,
    required_arrival_at: str | None = None,
    override_buffer_percent: float | None = None,
) -> dict:
    run = _get_run(run_id)
    effective_deadline_at = required_arrival_at or run["cutoff_at"]
    normalized_override = _normalize_override_percent(override_buffer_percent)
    route_result, forecast_result, time_sensitivity = _route_request_for_context(
        run,
        transfer_amount_usd=float(transfer_amount_usd),
        effective_deadline_at=effective_deadline_at,
    )
    exceptions = [dict(item) for item in run["exceptions"]]
    buffer_posture = _build_buffer_posture(
        run,
        route_result,
        forecast_result,
        exceptions,
        transfer_amount_usd=float(transfer_amount_usd),
        effective_deadline_at=effective_deadline_at,
        override_buffer_percent=normalized_override,
    )
    readiness = _build_readiness(
        run,
        route_result,
        forecast_result,
        exceptions,
        buffer_posture,
        effective_deadline_at=effective_deadline_at,
    )
    route_comparison = _build_route_comparison(route_result, time_sensitivity=time_sensitivity)
    capital_impact = _build_capital_impact(
        run,
        transfer_amount_usd=float(transfer_amount_usd),
        recommended_buffer_percent=float(buffer_posture["recommended_buffer_percent"]),
        override_buffer_percent=normalized_override,
        effective_deadline_at=effective_deadline_at,
        safe_buffer_range_min=float(buffer_posture["safe_buffer_range_min"]),
        safe_buffer_range_max=float(buffer_posture["safe_buffer_range_max"]),
    )
    evaluation_summary = {
        "inputs": {
            "transfer_amount_usd": round(float(transfer_amount_usd), 2),
            "required_arrival_at": required_arrival_at,
            "effective_deadline_at": effective_deadline_at,
            "payroll_currency": payroll_currency,
            "override_buffer_percent": normalized_override,
            "time_sensitivity": time_sensitivity,
        },
        "outputs": {
            "buffer_range_min": buffer_posture["safe_buffer_range_min"],
            "buffer_range_max": buffer_posture["safe_buffer_range_max"],
            "recommended_buffer_percent": buffer_posture["recommended_buffer_percent"],
            "selected_rail": route_result.get("recommended_rail"),
            "readiness_state": readiness["readiness_state"],
            "route_comparison": route_comparison,
            "capital_impact": capital_impact,
        },
    }
    evaluation = record_payroll_evaluation(
        payroll_run_id=run_id,
        transfer_amount_usd=float(transfer_amount_usd),
        required_arrival_at=required_arrival_at,
        effective_deadline_at=effective_deadline_at,
        payroll_currency=payroll_currency,
        override_buffer_percent=normalized_override,
        time_sensitivity=time_sensitivity,
        selected_rail=route_result.get("recommended_rail") or "",
        readiness_state=readiness["readiness_state"],
        buffer_range_min=float(buffer_posture["safe_buffer_range_min"]),
        buffer_range_max=float(buffer_posture["safe_buffer_range_max"]),
        recommended_buffer_percent=float(buffer_posture["recommended_buffer_percent"]),
        route_payload=route_result,
        forecast_payload=forecast_result,
        evaluation_summary=evaluation_summary,
    )
    return {
        "status": "evaluated",
        "evaluation_timestamp": evaluation.get("updated_at"),
        "evaluation": evaluation,
        "run": get_payroll_run_detail(run_id),
    }


def ingest_payroll_file(
    run_id: str,
    *,
    source_type: str,
    file_name: str,
    content_base64: str,
) -> dict:
    _get_run(run_id)
    if source_type not in PAYROLL_SOURCE_LABELS:
        raise ValueError("Unsupported payroll data source type")
    records, snapshot_format = _parse_payroll_file(file_name=file_name, content_base64=content_base64)
    validation_errors = _validate_payroll_records(records)
    previous_verified = get_last_verified_payroll_data_snapshot(run_id)
    previous_records = previous_verified.get("snapshot", []) if previous_verified else []
    beneficiary_change_count = _count_beneficiary_changes(records, previous_records) if not validation_errors else 0
    verification_status = "failed" if validation_errors else ("review_required" if beneficiary_change_count > 0 else "verified")
    data_status = "missing_data" if validation_errors else ("pending_review" if beneficiary_change_count > 0 else "ready")
    loaded_at = datetime.now(timezone.utc).isoformat()
    snapshot = record_payroll_data_snapshot(
        payroll_run_id=run_id,
        source_type=source_type,
        source_label=PAYROLL_SOURCE_LABELS[source_type],
        snapshot_format=snapshot_format,
        file_name=file_name,
        last_loaded_timestamp=loaded_at,
        record_count=len(records),
        beneficiary_change_count=beneficiary_change_count,
        verification_status=verification_status,
        data_status=data_status,
        lineage_label="Payroll dataset snapshot -> BigQuery -> Decision Engine",
        snapshot=records,
        validation_errors=validation_errors,
    )
    event = record_payroll_data_event(
        payroll_run_id=run_id,
        event_name="Payroll file loaded",
        source_type=source_type,
        record_count=len(records),
        beneficiary_change_count=beneficiary_change_count,
        metadata={
            "file_name": file_name,
            "snapshot_format": snapshot_format,
            "source_label": PAYROLL_SOURCE_LABELS[source_type],
            "validation_errors": validation_errors,
        },
    )
    return {
        "status": "payroll_data_received",
        "snapshot": snapshot,
        "event": event,
        "run": get_payroll_run_detail(run_id),
    }


def list_exceptions(*, run_id: str | None = None) -> List[dict]:
    if run_id:
        return get_payroll_run_detail(run_id)["exceptions"]
    items: List[dict] = []
    for item in list_payroll_runs():
        for exception in get_payroll_run_detail(item["id"])["exceptions"]:
            items.append({**exception, "payroll_run_id": item["id"], "corridor": item["corridor"]})
    return sorted(
        items,
        key=lambda item: (
            -RISK_ORDER.get(item["severity"], 0),
            item["sla_due_at"],
        ),
    )


def record_run_decision(
    run_id: str,
    *,
    action: str,
    approver: str = "",
    notes: str = "",
    decision_reason: str = "",
    decision_reason_other: str = "",
) -> dict:
    detail = get_payroll_run_detail(run_id)
    receipt_id = f"receipt_{uuid4().hex[:10]}"
    normalized_reason = decision_reason or notes or "Manual override"
    normalized_other = decision_reason_other if normalized_reason == "Other" else (notes if normalized_reason not in DECISION_REASON_OPTIONS else "")
    decision = record_payroll_decision(
        payroll_run_id=run_id,
        action=action,
        approver=approver,
        notes=normalized_other,
        decision_reason=normalized_reason,
        decision_reason_other=normalized_other,
        decision_rule=detail["decision_rule"]["logic"],
        readiness_state=detail["readiness_state"],
        risk_level=detail["risk_level"],
        receipt_id=receipt_id,
    )
    if action == "APPROVE" and get_latest_payroll_handoff(run_id) is None:
        record_payroll_handoff(
            payroll_run_id=run_id,
            decision_id=decision["id"],
            status="Queued",
        )
    return {
        "status": "recorded",
        "decision": decision,
        "run": get_payroll_run_detail(run_id),
    }


def trigger_run_handoff(run_id: str) -> dict:
    detail = get_payroll_run_detail(run_id)
    latest_decision = detail.get("latest_decision")
    if not latest_decision or latest_decision.get("action") != "APPROVE":
        raise ValueError("Run must be approved before handoff can be triggered")
    latest_handoff = get_latest_payroll_handoff(run_id)
    if latest_handoff is None:
        latest_handoff = record_payroll_handoff(
            payroll_run_id=run_id,
            decision_id=latest_decision.get("id"),
            status="Queued",
        )
    handoff = transition_payroll_handoff(payroll_run_id=run_id, status="Acknowledged")
    return {
        "status": "handoff_recorded",
        "handoff": handoff,
        "run": get_payroll_run_detail(run_id),
    }


def build_receipt_context(run_id: str) -> dict:
    detail = get_payroll_run_detail(run_id)
    latest_decision = detail.get("latest_decision")
    evaluation_timestamp = detail.get("last_evaluation_at") or datetime.now(timezone.utc).isoformat()
    return {
        "payroll_run_id": detail["id"],
        "client_name": detail["client_name"],
        "corridor": detail["corridor"],
        "payroll_date": detail["payroll_date"],
        "readiness_state": detail["readiness_label"],
        "risk_level": detail["risk_label"],
        "recommended_action": detail["recommended_action_label"],
        "last_evaluation_at": evaluation_timestamp,
        "decision_context": detail.get("decision_context"),
        "latest_evaluation": detail.get("latest_evaluation"),
        "evaluation_log_summary": detail.get("evaluation_log_summary"),
        "route_comparison": detail.get("route_comparison"),
        "decision_surface": detail.get("decision_surface"),
        "blocking_summary": detail.get("blocking_summary"),
        "decision_flip_conditions": detail.get("decision_flip_conditions"),
        "alternative_paths": detail.get("alternative_paths"),
        "capital_impact": detail.get("capital_impact"),
        "decision_confidence": detail.get("route_recommendation", {}).get("confidence_label"),
        "payroll_data_state": detail.get("payroll_data_state"),
        "top_blockers": [item["label"] for item in detail["blockers"]],
        "blocker_owners": [item["owner"] for item in detail["exceptions"] if item["status"] == "OPEN"],
        "policy_checks": detail["policy_checks"],
        "decision_rule": detail["decision_rule"],
        "decision_log": detail["decision_log"],
        "buffer_recommendation": detail["buffer_recommendation"],
        "forecast_advisory": detail["forecast_advisory"],
        "forecast_action_path": detail["forecast_action_path"],
        "system_status": detail["system_status"],
        "system_state": detail["system_state"],
        "query_posture": detail["query_posture"],
        "evidence_ladder": detail["evidence_ladder"],
        "measured_snapshot": detail["measured_route_snapshot"],
        "recommended_next_action": detail.get("recommended_next_action"),
        "top_line_reason": detail.get("top_line_reason"),
        "operator_action": latest_decision["action"] if latest_decision else None,
        "operator_approver": latest_decision["approver"] if latest_decision else None,
        "operator_reason": latest_decision["decision_reason"] if latest_decision else None,
        "operator_reason_other": latest_decision["decision_reason_other"] if latest_decision else None,
        "handoff_record": detail["handoff_record"],
        "journey_progress": detail["journey_progress"],
        "approval_boundary_note": detail["approval_boundary_note"],
        "data_lineage": [
            "BigQuery",
            "Background refresh",
            "Materialized summary",
            "Decision engine",
            "Decision receipt",
        ],
    }
