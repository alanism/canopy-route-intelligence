"""Phase 17 Solana corridor intelligence product-layer helpers."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.solana.shadow_validation import SHADOW_QUERY_NAMES

DEFAULT_ARTIFACT_PATH = Path(__file__).resolve().parents[2] / "data" / "solana_corridor_intelligence.json"
SCOPE_DISCLAIMER = (
    "Solana corridor intelligence reflects observed SPL token movements within "
    "configured watched sources and measured windows."
)
REQUEST_PATH_NOTE = "Request handlers serve materialized Solana intelligence only; they do not run BigQuery."


class CorridorIntelligenceError(ValueError):
    """Raised when a shadow report cannot become a product-layer artifact."""


def artifact_path(explicit_path: str | os.PathLike[str] | None = None) -> Path:
    configured = explicit_path or os.getenv("SOLANA_CORRIDOR_INTELLIGENCE_PATH")
    return Path(configured) if configured else DEFAULT_ARTIFACT_PATH


def empty_corridor_intelligence() -> dict[str, Any]:
    return {
        "chain": "Solana",
        "status": "unavailable",
        "signal_state": "no_materialized_artifact",
        "claim_level": "unavailable",
        "scope": "observed_solana_watched_sources",
        "scope_disclaimer": SCOPE_DISCLAIMER,
        "request_path_note": REQUEST_PATH_NOTE,
        "signals": {},
        "quality_gates": {
            "materialized_artifact_present": False,
            "missing_fields_clear": False,
            "cold_start_clear": False,
            "request_path_bigquery_free": True,
        },
        "open_risks": ["No materialized Solana corridor intelligence artifact is available."],
    }


def load_corridor_intelligence(
    *,
    path: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
    resolved = artifact_path(path)
    if not resolved.exists():
        return empty_corridor_intelligence()
    with resolved.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    payload.setdefault("request_path_note", REQUEST_PATH_NOTE)
    payload.setdefault("scope_disclaimer", SCOPE_DISCLAIMER)
    return payload


def write_corridor_intelligence(
    payload: dict[str, Any],
    *,
    path: str | os.PathLike[str] | None = None,
) -> Path:
    resolved = artifact_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    temp_path = resolved.with_suffix(resolved.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)
        fh.write("\n")
    os.replace(temp_path, resolved)
    return resolved


def build_corridor_intelligence_from_shadow_report(
    report: dict[str, Any],
    *,
    corridor_id: str = "SOLANA-WATCHED",
    corridor_label: str = "Solana watched-source corridor",
    token: str = "USDC",
    generated_at: str | None = None,
) -> dict[str, Any]:
    _validate_shadow_report(report)
    header = report["header"]
    queries = {query["name"]: query for query in report["queries"]}
    missing_fields = list(header.get("missing_fields") or [])
    cold_start = _as_bool(header.get("seven_day_unavailable_due_to_cold_start"))

    success = _first_row(queries["shadow_success_purity"])
    mev = _first_row(queries["shadow_mev_protection_rate"])
    velocity = _first_row(queries["shadow_settlement_velocity"])
    fee = _first_row(queries["shadow_fee_efficiency"])
    missing_report = queries["shadow_missing_field_report"].get("rows", [])

    total_rows = _as_int(success.get("total_rows"))
    signal_state = _signal_state(
        missing_fields=missing_fields,
        cold_start=cold_start,
        total_rows=total_rows,
    )
    quality_gates = {
        "materialized_artifact_present": True,
        "missing_fields_clear": not missing_fields,
        "cold_start_clear": not cold_start,
        "request_path_bigquery_free": True,
        "slot_bounded": header.get("slot_min") is not None and header.get("slot_max") is not None,
    }

    return {
        "chain": "Solana",
        "corridor_id": corridor_id,
        "corridor_label": corridor_label,
        "token": token,
        "status": "ready" if signal_state == "ready" else "degraded",
        "signal_state": signal_state,
        "claim_level": "production_candidate" if signal_state == "ready" else "evidence_limited",
        "scope": "observed_solana_watched_sources",
        "scope_disclaimer": SCOPE_DISCLAIMER,
        "request_path_note": REQUEST_PATH_NOTE,
        "source_table": header.get("target_table"),
        "slot_min": _as_int(header.get("slot_min")),
        "slot_max": _as_int(header.get("slot_max")),
        "observed_window_seconds": _as_int(header.get("cold_start_window_seconds")),
        "generated_at": generated_at or datetime.now(timezone.utc).isoformat(),
        "signals": {
            "success_purity": {
                "value": _as_float(success.get("success_purity_rate")),
                "qualifying_rows": _as_int(success.get("qualifying_rows")),
                "success_rows": _as_int(success.get("success_rows")),
                "total_rows": total_rows,
                "seven_day_unavailable_due_to_cold_start": cold_start,
            },
            "mev_protection_rate": {
                "value": _as_float(mev.get("mev_protection_proxy_rate")),
                "protected_proxy_rows": _as_int(mev.get("protected_proxy_rows")),
                "jito_tip_positive_rows": _as_int(mev.get("jito_tip_positive_rows")),
                "note": "Proxy signal based on observed tips and settlement evidence, not product truth.",
            },
            "settlement_velocity": {
                "latency_rows": _as_int(velocity.get("latency_rows")),
                "avg_latency_seconds": _as_float(velocity.get("avg_latency_seconds")),
                "p50_latency_seconds": _as_float(velocity.get("p50_latency_seconds")),
                "p95_latency_seconds": _as_float(velocity.get("p95_latency_seconds")),
            },
            "fee_efficiency": {
                "qualified_rows": _as_int(fee.get("qualified_rows")),
                "avg_transfer_per_lamport": _as_float(fee.get("avg_transfer_per_lamport")),
                "avg_total_native_observed_cost_lamports": _as_float(
                    fee.get("avg_total_native_observed_cost_lamports")
                ),
            },
        },
        "quality_gates": quality_gates,
        "missing_fields": missing_fields,
        "null_rate_findings": _null_rate_findings(missing_report),
        "open_risks": _open_risks(
            missing_fields=missing_fields,
            cold_start=cold_start,
            total_rows=total_rows,
        ),
    }


def _validate_shadow_report(report: dict[str, Any]) -> None:
    query_names = tuple(report.get("query_names") or [])
    if query_names != SHADOW_QUERY_NAMES:
        raise CorridorIntelligenceError(f"Unexpected shadow query names: {query_names!r}")
    queries = {query.get("name") for query in report.get("queries", [])}
    missing = sorted(set(SHADOW_QUERY_NAMES) - queries)
    if missing:
        raise CorridorIntelligenceError(f"Missing shadow query outputs: {', '.join(missing)}")
    if "header" not in report:
        raise CorridorIntelligenceError("Shadow report missing header.")


def _signal_state(*, missing_fields: list[str], cold_start: bool, total_rows: int) -> str:
    if missing_fields:
        return "schema_gap"
    if cold_start:
        return "cold_start"
    if total_rows <= 0:
        return "no_observations"
    return "ready"


def _open_risks(*, missing_fields: list[str], cold_start: bool, total_rows: int) -> list[str]:
    risks: list[str] = []
    if missing_fields:
        risks.append("Required S3 fields are missing from the Solana measured table.")
    if cold_start:
        risks.append("Seven-day success purity is unavailable because the observed window is too short.")
    if total_rows <= 0:
        risks.append("No qualifying Solana observations are available.")
    return risks


def _first_row(query: dict[str, Any]) -> dict[str, Any]:
    rows = query.get("rows") or []
    return rows[0] if rows else {}


def _null_rate_findings(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    findings = []
    for row in rows:
        null_rate = _as_float(row.get("null_rate"))
        if null_rate > 0:
            findings.append(
                {
                    "field_name": row.get("field_name"),
                    "null_count": _as_int(row.get("null_count")),
                    "null_rate": null_rate,
                }
            )
    return findings


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() == "true"
    return bool(value)


def _as_int(value: Any) -> int:
    if value is None or value == "":
        return 0
    return int(value)


def _as_float(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    return float(value)
