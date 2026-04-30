"""Lightweight persistence for Canopy demo feedback, discovery events, and V5 scenarios."""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "sample_dataset.sqlite3"


def _ensure_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS demo_feedback (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                corridor_key TEXT,
                corridor_label TEXT,
                recommended_rail TEXT,
                scenario_json TEXT NOT NULL,
                route_json TEXT NOT NULL,
                feedback_decision TEXT,
                feedback_reviewers TEXT,
                feedback_notes TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS v5_scenarios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                corridor_key TEXT,
                corridor_label TEXT,
                token TEXT NOT NULL,
                recommended_rail TEXT,
                scenario_json TEXT NOT NULL,
                route_json TEXT NOT NULL,
                review_state TEXT NOT NULL,
                reviewer TEXT,
                review_notes TEXT,
                follow_up_requested INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS discovery_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                event_name TEXT NOT NULL,
                corridor_key TEXT,
                corridor_label TEXT,
                token TEXT,
                lens TEXT,
                metadata_json TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS payroll_decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                payroll_run_id TEXT NOT NULL,
                action TEXT NOT NULL,
                approver TEXT,
                notes TEXT,
                readiness_state TEXT,
                risk_level TEXT,
                receipt_id TEXT,
                decision_reason TEXT,
                decision_reason_other TEXT,
                decision_rule TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS payroll_handoffs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                payroll_run_id TEXT NOT NULL,
                decision_id INTEGER,
                execution_system TEXT,
                status TEXT NOT NULL,
                queued_at TEXT,
                sent_at TEXT,
                acknowledged_at TEXT,
                triggered_by TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS payroll_evaluations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                payroll_run_id TEXT NOT NULL,
                transfer_amount_usd REAL NOT NULL,
                required_arrival_at TEXT,
                effective_deadline_at TEXT NOT NULL,
                payroll_currency TEXT NOT NULL,
                override_buffer_percent REAL,
                time_sensitivity TEXT NOT NULL,
                selected_rail TEXT,
                readiness_state TEXT,
                buffer_range_min REAL,
                buffer_range_max REAL,
                recommended_buffer_percent REAL,
                route_payload_json TEXT NOT NULL,
                forecast_payload_json TEXT NOT NULL,
                evaluation_summary_json TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS payroll_data_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                payroll_run_id TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_label TEXT NOT NULL,
                snapshot_format TEXT NOT NULL,
                file_name TEXT,
                last_loaded_timestamp TEXT,
                record_count INTEGER NOT NULL DEFAULT 0,
                beneficiary_change_count INTEGER NOT NULL DEFAULT 0,
                verification_status TEXT NOT NULL,
                data_status TEXT NOT NULL,
                lineage_label TEXT NOT NULL,
                snapshot_json TEXT NOT NULL,
                validation_errors_json TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS payroll_data_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                payroll_run_id TEXT NOT NULL,
                event_name TEXT NOT NULL,
                source_type TEXT,
                record_count INTEGER NOT NULL DEFAULT 0,
                beneficiary_change_count INTEGER NOT NULL DEFAULT 0,
                metadata_json TEXT NOT NULL
            )
            """
        )
        existing_columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(payroll_decisions)").fetchall()
        }
        if "decision_reason" not in existing_columns:
            conn.execute("ALTER TABLE payroll_decisions ADD COLUMN decision_reason TEXT")
        if "decision_reason_other" not in existing_columns:
            conn.execute("ALTER TABLE payroll_decisions ADD COLUMN decision_reason_other TEXT")
        if "decision_rule" not in existing_columns:
            conn.execute("ALTER TABLE payroll_decisions ADD COLUMN decision_rule TEXT")
        conn.commit()


def save_feedback(
    *,
    corridor_key: str,
    corridor_label: str,
    recommended_rail: str,
    scenario_payload: Dict[str, Any],
    route_payload: Dict[str, Any],
    feedback_decision: str,
    feedback_reviewers: str,
    feedback_notes: str,
) -> Dict[str, Any]:
    _ensure_db()
    created_at = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute(
            """
            INSERT INTO demo_feedback (
                created_at,
                corridor_key,
                corridor_label,
                recommended_rail,
                scenario_json,
                route_json,
                feedback_decision,
                feedback_reviewers,
                feedback_notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                created_at,
                corridor_key,
                corridor_label,
                recommended_rail,
                json.dumps(scenario_payload),
                json.dumps(route_payload),
                feedback_decision,
                feedback_reviewers,
                feedback_notes,
            ),
        )
        conn.commit()
        feedback_id = cursor.lastrowid

    return {
        "id": feedback_id,
        "created_at": created_at,
        "db_path": str(DB_PATH),
    }


def create_scenario(
    *,
    corridor_key: str,
    corridor_label: str,
    token: str,
    recommended_rail: str,
    scenario_payload: Dict[str, Any],
    route_payload: Dict[str, Any],
    review_state: str = "proposed",
    reviewer: str = "",
    review_notes: str = "",
    follow_up_requested: bool = False,
) -> Dict[str, Any]:
    _ensure_db()
    created_at = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute(
            """
            INSERT INTO v5_scenarios (
                created_at,
                updated_at,
                corridor_key,
                corridor_label,
                token,
                recommended_rail,
                scenario_json,
                route_json,
                review_state,
                reviewer,
                review_notes,
                follow_up_requested
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                created_at,
                created_at,
                corridor_key,
                corridor_label,
                token,
                recommended_rail,
                json.dumps(scenario_payload),
                json.dumps(route_payload),
                review_state,
                reviewer,
                review_notes,
                1 if follow_up_requested else 0,
            ),
        )
        conn.commit()
        scenario_id = cursor.lastrowid
    return {
        "id": scenario_id,
        "created_at": created_at,
        "updated_at": created_at,
        "db_path": str(DB_PATH),
    }


def get_scenario(scenario_id: int) -> Optional[Dict[str, Any]]:
    _ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT
                id,
                created_at,
                updated_at,
                corridor_key,
                corridor_label,
                token,
                recommended_rail,
                scenario_json,
                route_json,
                review_state,
                reviewer,
                review_notes,
                follow_up_requested
            FROM v5_scenarios
            WHERE id = ?
            """,
            (scenario_id,),
        ).fetchone()
    if row is None:
        return None
    return {
        "id": row["id"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "corridor_key": row["corridor_key"],
        "corridor_label": row["corridor_label"],
        "token": row["token"],
        "recommended_rail": row["recommended_rail"],
        "scenario": json.loads(row["scenario_json"]),
        "route": json.loads(row["route_json"]),
        "review_state": row["review_state"],
        "reviewer": row["reviewer"],
        "review_notes": row["review_notes"],
        "follow_up_requested": bool(row["follow_up_requested"]),
    }


def list_scenarios(*, limit: int = 10, corridor_key: Optional[str] = None) -> list[Dict[str, Any]]:
    _ensure_db()
    safe_limit = max(1, min(int(limit), 50))
    query = """
        SELECT
            id,
            created_at,
            updated_at,
            corridor_key,
            corridor_label,
            token,
            recommended_rail,
            scenario_json,
            route_json,
            review_state,
            reviewer,
            review_notes,
            follow_up_requested
        FROM v5_scenarios
    """
    params: list[Any] = []
    if corridor_key:
        query += " WHERE corridor_key = ?"
        params.append(corridor_key)
    query += " ORDER BY updated_at DESC LIMIT ?"
    params.append(safe_limit)

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query, params).fetchall()

    return [
        {
            "id": row["id"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "corridor_key": row["corridor_key"],
            "corridor_label": row["corridor_label"],
            "token": row["token"],
            "recommended_rail": row["recommended_rail"],
            "scenario": json.loads(row["scenario_json"]),
            "route": json.loads(row["route_json"]),
            "review_state": row["review_state"],
            "reviewer": row["reviewer"],
            "review_notes": row["review_notes"],
            "follow_up_requested": bool(row["follow_up_requested"]),
        }
        for row in rows
    ]


def review_scenario(
    scenario_id: int,
    *,
    review_state: str,
    reviewer: str = "",
    review_notes: str = "",
    follow_up_requested: bool = False,
) -> Optional[Dict[str, Any]]:
    _ensure_db()
    updated_at = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute(
            """
            UPDATE v5_scenarios
            SET
                updated_at = ?,
                review_state = ?,
                reviewer = ?,
                review_notes = ?,
                follow_up_requested = ?
            WHERE id = ?
            """,
            (
                updated_at,
                review_state,
                reviewer,
                review_notes,
                1 if follow_up_requested else 0,
                scenario_id,
            ),
        )
        conn.commit()
        if cursor.rowcount == 0:
            return None
    return get_scenario(scenario_id)


def save_discovery_event(
    *,
    event_name: str,
    corridor_key: str = "",
    corridor_label: str = "",
    token: str = "",
    lens: str = "",
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    _ensure_db()
    created_at = datetime.now(timezone.utc).isoformat()
    payload = metadata or {}
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute(
            """
            INSERT INTO discovery_events (
                created_at,
                event_name,
                corridor_key,
                corridor_label,
                token,
                lens,
                metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                created_at,
                event_name,
                corridor_key,
                corridor_label,
                token,
                lens,
                json.dumps(payload),
            ),
        )
        conn.commit()
        event_id = cursor.lastrowid

    return {
        "id": event_id,
        "created_at": created_at,
    }


def list_discovery_events(*, limit: int = 20, corridor_key: Optional[str] = None) -> list[Dict[str, Any]]:
    _ensure_db()
    safe_limit = max(1, min(int(limit), 100))
    query = """
        SELECT
            id,
            created_at,
            event_name,
            corridor_key,
            corridor_label,
            token,
            lens,
            metadata_json
        FROM discovery_events
    """
    params: list[Any] = []
    if corridor_key:
        query += " WHERE corridor_key = ?"
        params.append(corridor_key)
    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(safe_limit)

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query, params).fetchall()

    return [
        {
            "id": row["id"],
            "created_at": row["created_at"],
            "event_name": row["event_name"],
            "corridor_key": row["corridor_key"],
            "corridor_label": row["corridor_label"],
            "token": row["token"],
            "lens": row["lens"],
            "metadata": json.loads(row["metadata_json"]),
        }
        for row in rows
    ]


def get_discovery_summary(*, corridor_key: Optional[str] = None) -> Dict[str, Any]:
    _ensure_db()

    events_where = " WHERE corridor_key = ?" if corridor_key else ""
    scenarios_where = " WHERE corridor_key = ?" if corridor_key else ""
    event_params: list[Any] = [corridor_key] if corridor_key else []
    scenario_params: list[Any] = [corridor_key] if corridor_key else []

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        total_events = conn.execute(
            f"SELECT COUNT(*) AS count FROM discovery_events{events_where}",
            event_params,
        ).fetchone()["count"]
        total_scenarios = conn.execute(
            f"SELECT COUNT(*) AS count FROM v5_scenarios{scenarios_where}",
            scenario_params,
        ).fetchone()["count"]
        follow_up_requested = conn.execute(
            f"SELECT COUNT(*) AS count FROM v5_scenarios{scenarios_where}{' AND' if corridor_key else ' WHERE'} follow_up_requested = 1",
            scenario_params,
        ).fetchone()["count"]
        event_rows = conn.execute(
            f"""
            SELECT event_name, COUNT(*) AS count
            FROM discovery_events
            {events_where}
            GROUP BY event_name
            ORDER BY count DESC, event_name ASC
            """,
            event_params,
        ).fetchall()
        token_rows = conn.execute(
            f"""
            SELECT token, COUNT(*) AS count
            FROM discovery_events
            {events_where}
            GROUP BY token
            ORDER BY count DESC, token ASC
            """,
            event_params,
        ).fetchall()
        review_rows = conn.execute(
            f"""
            SELECT review_state, COUNT(*) AS count
            FROM v5_scenarios
            {scenarios_where}
            GROUP BY review_state
            ORDER BY count DESC, review_state ASC
            """,
            scenario_params,
        ).fetchall()
        recent_follow_up_rows = conn.execute(
            f"""
            SELECT
                id,
                updated_at,
                corridor_key,
                corridor_label,
                token,
                recommended_rail,
                review_state
            FROM v5_scenarios
            {scenarios_where}{' AND' if corridor_key else ' WHERE'} follow_up_requested = 1
            ORDER BY updated_at DESC
            LIMIT 3
            """,
            scenario_params,
        ).fetchall()

    return {
        "total_events": int(total_events or 0),
        "total_scenarios": int(total_scenarios or 0),
        "follow_up_requested": int(follow_up_requested or 0),
        "event_counts": [
            {"event_name": row["event_name"], "count": int(row["count"] or 0)}
            for row in event_rows
        ],
        "token_counts": [
            {"token": row["token"] or "UNKNOWN", "count": int(row["count"] or 0)}
            for row in token_rows
            if row["token"]
        ],
        "review_state_counts": [
            {"review_state": row["review_state"], "count": int(row["count"] or 0)}
            for row in review_rows
        ],
        "recent_follow_up": [
            {
                "id": row["id"],
                "updated_at": row["updated_at"],
                "corridor_key": row["corridor_key"],
                "corridor_label": row["corridor_label"],
                "token": row["token"],
                "recommended_rail": row["recommended_rail"],
                "review_state": row["review_state"],
            }
            for row in recent_follow_up_rows
        ],
    }


def record_payroll_decision(
    *,
    payroll_run_id: str,
    action: str,
    approver: str = "",
    notes: str = "",
    decision_reason: str = "",
    decision_reason_other: str = "",
    decision_rule: str = "",
    readiness_state: str = "",
    risk_level: str = "",
    receipt_id: str = "",
) -> Dict[str, Any]:
    _ensure_db()
    created_at = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute(
            """
            INSERT INTO payroll_decisions (
                created_at,
                updated_at,
                payroll_run_id,
                action,
                approver,
                notes,
                readiness_state,
                risk_level,
                receipt_id,
                decision_reason,
                decision_reason_other,
                decision_rule
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                created_at,
                created_at,
                payroll_run_id,
                action,
                approver,
                notes,
                readiness_state,
                risk_level,
                receipt_id,
                decision_reason,
                decision_reason_other,
                decision_rule,
            ),
        )
        conn.commit()
        decision_id = cursor.lastrowid
    return {
        "id": decision_id,
        "created_at": created_at,
        "updated_at": created_at,
        "payroll_run_id": payroll_run_id,
        "action": action,
        "approver": approver,
        "notes": notes,
        "decision_reason": decision_reason,
        "decision_reason_other": decision_reason_other,
        "decision_rule": decision_rule,
        "readiness_state": readiness_state,
        "risk_level": risk_level,
        "receipt_id": receipt_id,
    }


def get_latest_payroll_decision(payroll_run_id: str) -> Optional[Dict[str, Any]]:
    _ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT
                id,
                created_at,
                updated_at,
                payroll_run_id,
                action,
                approver,
                notes,
                readiness_state,
                risk_level,
                receipt_id,
                decision_reason,
                decision_reason_other,
                decision_rule
            FROM payroll_decisions
            WHERE payroll_run_id = ?
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """,
            (payroll_run_id,),
        ).fetchone()
    if row is None:
        return None
    return {
        "id": row["id"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "payroll_run_id": row["payroll_run_id"],
        "action": row["action"],
        "approver": row["approver"],
        "notes": row["notes"],
        "decision_reason": row["decision_reason"],
        "decision_reason_other": row["decision_reason_other"],
        "decision_rule": row["decision_rule"],
        "readiness_state": row["readiness_state"],
        "risk_level": row["risk_level"],
        "receipt_id": row["receipt_id"],
    }


def record_payroll_handoff(
    *,
    payroll_run_id: str,
    decision_id: Optional[int] = None,
    execution_system: str = "Airwallex",
    status: str = "Queued",
    triggered_by: str = "Approved Handoff API",
) -> Dict[str, Any]:
    _ensure_db()
    created_at = datetime.now(timezone.utc).isoformat()
    queued_at = created_at
    sent_at = created_at if status in {"Sent", "Acknowledged"} else None
    acknowledged_at = created_at if status == "Acknowledged" else None
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute(
            """
            INSERT INTO payroll_handoffs (
                created_at,
                updated_at,
                payroll_run_id,
                decision_id,
                execution_system,
                status,
                queued_at,
                sent_at,
                acknowledged_at,
                triggered_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                created_at,
                created_at,
                payroll_run_id,
                decision_id,
                execution_system,
                status,
                queued_at,
                sent_at,
                acknowledged_at,
                triggered_by,
            ),
        )
        conn.commit()
        handoff_id = cursor.lastrowid
    return get_latest_payroll_handoff(payroll_run_id) or {
        "id": handoff_id,
        "created_at": created_at,
        "updated_at": created_at,
        "payroll_run_id": payroll_run_id,
        "decision_id": decision_id,
        "execution_system": execution_system,
        "status": status,
        "queued_at": queued_at,
        "sent_at": sent_at,
        "acknowledged_at": acknowledged_at,
        "triggered_by": triggered_by,
    }


def transition_payroll_handoff(
    *,
    payroll_run_id: str,
    status: str = "Acknowledged",
) -> Optional[Dict[str, Any]]:
    _ensure_db()
    existing = get_latest_payroll_handoff(payroll_run_id)
    if existing is None:
        return None
    updated_at = datetime.now(timezone.utc).isoformat()
    sent_at = existing.get("sent_at") or updated_at
    acknowledged_at = existing.get("acknowledged_at") or (updated_at if status == "Acknowledged" else None)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            UPDATE payroll_handoffs
            SET
                updated_at = ?,
                status = ?,
                sent_at = ?,
                acknowledged_at = ?,
                triggered_by = ?
            WHERE id = ?
            """,
            (
                updated_at,
                status,
                sent_at,
                acknowledged_at,
                "Approved Handoff API",
                existing["id"],
            ),
        )
        conn.commit()
    return get_latest_payroll_handoff(payroll_run_id)


def get_latest_payroll_handoff(payroll_run_id: str) -> Optional[Dict[str, Any]]:
    _ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT
                id,
                created_at,
                updated_at,
                payroll_run_id,
                decision_id,
                execution_system,
                status,
                queued_at,
                sent_at,
                acknowledged_at,
                triggered_by
            FROM payroll_handoffs
            WHERE payroll_run_id = ?
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """,
            (payroll_run_id,),
        ).fetchone()
    if row is None:
        return None
    return {
        "id": row["id"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "payroll_run_id": row["payroll_run_id"],
        "decision_id": row["decision_id"],
        "execution_system": row["execution_system"],
        "status": row["status"],
        "queued_at": row["queued_at"],
        "sent_at": row["sent_at"],
        "acknowledged_at": row["acknowledged_at"],
        "triggered_by": row["triggered_by"],
    }


def record_payroll_evaluation(
    *,
    payroll_run_id: str,
    transfer_amount_usd: float,
    required_arrival_at: Optional[str],
    effective_deadline_at: str,
    payroll_currency: str,
    override_buffer_percent: Optional[float],
    time_sensitivity: str,
    selected_rail: str,
    readiness_state: str,
    buffer_range_min: float,
    buffer_range_max: float,
    recommended_buffer_percent: float,
    route_payload: Dict[str, Any],
    forecast_payload: Dict[str, Any],
    evaluation_summary: Dict[str, Any],
) -> Dict[str, Any]:
    _ensure_db()
    created_at = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute(
            """
            INSERT INTO payroll_evaluations (
                created_at,
                updated_at,
                payroll_run_id,
                transfer_amount_usd,
                required_arrival_at,
                effective_deadline_at,
                payroll_currency,
                override_buffer_percent,
                time_sensitivity,
                selected_rail,
                readiness_state,
                buffer_range_min,
                buffer_range_max,
                recommended_buffer_percent,
                route_payload_json,
                forecast_payload_json,
                evaluation_summary_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                created_at,
                created_at,
                payroll_run_id,
                transfer_amount_usd,
                required_arrival_at,
                effective_deadline_at,
                payroll_currency,
                override_buffer_percent,
                time_sensitivity,
                selected_rail,
                readiness_state,
                buffer_range_min,
                buffer_range_max,
                recommended_buffer_percent,
                json.dumps(route_payload),
                json.dumps(forecast_payload),
                json.dumps(evaluation_summary),
            ),
        )
        conn.commit()
        evaluation_id = cursor.lastrowid
    return get_latest_payroll_evaluation(payroll_run_id) or {
        "id": evaluation_id,
        "created_at": created_at,
        "updated_at": created_at,
        "payroll_run_id": payroll_run_id,
        "transfer_amount_usd": transfer_amount_usd,
        "required_arrival_at": required_arrival_at,
        "effective_deadline_at": effective_deadline_at,
        "payroll_currency": payroll_currency,
        "override_buffer_percent": override_buffer_percent,
        "time_sensitivity": time_sensitivity,
        "selected_rail": selected_rail,
        "readiness_state": readiness_state,
        "buffer_range_min": buffer_range_min,
        "buffer_range_max": buffer_range_max,
        "recommended_buffer_percent": recommended_buffer_percent,
        "route_payload": route_payload,
        "forecast_payload": forecast_payload,
        "evaluation_summary": evaluation_summary,
    }


def get_latest_payroll_evaluation(payroll_run_id: str) -> Optional[Dict[str, Any]]:
    _ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT
                id,
                created_at,
                updated_at,
                payroll_run_id,
                transfer_amount_usd,
                required_arrival_at,
                effective_deadline_at,
                payroll_currency,
                override_buffer_percent,
                time_sensitivity,
                selected_rail,
                readiness_state,
                buffer_range_min,
                buffer_range_max,
                recommended_buffer_percent,
                route_payload_json,
                forecast_payload_json,
                evaluation_summary_json
            FROM payroll_evaluations
            WHERE payroll_run_id = ?
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """,
            (payroll_run_id,),
        ).fetchone()
    if row is None:
        return None
    return {
        "id": row["id"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "payroll_run_id": row["payroll_run_id"],
        "transfer_amount_usd": row["transfer_amount_usd"],
        "required_arrival_at": row["required_arrival_at"],
        "effective_deadline_at": row["effective_deadline_at"],
        "payroll_currency": row["payroll_currency"],
        "override_buffer_percent": row["override_buffer_percent"],
        "time_sensitivity": row["time_sensitivity"],
        "selected_rail": row["selected_rail"],
        "readiness_state": row["readiness_state"],
        "buffer_range_min": row["buffer_range_min"],
        "buffer_range_max": row["buffer_range_max"],
        "recommended_buffer_percent": row["recommended_buffer_percent"],
        "route_payload": json.loads(row["route_payload_json"]),
        "forecast_payload": json.loads(row["forecast_payload_json"]),
        "evaluation_summary": json.loads(row["evaluation_summary_json"]),
    }


def list_payroll_evaluations(*, payroll_run_id: str, limit: int = 20) -> List[Dict[str, Any]]:
    _ensure_db()
    safe_limit = max(1, min(int(limit), 100))
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT
                id,
                created_at,
                updated_at,
                payroll_run_id,
                transfer_amount_usd,
                required_arrival_at,
                effective_deadline_at,
                payroll_currency,
                override_buffer_percent,
                time_sensitivity,
                selected_rail,
                readiness_state,
                buffer_range_min,
                buffer_range_max,
                recommended_buffer_percent,
                route_payload_json,
                forecast_payload_json,
                evaluation_summary_json
            FROM payroll_evaluations
            WHERE payroll_run_id = ?
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
            """,
            (payroll_run_id, safe_limit),
        ).fetchall()
    return [
        {
            "id": row["id"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "payroll_run_id": row["payroll_run_id"],
            "transfer_amount_usd": row["transfer_amount_usd"],
            "required_arrival_at": row["required_arrival_at"],
            "effective_deadline_at": row["effective_deadline_at"],
            "payroll_currency": row["payroll_currency"],
            "override_buffer_percent": row["override_buffer_percent"],
            "time_sensitivity": row["time_sensitivity"],
            "selected_rail": row["selected_rail"],
            "readiness_state": row["readiness_state"],
            "buffer_range_min": row["buffer_range_min"],
            "buffer_range_max": row["buffer_range_max"],
            "recommended_buffer_percent": row["recommended_buffer_percent"],
            "route_payload": json.loads(row["route_payload_json"]),
            "forecast_payload": json.loads(row["forecast_payload_json"]),
            "evaluation_summary": json.loads(row["evaluation_summary_json"]),
        }
        for row in rows
    ]


def record_payroll_data_snapshot(
    *,
    payroll_run_id: str,
    source_type: str,
    source_label: str,
    snapshot_format: str,
    file_name: str,
    last_loaded_timestamp: Optional[str],
    record_count: int,
    beneficiary_change_count: int,
    verification_status: str,
    data_status: str,
    lineage_label: str,
    snapshot: List[Dict[str, Any]],
    validation_errors: List[str],
) -> Dict[str, Any]:
    _ensure_db()
    created_at = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute(
            """
            INSERT INTO payroll_data_snapshots (
                created_at,
                updated_at,
                payroll_run_id,
                source_type,
                source_label,
                snapshot_format,
                file_name,
                last_loaded_timestamp,
                record_count,
                beneficiary_change_count,
                verification_status,
                data_status,
                lineage_label,
                snapshot_json,
                validation_errors_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                created_at,
                created_at,
                payroll_run_id,
                source_type,
                source_label,
                snapshot_format,
                file_name,
                last_loaded_timestamp,
                record_count,
                beneficiary_change_count,
                verification_status,
                data_status,
                lineage_label,
                json.dumps(snapshot),
                json.dumps(validation_errors),
            ),
        )
        conn.commit()
        snapshot_id = cursor.lastrowid
    return get_latest_payroll_data_snapshot(payroll_run_id) or {
        "id": snapshot_id,
        "created_at": created_at,
        "updated_at": created_at,
        "payroll_run_id": payroll_run_id,
        "source_type": source_type,
        "source_label": source_label,
        "snapshot_format": snapshot_format,
        "file_name": file_name,
        "last_loaded_timestamp": last_loaded_timestamp,
        "record_count": record_count,
        "beneficiary_change_count": beneficiary_change_count,
        "verification_status": verification_status,
        "data_status": data_status,
        "lineage_label": lineage_label,
        "snapshot": snapshot,
        "validation_errors": validation_errors,
    }


def get_latest_payroll_data_snapshot(payroll_run_id: str) -> Optional[Dict[str, Any]]:
    _ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT
                id,
                created_at,
                updated_at,
                payroll_run_id,
                source_type,
                source_label,
                snapshot_format,
                file_name,
                last_loaded_timestamp,
                record_count,
                beneficiary_change_count,
                verification_status,
                data_status,
                lineage_label,
                snapshot_json,
                validation_errors_json
            FROM payroll_data_snapshots
            WHERE payroll_run_id = ?
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """,
            (payroll_run_id,),
        ).fetchone()
    if row is None:
        return None
    return {
        "id": row["id"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "payroll_run_id": row["payroll_run_id"],
        "source_type": row["source_type"],
        "source_label": row["source_label"],
        "snapshot_format": row["snapshot_format"],
        "file_name": row["file_name"],
        "last_loaded_timestamp": row["last_loaded_timestamp"],
        "record_count": int(row["record_count"] or 0),
        "beneficiary_change_count": int(row["beneficiary_change_count"] or 0),
        "verification_status": row["verification_status"],
        "data_status": row["data_status"],
        "lineage_label": row["lineage_label"],
        "snapshot": json.loads(row["snapshot_json"]),
        "validation_errors": json.loads(row["validation_errors_json"]),
    }


def get_last_verified_payroll_data_snapshot(payroll_run_id: str) -> Optional[Dict[str, Any]]:
    _ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT
                id,
                created_at,
                updated_at,
                payroll_run_id,
                source_type,
                source_label,
                snapshot_format,
                file_name,
                last_loaded_timestamp,
                record_count,
                beneficiary_change_count,
                verification_status,
                data_status,
                lineage_label,
                snapshot_json,
                validation_errors_json
            FROM payroll_data_snapshots
            WHERE payroll_run_id = ? AND verification_status = 'verified'
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """,
            (payroll_run_id,),
        ).fetchone()
    if row is None:
        return None
    return {
        "id": row["id"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "payroll_run_id": row["payroll_run_id"],
        "source_type": row["source_type"],
        "source_label": row["source_label"],
        "snapshot_format": row["snapshot_format"],
        "file_name": row["file_name"],
        "last_loaded_timestamp": row["last_loaded_timestamp"],
        "record_count": int(row["record_count"] or 0),
        "beneficiary_change_count": int(row["beneficiary_change_count"] or 0),
        "verification_status": row["verification_status"],
        "data_status": row["data_status"],
        "lineage_label": row["lineage_label"],
        "snapshot": json.loads(row["snapshot_json"]),
        "validation_errors": json.loads(row["validation_errors_json"]),
    }


def record_payroll_data_event(
    *,
    payroll_run_id: str,
    event_name: str,
    source_type: str,
    record_count: int,
    beneficiary_change_count: int,
    metadata: Dict[str, Any],
) -> Dict[str, Any]:
    _ensure_db()
    created_at = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute(
            """
            INSERT INTO payroll_data_events (
                created_at,
                payroll_run_id,
                event_name,
                source_type,
                record_count,
                beneficiary_change_count,
                metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                created_at,
                payroll_run_id,
                event_name,
                source_type,
                record_count,
                beneficiary_change_count,
                json.dumps(metadata),
            ),
        )
        conn.commit()
        event_id = cursor.lastrowid
    return {
        "id": event_id,
        "created_at": created_at,
        "payroll_run_id": payroll_run_id,
        "event_name": event_name,
        "source_type": source_type,
        "record_count": record_count,
        "beneficiary_change_count": beneficiary_change_count,
        "metadata": metadata,
    }


def list_payroll_data_events(*, payroll_run_id: str, limit: int = 20) -> List[Dict[str, Any]]:
    _ensure_db()
    safe_limit = max(1, min(int(limit), 100))
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT
                id,
                created_at,
                payroll_run_id,
                event_name,
                source_type,
                record_count,
                beneficiary_change_count,
                metadata_json
            FROM payroll_data_events
            WHERE payroll_run_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (payroll_run_id, safe_limit),
        ).fetchall()
    return [
        {
            "id": row["id"],
            "created_at": row["created_at"],
            "payroll_run_id": row["payroll_run_id"],
            "event_name": row["event_name"],
            "source_type": row["source_type"],
            "record_count": int(row["record_count"] or 0),
            "beneficiary_change_count": int(row["beneficiary_change_count"] or 0),
            "metadata": json.loads(row["metadata_json"]),
        }
        for row in rows
    ]
