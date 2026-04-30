"""Local summary tables for precomputed Canopy analytics."""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional

DEFAULT_DB_PATH = (
    Path(os.getenv("CANOPY_SUMMARY_DB_PATH", ""))
    if os.getenv("CANOPY_SUMMARY_DB_PATH")
    else Path(__file__).resolve().parent.parent / "data" / "canopy_summary.db"
)


def _ensure_parent() -> None:
    DEFAULT_DB_PATH.parent.mkdir(parents=True, exist_ok=True)


@contextmanager
def _connect() -> Iterator[sqlite3.Connection]:
    _ensure_parent()
    connection = sqlite3.connect(DEFAULT_DB_PATH)
    connection.row_factory = sqlite3.Row
    try:
        yield connection
    finally:
        connection.close()


def init_summary_store() -> None:
    with _connect() as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS fee_activity_summary (
                chain TEXT NOT NULL,
                token TEXT NOT NULL,
                avg_fee_usd REAL,
                median_fee_usd REAL,
                p90_fee_usd REAL,
                transfer_count INTEGER,
                volume_usdc REAL,
                adjusted_transaction_count INTEGER,
                adjusted_transfer_count INTEGER,
                adjusted_volume_usdc REAL,
                adjusted_freshness_timestamp TEXT,
                minutes_since_last_adjusted_transfer INTEGER,
                avg_minutes_between_adjusted_transfers REAL,
                activity_filter_method TEXT,
                window_used TEXT,
                freshness_timestamp TEXT,
                native_price_used_usd REAL,
                queried_at TEXT,
                materialized_at TEXT NOT NULL,
                PRIMARY KEY (chain, token)
            );

            CREATE TABLE IF NOT EXISTS corridor_summary (
                corridor_id TEXT NOT NULL,
                rail TEXT NOT NULL,
                token TEXT NOT NULL,
                time_range TEXT NOT NULL,
                volume_24h REAL,
                volume_7d REAL,
                tx_count INTEGER,
                unique_senders INTEGER,
                unique_receivers INTEGER,
                velocity_unique_capital REAL,
                concentration_score REAL,
                bridge_name TEXT,
                bridge_share REAL,
                bridge_volume REAL,
                bridge_transactions INTEGER,
                whale_threshold_usd REAL,
                whale_activity_score REAL,
                net_flow_7d REAL,
                top_whale_flows_json TEXT,
                source TEXT,
                data_layer TEXT,
                serving_path TEXT,
                materialized_at TEXT NOT NULL,
                PRIMARY KEY (corridor_id, rail, token, time_range)
            );

            CREATE TABLE IF NOT EXISTS context_graph_summary (
                chain TEXT NOT NULL,
                token TEXT NOT NULL,
                time_range TEXT NOT NULL,
                snapshot_json TEXT NOT NULL,
                materialized_at TEXT NOT NULL,
                PRIMARY KEY (chain, token, time_range)
            );
            """
        )
        existing_columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(corridor_summary)").fetchall()
        }
        if "data_layer" not in existing_columns:
            connection.execute("ALTER TABLE corridor_summary ADD COLUMN data_layer TEXT")
        if "serving_path" not in existing_columns:
            connection.execute("ALTER TABLE corridor_summary ADD COLUMN serving_path TEXT")
        connection.commit()


def upsert_fee_activity_summary(rows: list[dict]) -> None:
    if not rows:
        return
    with _connect() as connection:
        connection.executemany(
            """
            INSERT INTO fee_activity_summary (
                chain, token, avg_fee_usd, median_fee_usd, p90_fee_usd,
                transfer_count, volume_usdc, adjusted_transaction_count,
                adjusted_transfer_count, adjusted_volume_usdc,
                adjusted_freshness_timestamp, minutes_since_last_adjusted_transfer,
                avg_minutes_between_adjusted_transfers, activity_filter_method,
                window_used, freshness_timestamp, native_price_used_usd,
                queried_at, materialized_at
            ) VALUES (
                :chain, :token, :avg_fee_usd, :median_fee_usd, :p90_fee_usd,
                :transfer_count, :volume_usdc, :adjusted_transaction_count,
                :adjusted_transfer_count, :adjusted_volume_usdc,
                :adjusted_freshness_timestamp, :minutes_since_last_adjusted_transfer,
                :avg_minutes_between_adjusted_transfers, :activity_filter_method,
                :window_used, :freshness_timestamp, :native_price_used_usd,
                :queried_at, :materialized_at
            )
            ON CONFLICT(chain, token) DO UPDATE SET
                avg_fee_usd=excluded.avg_fee_usd,
                median_fee_usd=excluded.median_fee_usd,
                p90_fee_usd=excluded.p90_fee_usd,
                transfer_count=excluded.transfer_count,
                volume_usdc=excluded.volume_usdc,
                adjusted_transaction_count=excluded.adjusted_transaction_count,
                adjusted_transfer_count=excluded.adjusted_transfer_count,
                adjusted_volume_usdc=excluded.adjusted_volume_usdc,
                adjusted_freshness_timestamp=excluded.adjusted_freshness_timestamp,
                minutes_since_last_adjusted_transfer=excluded.minutes_since_last_adjusted_transfer,
                avg_minutes_between_adjusted_transfers=excluded.avg_minutes_between_adjusted_transfers,
                activity_filter_method=excluded.activity_filter_method,
                window_used=excluded.window_used,
                freshness_timestamp=excluded.freshness_timestamp,
                native_price_used_usd=excluded.native_price_used_usd,
                queried_at=excluded.queried_at,
                materialized_at=excluded.materialized_at
            """,
            rows,
        )
        connection.commit()


def fetch_fee_activity_summaries() -> list[dict]:
    with _connect() as connection:
        rows = connection.execute("SELECT * FROM fee_activity_summary").fetchall()
    return [dict(row) for row in rows]


def upsert_corridor_summary(rows: list[dict]) -> None:
    if not rows:
        return
    with _connect() as connection:
        connection.executemany(
            """
            INSERT INTO corridor_summary (
                corridor_id, rail, token, time_range, volume_24h, volume_7d,
                tx_count, unique_senders, unique_receivers,
                velocity_unique_capital, concentration_score, bridge_name,
                bridge_share, bridge_volume, bridge_transactions,
                whale_threshold_usd, whale_activity_score, net_flow_7d,
                top_whale_flows_json, source, data_layer, serving_path, materialized_at
            ) VALUES (
                :corridor_id, :rail, :token, :time_range, :volume_24h, :volume_7d,
                :tx_count, :unique_senders, :unique_receivers,
                :velocity_unique_capital, :concentration_score, :bridge_name,
                :bridge_share, :bridge_volume, :bridge_transactions,
                :whale_threshold_usd, :whale_activity_score, :net_flow_7d,
                :top_whale_flows_json, :source, :data_layer, :serving_path, :materialized_at
            )
            ON CONFLICT(corridor_id, rail, token, time_range) DO UPDATE SET
                volume_24h=excluded.volume_24h,
                volume_7d=excluded.volume_7d,
                tx_count=excluded.tx_count,
                unique_senders=excluded.unique_senders,
                unique_receivers=excluded.unique_receivers,
                velocity_unique_capital=excluded.velocity_unique_capital,
                concentration_score=excluded.concentration_score,
                bridge_name=excluded.bridge_name,
                bridge_share=excluded.bridge_share,
                bridge_volume=excluded.bridge_volume,
                bridge_transactions=excluded.bridge_transactions,
                whale_threshold_usd=excluded.whale_threshold_usd,
                whale_activity_score=excluded.whale_activity_score,
                net_flow_7d=excluded.net_flow_7d,
                top_whale_flows_json=excluded.top_whale_flows_json,
                source=excluded.source,
                data_layer=excluded.data_layer,
                serving_path=excluded.serving_path,
                materialized_at=excluded.materialized_at
            """,
            rows,
        )
        connection.commit()


def get_corridor_summary(
    corridor_id: str,
    rail: str,
    *,
    token: str = "USDC",
    time_range: str = "24h",
) -> Optional[dict]:
    with _connect() as connection:
        row = connection.execute(
            """
            SELECT * FROM corridor_summary
            WHERE corridor_id = ? AND rail = ? AND token = ? AND time_range = ?
            """,
            (corridor_id, rail, token, time_range),
        ).fetchone()
    if row is None:
        return None
    payload = dict(row)
    payload["top_whale_flows"] = json.loads(payload.pop("top_whale_flows_json") or "[]")
    return payload


def encode_top_whale_flows(flows: list[dict]) -> str:
    return json.dumps(flows, sort_keys=True)


def upsert_context_graph_summary(rows: list[dict]) -> None:
    if not rows:
        return
    with _connect() as connection:
        connection.executemany(
            """
            INSERT INTO context_graph_summary (
                chain, token, time_range, snapshot_json, materialized_at
            ) VALUES (
                :chain, :token, :time_range, :snapshot_json, :materialized_at
            )
            ON CONFLICT(chain, token, time_range) DO UPDATE SET
                snapshot_json=excluded.snapshot_json,
                materialized_at=excluded.materialized_at
            """,
            rows,
        )
        connection.commit()


def get_context_graph_summary(
    chain: str,
    *,
    token: str = "USDC",
    time_range: str = "24h",
) -> Optional[dict]:
    with _connect() as connection:
        row = connection.execute(
            """
            SELECT snapshot_json, materialized_at
            FROM context_graph_summary
            WHERE chain = ? AND token = ? AND time_range = ?
            """,
            (chain, token, time_range),
        ).fetchone()
    if row is None:
        return None
    payload = json.loads(row["snapshot_json"])
    payload["materialized_at"] = row["materialized_at"]
    return payload
