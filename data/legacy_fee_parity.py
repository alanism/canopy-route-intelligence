"""Parity-only legacy fee query path.

This module is intentionally quarantined from the active fee pipeline.
It exists only for parity comparison and must be explicitly enabled.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Optional

from services.bigquery_client import run_query
from services.token_registry import get_token_config, normalize_token

logger = logging.getLogger("sci-agent.query")

LEGACY_FEE_PARITY_ENABLED = (
    os.getenv("CANOPY_ENABLE_LEGACY_FEE_PARITY", "false").lower() == "true"
)
MIN_PAYMENT_STABLECOIN = 1.0


def _require_legacy_fee_parity_enabled() -> None:
    if not LEGACY_FEE_PARITY_ENABLED:
        raise RuntimeError(
            "Legacy fee parity path is quarantined. Set CANOPY_ENABLE_LEGACY_FEE_PARITY=true "
            "only for parity or dev validation."
        )


def build_legacy_fee_query(
    chain_config: dict,
    *,
    token_contract: str,
    decimals: int,
    native_price_usd: float,
    hours: int = 24,
) -> str:
    cfg = chain_config
    divisor = 10 ** int(decimals)

    return f"""
    WITH transfer_events AS (
        SELECT
            transaction_hash,
            block_timestamp,
            SAFE_CAST(JSON_VALUE(args, '$[2]') AS BIGNUMERIC) / {divisor} AS transfer_value_token
        FROM `{cfg['dataset_events']}`
        WHERE
            LOWER(address) = LOWER('{token_contract}')
            AND event_signature = 'Transfer(address,address,uint256)'
            AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
    ),
    tx_rollup AS (
        SELECT
            transaction_hash,
            MAX(block_timestamp) AS block_timestamp,
            COUNT(*) AS transfer_logs,
            COUNTIF(transfer_value_token >= {MIN_PAYMENT_STABLECOIN}) AS payment_like_transfer_logs,
            SUM(transfer_value_token) AS transfer_volume_usdc,
            SUM(IF(transfer_value_token >= {MIN_PAYMENT_STABLECOIN}, transfer_value_token, 0)) AS payment_like_volume_usdc
        FROM transfer_events
        GROUP BY transaction_hash
    ),
    tx_context AS (
        SELECT
            t.transaction_hash,
            LOWER(t.to_address) AS to_address,
            t.input
        FROM tx_rollup rollup
        JOIN `{cfg['dataset_transactions']}` t
          ON rollup.transaction_hash = t.transaction_hash
        WHERE
            t.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
    ),
    receipt_context AS (
        SELECT
            rollup.transaction_hash,
            r.status,
            r.gas_used,
            r.effective_gas_price
        FROM tx_rollup rollup
        JOIN `{cfg['dataset_receipts']}` r
          ON rollup.transaction_hash = r.transaction_hash
        WHERE
            r.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
    ),
    classified AS (
        SELECT
            t.transaction_hash,
            t.block_timestamp,
            t.transfer_logs,
            t.payment_like_transfer_logs,
            t.transfer_volume_usdc,
            t.payment_like_volume_usdc,
            c.to_address,
            c.input,
            ((CAST(r.gas_used AS FLOAT64) * CAST(r.effective_gas_price AS FLOAT64)) / 1e18) * {native_price_usd} AS tx_fee_usd,
            (((CAST(r.gas_used AS FLOAT64) * CAST(r.effective_gas_price AS FLOAT64)) / 1e18) * {native_price_usd}) / t.transfer_logs AS fee_usd_per_transfer,
            c.to_address = LOWER('{token_contract}') AND t.payment_like_transfer_logs > 0 AS is_adjusted_direct
        FROM tx_rollup t
        JOIN tx_context c
          ON t.transaction_hash = c.transaction_hash
        JOIN receipt_context r
          ON t.transaction_hash = r.transaction_hash
        WHERE
            r.status = 1
            AND r.gas_used IS NOT NULL
            AND r.effective_gas_price IS NOT NULL
    ),
    adjusted_events AS (
        SELECT
            transaction_hash,
            block_timestamp,
            payment_like_transfer_logs,
            payment_like_volume_usdc
        FROM classified
        WHERE is_adjusted_direct
    ),
    adjusted_gap_rows AS (
        SELECT
            TIMESTAMP_DIFF(
                block_timestamp,
                LAG(block_timestamp) OVER (ORDER BY block_timestamp),
                MINUTE
            ) AS gap_minutes
        FROM adjusted_events
    ),
    agg AS (
        SELECT
            SUM(transfer_logs) AS transfer_count,
            SUM(tx_fee_usd) / SUM(transfer_logs) AS avg_fee_usd,
            SUM(transfer_volume_usdc) AS volume_usdc,
            MAX(block_timestamp) AS freshness_timestamp
        FROM classified
    ),
    percentiles AS (
        SELECT
            PERCENTILE_CONT(fee_usd_per_transfer, 0.5) OVER() AS median_fee_usd,
            PERCENTILE_CONT(fee_usd_per_transfer, 0.9) OVER() AS p90_fee_usd
        FROM classified
        LIMIT 1
    ),
    adjusted_activity AS (
        SELECT
            COUNT(*) AS adjusted_transaction_count,
            SUM(payment_like_transfer_logs) AS adjusted_transfer_count,
            SUM(payment_like_volume_usdc) AS adjusted_volume_usdc,
            MAX(block_timestamp) AS adjusted_freshness_timestamp,
            TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(block_timestamp), MINUTE) AS minutes_since_last_adjusted_transfer
        FROM adjusted_events
    ),
    gap_stats AS (
        SELECT AVG(gap_minutes) AS avg_minutes_between_adjusted_transfers
        FROM adjusted_gap_rows
        WHERE gap_minutes IS NOT NULL
    )
    SELECT
        a.transfer_count,
        a.avg_fee_usd,
        p.median_fee_usd,
        p.p90_fee_usd,
        a.volume_usdc,
        a.freshness_timestamp,
        x.adjusted_transaction_count,
        x.adjusted_transfer_count,
        x.adjusted_volume_usdc,
        x.adjusted_freshness_timestamp,
        x.minutes_since_last_adjusted_transfer,
        g.avg_minutes_between_adjusted_transfers
    FROM agg a
    CROSS JOIN percentiles p
    CROSS JOIN adjusted_activity x
    CROSS JOIN gap_stats g
    """


def run_legacy_chain_token_query(
    chain_config: dict,
    *,
    native_price_usd: float,
    token: str,
    maximum_bytes_billed: int,
) -> Optional[dict]:
    _require_legacy_fee_parity_enabled()

    chain = chain_config["chain"]
    token_key = normalize_token(token)
    token_config = get_token_config(token_key)
    token_contract = token_config["contracts"].get(chain)
    if not token_contract:
        logger.info("[%s:%s] No active contract configured; skipping query", chain, token_key)
        return None

    for hours, window_label in [(24, "24h"), (48, "48h")]:
        query = build_legacy_fee_query(
            chain_config,
            token_contract=token_contract,
            decimals=token_config["decimals"],
            native_price_usd=native_price_usd,
            hours=hours,
        )
        _, result = run_query(
            query,
            query_name=f"legacy_fee_activity_{chain.lower()}_{token_key.lower()}_{window_label}",
            query_family="fee_activity_legacy_parity",
            maximum_bytes_billed=maximum_bytes_billed,
            query_classification="dev_only",
        )
        rows = list(result)
        if not rows or rows[0].transfer_count == 0:
            if hours == 24:
                continue
            return None

        row = rows[0]
        freshness = row.freshness_timestamp.isoformat() if row.freshness_timestamp else None
        volume = float(row.volume_usdc) if row.volume_usdc is not None else None
        return {
            "chain": chain,
            "token": token_key,
            "avg_fee_usd": round(float(row.avg_fee_usd), 6) if row.avg_fee_usd else 0.0,
            "median_fee_usd": round(float(row.median_fee_usd), 6) if row.median_fee_usd else 0.0,
            "p90_fee_usd": round(float(row.p90_fee_usd), 6) if row.p90_fee_usd else 0.0,
            "transfer_count": int(row.transfer_count),
            "volume_usdc": volume,
            "adjusted_transaction_count": int(row.adjusted_transaction_count or 0),
            "adjusted_transfer_count": int(row.adjusted_transfer_count or 0),
            "adjusted_volume_usdc": float(row.adjusted_volume_usdc) if row.adjusted_volume_usdc is not None else None,
            "adjusted_freshness_timestamp": row.adjusted_freshness_timestamp.isoformat() if row.adjusted_freshness_timestamp else None,
            "minutes_since_last_adjusted_transfer": int(row.minutes_since_last_adjusted_transfer) if row.minutes_since_last_adjusted_transfer is not None else None,
            "avg_minutes_between_adjusted_transfers": round(float(row.avg_minutes_between_adjusted_transfers), 2) if row.avg_minutes_between_adjusted_transfers is not None else None,
            "activity_filter_method": (
                f"Direct {token_key} contract calls with transfer value >= $1; excludes obvious "
                "router-mediated flows, zero-value logs, and dust-like activity."
            ),
            "window_used": window_label,
            "freshness_timestamp": freshness,
            "native_price_used_usd": native_price_usd,
            "queried_at": datetime.now(timezone.utc).isoformat(),
        }

    return None
