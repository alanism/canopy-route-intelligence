"""BigQuery query module for token-aware Canopy fee analytics."""

import logging
import os
from typing import Optional

from dotenv import load_dotenv

from services.derived_fee_metrics import derive_fee_metrics
from services.bigquery_client import DEFAULT_MAX_BYTES_BILLED, run_query
from services.token_registry import get_token_config, normalize_token

load_dotenv()

logger = logging.getLogger("sci-agent.query")

CHAIN_CONFIGS = {
    "Polygon": {
        "chain": "Polygon",
        "query_style": "decoded_events_receipts",
        "dataset_events": "bigquery-public-data.goog_blockchain_polygon_mainnet_us.decoded_events",
        "dataset_receipts": "bigquery-public-data.goog_blockchain_polygon_mainnet_us.receipts",
        "dataset_transactions": "bigquery-public-data.goog_blockchain_polygon_mainnet_us.transactions",
        "native_asset": "POL",
    },
    "Ethereum": {
        "chain": "Ethereum",
        "query_style": "decoded_events_receipts",
        "dataset_events": "bigquery-public-data.goog_blockchain_ethereum_mainnet_us.decoded_events",
        "dataset_receipts": "bigquery-public-data.goog_blockchain_ethereum_mainnet_us.receipts",
        "dataset_transactions": "bigquery-public-data.goog_blockchain_ethereum_mainnet_us.transactions",
        "native_asset": "ETH",
    },
}

MIN_PAYMENT_STABLECOIN = 1.0
FEE_QUERY_MAX_BYTES_BILLED = int(
    os.getenv("CANOPY_FEE_QUERY_MAX_BYTES_PER_QUERY", str(DEFAULT_MAX_BYTES_BILLED))
)


def _query_windows_hours() -> list[int]:
    raw = os.getenv("CANOPY_MEASURED_QUERY_WINDOWS_HOURS", "24,48")
    windows = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            value = int(item)
        except ValueError:
            continue
        if value > 0:
            windows.append(value)
    return windows or [24, 48]


def _build_measured_fee_extraction_query(
    chain_config: dict,
    *,
    token_contract: str,
    decimals: int,
    hours: int = 24,
) -> str:
    """
    Build a measured-layer extraction query for fee analytics.

    This query is extraction-only. It returns raw transfer and receipt facts
    without applying heuristics, percentiles, freshness summaries, or business
    interpretation.
    """

    cfg = chain_config
    divisor = 10 ** int(decimals)
    return f"""
    WITH transfer_events AS (
        SELECT
            LOWER(transaction_hash) AS transaction_hash,
            block_timestamp,
            LOWER(JSON_VALUE(args, '$[0]')) AS from_address,
            LOWER(JSON_VALUE(args, '$[1]')) AS to_address,
            LOWER(address) AS token_address,
            SAFE_CAST(JSON_VALUE(args, '$[2]') AS BIGNUMERIC) / {divisor} AS transfer_value_token
        FROM `{cfg['dataset_events']}`
        WHERE
            LOWER(address) = LOWER('{token_contract}')
            AND event_signature = 'Transfer(address,address,uint256)'
            AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
            AND JSON_VALUE(args, '$[0]') IS NOT NULL
            AND JSON_VALUE(args, '$[1]') IS NOT NULL
    ),
    transfer_hashes AS (
        SELECT DISTINCT transaction_hash
        FROM transfer_events
    ),
    tx_context AS (
        SELECT
            LOWER(t.transaction_hash) AS transaction_hash,
            LOWER(t.to_address) AS tx_to_address
        FROM transfer_hashes hashes
        JOIN `{cfg['dataset_transactions']}` t
          ON hashes.transaction_hash = LOWER(t.transaction_hash)
        WHERE
            t.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
    ),
    receipt_context AS (
        SELECT
            LOWER(r.transaction_hash) AS transaction_hash,
            r.status,
            r.gas_used,
            r.effective_gas_price
        FROM transfer_hashes hashes
        JOIN `{cfg['dataset_receipts']}` r
          ON hashes.transaction_hash = LOWER(r.transaction_hash)
        WHERE
            r.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
    )
    SELECT
        e.transaction_hash,
        e.block_timestamp,
        e.from_address,
        e.to_address,
        e.token_address,
        e.transfer_value_token,
        tx.tx_to_address,
        rc.status,
        rc.gas_used,
        rc.effective_gas_price
    FROM transfer_events e
    LEFT JOIN tx_context tx
      ON e.transaction_hash = tx.transaction_hash
    LEFT JOIN receipt_context rc
      ON e.transaction_hash = rc.transaction_hash
    """


def run_chain_token_query(chain_config: dict, native_price_usd: float, token: str) -> Optional[dict]:
    chain = chain_config["chain"]
    token_key = normalize_token(token)
    token_config = get_token_config(token_key)
    token_contract = token_config["contracts"].get(chain)
    if not token_contract:
        logger.info("[%s:%s] No active contract configured; skipping query", chain, token_key)
        return None

    windows = _query_windows_hours()
    for hours in windows:
        window_label = f"{hours}h"
        logger.info("[%s:%s] Querying measured extraction %s window...", chain, token_key, window_label)
        query = _build_measured_fee_extraction_query(
            chain_config,
            token_contract=token_contract,
            decimals=token_config["decimals"],
            hours=hours,
        )
        try:
            _, result = run_query(
                query,
                query_name=f"measured_fee_extraction_{chain.lower()}_{token_key.lower()}_{window_label}",
                query_family="fee_activity",
                maximum_bytes_billed=FEE_QUERY_MAX_BYTES_BILLED,
                query_classification="measured",
                enforce_validation=True,
            )
            rows = [dict(row.items()) for row in result]
        except Exception as exc:
            logger.error(
                "[%s:%s] BigQuery measured extraction error (%s): %s",
                chain,
                token_key,
                window_label,
                exc,
            )
            raise

        derived = derive_fee_metrics(
            rows,
            chain=chain,
            token=token_key,
            token_contract=token_contract,
            native_price_usd=native_price_usd,
            window_label=window_label,
            min_payment_stablecoin=MIN_PAYMENT_STABLECOIN,
        )
        if derived is None or derived["transfer_count"] == 0:
            if hours != windows[-1]:
                logger.warning(
                    "[%s:%s] No transfers in %s, falling back to the next configured window",
                    chain,
                    token_key,
                    window_label,
                )
                continue
            logger.warning("[%s:%s] No transfers in %s either", chain, token_key, window_label)
            return None
        return derived

    return None


def run_chain_query(chain_config: dict, native_price_usd: float) -> Optional[dict]:
    """Backwards-compatible wrapper used by legacy validation/tests."""
    return run_chain_token_query(chain_config, native_price_usd, "USDC")
