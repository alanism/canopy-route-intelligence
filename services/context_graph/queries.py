"""SQL builders for deterministic context graph extraction."""

from __future__ import annotations

from services.context_graph.schema import ChainSchema

SUPPORTED_TIME_RANGES = {"1h": 1, "2h": 2, "6h": 6, "24h": 24, "7d": 24 * 7}


def parse_time_range(time_range: str) -> int:
    hours = SUPPORTED_TIME_RANGES.get(str(time_range).lower())
    if hours is None:
        raise ValueError(f"Unsupported time range: {time_range}")
    return hours


def resolve_budget_safe_time_range(chain: str, requested_time_range: str, *, mode: str = "full") -> str:
    normalized = str(requested_time_range).lower()
    parse_time_range(normalized)
    if chain == "Polygon" and mode == "transfer_only" and parse_time_range(normalized) > 1:
        return "1h"
    return normalized


def _build_registry_cte(entries: list[dict], *, cte_name: str) -> str:
    if not entries:
        return (
            f"{cte_name} AS (\n"
            "    SELECT\n"
            "        CAST(NULL AS STRING) AS contract_address\n"
            "    WHERE FALSE\n"
            ")"
        )

    rows = []
    for entry in entries:
        rows.append(
            "    SELECT "
            f"'{entry['contract_address']}' AS contract_address"
        )
    return f"{cte_name} AS (\n" + "\n    UNION ALL\n".join(rows) + "\n)"


def _date_filter(hours: int) -> str:
    days = max(1, (hours + 23) // 24)
    return (
        f"DATE(block_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)\n"
        f"      AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)"
    )


def _q(identifier: str) -> str:
    return f"`{identifier}`"


def _expr(identifier: str) -> str:
    if "." in identifier:
        parts = identifier.split(".")
        return ".".join(_q(part) for part in parts)
    return _q(identifier)


def _transfer_select(schema: ChainSchema, token_contract: str, date_filter: str) -> str:
    if schema.transfer_source == "token_transfers":
        value_expr = f"SAFE_CAST({_q(schema.transfer_value_field)} AS FLOAT64) / 1000000"
        return f"""
filtered_transfers AS (
    SELECT
        LOWER(from_address) AS source_wallet,
        LOWER(to_address) AS destination_wallet,
        LOWER({_q(schema.transfer_transaction_hash_field)}) AS transaction_hash,
        {value_expr} AS transfer_volume_usdc,
        block_timestamp
    FROM `{schema.transfer_table}`
    WHERE {date_filter}
      AND LOWER({_q(schema.transfer_contract_field)}) = LOWER('{token_contract}')
      AND from_address IS NOT NULL
      AND to_address IS NOT NULL
)""".strip()

    value_expr = f"SAFE_CAST(JSON_VALUE(TO_JSON_STRING({_q(schema.transfer_value_field)}), '$[2]') AS FLOAT64) / 1000000"
    return f"""
filtered_transfers AS (
    SELECT
        LOWER(JSON_VALUE(TO_JSON_STRING({_q(schema.transfer_value_field)}), '$[0]')) AS source_wallet,
        LOWER(JSON_VALUE(TO_JSON_STRING({_q(schema.transfer_value_field)}), '$[1]')) AS destination_wallet,
        LOWER({_q(schema.transfer_transaction_hash_field)}) AS transaction_hash,
        {value_expr} AS transfer_volume_usdc,
        block_timestamp
    FROM `{schema.transfer_table}`
    WHERE {date_filter}
      AND LOWER({_q(schema.transfer_contract_field)}) = LOWER('{token_contract}')
      AND event_signature = 'Transfer(address,address,uint256)'
      AND JSON_VALUE(TO_JSON_STRING({_q(schema.transfer_value_field)}), '$[0]') IS NOT NULL
      AND JSON_VALUE(TO_JSON_STRING({_q(schema.transfer_value_field)}), '$[1]') IS NOT NULL
)""".strip()


def _trace_select(schema: ChainSchema, date_filter: str) -> str:
    if not schema.traces_table:
        return """
filtered_traces AS (
    SELECT
        CAST(NULL AS STRING) AS transaction_hash,
        CAST(NULL AS STRING) AS to_address,
        CAST(NULL AS STRING) AS from_address,
        CAST(NULL AS TIMESTAMP) AS block_timestamp
    FROM (SELECT 1)
    WHERE FALSE
)""".strip()

    return f"""
filtered_traces AS (
    SELECT
        LOWER({_q(schema.traces_transaction_hash_field)}) AS transaction_hash,
        LOWER({_expr(schema.traces_to_address_field)}) AS to_address,
        LOWER({_expr(schema.traces_from_address_field)}) AS from_address,
        block_timestamp
    FROM `{schema.traces_table}`
    WHERE {date_filter}
      AND LOWER({_q(schema.traces_transaction_hash_field)}) IN (
          SELECT transaction_hash FROM tracked_transfer_hashes
      )
)""".strip()


def _empty_trace_select() -> str:
    return """
filtered_traces AS (
    SELECT
        CAST(NULL AS STRING) AS transaction_hash,
        CAST(NULL AS STRING) AS to_address,
        CAST(NULL AS STRING) AS from_address,
        CAST(NULL AS TIMESTAMP) AS block_timestamp
    FROM (SELECT 1)
    WHERE FALSE
)""".strip()


def build_context_graph_query(
    schema: ChainSchema,
    *,
    token_contract: str,
    protocol_registry: list[dict],
    bridge_registry: list[dict],
    time_range: str = "24h",
    mode: str = "full",
) -> str:
    hours = parse_time_range(time_range)
    date_filter = _date_filter(hours)
    lightweight_mode = mode == "lightweight"
    transfer_only_mode = mode == "transfer_only"
    extra_entity_matches_sql = ""
    protocol_protocol_union_sql = "\nUNION ALL\nSELECT * FROM protocol_protocol_facts"
    if not lightweight_mode and not transfer_only_mode:
        extra_entity_matches_sql = (
            "\n    UNION DISTINCT\n"
            "    SELECT * FROM protocol_matches\n"
            "    UNION DISTINCT\n"
            "    SELECT * FROM bridge_matches"
        )
    else:
        protocol_protocol_union_sql = ""
    if transfer_only_mode:
        filtered_transactions_sql = """
filtered_transactions AS (
    SELECT
        CAST(NULL AS STRING) AS transaction_hash,
        CAST(NULL AS STRING) AS initiator_wallet,
        CAST(NULL AS STRING) AS destination_contract,
        CAST(NULL AS TIMESTAMP) AS block_timestamp
    FROM (SELECT 1)
    WHERE FALSE
)""".strip()
        filtered_receipts_sql = """
filtered_receipts AS (
    SELECT
        CAST(NULL AS STRING) AS transaction_hash,
        CAST(NULL AS FLOAT64) AS gas_price,
        CAST(NULL AS FLOAT64) AS gas_used,
        CAST(NULL AS TIMESTAMP) AS block_timestamp
    FROM (SELECT 1)
    WHERE FALSE
)""".strip()
        filtered_logs_sql = """
filtered_logs AS (
    SELECT
        CAST(NULL AS STRING) AS transaction_hash,
        CAST(NULL AS STRING) AS contract_address,
        CAST(NULL AS TIMESTAMP) AS block_timestamp
    FROM (SELECT 1)
    WHERE FALSE
)""".strip()
        filtered_traces_sql = _empty_trace_select()
    else:
        filtered_transactions_sql = f"""
filtered_transactions AS (
    SELECT
        LOWER({_q(schema.transactions_hash_field)}) AS transaction_hash,
        LOWER(from_address) AS initiator_wallet,
        LOWER(to_address) AS destination_contract,
        block_timestamp
    FROM `{schema.transactions_table}`
    WHERE {date_filter}
      AND LOWER({_q(schema.transactions_hash_field)}) IN (
          SELECT transaction_hash FROM tracked_transfer_hashes
      )
)""".strip()
        filtered_receipts_sql = f"""
filtered_receipts AS (
    SELECT
        LOWER({_q(schema.receipts_transaction_hash_field)}) AS transaction_hash,
        SAFE_CAST({_q(schema.receipt_gas_price_field)} AS FLOAT64) AS gas_price,
        SAFE_CAST({_q(schema.gas_used_field)} AS FLOAT64) AS gas_used,
        block_timestamp
    FROM `{schema.receipts_table}`
    WHERE {date_filter}
      AND LOWER({_q(schema.receipts_transaction_hash_field)}) IN (
          SELECT transaction_hash FROM tracked_transfer_hashes
      )
)""".strip()
        filtered_logs_sql = f"""
filtered_logs AS (
    SELECT
        LOWER({_q(schema.logs_transaction_hash_field)}) AS transaction_hash,
        LOWER({_q(schema.logs_address_field)}) AS contract_address,
        block_timestamp
    FROM `{schema.logs_table}`
    WHERE {date_filter}
      AND LOWER({_q(schema.logs_transaction_hash_field)}) IN (
          SELECT transaction_hash FROM tracked_transfer_hashes
      )
      AND LOWER({_q(schema.logs_address_field)}) IN (SELECT contract_address FROM tracked_contracts)
)""".strip()
        filtered_traces_sql = _trace_select(schema, date_filter)
    protocol_cte = _build_registry_cte(protocol_registry, cte_name="protocol_registry")
    bridge_cte = _build_registry_cte(bridge_registry, cte_name="bridge_registry")

    return f"""
WITH
{protocol_cte},
{bridge_cte},
tracked_contracts AS (
    SELECT contract_address FROM protocol_registry
    UNION DISTINCT
    SELECT contract_address FROM bridge_registry
),
{_transfer_select(schema, token_contract, date_filter)},
tracked_transfer_hashes AS (
    SELECT DISTINCT transaction_hash
    FROM filtered_transfers
),
{filtered_transactions_sql},
{filtered_receipts_sql},
{filtered_logs_sql},
{filtered_traces_sql},
transfer_base AS (
    SELECT
        t.source_wallet,
        t.destination_wallet,
        t.transaction_hash,
        t.transfer_volume_usdc,
        t.block_timestamp,
        tx.destination_contract,
        IFNULL((rc.gas_price * rc.gas_used) / 1e18, 0) AS gas_fee_native
    FROM filtered_transfers t
    LEFT JOIN filtered_transactions tx
      ON t.transaction_hash = tx.transaction_hash
    LEFT JOIN filtered_receipts rc
      ON t.transaction_hash = rc.transaction_hash
),
direct_protocol_matches AS (
    SELECT DISTINCT
        t.transaction_hash,
        p.contract_address AS entity_address,
        'address' AS evidence_type
    FROM transfer_base t
    JOIN protocol_registry p
      ON t.destination_wallet = p.contract_address
      OR t.destination_contract = p.contract_address
),
direct_bridge_matches AS (
    SELECT DISTINCT
        t.transaction_hash,
        b.contract_address AS entity_address,
        'address' AS evidence_type
    FROM transfer_base t
    JOIN bridge_registry b
      ON t.destination_wallet = b.contract_address
      OR t.destination_contract = b.contract_address
),
protocol_matches AS (
    SELECT DISTINCT
        l.transaction_hash,
        p.contract_address AS entity_address,
        'log' AS evidence_type
    FROM filtered_logs l
    JOIN protocol_registry p
      ON l.contract_address = p.contract_address
),
bridge_matches AS (
    SELECT DISTINCT
        l.transaction_hash,
        b.contract_address AS entity_address,
        'log' AS evidence_type
    FROM filtered_logs l
    JOIN bridge_registry b
      ON l.contract_address = b.contract_address
    UNION DISTINCT
    SELECT DISTINCT
        tr.transaction_hash,
        b.contract_address AS entity_address,
        'trace' AS evidence_type
    FROM filtered_traces tr
    JOIN bridge_registry b
      ON tr.to_address = b.contract_address
      OR tr.from_address = b.contract_address
),
entity_matches AS (
    SELECT * FROM direct_protocol_matches
    UNION DISTINCT
    SELECT * FROM direct_bridge_matches
    {extra_entity_matches_sql}
),
tx_transfer_stats AS (
    SELECT
        transaction_hash,
        SUM(transfer_volume_usdc) AS tx_volume_usdc,
        AVG(gas_fee_native) AS avg_gas_fee,
        COUNT(*) AS transfer_events,
        MAX(block_timestamp) AS last_seen,
        ANY_VALUE(source_wallet) AS source_wallet
    FROM transfer_base
    GROUP BY transaction_hash
),
wallet_wallet_facts AS (
    SELECT
        source_wallet AS source_node,
        destination_wallet AS destination_node,
        'wallet' AS source_type,
        'wallet' AS destination_type,
        'wallet_wallet' AS edge_type,
        'USDC' AS token,
        transaction_hash,
        transfer_volume_usdc AS fact_volume,
        block_timestamp AS last_seen,
        gas_fee_native,
        'transfer' AS evidence_type
    FROM transfer_base
),
wallet_protocol_facts AS (
    SELECT
        stats.source_wallet AS source_node,
        em.entity_address AS destination_node,
        'wallet' AS source_type,
        'contract' AS destination_type,
        'wallet_contract' AS edge_type,
        'USDC' AS token,
        stats.transaction_hash,
        stats.tx_volume_usdc AS fact_volume,
        stats.last_seen,
        stats.avg_gas_fee AS gas_fee_native,
        em.evidence_type AS evidence_type
    FROM tx_transfer_stats stats
    JOIN entity_matches em
      ON stats.transaction_hash = em.transaction_hash
),
protocol_protocol_facts AS (
    SELECT
        left_match.entity_address AS source_node,
        right_match.entity_address AS destination_node,
        'contract' AS source_type,
        'contract' AS destination_type,
        'contract_contract' AS edge_type,
        'USDC' AS token,
        left_match.transaction_hash,
        stats.tx_volume_usdc AS fact_volume,
        stats.last_seen,
        stats.avg_gas_fee AS gas_fee_native,
        CONCAT(left_match.evidence_type, ',', right_match.evidence_type) AS evidence_type
    FROM entity_matches left_match
    JOIN entity_matches right_match
      ON left_match.transaction_hash = right_match.transaction_hash
     AND left_match.entity_address < right_match.entity_address
    JOIN tx_transfer_stats stats
      ON stats.transaction_hash = left_match.transaction_hash
)
SELECT * FROM wallet_wallet_facts
UNION ALL
SELECT * FROM wallet_protocol_facts
{protocol_protocol_union_sql}
ORDER BY last_seen DESC, fact_volume DESC
LIMIT 5000
""".strip()


def build_liquidity_gap_query(
    schema: ChainSchema,
    *,
    token_contract: str,
    time_range: str = "24h",
) -> str:
    hours = parse_time_range(time_range)
    date_filter = _date_filter(hours)
    return f"""
WITH filtered_transfers AS (
    SELECT
        block_timestamp
    FROM `{schema.transfer_table}`
    WHERE {date_filter}
      AND LOWER({_q(schema.transfer_contract_field)}) = LOWER('{token_contract}')
      {"AND event_signature = 'Transfer(address,address,uint256)'" if schema.transfer_source == "decoded_events" else ""}
),
gap_rows AS (
    SELECT
        TIMESTAMP_DIFF(
            block_timestamp,
            LAG(block_timestamp) OVER (ORDER BY block_timestamp),
            SECOND
        ) AS gap_seconds
    FROM filtered_transfers
)
SELECT
    AVG(gap_seconds) AS avg_gap_seconds,
    COUNT(*) AS observed_transfer_events
FROM gap_rows
WHERE gap_seconds IS NOT NULL
""".strip()
