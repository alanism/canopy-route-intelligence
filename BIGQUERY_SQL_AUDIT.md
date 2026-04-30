# BigQuery SQL Audit

Last updated: 2026-03-21

## Purpose

This file collects the current BigQuery SQL shapes and call sites used by Canopy so they can be audited without tracing through multiple modules.

Related audit evidence:

- `BIGQUERY_PARITY_REPORTS.md` for deterministic migration parity checks

## Query Inventory

| Query family | Layer status | SQL owner | Execution path |
| --- | --- | --- | --- |
| Legacy fee activity query | mixed parity-only | `data/legacy_fee_parity.py` | quarantined parity module; explicit opt-in required |
| Measured fee extraction | measured | `data/query.py` | active fee path via `run_chain_query(...)` / `run_chain_token_query(...)` |
| Corridor measured extraction | measured | `services/corridor_analytics.py` | optional batch-only corridor read when explicitly enabled |
| Context graph edges | derived / transitional raw-fact extraction | `services/context_graph/queries.py` | background context-graph poller |
| Liquidity gap query | derived | `services/context_graph/queries.py` | background context-graph poller |

## 1. Legacy Fee Activity Query

Source:

- `data/legacy_fee_parity.py::build_legacy_fee_query(...)`
- `data/legacy_fee_parity.py::run_legacy_chain_token_query(...)`

Current status:

- transitional legacy path
- mixed measured + derived logic
- retained for parity comparison only
- moved out of the active fee module
- explicit runtime opt-in required via `CANOPY_ENABLE_LEGACY_FEE_PARITY=true`

Simple description:

- finds recent token transfer activity for one chain and token
- groups transfers by transaction
- estimates fee per transfer in USD
- applies payment-like and adjusted-direct heuristics inside SQL
- returns summary metrics such as average fee, percentiles, volume, and freshness

```sql
WITH transfer_events AS (
    SELECT
        transaction_hash,
        block_timestamp,
        SAFE_CAST(JSON_VALUE(args, '$[2]') AS BIGNUMERIC) / {divisor} AS transfer_value_token
    FROM `{dataset_events}`
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
        COUNTIF(transfer_value_token >= 1.0) AS payment_like_transfer_logs,
        SUM(transfer_value_token) AS transfer_volume_usdc,
        SUM(IF(transfer_value_token >= 1.0, transfer_value_token, 0)) AS payment_like_volume_usdc
    FROM transfer_events
    GROUP BY transaction_hash
),
tx_context AS (
    SELECT
        t.transaction_hash,
        LOWER(t.to_address) AS to_address,
        t.input
    FROM tx_rollup rollup
    JOIN `{dataset_transactions}` t
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
    JOIN `{dataset_receipts}` r
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
```

## 2. Measured Fee Extraction Query

Source:

- `data/query.py::_build_measured_fee_extraction_query(...)`
- `data/query.py::run_chain_token_query(...)`

Current status:

- active measured fee extraction path
- extraction-only BigQuery query
- derived fee metrics computed in Python

Simple description:

- finds recent token transfer events for one chain and token
- pulls the raw sender, receiver, token amount, transaction destination, and receipt gas fields
- returns raw rows only
- does not calculate business metrics or heuristics inside SQL

```sql
WITH transfer_events AS (
    SELECT
        LOWER(transaction_hash) AS transaction_hash,
        block_timestamp,
        LOWER(JSON_VALUE(args, '$[0]')) AS from_address,
        LOWER(JSON_VALUE(args, '$[1]')) AS to_address,
        LOWER(address) AS token_address,
        SAFE_CAST(JSON_VALUE(args, '$[2]') AS BIGNUMERIC) / {divisor} AS transfer_value_token
    FROM `{dataset_events}`
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
    JOIN `{dataset_transactions}` t
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
    JOIN `{dataset_receipts}` r
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
```

## 3. Corridor Measured Extraction Query

Source:

- `services/corridor_analytics.py::MEASURED_CORRIDOR_EXTRACTION_SQL`
- `services/corridor_analytics.py::get_corridor_volume(...)`

Current status:

- measured-row extraction exists upstream
- used only when live corridor BigQuery is explicitly enabled
- request-time corridor logic otherwise reads materialized summaries
- corridor product semantics are derived, not measured
- materialized corridor summaries now preserve `source`, `data_layer`, and `serving_path`

Simple description:

- finds recent raw token transfer rows for one token over the last 7 days
- returns sender, receiver, token address, transfer amount, and timestamp
- gives the Python layer enough raw data to calculate 24h and 7d corridor volume later
- request-facing corridor metrics are therefore derived-from-measured, not measured

```sql
SELECT
  LOWER(transaction_hash) AS transaction_hash,
  block_timestamp,
  LOWER(from_address) AS from_address,
  LOWER(to_address) AS to_address,
  LOWER(token_address) AS token_address,
  SAFE_CAST(value AS FLOAT64) / 1000000 AS transfer_value_token
FROM `token_transfers`
WHERE LOWER(token_address) = LOWER(@token_contract)
  AND DATE(block_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND from_address IS NOT NULL
  AND to_address IS NOT NULL
```

## 4. Context Graph Edge Query

Source:

- `services/context_graph/queries.py::build_context_graph_query(...)`
- `services/context_graph/cache.py::refresh_snapshots(...)`

Current status:

- background-only
- dry-run and max-bytes guarded
- less mixed than before: SQL now emits raw contract-address relationship facts instead of grouped named edges
- grouped edge aggregation, registry matching, edge labeling, topology, and confidence now happen in Python
- deterministic parity coverage exists for the grouped-edge to fact-plus-aggregation migration
- auxiliary tables now narrowed by filtered transfer hashes
- safer default deploy posture uses `transfer_only` for both active chains
- `transfer_only` now disables transactions, receipts, logs, and traces completely

Simple description:

- finds recent token transfer transactions for a token
- checks whether those transactions interacted with tracked contract addresses
- emits lower-level relationship facts for wallet and contract interactions using raw addresses
- Python matches those addresses to protocol and bridge registries, then builds graph edges and snapshot summaries later

Default full-mode shape:

```sql
WITH
protocol_registry AS (...),
bridge_registry AS (...),
tracked_contracts AS (
    SELECT contract_address FROM protocol_registry
    UNION DISTINCT
    SELECT contract_address FROM bridge_registry
),
filtered_transfers AS (...),
tracked_transfer_hashes AS (
    SELECT DISTINCT transaction_hash
    FROM filtered_transfers
),
filtered_transactions AS (
    SELECT ...
    FROM `{transactions_table}`
    WHERE {date_filter}
      AND LOWER({transactions_hash_field}) IN (
          SELECT transaction_hash FROM tracked_transfer_hashes
      )
),
filtered_receipts AS (
    SELECT ...
    FROM `{receipts_table}`
    WHERE {date_filter}
      AND LOWER({receipts_transaction_hash_field}) IN (
          SELECT transaction_hash FROM tracked_transfer_hashes
      )
),
filtered_logs AS (
    SELECT ...
    FROM `{logs_table}`
    WHERE {date_filter}
      AND LOWER({logs_transaction_hash_field}) IN (
          SELECT transaction_hash FROM tracked_transfer_hashes
      )
      AND LOWER({logs_address_field}) IN (SELECT contract_address FROM tracked_contracts)
),
filtered_traces AS (
    SELECT ...
    FROM `{traces_table}`
    WHERE {date_filter}
      AND LOWER({traces_transaction_hash_field}) IN (
          SELECT transaction_hash FROM tracked_transfer_hashes
      )
),
transfer_base AS (...),
direct_protocol_matches AS (...),
direct_bridge_matches AS (...),
protocol_matches AS (...),
bridge_matches AS (...),
entity_matches AS (...),
tx_transfer_stats AS (...),
wallet_wallet_facts AS (...),
wallet_protocol_facts AS (...),
protocol_protocol_facts AS (...)
SELECT * FROM wallet_wallet_facts
UNION ALL
SELECT * FROM wallet_protocol_facts
[UNION ALL SELECT * FROM protocol_protocol_facts]
ORDER BY last_seen DESC, fact_volume DESC
LIMIT 5000
```

Transfer-only mode differs in one important way:

- `filtered_transactions`, `filtered_receipts`, `filtered_logs`, and `filtered_traces` are empty CTEs (`WHERE FALSE`)
- only `filtered_transfers` is scanned before downstream graph assembly

## 5. Liquidity Gap Query

Source:

- `services/context_graph/queries.py::build_liquidity_gap_query(...)`
- `services/context_graph/cache.py::refresh_snapshots(...)`

Current status:

- derived metric query
- background-only

Simple description:

- looks at the timestamps of recent token transfers
- measures the time gap between one transfer and the next
- returns the average gap so Canopy can estimate how continuous or sparse the activity is

```sql
WITH filtered_transfers AS (
    SELECT block_timestamp
    FROM `{transfer_table}`
    WHERE DATE(block_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
      AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
      AND LOWER(`{contract_field}`) = LOWER('{token_contract}')
      [AND event_signature = 'Transfer(address,address,uint256)']
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
```

## Execution Notes

- Shared execution helper: `services/bigquery_client.py`
- Guardrails:
  - dry run before execution
  - `maximum_bytes_billed`
  - explicit `query_family` metadata required at runtime
  - query logging with `query_name`, `bytes_processed`, `execution_time`
  - in-memory query-family metrics registry in `services/query_metrics.py`
  - runtime metrics endpoint at `/v1/system/bigquery-metrics`
  - poller digest logs via `bigquery.metrics.digest`
  - request-scoped execution blocked in the shared client unless explicitly allowed
  - `dev_only` BigQuery queries require `CANOPY_ENABLE_DEV_BIGQUERY=true`
- current family-specific caps:
  - fee extraction: `CANOPY_FEE_QUERY_MAX_BYTES_PER_QUERY`
  - corridor extraction: `CANOPY_CORRIDOR_MAX_BYTES_PER_QUERY`
  - context graph baseline: `CANOPY_CONTEXT_GRAPH_MAX_BYTES_PER_QUERY`
  - context graph edges: `CANOPY_CONTEXT_GRAPH_EDGE_MAX_BYTES_PER_QUERY`
  - context graph gap: `CANOPY_CONTEXT_GRAPH_GAP_MAX_BYTES_PER_QUERY`
- current safer mode defaults:
  - `CANOPY_CONTEXT_GRAPH_ETHEREUM_MODE=transfer_only`
  - `CANOPY_CONTEXT_GRAPH_POLYGON_MODE=transfer_only`
- current materialized outputs:
  - `fee_activity_summary`
  - `corridor_summary`
  - `context_graph_summary`
- materialized ownership metadata:
  - `corridor_summary` persists `source`, `data_layer`, and `serving_path`
- Measured query validation:
  - partition filter required
  - no `SELECT *`
  - no `LIMIT` as cost control
  - explicit bounded windows required
  - JOINs must use pre-filtered hash CTE shapes
  - no forbidden measured-layer business/heuristic patterns
- Audit entrypoint:
  - `scripts/run_bigquery_audit.py`
  - writes `audit/bigquery_live_audit_report.json`
  - writes `audit/bigquery_live_audit_report.md`
  - current live run passes the sampled set
  - documented budget-safe fallback: `Polygon / requested 24h / transfer_only` resolves to `1h`

## Audit Focus Areas

1. legacy fee query still contains hidden heuristics and should remain parity-only until explicitly retired
2. measured fee extraction is the active fee path and the cleanest candidate for optimization review
3. corridor extraction is request-gated, but corridor product outputs should be treated as derived-from-measured rather than measured
4. context graph remains the cost-heaviest background path despite recent hash narrowing, even though registry matching and grouped edge aggregation have now moved to Python
5. runtime monitoring now exists, but cost safety still depends on watching query-family drift over time, especially `context_graph_edges`
6. the main remaining design choice is whether the documented `Polygon requested 24h -> resolved 1h` fallback is the final desired audit posture
