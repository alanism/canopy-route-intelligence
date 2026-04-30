# BigQuery Full Audit Gap Report

Last updated: 2026-03-21

## Purpose

This file measures the current Canopy BigQuery refactor against the full audit-pass standard, not just cost reduction.

Audit standard:

- BigQuery must be batch-only for product behavior.
- measured, derived, and decision layers must remain cleanly separated.
- reused analytics should be materialized rather than recomputed.

## Condition Review

### 1. No active user-facing code path can trigger raw BigQuery execution

Current behavior:

- fee/activity BigQuery runs in the background poller
- context graph BigQuery runs in the background graph poller
- corridor request paths read summary-store data or deterministic derived outputs
- live corridor BigQuery remains behind both `allow_live_bigquery=True` and `CANOPY_CORRIDOR_BIGQUERY=true`
- request-scoped runtime guard now raises if corridor live BigQuery is attempted during an HTTP request

Status:

- passed with guardrails

Remaining risk:

- helper code still contains a gated `allow_live_bigquery` escape hatch for non-request batch/debug use

### 2. The fee/activity path is split into measured and derived layers

Current behavior:

- `data/query.py::_build_measured_fee_extraction_query(...)` returns raw facts only
- `services/derived_fee_metrics.py` computes rollups, percentiles, adjusted activity, and gap metrics
- legacy mixed SQL is now moved to `data/legacy_fee_parity.py` and requires explicit opt-in

Status:

- passed

Remaining risk:

- legacy mixed query still exists for parity, but it is no longer part of the active fee module

### 3. Measured queries emit raw facts only, not business meaning

Current behavior:

- fee measured query is cleanly extraction-only
- corridor measured extraction returns raw transfer rows
- measured validator blocks common mixed-layer patterns
- corridor product outputs are now explicitly labeled derived or derived-from-measured
- materialized corridor summaries now preserve explicit `data_layer` and `serving_path` metadata

Status:

- passed with minor metadata follow-up

Remaining risk:

- corridor storage still retains `source` as a convenience field alongside stronger ownership metadata

### 4. Corridor semantics are explicitly classified and corrected if still mixed

Current behavior:

- request path consumes materialized corridor summaries or deterministic derived analytics
- live corridor BigQuery is optional and gated
- the product path is semantically derived, even when measured batch rows are used upstream
- materialized summaries now round-trip `data_layer` and `serving_path`

Status:

- passed

Remaining risk:

- no material semantic ambiguity remains in the active corridor product path

### 5. Context graph heavy auxiliary scans are narrowed further or clearly documented as transitional

Current behavior:

- transfer hashes are narrowed before auxiliary-table access
- `transfer_only` is the safer default for both active chains
- `transfer_only` now disables transactions, receipts, logs, and traces completely
- SQL now emits raw contract-address relationship facts instead of grouped named edge summaries
- Python now owns registry matching, named entity assignment, grouped edge aggregation, topology, confidence, and signal summaries

Status:

- passed with one explicit transitional exception

Remaining risk:

- context graph edges are still the heaviest background query family
- SQL still filters auxiliary evidence through tracked registry contract addresses, so contract-address narrowing remains the final explicit transitional exception

### 6. Validator enforcement is active for measured queries

Current behavior:

- measured fee and measured corridor paths run with `enforce_validation=True`
- validator blocks missing partition filters, `SELECT *`, `LIMIT`, unbounded windows, direct join-first shapes, and missing prefiltered join keys in measured SQL
- shared execution now requires query metadata, blocks request-scoped BigQuery by default, and requires explicit opt-in for `dev_only` queries

Status:

- passed with minor follow-up

Remaining risk:

- derived and dev-only request-policy enforcement now lives partly in the shared execution helper rather than purely in SQL validation

### 7. Materialized outputs exist for reused analytics

Current behavior:

- fee summaries are materialized in `fee_activity_summary`
- corridor summaries are materialized in `corridor_summary`
- context graph snapshots are materialized in `context_graph_summary`
- request paths read those stores instead of recomputing fee data live

Status:

- mostly passed

Remaining risk:

- graph outputs are still recomputed by poller on cadence, even though the latest snapshots are now persisted to a reusable summary store

### 8. Parity reports exist for major migrated paths

Current behavior:

- fee migration map exists
- parity check documentation exists
- synthetic parity tests exist in-repo
- corridor materialization parity report now exists
- context-graph aggregation parity report now exists
- non-request-path live audit runner now exists at `scripts/run_bigquery_audit.py`
- live audit artifacts now exist under `audit/`
- the current sampled live audit run passes

Status:

- passed with one documented fallback

Remaining risk:

- `Polygon / USDC / requested 24h` currently resolves to `1h` in `transfer_only` mode to stay under the 1 GB audit cap

### 9. Legacy mixed queries are removed or quarantined as dev/parity-only

Current behavior:

- legacy fee query is not wired to the active wrapper
- legacy fee query is quarantined in `data/legacy_fee_parity.py`
- runtime opt-in is required for parity execution

Status:

- mostly passed

Remaining risk:

- legacy mixed SQL still exists in-repo, but it is no longer colocated with the active measured fee path

## Current Verdict

The system clearly passes the emergency cost-safety bar.

It is much closer to the full audit bar, but does not fully pass yet.

The remaining gaps are now concentrated in:

1. deciding whether the documented `Polygon requested 24h -> resolved 1h` fallback is the final acceptable audit posture
2. retiring the final context-graph registry-address narrowing exception in SQL if feasible
3. deciding whether to remove or retain the legacy-friendly `source` convenience field long term

## Recommended Next Phase

Next phase should focus on final audit closure work:

1. decide whether to keep or revisit the `Polygon requested 24h -> resolved 1h` budget-safe fallback
2. evaluate whether tracked-contract narrowing in context-graph SQL should remain the documented final exception or move fully into Python
3. decide whether `source` remains compatibility metadata or is removed from serving contracts

## Files Reviewed For This Pass

- `api/cache.py`
- `api/corridor_metrics.py`
- `api/main.py`
- `data/query.py`
- `services/bigquery_client.py`
- `services/context_graph/cache.py`
- `services/context_graph/queries.py`
- `services/bigquery_audit.py`
- `services/corridor_analytics.py`
- `services/summary_store.py`
- `BIGQUERY_PARITY_REPORTS.md`
- `BIGQUERY_SQL_AUDIT.md`
