# BigQuery Parity Reports

Last updated: 2026-03-21

## Purpose

This file captures deterministic parity evidence for major BigQuery refactor migrations.

The goal is not to prove that every implementation detail stayed identical. The goal is
to prove that the migrated path preserves the intended product behavior while making any
intentional layer-ownership changes explicit.

## 1. Corridor Materialization Parity

Migration:

- before: corridor analytics were rebuilt in-process and then consumed directly
- now: corridor analytics are materialized into `corridor_summary` and served from that store

Comparison basis:

- same corridor
- same rail
- same token
- same deterministic upstream rail inputs

Parity result:

- core corridor business metrics are preserved across the materialization boundary
- `volume_24h`, `volume_7d`, `tx_count`, sender/receiver counts, bridge metrics, whale metrics, and flow metrics round-trip without change
- explicit ownership metadata now also survives the round-trip:
  - `source`
  - `data_layer`
  - `serving_path`

Intentional delta:

- no numerical delta is expected or allowed in this migration
- the main change is architectural: serving now reads a stored summary instead of recomputing the same result on demand

Evidence:

- `tests/test_bigquery_parity_reports.py::CorridorParityTests`

## 2. Context Graph Aggregation Parity

Migration:

- before: SQL could emit grouped edge summaries directly
- now: SQL emits relationship facts and Python aggregates them into edge summaries

Comparison basis:

- same chain
- same token
- same time range
- same effective edge relationships

Parity result:

- the final graph snapshot preserves:
  - topology
  - topology classification
  - flow density
  - protocol noise ratio
  - bridge usage rate
  - counterparty entropy
  - liquidity gap
  - total transaction count
  - visible edge summaries
  - evidence stack

Intentional delta:

- the layer boundary changed on purpose
- grouped edge interpretation moved out of SQL and into Python aggregation
- this is a correctness and auditability improvement, not a product-behavior change

Evidence:

- `tests/test_bigquery_parity_reports.py::ContextGraphParityTests`

## Scope Notes

- these are deterministic in-repo parity checks
- they do not replace live side-by-side validation against production BigQuery data
- fee-path parity remains covered separately by the measured-versus-legacy fee migration tests
- live audit entrypoint now exists at `scripts/run_bigquery_audit.py`
- live runs write:
  - `audit/bigquery_live_audit_report.json`
  - `audit/bigquery_live_audit_report.md`

## Current Read

Parity evidence is now stronger for:

- fee/activity
- corridor materialization
- context graph aggregation split

Live audit artifact:

- `audit/bigquery_live_audit_report.json`
- `audit/bigquery_live_audit_report.md`
- current live run on 2026-03-21 passes the sampled corridor and context-graph set

Remaining parity gap:

- the sampled set now passes by using a documented budget-safe fallback:
  - `Polygon / USDC / requested 24h` resolves to `1h` in `transfer_only` mode
- if future audit scope requires true `Polygon 24h` raw extraction under the same cap, additional source-table optimization would still be needed
