# BigQuery Pipeline Architecture

Last updated: 2026-03-20

## Summary

Canopy uses BigQuery as a batch extraction system. Request handlers must never trigger raw BigQuery reads.

The pipeline is split into three layers:

- `measured`
- `derived`
- `decision`

## Measured Queries

- measured fee extraction
- measured corridor extraction
- future measured graph extraction

Measured queries may emit raw facts only.

## Derived Computations

- fee metrics rollups
- corridor summaries
- liquidity gap metrics
- graph summaries
- validator-backed migration checks

Derived code may aggregate, score data quality, and compute heuristics, but may not emit UI-facing route recommendations.

## Decision Layer

- route scoring
- confidence labels
- recommendation payloads
- user-facing summaries

## Batch Ownership

Background pollers own:

- BigQuery extraction
- summary table refreshes
- cache updates

## Cache and Summary Stores

- `fee_activity_summary`
- `corridor_summary`
- in-memory route cache

These are transitional until all upstream mixed paths are fully separated, but they remain batch-owned.

## Forbidden Request-Path Behaviors

- direct BigQuery reads
- query builders invoked from request handlers
- runtime-derived fallbacks that recompute batch data when summaries are missing

## Rules of the Road

- If a query emits business meaning, it is not measured.
- If a request path needs BigQuery, the architecture is broken.
- New measured queries must pass validator enforcement.
- Parity is required before switching a legacy mixed path.
- Query families should carry explicit byte caps instead of relying only on one global default.
