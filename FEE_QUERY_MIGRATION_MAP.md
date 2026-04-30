# Fee Query Migration Map

Last updated: 2026-03-20

## Summary

The current fee query in `data/query.py` mixes measured extraction, derived metrics, and hidden heuristics inside a single BigQuery query. This file maps every current output and assigns its target ownership.

## Current Outputs

| Output field | Current classification | Target layer | Notes |
| --- | --- | --- | --- |
| `transfer_count` | derived | derived | Aggregate count of transfer logs. |
| `avg_fee_usd` | derived | derived | USD fee conversion plus aggregate average. |
| `median_fee_usd` | derived | derived | Percentile metric. |
| `p90_fee_usd` | derived | derived | Percentile metric. |
| `volume_usdc` | derived | derived | Aggregate token volume. |
| `freshness_timestamp` | derived | derived | Freshness rollup. |
| `adjusted_transaction_count` | derived | derived | Heuristic-based filtered count. |
| `adjusted_transfer_count` | derived | derived | Heuristic-based filtered count. |
| `adjusted_volume_usdc` | derived | derived | Heuristic-based filtered volume. |
| `adjusted_freshness_timestamp` | derived | derived | Heuristic-based freshness rollup. |
| `minutes_since_last_adjusted_transfer` | derived | derived | Freshness metric. |
| `avg_minutes_between_adjusted_transfers` | derived | derived | Gap metric. |
| `activity_filter_method` | decision-ish derived | derived | Explicit explanation of heuristic filtering. |
| `window_used` | derived | derived | Batch-level metadata. |
| `native_price_used_usd` | derived | derived | Modeling input. |
| `queried_at` | derived | derived | Batch metadata. |

## Hidden Assumptions Buried in the Current Query

- `payment_like_transfer_logs`
  - Assumes transfers below `$1` are not payment-like.
- `payment_like_volume_usdc`
  - Excludes low-value transfers from adjusted volume.
- `is_adjusted_direct`
  - Assumes direct token-contract destination plus payment-like transfers is a meaningful activity heuristic.
- `fee_usd_per_transfer`
  - Converts gas facts into USD and divides by transfer log count.
- percentile logic
  - Uses `PERCENTILE_CONT` over query output, which is derived analytics, not measured data.
- freshness logic
  - Treats max timestamps and time deltas as query outputs.
- USD conversions
  - Applies runtime native asset pricing inside the data path.

## What Stays in BigQuery Measured Extraction

- `transaction_hash`
- `block_timestamp`
- `from_address`
- `to_address`
- `token_address`
- normalized token value
- transaction destination address
- receipt status
- gas used
- effective gas price

## What Moves to Derived Python Computation

- transaction-level rollups
- transfer log counts
- payment-like filtering
- direct / adjusted heuristics
- fee-in-USD math
- avg / median / p90
- volume aggregates
- freshness rollups
- gap metrics
- response metadata built from those metrics

## Migration Sequence

1. Add measured fee extraction query path.
2. Add derived fee metrics module that reproduces current semantics.
3. Compare legacy mixed output vs derived output on the same measured fixture.
4. Switch the batch poller only after parity is documented.
