# Fee Path Parity Check

Last updated: 2026-03-20

## Summary

The fee path parity check has two parts:

1. synthetic fixture parity in-repo
2. optional live parity when GCP credentials are available

## Current Environment Status

- synthetic parity: implemented and testable in-repo
- synthetic parity coverage: `tests/test_fee_layer_split.py`
- live BigQuery parity: not executed in this workspace because no GCP project or credentials are configured

## Fields Compared

- `avg_fee_usd`
- `median_fee_usd`
- `p90_fee_usd`
- `transfer_count`
- `volume_usdc`
- `adjusted_transaction_count`
- `adjusted_transfer_count`
- `adjusted_volume_usdc`
- `adjusted_freshness_timestamp`
- `minutes_since_last_adjusted_transfer`
- `avg_minutes_between_adjusted_transfers`

## Comparison Method

- legacy semantics are reproduced from measured rows using the current mixed-query logic
- new derived metrics are computed from the same measured rows
- synthetic parity passes when all fields match exactly or differ only in documented rounding
- targeted validator coverage exists in `tests/test_query_validator.py`

## Known Live Limitation

Live parity remains pending until the environment provides:

- `GCP_PROJECT_ID` or ADC-discoverable project
- working BigQuery credentials

## Interpretation Guidance

If live parity later shows differences, classify them as one of:

- old hidden heuristic behavior
- old sampling behavior
- corrected layer separation
- implementation bug
