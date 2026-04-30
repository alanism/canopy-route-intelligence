# Data Quality

Project DG uses validation and freshness checks so the dashboard can distinguish current observed data from stale, fallback, or sample data.

## Freshness Gate

Freshness is based on the age of the latest successful source refresh or cached summary. Fresh data can support current benchmark views. Stale data should be shown with degraded health and should not be described as real-time.

When data is stale, the API can continue to serve the last known benchmark context, but the dashboard should communicate the degraded state.

## Reconciliation Checks

Reconciliation checks compare normalized records and summary outputs for internal consistency. These checks are intended to catch gaps such as missing transfer values, unexpected empty result sets, inconsistent token metadata, or mismatched route summaries.

## Supply Parity Checks

Supply parity checks compare expected token and route coverage against available source outputs. They help identify when a supported chain/token pair is missing, stale, or only available as limited/demo coverage.

## Sample Fields

The following fields may be sample/demo values in this hackathon repo:

- corridor operating assumptions
- payout readiness labels
- review state
- workflow status
- sample merchant source labels
- route share in demo contexts
- settlement operations notes

## Dashboard Health States

`fresh` means the cache or source-derived summary is recent enough for the benchmark view.

`stale` means data is available but old enough that it should be interpreted cautiously.

`fallback` or `demo` means the value is from bootstrap/sample data rather than current source data.

`unavailable` means the source did not return usable evidence for that chain/token pair.

## Known Limitations

- BigQuery credentials are required for live EVM validation.
- The public snapshot does not include complete Solana indexer infrastructure.
- Observed on-chain settlement health does not prove off-chain fulfillment.
- Configured demo metadata can influence benchmark presentation.
- Route cost is observed/computable cost, not true facilitator margin.
