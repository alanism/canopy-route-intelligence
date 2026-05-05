# Solana S3-Readiness Field Checklist

Generated for Phase 16 from `services.solana.bigquery_writer.S3_READINESS_FIELDS` and `BQ_SCHEMA`.

## Present Fields

- `chain`
- `signature`
- `slot`
- `block_time`
- `token_mint`
- `watched_address`
- `source_token_account`
- `destination_token_account`
- `source_owner`
- `destination_owner`
- `amount_raw`
- `amount_decimal`
- `amount_transferred_raw`
- `amount_received_raw`
- `fee_lamports`
- `jito_tip_lamports`
- `total_native_observed_cost_lamports`
- `transaction_success`
- `observed_transfer_inclusion`
- `settlement_evidence_type`
- `validation_status`
- `raw_event_id`
- `normalized_event_id`
- `event_fingerprint`
- `collision_detected`
- `alt_resolution_status`
- `owner_resolution_status`
- `amount_resolution_status`
- `ingested_at`

## Missing Fields

None.

## Notes

- This checklist verifies field availability only. Phase 16.5 shadow signal validation must still verify internal-only shadow views and no leakage to external surfaces.
- BigQuery DDL is generated from `BQ_SCHEMA`; do not duplicate schema definitions in deployment tooling.
