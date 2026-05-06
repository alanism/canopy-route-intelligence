# Phase 17 — Solana Corridor Intelligence Product Layer

## Scope

Phase 17 turns the Phase 16.5 shadow evidence into a materialized product-layer artifact that can be served by the API without running BigQuery on the request path.

This phase does not add routing execution, production SLA claims, or dashboard product claims. It exposes the evidence state and its limits.

## Interface

- `scripts/materialize_solana_corridor_intelligence.py`
  - Runs Phase 16.5 shadow validation or reads a saved shadow report.
  - Writes `data/solana_corridor_intelligence.json` by default.
- `GET /v1/solana/corridor-intelligence`
  - Serves only the materialized artifact.
  - Returns unavailable when the artifact is missing.

## Acceptance Evidence

Live materialization from the sandbox table produced:

- `status=degraded`
- `signal_state=cold_start`
- `claim_level=evidence_limited`
- `missing_fields=[]`
- `slot_min=417663784`
- `slot_max=417663784`
- `quality_gates.request_path_bigquery_free=true`

The degraded state is expected because the live sandbox window contains one row and cannot support a seven-day success-purity product signal yet.

## Guardrails

- Request handlers do not run BigQuery, RPC, or shadow validation.
- Cold-start evidence cannot become a production candidate signal.
- Missing S3 fields produce `signal_state=schema_gap`.
- The API response always includes the watched-source scope disclaimer.
