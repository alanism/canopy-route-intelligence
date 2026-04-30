# Context Graph Layer Notes

Last updated: 2026-03-20

## Current State

The context graph batch path is the safest BigQuery path operationally because it already uses:

- dry runs
- maximum bytes billed
- background-only execution

It is still partially mixed conceptually because extraction and interpreted graph relationships are built inside one SQL shape.

## Safe Improvements Applied

- auxiliary tables are narrowed by filtered transaction hashes where feasible
- dry run and max-bytes guardrails remain unchanged

## What Remains Intentionally Mixed

- entity matching and edge construction still happen inside the SQL builder
- graph relationship outputs are not yet separated into measured graph facts vs derived graph summaries

## Why This Remains Deferred

- the fee and corridor paths are higher priority
- context graph has the largest blast radius
- current request path is already isolated from BigQuery

## Next Refactor Boundary

The next safe split for context graph is:

1. measured graph extraction
   - raw transfer rows
   - raw log / trace matches
   - raw receipt / gas facts
2. derived graph summarization
   - wallet-wallet edges
   - protocol edges
   - topology classification
   - hub ranking
