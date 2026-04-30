# Context Graph Layer Map

Last updated: 2026-03-21

## Purpose

This file makes the current context-graph ownership explicit while the path retains one documented transitional exception.

## Current Behavior

The context-graph pipeline is batch-only and much cleaner than before:

- BigQuery query builders extract raw transfer-linked facts plus raw contract-address relationship evidence
- SQL still narrows auxiliary evidence through tracked registry contract addresses
- Python graph code now performs registry matching, named entity assignment, edge labeling, topology, confidence, hub rankings, and signal summaries

## Current Ownership

### BigQuery-side raw or near-raw facts

- filtered transfers scoped by token and time window
- filtered transaction hashes
- filtered logs and traces narrowed by those hashes
- raw destination contract evidence
- raw log and trace evidence linked to tracked contract addresses
- raw wallet-to-contract and contract-to-contract relationship facts using addresses, not named entities

### BigQuery-side residual transitional behavior

- tracked-contract narrowing still depends on repo-managed registry addresses in SQL
- this is now the final documented transitional exception

### Python-side derived outputs

- protocol and bridge registry matching
- named entity assignment
- edge labeling
- grouped edge summaries
- grouped transaction counts
- grouped total volume
- grouped average gas fee
- topology
- topology classification
- confidence score
- flow density
- protocol noise ratio
- bridge usage rate
- counterparty entropy
- liquidity hubs
- evidence stack

## Audit Interpretation

The context-graph serving payload should currently be treated as:

- `data_layer=derived`
- `serving_path=in_memory_snapshot`
- `query_layer_status=mixed_transitional`

This is accurate even when the upstream query is cost-safe and batch-only, because the SQL still emits interpreted graph relationships rather than raw graph facts only.
This remains accurate even though the largest prior source of interpretation has moved out of SQL, because tracked-contract narrowing still happens before Python assembly.

## Target State

Longer term, the target split is:

1. measured graph extraction
   - raw transfer-linked addresses
   - raw transaction hashes
   - raw protocol/bridge evidence rows

2. derived graph summaries
   - registry matching
   - entity naming and typing
   - grouped edges
   - counts
   - volume rollups
   - signal summaries
   - topology and confidence

## What This Phase Changes

This phase moves registry matching and edge labeling out of SQL and into Python.

The remaining mixed ownership is the SQL-side narrowing through tracked registry addresses. That exception is now explicit in code and audit docs so future contributors cannot mistake the current graph snapshot for a measured-layer artifact.
