# Data Layer Contract

Last updated: 2026-03-20

## Summary

Canopy uses three data layers:

1. `measured`
2. `derived`
3. `decision`

The contract is strict:

- BigQuery is batch-only.
- Measured queries may return raw facts and unavoidable, explicitly labeled normalization only.
- Measured queries must not emit business meaning.
- Derived code owns aggregations, metrics, heuristics, and freshness rollups.
- Decision code owns route scoring, confidence labels, and UI-facing recommendations.

## Layer Definitions

### Measured

Allowed inputs:

- raw chain events
- raw token transfer facts
- raw transaction facts
- raw receipt facts
- raw log / trace facts
- unavoidable labeled normalization, such as token decimal normalization

Allowed outputs:

- transaction hashes
- timestamps
- raw addresses
- token address
- raw or normalized token value
- raw gas price / gas used / status
- raw graph facts

Forbidden logic:

- heuristics such as `payment_like_*`
- adjusted / direct classifications
- scores
- labels
- route or corridor interpretations
- confidence values
- rolling business metrics
- aggregate summaries such as avg / median / p90
- freshness summaries

### Derived

Allowed inputs:

- measured outputs
- corridor registries / static configuration
- pricing inputs when explicitly modeled
- summary stores built from measured outputs

Allowed outputs:

- metrics
- aggregations
- heuristics
- rollups
- grouped summaries
- freshness summaries
- bridge and whale analysis

Forbidden logic:

- route recommendations
- confidence labels intended for UI consumption
- decision narratives

### Decision

Allowed inputs:

- derived outputs
- corridor configuration
- product rules
- UI / product framing rules

Allowed outputs:

- route scoring
- confidence labels
- recommendation payloads
- UI-facing summaries and narratives

Forbidden logic:

- direct raw data extraction
- direct BigQuery access

## Query Family Audit

| Query family | Current owner | Current status | Target owner | Notes |
| --- | --- | --- | --- | --- |
| Fee path in `data/query.py` | BigQuery + batch poller | `measured + derived` with parity-only legacy reference | measured + derived | Active wrapper uses measured extraction plus Python-derived metrics; legacy mixed SQL remains for parity only. |
| Corridor volume path in `services/corridor_analytics.py` | corridor analytics | `derived` with optional measured extraction fallback | measured + derived | Request path defaults to summary/deterministic output and only uses live BigQuery when explicitly enabled. |
| Context graph edge queries in `services/context_graph/queries.py` | context graph batch path | `mixed` | measured + derived | Extracts graph facts and interpreted graph relationships together. |
| Liquidity gap query | context graph batch path | `derived` | derived | This is a metric query, not a raw extraction query. |

## Non-Negotiable Rules

- If a BigQuery query is producing business meaning instead of raw facts, the layer contract is broken.
- Request handlers must never trigger BigQuery directly.
- New measured queries must be validator-enforced.
- Existing mixed queries remain transitional until parity-backed replacements are switched in.
- Production query families should define explicit `maximum_bytes_billed` caps.
