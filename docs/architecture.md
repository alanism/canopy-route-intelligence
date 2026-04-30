# Architecture

Canopy Route Intelligence is organized around a simple separation of concerns:

```text
Ethereum / Solana data sources
        |
chain-specific ingestion adapters
        |
Project DG normalization layer
        |
validation checks
        |
freshness gates
        |
API
        |
dashboard
```

The implemented BigQuery path currently follows this shape:

```text
Ethereum / Polygon public BigQuery datasets
        |
background ingestion
        |
BigQuery-derived metrics + local summary cache
        |
reconciliation + supply parity checks
        |
FastAPI cache/API
        |
dashboard
```

## Components

Data ingestion reads chain-specific source data outside the request path. This keeps the dashboard responsive and lets the app degrade by chain/token pair when a source is stale or unavailable.

Normalization converts chain-specific transfer evidence into stablecoin route records. Project DG treats each chain as having its own native data shape; it does not assume every chain looks like Ethereum logs.

Validation checks whether records are internally consistent, recent enough to use, and compatible with the configured token and route metadata.

Freshness gates prevent stale data from being presented as current route evidence. Stale data can still appear as demo context, but it should be labeled as degraded or fallback data.

The API presents route intelligence payloads to the dashboard. It does not custody funds or execute payments.

## Why The Layers Are Separate

Ingestion, normalization, validation, and presentation are intentionally separate so that chain-specific parsing can change without rewriting the dashboard, validation can reject questionable records before they become product claims, and the API can expose conservative benchmark data without depending on live queries during user requests.
