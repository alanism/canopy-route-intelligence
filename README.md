# Canopy Route Intelligence

A benchmark-grade stablecoin route intelligence layer for agentic payments.

Canopy Route Intelligence exposes observed route cost, route share, freshness, and observed on-chain settlement-health signals through a dashboard and API. The technical engine underneath the demo is Project DG, which validates and normalizes stablecoin data into a comparable route intelligence model.

## What Was Built

- Route intelligence dashboard
- FastAPI demo API
- Project DG validation and normalization layer
- Ethereum stablecoin data model for USDC, USDT, and PYUSD
- Solana normalization design documented for first-class chain handling
- Observed route cost benchmark
- Route share and corridor comparison payloads
- Freshness monitoring and cache health states
- Observed on-chain settlement-health signals
- Sample dataset and demo persistence layer

## Why Solana

Solana stablecoin transfer data is not treated as Ethereum-style logs. Project DG normalizes Solana-native transaction structure -- signatures, slots, token mints, accounts, instructions, and inner instructions -- into a comparable stablecoin route intelligence model.

The current codebase contains the working EVM/BigQuery path and documents the Solana-specific adapter contract for the hackathon version. See [docs/solana-integration.md](docs/solana-integration.md).

## Architecture

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

For the implemented BigQuery path:

```text
Ethereum / Polygon BigQuery public datasets
        |
background ingestion
        |
validated summary tables and local cache
        |
reconciliation + parity checks
        |
FastAPI
        |
dashboard
```

## Run Locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn api.main:app --reload --port 8000
```

Open [http://localhost:8000](http://localhost:8000).

## Environment

Copy `.env.example` to `.env` and fill in local values only. Do not commit `.env`, service account JSON, private keys, provider keys, OAuth credentials, or local machine paths.

BigQuery is optional for a local demo. Without configured credentials, the app can still run from demo/bootstrap data, but live validation and audit commands will be limited.

## Demo Data

The repository uses sample/demo corridor and settlement data. Fields that describe operator readiness, payout capacity, beneficiary review, or workflow status are illustrative demo values. They are not counterparty-specific data and should not be interpreted as production merchant evidence.

## API Surface

| Method | Path | Description |
| --- | --- | --- |
| GET | `/` | Dashboard |
| GET | `/health` | Cache and service health |
| GET | `/v1/client-config` | Browser-safe runtime config |
| GET | `/v1/demo/presets` | Demo corridor presets |
| GET | `/v1/landscape` | Stablecoin landscape summary |
| POST | `/v1/route` | Token-scoped route benchmark |
| GET | `/v1/corridor/{slug}/graph` | Cached context graph snapshot |
| GET | `/v1/system/bigquery-metrics` | BigQuery drift metrics and summaries |
| POST | `/v1/demo/export` | Printable demo summary |
| POST | `/v1/demo/decision-receipt` | Plain-text benchmark receipt |
| POST | `/v1/scenarios` | Create saved demo scenario |
| GET | `/v1/scenarios/{id}` | Read saved demo scenario |
| POST | `/v1/scenarios/{id}/review` | Review scenario state |
| POST | `/v1/simulate` | USDC-only simulation endpoint |

The `/v1/demo/*` route names expose sanitized demo/sample payloads for the public hackathon snapshot.

## Limitations

This project does not custody funds.
This project does not execute payments.
This project does not claim true facilitator margin.
This project does not claim full production autonomous routing.
This project does not claim complete off-chain x402 payment visibility.
This project benchmarks observed stablecoin route behavior from available chain data.

Observed route cost means cost visible or computable from available chain data and configured route metadata. It is not the same as true facilitator margin.

Observed on-chain settlement health means whether available on-chain settlement evidence appears complete, timely, and internally consistent. It does not prove off-chain API delivery, merchant fulfillment, or full x402 resource delivery.

## Hackathon Notes

Prioritized:

- conservative benchmark framing
- Project DG normalization and validation documentation
- dashboard/API legibility for judges
- freshness-gated route intelligence
- Ethereum stablecoin data and EVM route comparison
- Solana-specific normalization design
- secret and private-reference sanitation

Cut from this hackathon version:

- custody
- payment execution
- production autonomous routing
- full off-chain x402 visibility
- production SLA claims
- private counterparty workflows or datasets
- complete Solana indexer implementation

## Repo Status

This repository is a public hackathon benchmark prototype and demo API. It is not a production payment router or a custody system.

## More Docs

- [Architecture](docs/architecture.md)
- [Methodology](docs/methodology.md)
- [Data Quality](docs/data-quality.md)
- [Solana Integration](docs/solana-integration.md)
- [Sanitization Report](SANITIZATION_REPORT.md)

## License

License TBD.
