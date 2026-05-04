# Solana Integration

Solana requires first-class chain-specific handling because stablecoin movement is represented through Solana transaction and account structure, not Ethereum-style ERC-20 logs.

Solana stablecoin transfer data is not treated as Ethereum-style logs. Canopy Route Intelligence normalizes Solana-native transaction structure — signatures, slots, token mints, accounts, instructions, and inner instructions — into a comparable stablecoin route intelligence model.

---

## Implementation Status

| Phase | Module | Status |
|-------|--------|--------|
| 0 — Parser + Cost Integrity | `services/solana/pre_normalizer.py`, `alt_manager.py`, `transfer_truth.py`, `jito_detector.py`, `cost_decomposition.py`, `canonical_key.py`, `token_program.py` | ✅ Complete |
| 1 — Scoped RPC Ingestion Adapter | `services/solana/ingestion_adapter.py` | ✅ Complete |
| 2 — Persistent Checkpointing | `services/solana/checkpoint.py` | ✅ Complete |
| 3 — Rate Limiter + Circuit Breaker | `services/solana/circuit_breaker.py` | ✅ Complete |
| 4 — Normalized Event Schema + BigQuery Writer | `services/solana/event_schema.py`, `bigquery_writer.py` | ✅ Complete |
| 5 — Owner + Amount Resolution | `services/solana/owner_resolver.py` | ✅ Complete |
| 6 — Validation + Reconciliation | `services/solana/validator.py` | ✅ Complete |
| 7 — Freshness + Health State Machine | `services/solana/freshness.py` | ✅ Complete |
| 8 — API + Cache Integration | `services/solana/api_integration.py` | ✅ Complete |
| 9 — Dashboard Integration | `api/main.py`, `ui/index.html` | ✅ Complete |

---

## Ethereum vs Solana

Ethereum ERC-20 analysis commonly starts from `Transfer(address,address,uint256)` logs emitted by token contracts. Those logs have a familiar contract address, topics, data payload, block number, transaction hash, and log index.

Solana analysis starts from transaction records. A normalized transfer requires: the transaction signature, slot, block time, token mint, source token account, destination token account, owner pubkeys for each account, instruction index, and inner instruction index.

Key structural differences:

| Concept | Ethereum | Solana |
|---------|----------|--------|
| Transfer identifier | `tx_hash + log_index` | `signature + instruction_index + inner_instruction_index` |
| Token account model | Contract owns balance; address = wallet | Separate ATA per (wallet, mint) pair; wallet ≠ token account |
| Fee structure | Gas (base + priority) | Lamport fee + optional Jito tip + optional priority fee |
| Inner call evidence | EVM traces | `innerInstructions` in transaction metadata |
| Account resolution | All accounts explicit | v0 transactions may use Address Lookup Tables (ALTs) |

---

## Solana Primitives

- **`signature`** — transaction identifier (base58, ~88 chars); used for traceability and as the primary cursor
- **`slot`** — ledger position; used for ordering and checkpoint cursors
- **`block_time`** — Unix timestamp (seconds); may be `null` for very recent slots
- **`token_mint`** — stablecoin mint address (e.g. USDC: `EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v`)
- **`source_token_account`** / **`destination_token_account`** — SPL associated token accounts (ATAs); distinct from owner wallets
- **`source_owner`** / **`destination_owner`** — wallet pubkeys that control the ATAs; resolved in Phase 5
- **`instruction_index`** — outer instruction position (0-based)
- **`inner_instruction_index`** — inner instruction position (0-based); `-1` for top-level instructions (never `0`)
- **`program_id`** — the executing program (Token Program, Token-2022, or DEX program)

---

## Watched-Address Scope

**This is the most important data limitation to understand.**

Solana ingestion is scoped to a set of watched addresses configured via `SOLANA_WATCHED_ADDRESSES` (comma-separated pubkeys). The ingestion pipeline calls `getSignaturesForAddress` for each watched address and fetches only those transactions.

**What this means:**
- Only transactions where a watched address appears as a participant are ingested
- Transactions between two non-watched accounts are invisible to the pipeline
- The ingestion does not scan the full Solana mempool or block history

**What this does not mean:**
- "Watched address" does not mean only transactions *from* that address — any transaction where the address appears in the account keys (as source, destination, or fee payer) is fetched

Required dashboard disclosure:
> "Solana data reflects observed SPL token movements within configured watched sources and measured windows."

---

## Data Flow

```
getSignaturesForAddress (RPC)
        ↓
getTransaction (RPC, encoding=json, self-parse)
        ↓
ALTManager (resolve v0 Address Lookup Tables)
        ↓
pre_normalizer → transfer_truth → jito_detector → cost_decomposition
        ↓
normalize_event (Phase 4 — 44-field canonical schema)
        ↓
apply_owner_and_amount_resolution (Phase 5 — owner lookup + balance delta)
        ↓
validate_batch (Phase 6 — 7 promotion gates)
        ↓
SolanaEventWriter → BigQuery (or local JSONL fallback)
        ↓
checkpoint.advance(promoted=True)
        ↓
SolanaCache.record_run → FreshnessMonitor.record_slot
        ↓
API (/v1/solana/health) → Dashboard
```

---

## Canonical Event Schema (44 fields)

Every normalized Solana event has exactly 44 fields. Missing source data produces `None`, never `KeyError`. The schema is validated by `validate_normalized_event()` which returns a list of missing field names.

**Amount rules (non-negotiable):**
- Raw amounts: `int` (u64-compatible)
- Decimal amounts: `decimal.Decimal` — never `float`
- `amount_decimal = Decimal(amount_raw) / Decimal(10 ** decimals)`
- BigQuery target type: `BIGNUMERIC` for raw amounts, `NUMERIC` for decimal

**Key fields:**

| Field | Type | Description |
|-------|------|-------------|
| `chain` | `str` | Always `"solana"` |
| `signature` | `str` | Transaction signature |
| `slot` | `int` | Ledger slot |
| `block_time` | `int` | Unix timestamp |
| `token_mint` | `str` | SPL token mint address |
| `source_token_account` | `str` | Source ATA |
| `destination_token_account` | `str` | Destination ATA |
| `source_owner` | `str\|None` | Wallet controlling source ATA |
| `destination_owner` | `str\|None` | Wallet controlling destination ATA |
| `instruction_index` | `int` | Outer instruction position |
| `inner_instruction_index` | `int` | Inner instruction position; `-1` for top-level |
| `transfer_ordinal` | `int` | Collision defense ordinal within same canonical key |
| `amount_raw` | `int` | Settled token amount in base units |
| `amount_decimal` | `Decimal` | Human-readable amount |
| `amount_transferred_raw` | `int\|None` | Source-side amount (may differ from received for Token-2022 fee) |
| `fee_withheld_raw` | `int\|None` | Token-2022 transfer fee withheld in transit |
| `fee_lamports` | `int` | Network fee in lamports |
| `jito_tip_lamports` | `int` | Jito tip amount |
| `raw_event_id` | `str` | `solana:{signature}:{ix}:{inner_ix}` |
| `normalized_event_id` | `str` | Collision-safe: `raw_event_id:{fingerprint[:8]}` |
| `event_fingerprint` | `str` | SHA-256 of program+mint+accounts+amount+data |
| `collision_detected` | `bool` | True if another event shares same raw_event_id |
| `validation_status` | `str` | `"ok"` \| `"degraded"` \| `"failed"` |
| `owner_resolution_status` | `str` | `"ok"` \| `"degraded"` \| `"pending"` |
| `amount_resolution_status` | `str` | `"ok"` \| `"degraded"` \| `"pending"` |
| `ingested_at` | `str` | ISO8601 UTC timestamp |

---

## Freshness States

The `FreshnessMonitor` produces a three-state signal based on the lag between the last ingested on-chain block time and wall clock:

| State | Condition | API behavior | Dashboard |
|-------|-----------|--------------|-----------|
| `fresh` | `lag ≤ SOLANA_FRESHNESS_THRESHOLD_SECONDS` (default: 300s) | Served as live data | Green label |
| `stale` | `threshold < lag ≤ SOLANA_STALE_THRESHOLD_SECONDS` (default: 3600s) | Served with `status: "degraded"` | Amber label + scope disclaimer |
| `unavailable` | No data ingested OR `lag > stale_threshold` | `status: "unavailable"` | Gray label + scope disclaimer |

**Invariants:**
- Zero Solana data → always `unavailable`. Never green.
- Stale data is always labeled as degraded. Never silently served as fresh.
- The freshness state transitions automatically as time passes — no new ingestion needed to become stale.

---

## Validation Gates (Phase 6)

Before any batch is promoted to the derived layer, all 7 gates must pass:

1. **Row count** — batch size matches expected ingestion count
2. **No float amounts** — all 11 amount fields are `int` or `Decimal`
3. **Decimal precision** — `amount_decimal` has ≤ 9 decimal places (BigQuery NUMERIC limit)
4. **No placeholder accounts** — no `__account_index_N__` strings remain in account fields
5. **Required fields** — all 44 canonical fields present in every row
6. **Transfer truth consistency** — if `transfer_detected=True` then `observed_transfer_inclusion=True`
7. **Reconciliation sample** — spot-check first 20 events for `amount_raw == amount_received_raw`, known `validation_status`, non-empty canonical IDs

A single gate failure rejects the entire batch. Use `validate_batch()` for observable failure reporting or `assert_batch_approved()` to halt the pipeline on rejection.

---

## Checkpointing

The checkpoint cursor (`checkpoint.py`) tracks ingestion progress per `(chain, token_mint, watched_address)` triple:

- **`last_processed_signature`** + **`last_processed_slot`** — together form the cursor; never slot alone
- **`advance(promoted=True)`** — only after BigQuery promotion; never speculatively
- **`mark_failed()`** — sets `ingestion_status="failed"` without advancing the cursor
- **Never scan from genesis** — a missing checkpoint with no configured start raises `MissingCheckpointError`
- **Atomic writes** — `os.replace()` on every flush; no partial checkpoint corruption

---

## Resilience

### Circuit Breaker
`CircuitBreaker` wraps the ingestion loop, not individual RPC calls. States: `CLOSED` (normal) → `OPEN` (tripped after N consecutive failures) → `HALF_OPEN` (testing recovery after cooldown). `CircuitOpenError` breaks the run cleanly without a crash.

### Rate Limiter
Token-bucket `RateLimiter` governs RPC call rate. `acquire()` sleeps the exact deficit seconds. Configure via `SOLANA_RPC_MAX_RPS` and `SOLANA_RPC_BURST_LIMIT`.

### Fallback Buffer
When no BigQuery client is available, `SolanaEventWriter` writes to `data/solana_events_buffer.jsonl`. Ingestion continues unblocked during development. Degraded data is better than lost data.

### Owner Cache
`OwnerCache` (`data/owner_cache.json`) persists owner resolution results across runs. Prevents repeated `getAccountInfo` calls for the same ATA. Atomic flush via `os.replace()`.

---

## API Endpoints

### `GET /v1/solana/health`

Returns the current Solana data layer state:

```json
{
  "chain": "Solana",
  "freshness_state": "fresh",
  "lag_seconds": 45.2,
  "ingestion_lag_seconds": 12.1,
  "freshness_threshold_seconds": 300,
  "stale_threshold_seconds": 3600,
  "last_slot": 300000000,
  "last_block_time": 1700000000,
  "last_ingested_at": "2026-05-05T00:00:00+00:00",
  "last_run_status": "ok",
  "last_run_at": "2026-05-05T00:00:00+00:00",
  "signatures_fetched": 50,
  "transactions_processed": 48,
  "transactions_degraded": 2,
  "events_written": 46,
  "last_validation_status": "approved",
  "last_validation_at": "2026-05-05T00:00:00+00:00",
  "last_error": null,
  "chain_health": {
    "status": "fresh",
    "freshness_state": "fresh",
    "freshness_level": "fresh",
    "cache_age_seconds": 45,
    "last_slot": 300000000,
    "last_run_status": "ok",
    "events_written": 46,
    "last_error": null
  },
  "scope_disclaimer": "Solana data reflects observed SPL token movements within configured watched sources and measured windows."
}
```

The `chain_health` sub-key mirrors the Polygon/Ethereum chain health structure in `GET /health`.

### `GET /health` (extended)

The existing `/health` response now includes a `"Solana"` entry in its `chains` dict alongside `"Polygon"` and `"Ethereum"`.

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SOLANA_RPC_PRIMARY_URL` | — | **Required.** Primary RPC endpoint |
| `SOLANA_RPC_FALLBACK_URL` | — | Optional fallback RPC endpoint |
| `SOLANA_WATCHED_ADDRESSES` | — | **Required.** Comma-separated pubkeys to watch |
| `SOLANA_TOKEN_MINT` | USDC mainnet | Token mint address to filter |
| `SOLANA_START_SIGNATURE` | — | Optional bootstrap cursor (signature) |
| `SOLANA_START_SLOT` | — | Optional bootstrap cursor (slot) |
| `SOLANA_CHECKPOINT_PATH` | `data/solana_checkpoint.json` | Checkpoint file path |
| `SOLANA_ALT_CACHE_PATH` | `data/solana_alt_cache.json` | ALT persistent cache path |
| `SOLANA_OWNER_CACHE_PATH` | `data/owner_cache.json` | Owner resolution cache path |
| `SOLANA_FRESHNESS_THRESHOLD_SECONDS` | `300` | Fresh → stale transition (seconds) |
| `SOLANA_STALE_THRESHOLD_SECONDS` | `3600` | Stale → unavailable transition (seconds) |
| `SOLANA_RPC_MAX_RPS` | `10` | Rate limiter: max requests per second |
| `SOLANA_RPC_BURST_LIMIT` | `20` | Rate limiter: burst capacity |
| `SOLANA_CIRCUIT_MAX_FAILURES` | `5` | Circuit breaker: consecutive failure threshold |
| `SOLANA_CIRCUIT_COOLDOWN_SECONDS` | `60` | Circuit breaker: cooldown before HALF_OPEN |
| `SOLANA_BQ_DATASET` | `solana_measured` | BigQuery dataset |
| `SOLANA_BQ_TABLE` | `solana_transfers` | BigQuery table |

---

## Known Limitations

- **Watched-address scope only** — transfers between non-watched accounts are invisible
- **No backfill** — the pipeline ingests forward from the checkpoint cursor; historical replay is not implemented
- **Token-2022 fee_withheld_raw** — computed from balance deltas when available; may be `None` for standard SPL tokens
- **Owner resolution degrades gracefully** — if `getAccountInfo` fails or the account is closed, `source_owner` / `destination_owner` are `None` with `owner_resolution_status="degraded"`; the row is still written
- **Block time may be null** — very recent slots sometimes have `block_time=null` in the RPC response; freshness lag cannot be computed without block_time
- **No Solana data → unavailable** — the monitor starts in `unavailable` and stays there until the first successful ingestion run; never inferred or estimated
