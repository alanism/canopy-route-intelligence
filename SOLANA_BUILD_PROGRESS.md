# Solana Build Progress — Canopy Route Intelligence
**Build Plan:** v2.2 Final Execution Draft  
**Last Updated:** 2026-05-05  
**Test Suite:** `tests/solana/` + `tests/test_solana_api_endpoints.py`

---

## Status Summary

| Layer | Status | Tests |
|-------|--------|-------|
| Phase 0 — Parser + Cost Integrity | ✅ Complete | 42 passed |
| Phase 1 — RPC Ingestion Adapter | ✅ Complete | (covered by integration tests) |
| Phase 2 — Persistent Checkpointing | ✅ Complete | (covered by integration tests) |
| Phase 3 — Retry / Rate Limit / Circuit Breaker | ✅ Complete | (covered by integration tests) |
| Phase 4 — Normalized Solana Event Schema + BigQuery Writer | ✅ Complete | 57 passed |
| Phase 5 — Owner + Amount Resolution | ✅ Complete | 36 passed |
| Phase 6 — Validation + Reconciliation | ✅ Complete | 42 passed |
| Phase 7 — Freshness + Health State Machine | ✅ Complete | 29 passed |
| Phase 8 — API + Cache Integration | ✅ Complete | 35 passed |
| Phase 9 — Dashboard Integration | ✅ Complete | 18 passed |
| Phase 10 — Documentation | ✅ Complete | — |
| Phase 11 — Burn-In Gates | 🔲 Not started | — |

**Total: 329 passed, 0 failed across all Solana tests**

---

## Phase 0 — Solana Parser + Cost Integrity Layer ✅

> Gate rule: do not proceed to Phase 1 until all 19 build-plan tests pass.  
> **Status: GATE PASSED — 42/42 tests green.**

### Sub-phases

| Sub-phase | Module | Status |
|-----------|--------|--------|
| 0A — Transaction Pre-Normalizer | `services/solana/pre_normalizer.py` | ✅ |
| 0B — ALTManager (two-layer cache + RPC client) | `services/solana/alt_manager.py`<br>`services/solana/rpc_client.py` | ✅ |
| 0C — Transfer Truth Rule | `services/solana/transfer_truth.py` | ✅ |
| 0D — Jito Tip Detector | `services/solana/jito_detector.py` | ✅ |
| 0E — Cost Decomposition | `services/solana/cost_decomposition.py` | ✅ |
| 0F — Canonical Key + Collision Defense | `services/solana/canonical_key.py` | ✅ |
| 0G — Token-2022 / Transfer Fee Handling | `services/solana/token_program.py` | ✅ |
| 0H — Parser Acceptance Gates (test suite) | `tests/solana/test_solana_parser.py` | ✅ |

### Phase 0H Acceptance Gate Tests (all 19 required + 3 bonus)

| # | Test | Status |
|---|------|--------|
| 1 | Legacy transaction with simple SPL transfer | ✅ |
| 2 | v0 transaction with Address Lookup Table resolves all accounts | ✅ |
| 3 | ALT persistent cache miss → RPC fetch → cache written | ✅ |
| 4 | ALT persistent cache hit avoids RPC across runs | ✅ |
| 5 | ALT ProcessingCache hit within one run | ✅ |
| 6 | Multiple versioned txs same ALT → fetched once per run | ✅ |
| 7 | Unresolved ALT marks degraded, blocks healthy promotion | ✅ |
| 8 | Top-level transfer uses `inner_instruction_index = -1` | ✅ |
| 9 | Inner SPL transfer detected and resolved | ✅ |
| 10 | Multiple inner transfers in one transaction | ✅ |
| 11 | Failed transaction: fee counted, no transfer inclusion | ✅ |
| 12 | Successful transaction: no watched-token movement → no inclusion | ✅ |
| 13 | Token-2022-style fixture with extra inner instructions — no crash | ✅ |
| 14 | Top-level Jito tip fixture detected | ✅ |
| 15 | Inner-instruction Jito tip fixture detected | ✅ |
| 16 | Unrelated SOL transfer not counted as Jito tip | ✅ |
| 17 | High-volume USDC: exact 6-decimal precision via `decimal.Decimal` | ✅ |
| 18 | Decimal output matches raw ÷ decimals exactly (no float) | ✅ |
| 19 | No Solana path uses `tx_hash + log_index` identity | ✅ |
| B1 | Multiple ALTs in one transaction — each fetched once | ✅ |
| B2 | `collision_defense` assigns ordinals on duplicate canonical key | ✅ |
| B3 | Priority fee not double-counted in total cost | ✅ |

### Key Implementation Notes

- `inner_instruction_index = -1` for top-level instructions (never 0 — documented REF-01 trap)
- `decimal.Decimal` mandatory for all token math; `float` prohibited in Solana path
- Canonical key format: `solana:{signature}:{instruction_index}:{inner_instruction_index}`
- `transaction_success != transfer_success` — enforced in `transfer_truth.py`
- ALT resolution: `None` pre-balance treated as `0` for newly-created ATAs
- Two-layer ALT cache: `ProcessingCache` (run-scoped in-memory) → `PersistentALTCache` (file: `data/solana_alt_cache.json`)
- Persistent cache uses SHA-256 checksum + atomic write (`os.replace`)
- ALT resolution failure → `resolve_transaction_loaded_addresses` returns `None` → transaction marked degraded
- `ALTFetcher` Protocol decouples `ALTManager` from `SolanaRPCClient` for test injection
- Jito tips: SOL transfers to 8 pinned tip accounts, separate from `fee_lamports`
- `total_native_observed_cost = fee_lamports + jito_tip_lamports + explicit_tip_lamports`

---

## Phase 1 — Scoped RPC Ingestion Adapter 🔲

**Difficulty: 🟡 Medium**  
**Prerequisite: Phase 0 gate passed ✅**

### Goal
Ingest scoped observed Solana USDC activity from configured watched sources.

### What to build
- `services/solana/ingestion_adapter.py`
- `getSignaturesForAddress` → pagination loop
- Per-signature `getTransaction` fetch
- Route through `ALTManager` → `normalize_transaction` pipeline
- Respect `SOLANA_MAX_SIGNATURES_PER_RUN` + `SOLANA_MAX_TRANSACTIONS_PER_RUN` caps
- Emit pre-normalized events for downstream phases

### Required env config
```
SOLANA_RPC_PRIMARY_URL
SOLANA_RPC_FALLBACK_URL
SOLANA_WATCHED_ADDRESSES
SOLANA_TOKEN_MINT_ALLOWLIST
SOLANA_START_SLOT
SOLANA_START_SIGNATURE
SOLANA_MAX_SIGNATURES_PER_RUN
SOLANA_MAX_TRANSACTIONS_PER_RUN
SOLANA_MAX_INNER_INSTRUCTIONS_PER_TX
```

---

## Phase 2 — Persistent Checkpointing 🔲

**Difficulty: 🟡 Medium**

- Last processed signature + slot persisted to `data/solana_checkpoint.json`
- Resume-safe: replay from last checkpoint on restart
- Checkpoint written only after successful batch promotion (not before)
- Never advance checkpoint past unvalidated data

---

## Phase 3 — Retry / Rate Limit / Circuit Breaker 🔲

**Difficulty: 🟡 Medium**  
**Note: RPC client retry/backoff already built in `rpc_client.py` (3 attempts, 2s/4s/8s, ±10% jitter). Phase 3 adds circuit breaker + rate limiter around the ingestion loop.**

- Circuit breaker: open after N consecutive RPC failures; half-open probe
- Rate limiter: respect `SOLANA_MAX_RPS` config
- Graceful degradation: mark pipeline degraded, do not crash

---

## Phase 4 — Normalized Solana Event Schema 🔲

**Difficulty: 🟡 Medium**

- Final event dict matching 14-field canonical schema (from Project DG)
- BigQuery row writer for `solana_measured` table
- Schema: signature, slot, block_time, chain, token_mint, source_account, destination_account, instruction_index, inner_instruction_index, amount_raw, amount_decimal, fee_lamports, jito_tip_lamports, cost_total_lamports, settlement_evidence_type, observed_transfer_inclusion, validation_status, canonical_key, ingested_at
- `BIGNUMERIC` for all amounts; no `FLOAT64` in Solana columns
- Partition on `slot` or `block_time`

---

## Phase 5 — Owner + Amount Resolution 🔲

**Difficulty: 🔴 Hard**

- Resolve `accountIndex` → token account pubkey → owner pubkey
- Map token account → mint for instruction-based evidence
- Required for full `source_token_account` / `destination_token_account` resolution
- Must handle: account not in static keys, account in ALT-resolved keys, account newly created in same transaction

---

## Phase 6 — Validation + Reconciliation 🔲

**Difficulty: 🟡 Medium**

### Batch promotion gates (all must pass before promoting to derived layer)
- Row count consistency check
- Decimal precision gate passes
- ALT `ProcessingCache` gate passes
- No unresolved account keys in validated set
- No float in Solana amount columns
- `observed_transfer_inclusion` matches `transfer_detected` logic
- Reconciliation sample: `VALIDATION_SAMPLE_SIZE = 20` transactions spot-checked

---

## Phase 7 — Freshness + Health State Machine 🔲

**Difficulty: 🟡 Medium**

- Track ingestion lag: `now - last_slot_time`
- Freshness states: `fresh` / `stale` / `unavailable`
- Health gate: stale data shown as degraded, never green
- Stale threshold config: `SOLANA_FRESHNESS_THRESHOLD_SECONDS`
- Zero Solana data → `unavailable`, never green

---

## Phase 8 — API + Cache Integration 🔲

**Difficulty: 🟡 Medium**

- Solana route metrics served from existing API shape
- Cache layer respects Solana freshness state
- Solana endpoints return `unavailable` when no data, never cached stale-as-fresh
- Ethereum/Polygon behavior unchanged

---

## Phase 9 — Dashboard Integration 🔲

**Difficulty: 🟢 Easy** (schema + API already done by Phase 8)

- Solana row appears beside Ethereum/Polygon
- Freshness label visible
- Degraded/unavailable states visually distinct from green
- No hardcoded or simulated Solana values in dashboard
- Scope disclaimer rendered where required

---

## Phase 10 — Documentation 🔲

**Difficulty: 🟢 Easy**

- Update `docs/solana-integration.md` with actual implemented status
- Document: watched-address scope, data limitations, freshness behavior
- Required scope statement on dashboard:
  > "Solana data reflects observed SPL token movements within configured watched sources and measured windows."

---

## Phase 11 — Burn-In Gates 🔲

**Difficulty: 🟡 Medium**

### First-Slice Gate
- 100-slot deterministic ingestion fixture
- Zero parser crashes
- Zero unresolved account keys in fixture set
- Jito tip detector passes fixture
- ALT resolver passes fixture
- `ProcessingCache` avoids duplicate ALT RPC calls
- Decimal precision gate passes
- No stale/degraded data shown as healthy

### Demo Readiness Gate
- Dashboard shows Solana data from at least one live observed transaction
- Freshness label correct
- Scope disclaimer visible
- No product-language violations

### Production Confidence Gate
- 1,000-transaction ingestion run without parser crash
- Reconciliation passes on sample
- Circuit breaker tested under simulated RPC failure
- Ethereum/Polygon behavior confirmed unchanged

---

## Acceptance Gates Summary

| Gate | Condition | Status |
|------|-----------|--------|
| Gate 1 — Phase 0 Parser Integrity | All 19 build-plan tests pass | ✅ |
| Gate 2 — ALT ProcessingCache | Same ALT fetched at most once per run | ✅ |
| Gate 3 — Decimal Precision | All amounts via `Decimal`, no float, exact 6-decimal match | ✅ |
| Gate 4 — Source + Scope | Watched-address config gates data | 🔲 Phase 1 |
| Gate 5 — Cost Integrity | Total cost = fee + jito + explicit tip (no double-count) | ✅ |
| Gate 6 — Validation | Batch promotion gates pass | 🔲 Phase 6 |
| Gate 7 — Freshness | Stale data never shown as healthy | 🔲 Phase 7 |
| Gate 8 — Product Language | No banned language in live dashboard | 🔲 Phase 9 |

---

## Files Created

```
services/solana/
├── __init__.py
├── constants.py          (USDC/PYUSD/USDT mints, token programs, Jito tip accounts)
├── pre_normalizer.py     (Phase 0A)
├── alt_manager.py        (Phase 0B — ALTManager, ProcessingCache, PersistentALTCache)
├── rpc_client.py         (Phase 0B — SolanaRPCClient, RPCError, client_from_env)
├── transfer_truth.py     (Phase 0C)
├── jito_detector.py      (Phase 0D)
├── cost_decomposition.py (Phase 0E)
├── canonical_key.py      (Phase 0F)
└── token_program.py      (Phase 0G)

tests/solana/
├── __init__.py
└── test_solana_parser.py (42 tests — all Phase 0 acceptance gates)

data/
└── solana_alt_cache.json (auto-created on first ALT RPC fetch)
```
