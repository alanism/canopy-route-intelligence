# Solana Build — Learning Log
**Project:** Canopy Route Intelligence — Solana Ingestion Layer  
**Build Plan:** v2.2 Final Execution Draft  
**Started:** 2026-05-01  
**Last Updated:** 2026-05-04

---

## Purpose

This log captures key decisions, traps, fixes, and non-obvious design choices made during the Solana build. It is written for the next engineer (or next session) who needs to understand *why* the code is the way it is — not just *what* it does.

---

## Phase 0 — Parser + Cost Integrity Layer

### 0A — Transaction Pre-Normalizer

**What it does:** Converts a raw `getTransaction` JSON response into a flat, resolved dict. All `programIdIndex` and account index references are replaced with actual pubkey strings.

**Key decision — `inner_instruction_index = -1` for top-level instructions**

The REF-01 document explicitly warned about this trap. Top-level instructions get `inner_instruction_index = -1`, never `0`. Inner instructions start at `0`. This is critical for the canonical key (`solana:{sig}:{ix}:{inner}`). Using `0` for top-level would cause key collisions with the first inner instruction of every transaction.

**Key decision — `resolved_loaded_addresses` parameter is optional**

The pre-normalizer works without ALT resolution. If called without `resolved_loaded_addresses`, it sets `alt_resolution_status = "pending_alt_manager"` and uses provider-loaded addresses as a structural scaffold. This lets Phase 0A stand alone in tests without Phase 0B being wired in. When ALTManager provides resolved addresses, it sets `alt_resolution_status = "ok"`.

**Key decision — legacy vs v0 detection**

`transaction_version = "legacy"` when `version` field is absent or `"legacy"`. v0 transactions have `version = 0` (integer). ALT resolution is only attempted for v0.

---

### 0B — ALTManager

**What it does:** Resolves Address Lookup Tables for v0 transactions using a two-layer cache.

**The ALT problem:** v0 transactions compress account keys by referencing on-chain lookup tables. Without resolving those tables, `programIdIndex` values point to the wrong accounts — or no accounts at all. The RPC response for a v0 transaction includes `meta.loadedAddresses` (provider-resolved), but the self-parse rule requires us to verify and own the resolution rather than trusting provider output blindly.

**Two-layer cache design:**
1. `ProcessingCache` — run-scoped in-memory dict. Prevents duplicate `getAccountInfo` calls within one ingestion run. If 1,000 txs reference the same 3 ALTs, RPC is called exactly 3 times.
2. `PersistentALTCache` — file-based JSON at `data/solana_alt_cache.json`. Survives restarts. Validated by SHA-256 checksum on read. Atomic write via `.tmp` + `os.replace()` to prevent corruption.

**Cache lookup order:** ProcessingCache → PersistentALTCache → RPC → write both.

**The `ALTFetcher` Protocol:** Decouples `ALTManager` from `SolanaRPCClient`. Tests inject `MockRPCClient` without HTTP. `SolanaRPCClient` satisfies the protocol automatically (structural subtyping via `runtime_checkable`).

**Why `jsonParsed` encoding for ALT accounts:** The ALT account is a binary-encoded Solana account format. Reading it raw requires a custom deserializer. `jsonParsed` is the only place in the entire pipeline where provider-parsed output is accepted — and only for reading ALT structure, never for transfer amounts or transaction data.

**Resolution failure = degraded:** If any ALT fails to resolve, `resolve_transaction_loaded_addresses` returns `None`. The caller must treat this as a degraded state and must not promote the transaction as healthy.

**Persistent cache schema:**
```json
{
  "schema_version": "1",
  "lookup_table_address": "...",
  "addresses": ["..."],
  "fetched_at": "2026-05-04T...",
  "slot_or_context": 0,
  "provider": "primary",
  "checksum": "<sha256 of pipe-joined sorted addresses>"
}
```

---

### 0C — Transfer Truth Rule

**The core rule:** `transaction_success != transfer_success`

A transaction can succeed on-chain while the watched transfer never happened (e.g. the token account had zero balance, the CPI failed silently, or the instruction was a no-op). `meta.err == null` alone is never sufficient evidence.

**Proof hierarchy (strongest to weakest):**
1. Pre/post token balance delta — computed from `preTokenBalances` and `postTokenBalances`
2. SPL transfer instruction in resolved instruction list
3. Log messages (support evidence only, not standalone proof)
4. `meta.err == null` alone → never sufficient

**Critical fix — newly-created ATAs:**
A newly-created ATA (associated token account) has no entry in `preTokenBalances`. The original code skipped any account where `pre_amount_raw is None OR post_amount_raw is None`, which meant it skipped newly-funded destination accounts entirely.

**Fix:** Treat `None` pre-balance as `0` (not unknown):
```python
if pre_amount_raw is None and post_amount_raw is None:
    continue  # truly unknown, skip
if pre_amount_raw is None:
    pre_amount_raw = 0   # new account — had zero balance before
if post_amount_raw is None:
    post_amount_raw = 0
```

This correctly detects transfers into brand-new ATAs.

**`settlement_evidence_type`:** One of `"both"`, `"balance_delta"`, `"instruction"`, `"none"`. Preserved in the event for downstream validation.

---

### 0D — Jito Tip Detector

**What Jito tips are:** Explicit SOL transfers to one of 8 pinned Jito tip accounts. They are separate from `fee_lamports` (which is the base + priority fee collected by validators). Jito tips go to the block engine.

**The 8 pinned tip accounts** are hardcoded in `constants.py` with a `JITO_TIP_ACCOUNTS` env override for testing or future changes.

**Detection:** Scan all instructions (top-level + inner). Look for System Program (`11111111111111111111111111111111`) transfers to any tip account. Measure lamports from the balance delta on the destination account.

**Key rule:** Do not count Jito tips in `fee_lamports` — that's already a separate field. Also do not count ordinary SOL transfers to non-tip addresses.

---

### 0E — Cost Decomposition

**The formula:**
```
total_native_observed_cost_lamports = fee_lamports + jito_tip_lamports + explicit_tip_lamports
```

**What NOT to add:** Priority fee is the portion of `fee_lamports` above base fee (`5000 * signature_count`). It is already included in `fee_lamports`. It is reported separately for analysis but never added again to the total — that would be double-counting.

**Token transfer fees** (Token-2022 `transferCheckedWithFee`) are kept separate from native SOL cost. Never mix lamports and token units.

**`decimal.Decimal` for USD conversion:** SOL → USD conversion uses `Decimal`, not float. The USD rate is injected as a parameter (never hardcoded) and may be absent, in which case USD fields are `None`.

---

### 0F — Canonical Key + Collision Defense

**Canonical key format:** `solana:{signature}:{instruction_index}:{inner_instruction_index}`

**Why not EVM format:** EVM uses `tx_hash + log_index`. Solana has no log index. Using that pattern would cause key collisions across different instructions in the same transaction. `validate_no_evm_identity()` raises `ValueError` if any Solana event has `log_index` or `tx_hash` fields.

**Collision defense:** When two events share the same base canonical key (same sig + indexes), they are grouped and assigned a `transfer_ordinal` (0, 1, 2, ...). If their fingerprints differ (different programs, mints, amounts), `collision_detected = True` and `validation_status = "degraded"`.

**Fingerprint:** SHA-256 of `program_id|token_mint|source|dest|amount_raw|data_hash`. Deterministic across Python versions.

---

### 0G — Token-2022 / Transfer Fee Handling

**Why Token-2022 needs special handling:** Token-2022 adds extension instructions with new discriminators and account layouts. An unknown discriminator must not crash the parser — it returns `HOOK_UNKNOWN` and marks the result degraded.

**Classification:** Based on `program_id` + first byte of base58-decoded instruction data (the discriminator):
- SPL Token `3` → `VANILLA_TRANSFER`
- SPL Token `12` → `TRANSFER_CHECKED`
- Token-2022 `12` → `TRANSFER_CHECKED`
- Token-2022 `26` → `TRANSFER_CHECKED_WITH_FEE`
- Token-2022 non-monetary (freeze, burn, approve, etc.) → `HOOK_NON_MONETARY`
- Anything else → `HOOK_UNKNOWN`

**Amount resolution hierarchy:** Balance delta first (strongest). If that fails, the result is marked degraded — never crash, never guess.

**`decimal.Decimal` prohibition on float:** All token amount math goes through `Decimal(str(value))` or `Decimal(int_value) / Decimal(10 ** decimals)`. `float` is prohibited in all Solana paths.

---

### 0H — Parser Acceptance Gate Tests

**The `-1` trap (REF-01):** Multiple early implementations used `0` as the sentinel for top-level instructions. This caused canonical key collisions. The test suite has an explicit test for `inner_instruction_index = -1` on top-level and `>= 0` on inner instructions.

**Decimal precision test fix:** The original test tried to prove that `float` diverges from `Decimal` for large USDC amounts. Python's `float64` can represent `9999999.999999` exactly (well within `2^53`), so the assertion `float_result != decimal_result` was false. Fixed by replacing with exact expected-value assertions for specific raw amounts.

**Test counts:** 42 tests, all passing. 19 required by build plan + 3 bonus.

---

## Phase 1 — Scoped RPC Ingestion Adapter

### Design

**Discovery path:**
```
getSignaturesForAddress(watched_address, before=cursor, limit=cap)
  -> reversed (chronological order)
  -> for each sig: getTransaction(sig, maxSupportedTransactionVersion=0, encoding="json")
  -> ALTManager.resolve_transaction_loaded_addresses(raw_tx)
  -> normalize_transaction(raw_tx, resolved_loaded_addresses)
  -> evaluate_transfer_truth(pre, watched_mints)
  -> detect_jito_tips(pre)
  -> decompose_cost(pre, jito)
  -> emit raw_event
```

**`encoding="json"` not `"jsonParsed"`:** All transaction fetches use `encoding="json"`. This is the self-parse rule — we never use provider-parsed output as source of record for transfer amounts. The only exception is ALT account fetches (`encoding="jsonParsed"` for ALT binary account structure).

**Fresh `ProcessingCache` per run:** Created inside `SolanaIngestionAdapter.run()`. This enforces the RPC efficiency guarantee: each unique ALT is fetched at most once per ingestion run, regardless of how many transactions reference it. The `PersistentALTCache` is shared across runs.

**Watched-mint filter is a skip, not an error:** A transaction with no USDC movement is normal on Solana. It is skipped silently and counted in `transactions_skipped_no_watched_mint`. It does not degrade `run_status`.

**Signature reversal:** `getSignaturesForAddress` returns newest-first. We reverse to process chronologically (oldest first). This keeps the cursor (`before=` parameter) semantics consistent with checkpointing in Phase 2.

**`getTransaction` returning `None`:** This is a degraded state — the transaction exists in the signature list but could not be fetched. Counted in `transactions_degraded`. The run continues to process remaining signatures.

### Key fix during build

`transactions_degraded` was not incremented when `getTransaction` returned `None` — only `errors` was appended. Fixed by adding `result.transactions_degraded += 1` in that branch. The test caught it immediately.

### Test counts

25 tests, all passing. Covers: config from env, empty signature set, USDC event fields, cost fields, watched-mint filter, failed tx, getTransaction None, run caps, multiple watched addresses, ALT metrics, inner instruction guard, start_signature cursor.

---

## Patterns Established Across Phases

### The `_pre_normalized` passthrough
Every raw event includes `"_pre_normalized": pre` — the full pre-normalized dict. Downstream phases (4, 5) consume this for schema normalization and owner resolution without re-fetching.

### Degraded does not mean crash
Every module follows this rule: on unexpected input, return a degraded result with `validation_status = "degraded"` and `None` fields. Never raise an exception that would crash the ingestion loop.

### `decimal.Decimal` is the only token math type
`float` is prohibited in all Solana paths. The existing EVM path (`services/transfer_math.py`) uses float — it must never be imported or reused for Solana amounts.

### Primary RPC is source of record
Fallback RPC is for reconciliation and emergency degraded reads only. Using fallback does not produce healthy status. This is enforced by the `provider_mode` property on `SolanaRPCClient`.

### MockRPCClient pattern
All tests inject a `MockRPCClient` — no real HTTP. The mock counts calls per address (for ALT efficiency tests) and supports `fail_addresses` / `fail_transactions` for degraded-path testing. This pattern is established and reused across test files.

---

## Phase 2 — Persistent Checkpointing

### Design

**The core rule:** Never scan from genesis silently.

If the `CheckpointStore` is wired into the adapter and no checkpoint exists for a `(chain, token_mint, watched_address)` triple, and no `SOLANA_START_SIGNATURE` or `SOLANA_START_SLOT` is configured, the adapter returns `run_status = "failed"` with a clear error message — it does not guess, it does not scan from slot 0.

**Checkpoint key:** SHA-256 of `{chain}|{token_mint}|{watched_address}`, truncated to 16 hex chars + a human-readable suffix. Deterministic and collision-resistant across all watched triples.

**Three write operations, each with different semantics:**
- `get_or_seed()` — read-or-initialize. Uses start config only when no checkpoint exists.
- `advance()` — moves the cursor forward after a successful processing step. `promoted=True` only after BigQuery batch promotion. `validated=True` only after reconciliation passes.
- `mark_failed()` — sets `ingestion_status=failed` WITHOUT advancing the cursor. The next run retries from the exact same signature position.

**Why slot alone is not enough for resume:** Slots can be skipped (missed blocks), and a given slot may contain zero or many transactions. Only the signature provides an unambiguous resume point. The build plan rule: "Use slot plus signature. Do not resume from slot alone."

**Atomic write:** All flushes write to `{path}.tmp` then `os.replace()` to the real path. Partial writes on crash leave the previous checkpoint intact.

**Backward compatibility:** `checkpoint_store` is an optional parameter on `SolanaIngestionAdapter`. Without it (Phase 1 mode), the adapter uses `config.start_signature` directly. Existing Phase 1 tests pass unchanged.

**Priority rule:** Checkpoint cursor wins over config cursor. If a checkpoint records `last_processed_signature = SIG_A` and config has `start_signature = SIG_B`, the adapter uses `SIG_A`. This prevents re-ingestion on restart even if the operator forgets to update the config.

### Checkpoint schema
```json
{
  "chain": "solana",
  "token_mint": "<mint_address>",
  "watched_address": "<address>",
  "last_processed_signature": "<sig>",
  "last_processed_slot": 123456,
  "last_successful_run_at": "2026-05-04T...",
  "last_promoted_slot": 123456,
  "last_validated_at": "2026-05-04T...",
  "ingestion_status": "ok" | "degraded" | "failed"
}
```

### Test count: 29 tests, all passing.

---

## Phase 3 — Rate Limiter + Circuit Breaker

### Two separate guards, two separate scopes

**`rpc_client.py` already handles per-call retry** (3 attempts, exponential backoff 2s/4s/8s, ±10% jitter, respects `Retry-After` header). That layer is not touched in Phase 3.

Phase 3 adds two **loop-level** guards in `services/solana/circuit_breaker.py`:

**`RateLimiter` — token bucket:**
- Tokens refill at `max_rps` per second, up to `burst_limit`.
- `acquire()` sleeps exactly the deficit time — no busy-wait.
- Clock and sleep are injectable for deterministic tests (no `time.sleep` in tests).
- Called before each `getSignaturesForAddress` and `getTransaction` in the ingestion loop.

**`CircuitBreaker` — three states:**

| State | Behavior |
|-------|----------|
| `CLOSED` | Normal. Failures accumulate. |
| `OPEN` | Blocked. `before_call()` raises `CircuitOpenError`. Cooldown timer running. |
| `HALF_OPEN` | Cooldown elapsed. One probe call allowed through. Success → CLOSED. Failure → OPEN again. |

**Why loop-level, not call-level:** Each `_post_with_retry` call already exhausts 3 retries before returning a failure. A circuit breaker that trips on the first HTTP error would fight with the retry layer. The circuit breaker trips only after N *fully-retried* failures — meaning the provider has been given 3×N chances before the circuit opens.

**`CircuitOpenError` stops the run, not crashes it:** The adapter catches `CircuitOpenError`, appends it to `result.errors`, sets `run_status = "degraded"`, and breaks the loop. The process does not crash. The operator sees the failure via logs and `/health/solana`.

**`health_dict()`** — the circuit breaker exposes `circuit_state`, `consecutive_failures`, `total_trips`, `cooldown_remaining_seconds`. This is the data source for the `/health/solana` endpoint (Phase 8).

### Backward compatibility
Both `circuit_breaker` and `rate_limiter` are optional parameters on `SolanaIngestionAdapter`. `None` (the default) disables them entirely. All Phase 1 and Phase 2 tests pass unchanged.

### Test design note — injectable clock
The circuit breaker cooldown is tested by injecting a fake clock (`_clock` parameter) that can jump forward by any amount. This avoids `time.sleep()` in tests and makes cooldown transitions instant and deterministic.

### Test count: 27 tests, all passing.

---

## Phase 4 — Normalized Solana Event Schema + BigQuery Writer

### `event_schema.py` — `normalize_event()`

Takes a `raw_event` dict from Phase 1 and produces all 44 required canonical fields. Key design choices:

**All 44 fields always present.** `validate_normalized_event()` checks completeness by set difference — any missing field name is returned. Empty list = valid. BigQuery insertion does not KeyError on a missing column.

**Owner fields are `None` / `owner_resolution_status = "pending"`.** Phase 5 will backfill these. The schema is fully present in Phase 4; the values are intentionally deferred.

**`decode_version`** is a string constant incremented when the schema changes in a breaking way. Downstream queries can filter by version to handle schema migrations.

**`normalized_event_id` vs `raw_event_id`:** `raw_event_id` = `solana:{sig}:{ix}:{inner}`. `normalized_event_id` = `raw_event_id:{fingerprint[:8]}`. If two events share the same raw key but have different fingerprints (collision), their normalized IDs still differ.

**Validation status aggregation:** Checks `transfer_validation_status`, `cost_validation_status`, and `pre_normalization_status` from the raw event AND from `_pre_normalized`. The status is `"degraded"` if any sub-status is `"degraded"`, `"failed"`, or `"partial"`.

### `bigquery_writer.py` — `SolanaEventWriter`

**BIGNUMERIC vs NUMERIC distinction:**
- Integer raw amounts (lamports, raw token amounts) → `BIGNUMERIC` column → serialized as `str(int)`, e.g. `"5000"`
- `amount_decimal` (Decimal) → `NUMERIC` column → serialized with fixed 6-decimal precision: `f"{val:.6f}"` → `"1.000000"`

**Why `f"{val:.6f}"` not `str(Decimal)`:** Python's `str(Decimal("1.000000"))` returns `"1"` (trailing zeros stripped). BigQuery's NUMERIC column accepts `"1"` correctly, but the explicit `"1.000000"` is clearer and self-documenting. The test verified this exact serialization.

**Float guard at serialization time:** `_serialize_for_bq()` raises `TypeError` immediately if a float is found in any BIGNUMERIC/NUMERIC field. This is the last line of defense before data leaves the process.

**Private key stripping:** Any key prefixed with `_` (e.g. `_pre_normalized`) is dropped from the BQ row. The `_pre_normalized` passthrough is for internal pipeline use only — never written to BigQuery.

**Fallback buffer:** When no BQ client is injected, events are appended to `data/solana_events_buffer.jsonl`. Ingestion continues unblocked. Operator can replay the buffer once BQ credentials are available.

### Two fixes during build

**Fix 1 — wrong keyword args on `build_event_fingerprint`:** The call used `source=` and `dest=` but the actual signature uses `source_token_account=` and `destination_token_account=`. Caught immediately on first test run.

**Fix 2 — validation status not catching `"failed"` pre_normalization:** The sub_status check only compared against `"degraded"`, not `"failed"` or `"partial"`. Fixed by expanding the check: `s in ("degraded", "failed", "partial")`. Also added `pre.get("pre_normalization_status")` from the `_pre_normalized` dict as a second source.

### Test count: 46 tests, all passing.

---

## Open Risks / Notes for Next Phases

| Risk | Phase | Status |
|------|-------|--------|
| `source_token_account` may be `__account_index_N__` placeholder | Phase 5 | Must resolve accountIndex → pubkey before BigQuery write |
| `source_owner` / `destination_owner` are `None` | Phase 5 | Owner resolution from token balance tables or getAccountInfo |
| `amount_transferred_raw` is `None` | Phase 5 | Requires source account delta resolution |
| `transfer_ordinal` hardcoded to `0` | Phase 5 | Collision defense assigns correct ordinal |
| `collision_detected` hardcoded to `False` | Phase 5 | Collision defense sets this |
| Checkpoint not advanced post-promotion yet | Phase 6 | `advance(promoted=True)` API is wired; BigQuery promotion not built |

---

## Phase 5 — Owner + Amount Resolution

### What it does
Phase 5 resolves the two fields that Phase 4 cannot fill from the raw transaction alone:
1. **Owner resolution** — who controls the source and destination token accounts
2. **Amount resolution** — precise `amount_transferred_raw`, `amount_received_raw`, and `fee_withheld_raw` from balance deltas

Results are patched into the normalized event in-place via `apply_owner_and_amount_resolution()`.

### Owner Resolution Hierarchy (4 tiers)

**Tier 1 — Token balance tables (fastest, free)**  
`preTokenBalances` and `postTokenBalances` in every `getTransaction` response include the `owner` field directly. No RPC needed. Always check here first.

**Tier 2 — OwnerCache (file-backed, cross-run)**  
Once we resolve an owner via RPC, we write it to `data/owner_cache.json`. On subsequent runs, we skip the RPC call entirely. Critical for production throughput when watching high-volume corridors — the same ATA appears in thousands of transactions.

**Tier 3 — getAccountInfo RPC (costly but authoritative)**  
If the owner isn't in the balance tables or cache, we call `getAccountInfo` with `encoding="jsonParsed"`. The parsed response includes `data.parsed.info.owner`. The result is written to the cache before returning.

**Tier 4 — Degraded (no crash)**  
If all three tiers fail (account closed, RPC error, etc.), `owner_resolution_status = "degraded"` and `source_owner` / `destination_owner` stay `None`. The row is still written — degraded data is better than lost data.

### The `__account_index_N__` Placeholder Trap

**Problem:** When Phase 1 sees a token transfer where the account is only identified by its index in `accountKeys`, it writes a placeholder string like `__account_index_2__` instead of the actual pubkey. This prevents a crash but produces a non-pubkey string in `source_token_account`.

**Resolution:** `_resolve_placeholder(token_account, account_keys)` detects the pattern and replaces it with `account_keys[N]`. If the index is out of range, the placeholder is returned unchanged (safe degradation).

**Where it matters:** Must resolve before calling `_owner_from_balances()`, because that function matches by pubkey string, not by index.

### Amount Resolution via Balance Deltas

Balance deltas are the most authoritative source for transfer amounts. The token balance tables record pre- and post-state for every ATA that changes, so the actual transferred amount = `post - pre` for the destination account.

**Key design:** `_compute_full_balance_delta()` finds the account with the largest positive delta (= received) and largest negative delta (= transferred). This works for both direct SPL transfers and wrapped program calls (e.g. DEX swaps) where the instruction data amount may differ from the settled amount.

**Token-2022 fee withheld:** `fee_withheld_raw = transferred_raw - received_raw`. If the protocol takes a fee in transit, the destination receives less than the source sends. The difference is the withheld fee. For standard SPL token: `transferred == received`, so `fee_withheld_raw = 0`.

**Newly-created ATA:** If `preTokenBalances` has no entry for an account (ATA created during the transaction), treat the pre-balance as `0`. The same rule applies in Phase 0C `transfer_truth.py` — consistent across both phases.

### `apply_owner_and_amount_resolution()` Design

Phase 4 `normalize_event()` sets safe placeholder values:
- `amount_transferred_raw = None`, `fee_withheld_raw = None`
- `source_owner = None`, `destination_owner = None`
- `owner_resolution_status = "pending"`, `amount_resolution_status = "pending"`

Phase 5 patches only the fields it can resolve with confidence. The rule: **never clobber a good Phase 4 value with a degraded None**. If `amount_resolution_status != "ok"`, the Phase 4 `amount_received_raw` (from instruction data) is preserved.

### Program-Owned Account Flagging

Some token accounts are owned by programs rather than user wallets — e.g. System Program (`11111...`), SPL Token program, Token-2022 program. These are flagged with `is_program_owned=True` in the resolution result. Currently surfaced in the resolution method string; future phases may use this for routing analytics (program-to-program flows vs wallet-to-wallet).

### OwnerCache Atomicity

Cache flushes use `os.replace()` (atomic on POSIX). Write to a temp file, then rename — prevents a partial JSON write corrupting the cache on crash. Same pattern as `CheckpointStore` from Phase 2.

### Key Numbers
- **Tests:** 36 / 36 passing
- **New files:** `services/solana/owner_resolver.py`
- **Modified files:** `services/solana/event_schema.py` (added `apply_owner_and_amount_resolution`)
- **Test file:** `tests/solana/test_owner_resolver.py`

### Traps to Avoid
| Trap | Impact | Fix |
|------|--------|-----|
| Calling `_owner_from_balances` before resolving placeholder | Never matches → unnecessary RPC call | Always `_resolve_placeholder()` first |
| Using `str(owner)` from balance table without validating | May get empty string or non-pubkey | Validate non-empty before accepting |
| Assuming `preTokenBalances` is always present | ATA creation tx has no pre-entry | Default missing pre-balance to 0 |
| Checking only `preTokenBalances` for owner | New ATA has owner in `postTokenBalances` only | Check both; post wins if pre absent |
| Writing None to cache | Pollutes cache with unresolved entries | Only cache entries where owner is not None |

---

## Phase 6 — Validation + Reconciliation

### What it does
Phase 6 is the promotion gate. Before any normalized batch advances to the derived (analytics) layer, it must pass all 7 gates in `services/solana/validator.py`. A single gate failure rejects the entire batch — no partial promotions.

### The 7 Gates

**Gate 1 — Row count consistency**  
Checks `len(batch) == expected_row_count`. The `expected_row_count` is the number of raw events ingested by Phase 1. If the pipeline dropped or duplicated rows during normalization, this gate catches it.

When `expected_row_count=None`, the gate passes unconditionally — useful in tests and dev runs where the expected count isn't tracked.

**Gate 2 — No float amounts**  
Checks all 11 amount fields (raw lamports + token amounts + `amount_decimal`). Any `float` is a hard rejection. This is the defense-in-depth companion to Phase 4's `assert_no_float_amounts()` — the gate runs on the entire batch, not just spot-checks.

**Gate 3 — Decimal precision**  
`amount_decimal` must have ≤ 9 decimal places. BigQuery NUMERIC supports up to 9 decimal places. USDC uses 6; the gate gives headroom for other tokens while rejecting values that would silently truncate on insert.

The precision check works on `Decimal` objects via `as_tuple().exponent` — much more reliable than string counting or float arithmetic.

**Gate 4 — No placeholder accounts**  
Checks that none of the five account fields (`source_token_account`, `destination_token_account`, `source_owner`, `destination_owner`, `token_mint`) contain the `__account_index_N__` pattern. These placeholders must be resolved in Phase 5 before promotion. A placeholder reaching BigQuery would corrupt corridor analytics.

`None` is allowed — unresolved owner with degraded status is a known state. Only the placeholder string is rejected.

**Gate 5 — Required fields**  
Calls `REQUIRED_FIELDS` from `event_schema.py` (the same 44-field frozenset used by `validate_normalized_event()`). Every row must have every field. The gate reports exactly which fields are missing and for which signature.

**Gate 6 — Transfer truth consistency**  
Enforces: if `transfer_detected=True` then `observed_transfer_inclusion` must also be `True`. This is a one-way implication — settlement evidence can exist without instruction-level detection (balance delta only), but the inverse is a contradiction.

`None` on either field passes — unknown is different from false.

**Gate 7 — Reconciliation sample**  
Spot-checks the first `VALIDATION_SAMPLE_SIZE=20` events for internal consistency:
- `amount_raw == amount_received_raw` (both are canonical aliases for the settled amount)
- `validation_status` is one of `{"ok", "degraded", "failed", "partial"}`
- `raw_event_id` and `normalized_event_id` are non-empty strings
- `amount_decimal` is approximately consistent with `amount_received_raw` (within 1% tolerance to accommodate non-USDC tokens)

The 1% tolerance is intentional — exact equality would require knowing each token's decimal count at gate time. The gate is a sanity check, not a precision audit; precision is handled by Gate 3.

### Gate Architecture — Why Not Raise Immediately?

Each gate runs to completion and collects all violations before the report is assembled. This means a batch with 3 failing gates surfaces all 3 failures at once, not just the first. Debugging a production ingestion problem is much faster when the full picture is visible in a single log line.

`GateResult.summary` shows the first violation plus a "+N more" count — enough to diagnose without flooding logs.

### `assert_batch_approved()` Pattern

For pipeline code that must halt on rejection:
```python
report = assert_batch_approved(normalized_events, expected_row_count=len(raw_events))
checkpoint.advance(promoted=True)
```

For observability code that should log and continue:
```python
report = validate_batch(normalized_events)
if not report.approved:
    logger.warning(report.summary())
    checkpoint.mark_failed(...)
```

Both paths use the same underlying gates — the difference is only in how the caller handles the `ValidationReport`.

### Key Numbers
- **Tests:** 42 / 42 passing
- **Full suite:** 247 / 247 passing
- **New files:** `services/solana/validator.py`
- **Test file:** `tests/solana/test_validator.py`

### Traps to Avoid
| Trap | Impact | Fix |
|------|--------|-----|
| Promoting partial batches on first-gate failure | Corrupted derived layer with incomplete data | Run all gates; reject entire batch on any failure |
| Using `float` comparison for decimal precision | `float(Decimal(...))` loses precision silently | Use `Decimal.as_tuple().exponent` |
| Treating `None` owner as a placeholder violation | Blocks valid degraded rows from promotion | Only reject the `__account_index_N__` string pattern, not None |
| Exact `amount_decimal` consistency check | Fails for non-USDC tokens (different decimals) | Allow 1% tolerance in reconciliation; Gate 3 enforces precision separately |
| Checking `validation_status == "ok"` only in reconciliation | Misses "degraded" rows that are valid pipeline output | Check against the known set `{"ok", "degraded", "failed", "partial"}` |

---

## Phase 7 — Freshness + Health State Machine

### What it does
Phase 7 defines the single source of truth for whether Solana data is current enough to display. It produces a three-state signal consumed by every layer above it (API, dashboard, cache).

### Three States and Their Meaning

| State | Condition | Display rule |
|-------|-----------|--------------|
| `fresh` | `lag ≤ freshness_threshold` (default 5 min) | Show as live data |
| `stale` | `freshness_threshold < lag ≤ stale_threshold` (default 1 hr) | Show with amber warning label |
| `unavailable` | No data OR `lag > stale_threshold` | Never green; never cached stale-as-fresh |

**Why two thresholds, not one?** A single threshold creates a binary cliff — data is either "fine" or "broken." Two thresholds create a grace zone (`stale`) where the data is old but still useful for trend analysis, while making the degraded state visible. This matches how the EVM cache uses `"stale"` vs `"critical"`.

### Clock Injection Pattern (same as CircuitBreaker)

`FreshnessMonitor` takes a `_clock: Callable[[], float]` parameter. Production uses `time.time`. Tests use `FakeClock` — a simple object with `.now` and `.advance(seconds)`. This eliminates all `time.sleep()` from the test suite and makes transition tests deterministic.

The same pattern was established in Phase 3 for `CircuitBreaker`. Consistent injection point across the project: every component that cares about wall time gets a `_clock` parameter.

### The `unavailable` Entry Point

The monitor starts in `unavailable` and stays there until `record_slot()` is called with real on-chain data. This is the correct default — the system should not claim freshness it hasn't earned. `reset()` also returns to `unavailable`, so a monitor that loses its state can't accidentally claim freshness from a previous run.

### Two Lag Metrics

`HealthReport` surfaces two distinct lag values:
- `lag_seconds` — `now - block_time` (chain-side lag: how old is the newest on-chain block we've seen?)
- `ingestion_lag_seconds` — `now - ingested_at_wall` (pipeline lag: how long ago did we write this?)

Both matter. Chain-side lag catches RPC fallback failures (we can write rows but they're based on old blocks). Ingestion lag catches pipeline stalls (the ingestion process hasn't run). Phase 8 surfaces both to the API.

### Key Numbers
- **Tests:** 29 / 29 passing
- **New files:** `services/solana/freshness.py`
- **Test file:** `tests/solana/test_freshness.py`

### Traps to Avoid
| Trap | Impact | Fix |
|------|--------|-----|
| Single freshness threshold | Binary cliff; no grace zone for trend analysis | Two thresholds: fresh → stale → unavailable |
| Starting monitor in `fresh` | Claims freshness before any data arrives | Always start in `unavailable`; require `record_slot()` |
| Real `time.time()` in tests | Non-deterministic; race conditions in CI | Inject `_clock`; never `time.sleep()` in tests |
| Evaluating lag only at `record_slot()` time | A fresh record ages to stale without triggering re-evaluation | Recompute lag on every `health_report()` call from the live clock |

---

## Phase 8 — API + Cache Integration

### What it does
Phase 8 bridges the FreshnessMonitor to the existing Canopy API without touching the EVM cache machinery. The key deliverable is `SolanaCache` — a push-updated in-process cache — and `SolanaAPIState` — the serializable state object that API endpoints read.

### Push vs Pull Architecture

The EVM cache (Polygon/Ethereum) is **pull-based**: a background poller periodically queries BigQuery and refreshes the cache. Solana is **push-based**: the ingestion adapter calls `record_run()` after each successful promotion cycle. The cache is then read by API handlers on demand.

This asymmetry is intentional. Solana ingestion is RPC-first and event-driven; a periodic BigQuery poller would be the wrong abstraction. The push model also makes the ingestion adapter the single point of authority for when Solana data is "new."

### `get_state()` Re-evaluates Lag from the Live Clock

A subtlety: `record_run()` stamps the state at ingestion time, but `get_state()` re-evaluates freshness from the live clock on every call. This means a state that was `fresh` at ingestion can return as `stale` on the next API read — without needing another `record_run()`.

This is the correct behavior. If the ingestion pipeline stalls, the API should reflect the growing lag automatically, not serve a cached `fresh` label indefinitely.

### Reference Replacement for Thread Safety

`SolanaCache._state` is replaced atomically via `self._state = SolanaAPIState(...)`. Python's GIL makes dict/object reference replacement atomic in single-threaded async code (same pattern as `_cache = new_dict` in `api/cache.py`). No locking needed for the read path.

### `to_chain_health_dict()` — Dashboard Compatibility

`SolanaAPIState.to_chain_health_dict()` maps Solana's three freshness states to the `status` / `freshness_level` vocabulary already used by the Polygon/Ethereum chain health entries in `/health`:

| Solana state | `status` | `freshness_level` |
|---|---|---|
| `fresh` | `"fresh"` | `"fresh"` |
| `stale` | `"degraded"` | `"stale"` |
| `unavailable` | `"unavailable"` | `"unknown"` |

The dashboard can render a Solana row using the same field names as the existing EVM rows — zero bespoke client handling.

### Key Numbers
- **Tests:** 35 / 35 passing
- **New files:** `services/solana/api_integration.py`
- **Test file:** `tests/solana/test_api_integration.py`

### Traps to Avoid
| Trap | Impact | Fix |
|------|--------|-----|
| Caching freshness label at `record_run()` time | API serves stale-as-fresh if pipeline stalls | Re-evaluate lag from live clock in every `get_state()` call |
| Mixing Solana and EVM cache machinery | EVM poller assumptions break Solana's push model | Additive module; no shared mutable state with `api/cache.py` |
| Returning `"fresh"` status when stale | Violates the core data quality rule | Explicit mapping: stale → `"degraded"`; unavailable → `"unavailable"` |

---

## Phase 9 — Dashboard Integration

### What it does
Phase 9 wires the Solana health state to the dashboard UI and adds the `/v1/solana/health` API endpoint. Three changes: a new FastAPI endpoint, Solana added to the `/health` chains dict, and a new "Solana Data Layer" panel in the dashboard.

### Endpoint Design — `/v1/solana/health`

Returns `SolanaAPIState.to_dict()` plus two additional keys:
- `chain_health` — the EVM-compatible chain health sub-dict (for `/health` consumers)
- `scope_disclaimer` — required disclaimer text, present in every response regardless of freshness state

The scope disclaimer is always present — not conditional on staleness. The dashboard shows it prominently when state is stale or unavailable; it's available for any consumer to display when fresh as well.

### Graceful Degradation in `loadWorkspace()`

The Solana fetch uses `.catch(() => null)` in the `Promise.all`:
```javascript
fetchJson(`${API_BASE}/v1/solana/health`).catch(() => null),
```
If Solana is not wired up (endpoint 404, network error, etc.), `state.solanaHealth` is `null` and the dashboard renders "No data" gracefully. The entire page load is never blocked by Solana availability.

### Scope Disclaimer Display Rule

The scope disclaimer card (amber background) renders in the dashboard when state is `stale` or `unavailable`. It is suppressed when `fresh` to avoid creating noise in the normal operating state. All three states include the disclaimer in the API payload — the dashboard chooses when to surface it visually.

### Why Not Modify `LIVE_CHAINS` in `api/cache.py`?

`LIVE_CHAINS = ("Polygon", "Ethereum")` drives the BigQuery polling loop. Adding `"Solana"` to it would wire Solana into the pull-based polling machinery — wrong model. Instead, Solana is added to the `/health` response directly via `get_solana_api_state().to_chain_health_dict()`, keeping the two ingestion models independent.

### Key Numbers
- **Tests:** 18 / 18 passing
- **Full suite:** 329 / 329 passing
- **New files:** `tests/test_solana_api_endpoints.py`
- **Modified files:** `api/main.py` (new endpoint + Solana in `/health`), `ui/index.html` (Solana panel + fetch)

### Traps to Avoid
| Trap | Impact | Fix |
|------|--------|-----|
| Adding Solana to `LIVE_CHAINS` | Wires Solana into BigQuery polling loop | Add directly to `/health` response; keep ingestion models independent |
| Blocking page load on Solana fetch | Page fails if Solana endpoint is unavailable | `.catch(() => null)` in `Promise.all` |
| Showing scope disclaimer only when stale | API consumers don't always know freshness context | Always include in API payload; dashboard chooses when to render visually |
| Rendering stale Solana as green | Violates core data quality rule | Explicit color: stale → amber, unavailable → gray, fresh only → green |
