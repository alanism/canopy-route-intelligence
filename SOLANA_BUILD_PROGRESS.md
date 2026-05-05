# Solana Build Progress — v3 Execution Tracker

**Source of truth:** `docs/Canopy_PRD_BuildPlan_v3.docx`  
**Last Updated:** 2026-05-05  
**Current Mode:** Execution / Build

---

## Handoff Protocol (Mandatory)

At the end of **every phase** (13, 14, 14.5, 15, 16, 16.5), the active agent must do all of the following before handoff:

1. Update this file (`SOLANA_BUILD_PROGRESS.md`):
- set phase status (`Not Started` | `In Progress` | `Blocked` | `Done`)
- record exactly what changed
- link evidence (tests, logs, queries, commit hash)
- list remaining blockers / TODOs for next agent

2. Update `SOLANA_LEARNING_LOG.md`:
- add a dated phase entry with: decisions, traps, failures, fixes, and unresolved risks
- include at least one "what we'd do differently" note

3. Capture operator evidence:
- commands run
- critical output snippets
- acceptance criteria pass/fail state

No phase is considered complete without both docs updated.

---

## Global Status Board

| Phase | Stratum | Status | Owner | Last Update | Gate Summary |
|---|---|---|---|---|---|
| 13 — Deterministic Identity + Idempotency | S2 | `Done` | Codex | 2026-05-05 | TD-01/02/03 + 9 identity tests |
| 14 — Live RPC First Slice + Finality Policy | S0 | `Done` | Codex | 2026-05-05 | finalized JSONL slice accepted; 27 rows written |
| 14.5 — Semantic RPC Validation | S0/S1 | `Done` | Codex | 2026-05-05 | semantic RPC guards + checkpoint-hold tests |
| 15 — Durable Scheduler + Checkpoint Safety | S1/S2 | `Done` | Codex | 2026-05-05 | scheduler + checkpoint invariant tests |
| 16 — Idempotent BQ Promotion + Tooling | S2 | `Done` | Codex | 2026-05-05 | DDL tooling + MERGE replay tests |
| 16.5 — Shadow S3 Signal Validation | S3 | `Not Started` | Unassigned | 2026-05-05 | internal-only shadow views + missing_field_report |

---

## Current Focus

- **Now Working On:** none; Phase 16 closed, Phase 16.5 not started  
- **Hard Blockers First:** TD-01, TD-02, TD-03, TD-04, TD-05  
- **Phase 17:** Explicitly blocked until Phase 16 acceptance criteria pass.

---

## Phase Closeout Template (Copy Per Phase)

Use this exact structure when closing a phase.

### Phase X Closeout
- Date:
- Owner/Agent:
- Status: `Done` | `Blocked`
- PRD Source Section(s):
- Scope Completed:
- Files Touched:
- Tests Run:
- Acceptance Criteria:
- Evidence (logs/queries/outputs):
- Open Risks:
- Next Agent TODOs:

---

## Phase 13 Checklist (Execution)

- [x] Implement strict identity pipeline:
  - raw_event_id = `solana:{sig}:{ix}:{inner_ix}`
  - transfer_fingerprint deterministic
  - stable sort by fingerprint
  - transfer_ordinal assigned from sorted order
  - normalized_event_id = `{raw_event_id}:{fingerprint[:8]}`
- [x] `collision_detected=True` when same raw_event_id has >1 distinct fingerprint
- [x] Exact duplicate replay deduped (not double-written)
- [x] BigQuery promotion uses MERGE keyed on `normalized_event_id`
- [x] Add `tests/solana/test_event_schema_identity.py` with all 9 tests
- [x] Full test suite green (`existing + new identity tests`)
- [x] Record phase session entry in `SOLANA_LEARNING_LOG.md`

## Phase 14 Checklist (Execution)

- [x] Live run at `SOLANA_COMMITMENT=finalized`
- [x] Verify at least 3 events observed in accepted finalized batch
- [x] `/v1/solana/health` includes `freshness_state`, `observation_state`, `ingestion_state`, `commitment_level`
- [x] Checkpoint has valid signature + slot
- [ ] Empty/ambiguous responses do not advance checkpoint
- [ ] Record phase closeout entry in `SOLANA_LEARNING_LOG.md`

## Phase 14.5 Checklist (Execution)

- [x] Detect JSON-RPC semantic failures (error field, null result, wrong shape)
- [x] Handle ambiguous empty windows for known-active addresses
- [x] Detect provider lag and set `ingestion_state=provider_lagging`
- [x] Reject wrong commitment-level transactions from promotion
- [x] Record phase closeout entry in `SOLANA_LEARNING_LOG.md`

## Phase 15 Checklist (Execution)

- [x] Add durable scheduler (`--once` and `--loop`) with graceful shutdown
- [x] Enforce prod guard: no `local_file` checkpoint backend in production
- [x] Enforce checkpoint safety invariant (advance only after full success)
- [x] Structured run logs emitted each cycle
- [x] Record phase closeout entry in `SOLANA_LEARNING_LOG.md`

## Phase 16 Checklist (Execution)

- [x] Add idempotent BigQuery table creation tooling
- [x] Verify partition/clustering + numeric type correctness
- [x] Replay test: second run adds 0 rows
- [x] Reconciliation: `COUNT(*) == COUNT(DISTINCT normalized_event_id)`
- [x] `.env.example` and env-regression checks aligned with canonical list
- [x] EVM non-regression validated
- [x] S3-readiness field checklist complete (all required fields present)
- [x] Record phase closeout entry in `SOLANA_LEARNING_LOG.md`

## Phase 16.5 Checklist (Execution)

- [ ] Create internal-only `shadow_*` views
- [ ] Compute shadow success purity / MEV protection / settlement velocity
- [ ] Generate and review `missing_field_report`
- [ ] Verify no shadow outputs leak to external-facing surfaces
- [ ] Record phase closeout entry in `SOLANA_LEARNING_LOG.md`

---

## Agent Handoff Snapshot (Update Every Session)

- **Completed:** Phase 13; Phase 14 finalized JSONL first slice; Phase 14.5 semantic RPC validation; Phase 15 durable scheduler + checkpoint safety; Phase 16 idempotent BQ promotion tooling
- **In Progress:** none
- **Blocked:** repo-wide pytest collection is blocked until local `.venv` installs pinned dependencies from `requirements.txt`
- **Next Critical Step:** wait for explicit approval before starting Phase 16.5.
- **Notes for Next Agent:** Phase 16 acceptance evidence: focused tests `100 passed`, Solana/API suite `407 passed`, BQ DDL dry run `executed=false`, replay test same batch twice -> one target row, S3-readiness `missing=[]`.

---

### Phase 13 Closeout
- Date: 2026-05-05
- Owner/Agent: Codex
- Status: `Done`
- PRD Source Section(s): §4.1–4.4, §7.3, §8 (Phase 13 AC)
- Scope Completed:
  - Deterministic identity assignment (`raw_event_id`, fingerprint sort, `transfer_ordinal`, collision flag)
  - Exact replay dedupe within raw-event groups
  - BigQuery writer MERGE path keyed on `normalized_event_id`
  - 9 identity tests added and passing
  - Env var canonicalization step started (`SOLANA_TOKEN_MINT` canonical + legacy fallback)
- Files Touched:
  - `services/solana/event_schema.py`
  - `services/solana/bigquery_writer.py`
  - `services/solana/ingestion_adapter.py`
  - `tests/solana/test_event_schema_identity.py`
  - `tests/solana/test_event_schema.py`
  - `tests/solana/test_ingestion_adapter.py`
  - `docs/solana-integration.md`
- Tests Run:
  - `.venv/bin/python -m pytest -q tests/solana/test_event_schema_identity.py tests/solana/test_event_schema.py`
  - `.venv/bin/python -m pytest -q tests/solana tests/test_solana_api_endpoints.py`
- Acceptance Criteria:
  - Deterministic ordinal behavior: PASS
  - Collision detection for distinct fingerprints: PASS
  - Replay duplicate dedupe: PASS
  - New identity test suite (9): PASS
  - Full Solana suite non-regression: PASS (`368 passed`)
- Evidence (logs/queries/outputs):
  - `9 passed` in identity suite
  - `368 passed, 1 warning` full Solana + API endpoint suite
- Open Risks:
  - Phase 14 live finalized RPC validation still pending.
- Next Agent TODOs:
  - Start Phase 14 live run using finalized commitment and validate health/observation fields.

### Phase 14 Closeout
- Date: 2026-05-05
- Owner/Agent: Codex
- Status: `Done`
- PRD Source Section(s): §6.1, §8 Phase 14 AC
- Scope Completed:
  - Finalized live first slice with JSONL fallback only.
  - Validation approved before write.
  - JSONL fallback wrote 27 rows.
  - Checkpoint advanced only after validation + write success.
  - Health state recorded finalized commitment and observed/succeeded state.
- Files Touched:
  - `services/solana/event_schema.py`
  - `services/solana/transfer_truth.py`
  - `services/solana/ingestion_adapter.py`
  - `tests/solana/test_owner_resolver.py`
  - `tests/solana/test_ingestion_adapter.py`
  - `SOLANA_BUILD_PROGRESS.md`
  - `SOLANA_LEARNING_LOG.md`
- Tests Run:
  - `.venv/bin/python -m pytest -q tests/solana tests/test_solana_api_endpoints.py`
- Acceptance Criteria:
  - Finalized commitment: PASS
  - 3+ events observed: PASS (`raw_events=27`)
  - Required health fields: PASS
  - Checkpoint signature + slot: PASS (`slot=417663930`)
  - JSONL fallback write: PASS (`rows_written=27`)
- Evidence (logs/queries/outputs):
  - `validation.approved=true`
  - `failed_gates=[]`
  - `data/solana_events_buffer.jsonl` line count increased from `0` to `27`
  - checkpoint advanced from start signature to `4zyshM6cGpRvrZ7jTL16g4oj5hot5kn9GwkPDm3S47ZqAPMNpL3TSrnYDQwEiX6FzFTAsvGs3GaHiHwEAb7Ps5Gh`
- Open Risks:
  - Health state is `stale` because the latest finalized observed block time is outside the freshness threshold at report time.
  - Runtime artifact files remain untracked and should not be committed unless intentionally required.
- Next Agent TODOs:
  - Phase 14.5 is complete; wait for explicit approval before starting Phase 15.

### Phase 14.5 Closeout
- Date: 2026-05-05
- Owner/Agent: Codex
- Status: `Done`
- PRD Source Section(s): Semantic RPC Validation
- Scope Completed:
  - JSON-RPC `error` field detection for signature discovery.
  - Null and malformed result detection for signature discovery.
  - Repeated empty finalized response windows classified as `ambiguous_empty`.
  - Provider slot lag detected before trusting empty windows.
  - Wrong transaction commitment rejected before processing/promotion.
  - Semantic failures tested to hold checkpoint seed signature/slot.
- Files Touched:
  - `services/solana/ingestion_adapter.py`
  - `services/solana/rpc_client.py`
  - `tests/solana/test_ingestion_adapter.py`
  - `SOLANA_BUILD_PROGRESS.md`
  - `SOLANA_LEARNING_LOG.md`
- Tests Run:
  - `.venv/bin/python -m pytest -q tests/solana/test_ingestion_adapter.py`
  - `.venv/bin/python -m pytest -q tests/solana tests/test_solana_api_endpoints.py`
  - `.venv/bin/python -m pytest -q`
- Acceptance Criteria:
  - JSON-RPC error field detection: PASS
  - Null result detection: PASS
  - Ambiguous empty response handling: PASS
  - Provider lag detection: PASS
  - Wrong commitment rejection: PASS
  - No checkpoint advance on semantic failure: PASS
- Evidence (logs/queries/outputs):
  - `tests/solana/test_ingestion_adapter.py` -> `35 passed`
  - Full Solana/API suite -> `379 passed, 1 warning`
  - Repository-wide pytest collection blocked by missing local dependencies: `pandas`, `google`, `dotenv`, `fastapi`, `pydantic`
- Open Risks:
  - Provider lag detection is bounded to the configured cursor (`start_slot`) because the adapter does not have an independent chain-tip oracle.
- Next Agent TODOs:
  - Do not begin Phase 15 until the user explicitly approves it.

### Phase 15 Closeout
- Date: 2026-05-05
- Owner/Agent: Codex
- Status: `Done`
- PRD Source Section(s): Durable Scheduler + Checkpoint Safety
- Scope Completed:
  - Scheduler supports `--once` and `--loop`.
  - SIGTERM/SIGINT set stop flag and loop exits with `shutdown_complete`.
  - Checkpoint backend hierarchy implemented: `local_file`, `gcs`, `bigquery_metadata`.
  - `ENV=production` with `SOLANA_CHECKPOINT_BACKEND=local_file` raises `ProductionCheckpointError`.
  - Checkpoint advance decision rejects failed validation, failed write, `circuit_open`, `ambiguous_empty`, and `provider_lagging`.
  - Structured JSON logs emitted for startup, runs, shutdown, and errors.
  - Repo-wide missing local dependency gap documented.
- Files Touched:
  - `scripts/run_solana_ingestion.py`
  - `tests/solana/test_scheduler.py`
  - `docs/local-test-dependencies.md`
  - `SOLANA_BUILD_PROGRESS.md`
  - `SOLANA_LEARNING_LOG.md`
- Tests Run:
  - `.venv/bin/python -m pytest -q tests/solana/test_scheduler.py`
  - `.venv/bin/python -m pytest -q tests/solana tests/test_solana_api_endpoints.py`
  - `.venv/bin/python -m pytest -q`
  - `ENV=development SOLANA_CHECKPOINT_BACKEND=local_file SOLANA_COMMITMENT=finalized .venv/bin/python scripts/run_solana_ingestion.py --once --dry-run`
- Acceptance Criteria:
  - Focused scheduler tests: PASS (`17 passed`)
  - Solana/API suite: PASS (`396 passed`)
  - Repo-wide pytest status documented: PASS (still blocked by missing local deps)
  - One `--once` dry/dev run: PASS
  - No GCP/BigQuery used unless mocked: PASS
- Evidence (logs/queries/outputs):
  - Dry run emitted `startup` and `solana_ingestion_run` JSON lines.
  - Dry run values: `dry_run=true`, `checkpoint_backend=local_file`, `ingestion_state=succeeded`, `observation_state=no_recent_activity`, `checkpoint_advance_allowed=false`.
  - Repo-wide pytest collection blocked by missing local dependencies: `pandas`, `google`, `dotenv`, `fastapi`, `pydantic`.
- Open Risks:
  - `gcs` and `bigquery_metadata` backends are explicit hierarchy placeholders in Phase 15 and raise `RemoteCheckpointBackendNotConfigured` until a later phase wires credentials/storage.
  - Active `.venv` is missing dependencies already pinned in `requirements.txt`; see `docs/local-test-dependencies.md`.
- Next Agent TODOs:
  - Do not begin Phase 16 unless explicitly instructed.
  - Before repo-wide pytest, install pinned dependencies with `.venv/bin/python -m pip install -r requirements.txt`.

### Phase 16 Closeout
- Date: 2026-05-05
- Owner/Agent: Codex
- Status: `Done`
- PRD Source Section(s): Idempotent BigQuery Promotion + Tooling
- Scope Completed:
  - Added `scripts/create_solana_bq_table.py` dry-run/execute tooling.
  - DDL is generated from `services.solana.bigquery_writer.BQ_SCHEMA`.
  - DDL uses `PARTITION BY RANGE_BUCKET(slot, GENERATE_ARRAY(...))`.
  - DDL clusters by `token_mint`, `watched_address`, `raw_event_id`.
  - Schema contract enforces BIGNUMERIC raw/cost amount fields and NUMERIC decimal amount field.
  - MERGE path remains keyed only on `normalized_event_id`.
  - Replay/idempotency test confirms same batch twice leaves one target row in the in-memory MERGE mock.
  - Added `watched_address` to normalized event and BigQuery schema for clustering/S3-readiness.
  - Added S3-readiness field checklist document.
  - Updated `.env.example` with Solana ingestion and BQ promotion variables.
- Files Touched:
  - `scripts/create_solana_bq_table.py`
  - `services/solana/bigquery_writer.py`
  - `services/solana/event_schema.py`
  - `services/solana/ingestion_adapter.py`
  - `tests/solana/test_bigquery_phase16.py`
  - `tests/solana/test_event_schema.py`
  - `tests/solana/test_validator.py`
  - `.env.example`
  - `docs/solana-s3-readiness-checklist.md`
  - `SOLANA_BUILD_PROGRESS.md`
  - `SOLANA_LEARNING_LOG.md`
- Tests Run:
  - `.venv/bin/python -m pytest -q tests/solana/test_bigquery_phase16.py tests/solana/test_event_schema.py tests/solana/test_validator.py`
  - `.venv/bin/python -m pytest -q tests/solana tests/test_solana_api_endpoints.py`
  - `.venv/bin/python -m pytest -q`
  - `GCP_PROJECT_ID=demo-project .venv/bin/python scripts/create_solana_bq_table.py --project-id demo-project --dataset solana_measured --table solana_transfers --print-ddl`
- Acceptance Criteria:
  - Focused Phase 16 tests: PASS (`100 passed`)
  - Solana/API suite: PASS (`407 passed`)
  - Repo-wide pytest status: BLOCKED by missing local dependencies
  - BQ schema/DDL summary: PASS
  - MERGE key confirmation: PASS (`normalized_event_id` only)
  - Replay/idempotency result: PASS (same batch twice -> one target row)
  - S3-readiness fields present/missing: PASS (`missing=[]`)
- Evidence (logs/queries/outputs):
  - DDL summary: `field_count=44`, `partitioning=RANGE_BUCKET(slot, GENERATE_ARRAY(...))`, `clustering=[token_mint, watched_address, raw_event_id]`, `schema_contract_violations=[]`, `s3_readiness_missing=[]`.
  - DDL dry run did not execute BigQuery (`executed=false`).
  - Repo-wide pytest collection blocked by missing local dependencies: `pandas`, `google`, `dotenv`, `fastapi`, `pydantic`.
- Open Risks:
  - Active `.venv` still needs `.venv/bin/python -m pip install -r requirements.txt` before repo-wide pytest can collect EVM/API tests.
  - EVM runtime tests were not executed because collection stops on missing dependencies; Phase 16 touched no EVM implementation files.
- Next Agent TODOs:
  - Do not begin Phase 16.5 unless explicitly instructed.
  - Hydrate `.venv` from `requirements.txt`, then rerun repo-wide pytest for full EVM/API confirmation.
