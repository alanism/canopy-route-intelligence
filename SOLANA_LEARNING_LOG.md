# Solana Build — Learning Log (v3)

**Source of truth:** `docs/Canopy_PRD_BuildPlan_v3.docx`  
**Purpose:** Capture decisions, traps, failed assumptions, and evidence at phase boundaries so any incoming agent can continue without context loss.

---

## Logging Rules (Mandatory)

For every completed or blocked phase:

1. Create one new entry using the template below.
2. Include concrete evidence (test names, command outputs, query results).
3. Record at least one trap or false start.
4. Record unresolved risks and explicit next-agent actions.

If this file is not updated, the phase is not complete.

---

## Entry Template

### [YYYY-MM-DD] Phase X — <Title>
- Status: `Done` | `Blocked`
- Owner/Agent:
- PRD Section(s):
- Scope Executed:
- Key Decisions:
- What Broke / Traps Hit:
- Fixes Applied:
- Validation Evidence:
- Risks Left Open:
- Next Agent Starts With:
- What We'd Do Differently Next Time:

---

## Baseline Entry

### [2026-05-05] v3 Execution Setup — Tracking Protocol Initialization
- Status: `Done`
- Owner/Agent: Codex
- PRD Section(s): Phase roadmap 13–16.5, acceptance criteria, first-moves ordering
- Scope Executed:
  - Rebased progress tracking onto PRD v3 phase model (13, 14, 14.5, 15, 16, 16.5).
  - Introduced mandatory end-of-phase documentation protocol.
  - Added deterministic handoff structure for "done / in-progress / remaining work".
- Key Decisions:
  - Treat `SOLANA_BUILD_PROGRESS.md` as the operational checklist and gate board.
  - Treat `SOLANA_LEARNING_LOG.md` as the narrative evidence ledger.
  - Require both updates before phase closeout is accepted.
- What Broke / Traps Hit:
  - Existing docs were anchored to pre-v3 phase language, which can cause agent drift and duplicated work.
- Fixes Applied:
  - Rewrote both tracking docs to align with PRD v3 terms and ordering.
- Validation Evidence:
  - File-level verification completed via shell readback after rewrite.
- Risks Left Open:
  - No execution work has started yet on TD-01..TD-05.
  - Phase 13 test additions are still pending implementation.
- Next Agent Starts With:
  - Implement Phase 13 identity pipeline and add 9 identity tests before any live-run activity.
- What We'd Do Differently Next Time:
  - Establish this tracking protocol before initial build to reduce retroactive cleanup.

### [2026-05-05] Phase 13 — Deterministic Identity + Idempotency (Work Session 1)
- Status: `Blocked`
- Owner/Agent: Codex
- PRD Section(s): §4.1–4.4 identity model and required 9 tests
- Scope Executed:
  - Added `assign_identity_and_dedupe_batch()` to `services/solana/event_schema.py`.
  - Implemented deterministic grouping/sort/ordinal assignment using `raw_event_id` + `event_fingerprint`.
  - Added exact duplicate replay dedupe within each raw_event_id group.
  - Added `tests/solana/test_event_schema_identity.py` with 9 tests matching Phase 13 checklist themes.
- Key Decisions:
  - Keep identity assignment as a batch-level pass in `event_schema` so it can run immediately before validation/write.
  - Preserve `normalized_event_id = raw_event_id:fingerprint[:8]` as the durable deterministic key.
- What Broke / Traps Hit:
  - Test execution environment missing pytest (`python3 -m pytest` fails with `No module named pytest`).
- Fixes Applied:
  - No code rollback; work preserved and phase status marked blocked pending test runtime availability.
- Validation Evidence:
  - Attempted:
    - `python3 -m pytest -q tests/solana/test_event_schema_identity.py`
    - `python3 -m pytest -q tests/solana/test_event_schema.py`
  - Both failed due to missing pytest module in environment.
- Risks Left Open:
  - Identity logic is unverified at runtime until tests execute.
  - BigQuery MERGE idempotency work (TD-03) not yet implemented in this session.
- Next Agent Starts With:
  - Install or activate test environment with pytest, run new and existing suites, then complete remaining Phase 13 acceptance gates.
- What We'd Do Differently Next Time:
  - Verify test runtime availability before making phase-scoped code changes to shorten feedback loop.

### [2026-05-05] Phase 13 — Deterministic Identity + Idempotency (Work Session 2)
- Status: `Done`
- Owner/Agent: Codex
- PRD Section(s): §4.1–4.4 identity model, §7.3 MERGE behavior
- Scope Executed:
  - Added deterministic batch identity assignment and replay dedupe in `assign_identity_and_dedupe_batch()`.
  - Added `tests/solana/test_event_schema_identity.py` with 9 identity tests.
  - Updated BigQuery writer to prefer staged-table `MERGE` keyed on `normalized_event_id`.
  - Added writer test asserting MERGE key uses `normalized_event_id` and not `canonical_key`.
- Key Decisions:
  - Keep normalized-event identity deterministic and independent of ingest order via fingerprint sort.
  - Use `normalized_event_id` as the only merge dedupe key.
  - Preserve compatibility path for minimal/mock clients by retaining insert fallback behavior.
- What Broke / Traps Hit:
  - Initial MERGE-path test failed because writer attempted legacy insert path when mock lacked merge-compatible behavior.
  - Merge implementation briefly depended on `google.cloud.bigquery` import availability.
- Fixes Applied:
  - Removed strict merge-path import dependency and simplified staging load call.
  - Added explicit merge-path unit test coverage.
- Validation Evidence:
  - `.venv/bin/python -m pytest -q tests/solana/test_event_schema_identity.py tests/solana/test_event_schema.py` -> `56 passed`.
  - `.venv/bin/python -m pytest -q tests/solana tests/test_solana_api_endpoints.py` -> `366 passed`.
- Risks Left Open:
  - Need explicit end-to-end invocation point to guarantee `assign_identity_and_dedupe_batch()` is called in live promotion pipeline.
  - Phase 14/14.5 items (finality policy + semantic RPC validation) not started.
- Next Agent Starts With:
  - Wire identity batch pass into integrated ingestion->validation->writer execution path and run replay scenario validation.
  - Begin Phase 14 finalized live-run gates after integration point is confirmed.
- What We'd Do Differently Next Time:
  - Add pipeline-integration tests for identity assignment at the same time as unit tests to avoid function-level drift.

### [2026-05-05] Phase 13 — Deterministic Identity + Idempotency (Work Session 3, Closeout)
- Status: `Done`
- Owner/Agent: Codex
- PRD Section(s): §5 canonical envs (TD-09 tie-in), Phase 13 AC closeout evidence
- Scope Executed:
  - Added canonical-env handling for token mint (`SOLANA_TOKEN_MINT`) with legacy `SOLANA_TOKEN_MINT_ALLOWLIST` fallback.
  - Added ingestion config tests for canonical var precedence and legacy fallback behavior.
  - Updated solana integration doc env table naming for circuit breaker vars to canonical v3 names.
  - Re-ran full Solana + API endpoint suite after all changes.
- Key Decisions:
  - Use compatibility-first migration for env names to avoid breaking existing deployments while moving toward canonical v3 config.
- What Broke / Traps Hit:
  - None in this session after prior merge-path fix.
- Fixes Applied:
  - N/A
- Validation Evidence:
  - `.venv/bin/python -m pytest -q tests/solana/test_ingestion_adapter.py tests/solana/test_event_schema_identity.py tests/solana/test_event_schema.py` -> `83 passed`.
  - `.venv/bin/python -m pytest -q tests/solana tests/test_solana_api_endpoints.py` -> `368 passed`.
- Risks Left Open:
  - Remaining Phase 14 live data acceptance checks require real RPC credentials and observed activity.
- Next Agent Starts With:
  - Execute Phase 14 first live finalized run and health-field verification.
- What We'd Do Differently Next Time:
  - Add a dedicated env-name regression test that compares documented and code-recognized Solana env vars in one assertion set.

### [2026-05-05] Phase 14 — Live RPC First Slice + Finality Policy (Work Session 1)
- Status: `Blocked`
- Owner/Agent: Codex
- PRD Section(s): §6.1 finality policy + health state fields
- Scope Executed:
  - Added `ingestion_state`, `observation_state`, and `commitment_level` to Solana API state contract.
  - Extended `SolanaCache.record_run()` to populate those fields with default inference and override support.
  - Added/updated tests for new state fields and endpoint payload shape.
- Key Decisions:
  - Default `commitment_level` to `finalized`.
  - Infer `observation_state` as `observed` when events are written, `no_recent_activity` when signatures exist but no events, and `unavailable` on failed/no data runs.
  - Keep explicit override hooks so Phase 14.5 semantic RPC logic can set `ambiguous_empty` and `provider_lagging`.
- What Broke / Traps Hit:
  - None functionally; blocker is runtime environment readiness for live validation.
- Fixes Applied:
  - N/A
- Validation Evidence:
  - `.venv/bin/python -m pytest -q tests/solana/test_api_integration.py tests/test_solana_api_endpoints.py` -> `55 passed`.
  - `.venv/bin/python -m pytest -q tests/solana tests/test_solana_api_endpoints.py` -> `370 passed`.
- Risks Left Open:
  - Cannot complete live acceptance checks without RPC env vars and watched addresses.
- Next Agent Starts With:
  - Set `SOLANA_RPC_PRIMARY_URL`, `SOLANA_WATCHED_ADDRESSES`, `SOLANA_START_SIGNATURE`, `SOLANA_COMMITMENT=finalized`; run first live ingestion and manual 3-event verification.
- What We'd Do Differently Next Time:
  - Add a repo script that validates required Phase 14 live env vars before starting execution to fail fast.

### [2026-05-05] Phase 15 — Durable Scheduler + Checkpoint Safety (Work Session 1)
- Status: `In Progress`
- Owner/Agent: Codex
- PRD Section(s): §6.3 scheduler + production checkpoint guard
- Scope Executed:
  - Added `scripts/run_solana_ingestion.py` with:
    - `--once` and `--loop` modes
    - SIGTERM/SIGINT graceful shutdown handling
    - structured JSON run logs
    - exponential backoff on failures (capped)
    - production guard rejecting `ENV=production` with `SOLANA_CHECKPOINT_BACKEND=local_file`
  - Added script bootstrap path handling so it runs from repo root.
- Key Decisions:
  - Keep scheduler script lightweight and adapter-driven, with state signals emitted from `IngestionRunResult`.
  - Preserve non-destructive behavior: no forced checkpoint advancement logic outside adapter/store flow.
- What Broke / Traps Hit:
  - Initial script invocation failed with `ModuleNotFoundError: services` due to path context.
- Fixes Applied:
  - Added repo-root path injection at script startup.
- Validation Evidence:
  - `.venv/bin/python scripts/run_solana_ingestion.py --help` executes successfully.
  - Regression suite remained green:
    - `.venv/bin/python -m pytest -q tests/solana tests/test_solana_api_endpoints.py` -> `371 passed`.
- Risks Left Open:
  - Full checkpoint safety invariant still depends on end-to-end promotion orchestration wiring and live run behavior.
  - Live Phase 14 execution remains blocked by missing runtime env vars.
- Next Agent Starts With:
  - Wire scheduler to full promote/validate/checkpoint lifecycle when that orchestrator path is finalized.
  - Execute live run once RPC env vars are present.
- What We'd Do Differently Next Time:
  - Add first-class integration tests for scheduler loop behavior with a mocked adapter result stream.

### [2026-05-05] Phase 14 — Validator Evidence Fixes (Fix A + Fix C/E)
- Status: `Done`
- Owner/Agent: Codex
- PRD Section(s): Phase 14 validation gates; diagnostic output from rejected live batch
- Scope Executed:
  - Fixed placeholder propagation by resolving `__account_index_N__` into token account pubkeys during `apply_owner_and_amount_resolution()`.
  - Tightened transfer truth so instruction-only token-program evidence does not set `transfer_detected=True` without watched-mint balance movement.
  - Skipped watched-mint/no-delta transactions in the ingestion adapter instead of emitting false transfer rows.
  - Added regression tests for placeholder rewrite and USDC-present/no-delta skip behavior.
- Key Decisions:
  - Keep validator gates strict; fix upstream data shape instead of relaxing validation.
  - Preserve instruction evidence as supporting context only unless balance-delta proof confirms a watched transfer.
- What Broke / Traps Hit:
  - The original bug came from a legitimate intermediate representation (`__account_index_N__`) escaping into validation.
  - Instruction evidence was too eager in mixed-token transactions where USDC appeared in balance tables but had zero movement.
- Fixes Applied:
  - `services/solana/event_schema.py`: rewrites placeholder token accounts using `account_keys_resolved`.
  - `services/solana/transfer_truth.py`: requires watched-mint balance movement for transfer detection.
  - `services/solana/ingestion_adapter.py`: skips no-transfer reasons consistently.
- Validation Evidence:
  - `.venv/bin/python -m pytest -q tests/solana/test_owner_resolver.py tests/solana/test_ingestion_adapter.py tests/solana/test_solana_parser.py` -> `108 passed`.
  - `.venv/bin/python -m pytest -q tests/solana/test_event_schema.py tests/solana/test_validator.py` -> `89 passed`.
  - `.venv/bin/python -m pytest -q tests/solana tests/test_solana_api_endpoints.py` -> `373 passed`.
  - Diagnostic replay of same live batch: validation approved, failed_gates=[], placeholder_rows=0, transfer_truth_fail_rows=0, JSONL not written, checkpoint not advanced.
- Risks Left Open:
  - Need one JSONL-only acceptance rerun to complete Phase 14 write/checkpoint/health outputs.
- Next Agent Starts With:
  - Rerun the Phase 14 first slice with JSONL fallback enabled; validation should now pass and checkpoint can advance after write success.
- What We'd Do Differently Next Time:
  - Treat placeholder values as internal-only types with a dedicated test ensuring they never cross a validation boundary.

### [2026-05-05] Phase 14 — Finalized JSONL First Slice Closeout
- Status: `Done`
- Owner/Agent: Codex
- PRD Section(s): §6.1 first run sequence; Phase 14 AC
- Scope Executed:
  - Ran the accepted live finalized first slice using JSONL fallback only.
  - Confirmed no GCP/BigQuery path was used (`bq_client_used=false`, `gcp_used=false`).
  - Wrote 27 validated Solana rows to `data/solana_events_buffer.jsonl`.
  - Advanced checkpoint only after validation and fallback write succeeded.
- Key Decisions:
  - Reused the held checkpoint for `2MFoS3MP...`, which still pointed at the provided start signature because prior rejected runs had not advanced it.
  - Preserved runtime artifacts as local evidence and left them uncommitted.
- What Broke / Traps Hit:
  - Helius returned one transient 429 during the run; retry succeeded and the final run status remained `ok`.
- Fixes Applied:
  - N/A during this closeout; prior Fix A + Fix C/E enabled validation approval.
- Validation Evidence:
  - `signatures_fetched=50`, `transactions_fetched=50`, `transactions_processed=27`, `raw_events=27`.
  - `validation.approved=true`, `failed_gates=[]`.
  - JSONL line count: `0 -> 27`.
  - Checkpoint advanced to signature `4zyshM6cGpRvrZ7jTL16g4oj5hot5kn9GwkPDm3S47ZqAPMNpL3TSrnYDQwEiX6FzFTAsvGs3GaHiHwEAb7Ps5Gh`, slot `417663930`.
  - Health: `freshness_state=stale`, `observation_state=observed`, `ingestion_state=succeeded`, `commitment_level=finalized`.
- Risks Left Open:
  - Health reported `stale`, not `fresh`, because the observed finalized data exceeded the configured freshness threshold at reporting time.
  - Runtime files `data/solana_alt_cache.json`, `data/solana_checkpoint.json`, and `data/solana_events_buffer.jsonl` should remain uncommitted unless deliberately promoted as fixtures.
- Next Agent Starts With:
  - Phase 14.5 semantic RPC validation and Phase 15 checkpoint invariant completion.
- What We'd Do Differently Next Time:
  - Add a dedicated one-command local Phase 14 runner that emits the acceptance JSON without ad hoc shell composition.

### [2026-05-05] Phase 14.5 — Semantic RPC Validation
- Status: `Done`
- Owner/Agent: Codex
- PRD Section(s): Semantic RPC Validation
- Scope Executed:
  - Added adapter-level detection for JSON-RPC `error`, null result, and malformed signature-result payloads.
  - Added ambiguous-empty tracking so three consecutive empty finalized windows become `observation_state=ambiguous_empty` and `ingestion_state=provider_lagging`.
  - Added provider lag detection against the configured finalized cursor slot before trusting empty windows.
  - Added wrong-commitment rejection before transaction processing so non-finalized responses cannot enter promotion candidates.
  - Added tests proving semantic failures keep the checkpoint at the seeded signature/slot with no promoted slot.
- Key Decisions:
  - Keep the adapter JSONL/dev-safe and promotion-averse; semantic failures degrade/fail the run instead of producing rows.
  - Use configured `start_slot` as the provider-lag reference because Phase 14.5 has no independent chain-tip oracle.
  - Preserve strict validation posture rather than relaxing downstream gates.
- What Broke / Traps Hit:
  - Provider lag needed a distinct health state; treating it as generic failure would hide a provider quality problem.
- Fixes Applied:
  - `services/solana/ingestion_adapter.py`: semantic guards, ambiguous-empty state, provider-lag state, wrong-commitment rejection.
  - `services/solana/rpc_client.py`: added `get_slot()` for finalized provider slot checks.
  - `tests/solana/test_ingestion_adapter.py`: added Phase 14.5 semantic RPC regression tests.
- Validation Evidence:
  - `.venv/bin/python -m pytest -q tests/solana/test_ingestion_adapter.py` -> `35 passed`.
  - `.venv/bin/python -m pytest -q tests/solana tests/test_solana_api_endpoints.py` -> `379 passed, 1 warning`.
  - `.venv/bin/python -m pytest -q` -> collection blocked by missing local dependencies (`pandas`, `google`, `dotenv`, `fastapi`, `pydantic`).
- Risks Left Open:
  - Provider lag detection is relative to the configured cursor slot; a future phase may add an independent reference/provider comparison if required.
- Next Agent Starts With:
  - Phase 15 remains paused by user request. Do not start it without explicit approval.
- What We'd Do Differently Next Time:
  - Add a tiny semantic-RPC fixture module so future providers can be regression-tested without expanding the adapter test double.

### [2026-05-05] Phase 15 — Durable Scheduler + Checkpoint Safety
- Status: `Done`
- Owner/Agent: Codex
- PRD Section(s): Durable Scheduler + Checkpoint Safety
- Scope Executed:
  - Completed `scripts/run_solana_ingestion.py` with `--once`, `--loop`, `--dry-run`, structured JSON logs, and interruptible sleep for clean shutdown.
  - Added checkpoint backend hierarchy for `local_file`, `gcs`, and `bigquery_metadata`.
  - Added named `ProductionCheckpointError` for `ENV=production` plus `local_file`.
  - Added checkpoint-advance invariant logic and tests covering failed validation, failed write, `circuit_open`, `ambiguous_empty`, and `provider_lagging`.
  - Added scheduler tests for backend resolution, dry run logs, and SIGTERM loop shutdown.
  - Documented repo-wide missing local dependency setup in `docs/local-test-dependencies.md`.
- Key Decisions:
  - Keep Phase 15 from performing promotion or checkpoint advance directly; it only exposes a strict `checkpoint_advance_allowed` gate until Phase 16 promotion wiring exists.
  - Treat `gcs` and `bigquery_metadata` as accepted backend names that fail explicitly if selected before remote store wiring is implemented.
  - Add a dry-run mode to allow deterministic scheduler acceptance without RPC, JSONL writes, GCP, or BigQuery.
- What Broke / Traps Hit:
  - Repo-wide pytest still fails at collection because the active `.venv` lacks packages already pinned in `requirements.txt`.
  - SIGTERM shutdown testing produced the existing LibreSSL urllib3 warning on stderr; no traceback or scheduler crash occurred.
- Fixes Applied:
  - `scripts/run_solana_ingestion.py`: backend hierarchy, named production error, interruptible shutdown sleep, dry run, structured invariant fields.
  - `tests/solana/test_scheduler.py`: 17 focused tests.
  - `docs/local-test-dependencies.md`: local dependency recovery instructions.
- Validation Evidence:
  - `.venv/bin/python -m pytest -q tests/solana/test_scheduler.py` -> `17 passed, 1 warning`.
  - `.venv/bin/python -m pytest -q tests/solana tests/test_solana_api_endpoints.py` -> `396 passed, 1 warning`.
  - `.venv/bin/python -m pytest -q` -> collection blocked by missing local dependencies (`pandas`, `google`, `dotenv`, `fastapi`, `pydantic`).
  - `ENV=development SOLANA_CHECKPOINT_BACKEND=local_file SOLANA_COMMITMENT=finalized .venv/bin/python scripts/run_solana_ingestion.py --once --dry-run` -> exit `0`, emitted structured `startup` and `solana_ingestion_run` logs.
- Risks Left Open:
  - Remote checkpoint backends are intentionally not connected to GCP/BigQuery in this phase.
  - Full repo test execution needs local dependency installation before it can move past collection.
- Next Agent Starts With:
  - Phase 16 remains not started. Wait for explicit user approval before beginning BigQuery promotion/tooling work.
- What We'd Do Differently Next Time:
  - Keep a `requirements-dev.txt` or lockfile and a bootstrap check so repo-wide pytest fails with one actionable setup message instead of many import errors.

### [2026-05-05] Phase 16 — Idempotent BigQuery Promotion + Tooling
- Status: `Done`
- Owner/Agent: Codex
- PRD Section(s): Idempotent BigQuery Promotion + Tooling
- Scope Executed:
  - Added BigQuery create-table tooling that generates DDL directly from `BQ_SCHEMA`.
  - Added schema contract helpers for numeric type enforcement, clustering fields, partition field, and S3-readiness field reporting.
  - Added `watched_address` to the normalized Solana event and BQ schema because Phase 16 clustering requires it.
  - Verified DDL uses `RANGE_BUCKET(slot, GENERATE_ARRAY(...))` and clusters by `token_mint`, `watched_address`, `raw_event_id`.
  - Verified the MERGE `ON` clause is exactly `target.normalized_event_id = source.normalized_event_id`.
  - Added an in-memory MERGE replay test proving the same batch written twice yields one target row.
  - Updated `.env.example` with Solana RPC/checkpoint/BQ settings.
  - Added `docs/solana-s3-readiness-checklist.md`.
- Key Decisions:
  - Keep table tooling dry-run by default; require `--execute` for real BigQuery calls.
  - Keep schema as a single source of truth in `BQ_SCHEMA`; scripts import it instead of duplicating field definitions.
  - Treat repo-wide pytest dependency failure as an environment setup issue because the missing packages are already pinned in `requirements.txt`.
- What Broke / Traps Hit:
  - Adding `watched_address` to required fields broke older validator fixtures until those fixtures were updated.
  - Repo-wide pytest remains blocked during collection because the active `.venv` is not fully hydrated.
- Fixes Applied:
  - `services/solana/event_schema.py`: added `watched_address` to normalized output and required fields.
  - `services/solana/ingestion_adapter.py`: stamps each emitted raw event with the watched address that produced it.
  - `services/solana/bigquery_writer.py`: added DDL/schema contract/S3-readiness helpers and `watched_address` schema field.
  - `tests/solana/test_validator.py`: updated valid fixtures for the new required field.
- Validation Evidence:
  - `.venv/bin/python -m pytest -q tests/solana/test_bigquery_phase16.py tests/solana/test_event_schema.py tests/solana/test_validator.py` -> `100 passed`.
  - `.venv/bin/python -m pytest -q tests/solana tests/test_solana_api_endpoints.py` -> `407 passed, 1 warning`.
  - `.venv/bin/python -m pytest -q` -> collection blocked by missing local dependencies (`pandas`, `google`, `dotenv`, `fastapi`, `pydantic`).
  - `scripts/create_solana_bq_table.py --print-ddl` dry run -> `executed=false`, `field_count=44`, `schema_contract_violations=[]`, `s3_readiness_missing=[]`.
- Risks Left Open:
  - Full repo and EVM test execution requires dependency installation into `.venv`.
  - DDL execution against live BigQuery was not performed in this phase; dry-run/tooling only.
- Next Agent Starts With:
  - Phase 16.5 remains not started. Wait for explicit approval before shadow S3 signal validation.
- What We'd Do Differently Next Time:
  - Add a bootstrap dependency check before pytest so missing pinned packages produce one actionable setup message.
