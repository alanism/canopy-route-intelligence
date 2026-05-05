#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
import traceback
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.solana.checkpoint import CheckpointStore
from services.solana.ingestion_adapter import IngestionConfig, SolanaIngestionAdapter


_STOP_REQUESTED = False
SAFE_TO_ADVANCE_STATES = {"succeeded"}
UNSAFE_OBSERVATION_STATES = {"ambiguous_empty", "unavailable"}
UNSAFE_INGESTION_STATES = {"failed", "circuit_open", "provider_lagging", "unavailable"}


class ProductionCheckpointError(RuntimeError):
    """Raised when production is configured with an unsafe checkpoint backend."""


class UnsupportedCheckpointBackendError(RuntimeError):
    """Raised when SOLANA_CHECKPOINT_BACKEND is not one of the supported backends."""


class RemoteCheckpointBackendNotConfigured(RuntimeError):
    """Raised for remote checkpoint backends until their credentials are wired."""


class CheckpointBackend:
    """Factory for a checkpoint store implementation."""

    name = ""

    def build_store(self) -> CheckpointStore:
        raise NotImplementedError


class LocalFileCheckpointBackend(CheckpointBackend):
    name = "local_file"

    def __init__(self, path: Optional[str] = None) -> None:
        self.path = path or os.environ.get("SOLANA_CHECKPOINT_PATH") or "data/solana_checkpoint.json"

    def build_store(self) -> CheckpointStore:
        return CheckpointStore(checkpoint_path=self.path)


class GCSCheckpointBackend(CheckpointBackend):
    name = "gcs"

    def build_store(self) -> CheckpointStore:
        raise RemoteCheckpointBackendNotConfigured(
            "gcs checkpoint backend selected but remote checkpoint store is not wired in Phase 15"
        )


class BigQueryMetadataCheckpointBackend(CheckpointBackend):
    name = "bigquery_metadata"

    def build_store(self) -> CheckpointStore:
        raise RemoteCheckpointBackendNotConfigured(
            "bigquery_metadata checkpoint backend selected but remote checkpoint store is not wired in Phase 15"
        )


def _handle_stop(signum, _frame):
    global _STOP_REQUESTED
    _STOP_REQUESTED = True
    _log("shutdown_signal", signal=signum)


def _log(event: str, **kwargs):
    payload = {"event": event, **kwargs}
    print(json.dumps(payload, sort_keys=True), flush=True)


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default


def _sleep_until_stop(seconds: float) -> None:
    deadline = time.time() + max(seconds, 0.0)
    while not _STOP_REQUESTED and time.time() < deadline:
        time.sleep(min(0.1, max(deadline - time.time(), 0.0)))


def _resolve_checkpoint_backend() -> tuple[str, CheckpointBackend]:
    env_mode = (os.environ.get("ENV") or "development").strip().lower()
    backend = (os.environ.get("SOLANA_CHECKPOINT_BACKEND") or "local_file").strip().lower()
    if env_mode == "production" and backend == "local_file":
        raise ProductionCheckpointError(
            "local_file checkpoint backend is not allowed in production. "
            "Use gcs or bigquery_metadata."
        )
    if backend == "local_file":
        return env_mode, LocalFileCheckpointBackend()
    if backend == "gcs":
        return env_mode, GCSCheckpointBackend()
    if backend == "bigquery_metadata":
        return env_mode, BigQueryMetadataCheckpointBackend()
    raise UnsupportedCheckpointBackendError(
        f"Unsupported SOLANA_CHECKPOINT_BACKEND={backend!r}. "
        "Allowed: local_file, gcs, bigquery_metadata."
    )


def _checkpoint_advance_allowed(result, *, validation_approved: bool, write_succeeded: bool) -> bool:
    """Return True only when a run is safe to promote and checkpoint."""
    if not validation_approved or not write_succeeded:
        return False
    if result.run_status != "ok":
        return False
    if result.ingestion_state not in SAFE_TO_ADVANCE_STATES:
        return False
    if result.ingestion_state in UNSAFE_INGESTION_STATES:
        return False
    if result.observation_state in UNSAFE_OBSERVATION_STATES:
        return False
    return True


def _dry_run_once() -> int:
    _log(
        "solana_ingestion_run",
        dry_run=True,
        run_status="ok",
        ingestion_state="succeeded",
        observation_state="no_recent_activity",
        commitment_level=os.environ.get("SOLANA_COMMITMENT", "finalized"),
        signatures_fetched=0,
        transactions_processed=0,
        transactions_degraded=0,
        events_emitted=0,
        checkpoint_advance_allowed=False,
        validation_approved=False,
        write_succeeded=False,
        errors=[],
        elapsed_ms=0,
    )
    return 0


def _run_once(*, dry_run: bool = False) -> int:
    if dry_run:
        return _dry_run_once()

    config = IngestionConfig.from_env()
    _, checkpoint_backend = _resolve_checkpoint_backend()
    cp = checkpoint_backend.build_store()
    adapter = SolanaIngestionAdapter(config=config, checkpoint_store=cp)

    started = time.time()
    result = adapter.run()
    elapsed_ms = int((time.time() - started) * 1000)

    _log(
        "solana_ingestion_run",
        run_status=result.run_status,
        ingestion_state=result.ingestion_state,
        observation_state=result.observation_state,
        commitment_level=result.commitment_level,
        signatures_fetched=result.signatures_fetched,
        transactions_processed=result.transactions_processed,
        transactions_degraded=result.transactions_degraded,
        events_emitted=len(result.raw_events),
        checkpoint_backend=checkpoint_backend.name,
        checkpoint_advance_allowed=_checkpoint_advance_allowed(
            result,
            validation_approved=False,
            write_succeeded=False,
        ),
        validation_approved=False,
        write_succeeded=False,
        errors=result.errors,
        elapsed_ms=elapsed_ms,
    )

    # Keep cursor safe: only flush persisted checkpoint store state at run end.
    cp.flush()

    if result.run_status == "failed":
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Solana ingestion once or in a loop.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="Run one ingestion cycle and exit.")
    mode.add_argument("--loop", action="store_true", help="Run continuously.")
    parser.add_argument("--dry-run", action="store_true", help="Emit one structured dev run without RPC/write side effects.")
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    env_mode, backend = _resolve_checkpoint_backend()
    _log("startup", env=env_mode, checkpoint_backend=backend.name, dry_run=args.dry_run)

    interval_seconds = _int_env("SOLANA_INGESTION_INTERVAL_SECONDS", 15)
    backoff_seconds = 5.0

    if args.once:
        return _run_once(dry_run=args.dry_run)

    while not _STOP_REQUESTED:
        try:
            code = _run_once(dry_run=args.dry_run)
            if code != 0:
                _log("run_failed", next_retry_seconds=backoff_seconds)
                _sleep_until_stop(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2.0, 300.0)
            else:
                backoff_seconds = 5.0
                _sleep_until_stop(interval_seconds)
        except Exception as exc:
            _log(
                "run_exception",
                error=str(exc),
                traceback="".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
                next_retry_seconds=backoff_seconds,
            )
            _sleep_until_stop(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2.0, 300.0)

        if _STOP_REQUESTED:
            break

    _log("shutdown_complete")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(0)
