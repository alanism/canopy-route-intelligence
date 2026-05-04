"""
Phase 2 — Persistent Checkpointing.

Prevents re-ingestion, genesis scans, and stateless-container failure.

Rules
-----
- Persist checkpoint outside process memory (file-based JSON).
- Use slot PLUS signature — do not resume from slot alone.
- Do not scan from genesis if checkpoint is missing.
- Checkpoint is written ONLY after successful batch promotion, never before.
- Never advance checkpoint past unvalidated data.
- One checkpoint entry per (chain, token_mint, watched_address) triple.

Missing checkpoint behavior
---------------------------
If no checkpoint exists and no SOLANA_START_SIGNATURE / SOLANA_START_SLOT
is configured, ingestion refuses to run and raises MissingCheckpointError.

Checkpoint schema (per entry)
------------------------------
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
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger("canopy.solana.checkpoint")

DEFAULT_CHECKPOINT_PATH = os.path.join("data", "solana_checkpoint.json")

# Valid ingestion status values
INGESTION_STATUS_OK = "ok"
INGESTION_STATUS_DEGRADED = "degraded"
INGESTION_STATUS_FAILED = "failed"
VALID_STATUSES = frozenset({INGESTION_STATUS_OK, INGESTION_STATUS_DEGRADED, INGESTION_STATUS_FAILED})


class MissingCheckpointError(Exception):
    """
    Raised when no checkpoint exists and no start position is configured.

    Callers must handle this by requiring the operator to configure
    SOLANA_START_SIGNATURE or SOLANA_START_SLOT before re-running.
    """


class CheckpointCorruptError(Exception):
    """Raised when the checkpoint file cannot be parsed or fails integrity check."""


# ---------------------------------------------------------------------------
# Checkpoint key
# ---------------------------------------------------------------------------

def _checkpoint_key(chain: str, token_mint: str, watched_address: str) -> str:
    """Deterministic key for one (chain, token_mint, watched_address) triple."""
    raw = f"{chain}|{token_mint}|{watched_address}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16] + f":{chain}:{watched_address[:8]}"


# ---------------------------------------------------------------------------
# CheckpointEntry — typed view of one checkpoint record
# ---------------------------------------------------------------------------

class CheckpointEntry:
    """
    Immutable view of one checkpoint record.

    Created by CheckpointStore.get() and updated via CheckpointStore.advance().
    """

    __slots__ = (
        "chain", "token_mint", "watched_address",
        "last_processed_signature", "last_processed_slot",
        "last_successful_run_at", "last_promoted_slot",
        "last_validated_at", "ingestion_status",
    )

    def __init__(self, data: dict[str, Any]) -> None:
        self.chain: str = data.get("chain", "solana")
        self.token_mint: str = data.get("token_mint", "")
        self.watched_address: str = data.get("watched_address", "")
        self.last_processed_signature: Optional[str] = data.get("last_processed_signature")
        self.last_processed_slot: Optional[int] = data.get("last_processed_slot")
        self.last_successful_run_at: Optional[str] = data.get("last_successful_run_at")
        self.last_promoted_slot: Optional[int] = data.get("last_promoted_slot")
        self.last_validated_at: Optional[str] = data.get("last_validated_at")
        self.ingestion_status: str = data.get("ingestion_status", INGESTION_STATUS_OK)

    def to_dict(self) -> dict[str, Any]:
        return {
            "chain": self.chain,
            "token_mint": self.token_mint,
            "watched_address": self.watched_address,
            "last_processed_signature": self.last_processed_signature,
            "last_processed_slot": self.last_processed_slot,
            "last_successful_run_at": self.last_successful_run_at,
            "last_promoted_slot": self.last_promoted_slot,
            "last_validated_at": self.last_validated_at,
            "ingestion_status": self.ingestion_status,
        }

    @property
    def resume_signature(self) -> Optional[str]:
        """
        The signature to pass as `before=` cursor to getSignaturesForAddress.

        Returns last_processed_signature if set, else None.
        """
        return self.last_processed_signature

    def __repr__(self) -> str:
        sig = (self.last_processed_signature or "")[:16]
        return (
            f"CheckpointEntry(chain={self.chain!r}, watched={self.watched_address[:8]!r}, "
            f"sig={sig!r}, slot={self.last_processed_slot}, status={self.ingestion_status!r})"
        )


# ---------------------------------------------------------------------------
# CheckpointStore — file-backed, atomic writes
# ---------------------------------------------------------------------------

class CheckpointStore:
    """
    File-backed checkpoint store for Solana ingestion.

    One store instance is shared across all watched addresses in a run.
    Writes are atomic (tmp + os.replace). Reads validate existence before use.

    Usage
    -----
    store = CheckpointStore()

    # At run start — get resume cursor:
    entry = store.get_or_seed(chain, mint, watched_address, start_signature, start_slot)

    # Pass entry.resume_signature to SolanaIngestionAdapter.run()

    # After successful batch promotion:
    store.advance(
        chain, mint, watched_address,
        last_processed_signature=sig,
        last_processed_slot=slot,
        ingestion_status="ok",
        promoted=True,
    )
    """

    def __init__(self, checkpoint_path: str = DEFAULT_CHECKPOINT_PATH) -> None:
        self._path = checkpoint_path
        self._store: dict[str, dict[str, Any]] = {}
        self._dirty = False
        self._load()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def get(
        self,
        chain: str,
        token_mint: str,
        watched_address: str,
    ) -> Optional[CheckpointEntry]:
        """Return the checkpoint for this triple, or None if not found."""
        key = _checkpoint_key(chain, token_mint, watched_address)
        data = self._store.get(key)
        if data is None:
            return None
        return CheckpointEntry(data)

    def get_or_seed(
        self,
        chain: str,
        token_mint: str,
        watched_address: str,
        *,
        start_signature: Optional[str] = None,
        start_slot: Optional[int] = None,
    ) -> CheckpointEntry:
        """
        Return existing checkpoint or seed a new one from config.

        If no checkpoint exists AND no start position is provided,
        raises MissingCheckpointError — never scan from genesis silently.

        Parameters
        ----------
        start_signature:
            From SOLANA_START_SIGNATURE env or IngestionConfig.
        start_slot:
            From SOLANA_START_SLOT env or IngestionConfig.
        """
        existing = self.get(chain, token_mint, watched_address)
        if existing is not None:
            logger.debug(
                "Checkpoint found for %s/%s: sig=%s slot=%s",
                watched_address[:8], token_mint[:8],
                (existing.last_processed_signature or "")[:16],
                existing.last_processed_slot,
            )
            return existing

        # No checkpoint exists
        if not start_signature and not start_slot:
            raise MissingCheckpointError(
                f"Solana ingestion checkpoint missing for {watched_address[:8]}. "
                "Refusing to ingest from genesis. "
                "Configure SOLANA_START_SLOT or SOLANA_START_SIGNATURE."
            )

        # Seed from config
        logger.info(
            "No checkpoint for %s — seeding from config: sig=%s slot=%s",
            watched_address[:8],
            (start_signature or "")[:16],
            start_slot,
        )
        entry = CheckpointEntry({
            "chain": chain,
            "token_mint": token_mint,
            "watched_address": watched_address,
            "last_processed_signature": start_signature,
            "last_processed_slot": start_slot,
            "last_successful_run_at": None,
            "last_promoted_slot": None,
            "last_validated_at": None,
            "ingestion_status": INGESTION_STATUS_OK,
        })
        self._write_entry(chain, token_mint, watched_address, entry.to_dict())
        return entry

    def advance(
        self,
        chain: str,
        token_mint: str,
        watched_address: str,
        *,
        last_processed_signature: str,
        last_processed_slot: int,
        ingestion_status: str = INGESTION_STATUS_OK,
        promoted: bool = False,
        validated: bool = False,
    ) -> CheckpointEntry:
        """
        Advance the checkpoint after a successful processing step.

        Call with promoted=True ONLY after successful BigQuery batch promotion.
        Call with validated=True ONLY after reconciliation passes.

        Never advance past unvalidated data.
        """
        if ingestion_status not in VALID_STATUSES:
            raise ValueError(f"Invalid ingestion_status: {ingestion_status!r}")

        now = datetime.now(timezone.utc).isoformat()
        existing = self.get(chain, token_mint, watched_address)
        base = existing.to_dict() if existing else {
            "chain": chain,
            "token_mint": token_mint,
            "watched_address": watched_address,
            "last_promoted_slot": None,
            "last_validated_at": None,
        }

        updated = {
            **base,
            "last_processed_signature": last_processed_signature,
            "last_processed_slot": last_processed_slot,
            "last_successful_run_at": now,
            "ingestion_status": ingestion_status,
        }
        if promoted:
            updated["last_promoted_slot"] = last_processed_slot
        if validated:
            updated["last_validated_at"] = now

        self._write_entry(chain, token_mint, watched_address, updated)
        logger.info(
            "Checkpoint advanced: %s sig=%s slot=%s promoted=%s status=%s",
            watched_address[:8],
            last_processed_signature[:16],
            last_processed_slot,
            promoted,
            ingestion_status,
        )
        return CheckpointEntry(updated)

    def mark_failed(
        self,
        chain: str,
        token_mint: str,
        watched_address: str,
        *,
        reason: str = "",
    ) -> None:
        """
        Mark a checkpoint as failed without advancing the signature cursor.

        Used when an ingestion run fails catastrophically — does not move the
        cursor forward so the next run retries from the same position.
        """
        existing = self.get(chain, token_mint, watched_address)
        if existing is None:
            return
        updated = existing.to_dict()
        updated["ingestion_status"] = INGESTION_STATUS_FAILED
        self._write_entry(chain, token_mint, watched_address, updated)
        logger.warning("Checkpoint marked failed for %s: %s", watched_address[:8], reason)

    def flush(self) -> None:
        """Flush dirty checkpoint to disk atomically."""
        if not self._dirty:
            return
        try:
            os.makedirs(os.path.dirname(os.path.abspath(self._path)), exist_ok=True)
            tmp_path = self._path + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as fh:
                json.dump(self._store, fh, indent=2)
            os.replace(tmp_path, self._path)
            self._dirty = False
            logger.debug("Checkpoint flushed to %s (%d entries)", self._path, len(self._store))
        except OSError as exc:
            logger.error("Checkpoint flush failed: %s", exc)
            raise

    def all_entries(self) -> list[CheckpointEntry]:
        """Return all checkpoint entries (for health reporting)."""
        return [CheckpointEntry(v) for v in self._store.values()]

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _write_entry(
        self,
        chain: str,
        token_mint: str,
        watched_address: str,
        data: dict[str, Any],
    ) -> None:
        key = _checkpoint_key(chain, token_mint, watched_address)
        self._store[key] = data
        self._dirty = True
        self.flush()

    def _load(self) -> None:
        if not os.path.exists(self._path):
            return
        try:
            with open(self._path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
            if isinstance(raw, dict):
                self._store = raw
                logger.debug(
                    "Checkpoint loaded %d entries from %s", len(self._store), self._path
                )
        except (OSError, json.JSONDecodeError) as exc:
            raise CheckpointCorruptError(
                f"Checkpoint file at {self._path} is corrupt: {exc}"
            ) from exc
