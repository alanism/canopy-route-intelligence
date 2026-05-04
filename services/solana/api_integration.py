"""
Phase 8 — Solana API + Cache Integration.

Bridges the FreshnessMonitor (Phase 7) and the normalized event store to the
existing Canopy API shape. Provides:

1. ``SolanaAPIState`` — the serializable state object consumed by API endpoints
2. ``SolanaCache`` — an in-process cache that holds the latest Solana state,
   updated by the ingestion pipeline after each successful run
3. ``get_solana_api_state()`` — module-level accessor for API handlers

Design constraints
------------------
- Ethereum/Polygon cache machinery is UNCHANGED. Solana is an additive module.
- Zero Solana data → state is ``unavailable``. NEVER served as fresh or cached
  stale-as-fresh.
- Stale data is always labeled as degraded — the API payload includes
  ``freshness_state`` so callers can render a warning label.
- The API shape is compatible with the existing per-chain health structure in
  ``/health`` so the dashboard can add a "Solana" row without bespoke handling.
- No BigQuery polling loop — Solana is RPC-first. The cache is updated by the
  ingestion adapter after promotion, not by a periodic BigQuery poller.

Thread / async safety
---------------------
``SolanaCache`` is updated via reference reassignment (same pattern as
``api/cache.py`` — ``_cache = new_dict``). Python's GIL makes dict reference
replacement atomic for single-threaded async code. No locking needed for the
read path.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from services.solana.freshness import (
    HEALTH_FRESH,
    HEALTH_STALE,
    HEALTH_UNAVAILABLE,
    FreshnessMonitor,
    HealthReport,
    get_default_monitor,
)

logger = logging.getLogger("canopy.solana.api_integration")


# ---------------------------------------------------------------------------
# Solana API state
# ---------------------------------------------------------------------------

@dataclass
class SolanaAPIState:
    """
    Serializable Solana data layer state for API responses.

    All fields have safe defaults so the API never crashes on missing data.
    """
    # Freshness / health
    freshness_state: str = HEALTH_UNAVAILABLE      # "fresh" | "stale" | "unavailable"
    lag_seconds: Optional[float] = None
    ingestion_lag_seconds: Optional[float] = None
    freshness_threshold_seconds: int = 300
    stale_threshold_seconds: int = 3600

    # Last ingested slot
    last_slot: Optional[int] = None
    last_block_time: Optional[int] = None
    last_ingested_at: Optional[str] = None         # ISO8601 wall-clock

    # Ingestion run metrics (from IngestionRunResult)
    last_run_status: str = "not_started"           # "ok" | "degraded" | "failed" | "not_started"
    last_run_at: Optional[str] = None              # ISO8601
    signatures_fetched: int = 0
    transactions_processed: int = 0
    transactions_degraded: int = 0
    events_written: int = 0

    # Validation gate
    last_validation_status: Optional[str] = None  # "approved" | "rejected" | None
    last_validation_at: Optional[str] = None

    # Error tracking
    last_error: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "freshness_state": self.freshness_state,
            "lag_seconds": self.lag_seconds,
            "ingestion_lag_seconds": self.ingestion_lag_seconds,
            "freshness_threshold_seconds": self.freshness_threshold_seconds,
            "stale_threshold_seconds": self.stale_threshold_seconds,
            "last_slot": self.last_slot,
            "last_block_time": self.last_block_time,
            "last_ingested_at": self.last_ingested_at,
            "last_run_status": self.last_run_status,
            "last_run_at": self.last_run_at,
            "signatures_fetched": self.signatures_fetched,
            "transactions_processed": self.transactions_processed,
            "transactions_degraded": self.transactions_degraded,
            "events_written": self.events_written,
            "last_validation_status": self.last_validation_status,
            "last_validation_at": self.last_validation_at,
            "last_error": self.last_error,
        }

    # ------------------------------------------------------------------
    # Compatibility helpers for the /health endpoint chain-state shape
    # ------------------------------------------------------------------

    def to_chain_health_dict(self) -> dict[str, Any]:
        """
        Render in the same shape as the Polygon/Ethereum chain health entries
        in the /health response — so the dashboard can add a Solana row
        without bespoke client handling.

        Maps freshness_state → status/freshness_level using the same vocabulary
        the EVM cache uses ("fresh", "stale", "critical", "initializing", "error").
        """
        status, freshness_level = _map_freshness_to_chain_health(self.freshness_state)
        return {
            "status": status,
            "freshness_state": self.freshness_state,
            "freshness_level": freshness_level,
            "cache_age_seconds": (
                int(self.lag_seconds) if self.lag_seconds is not None else None
            ),
            "last_slot": self.last_slot,
            "last_block_time": self.last_block_time,
            "last_ingested_at": self.last_ingested_at,
            "last_run_status": self.last_run_status,
            "last_run_at": self.last_run_at,
            "transactions_processed": self.transactions_processed,
            "events_written": self.events_written,
            "last_error": self.last_error,
        }

    @property
    def is_available(self) -> bool:
        return self.freshness_state != HEALTH_UNAVAILABLE

    @property
    def is_fresh(self) -> bool:
        return self.freshness_state == HEALTH_FRESH

    @property
    def is_stale(self) -> bool:
        return self.freshness_state == HEALTH_STALE


def _map_freshness_to_chain_health(state: str) -> tuple[str, str]:
    """Map Solana freshness state to (status, freshness_level) for chain health."""
    if state == HEALTH_FRESH:
        return ("fresh", "fresh")
    elif state == HEALTH_STALE:
        return ("degraded", "stale")
    else:
        return ("unavailable", "unknown")


# ---------------------------------------------------------------------------
# Solana cache
# ---------------------------------------------------------------------------

class SolanaCache:
    """
    In-process cache for the latest Solana API state.

    Updated by ``record_run()`` after a successful ingestion + promotion cycle.
    Read by API handlers via ``get_state()``.

    No background poller — Solana state is pushed (ingestion triggers update),
    not pulled (periodic BigQuery query).
    """

    def __init__(self, monitor: Optional[FreshnessMonitor] = None) -> None:
        self._monitor = monitor or get_default_monitor()
        self._state = SolanaAPIState()   # starts unavailable

    def record_run(
        self,
        *,
        slot: int,
        block_time: int,
        run_status: str,
        signatures_fetched: int = 0,
        transactions_processed: int = 0,
        transactions_degraded: int = 0,
        events_written: int = 0,
        validation_status: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        """
        Update cache after a completed ingestion run.

        Should be called after BigQuery promotion (or fallback buffer write).
        Advances the FreshnessMonitor with the slot and block_time.

        Parameters
        ----------
        slot:
            Highest slot number processed in this run.
        block_time:
            On-chain block timestamp (seconds) for that slot.
        run_status:
            ``"ok"``, ``"degraded"``, or ``"failed"`` from IngestionRunResult.
        signatures_fetched:
            From IngestionRunResult.
        transactions_processed:
            From IngestionRunResult.
        transactions_degraded:
            From IngestionRunResult.
        events_written:
            Number of normalized events successfully written.
        validation_status:
            ``"approved"`` or ``"rejected"`` from Phase 6 validation gate.
        error:
            Last error string, if any.
        """
        # Advance freshness monitor with real on-chain data
        self._monitor.record_slot(slot, block_time)

        now_iso = _utcnow_iso()
        report = self._monitor.health_report()

        # Reference-replace the state dict (GIL-safe for async read path)
        self._state = SolanaAPIState(
            freshness_state=report.state,
            lag_seconds=report.lag_seconds,
            ingestion_lag_seconds=report.ingestion_lag_seconds,
            freshness_threshold_seconds=report.freshness_threshold_seconds,
            stale_threshold_seconds=report.stale_threshold_seconds,
            last_slot=report.last_slot,
            last_block_time=report.last_block_time,
            last_ingested_at=now_iso,
            last_run_status=run_status,
            last_run_at=now_iso,
            signatures_fetched=signatures_fetched,
            transactions_processed=transactions_processed,
            transactions_degraded=transactions_degraded,
            events_written=events_written,
            last_validation_status=validation_status,
            last_validation_at=now_iso if validation_status else None,
            last_error=error,
        )

        logger.info(
            "SolanaCache updated: slot=%d freshness=%s run_status=%s events=%d",
            slot, report.state, run_status, events_written,
        )

    def get_state(self) -> SolanaAPIState:
        """
        Return the current Solana API state.

        Always returns a complete SolanaAPIState — never raises.
        The state reflects the freshness monitor's current lag assessment.

        Note: lag is re-evaluated from the live clock on every call, so
        a previously-fresh state can transition to stale between calls even
        without a new ``record_run()``.
        """
        # Re-evaluate freshness from live clock (lag increases with real time)
        report = self._monitor.health_report()

        # If no data has ever been recorded, return the initial unavailable state
        if report.last_slot is None:
            return SolanaAPIState()

        # Refresh freshness fields from live monitor; preserve run metrics
        s = self._state
        return SolanaAPIState(
            freshness_state=report.state,
            lag_seconds=report.lag_seconds,
            ingestion_lag_seconds=report.ingestion_lag_seconds,
            freshness_threshold_seconds=report.freshness_threshold_seconds,
            stale_threshold_seconds=report.stale_threshold_seconds,
            last_slot=s.last_slot,
            last_block_time=s.last_block_time,
            last_ingested_at=s.last_ingested_at,
            last_run_status=s.last_run_status,
            last_run_at=s.last_run_at,
            signatures_fetched=s.signatures_fetched,
            transactions_processed=s.transactions_processed,
            transactions_degraded=s.transactions_degraded,
            events_written=s.events_written,
            last_validation_status=s.last_validation_status,
            last_validation_at=s.last_validation_at,
            last_error=s.last_error,
        )

    def reset(self) -> None:
        """Return cache to unavailable state and reset the freshness monitor."""
        self._monitor.reset()
        self._state = SolanaAPIState()


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_default_cache: Optional[SolanaCache] = None


def get_default_cache() -> SolanaCache:
    """
    Return the process-level SolanaCache singleton.

    Built from the default FreshnessMonitor on first call.
    Override with ``set_default_cache()`` in tests or bootstrapping code.
    """
    global _default_cache
    if _default_cache is None:
        _default_cache = SolanaCache()
    return _default_cache


def set_default_cache(cache: SolanaCache) -> None:
    """Replace the process-level singleton (for tests and bootstrapping)."""
    global _default_cache
    _default_cache = cache


def get_solana_api_state() -> SolanaAPIState:
    """
    Module-level convenience — get the current Solana API state.

    Equivalent to ``get_default_cache().get_state()``.
    """
    return get_default_cache().get_state()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
