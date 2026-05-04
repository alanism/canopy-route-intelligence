"""
Phase 7 — Solana Freshness + Health State Machine.

Tracks ingestion lag and produces a three-state health signal for the Solana
data layer. The health state is consumed by the API (Phase 8) and Dashboard
(Phase 9) to decide how to label Solana data.

Freshness states
----------------
- ``fresh``       — last ingested slot is recent; data can be shown as live
- ``stale``       — ingestion lag exceeds threshold; show with a warning label
- ``unavailable`` — no Solana data exists at all; never show as green or cached

Rules (non-negotiable)
-----------------------
1. Zero Solana data → ``unavailable``. NEVER green. NEVER cached stale-as-fresh.
2. Stale data MUST be labeled as degraded — shown with a warning, not silently
   served as fresh.
3. The freshness threshold is configurable via ``SOLANA_FRESHNESS_THRESHOLD_SECONDS``
   (default: 300 — 5 minutes). Tighten in production.
4. The health state machine is the single source of truth for data quality labels.
   No other layer should make freshness decisions independently.

Clock injection
---------------
``FreshnessMonitor`` accepts a ``_clock`` callable for deterministic tests
(same pattern as ``CircuitBreaker``). Production uses ``time.time``.

Usage
-----
    monitor = FreshnessMonitor.from_env()
    monitor.record_slot(slot=300_000_000, block_time=1_700_000_000)
    state = monitor.health_state()   # "fresh" | "stale" | "unavailable"
    report = monitor.health_report() # full dict for API / logging
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

logger = logging.getLogger("canopy.solana.freshness")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_FRESHNESS_THRESHOLD_SECONDS = 300   # 5 minutes
DEFAULT_STALE_THRESHOLD_SECONDS = 3600      # 1 hour → unavailable if exceeded

HEALTH_FRESH = "fresh"
HEALTH_STALE = "stale"
HEALTH_UNAVAILABLE = "unavailable"

_VALID_STATES = frozenset({HEALTH_FRESH, HEALTH_STALE, HEALTH_UNAVAILABLE})


# ---------------------------------------------------------------------------
# Slot record
# ---------------------------------------------------------------------------

@dataclass
class SlotRecord:
    """Last observed slot with its on-chain block time and wall-clock ingestion time."""
    slot: int
    block_time: int          # Unix timestamp from Solana (on-chain)
    ingested_at_wall: float  # time.time() at ingestion


# ---------------------------------------------------------------------------
# Health report
# ---------------------------------------------------------------------------

@dataclass
class HealthReport:
    state: str                          # "fresh" | "stale" | "unavailable"
    last_slot: Optional[int] = None
    last_block_time: Optional[int] = None
    last_ingested_at_wall: Optional[float] = None
    lag_seconds: Optional[float] = None        # now - last_block_time
    ingestion_lag_seconds: Optional[float] = None  # now - last_ingested_at_wall
    freshness_threshold_seconds: int = DEFAULT_FRESHNESS_THRESHOLD_SECONDS
    stale_threshold_seconds: int = DEFAULT_STALE_THRESHOLD_SECONDS

    def to_dict(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "last_slot": self.last_slot,
            "last_block_time": self.last_block_time,
            "last_ingested_at_wall": self.last_ingested_at_wall,
            "lag_seconds": self.lag_seconds,
            "ingestion_lag_seconds": self.ingestion_lag_seconds,
            "freshness_threshold_seconds": self.freshness_threshold_seconds,
            "stale_threshold_seconds": self.stale_threshold_seconds,
        }

    @property
    def is_fresh(self) -> bool:
        return self.state == HEALTH_FRESH

    @property
    def is_stale(self) -> bool:
        return self.state == HEALTH_STALE

    @property
    def is_unavailable(self) -> bool:
        return self.state == HEALTH_UNAVAILABLE


# ---------------------------------------------------------------------------
# FreshnessMonitor
# ---------------------------------------------------------------------------

class FreshnessMonitor:
    """
    Tracks the most-recently ingested Solana slot and computes a freshness state.

    Parameters
    ----------
    freshness_threshold_seconds:
        Lag above which state transitions from ``fresh`` → ``stale``.
    stale_threshold_seconds:
        Lag above which state transitions from ``stale`` → ``unavailable``.
        Must be > freshness_threshold_seconds.
    _clock:
        Callable that returns current wall time (default: ``time.time``).
        Inject a fake clock for deterministic tests.
    """

    def __init__(
        self,
        freshness_threshold_seconds: int = DEFAULT_FRESHNESS_THRESHOLD_SECONDS,
        stale_threshold_seconds: int = DEFAULT_STALE_THRESHOLD_SECONDS,
        *,
        _clock: Callable[[], float] = time.time,
    ) -> None:
        if stale_threshold_seconds <= freshness_threshold_seconds:
            raise ValueError(
                f"stale_threshold_seconds ({stale_threshold_seconds}) must be "
                f"> freshness_threshold_seconds ({freshness_threshold_seconds})"
            )
        self._freshness_threshold = freshness_threshold_seconds
        self._stale_threshold = stale_threshold_seconds
        self._clock = _clock
        self._last_record: Optional[SlotRecord] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record_slot(self, slot: int, block_time: int) -> None:
        """
        Record the most recently ingested slot.

        Call once per successful ingestion run with the highest slot observed.
        ``block_time`` is the on-chain Unix timestamp (seconds) from the
        ``getTransaction`` response — used to measure chain-side lag.

        Parameters
        ----------
        slot:
            Slot number (u64-compatible int).
        block_time:
            On-chain block timestamp in seconds since Unix epoch.
        """
        self._last_record = SlotRecord(
            slot=slot,
            block_time=block_time,
            ingested_at_wall=self._clock(),
        )
        logger.debug(
            "FreshnessMonitor: recorded slot=%d block_time=%d", slot, block_time
        )

    def health_state(self) -> str:
        """
        Return the current freshness state string.

        ``"fresh"`` | ``"stale"`` | ``"unavailable"``
        """
        return self.health_report().state

    def health_report(self) -> HealthReport:
        """
        Return a full HealthReport with lag metrics.

        If no slot has been recorded, returns ``unavailable`` with all
        lag fields as None — never raise on missing data.
        """
        now = self._clock()

        if self._last_record is None:
            return HealthReport(
                state=HEALTH_UNAVAILABLE,
                freshness_threshold_seconds=self._freshness_threshold,
                stale_threshold_seconds=self._stale_threshold,
            )

        r = self._last_record
        lag = now - r.block_time
        ingestion_lag = now - r.ingested_at_wall

        state = self._compute_state(lag)

        return HealthReport(
            state=state,
            last_slot=r.slot,
            last_block_time=r.block_time,
            last_ingested_at_wall=r.ingested_at_wall,
            lag_seconds=lag,
            ingestion_lag_seconds=ingestion_lag,
            freshness_threshold_seconds=self._freshness_threshold,
            stale_threshold_seconds=self._stale_threshold,
        )

    def reset(self) -> None:
        """Clear all recorded state — returns monitor to unavailable."""
        self._last_record = None

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_env(cls, *, _clock: Callable[[], float] = time.time) -> "FreshnessMonitor":
        """
        Construct from environment variables.

        Environment
        -----------
        SOLANA_FRESHNESS_THRESHOLD_SECONDS  (default: 300)
        SOLANA_STALE_THRESHOLD_SECONDS      (default: 3600)
        """
        fresh_thresh = int(
            os.environ.get("SOLANA_FRESHNESS_THRESHOLD_SECONDS", DEFAULT_FRESHNESS_THRESHOLD_SECONDS)
        )
        stale_thresh = int(
            os.environ.get("SOLANA_STALE_THRESHOLD_SECONDS", DEFAULT_STALE_THRESHOLD_SECONDS)
        )
        return cls(
            freshness_threshold_seconds=fresh_thresh,
            stale_threshold_seconds=stale_thresh,
            _clock=_clock,
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _compute_state(self, lag_seconds: float) -> str:
        """Map chain-side lag to a freshness state."""
        if lag_seconds <= self._freshness_threshold:
            return HEALTH_FRESH
        elif lag_seconds <= self._stale_threshold:
            return HEALTH_STALE
        else:
            return HEALTH_UNAVAILABLE


# ---------------------------------------------------------------------------
# Module-level convenience — singleton for simple integrations
# ---------------------------------------------------------------------------

_default_monitor: Optional[FreshnessMonitor] = None


def get_default_monitor() -> FreshnessMonitor:
    """
    Return the process-level FreshnessMonitor singleton.

    Created from environment variables on first call. Inject a custom
    monitor via ``set_default_monitor()`` in tests or bootstrapping code.
    """
    global _default_monitor
    if _default_monitor is None:
        _default_monitor = FreshnessMonitor.from_env()
    return _default_monitor


def set_default_monitor(monitor: FreshnessMonitor) -> None:
    """Replace the process-level singleton (for tests and bootstrapping)."""
    global _default_monitor
    _default_monitor = monitor
