"""
Phase 8 — Solana API + Cache Integration Tests.

Tests SolanaAPIState serialization, SolanaCache state management, freshness
propagation, and the module-level singleton. All time-dependent paths use an
injectable clock.
"""

from __future__ import annotations

import pytest

from services.solana.freshness import (
    FreshnessMonitor,
    HEALTH_FRESH,
    HEALTH_STALE,
    HEALTH_UNAVAILABLE,
)
from services.solana.api_integration import (
    SolanaAPIState,
    SolanaCache,
    get_default_cache,
    set_default_cache,
    get_solana_api_state,
    _map_freshness_to_chain_health,
)


# ---------------------------------------------------------------------------
# Clock fixture
# ---------------------------------------------------------------------------

class FakeClock:
    def __init__(self, now: float = 1_700_000_000.0):
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


BLOCK_TIME = 1_700_000_000
SLOT = 300_000_000


def _monitor(fresh=300, stale=3600, now=BLOCK_TIME):
    clock = FakeClock(now=float(now))
    return FreshnessMonitor(
        freshness_threshold_seconds=fresh,
        stale_threshold_seconds=stale,
        _clock=clock,
    ), clock


# ---------------------------------------------------------------------------
# SolanaAPIState
# ---------------------------------------------------------------------------

class TestSolanaAPIState:
    def test_default_is_unavailable(self):
        s = SolanaAPIState()
        assert s.freshness_state == HEALTH_UNAVAILABLE

    def test_is_available_false_when_unavailable(self):
        s = SolanaAPIState(freshness_state=HEALTH_UNAVAILABLE)
        assert s.is_available is False

    def test_is_available_true_when_fresh(self):
        s = SolanaAPIState(freshness_state=HEALTH_FRESH)
        assert s.is_available is True

    def test_is_available_true_when_stale(self):
        s = SolanaAPIState(freshness_state=HEALTH_STALE)
        assert s.is_available is True

    def test_is_fresh(self):
        assert SolanaAPIState(freshness_state=HEALTH_FRESH).is_fresh is True
        assert SolanaAPIState(freshness_state=HEALTH_STALE).is_fresh is False

    def test_is_stale(self):
        assert SolanaAPIState(freshness_state=HEALTH_STALE).is_stale is True
        assert SolanaAPIState(freshness_state=HEALTH_FRESH).is_stale is False

    def test_to_dict_contains_all_keys(self):
        s = SolanaAPIState()
        d = s.to_dict()
        expected = {
            "freshness_state", "lag_seconds", "ingestion_lag_seconds",
            "freshness_threshold_seconds", "stale_threshold_seconds",
            "last_slot", "last_block_time", "last_ingested_at",
            "last_run_status", "ingestion_state", "observation_state",
            "commitment_level", "last_run_at",
            "signatures_fetched", "transactions_processed",
            "transactions_degraded", "events_written",
            "last_validation_status", "last_validation_at",
            "last_error",
        }
        assert set(d.keys()) == expected

    def test_to_dict_values_serializable(self):
        """Verify to_dict() produces only JSON-serializable primitives."""
        import json
        s = SolanaAPIState(
            freshness_state=HEALTH_FRESH,
            lag_seconds=120.5,
            last_slot=SLOT,
            last_block_time=BLOCK_TIME,
            last_ingested_at="2026-05-04T00:00:00+00:00",
        )
        # Should not raise
        json.dumps(s.to_dict())

    def test_to_chain_health_dict_fresh(self):
        s = SolanaAPIState(freshness_state=HEALTH_FRESH, lag_seconds=10.0)
        d = s.to_chain_health_dict()
        assert d["status"] == "fresh"
        assert d["freshness_level"] == "fresh"
        assert d["cache_age_seconds"] == 10

    def test_to_chain_health_dict_stale(self):
        s = SolanaAPIState(freshness_state=HEALTH_STALE, lag_seconds=600.0)
        d = s.to_chain_health_dict()
        assert d["status"] == "degraded"
        assert d["freshness_level"] == "stale"

    def test_to_chain_health_dict_unavailable(self):
        s = SolanaAPIState(freshness_state=HEALTH_UNAVAILABLE)
        d = s.to_chain_health_dict()
        assert d["status"] == "unavailable"
        assert d["freshness_level"] == "unknown"

    def test_to_chain_health_dict_contains_run_fields(self):
        s = SolanaAPIState(
            freshness_state=HEALTH_FRESH,
            transactions_processed=42,
            events_written=40,
            last_run_status="ok",
        )
        d = s.to_chain_health_dict()
        assert d["transactions_processed"] == 42
        assert d["events_written"] == 40
        assert d["last_run_status"] == "ok"

    def test_none_lag_produces_none_cache_age(self):
        s = SolanaAPIState(freshness_state=HEALTH_UNAVAILABLE, lag_seconds=None)
        d = s.to_chain_health_dict()
        assert d["cache_age_seconds"] is None


# ---------------------------------------------------------------------------
# _map_freshness_to_chain_health
# ---------------------------------------------------------------------------

class TestMapFreshnessToChainHealth:
    def test_fresh_maps(self):
        status, level = _map_freshness_to_chain_health(HEALTH_FRESH)
        assert status == "fresh"
        assert level == "fresh"

    def test_stale_maps(self):
        status, level = _map_freshness_to_chain_health(HEALTH_STALE)
        assert status == "degraded"
        assert level == "stale"

    def test_unavailable_maps(self):
        status, level = _map_freshness_to_chain_health(HEALTH_UNAVAILABLE)
        assert status == "unavailable"
        assert level == "unknown"


# ---------------------------------------------------------------------------
# SolanaCache — initial state
# ---------------------------------------------------------------------------

class TestSolanaCache_Initial:
    def test_initial_state_is_unavailable(self):
        monitor, _ = _monitor()
        cache = SolanaCache(monitor=monitor)
        state = cache.get_state()
        assert state.freshness_state == HEALTH_UNAVAILABLE

    def test_initial_last_slot_is_none(self):
        monitor, _ = _monitor()
        cache = SolanaCache(monitor=monitor)
        assert cache.get_state().last_slot is None

    def test_initial_run_status_is_not_started(self):
        monitor, _ = _monitor()
        cache = SolanaCache(monitor=monitor)
        assert cache.get_state().last_run_status == "not_started"


# ---------------------------------------------------------------------------
# SolanaCache — record_run
# ---------------------------------------------------------------------------

class TestSolanaCache_RecordRun:
    def test_record_run_sets_fresh_state(self):
        monitor, clock = _monitor(now=BLOCK_TIME)
        cache = SolanaCache(monitor=monitor)
        cache.record_run(
            slot=SLOT,
            block_time=BLOCK_TIME,
            run_status="ok",
            signatures_fetched=10,
            transactions_processed=8,
            events_written=8,
        )
        state = cache.get_state()
        assert state.freshness_state == HEALTH_FRESH

    def test_record_run_stores_slot_and_block_time(self):
        monitor, _ = _monitor()
        cache = SolanaCache(monitor=monitor)
        cache.record_run(slot=SLOT, block_time=BLOCK_TIME, run_status="ok")
        state = cache.get_state()
        assert state.last_slot == SLOT
        assert state.last_block_time == BLOCK_TIME

    def test_record_run_stores_metrics(self):
        monitor, _ = _monitor()
        cache = SolanaCache(monitor=monitor)
        cache.record_run(
            slot=SLOT,
            block_time=BLOCK_TIME,
            run_status="degraded",
            signatures_fetched=20,
            transactions_processed=18,
            transactions_degraded=2,
            events_written=16,
        )
        state = cache.get_state()
        assert state.signatures_fetched == 20
        assert state.transactions_processed == 18
        assert state.transactions_degraded == 2
        assert state.events_written == 16
        assert state.last_run_status == "degraded"
        assert state.ingestion_state == "provider_lagging"
        assert state.observation_state == "observed"
        assert state.commitment_level == "finalized"

    def test_record_run_no_events_is_no_recent_activity(self):
        monitor, _ = _monitor()
        cache = SolanaCache(monitor=monitor)
        cache.record_run(
            slot=SLOT,
            block_time=BLOCK_TIME,
            run_status="ok",
            signatures_fetched=4,
            events_written=0,
        )
        state = cache.get_state()
        assert state.observation_state == "no_recent_activity"

    def test_record_run_custom_state_overrides(self):
        monitor, _ = _monitor()
        cache = SolanaCache(monitor=monitor)
        cache.record_run(
            slot=SLOT,
            block_time=BLOCK_TIME,
            run_status="degraded",
            commitment_level="confirmed",
            ingestion_state="circuit_open",
            observation_state="ambiguous_empty",
            signatures_fetched=5,
            events_written=0,
        )
        state = cache.get_state()
        assert state.commitment_level == "confirmed"
        assert state.ingestion_state == "circuit_open"
        assert state.observation_state == "ambiguous_empty"

    def test_record_run_stores_validation_status(self):
        monitor, _ = _monitor()
        cache = SolanaCache(monitor=monitor)
        cache.record_run(
            slot=SLOT,
            block_time=BLOCK_TIME,
            run_status="ok",
            validation_status="approved",
        )
        state = cache.get_state()
        assert state.last_validation_status == "approved"
        assert state.last_validation_at is not None

    def test_record_run_stores_error(self):
        monitor, _ = _monitor()
        cache = SolanaCache(monitor=monitor)
        cache.record_run(
            slot=SLOT,
            block_time=BLOCK_TIME,
            run_status="failed",
            error="RPC timeout",
        )
        assert cache.get_state().last_error == "RPC timeout"

    def test_second_record_run_overwrites_first(self):
        monitor, _ = _monitor()
        cache = SolanaCache(monitor=monitor)
        cache.record_run(slot=SLOT, block_time=BLOCK_TIME, run_status="ok", events_written=5)
        cache.record_run(slot=SLOT + 100, block_time=BLOCK_TIME + 10, run_status="ok", events_written=10)
        state = cache.get_state()
        assert state.last_slot == SLOT + 100
        assert state.events_written == 10


# ---------------------------------------------------------------------------
# SolanaCache — freshness transitions via live clock
# ---------------------------------------------------------------------------

class TestSolanaCache_FreshnessTransitions:
    def test_transitions_to_stale_with_time(self):
        monitor, clock = _monitor(fresh=300, stale=3600, now=BLOCK_TIME)
        cache = SolanaCache(monitor=monitor)
        cache.record_run(slot=SLOT, block_time=BLOCK_TIME, run_status="ok")
        assert cache.get_state().freshness_state == HEALTH_FRESH

        clock.advance(301)
        assert cache.get_state().freshness_state == HEALTH_STALE

    def test_transitions_to_unavailable_with_excessive_lag(self):
        monitor, clock = _monitor(fresh=300, stale=3600, now=BLOCK_TIME)
        cache = SolanaCache(monitor=monitor)
        cache.record_run(slot=SLOT, block_time=BLOCK_TIME, run_status="ok")
        clock.advance(3601)
        assert cache.get_state().freshness_state == HEALTH_UNAVAILABLE

    def test_new_run_resets_to_fresh(self):
        monitor, clock = _monitor(fresh=300, stale=3600, now=BLOCK_TIME)
        cache = SolanaCache(monitor=monitor)
        cache.record_run(slot=SLOT, block_time=BLOCK_TIME, run_status="ok")
        clock.advance(400)
        assert cache.get_state().freshness_state == HEALTH_STALE

        new_block_time = int(clock.now)
        cache.record_run(slot=SLOT + 50, block_time=new_block_time, run_status="ok")
        assert cache.get_state().freshness_state == HEALTH_FRESH

    def test_lag_seconds_increases_over_time(self):
        monitor, clock = _monitor(fresh=300, stale=3600, now=BLOCK_TIME)
        cache = SolanaCache(monitor=monitor)
        cache.record_run(slot=SLOT, block_time=BLOCK_TIME, run_status="ok")
        clock.advance(100)
        lag1 = cache.get_state().lag_seconds
        clock.advance(100)
        lag2 = cache.get_state().lag_seconds
        assert lag2 > lag1


# ---------------------------------------------------------------------------
# SolanaCache — reset
# ---------------------------------------------------------------------------

class TestSolanaCache_Reset:
    def test_reset_returns_to_unavailable(self):
        monitor, _ = _monitor()
        cache = SolanaCache(monitor=monitor)
        cache.record_run(slot=SLOT, block_time=BLOCK_TIME, run_status="ok")
        assert cache.get_state().freshness_state == HEALTH_FRESH
        cache.reset()
        assert cache.get_state().freshness_state == HEALTH_UNAVAILABLE

    def test_reset_clears_slot(self):
        monitor, _ = _monitor()
        cache = SolanaCache(monitor=monitor)
        cache.record_run(slot=SLOT, block_time=BLOCK_TIME, run_status="ok")
        cache.reset()
        assert cache.get_state().last_slot is None


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

class TestDefaultCache:
    def test_get_default_cache_returns_instance(self):
        assert isinstance(get_default_cache(), SolanaCache)

    def test_set_default_cache_replaces_singleton(self):
        monitor, _ = _monitor()
        custom = SolanaCache(monitor=monitor)
        set_default_cache(custom)
        assert get_default_cache() is custom

    def test_get_solana_api_state_returns_state(self):
        monitor, _ = _monitor()
        custom = SolanaCache(monitor=monitor)
        set_default_cache(custom)
        state = get_solana_api_state()
        assert isinstance(state, SolanaAPIState)

    def test_get_solana_api_state_reflects_recorded_run(self):
        monitor, _ = _monitor(now=BLOCK_TIME)
        custom = SolanaCache(monitor=monitor)
        set_default_cache(custom)
        custom.record_run(slot=SLOT, block_time=BLOCK_TIME, run_status="ok", events_written=5)
        state = get_solana_api_state()
        assert state.freshness_state == HEALTH_FRESH
        assert state.events_written == 5
