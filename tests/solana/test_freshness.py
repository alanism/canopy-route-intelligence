"""
Phase 7 — Solana Freshness + Health State Machine Tests.

All time-dependent behaviour is tested with an injectable clock — no real
time.sleep() calls anywhere in this file.
"""

from __future__ import annotations

import pytest

from services.solana.freshness import (
    FreshnessMonitor,
    HealthReport,
    SlotRecord,
    HEALTH_FRESH,
    HEALTH_STALE,
    HEALTH_UNAVAILABLE,
    get_default_monitor,
    set_default_monitor,
    DEFAULT_FRESHNESS_THRESHOLD_SECONDS,
    DEFAULT_STALE_THRESHOLD_SECONDS,
)


# ---------------------------------------------------------------------------
# Clock helpers
# ---------------------------------------------------------------------------

class FakeClock:
    """Controllable wall clock for deterministic tests."""
    def __init__(self, now: float = 1_700_000_000.0):
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


# Slot / block_time fixtures
SLOT = 300_000_000
BLOCK_TIME = 1_700_000_000   # Unix timestamp matching FakeClock default


# ---------------------------------------------------------------------------
# FreshnessMonitor — construction
# ---------------------------------------------------------------------------

class TestFreshnessMonitorConstruction:
    def test_default_thresholds(self):
        m = FreshnessMonitor()
        assert m._freshness_threshold == DEFAULT_FRESHNESS_THRESHOLD_SECONDS
        assert m._stale_threshold == DEFAULT_STALE_THRESHOLD_SECONDS

    def test_custom_thresholds(self):
        m = FreshnessMonitor(freshness_threshold_seconds=60, stale_threshold_seconds=600)
        assert m._freshness_threshold == 60
        assert m._stale_threshold == 600

    def test_raises_when_stale_not_greater_than_fresh(self):
        with pytest.raises(ValueError, match="must be >"):
            FreshnessMonitor(freshness_threshold_seconds=300, stale_threshold_seconds=300)

    def test_raises_when_stale_less_than_fresh(self):
        with pytest.raises(ValueError, match="must be >"):
            FreshnessMonitor(freshness_threshold_seconds=600, stale_threshold_seconds=300)

    def test_from_env_uses_defaults(self, monkeypatch):
        monkeypatch.delenv("SOLANA_FRESHNESS_THRESHOLD_SECONDS", raising=False)
        monkeypatch.delenv("SOLANA_STALE_THRESHOLD_SECONDS", raising=False)
        m = FreshnessMonitor.from_env()
        assert m._freshness_threshold == DEFAULT_FRESHNESS_THRESHOLD_SECONDS
        assert m._stale_threshold == DEFAULT_STALE_THRESHOLD_SECONDS

    def test_from_env_reads_env_vars(self, monkeypatch):
        monkeypatch.setenv("SOLANA_FRESHNESS_THRESHOLD_SECONDS", "120")
        monkeypatch.setenv("SOLANA_STALE_THRESHOLD_SECONDS", "1800")
        m = FreshnessMonitor.from_env()
        assert m._freshness_threshold == 120
        assert m._stale_threshold == 1800


# ---------------------------------------------------------------------------
# Unavailable — no data
# ---------------------------------------------------------------------------

class TestUnavailableState:
    def test_unavailable_before_any_slot_recorded(self):
        m = FreshnessMonitor(_clock=FakeClock())
        assert m.health_state() == HEALTH_UNAVAILABLE

    def test_report_has_none_lag_when_unavailable(self):
        m = FreshnessMonitor(_clock=FakeClock())
        report = m.health_report()
        assert report.lag_seconds is None
        assert report.ingestion_lag_seconds is None
        assert report.last_slot is None

    def test_unavailable_after_reset(self):
        clock = FakeClock()
        m = FreshnessMonitor(_clock=clock)
        m.record_slot(SLOT, BLOCK_TIME)
        m.reset()
        assert m.health_state() == HEALTH_UNAVAILABLE

    def test_report_properties_on_unavailable(self):
        m = FreshnessMonitor(_clock=FakeClock())
        report = m.health_report()
        assert report.is_unavailable is True
        assert report.is_fresh is False
        assert report.is_stale is False


# ---------------------------------------------------------------------------
# Fresh state
# ---------------------------------------------------------------------------

class TestFreshState:
    def test_fresh_when_lag_zero(self):
        clock = FakeClock(now=BLOCK_TIME)
        m = FreshnessMonitor(freshness_threshold_seconds=300, stale_threshold_seconds=3600, _clock=clock)
        m.record_slot(SLOT, BLOCK_TIME)
        assert m.health_state() == HEALTH_FRESH

    def test_fresh_when_lag_at_threshold(self):
        clock = FakeClock(now=BLOCK_TIME)
        m = FreshnessMonitor(freshness_threshold_seconds=300, stale_threshold_seconds=3600, _clock=clock)
        m.record_slot(SLOT, BLOCK_TIME)
        clock.advance(300)   # exactly at threshold
        assert m.health_state() == HEALTH_FRESH

    def test_fresh_within_threshold(self):
        clock = FakeClock(now=BLOCK_TIME)
        m = FreshnessMonitor(freshness_threshold_seconds=300, stale_threshold_seconds=3600, _clock=clock)
        m.record_slot(SLOT, BLOCK_TIME)
        clock.advance(100)
        assert m.health_state() == HEALTH_FRESH

    def test_report_properties_on_fresh(self):
        clock = FakeClock(now=BLOCK_TIME)
        m = FreshnessMonitor(freshness_threshold_seconds=300, stale_threshold_seconds=3600, _clock=clock)
        m.record_slot(SLOT, BLOCK_TIME)
        report = m.health_report()
        assert report.is_fresh is True
        assert report.is_stale is False
        assert report.is_unavailable is False


# ---------------------------------------------------------------------------
# Stale state
# ---------------------------------------------------------------------------

class TestStaleState:
    def test_stale_just_past_fresh_threshold(self):
        clock = FakeClock(now=BLOCK_TIME)
        m = FreshnessMonitor(freshness_threshold_seconds=300, stale_threshold_seconds=3600, _clock=clock)
        m.record_slot(SLOT, BLOCK_TIME)
        clock.advance(301)  # one second past fresh threshold
        assert m.health_state() == HEALTH_STALE

    def test_stale_at_stale_threshold(self):
        clock = FakeClock(now=BLOCK_TIME)
        m = FreshnessMonitor(freshness_threshold_seconds=300, stale_threshold_seconds=3600, _clock=clock)
        m.record_slot(SLOT, BLOCK_TIME)
        clock.advance(3600)  # exactly at stale threshold
        assert m.health_state() == HEALTH_STALE

    def test_report_lag_seconds_correct(self):
        clock = FakeClock(now=BLOCK_TIME)
        m = FreshnessMonitor(freshness_threshold_seconds=300, stale_threshold_seconds=3600, _clock=clock)
        m.record_slot(SLOT, BLOCK_TIME)
        clock.advance(500)
        report = m.health_report()
        assert report.state == HEALTH_STALE
        assert report.lag_seconds == pytest.approx(500.0)

    def test_report_properties_on_stale(self):
        clock = FakeClock(now=BLOCK_TIME)
        m = FreshnessMonitor(freshness_threshold_seconds=300, stale_threshold_seconds=3600, _clock=clock)
        m.record_slot(SLOT, BLOCK_TIME)
        clock.advance(400)
        report = m.health_report()
        assert report.is_stale is True
        assert report.is_fresh is False
        assert report.is_unavailable is False


# ---------------------------------------------------------------------------
# Unavailable via excessive lag
# ---------------------------------------------------------------------------

class TestUnavailableViaLag:
    def test_unavailable_past_stale_threshold(self):
        clock = FakeClock(now=BLOCK_TIME)
        m = FreshnessMonitor(freshness_threshold_seconds=300, stale_threshold_seconds=3600, _clock=clock)
        m.record_slot(SLOT, BLOCK_TIME)
        clock.advance(3601)  # one second past stale threshold
        assert m.health_state() == HEALTH_UNAVAILABLE

    def test_unavailable_far_in_future(self):
        clock = FakeClock(now=BLOCK_TIME)
        m = FreshnessMonitor(freshness_threshold_seconds=300, stale_threshold_seconds=3600, _clock=clock)
        m.record_slot(SLOT, BLOCK_TIME)
        clock.advance(86400)  # 24 hours
        assert m.health_state() == HEALTH_UNAVAILABLE


# ---------------------------------------------------------------------------
# State transitions
# ---------------------------------------------------------------------------

class TestStateTransitions:
    def test_fresh_to_stale_to_unavailable(self):
        clock = FakeClock(now=BLOCK_TIME)
        m = FreshnessMonitor(freshness_threshold_seconds=300, stale_threshold_seconds=3600, _clock=clock)
        m.record_slot(SLOT, BLOCK_TIME)

        assert m.health_state() == HEALTH_FRESH
        clock.advance(301)
        assert m.health_state() == HEALTH_STALE
        clock.advance(3300)   # total 3601s
        assert m.health_state() == HEALTH_UNAVAILABLE

    def test_new_slot_resets_to_fresh(self):
        clock = FakeClock(now=BLOCK_TIME)
        m = FreshnessMonitor(freshness_threshold_seconds=300, stale_threshold_seconds=3600, _clock=clock)
        m.record_slot(SLOT, BLOCK_TIME)
        clock.advance(400)
        assert m.health_state() == HEALTH_STALE

        # New block arrives
        new_block_time = int(clock.now)
        m.record_slot(SLOT + 100, new_block_time)
        assert m.health_state() == HEALTH_FRESH

    def test_updating_slot_updates_report_fields(self):
        clock = FakeClock(now=BLOCK_TIME)
        m = FreshnessMonitor(freshness_threshold_seconds=300, stale_threshold_seconds=3600, _clock=clock)
        m.record_slot(SLOT, BLOCK_TIME)
        clock.advance(400)
        new_block_time = int(clock.now)
        m.record_slot(SLOT + 50, new_block_time)
        report = m.health_report()
        assert report.last_slot == SLOT + 50
        assert report.last_block_time == new_block_time


# ---------------------------------------------------------------------------
# HealthReport
# ---------------------------------------------------------------------------

class TestHealthReport:
    def test_to_dict_contains_all_keys(self):
        clock = FakeClock(now=BLOCK_TIME)
        m = FreshnessMonitor(freshness_threshold_seconds=300, stale_threshold_seconds=3600, _clock=clock)
        m.record_slot(SLOT, BLOCK_TIME)
        d = m.health_report().to_dict()
        expected_keys = {
            "state", "last_slot", "last_block_time", "last_ingested_at_wall",
            "lag_seconds", "ingestion_lag_seconds",
            "freshness_threshold_seconds", "stale_threshold_seconds",
        }
        assert set(d.keys()) == expected_keys

    def test_ingestion_lag_increases_with_time(self):
        clock = FakeClock(now=BLOCK_TIME)
        m = FreshnessMonitor(freshness_threshold_seconds=300, stale_threshold_seconds=3600, _clock=clock)
        m.record_slot(SLOT, BLOCK_TIME)
        clock.advance(10)
        report = m.health_report()
        assert report.ingestion_lag_seconds == pytest.approx(10.0)

    def test_report_thresholds_match_monitor(self):
        m = FreshnessMonitor(freshness_threshold_seconds=60, stale_threshold_seconds=600, _clock=FakeClock())
        report = m.health_report()
        assert report.freshness_threshold_seconds == 60
        assert report.stale_threshold_seconds == 600


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

class TestDefaultMonitor:
    def test_get_default_monitor_returns_instance(self):
        m = get_default_monitor()
        assert isinstance(m, FreshnessMonitor)

    def test_set_default_monitor_replaces_singleton(self):
        clock = FakeClock()
        custom = FreshnessMonitor(freshness_threshold_seconds=60, stale_threshold_seconds=600, _clock=clock)
        set_default_monitor(custom)
        assert get_default_monitor() is custom

    def test_custom_singleton_reflects_recorded_slots(self):
        clock = FakeClock(now=BLOCK_TIME)
        custom = FreshnessMonitor(freshness_threshold_seconds=300, stale_threshold_seconds=3600, _clock=clock)
        set_default_monitor(custom)
        get_default_monitor().record_slot(SLOT, BLOCK_TIME)
        assert get_default_monitor().health_state() == HEALTH_FRESH
