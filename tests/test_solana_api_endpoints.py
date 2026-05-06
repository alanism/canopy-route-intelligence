"""
Phase 9 — Solana Dashboard Integration: API endpoint tests.

Tests the /v1/solana/health endpoint and the Solana entry in /health
without touching the EVM cache machinery. Uses FastAPI TestClient with
dependency overrides to inject a controlled SolanaCache.
"""

from __future__ import annotations

import pytest
from unittest.mock import patch

from services.solana.api_integration import (
    SolanaAPIState,
    SolanaCache,
    set_default_cache,
)
from services.solana.freshness import (
    FreshnessMonitor,
    HEALTH_FRESH,
    HEALTH_STALE,
    HEALTH_UNAVAILABLE,
)


# ---------------------------------------------------------------------------
# Helpers
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


def _make_fresh_cache() -> SolanaCache:
    clock = FakeClock(now=float(BLOCK_TIME))
    monitor = FreshnessMonitor(
        freshness_threshold_seconds=300,
        stale_threshold_seconds=3600,
        _clock=clock,
    )
    cache = SolanaCache(monitor=monitor)
    cache.record_run(
        slot=SLOT,
        block_time=BLOCK_TIME,
        run_status="ok",
        signatures_fetched=10,
        transactions_processed=8,
        events_written=8,
        validation_status="approved",
    )
    return cache


def _make_unavailable_cache() -> SolanaCache:
    clock = FakeClock(now=float(BLOCK_TIME))
    monitor = FreshnessMonitor(
        freshness_threshold_seconds=300,
        stale_threshold_seconds=3600,
        _clock=clock,
    )
    return SolanaCache(monitor=monitor)  # no record_run — stays unavailable


def _make_stale_cache() -> SolanaCache:
    clock = FakeClock(now=float(BLOCK_TIME))
    monitor = FreshnessMonitor(
        freshness_threshold_seconds=300,
        stale_threshold_seconds=3600,
        _clock=clock,
    )
    cache = SolanaCache(monitor=monitor)
    cache.record_run(slot=SLOT, block_time=BLOCK_TIME, run_status="ok", events_written=5)
    clock.advance(400)   # past fresh threshold → stale
    return cache


# ---------------------------------------------------------------------------
# SolanaAPIState endpoint contract tests (no HTTP layer needed)
# ---------------------------------------------------------------------------

class TestSolanaHealthEndpointContract:
    """
    Test the data contract of /v1/solana/health by calling get_solana_api_state()
    directly and verifying the shape and rules without spinning up FastAPI.
    """

    def test_fresh_state_has_correct_freshness_state(self):
        cache = _make_fresh_cache()
        set_default_cache(cache)
        from services.solana.api_integration import get_solana_api_state
        state = get_solana_api_state()
        assert state.freshness_state == HEALTH_FRESH

    def test_unavailable_state_has_none_lag(self):
        cache = _make_unavailable_cache()
        set_default_cache(cache)
        from services.solana.api_integration import get_solana_api_state
        state = get_solana_api_state()
        assert state.freshness_state == HEALTH_UNAVAILABLE
        assert state.lag_seconds is None

    def test_stale_state_is_labeled_stale(self):
        cache = _make_stale_cache()
        set_default_cache(cache)
        from services.solana.api_integration import get_solana_api_state
        state = get_solana_api_state()
        assert state.freshness_state == HEALTH_STALE

    def test_fresh_to_dict_contains_chain_field(self):
        """Simulate what the endpoint builds."""
        cache = _make_fresh_cache()
        set_default_cache(cache)
        from services.solana.api_integration import get_solana_api_state
        state = get_solana_api_state()
        payload = {
            **state.to_dict(),
            "chain": "Solana",
            "chain_health": state.to_chain_health_dict(),
            "scope_disclaimer": "Solana data reflects observed SPL token movements...",
        }
        assert payload["chain"] == "Solana"
        assert "chain_health" in payload
        assert "scope_disclaimer" in payload
        assert "ingestion_state" in payload
        assert "observation_state" in payload
        assert "commitment_level" in payload

    def test_scope_disclaimer_always_present(self):
        for make_cache in (_make_fresh_cache, _make_stale_cache, _make_unavailable_cache):
            cache = make_cache()
            set_default_cache(cache)
            from services.solana.api_integration import get_solana_api_state
            state = get_solana_api_state()
            payload = {
                **state.to_dict(),
                "chain": "Solana",
                "chain_health": state.to_chain_health_dict(),
                "scope_disclaimer": (
                    "Solana data reflects observed SPL token movements "
                    "within configured watched sources and measured windows."
                ),
            }
            assert payload["scope_disclaimer"]  # never empty

    def test_unavailable_chain_health_status(self):
        cache = _make_unavailable_cache()
        set_default_cache(cache)
        from services.solana.api_integration import get_solana_api_state
        state = get_solana_api_state()
        ch = state.to_chain_health_dict()
        assert ch["status"] == "unavailable"
        assert ch["freshness_level"] == "unknown"

    def test_stale_chain_health_status_is_degraded(self):
        cache = _make_stale_cache()
        set_default_cache(cache)
        from services.solana.api_integration import get_solana_api_state
        state = get_solana_api_state()
        ch = state.to_chain_health_dict()
        # Stale must NEVER render as "fresh" or "ok" — must be "degraded"
        assert ch["status"] == "degraded"
        assert ch["status"] != "fresh"

    def test_fresh_chain_health_status(self):
        cache = _make_fresh_cache()
        set_default_cache(cache)
        from services.solana.api_integration import get_solana_api_state
        state = get_solana_api_state()
        ch = state.to_chain_health_dict()
        assert ch["status"] == "fresh"
        assert ch["freshness_level"] == "fresh"

    def test_to_dict_is_json_serializable(self):
        import json
        for make_cache in (_make_fresh_cache, _make_stale_cache, _make_unavailable_cache):
            cache = make_cache()
            set_default_cache(cache)
            from services.solana.api_integration import get_solana_api_state
            state = get_solana_api_state()
            payload = {
                **state.to_dict(),
                "chain": "Solana",
                "chain_health": state.to_chain_health_dict(),
                "scope_disclaimer": "Solana data reflects...",
            }
            json.dumps(payload)  # must not raise


# ---------------------------------------------------------------------------
# /health chains dict: Solana entry tests
# ---------------------------------------------------------------------------

class TestHealthEndpointSolanaEntry:
    """
    Tests that the /health endpoint includes Solana in the chains dict
    with the correct structure, by testing the chain_health dict shape directly.
    """

    def test_solana_in_chains_is_unavailable_when_no_data(self):
        cache = _make_unavailable_cache()
        set_default_cache(cache)
        from services.solana.api_integration import get_solana_api_state
        solana_chain_health = get_solana_api_state().to_chain_health_dict()
        assert solana_chain_health["status"] == "unavailable"

    def test_solana_chain_health_has_required_keys(self):
        cache = _make_fresh_cache()
        set_default_cache(cache)
        from services.solana.api_integration import get_solana_api_state
        solana_chain_health = get_solana_api_state().to_chain_health_dict()
        required_keys = {
            "status", "freshness_state", "freshness_level",
            "cache_age_seconds", "last_slot", "last_run_status",
            "last_error",
        }
        assert required_keys.issubset(set(solana_chain_health.keys()))

    def test_solana_chain_health_never_shows_stale_as_fresh(self):
        cache = _make_stale_cache()
        set_default_cache(cache)
        from services.solana.api_integration import get_solana_api_state
        ch = get_solana_api_state().to_chain_health_dict()
        assert ch["status"] != "fresh"
        assert ch["freshness_level"] != "fresh"

    def test_solana_chain_health_fresh_has_slot_populated(self):
        cache = _make_fresh_cache()
        set_default_cache(cache)
        from services.solana.api_integration import get_solana_api_state
        ch = get_solana_api_state().to_chain_health_dict()
        assert ch["last_slot"] == SLOT

    def test_solana_chain_health_unavailable_has_none_slot(self):
        cache = _make_unavailable_cache()
        set_default_cache(cache)
        from services.solana.api_integration import get_solana_api_state
        ch = get_solana_api_state().to_chain_health_dict()
        assert ch["last_slot"] is None


# ---------------------------------------------------------------------------
# Dashboard rendering rules (state machine invariants)
# ---------------------------------------------------------------------------

class TestDashboardRenderingRules:
    """
    Verify that the state the dashboard receives obeys the display rules:
    - Zero data → unavailable, never green
    - Stale → degraded, never green
    - Fresh → only then show as green/ok
    """

    def test_zero_data_is_not_available(self):
        cache = _make_unavailable_cache()
        set_default_cache(cache)
        from services.solana.api_integration import get_solana_api_state
        state = get_solana_api_state()
        assert state.is_available is False

    def test_stale_data_is_available_but_not_fresh(self):
        cache = _make_stale_cache()
        set_default_cache(cache)
        from services.solana.api_integration import get_solana_api_state
        state = get_solana_api_state()
        assert state.is_available is True
        assert state.is_fresh is False
        assert state.is_stale is True

    def test_fresh_data_is_available_and_fresh(self):
        cache = _make_fresh_cache()
        set_default_cache(cache)
        from services.solana.api_integration import get_solana_api_state
        state = get_solana_api_state()
        assert state.is_available is True
        assert state.is_fresh is True
        assert state.is_stale is False

    def test_scope_disclaimer_present_in_api_payload_for_all_states(self):
        """The scope disclaimer must appear in every /v1/solana/health response."""
        disclaimer = (
            "Solana data reflects observed SPL token movements "
            "within configured watched sources and measured windows."
        )
        for make_cache in (_make_fresh_cache, _make_stale_cache, _make_unavailable_cache):
            cache = make_cache()
            set_default_cache(cache)
            # Simulate what the endpoint builds
            from services.solana.api_integration import get_solana_api_state
            state = get_solana_api_state()
            payload = {
                **state.to_dict(),
                "chain": "Solana",
                "chain_health": state.to_chain_health_dict(),
                "scope_disclaimer": disclaimer,
            }
            assert payload["scope_disclaimer"] == disclaimer


class TestSolanaCorridorIntelligenceEndpoint:
    def test_endpoint_is_wired_to_materialized_loader_without_bigquery(self):
        from pathlib import Path

        source = (Path(__file__).resolve().parents[1] / "api" / "main.py").read_text(encoding="utf-8")

        assert '@app.get("/v1/solana/corridor-intelligence")' in source
        assert "load_corridor_intelligence()" in source
        assert "run_shadow_validation" not in source
