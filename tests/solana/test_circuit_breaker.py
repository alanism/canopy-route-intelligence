"""
Phase 3 — Rate Limiter + Circuit Breaker Tests.

Covers:
RateLimiter:
- acquire() passes immediately when tokens available
- acquire() throttles when bucket empty (sleep called)
- burst_limit allows multiple immediate calls up to limit
- refill over time restores tokens

CircuitBreaker:
- starts CLOSED
- record_failure increments consecutive count
- threshold reached → state transitions CLOSED → OPEN
- before_call raises CircuitOpenError when OPEN
- cooldown elapsed → state transitions OPEN → HALF_OPEN
- probe success → HALF_OPEN → CLOSED, counter reset
- probe failure → HALF_OPEN → OPEN again (reset cooldown)
- record_success resets failure count from CLOSED
- total_trips increments each time circuit opens
- health_dict contains all expected keys

Adapter integration:
- circuit breaker OPEN stops the run early (run_status=degraded)
- rate limiter acquire() called before each fetch
- consecutive failures open the circuit mid-run
- circuit breaker not wired → no impact (backward compatible)
"""

from __future__ import annotations

from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

from services.solana.circuit_breaker import (
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
    RateLimiter,
)
from services.solana.constants import USDC_MINT
from services.solana.ingestion_adapter import IngestionConfig, SolanaIngestionAdapter
from services.solana.alt_manager import PersistentALTCache

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ADDR_1 = "WatchedAddr1111111111111111111111111111111111"
SIG_1 = "Sig1" + "1" * 84


# ---------------------------------------------------------------------------
# RateLimiter tests
# ---------------------------------------------------------------------------

class TestRateLimiter:

    def test_acquire_passes_immediately_with_full_bucket(self):
        """Full token bucket — acquire() should not sleep."""
        sleeps = []
        rl = RateLimiter(max_rps=10, burst_limit=10,
                         _clock=_monotonic_counter(), _sleep=sleeps.append)
        rl.acquire()
        assert sleeps == []

    def test_acquire_sleeps_when_bucket_empty(self):
        """Empty bucket — acquire() must sleep to wait for refill."""
        sleep_calls = []
        clock = _monotonic_counter(step=0.0)  # time does not advance
        rl = RateLimiter(max_rps=10, burst_limit=1,
                         _clock=clock, _sleep=sleep_calls.append)
        rl.acquire()          # consumes the one token — no sleep
        rl.acquire()          # bucket empty — must sleep
        assert len(sleep_calls) == 1
        assert sleep_calls[0] > 0

    def test_burst_limit_allows_multiple_immediate_calls(self):
        """Burst limit of 5 allows 5 calls without sleeping."""
        sleeps = []
        rl = RateLimiter(max_rps=10, burst_limit=5,
                         _clock=_monotonic_counter(), _sleep=sleeps.append)
        for _ in range(5):
            rl.acquire()
        assert sleeps == []

    def test_sixth_call_beyond_burst_sleeps(self):
        """6th call when burst is 5 must sleep."""
        sleeps = []
        rl = RateLimiter(max_rps=10, burst_limit=5,
                         _clock=_monotonic_counter(step=0.0), _sleep=sleeps.append)
        for _ in range(5):
            rl.acquire()
        rl.acquire()  # 6th — must sleep
        assert len(sleeps) == 1

    def test_tokens_refill_over_time(self):
        """After time passes, tokens refill up to burst_limit."""
        sleeps = []
        # Clock advances by 1 second each call (at 10 rps → 10 tokens refill)
        clock = _monotonic_counter(step=1.0)
        rl = RateLimiter(max_rps=10, burst_limit=10,
                         _clock=clock, _sleep=sleeps.append)
        # Drain all tokens
        for _ in range(10):
            rl.acquire()
        # After 1 second of clock advance, 10 tokens refill — next acquire no sleep
        rl.acquire()
        assert sleeps == []


# ---------------------------------------------------------------------------
# CircuitBreaker — state transitions
# ---------------------------------------------------------------------------

class TestCircuitBreakerClosed:

    def test_starts_closed(self):
        cb = CircuitBreaker(max_consecutive_failures=3)
        assert cb.state == CircuitState.CLOSED

    def test_before_call_passes_when_closed(self):
        cb = CircuitBreaker(max_consecutive_failures=3)
        cb.before_call()  # no exception

    def test_record_failure_increments_count(self):
        cb = CircuitBreaker(max_consecutive_failures=5)
        cb.record_failure()
        cb.record_failure()
        assert cb.consecutive_failures == 2

    def test_record_success_resets_failure_count(self):
        cb = CircuitBreaker(max_consecutive_failures=5)
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        assert cb.consecutive_failures == 0
        assert cb.state == CircuitState.CLOSED

    def test_threshold_triggers_open(self):
        cb = CircuitBreaker(max_consecutive_failures=3)
        for _ in range(3):
            cb.record_failure()
        assert cb.state == CircuitState.OPEN

    def test_total_trips_increments_on_open(self):
        cb = CircuitBreaker(max_consecutive_failures=2)
        cb.record_failure()
        cb.record_failure()
        assert cb.total_trips == 1


class TestCircuitBreakerOpen:

    def test_before_call_raises_when_open(self):
        cb = CircuitBreaker(max_consecutive_failures=1, cooldown_seconds=60)
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        with pytest.raises(CircuitOpenError):
            cb.before_call()

    def test_error_message_includes_failure_count(self):
        cb = CircuitBreaker(max_consecutive_failures=1, cooldown_seconds=60)
        cb.record_failure()
        with pytest.raises(CircuitOpenError, match="consecutive failures"):
            cb.before_call()

    def test_stays_open_before_cooldown_elapses(self):
        tick = [0.0]
        def clock(): return tick[0]
        cb = CircuitBreaker(max_consecutive_failures=1, cooldown_seconds=30, _clock=clock)
        cb.record_failure()
        tick[0] = 15.0  # only 15s — cooldown not elapsed
        assert cb.state == CircuitState.OPEN

    def test_transitions_to_half_open_after_cooldown(self):
        tick = [0.0]
        def clock(): return tick[0]
        cb = CircuitBreaker(max_consecutive_failures=1, cooldown_seconds=30, _clock=clock)
        cb.record_failure()
        tick[0] = 31.0  # cooldown elapsed
        assert cb.state == CircuitState.HALF_OPEN


class TestCircuitBreakerHalfOpen:

    def _open_then_cool(self, failures=1, cooldown=30.0):
        tick = [0.0]
        def clock(): return tick[0]
        cb = CircuitBreaker(max_consecutive_failures=failures,
                            cooldown_seconds=cooldown, _clock=clock)
        for _ in range(failures):
            cb.record_failure()
        tick[0] = cooldown + 1.0
        assert cb.state == CircuitState.HALF_OPEN
        return cb

    def test_probe_success_transitions_to_closed(self):
        cb = self._open_then_cool()
        cb.before_call()  # probe allowed through
        cb.record_success()
        assert cb.state == CircuitState.CLOSED
        assert cb.consecutive_failures == 0

    def test_probe_failure_reopens_circuit(self):
        cb = self._open_then_cool()
        cb.before_call()
        cb.record_failure()  # probe failed
        assert cb.state == CircuitState.OPEN

    def test_probe_failure_increments_total_trips(self):
        cb = self._open_then_cool()
        trips_before = cb.total_trips
        cb.record_failure()
        assert cb.total_trips == trips_before + 1

    def test_before_call_allowed_once_in_half_open(self):
        cb = self._open_then_cool()
        cb.before_call()  # should not raise


class TestCircuitBreakerHealthDict:

    def test_health_dict_contains_expected_keys(self):
        cb = CircuitBreaker()
        h = cb.health_dict()
        assert "circuit_state" in h
        assert "consecutive_failures" in h
        assert "max_consecutive_failures" in h
        assert "total_trips" in h
        assert "cooldown_remaining_seconds" in h

    def test_health_dict_state_closed_when_healthy(self):
        cb = CircuitBreaker()
        assert cb.health_dict()["circuit_state"] == "closed"

    def test_health_dict_state_open_after_failure(self):
        cb = CircuitBreaker(max_consecutive_failures=1, cooldown_seconds=60)
        cb.record_failure()
        assert cb.health_dict()["circuit_state"] == "open"


# ---------------------------------------------------------------------------
# Adapter integration
# ---------------------------------------------------------------------------

class FailingMockRPC:
    """RPC that always raises an exception — simulates persistent provider failure."""

    def __init__(self):
        self.primary_url = "http://mock"
        self.fallback_url = None
        self._provider_mode = "primary"
        self.call_count = 0

    @property
    def provider_mode(self): return self._provider_mode

    def get_account_info(self, address, *, encoding, commitment, use_fallback=False):
        return None

    def get_transaction(self, signature, **kwargs):
        return None

    def _post_with_retry(self, url, payload, *, context=""):
        self.call_count += 1
        raise Exception("Simulated RPC failure")


class EmptyMockRPC:
    """RPC that returns empty signatures — circuit breaker is not triggered."""

    def __init__(self, *, acquire_calls=None):
        self.primary_url = "http://mock"
        self.fallback_url = None
        self._provider_mode = "primary"
        self._acquire_calls = acquire_calls  # list to track rate limiter calls

    @property
    def provider_mode(self): return self._provider_mode

    def get_account_info(self, address, *, encoding, commitment, use_fallback=False):
        return None

    def get_transaction(self, signature, **kwargs):
        return None

    def _post_with_retry(self, url, payload, *, context=""):
        return {"result": []}


def _make_config(addresses=None):
    return IngestionConfig(
        primary_url="http://mock",
        watched_addresses=addresses or [ADDR_1],
        token_mint_allowlist={USDC_MINT},
        start_signature="StartSig" + "1" * 80,
    )


def _make_adapter(rpc, *, tmp_path, cb=None, rl=None):
    pers = PersistentALTCache(cache_path=str(tmp_path / "alt.json"))
    return SolanaIngestionAdapter(
        _make_config(),
        rpc_client=rpc,
        persistent_cache=pers,
        circuit_breaker=cb,
        rate_limiter=rl,
    )


class TestAdapterCircuitBreakerIntegration:

    def test_circuit_breaker_not_wired_no_impact(self, tmp_path):
        """No circuit breaker — adapter runs normally (backward compatible)."""
        rpc = EmptyMockRPC()
        adapter = _make_adapter(rpc, tmp_path=tmp_path, cb=None)
        result = adapter.run()
        assert result.run_status == "ok"

    def test_open_circuit_stops_run_with_degraded_status(self, tmp_path):
        """Pre-opened circuit breaker causes run to stop immediately."""
        cb = CircuitBreaker(max_consecutive_failures=1, cooldown_seconds=9999)
        cb.record_failure()  # force OPEN before run starts
        assert cb.state == CircuitState.OPEN

        rpc = EmptyMockRPC()
        adapter = _make_adapter(rpc, tmp_path=tmp_path, cb=cb)
        result = adapter.run()
        assert result.run_status == "degraded"
        assert any("Circuit breaker OPEN" in e for e in result.errors)

    def test_consecutive_failures_open_circuit_mid_run(self, tmp_path):
        """Failures during signature fetch accumulate and open the circuit."""
        cb = CircuitBreaker(max_consecutive_failures=2, cooldown_seconds=9999)
        # Two watched addresses — each signature fetch fails → 2 failures → circuit opens
        addr_b = "WatchedAddr2222222222222222222222222222222222"
        pers = PersistentALTCache(cache_path=str(tmp_path / "alt.json"))
        rpc = FailingMockRPC()
        config = IngestionConfig(
            primary_url="http://mock",
            watched_addresses=[ADDR_1, addr_b],
            token_mint_allowlist={USDC_MINT},
            start_signature="StartSig" + "1" * 80,
        )
        adapter = SolanaIngestionAdapter(
            config, rpc_client=rpc, persistent_cache=pers, circuit_breaker=cb
        )
        result = adapter.run()
        assert result.run_status == "degraded"
        assert cb.state == CircuitState.OPEN

    def test_rate_limiter_acquire_called_per_fetch(self, tmp_path):
        """Rate limiter acquire() is called before signature discovery."""
        acquire_calls = []

        class TrackingRateLimiter:
            def acquire(self):
                acquire_calls.append(1)

        rpc = EmptyMockRPC()
        rl = TrackingRateLimiter()
        adapter = _make_adapter(rpc, tmp_path=tmp_path, rl=rl)
        adapter.run()
        # At least one acquire() call for the watched address signature fetch
        assert len(acquire_calls) >= 1

    def test_rate_limiter_not_wired_no_impact(self, tmp_path):
        """No rate limiter — adapter runs normally (backward compatible)."""
        rpc = EmptyMockRPC()
        adapter = _make_adapter(rpc, tmp_path=tmp_path, rl=None)
        result = adapter.run()
        assert result.run_status == "ok"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _monotonic_counter(step: float = 0.001):
    """Returns a clock function whose value advances by `step` each call."""
    state = [0.0]
    def clock():
        val = state[0]
        state[0] += step
        return val
    return clock
