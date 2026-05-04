"""
Phase 3 — Rate Limiter + Circuit Breaker.

Two complementary guards around the Solana ingestion loop:

RateLimiter
-----------
Token-bucket rate limiter. Enforces SOLANA_RPC_MAX_RPS and
SOLANA_RPC_BURST_LIMIT. Throttles the ingestion loop — not individual
RPC calls (retries in rpc_client.py handle per-call 429s).

CircuitBreaker
--------------
Tracks consecutive RPC failures across the ingestion loop.

States:
  CLOSED   — normal operation; failures accumulate
  OPEN     — failure threshold exceeded; calls blocked; cooldown timer running
  HALF_OPEN — cooldown elapsed; one probe allowed; success → CLOSED, failure → OPEN

On OPEN:
  - The call is rejected immediately (CircuitOpenError raised).
  - The adapter marks the run degraded or failed and stops hammering the provider.
  - Cooldown is configurable (SOLANA_CIRCUIT_BREAKER_COOLDOWN_SECONDS).

Note: rpc_client.py already handles per-call retry (3 attempts, 2s/4s/8s backoff,
Retry-After respect). The circuit breaker operates at the loop level — it opens
after SOLANA_MAX_CONSECUTIVE_FAILURES full retried failures in a row.

Config env vars
---------------
SOLANA_RPC_MAX_RPS                  — max requests per second (default: 10)
SOLANA_RPC_BURST_LIMIT              — max burst tokens (default: same as max_rps)
SOLANA_MAX_CONSECUTIVE_FAILURES     — failures before circuit opens (default: 5)
SOLANA_CIRCUIT_BREAKER_COOLDOWN_SECONDS — cooldown before half-open probe (default: 30)
"""

from __future__ import annotations

import logging
import os
import time
from enum import Enum
from typing import Optional

logger = logging.getLogger("canopy.solana.circuit_breaker")

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DEFAULT_MAX_RPS = 10
DEFAULT_MAX_CONSECUTIVE_FAILURES = 5
DEFAULT_COOLDOWN_SECONDS = 30.0


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class CircuitOpenError(Exception):
    """
    Raised when a call is attempted while the circuit breaker is OPEN.

    The caller (ingestion adapter) should catch this, mark the run degraded,
    and stop the current ingestion loop without crashing.
    """


# ---------------------------------------------------------------------------
# Circuit breaker states
# ---------------------------------------------------------------------------

class CircuitState(Enum):
    CLOSED = "closed"       # Normal — failures accumulate
    OPEN = "open"           # Blocked — cooldown running
    HALF_OPEN = "half_open" # Cooldown elapsed — one probe allowed


# ---------------------------------------------------------------------------
# RateLimiter — token bucket
# ---------------------------------------------------------------------------

class RateLimiter:
    """
    Token-bucket rate limiter.

    Allows up to `burst_limit` calls immediately, then refills at `max_rps`
    tokens per second. `acquire()` blocks (sleeps) until a token is available.

    In tests, inject a mock clock via the `_clock` parameter.
    """

    def __init__(
        self,
        max_rps: float = DEFAULT_MAX_RPS,
        burst_limit: Optional[float] = None,
        *,
        _clock=None,
        _sleep=None,
    ) -> None:
        self.max_rps = max(max_rps, 0.1)
        self.burst_limit = burst_limit if burst_limit is not None else self.max_rps
        self._tokens: float = self.burst_limit
        self._clock = _clock or time.monotonic
        self._sleep = _sleep or time.sleep
        self._last_refill: float = self._clock()

    def acquire(self) -> None:
        """
        Block until a token is available, then consume one token.

        In normal operation this is a sub-millisecond no-op when the rate is
        not exceeded. Under load it sleeps for exactly the time needed.
        """
        self._refill()
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return
        # Need to wait for the next token
        deficit = 1.0 - self._tokens
        wait = deficit / self.max_rps
        logger.debug("RateLimiter: throttling %.3fs", wait)
        self._sleep(wait)
        self._refill()
        self._tokens = max(self._tokens - 1.0, 0.0)

    def _refill(self) -> None:
        now = self._clock()
        elapsed = now - self._last_refill
        self._tokens = min(self._tokens + elapsed * self.max_rps, self.burst_limit)
        self._last_refill = now

    @classmethod
    def from_env(cls) -> "RateLimiter":
        max_rps = float(os.environ.get("SOLANA_RPC_MAX_RPS", str(DEFAULT_MAX_RPS)))
        burst = os.environ.get("SOLANA_RPC_BURST_LIMIT")
        burst_limit = float(burst) if burst else None
        return cls(max_rps=max_rps, burst_limit=burst_limit)


# ---------------------------------------------------------------------------
# CircuitBreaker
# ---------------------------------------------------------------------------

class CircuitBreaker:
    """
    Loop-level circuit breaker for the Solana ingestion adapter.

    Usage
    -----
    cb = CircuitBreaker()

    # Before each RPC-dependent operation in the ingestion loop:
    cb.before_call()   # raises CircuitOpenError if circuit is OPEN

    # After success:
    cb.record_success()

    # After a full retried failure (all rpc_client.py attempts exhausted):
    cb.record_failure()
    """

    def __init__(
        self,
        max_consecutive_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES,
        cooldown_seconds: float = DEFAULT_COOLDOWN_SECONDS,
        *,
        _clock=None,
    ) -> None:
        self.max_consecutive_failures = max(max_consecutive_failures, 1)
        self.cooldown_seconds = max(cooldown_seconds, 0.0)
        self._clock = _clock or time.monotonic

        self._state: CircuitState = CircuitState.CLOSED
        self._consecutive_failures: int = 0
        self._opened_at: Optional[float] = None
        self._total_trips: int = 0   # how many times the circuit has opened

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def state(self) -> CircuitState:
        self._maybe_transition_to_half_open()
        return self._state

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    @property
    def total_trips(self) -> int:
        return self._total_trips

    def before_call(self) -> None:
        """
        Call before each RPC-dependent operation.

        Raises CircuitOpenError if the circuit is OPEN (cooldown not elapsed).
        Allows one probe through if HALF_OPEN.
        """
        self._maybe_transition_to_half_open()
        if self._state == CircuitState.OPEN:
            remaining = self._cooldown_remaining()
            raise CircuitOpenError(
                f"Circuit breaker is OPEN — {self._consecutive_failures} consecutive failures. "
                f"Cooldown: {remaining:.1f}s remaining."
            )
        # CLOSED or HALF_OPEN: allow the call through

    def record_success(self) -> None:
        """Call after a successful RPC operation. Resets failure count."""
        if self._state == CircuitState.HALF_OPEN:
            logger.info("Circuit breaker: probe succeeded — transitioning HALF_OPEN → CLOSED")
        self._consecutive_failures = 0
        self._opened_at = None
        self._state = CircuitState.CLOSED

    def record_failure(self) -> None:
        """
        Call after a fully-retried RPC failure (all rpc_client.py attempts exhausted).

        Increments consecutive failure count. Opens the circuit if threshold reached.
        """
        self._consecutive_failures += 1
        logger.warning(
            "Circuit breaker: consecutive failure %d/%d",
            self._consecutive_failures, self.max_consecutive_failures,
        )

        if self._state == CircuitState.HALF_OPEN:
            logger.warning("Circuit breaker: probe failed — HALF_OPEN → OPEN (reset cooldown)")
            self._open_circuit()
        elif self._consecutive_failures >= self.max_consecutive_failures:
            logger.error(
                "Circuit breaker: threshold %d reached — CLOSED → OPEN",
                self.max_consecutive_failures,
            )
            self._open_circuit()

    def health_dict(self) -> dict:
        """Return a dict suitable for /health/solana endpoint."""
        return {
            "circuit_state": self.state.value,
            "consecutive_failures": self._consecutive_failures,
            "max_consecutive_failures": self.max_consecutive_failures,
            "total_trips": self._total_trips,
            "cooldown_remaining_seconds": round(self._cooldown_remaining(), 1),
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _open_circuit(self) -> None:
        self._state = CircuitState.OPEN
        self._opened_at = self._clock()
        self._total_trips += 1

    def _maybe_transition_to_half_open(self) -> None:
        if self._state == CircuitState.OPEN and self._opened_at is not None:
            elapsed = self._clock() - self._opened_at
            if elapsed >= self.cooldown_seconds:
                logger.info(
                    "Circuit breaker: cooldown elapsed (%.1fs) — OPEN → HALF_OPEN",
                    elapsed,
                )
                self._state = CircuitState.HALF_OPEN

    def _cooldown_remaining(self) -> float:
        if self._state != CircuitState.OPEN or self._opened_at is None:
            return 0.0
        elapsed = self._clock() - self._opened_at
        return max(self.cooldown_seconds - elapsed, 0.0)

    @classmethod
    def from_env(cls) -> "CircuitBreaker":
        max_failures = int(os.environ.get("SOLANA_MAX_CONSECUTIVE_FAILURES",
                                          str(DEFAULT_MAX_CONSECUTIVE_FAILURES)))
        cooldown = float(os.environ.get("SOLANA_CIRCUIT_BREAKER_COOLDOWN_SECONDS",
                                        str(DEFAULT_COOLDOWN_SECONDS)))
        return cls(max_consecutive_failures=max_failures, cooldown_seconds=cooldown)
