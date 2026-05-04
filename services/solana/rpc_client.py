"""
Solana RPC client — thin HTTP wrapper for ALT and transaction fetches.

Uses requests (already in requirements.txt). Applies the retry/backoff config
from the build plan: max 3 attempts, 2s/4s/8s exponential backoff with jitter,
retryable vs terminal error classification.

Primary vs fallback provider:
  - Primary is the source of record.
  - Fallback is for reconciliation and degraded emergency reads only.
  - Fallback must never silently replace primary and render data healthy.
"""

from __future__ import annotations

import logging
import os
import random
import time
from typing import Any, Optional

import requests

logger = logging.getLogger("canopy.solana.rpc_client")

# ---------------------------------------------------------------------------
# Retry config (from build plan)
# ---------------------------------------------------------------------------
MAX_ATTEMPTS = 3
BACKOFF_SECONDS = [2.0, 4.0, 8.0]
JITTER_FRACTION = 0.1  # ±10% jitter on each backoff

# HTTP status codes that are safe to retry
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# HTTP status codes that are terminal — do not retry
TERMINAL_STATUS_CODES = {400, 401, 403, 404}


class RPCError(Exception):
    """Raised when an RPC call fails terminally."""
    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


class SolanaRPCClient:
    """
    Minimal Solana JSON-RPC client for ALT resolution and transaction fetching.

    Designed to be injected into ALTManager. Tests can replace this with a
    MockRPCClient that implements the same interface without HTTP calls.
    """

    def __init__(
        self,
        primary_url: str,
        *,
        fallback_url: Optional[str] = None,
        timeout_seconds: float = 10.0,
        max_rps: Optional[int] = None,
    ):
        self.primary_url = primary_url
        self.fallback_url = fallback_url
        self.timeout_seconds = timeout_seconds
        self.max_rps = max_rps
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})
        # Track which provider is active
        self._provider_mode: str = "primary"

    @property
    def provider_mode(self) -> str:
        return self._provider_mode

    def get_account_info(
        self,
        address: str,
        *,
        encoding: str = "jsonParsed",
        commitment: str = "finalized",
        use_fallback: bool = False,
    ) -> Optional[dict[str, Any]]:
        """
        Fetch account info for a given address.

        Returns the parsed result value or None if the account does not exist.
        Raises RPCError on terminal failures.
        """
        url = self._pick_url(use_fallback)
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [
                address,
                {"encoding": encoding, "commitment": commitment},
            ],
        }
        raw = self._post_with_retry(url, payload, context=f"getAccountInfo({address[:8]}…)")
        result = raw.get("result", {})
        if result is None:
            return None
        value = result.get("value")
        if value is None:
            return None
        # Attach context slot for cache metadata
        context = result.get("context", {})
        value["_slot"] = context.get("slot", 0)
        return value

    def get_transaction(
        self,
        signature: str,
        *,
        max_supported_transaction_version: int = 0,
        commitment: str = "finalized",
        encoding: str = "json",
        use_fallback: bool = False,
    ) -> Optional[dict[str, Any]]:
        """Fetch a transaction by signature."""
        url = self._pick_url(use_fallback)
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [
                signature,
                {
                    "encoding": encoding,
                    "commitment": commitment,
                    "maxSupportedTransactionVersion": max_supported_transaction_version,
                },
            ],
        }
        raw = self._post_with_retry(url, payload, context=f"getTransaction({signature[:8]}…)")
        return raw.get("result")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _pick_url(self, use_fallback: bool) -> str:
        if use_fallback and self.fallback_url:
            self._provider_mode = "fallback"
            return self.fallback_url
        self._provider_mode = "primary"
        return self.primary_url

    def _post_with_retry(
        self,
        url: str,
        payload: dict[str, Any],
        *,
        context: str = "rpc_call",
    ) -> dict[str, Any]:
        last_exc: Optional[Exception] = None

        for attempt in range(MAX_ATTEMPTS):
            try:
                resp = self._session.post(
                    url,
                    json=payload,
                    timeout=self.timeout_seconds,
                )

                if resp.status_code in TERMINAL_STATUS_CODES:
                    raise RPCError(
                        f"{context}: terminal HTTP {resp.status_code}",
                        status_code=resp.status_code,
                    )

                if resp.status_code in RETRYABLE_STATUS_CODES:
                    retry_after = _parse_retry_after(resp)
                    wait = retry_after or _backoff(attempt)
                    logger.warning(
                        "%s: retryable HTTP %d (attempt %d/%d), waiting %.1fs",
                        context, resp.status_code, attempt + 1, MAX_ATTEMPTS, wait,
                    )
                    last_exc = RPCError(
                        f"{context}: HTTP {resp.status_code}", status_code=resp.status_code
                    )
                    time.sleep(wait)
                    continue

                resp.raise_for_status()
                data = resp.json()

                if "error" in data and data["error"] is not None:
                    err = data["error"]
                    raise RPCError(
                        f"{context}: RPC error {err.get('code', '?')}: {err.get('message', '?')}"
                    )

                return data

            except RPCError:
                raise
            except requests.exceptions.Timeout:
                wait = _backoff(attempt)
                logger.warning(
                    "%s: timeout (attempt %d/%d), waiting %.1fs",
                    context, attempt + 1, MAX_ATTEMPTS, wait,
                )
                last_exc = Exception(f"{context}: timeout")
                if attempt < MAX_ATTEMPTS - 1:
                    time.sleep(wait)
            except requests.exceptions.ConnectionError as exc:
                wait = _backoff(attempt)
                logger.warning(
                    "%s: connection error (attempt %d/%d): %s, waiting %.1fs",
                    context, attempt + 1, MAX_ATTEMPTS, exc, wait,
                )
                last_exc = exc
                if attempt < MAX_ATTEMPTS - 1:
                    time.sleep(wait)

        raise RPCError(
            f"{context}: all {MAX_ATTEMPTS} attempts failed — last: {last_exc}"
        )


def _backoff(attempt: int) -> float:
    base = BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS) - 1)]
    jitter = base * JITTER_FRACTION * (random.random() * 2 - 1)
    return max(base + jitter, 0.1)


def _parse_retry_after(resp: requests.Response) -> Optional[float]:
    header = resp.headers.get("Retry-After")
    if not header:
        return None
    try:
        return float(header)
    except (ValueError, TypeError):
        return None


def client_from_env() -> SolanaRPCClient:
    """Build a SolanaRPCClient from environment variables."""
    primary = os.environ.get("SOLANA_RPC_PRIMARY_URL", "")
    fallback = os.environ.get("SOLANA_RPC_FALLBACK_URL")
    if not primary:
        raise ValueError(
            "SOLANA_RPC_PRIMARY_URL is not set. "
            "Configure a primary RPC endpoint before running Solana ingestion."
        )
    return SolanaRPCClient(primary_url=primary, fallback_url=fallback)
