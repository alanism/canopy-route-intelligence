"""
Phase 0B — ALTManager.

Resolves Address Lookup Tables for Solana v0 transactions using a two-layer
cache strategy: run-scoped ProcessingCache (in-memory) + persistent file cache
(data/solana_alt_cache.json).

Cache lookup order
------------------
1. ProcessingCache  — avoids duplicate RPC calls within one ingestion run
2. PersistentALTCache — avoids repeat fetches across runs
3. RPC fetch (getAccountInfo on the ALT address)
4. On success, write to both caches

Key rules
---------
- Do not fetch an ALT repeatedly if it is cached and valid.
- Do not silently continue with unresolved account indexes.
- Provider-parsed output (jsonParsed encoding) is used ONLY to read the ALT
  account's address list — not as source of record for transaction transfer
  events. The self-parse rule applies to transaction data, not ALT structure.
- Failed ALT resolution marks the transaction degraded; healthy promotion is
  blocked until ALTManager has fully resolved all lookups.

ProcessingCache efficiency guarantee
--------------------------------------
If 1,000 versioned transactions in one ingestion run all reference the same
3 ALTs, getAccountInfo must be called at most 3 times — once per unique ALT
address. The ProcessingCache is the mechanism that enforces this.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional, Protocol, runtime_checkable

logger = logging.getLogger("canopy.solana.alt_manager")

PERSISTENT_CACHE_PATH = os.path.join("data", "solana_alt_cache.json")
CACHE_SCHEMA_VERSION = "1"


# ---------------------------------------------------------------------------
# RPC protocol — allows tests to inject a mock without importing rpc_client
# ---------------------------------------------------------------------------

@runtime_checkable
class ALTFetcher(Protocol):
    """Minimal interface required by ALTManager. SolanaRPCClient satisfies this."""

    def get_account_info(
        self,
        address: str,
        *,
        encoding: str,
        commitment: str,
        use_fallback: bool,
    ) -> Optional[dict[str, Any]]: ...

    @property
    def provider_mode(self) -> str: ...


# ---------------------------------------------------------------------------
# Layer 1 — ProcessingCache (run-scoped in-memory)
# ---------------------------------------------------------------------------

class ProcessingCache:
    """
    Run-scoped in-memory cache for ALT resolution.

    Created fresh at the start of each ingestion run. Prevents redundant
    getAccountInfo calls when many transactions reference the same ALTs
    within a single run.

    Metrics track hits/misses so the first-slice gate can verify the
    efficiency guarantee.
    """

    def __init__(self) -> None:
        self._cache: dict[str, list[str]] = {}
        self._hits: int = 0
        self._misses: int = 0

    def get(self, alt_address: str) -> Optional[list[str]]:
        result = self._cache.get(alt_address)
        if result is not None:
            self._hits += 1
            logger.debug("ProcessingCache HIT: %s", alt_address[:8])
            return result
        self._misses += 1
        return None

    def set(self, alt_address: str, addresses: list[str]) -> None:
        self._cache[alt_address] = addresses

    def metrics(self) -> dict[str, int]:
        return {
            "processing_cache_hits": self._hits,
            "processing_cache_misses": self._misses,
            "processing_cache_size": len(self._cache),
        }

    def clear(self) -> None:
        self._cache.clear()
        self._hits = 0
        self._misses = 0


# ---------------------------------------------------------------------------
# Layer 2 — PersistentALTCache (file-based JSON, across runs)
# ---------------------------------------------------------------------------

class PersistentALTCache:
    """
    File-based JSON cache that persists ALT resolution across ingestion runs.

    Schema per entry (nested under lookup_table_address key):
    {
      "schema_version": "1",
      "lookup_table_address": "...",
      "addresses": ["..."],
      "fetched_at": "2026-05-04T...",
      "slot_or_context": 0,
      "provider": "primary|fallback",
      "checksum": "<sha256 of '|'.join(sorted(addresses))>"
    }
    """

    SCHEMA_VERSION = CACHE_SCHEMA_VERSION

    def __init__(self, cache_path: str = PERSISTENT_CACHE_PATH) -> None:
        self._path = cache_path
        self._store: dict[str, dict[str, Any]] = {}
        self._hits: int = 0
        self._misses: int = 0
        self._dirty: bool = False
        self._load()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def get(self, alt_address: str) -> Optional[list[str]]:
        """
        Return cached addresses for alt_address, or None on miss.

        Validates checksum before returning. Returns None on checksum failure
        (forces a fresh fetch).
        """
        entry = self._store.get(alt_address)
        if entry is None:
            self._misses += 1
            logger.debug("PersistentCache MISS: %s", alt_address[:8])
            return None

        addresses = entry.get("addresses", [])
        stored_checksum = entry.get("checksum", "")
        computed = _checksum(addresses)

        if stored_checksum and computed != stored_checksum:
            logger.warning(
                "PersistentCache checksum mismatch for %s — evicting and re-fetching",
                alt_address,
            )
            del self._store[alt_address]
            self._dirty = True
            self._misses += 1
            return None

        self._hits += 1
        logger.debug("PersistentCache HIT: %s (%d addresses)", alt_address[:8], len(addresses))
        return addresses

    def set(
        self,
        alt_address: str,
        addresses: list[str],
        *,
        slot_or_context: int = 0,
        provider: str = "primary",
    ) -> None:
        """Store an ALT resolution in the persistent cache."""
        entry: dict[str, Any] = {
            "schema_version": self.SCHEMA_VERSION,
            "lookup_table_address": alt_address,
            "addresses": addresses,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "slot_or_context": slot_or_context,
            "provider": provider,
            "checksum": _checksum(addresses),
        }
        self._store[alt_address] = entry
        self._dirty = True

    def flush(self) -> None:
        """Write dirty cache to disk. No-op if nothing changed."""
        if not self._dirty:
            return
        try:
            os.makedirs(os.path.dirname(os.path.abspath(self._path)), exist_ok=True)
            tmp_path = self._path + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as fh:
                json.dump(self._store, fh, indent=2)
            os.replace(tmp_path, self._path)
            self._dirty = False
            logger.debug("PersistentCache flushed to %s (%d entries)", self._path, len(self._store))
        except OSError as exc:
            logger.error("PersistentCache flush failed: %s", exc)

    def metrics(self) -> dict[str, int]:
        return {
            "persistent_cache_hits": self._hits,
            "persistent_cache_misses": self._misses,
            "persistent_cache_size": len(self._store),
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _load(self) -> None:
        if not os.path.exists(self._path):
            return
        try:
            with open(self._path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
            if isinstance(raw, dict):
                self._store = raw
                logger.debug(
                    "PersistentCache loaded %d entries from %s", len(self._store), self._path
                )
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("PersistentCache load failed (%s); starting empty", exc)
            self._store = {}


# ---------------------------------------------------------------------------
# ALTManager — orchestrates the two-layer cache + RPC fetch
# ---------------------------------------------------------------------------

class ALTManager:
    """
    Resolves Address Lookup Tables for Solana v0 transactions.

    Inject a fresh ProcessingCache at the start of each ingestion run.
    The PersistentALTCache and rpc_client are shared across runs.

    Usage
    -----
    # At ingestion-run start:
    processing_cache = ProcessingCache()
    manager = ALTManager(rpc_client, processing_cache, persistent_cache)

    # For each raw v0 transaction:
    resolved = manager.resolve_transaction_loaded_addresses(raw_tx)
    if resolved is None:
        # ALT resolution failed — mark degraded, skip healthy promotion
        ...
    pre = normalize_transaction(raw_tx, resolved_loaded_addresses=resolved)
    """

    def __init__(
        self,
        rpc_client: ALTFetcher,
        processing_cache: ProcessingCache,
        persistent_cache: PersistentALTCache,
    ) -> None:
        self._rpc = rpc_client
        self._proc = processing_cache
        self._pers = persistent_cache

    def resolve_transaction_loaded_addresses(
        self,
        raw_tx: dict[str, Any],
    ) -> Optional[dict[str, list[str]]]:
        """
        Resolve all loaded addresses for a v0 transaction.

        Returns {"writable": [...], "readonly": [...]} ready to pass into
        pre_normalizer.normalize_transaction() as resolved_loaded_addresses.

        Returns None if any ALT fails to resolve. The caller must treat None
        as a degraded state and must not promote canonical metrics as healthy.
        """
        message = raw_tx.get("transaction", {}).get("message", {})
        alt_lookups: list[dict] = message.get("addressTableLookups", [])

        if not alt_lookups:
            return {"writable": [], "readonly": []}

        all_writable: list[str] = []
        all_readonly: list[str] = []

        for lookup in alt_lookups:
            alt_address: str = lookup.get("accountKey", "")
            writable_indexes: list[int] = lookup.get("writableIndexes", [])
            readonly_indexes: list[int] = lookup.get("readonlyIndexes", [])

            if not alt_address:
                logger.error("ALT lookup entry has no accountKey — transaction degraded")
                return None

            alt_addresses = self._resolve_alt(alt_address)

            if alt_addresses is None:
                logger.error(
                    "ALT resolution failed for %s — marking transaction degraded", alt_address
                )
                return None

            # Pick writable addresses by index
            for idx in writable_indexes:
                if idx >= len(alt_addresses):
                    logger.error(
                        "ALT %s writable index %d out of range (len=%d)",
                        alt_address, idx, len(alt_addresses),
                    )
                    return None
                all_writable.append(alt_addresses[idx])

            # Pick readonly addresses by index
            for idx in readonly_indexes:
                if idx >= len(alt_addresses):
                    logger.error(
                        "ALT %s readonly index %d out of range (len=%d)",
                        alt_address, idx, len(alt_addresses),
                    )
                    return None
                all_readonly.append(alt_addresses[idx])

        return {"writable": all_writable, "readonly": all_readonly}

    def combined_metrics(self) -> dict[str, int]:
        """Return cache metrics for both layers — used by the first-slice gate."""
        return {
            **self._proc.metrics(),
            **self._pers.metrics(),
        }

    # ------------------------------------------------------------------
    # Internal: resolve a single ALT through the two-layer cache
    # ------------------------------------------------------------------

    def _resolve_alt(self, alt_address: str) -> Optional[list[str]]:
        """
        Resolve one ALT through the cache hierarchy.

        Order: ProcessingCache → PersistentCache → RPC → write both caches.
        """
        # 1. ProcessingCache
        cached = self._proc.get(alt_address)
        if cached is not None:
            return cached

        # 2. PersistentCache
        persisted = self._pers.get(alt_address)
        if persisted is not None:
            # Warm ProcessingCache so subsequent calls in this run skip RPC
            self._proc.set(alt_address, persisted)
            return persisted

        # 3. RPC fetch
        logger.info("Fetching ALT via RPC: %s", alt_address)
        addresses = self._fetch_via_rpc(alt_address)

        if addresses is None:
            return None

        # 4. Write to both caches
        slot = getattr(self._rpc, "_last_slot", 0)
        provider = getattr(self._rpc, "provider_mode", "primary")

        self._pers.set(alt_address, addresses, slot_or_context=slot, provider=provider)
        self._pers.flush()
        self._proc.set(alt_address, addresses)

        return addresses

    def _fetch_via_rpc(self, alt_address: str) -> Optional[list[str]]:
        """
        Fetch the ALT address list from the RPC provider.

        Uses jsonParsed encoding to read the ALT account structure.
        This is the only place provider-parsed output is accepted — for reading
        the binary ALT account format, not for transaction transfer events.
        """
        try:
            account_info = self._rpc.get_account_info(
                alt_address,
                encoding="jsonParsed",
                commitment="finalized",
                use_fallback=False,
            )
        except Exception as exc:
            logger.error("RPC getAccountInfo failed for ALT %s: %s", alt_address, exc)
            return None

        if account_info is None:
            logger.warning("ALT account %s not found on-chain", alt_address)
            return None

        addresses = _extract_alt_addresses(account_info, alt_address)
        return addresses


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_alt_addresses(
    account_info: dict[str, Any],
    alt_address: str,
) -> Optional[list[str]]:
    """
    Extract the address list from a jsonParsed ALT account response.

    Expected shape:
    {
      "data": {
        "parsed": {
          "info": {"addresses": [...]},
          "type": "lookupTable"
        }
      }
    }
    """
    try:
        data = account_info.get("data", {})

        # jsonParsed format
        if isinstance(data, dict):
            parsed = data.get("parsed", {})
            if parsed.get("type") != "lookupTable":
                logger.warning(
                    "ALT %s account is not a lookupTable (type=%s)",
                    alt_address, parsed.get("type"),
                )
                return None
            info = parsed.get("info", {})
            addresses = info.get("addresses", [])
            if not isinstance(addresses, list):
                logger.error("ALT %s addresses field is not a list", alt_address)
                return None
            return [str(a) for a in addresses]

        logger.error(
            "ALT %s account data is in unexpected format (type=%s)",
            alt_address, type(data).__name__,
        )
        return None

    except Exception as exc:
        logger.error("Failed to extract ALT addresses for %s: %s", alt_address, exc)
        return None


def _checksum(addresses: list[str]) -> str:
    """SHA-256 of pipe-joined sorted addresses — deterministic across Python versions."""
    payload = "|".join(sorted(addresses))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
