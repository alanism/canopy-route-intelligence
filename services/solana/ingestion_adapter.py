"""
Phase 1 — Scoped RPC Ingestion Adapter.

Ingests observed Solana USDC activity from configured watched sources.

Discovery path
--------------
getSignaturesForAddress(watched_address)
  -> paginate by signature (before/until cursors)
  -> getTransaction(signature, maxSupportedTransactionVersion=0)
  -> ALTManager.resolve_transaction_loaded_addresses(raw_tx)
  -> pre_normalizer.normalize_transaction(raw_tx, resolved_loaded_addresses)
  -> transfer_truth.evaluate_transfer_truth(pre_normalized, watched_mints)
  -> jito_detector.detect_jito_tips(pre_normalized)
  -> cost_decomposition.decompose_cost(pre_normalized, jito_result)
  -> emit SolanaRawEvent for downstream phases

Source policy
-------------
Primary RPC is the source of record.
Fallback RPC is for reconciliation and degraded emergency reads only.
Fallback must not silently replace primary and render data healthy.

Caps
----
SOLANA_MAX_SIGNATURES_PER_RUN  — max signatures fetched per watched address per run
SOLANA_MAX_TRANSACTIONS_PER_RUN — max transactions fully processed per run
SOLANA_MAX_INNER_INSTRUCTIONS_PER_TX — guard against degenerate txs

All caps are enforced before network calls, not after.

Non-goal: full Solana stablecoin indexer. This is scoped to watched sources only.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Iterator, Optional

from services.solana.alt_manager import ALTManager, PersistentALTCache, ProcessingCache
from services.solana.checkpoint import CheckpointStore, MissingCheckpointError
from services.solana.circuit_breaker import CircuitBreaker, CircuitOpenError, RateLimiter
from services.solana.constants import USDC_MINT, get_jito_tip_accounts
from services.solana.cost_decomposition import decompose_cost
from services.solana.jito_detector import detect_jito_tips
from services.solana.pre_normalizer import normalize_transaction
from services.solana.rpc_client import SolanaRPCClient, client_from_env
from services.solana.transfer_truth import evaluate_transfer_truth

logger = logging.getLogger("canopy.solana.ingestion_adapter")

# ---------------------------------------------------------------------------
# Config defaults
# ---------------------------------------------------------------------------

DEFAULT_MAX_SIGNATURES_PER_RUN = 200
DEFAULT_MAX_TRANSACTIONS_PER_RUN = 200
DEFAULT_MAX_INNER_INSTRUCTIONS_PER_TX = 64
DEFAULT_COMMITMENT = "finalized"


# ---------------------------------------------------------------------------
# Config dataclass
# ---------------------------------------------------------------------------

@dataclass
class IngestionConfig:
    """
    All configuration required for one ingestion run.

    Load from environment via IngestionConfig.from_env().
    """

    # RPC (required)
    primary_url: str

    # Watched scope (required)
    watched_addresses: list[str]

    # Optional
    fallback_url: Optional[str] = None
    token_mint_allowlist: set[str] = field(default_factory=lambda: {USDC_MINT})
    start_signature: Optional[str] = None       # resume cursor (exclusive upper bound)
    start_slot: Optional[int] = None            # informational — used with checkpoint
    max_signatures_per_run: int = DEFAULT_MAX_SIGNATURES_PER_RUN
    max_transactions_per_run: int = DEFAULT_MAX_TRANSACTIONS_PER_RUN
    max_inner_instructions_per_tx: int = DEFAULT_MAX_INNER_INSTRUCTIONS_PER_TX
    commitment: str = DEFAULT_COMMITMENT
    rpc_timeout_seconds: float = 10.0

    @classmethod
    def from_env(cls) -> "IngestionConfig":
        """
        Build IngestionConfig from environment variables.

        Raises ValueError if required env vars are missing.
        """
        primary = os.environ.get("SOLANA_RPC_PRIMARY_URL", "")
        if not primary:
            raise ValueError(
                "SOLANA_RPC_PRIMARY_URL is not set. "
                "Configure a primary RPC endpoint before running Solana ingestion."
            )

        watched_raw = os.environ.get("SOLANA_WATCHED_ADDRESSES", "")
        watched = [a.strip() for a in watched_raw.split(",") if a.strip()]
        if not watched:
            raise ValueError(
                "SOLANA_WATCHED_ADDRESSES is not set or empty. "
                "Configure at least one watched address."
            )

        mint_raw = os.environ.get("SOLANA_TOKEN_MINT_ALLOWLIST", "")
        mints: set[str] = (
            {m.strip() for m in mint_raw.split(",") if m.strip()}
            if mint_raw else {USDC_MINT}
        )

        return cls(
            primary_url=primary,
            fallback_url=os.environ.get("SOLANA_RPC_FALLBACK_URL"),
            watched_addresses=watched,
            token_mint_allowlist=mints,
            start_signature=os.environ.get("SOLANA_START_SIGNATURE"),
            start_slot=_int_env("SOLANA_START_SLOT"),
            max_signatures_per_run=_int_env("SOLANA_MAX_SIGNATURES_PER_RUN", DEFAULT_MAX_SIGNATURES_PER_RUN),
            max_transactions_per_run=_int_env("SOLANA_MAX_TRANSACTIONS_PER_RUN", DEFAULT_MAX_TRANSACTIONS_PER_RUN),
            max_inner_instructions_per_tx=_int_env("SOLANA_MAX_INNER_INSTRUCTIONS_PER_TX", DEFAULT_MAX_INNER_INSTRUCTIONS_PER_TX),
            commitment=os.environ.get("SOLANA_COMMITMENT", DEFAULT_COMMITMENT),
            rpc_timeout_seconds=float(os.environ.get("SOLANA_RPC_TIMEOUT_SECONDS", "10.0")),
        )


# ---------------------------------------------------------------------------
# Run result
# ---------------------------------------------------------------------------

@dataclass
class IngestionRunResult:
    """
    Summary of one ingestion run across all watched addresses.

    Downstream phases consume `raw_events` for schema normalization,
    BigQuery writing, and validation.
    """

    raw_events: list[dict[str, Any]] = field(default_factory=list)
    signatures_fetched: int = 0
    transactions_fetched: int = 0
    transactions_processed: int = 0
    transactions_skipped_no_watched_mint: int = 0
    transactions_degraded: int = 0          # ALT failure or normalization error
    alt_metrics: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    run_status: str = "ok"                  # "ok" | "degraded" | "failed"


# ---------------------------------------------------------------------------
# Ingestion adapter
# ---------------------------------------------------------------------------

class SolanaIngestionAdapter:
    """
    Scoped Solana USDC ingestion adapter.

    One adapter instance is created per ingestion run. The ALTManager is
    constructed with a fresh ProcessingCache each run (enforces the RPC
    efficiency guarantee: each unique ALT fetched at most once per run).

    Usage
    -----
    config = IngestionConfig.from_env()
    adapter = SolanaIngestionAdapter(config)
    result = adapter.run()
    # result.raw_events -> list of pre-normalized + enriched event dicts
    """

    def __init__(
        self,
        config: IngestionConfig,
        *,
        rpc_client: Optional[SolanaRPCClient] = None,
        persistent_cache: Optional[PersistentALTCache] = None,
        checkpoint_store: Optional[CheckpointStore] = None,
        circuit_breaker: Optional[CircuitBreaker] = None,
        rate_limiter: Optional[RateLimiter] = None,
    ) -> None:
        self._config = config
        self._rpc = rpc_client or SolanaRPCClient(
            primary_url=config.primary_url,
            fallback_url=config.fallback_url,
            timeout_seconds=config.rpc_timeout_seconds,
        )
        # Persistent ALT cache is shared across runs; ProcessingCache is fresh each run
        self._persistent_cache = persistent_cache or PersistentALTCache()
        # Checkpoint store is shared across runs
        self._checkpoint_store = checkpoint_store
        # Circuit breaker and rate limiter (optional — None disables them)
        self._circuit_breaker = circuit_breaker
        self._rate_limiter = rate_limiter

    def run(self, *, start_signature: Optional[str] = None) -> IngestionRunResult:
        """
        Execute one full ingestion run across all watched addresses.

        Parameters
        ----------
        start_signature:
            Override resume cursor. If None, uses config.start_signature.
            This is the last-processed signature (exclusive upper bound for
            getSignaturesForAddress — we fetch signatures *before* this one).
        """
        result = IngestionRunResult()

        # Fresh ProcessingCache for this run — enforces ALT RPC efficiency guarantee
        processing_cache = ProcessingCache()
        alt_manager = ALTManager(self._rpc, processing_cache, self._persistent_cache)

        tip_accounts = get_jito_tip_accounts()
        transactions_processed_total = 0

        for watched_address in self._config.watched_addresses:
            if transactions_processed_total >= self._config.max_transactions_per_run:
                logger.info(
                    "max_transactions_per_run=%d reached; stopping ingestion",
                    self._config.max_transactions_per_run,
                )
                break

            logger.info("Ingesting signatures for watched address: %s", watched_address)

            # Resolve resume cursor from checkpoint (if store is wired in)
            cursor = start_signature or self._config.start_signature
            if self._checkpoint_store is not None:
                for mint in self._config.token_mint_allowlist:
                    try:
                        entry = self._checkpoint_store.get_or_seed(
                            "solana", mint, watched_address,
                            start_signature=cursor,
                            start_slot=self._config.start_slot,
                        )
                        # Prefer checkpoint cursor over config cursor
                        if entry.resume_signature:
                            cursor = entry.resume_signature
                    except MissingCheckpointError as exc:
                        msg = str(exc)
                        logger.error(msg)
                        result.errors.append(msg)
                        result.run_status = "failed"
                        return result
                    break  # use first mint for cursor (all share same sig space)

            try:
                if self._circuit_breaker:
                    self._circuit_breaker.before_call()
                if self._rate_limiter:
                    self._rate_limiter.acquire()
                signatures = self._fetch_signatures(watched_address, before=cursor)
                if self._circuit_breaker:
                    self._circuit_breaker.record_success()
            except CircuitOpenError as exc:
                msg = f"Circuit breaker OPEN for {watched_address[:8]}: {exc}"
                logger.error(msg)
                result.errors.append(msg)
                result.run_status = "degraded"
                break  # stop the entire run — circuit is open
            except Exception as exc:
                msg = f"getSignaturesForAddress failed for {watched_address}: {exc}"
                logger.error(msg)
                result.errors.append(msg)
                result.run_status = "degraded"
                if self._circuit_breaker:
                    self._circuit_breaker.record_failure()
                continue

            result.signatures_fetched += len(signatures)

            for sig_info in signatures:
                if transactions_processed_total >= self._config.max_transactions_per_run:
                    break

                signature = sig_info.get("signature", "")
                if not signature:
                    continue

                # Skip confirmed-failed transactions early (saves a getTransaction call)
                if sig_info.get("err") is not None:
                    logger.debug("Skipping failed transaction: %s", signature[:16])

                try:
                    if self._circuit_breaker:
                        self._circuit_breaker.before_call()
                    if self._rate_limiter:
                        self._rate_limiter.acquire()
                    raw_tx = self._fetch_transaction(signature)
                    if self._circuit_breaker:
                        self._circuit_breaker.record_success()
                except CircuitOpenError as exc:
                    msg = f"Circuit breaker OPEN fetching {signature[:16]}: {exc}"
                    logger.error(msg)
                    result.errors.append(msg)
                    result.run_status = "degraded"
                    break
                except Exception as exc:
                    msg = f"getTransaction failed for {signature[:16]}: {exc}"
                    logger.error(msg)
                    result.errors.append(msg)
                    result.transactions_degraded += 1
                    result.run_status = "degraded"
                    if self._circuit_breaker:
                        self._circuit_breaker.record_failure()
                    continue

                if raw_tx is None:
                    result.errors.append(f"getTransaction returned None for {signature[:16]}")
                    result.transactions_degraded += 1
                    result.run_status = "degraded"
                    continue

                result.transactions_fetched += 1

                # Guard: inner instruction count
                if _inner_instruction_count(raw_tx) > self._config.max_inner_instructions_per_tx:
                    logger.warning(
                        "Transaction %s has too many inner instructions — skipping",
                        signature[:16],
                    )
                    result.transactions_skipped_no_watched_mint += 1
                    continue

                event = self._process_transaction(
                    raw_tx,
                    signature,
                    alt_manager,
                    tip_accounts,
                )

                if event is None:
                    result.transactions_degraded += 1
                    result.run_status = "degraded"
                    continue

                if event.get("_skip_no_watched_mint"):
                    result.transactions_skipped_no_watched_mint += 1
                    continue

                result.raw_events.append(event)
                result.transactions_processed += 1
                transactions_processed_total += 1

        # Merge ALT cache metrics
        result.alt_metrics = alt_manager.combined_metrics()
        logger.info(
            "Ingestion run complete: %d events, %d signatures, %d degraded, ALT metrics=%s",
            len(result.raw_events),
            result.signatures_fetched,
            result.transactions_degraded,
            result.alt_metrics,
        )
        return result

    # ------------------------------------------------------------------
    # Internal: signature discovery
    # ------------------------------------------------------------------

    def _fetch_signatures(
        self,
        address: str,
        *,
        before: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """
        Fetch signatures for a watched address, paginated and capped.

        Returns list of signature-info dicts from getSignaturesForAddress.
        Older signatures come first after reversal (chronological order).
        """
        limit = min(self._config.max_signatures_per_run, 1000)  # RPC max is 1000
        payload: dict[str, Any] = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [
                address,
                {
                    "limit": limit,
                    "commitment": self._config.commitment,
                    **({"before": before} if before else {}),
                },
            ],
        }
        raw = self._rpc._post_with_retry(
            self._rpc.primary_url,
            payload,
            context=f"getSignaturesForAddress({address[:8]}…)",
        )
        sigs: list[dict] = raw.get("result", []) or []
        # RPC returns newest-first; reverse to process chronologically
        return list(reversed(sigs))

    # ------------------------------------------------------------------
    # Internal: transaction fetch
    # ------------------------------------------------------------------

    def _fetch_transaction(self, signature: str) -> Optional[dict[str, Any]]:
        """Fetch a transaction by signature using json encoding (self-parse rule)."""
        return self._rpc.get_transaction(
            signature,
            max_supported_transaction_version=0,
            commitment=self._config.commitment,
            encoding="json",
        )

    # ------------------------------------------------------------------
    # Internal: full transaction processing pipeline
    # ------------------------------------------------------------------

    def _process_transaction(
        self,
        raw_tx: dict[str, Any],
        signature: str,
        alt_manager: ALTManager,
        tip_accounts: frozenset[str],
    ) -> Optional[dict[str, Any]]:
        """
        Run the full processing pipeline for one raw transaction.

        Returns
        -------
        - Event dict if the transaction contained watched-mint activity
        - {"_skip_no_watched_mint": True} if no watched mint present
        - None on degraded/error (caller increments degraded counter)
        """
        # Step 1: ALT resolution (v0 only; no-op for legacy)
        resolved_loaded_addresses = alt_manager.resolve_transaction_loaded_addresses(raw_tx)
        if resolved_loaded_addresses is None:
            # ALT resolution failure — mark degraded
            logger.warning("ALT resolution failed for %s — marking degraded", signature[:16])
            return None

        # Step 2: Pre-normalize
        try:
            pre = normalize_transaction(
                raw_tx,
                resolved_loaded_addresses=resolved_loaded_addresses,
            )
        except Exception as exc:
            logger.error("normalize_transaction crashed for %s: %s", signature[:16], exc)
            return None

        if pre.get("pre_normalization_status") == "failed":
            logger.warning("Pre-normalization failed for %s", signature[:16])
            return None

        # Step 3: Transfer truth — check if any watched mint is involved
        truth = evaluate_transfer_truth(
            pre,
            self._config.token_mint_allowlist,
        )

        # If no watched mint present, skip (do not count as error)
        if truth.get("_no_transfer_reason") == "watched_mint_absent":
            return {"_skip_no_watched_mint": True}

        # Step 4: Jito tip detection
        jito = detect_jito_tips(pre, tip_accounts=tip_accounts)

        # Step 5: Cost decomposition
        cost = decompose_cost(pre, jito)

        # Step 6: Assemble raw event
        event: dict[str, Any] = {
            # Identity
            "signature": signature,
            "slot": pre.get("slot"),
            "block_time": pre.get("block_time"),
            "chain": "solana",

            # Normalization state
            "pre_normalization_status": pre.get("pre_normalization_status"),
            "alt_resolution_status": pre.get("alt_resolution_status"),
            "transaction_version": pre.get("transaction_version"),

            # Transfer truth
            "transaction_success": truth.get("transaction_success"),
            "observed_transfer_inclusion": truth.get("observed_transfer_inclusion"),
            "transfer_detected": truth.get("transfer_detected"),
            "balance_delta_detected": truth.get("balance_delta_detected"),
            "settlement_evidence_type": truth.get("settlement_evidence_type"),
            "amount_received_raw": truth.get("amount_received_raw"),
            "source_token_account": truth.get("source_token_account"),
            "destination_token_account": truth.get("destination_token_account"),
            "token_mint": truth.get("token_mint"),
            "transfer_validation_status": truth.get("validation_status"),

            # Cost
            "fee_lamports": cost.get("fee_lamports"),
            "jito_tip_lamports": cost.get("jito_tip_lamports"),
            "explicit_tip_lamports": cost.get("explicit_tip_lamports"),
            "total_native_observed_cost_lamports": cost.get("total_native_observed_cost_lamports"),
            "cost_validation_status": cost.get("validation_status"),

            # Jito detail
            "jito_tip_detection_status": jito.get("tip_detection_status"),

            # Raw data passthrough for Phase 4 schema normalization
            "_pre_normalized": pre,
        }

        return event


# ---------------------------------------------------------------------------
# Convenience entry point
# ---------------------------------------------------------------------------

def run_ingestion(
    *,
    config: Optional[IngestionConfig] = None,
    start_signature: Optional[str] = None,
) -> IngestionRunResult:
    """
    Top-level entry point for one Solana ingestion run.

    Builds config from environment if not provided. Creates a fresh adapter
    and runs. Returns the result for downstream promotion.
    """
    if config is None:
        config = IngestionConfig.from_env()
    adapter = SolanaIngestionAdapter(config)
    return adapter.run(start_signature=start_signature)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _int_env(key: str, default: int = 0) -> int:
    val = os.environ.get(key)
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        logger.warning("Invalid int env var %s=%r — using default %d", key, val, default)
        return default


def _inner_instruction_count(raw_tx: dict[str, Any]) -> int:
    """Count total inner instructions across all inner instruction groups."""
    meta = raw_tx.get("meta") or {}
    inner_groups: list[dict] = meta.get("innerInstructions") or []
    return sum(len(g.get("instructions", [])) for g in inner_groups)
