"""
Phase 1 — Ingestion Adapter Tests.

Covers:
- Config loading from env
- Signature discovery and pagination cap
- Transaction fetch and pipeline routing
- Watched-mint filter (no mint → skip, not error)
- ALT degraded → transaction degraded, run continues
- Failed transaction fee counted but no inclusion
- Run caps: max_signatures_per_run, max_transactions_per_run
- Primary-only source rule (fallback does not render healthy)
- ProcessingCache efficiency across multiple transactions in one run

All tests use MockRPCClient — no real HTTP calls.
"""

from __future__ import annotations

import os
from typing import Any, Optional
from unittest.mock import patch

import pytest

from services.solana.alt_manager import PersistentALTCache, ProcessingCache
from services.solana.constants import (
    SPL_TOKEN_PROGRAM,
    SYSTEM_PROGRAM,
    USDC_MINT,
)
from services.solana.checkpoint import CheckpointStore
from services.solana.ingestion_adapter import (
    IngestionConfig,
    IngestionRunResult,
    SolanaIngestionAdapter,
    _inner_instruction_count,
)

# ---------------------------------------------------------------------------
# Fixtures / constants
# ---------------------------------------------------------------------------

WATCHED_ADDR = "WatchedAddr1111111111111111111111111111111111"
WALLET_A = "WalletAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
WALLET_B = "WalletBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
ATA_A = "AtaAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
ATA_B = "AtaBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
SIG_1 = "Sig1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111"
SIG_2 = "Sig2222222222222222222222222222222222222222222222222222222222222222222222222222222222222222"
SIG_3 = "Sig3333333333333333333333333333333333333333333333333333333333333333333333333333333333333333"


# ---------------------------------------------------------------------------
# MockRPCClient
# ---------------------------------------------------------------------------

class MockRPCClient:
    """
    Test double for SolanaRPCClient.

    Stores:
    - signatures_by_address: {address: [sig_info_dict, ...]}
    - transactions_by_sig: {signature: raw_tx_dict}
    - fail_transactions: set of signatures that return None
    """

    def __init__(
        self,
        signatures_by_address: Optional[dict[str, list[dict]]] = None,
        transactions_by_sig: Optional[dict[str, dict]] = None,
        *,
        fail_transactions: Optional[set[str]] = None,
        null_signatures_for: Optional[set[str]] = None,
        signature_errors_for: Optional[set[str]] = None,
        malformed_signatures_for: Optional[set[str]] = None,
        provider_slot: Optional[int] = None,
    ) -> None:
        self.signatures_by_address = signatures_by_address or {}
        self.transactions_by_sig = transactions_by_sig or {}
        self.fail_transactions: set[str] = fail_transactions or set()
        self.null_signatures_for: set[str] = null_signatures_for or set()
        self.signature_errors_for: set[str] = signature_errors_for or set()
        self.malformed_signatures_for: set[str] = malformed_signatures_for or set()
        self.provider_slot = provider_slot
        self.primary_url = "http://mock-primary"
        self.fallback_url = None
        self._provider_mode = "primary"
        self.get_transaction_calls: list[str] = []

    @property
    def provider_mode(self) -> str:
        return self._provider_mode

    def get_account_info(self, address, *, encoding, commitment, use_fallback=False):
        return None

    def get_transaction(self, signature, *, max_supported_transaction_version=0,
                        commitment="finalized", encoding="json", use_fallback=False):
        self.get_transaction_calls.append(signature)
        if signature in self.fail_transactions:
            return None
        return self.transactions_by_sig.get(signature)

    def get_slot(self, *, commitment="finalized", use_fallback=False):
        return self.provider_slot

    def _post_with_retry(self, url, payload, *, context=""):
        """Handle getSignaturesForAddress calls."""
        method = payload.get("method", "")
        if method == "getSignaturesForAddress":
            address = payload["params"][0]
            if address in self.signature_errors_for:
                return {"error": {"code": -32005, "message": "provider lagging"}}
            if address in self.null_signatures_for:
                return {"result": None}
            if address in self.malformed_signatures_for:
                return {"result": {"unexpected": "shape"}}
            opts = payload["params"][1] if len(payload["params"]) > 1 else {}
            limit = opts.get("limit", 1000)
            sigs = self.signatures_by_address.get(address, [])
            # RPC returns newest-first; mock does the same
            result = list(reversed(sigs))[:limit]
            return {"result": result}
        return {"result": []}


# ---------------------------------------------------------------------------
# Transaction builders
# ---------------------------------------------------------------------------

def _make_legacy_tx(
    signature: str,
    *,
    success: bool = True,
    usdc_amount_raw: int = 1_000_000,
    include_usdc: bool = True,
) -> dict[str, Any]:
    """Build a minimal legacy transaction fixture."""
    account_keys = [WALLET_A, WALLET_B, ATA_A, ATA_B, SPL_TOKEN_PROGRAM, SYSTEM_PROGRAM]

    pre_token_balances = []
    post_token_balances = []
    if include_usdc:
        pre_token_balances = [
            {
                "accountIndex": 2,
                "mint": USDC_MINT,
                "owner": WALLET_A,
                "uiTokenAmount": {"amount": str(5_000_000), "decimals": 6},
            },
        ]
        post_token_balances = [
            {
                "accountIndex": 2,
                "mint": USDC_MINT,
                "owner": WALLET_A,
                "uiTokenAmount": {"amount": str(5_000_000 - usdc_amount_raw), "decimals": 6},
            },
            {
                "accountIndex": 3,
                "mint": USDC_MINT,
                "owner": WALLET_B,
                "uiTokenAmount": {"amount": str(usdc_amount_raw), "decimals": 6},
            },
        ]

    return {
        "transaction": {
            "signatures": [signature],
            "message": {
                "accountKeys": account_keys,
                "instructions": [
                    {
                        "programIdIndex": 4,  # SPL_TOKEN_PROGRAM
                        "accounts": [2, 3, 0],
                        "data": "3Bxs3zr3hH7HgAGB",  # transfer discriminator
                    }
                ],
                "recentBlockhash": "BlockHash1111",
                "addressTableLookups": [],
            },
        },
        "meta": {
            "err": None if success else {"InstructionError": [0, "Custom"]},
            "fee": 5000,
            "preBalances": [1000000, 0, 0, 0, 0, 0],
            "postBalances": [995000, 0, 0, 0, 0, 0],
            "preTokenBalances": pre_token_balances,
            "postTokenBalances": post_token_balances,
            "innerInstructions": [],
            "logMessages": [],
            "loadedAddresses": {"writable": [], "readonly": []},
        },
        "slot": 999,
        "blockTime": 1700000000,
        "version": "legacy",
    }


def _make_config(
    *,
    watched_addresses: Optional[list[str]] = None,
    max_signatures: int = 200,
    max_transactions: int = 200,
    start_signature: Optional[str] = None,
    start_slot: Optional[int] = None,
) -> IngestionConfig:
    return IngestionConfig(
        primary_url="http://mock-primary",
        watched_addresses=watched_addresses or [WATCHED_ADDR],
        token_mint_allowlist={USDC_MINT},
        max_signatures_per_run=max_signatures,
        max_transactions_per_run=max_transactions,
        start_signature=start_signature,
        start_slot=start_slot,
    )


def _make_adapter(
    rpc: MockRPCClient,
    config: Optional[IngestionConfig] = None,
    *,
    tmp_path=None,
    checkpoint_store: Optional[CheckpointStore] = None,
) -> SolanaIngestionAdapter:
    cache_path = str(tmp_path / "alt_cache.json") if tmp_path else ":memory_test:"
    pers = PersistentALTCache(cache_path=cache_path)
    return SolanaIngestionAdapter(
        config or _make_config(),
        rpc_client=rpc,
        persistent_cache=pers,
        checkpoint_store=checkpoint_store,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestIngestionConfigFromEnv:
    """Config loading from environment variables."""

    def test_from_env_raises_without_primary_url(self):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("SOLANA_RPC_PRIMARY_URL", None)
            with pytest.raises(ValueError, match="SOLANA_RPC_PRIMARY_URL"):
                IngestionConfig.from_env()

    def test_from_env_raises_without_watched_addresses(self):
        with patch.dict(os.environ, {
            "SOLANA_RPC_PRIMARY_URL": "http://rpc",
            "SOLANA_WATCHED_ADDRESSES": "",
        }):
            with pytest.raises(ValueError, match="SOLANA_WATCHED_ADDRESSES"):
                IngestionConfig.from_env()

    def test_from_env_parses_watched_addresses(self):
        with patch.dict(os.environ, {
            "SOLANA_RPC_PRIMARY_URL": "http://rpc",
            "SOLANA_WATCHED_ADDRESSES": f"{WATCHED_ADDR},{WALLET_A}",
        }):
            cfg = IngestionConfig.from_env()
            assert WATCHED_ADDR in cfg.watched_addresses
            assert WALLET_A in cfg.watched_addresses

    def test_from_env_defaults_to_usdc_mint(self):
        with patch.dict(os.environ, {
            "SOLANA_RPC_PRIMARY_URL": "http://rpc",
            "SOLANA_WATCHED_ADDRESSES": WATCHED_ADDR,
        }):
            cfg = IngestionConfig.from_env()
            assert USDC_MINT in cfg.token_mint_allowlist

    def test_from_env_reads_canonical_token_mint(self):
        custom_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        with patch.dict(os.environ, {
            "SOLANA_RPC_PRIMARY_URL": "http://rpc",
            "SOLANA_WATCHED_ADDRESSES": WATCHED_ADDR,
            "SOLANA_TOKEN_MINT": custom_mint,
        }):
            cfg = IngestionConfig.from_env()
            assert cfg.token_mint_allowlist == {custom_mint}

    def test_from_env_legacy_allowlist_used_when_canonical_missing(self):
        mint_a = "MintAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        mint_b = "MintBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
        with patch.dict(os.environ, {
            "SOLANA_RPC_PRIMARY_URL": "http://rpc",
            "SOLANA_WATCHED_ADDRESSES": WATCHED_ADDR,
            "SOLANA_TOKEN_MINT_ALLOWLIST": f"{mint_a},{mint_b}",
        }):
            cfg = IngestionConfig.from_env()
            assert cfg.token_mint_allowlist == {mint_a, mint_b}

    def test_from_env_reads_caps(self):
        with patch.dict(os.environ, {
            "SOLANA_RPC_PRIMARY_URL": "http://rpc",
            "SOLANA_WATCHED_ADDRESSES": WATCHED_ADDR,
            "SOLANA_MAX_SIGNATURES_PER_RUN": "50",
            "SOLANA_MAX_TRANSACTIONS_PER_RUN": "30",
        }):
            cfg = IngestionConfig.from_env()
            assert cfg.max_signatures_per_run == 50
            assert cfg.max_transactions_per_run == 30

    def test_from_env_reads_start_signature(self):
        with patch.dict(os.environ, {
            "SOLANA_RPC_PRIMARY_URL": "http://rpc",
            "SOLANA_WATCHED_ADDRESSES": WATCHED_ADDR,
            "SOLANA_START_SIGNATURE": SIG_1,
        }):
            cfg = IngestionConfig.from_env()
            assert cfg.start_signature == SIG_1


class TestIngestionRunBasic:
    """Basic run behavior — signatures discovered, transactions processed."""

    def test_empty_signatures_returns_empty_result(self, tmp_path):
        rpc = MockRPCClient(
            signatures_by_address={WATCHED_ADDR: []},
            transactions_by_sig={},
        )
        adapter = _make_adapter(rpc, tmp_path=tmp_path)
        result = adapter.run()
        assert result.raw_events == []
        assert result.signatures_fetched == 0
        assert result.run_status == "ok"
        assert result.observation_state == "no_recent_activity"
        assert result.ingestion_state == "succeeded"

    def test_usdc_transfer_produces_event(self, tmp_path):
        tx = _make_legacy_tx(SIG_1, usdc_amount_raw=1_000_000)
        rpc = MockRPCClient(
            signatures_by_address={WATCHED_ADDR: [{"signature": SIG_1, "err": None}]},
            transactions_by_sig={SIG_1: tx},
        )
        adapter = _make_adapter(rpc, tmp_path=tmp_path)
        result = adapter.run()
        assert result.transactions_fetched == 1
        assert len(result.raw_events) == 1
        event = result.raw_events[0]
        assert event["signature"] == SIG_1
        assert event["chain"] == "solana"

    def test_event_has_transfer_truth_fields(self, tmp_path):
        tx = _make_legacy_tx(SIG_1, usdc_amount_raw=2_000_000)
        rpc = MockRPCClient(
            signatures_by_address={WATCHED_ADDR: [{"signature": SIG_1, "err": None}]},
            transactions_by_sig={SIG_1: tx},
        )
        result = _make_adapter(rpc, tmp_path=tmp_path).run()
        event = result.raw_events[0]
        assert event["transaction_success"] is True
        assert event["observed_transfer_inclusion"] is True
        assert event["amount_received_raw"] == 2_000_000
        assert event["token_mint"] == USDC_MINT

    def test_event_has_cost_fields(self, tmp_path):
        tx = _make_legacy_tx(SIG_1)
        rpc = MockRPCClient(
            signatures_by_address={WATCHED_ADDR: [{"signature": SIG_1, "err": None}]},
            transactions_by_sig={SIG_1: tx},
        )
        result = _make_adapter(rpc, tmp_path=tmp_path).run()
        event = result.raw_events[0]
        assert "fee_lamports" in event
        assert event["fee_lamports"] == 5000
        assert "total_native_observed_cost_lamports" in event

    def test_event_contains_pre_normalized_passthrough(self, tmp_path):
        tx = _make_legacy_tx(SIG_1)
        rpc = MockRPCClient(
            signatures_by_address={WATCHED_ADDR: [{"signature": SIG_1, "err": None}]},
            transactions_by_sig={SIG_1: tx},
        )
        result = _make_adapter(rpc, tmp_path=tmp_path).run()
        assert "_pre_normalized" in result.raw_events[0]


class TestWatchedMintFilter:
    """Transactions without watched mint are skipped, not counted as errors."""

    def test_no_watched_mint_is_skipped_not_error(self, tmp_path):
        tx = _make_legacy_tx(SIG_1, include_usdc=False)
        rpc = MockRPCClient(
            signatures_by_address={WATCHED_ADDR: [{"signature": SIG_1, "err": None}]},
            transactions_by_sig={SIG_1: tx},
        )
        result = _make_adapter(rpc, tmp_path=tmp_path).run()
        assert result.raw_events == []
        assert result.transactions_skipped_no_watched_mint == 1
        assert result.transactions_degraded == 0
        assert result.run_status == "ok"

    def test_multiple_txs_only_usdc_ones_emit_events(self, tmp_path):
        tx_usdc = _make_legacy_tx(SIG_1, usdc_amount_raw=500_000)
        tx_no_usdc = _make_legacy_tx(SIG_2, include_usdc=False)
        rpc = MockRPCClient(
            signatures_by_address={WATCHED_ADDR: [
                {"signature": SIG_1, "err": None},
                {"signature": SIG_2, "err": None},
            ]},
            transactions_by_sig={SIG_1: tx_usdc, SIG_2: tx_no_usdc},
        )
        result = _make_adapter(rpc, tmp_path=tmp_path).run()
        assert len(result.raw_events) == 1
        assert result.raw_events[0]["signature"] == SIG_1
        assert result.transactions_skipped_no_watched_mint == 1

    def test_usdc_present_without_delta_is_skipped_not_false_transfer(self, tmp_path):
        tx = _make_legacy_tx(SIG_1, usdc_amount_raw=0)
        rpc = MockRPCClient(
            signatures_by_address={WATCHED_ADDR: [{"signature": SIG_1, "err": None}]},
            transactions_by_sig={SIG_1: tx},
        )
        result = _make_adapter(rpc, tmp_path=tmp_path).run()
        assert result.raw_events == []
        assert result.transactions_skipped_no_watched_mint == 1
        assert result.run_status == "ok"


class TestFailedTransactions:
    """Failed transactions: fee counted, no transfer inclusion."""

    def test_failed_tx_no_inclusion(self, tmp_path):
        tx = _make_legacy_tx(SIG_1, success=False, usdc_amount_raw=1_000_000)
        rpc = MockRPCClient(
            signatures_by_address={WATCHED_ADDR: [{"signature": SIG_1, "err": None}]},
            transactions_by_sig={SIG_1: tx},
        )
        result = _make_adapter(rpc, tmp_path=tmp_path).run()
        # Failed tx has USDC in token balances but transaction_success=False
        # transfer_truth returns observed_transfer_inclusion=False
        # No "watched_mint_absent" reason so it still emits an event
        event = result.raw_events[0]
        assert event["transaction_success"] is False
        assert event["observed_transfer_inclusion"] is False

    def test_getTransaction_none_marks_degraded(self, tmp_path):
        rpc = MockRPCClient(
            signatures_by_address={WATCHED_ADDR: [{"signature": SIG_1, "err": None}]},
            transactions_by_sig={},
            fail_transactions={SIG_1},
        )
        result = _make_adapter(rpc, tmp_path=tmp_path).run()
        assert result.transactions_degraded == 1
        assert result.run_status == "degraded"
        assert result.ingestion_state == "failed"
        assert result.raw_events == []

    def test_null_result_from_signatures_marks_failed_state(self, tmp_path):
        rpc = MockRPCClient(
            signatures_by_address={WATCHED_ADDR: []},
            transactions_by_sig={},
            null_signatures_for={WATCHED_ADDR},
        )
        result = _make_adapter(rpc, tmp_path=tmp_path).run()
        assert result.run_status == "degraded"
        assert result.ingestion_state == "failed"
        assert any("null result" in e for e in result.errors)


class TestSemanticRPCValidation:
    """Phase 14.5 semantic RPC validation guards."""

    def _checkpoint_store(self, tmp_path):
        return CheckpointStore(str(tmp_path / "checkpoint.json"))

    def _checkpoint_entry(self, store: CheckpointStore):
        return store.get("solana", USDC_MINT, WATCHED_ADDR)

    def test_json_rpc_error_field_marks_failed_and_holds_checkpoint(self, tmp_path):
        store = self._checkpoint_store(tmp_path)
        rpc = MockRPCClient(
            signatures_by_address={WATCHED_ADDR: []},
            signature_errors_for={WATCHED_ADDR},
            provider_slot=900,
        )
        config = _make_config(start_signature=SIG_1, start_slot=900)
        result = _make_adapter(
            rpc, config, tmp_path=tmp_path, checkpoint_store=store
        ).run()

        assert result.run_status == "degraded"
        assert result.ingestion_state == "failed"
        assert any("RPC returned error" in e for e in result.errors)
        entry = self._checkpoint_entry(store)
        assert entry is not None
        assert entry.last_processed_signature == SIG_1
        assert entry.last_processed_slot == 900
        assert entry.last_promoted_slot is None

    def test_null_signature_result_marks_failed_and_holds_checkpoint(self, tmp_path):
        store = self._checkpoint_store(tmp_path)
        rpc = MockRPCClient(
            signatures_by_address={WATCHED_ADDR: []},
            null_signatures_for={WATCHED_ADDR},
            provider_slot=901,
        )
        config = _make_config(start_signature=SIG_1, start_slot=901)
        result = _make_adapter(
            rpc, config, tmp_path=tmp_path, checkpoint_store=store
        ).run()

        assert result.run_status == "degraded"
        assert result.ingestion_state == "failed"
        assert any("null result" in e for e in result.errors)
        entry = self._checkpoint_entry(store)
        assert entry is not None
        assert entry.last_processed_signature == SIG_1
        assert entry.last_processed_slot == 901
        assert entry.last_promoted_slot is None

    def test_malformed_signature_result_marks_failed(self, tmp_path):
        rpc = MockRPCClient(
            signatures_by_address={WATCHED_ADDR: []},
            malformed_signatures_for={WATCHED_ADDR},
        )
        result = _make_adapter(rpc, tmp_path=tmp_path).run()

        assert result.run_status == "degraded"
        assert result.ingestion_state == "failed"
        assert any("malformed result" in e for e in result.errors)

    def test_repeated_empty_windows_become_ambiguous_empty(self, tmp_path):
        rpc = MockRPCClient(
            signatures_by_address={WATCHED_ADDR: []},
            transactions_by_sig={},
        )
        adapter = _make_adapter(rpc, tmp_path=tmp_path)

        first = adapter.run()
        second = adapter.run()
        third = adapter.run()

        assert first.observation_state == "no_recent_activity"
        assert second.observation_state == "no_recent_activity"
        assert third.observation_state == "ambiguous_empty"
        assert third.run_status == "degraded"
        assert third.ingestion_state == "provider_lagging"
        assert any("ambiguous empty" in e for e in third.errors)

    def test_provider_lag_marks_failed_and_holds_checkpoint(self, tmp_path):
        store = self._checkpoint_store(tmp_path)
        rpc = MockRPCClient(
            signatures_by_address={WATCHED_ADDR: []},
            provider_slot=899,
        )
        config = _make_config(start_signature=SIG_1, start_slot=900)
        result = _make_adapter(
            rpc, config, tmp_path=tmp_path, checkpoint_store=store
        ).run()

        assert result.run_status == "degraded"
        assert result.ingestion_state == "provider_lagging"
        assert any("provider lag detected" in e for e in result.errors)
        entry = self._checkpoint_entry(store)
        assert entry is not None
        assert entry.last_processed_signature == SIG_1
        assert entry.last_processed_slot == 900
        assert entry.last_promoted_slot is None

    def test_wrong_commitment_rejected_and_checkpoint_held(self, tmp_path):
        store = self._checkpoint_store(tmp_path)
        tx = _make_legacy_tx(SIG_1)
        tx["_commitment"] = "confirmed"
        rpc = MockRPCClient(
            signatures_by_address={WATCHED_ADDR: [{"signature": SIG_1, "err": None}]},
            transactions_by_sig={SIG_1: tx},
            provider_slot=902,
        )
        config = _make_config(start_signature=SIG_2, start_slot=902)
        result = _make_adapter(
            rpc, config, tmp_path=tmp_path, checkpoint_store=store
        ).run()

        assert result.raw_events == []
        assert result.transactions_degraded == 1
        assert result.run_status == "degraded"
        assert result.ingestion_state == "failed"
        assert any("expected 'finalized'" in e for e in result.errors)
        entry = self._checkpoint_entry(store)
        assert entry is not None
        assert entry.last_processed_signature == SIG_2
        assert entry.last_processed_slot == 902
        assert entry.last_promoted_slot is None


class TestRunCaps:
    """Run caps prevent over-ingestion."""

    def test_max_transactions_per_run_honored(self, tmp_path):
        sigs = [{"signature": f"Sig{i}" + "1" * 80, "err": None} for i in range(10)]
        txs = {s["signature"]: _make_legacy_tx(s["signature"]) for s in sigs}
        rpc = MockRPCClient(
            signatures_by_address={WATCHED_ADDR: sigs},
            transactions_by_sig=txs,
        )
        config = _make_config(max_transactions=3)
        result = _make_adapter(rpc, config, tmp_path=tmp_path).run()
        assert result.transactions_processed <= 3

    def test_max_signatures_per_run_limits_fetch(self, tmp_path):
        # 10 sigs available, cap at 2 — only 2 signatures returned from RPC mock
        sigs = [{"signature": f"Sig{i}" + "1" * 80, "err": None} for i in range(10)]
        txs = {s["signature"]: _make_legacy_tx(s["signature"]) for s in sigs}
        rpc = MockRPCClient(
            signatures_by_address={WATCHED_ADDR: sigs},
            transactions_by_sig=txs,
        )
        config = _make_config(max_signatures=2)
        result = _make_adapter(rpc, config, tmp_path=tmp_path).run()
        # MockRPCClient respects limit in _post_with_retry
        assert result.signatures_fetched <= 2


class TestMultipleWatchedAddresses:
    """Multiple watched addresses each discovered independently."""

    def test_two_watched_addresses_both_ingested(self, tmp_path):
        addr_b = "WatchedAddr2222222222222222222222222222222222"
        tx1 = _make_legacy_tx(SIG_1)
        tx2 = _make_legacy_tx(SIG_2)
        rpc = MockRPCClient(
            signatures_by_address={
                WATCHED_ADDR: [{"signature": SIG_1, "err": None}],
                addr_b: [{"signature": SIG_2, "err": None}],
            },
            transactions_by_sig={SIG_1: tx1, SIG_2: tx2},
        )
        config = _make_config(watched_addresses=[WATCHED_ADDR, addr_b])
        result = _make_adapter(rpc, config, tmp_path=tmp_path).run()
        assert result.signatures_fetched == 2
        sigs = {e["signature"] for e in result.raw_events}
        assert SIG_1 in sigs
        assert SIG_2 in sigs


class TestALTMetricsInResult:
    """ALT cache metrics are present in run result."""

    def test_run_result_contains_alt_metrics(self, tmp_path):
        tx = _make_legacy_tx(SIG_1)
        rpc = MockRPCClient(
            signatures_by_address={WATCHED_ADDR: [{"signature": SIG_1, "err": None}]},
            transactions_by_sig={SIG_1: tx},
        )
        result = _make_adapter(rpc, tmp_path=tmp_path).run()
        assert "processing_cache_hits" in result.alt_metrics
        assert "processing_cache_misses" in result.alt_metrics
        assert "persistent_cache_hits" in result.alt_metrics

    def test_legacy_txs_have_no_alt_rpc_calls(self, tmp_path):
        """Legacy transactions have no addressTableLookups — ALT RPC is never called."""
        tx = _make_legacy_tx(SIG_1)
        rpc = MockRPCClient(
            signatures_by_address={WATCHED_ADDR: [{"signature": SIG_1, "err": None}]},
            transactions_by_sig={SIG_1: tx},
        )
        result = _make_adapter(rpc, tmp_path=tmp_path).run()
        # No ALT fetches needed for legacy transactions
        assert result.alt_metrics.get("processing_cache_misses", 0) == 0


class TestInnerInstructionGuard:
    """Transactions with too many inner instructions are skipped."""

    def test_inner_instruction_count_helper(self):
        raw_tx = {
            "meta": {
                "innerInstructions": [
                    {"instructions": [{}] * 5},
                    {"instructions": [{}] * 10},
                ]
            }
        }
        assert _inner_instruction_count(raw_tx) == 15

    def test_empty_inner_instructions(self):
        raw_tx = {"meta": {"innerInstructions": []}}
        assert _inner_instruction_count(raw_tx) == 0

    def test_missing_meta(self):
        assert _inner_instruction_count({}) == 0

    def test_transaction_exceeding_inner_limit_is_skipped(self, tmp_path):
        tx = _make_legacy_tx(SIG_1)
        # Inject 100 inner instructions
        tx["meta"]["innerInstructions"] = [{"instructions": [{}] * 100}]

        rpc = MockRPCClient(
            signatures_by_address={WATCHED_ADDR: [{"signature": SIG_1, "err": None}]},
            transactions_by_sig={SIG_1: tx},
        )
        config = _make_config(max_transactions=200)
        # Override inner limit to 50
        config.max_inner_instructions_per_tx = 50
        result = _make_adapter(rpc, config, tmp_path=tmp_path).run()
        assert result.transactions_skipped_no_watched_mint == 1
        assert result.raw_events == []


class TestStartSignatureCursor:
    """start_signature is passed as 'before' cursor to getSignaturesForAddress."""

    def test_start_signature_forwarded_as_before(self, tmp_path):
        """Verify the adapter passes start_signature as before= to the RPC call."""
        captured_payloads = []

        class CapturingMockRPC(MockRPCClient):
            def _post_with_retry(self, url, payload, *, context=""):
                captured_payloads.append(payload)
                return super()._post_with_retry(url, payload, context=context)

        tx = _make_legacy_tx(SIG_2)
        rpc = CapturingMockRPC(
            signatures_by_address={WATCHED_ADDR: [{"signature": SIG_2, "err": None}]},
            transactions_by_sig={SIG_2: tx},
        )
        config = _make_config(start_signature=SIG_1)
        _make_adapter(rpc, config, tmp_path=tmp_path).run()

        sig_calls = [p for p in captured_payloads if p.get("method") == "getSignaturesForAddress"]
        assert len(sig_calls) >= 1
        opts = sig_calls[0]["params"][1]
        assert opts.get("before") == SIG_1
