"""
Phase 2 — Persistent Checkpointing Tests.

Covers:
- get_or_seed raises MissingCheckpointError with no checkpoint and no config
- get_or_seed seeds from start_signature when no checkpoint exists
- get_or_seed seeds from start_slot when no checkpoint exists
- get_or_seed returns existing checkpoint without touching start config
- advance updates all fields correctly
- advance with promoted=True sets last_promoted_slot
- advance with validated=True sets last_validated_at
- mark_failed preserves signature cursor, sets status=failed
- checkpoint persists to disk and reloads correctly across store instances
- atomic write: flush writes to tmp then replaces (no partial reads)
- CheckpointCorruptError on malformed JSON
- ingestion_status validation rejects unknown values
- all_entries returns all stored checkpoints
- adapter integrates checkpoint store (cursor comes from checkpoint, not config)
- adapter raises run_status=failed when checkpoint missing and no start config
"""

from __future__ import annotations

import json
import os
from typing import Optional

import pytest

from services.solana.checkpoint import (
    INGESTION_STATUS_DEGRADED,
    INGESTION_STATUS_FAILED,
    INGESTION_STATUS_OK,
    CheckpointCorruptError,
    CheckpointEntry,
    CheckpointStore,
    MissingCheckpointError,
)
from services.solana.constants import USDC_MINT
from services.solana.ingestion_adapter import IngestionConfig, SolanaIngestionAdapter
from services.solana.alt_manager import PersistentALTCache

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CHAIN = "solana"
MINT = USDC_MINT
ADDR_1 = "WatchedAddr1111111111111111111111111111111111"
ADDR_2 = "WatchedAddr2222222222222222222222222222222222"
SIG_A = "SigAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
SIG_B = "SigBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
SLOT_A = 100_000
SLOT_B = 200_000


def _store(tmp_path, name="checkpoint.json") -> CheckpointStore:
    return CheckpointStore(checkpoint_path=str(tmp_path / name))


# ---------------------------------------------------------------------------
# MissingCheckpointError
# ---------------------------------------------------------------------------

class TestMissingCheckpoint:

    def test_raises_when_no_checkpoint_and_no_start(self, tmp_path):
        store = _store(tmp_path)
        with pytest.raises(MissingCheckpointError, match="Refusing to ingest from genesis"):
            store.get_or_seed(CHAIN, MINT, ADDR_1)

    def test_raises_includes_address_hint(self, tmp_path):
        store = _store(tmp_path)
        with pytest.raises(MissingCheckpointError, match=ADDR_1[:8]):
            store.get_or_seed(CHAIN, MINT, ADDR_1)

    def test_no_raise_when_start_signature_provided(self, tmp_path):
        store = _store(tmp_path)
        entry = store.get_or_seed(CHAIN, MINT, ADDR_1, start_signature=SIG_A)
        assert entry is not None

    def test_no_raise_when_start_slot_provided(self, tmp_path):
        store = _store(tmp_path)
        entry = store.get_or_seed(CHAIN, MINT, ADDR_1, start_slot=SLOT_A)
        assert entry is not None


# ---------------------------------------------------------------------------
# Seeding from config
# ---------------------------------------------------------------------------

class TestSeedFromConfig:

    def test_seeds_last_processed_signature_from_start_sig(self, tmp_path):
        store = _store(tmp_path)
        entry = store.get_or_seed(CHAIN, MINT, ADDR_1, start_signature=SIG_A)
        assert entry.last_processed_signature == SIG_A

    def test_seeds_last_processed_slot_from_start_slot(self, tmp_path):
        store = _store(tmp_path)
        entry = store.get_or_seed(CHAIN, MINT, ADDR_1, start_slot=SLOT_A)
        assert entry.last_processed_slot == SLOT_A

    def test_seeded_entry_has_ok_status(self, tmp_path):
        store = _store(tmp_path)
        entry = store.get_or_seed(CHAIN, MINT, ADDR_1, start_signature=SIG_A)
        assert entry.ingestion_status == INGESTION_STATUS_OK

    def test_seeded_entry_written_to_disk(self, tmp_path):
        store = _store(tmp_path)
        store.get_or_seed(CHAIN, MINT, ADDR_1, start_signature=SIG_A)
        # Reload from disk in a new store instance
        store2 = _store(tmp_path)
        entry = store2.get(CHAIN, MINT, ADDR_1)
        assert entry is not None
        assert entry.last_processed_signature == SIG_A

    def test_existing_checkpoint_returned_over_seed(self, tmp_path):
        """If checkpoint exists, get_or_seed returns it without using start config."""
        store = _store(tmp_path)
        store.get_or_seed(CHAIN, MINT, ADDR_1, start_signature=SIG_A)
        # Second call with different start_signature — should still return SIG_A
        entry = store.get_or_seed(CHAIN, MINT, ADDR_1, start_signature=SIG_B)
        assert entry.last_processed_signature == SIG_A


# ---------------------------------------------------------------------------
# Advancing checkpoint
# ---------------------------------------------------------------------------

class TestAdvance:

    def test_advance_updates_signature_and_slot(self, tmp_path):
        store = _store(tmp_path)
        store.get_or_seed(CHAIN, MINT, ADDR_1, start_signature=SIG_A, start_slot=SLOT_A)
        store.advance(CHAIN, MINT, ADDR_1,
                      last_processed_signature=SIG_B,
                      last_processed_slot=SLOT_B)
        entry = store.get(CHAIN, MINT, ADDR_1)
        assert entry.last_processed_signature == SIG_B
        assert entry.last_processed_slot == SLOT_B

    def test_advance_sets_last_successful_run_at(self, tmp_path):
        store = _store(tmp_path)
        store.get_or_seed(CHAIN, MINT, ADDR_1, start_signature=SIG_A)
        store.advance(CHAIN, MINT, ADDR_1,
                      last_processed_signature=SIG_B,
                      last_processed_slot=SLOT_B)
        entry = store.get(CHAIN, MINT, ADDR_1)
        assert entry.last_successful_run_at is not None

    def test_advance_with_promoted_sets_last_promoted_slot(self, tmp_path):
        store = _store(tmp_path)
        store.get_or_seed(CHAIN, MINT, ADDR_1, start_signature=SIG_A)
        store.advance(CHAIN, MINT, ADDR_1,
                      last_processed_signature=SIG_B,
                      last_processed_slot=SLOT_B,
                      promoted=True)
        entry = store.get(CHAIN, MINT, ADDR_1)
        assert entry.last_promoted_slot == SLOT_B

    def test_advance_without_promoted_leaves_promoted_slot_unchanged(self, tmp_path):
        store = _store(tmp_path)
        store.get_or_seed(CHAIN, MINT, ADDR_1, start_signature=SIG_A)
        store.advance(CHAIN, MINT, ADDR_1,
                      last_processed_signature=SIG_B,
                      last_processed_slot=SLOT_B,
                      promoted=False)
        entry = store.get(CHAIN, MINT, ADDR_1)
        assert entry.last_promoted_slot is None

    def test_advance_with_validated_sets_last_validated_at(self, tmp_path):
        store = _store(tmp_path)
        store.get_or_seed(CHAIN, MINT, ADDR_1, start_signature=SIG_A)
        store.advance(CHAIN, MINT, ADDR_1,
                      last_processed_signature=SIG_B,
                      last_processed_slot=SLOT_B,
                      validated=True)
        entry = store.get(CHAIN, MINT, ADDR_1)
        assert entry.last_validated_at is not None

    def test_advance_sets_degraded_status(self, tmp_path):
        store = _store(tmp_path)
        store.get_or_seed(CHAIN, MINT, ADDR_1, start_signature=SIG_A)
        store.advance(CHAIN, MINT, ADDR_1,
                      last_processed_signature=SIG_B,
                      last_processed_slot=SLOT_B,
                      ingestion_status=INGESTION_STATUS_DEGRADED)
        entry = store.get(CHAIN, MINT, ADDR_1)
        assert entry.ingestion_status == INGESTION_STATUS_DEGRADED

    def test_advance_rejects_invalid_status(self, tmp_path):
        store = _store(tmp_path)
        store.get_or_seed(CHAIN, MINT, ADDR_1, start_signature=SIG_A)
        with pytest.raises(ValueError, match="Invalid ingestion_status"):
            store.advance(CHAIN, MINT, ADDR_1,
                          last_processed_signature=SIG_B,
                          last_processed_slot=SLOT_B,
                          ingestion_status="invalid_value")


# ---------------------------------------------------------------------------
# mark_failed
# ---------------------------------------------------------------------------

class TestMarkFailed:

    def test_mark_failed_sets_status(self, tmp_path):
        store = _store(tmp_path)
        store.get_or_seed(CHAIN, MINT, ADDR_1, start_signature=SIG_A)
        store.mark_failed(CHAIN, MINT, ADDR_1, reason="RPC unavailable")
        entry = store.get(CHAIN, MINT, ADDR_1)
        assert entry.ingestion_status == INGESTION_STATUS_FAILED

    def test_mark_failed_preserves_signature_cursor(self, tmp_path):
        """mark_failed must NOT advance the cursor — next run retries from same position."""
        store = _store(tmp_path)
        store.get_or_seed(CHAIN, MINT, ADDR_1, start_signature=SIG_A)
        store.mark_failed(CHAIN, MINT, ADDR_1)
        entry = store.get(CHAIN, MINT, ADDR_1)
        assert entry.last_processed_signature == SIG_A

    def test_mark_failed_noop_when_no_checkpoint(self, tmp_path):
        """mark_failed on a missing entry does nothing (no crash)."""
        store = _store(tmp_path)
        store.mark_failed(CHAIN, MINT, ADDR_1)  # should not raise


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

class TestPersistence:

    def test_checkpoint_reloads_across_store_instances(self, tmp_path):
        store1 = _store(tmp_path)
        store1.get_or_seed(CHAIN, MINT, ADDR_1, start_signature=SIG_A)
        store1.advance(CHAIN, MINT, ADDR_1,
                       last_processed_signature=SIG_B,
                       last_processed_slot=SLOT_B,
                       promoted=True)

        store2 = _store(tmp_path)
        entry = store2.get(CHAIN, MINT, ADDR_1)
        assert entry is not None
        assert entry.last_processed_signature == SIG_B
        assert entry.last_promoted_slot == SLOT_B

    def test_two_addresses_stored_independently(self, tmp_path):
        store = _store(tmp_path)
        store.get_or_seed(CHAIN, MINT, ADDR_1, start_signature=SIG_A)
        store.get_or_seed(CHAIN, MINT, ADDR_2, start_signature=SIG_B)

        e1 = store.get(CHAIN, MINT, ADDR_1)
        e2 = store.get(CHAIN, MINT, ADDR_2)
        assert e1.last_processed_signature == SIG_A
        assert e2.last_processed_signature == SIG_B

    def test_all_entries_returns_both(self, tmp_path):
        store = _store(tmp_path)
        store.get_or_seed(CHAIN, MINT, ADDR_1, start_signature=SIG_A)
        store.get_or_seed(CHAIN, MINT, ADDR_2, start_signature=SIG_B)
        entries = store.all_entries()
        assert len(entries) == 2

    def test_corrupt_json_raises_checkpoint_corrupt_error(self, tmp_path):
        path = tmp_path / "checkpoint.json"
        path.write_text("{not valid json", encoding="utf-8")
        with pytest.raises(CheckpointCorruptError):
            CheckpointStore(checkpoint_path=str(path))

    def test_atomic_write_uses_tmp_file(self, tmp_path):
        """Flush writes to .tmp then replaces — verify final file is valid JSON."""
        store = _store(tmp_path)
        store.get_or_seed(CHAIN, MINT, ADDR_1, start_signature=SIG_A)
        path = tmp_path / "checkpoint.json"
        raw = json.loads(path.read_text())
        assert isinstance(raw, dict)


# ---------------------------------------------------------------------------
# resume_signature property
# ---------------------------------------------------------------------------

class TestResumeSignature:

    def test_resume_signature_returns_last_processed(self, tmp_path):
        store = _store(tmp_path)
        entry = store.get_or_seed(CHAIN, MINT, ADDR_1, start_signature=SIG_A)
        assert entry.resume_signature == SIG_A

    def test_resume_signature_none_when_not_set(self, tmp_path):
        store = _store(tmp_path)
        entry = store.get_or_seed(CHAIN, MINT, ADDR_1, start_slot=SLOT_A)
        # start_slot set but no start_signature
        assert entry.resume_signature is None


# ---------------------------------------------------------------------------
# Adapter integration
# ---------------------------------------------------------------------------

class MockRPCForCheckpoint:
    """Minimal mock — returns empty signatures so the run completes cleanly."""

    def __init__(self):
        self.primary_url = "http://mock"
        self.fallback_url = None
        self._provider_mode = "primary"

    @property
    def provider_mode(self):
        return self._provider_mode

    def get_account_info(self, address, *, encoding, commitment, use_fallback=False):
        return None

    def get_transaction(self, signature, **kwargs):
        return None

    def _post_with_retry(self, url, payload, *, context=""):
        if payload.get("method") == "getSignaturesForAddress":
            return {"result": []}
        return {"result": []}


class TestAdapterCheckpointIntegration:

    def test_adapter_uses_checkpoint_cursor(self, tmp_path):
        """Checkpoint cursor is used as the before= param for signature discovery."""
        captured = []

        class CapturingMock(MockRPCForCheckpoint):
            def _post_with_retry(self, url, payload, *, context=""):
                captured.append(payload)
                return super()._post_with_retry(url, payload, context=context)

        store = _store(tmp_path)
        store.get_or_seed(CHAIN, MINT, ADDR_1, start_signature=SIG_A)

        config = IngestionConfig(
            primary_url="http://mock",
            watched_addresses=[ADDR_1],
            token_mint_allowlist={MINT},
            start_signature=SIG_B,  # config cursor — should be overridden by checkpoint
        )
        alt_cache = PersistentALTCache(cache_path=str(tmp_path / "alt.json"))
        adapter = SolanaIngestionAdapter(
            config,
            rpc_client=CapturingMock(),
            persistent_cache=alt_cache,
            checkpoint_store=store,
        )
        adapter.run()

        sig_calls = [p for p in captured if p.get("method") == "getSignaturesForAddress"]
        assert len(sig_calls) >= 1
        opts = sig_calls[0]["params"][1]
        # Checkpoint cursor (SIG_A) should win over config cursor (SIG_B)
        assert opts.get("before") == SIG_A

    def test_adapter_run_fails_when_no_checkpoint_and_no_start(self, tmp_path):
        """
        With a checkpoint store wired in but no checkpoint and no start config,
        run() must return run_status='failed' — not crash.
        """
        store = _store(tmp_path)
        config = IngestionConfig(
            primary_url="http://mock",
            watched_addresses=[ADDR_1],
            token_mint_allowlist={MINT},
            start_signature=None,
            start_slot=None,
        )
        alt_cache = PersistentALTCache(cache_path=str(tmp_path / "alt.json"))
        adapter = SolanaIngestionAdapter(
            config,
            rpc_client=MockRPCForCheckpoint(),
            persistent_cache=alt_cache,
            checkpoint_store=store,
        )
        result = adapter.run()
        assert result.run_status == "failed"
        assert any("Refusing to ingest from genesis" in e for e in result.errors)

    def test_adapter_without_checkpoint_store_uses_config_cursor(self, tmp_path):
        """
        When no checkpoint store is injected, adapter uses config.start_signature
        directly — backward compatible with Phase 1 behavior.
        """
        captured = []

        class CapturingMock(MockRPCForCheckpoint):
            def _post_with_retry(self, url, payload, *, context=""):
                captured.append(payload)
                return super()._post_with_retry(url, payload, context=context)

        config = IngestionConfig(
            primary_url="http://mock",
            watched_addresses=[ADDR_1],
            token_mint_allowlist={MINT},
            start_signature=SIG_A,
        )
        alt_cache = PersistentALTCache(cache_path=str(tmp_path / "alt.json"))
        adapter = SolanaIngestionAdapter(
            config,
            rpc_client=CapturingMock(),
            persistent_cache=alt_cache,
            # No checkpoint_store — Phase 1 mode
        )
        adapter.run()

        sig_calls = [p for p in captured if p.get("method") == "getSignaturesForAddress"]
        opts = sig_calls[0]["params"][1]
        assert opts.get("before") == SIG_A
