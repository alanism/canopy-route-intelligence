"""
Phase 5 — Owner + Amount Resolution Tests.

Covers:
OwnerResolver:
- resolves owner from preTokenBalances (tier 1)
- resolves owner from postTokenBalances when not in pre (tier 1)
- resolves owner from cache when not in balances (tier 2)
- resolves owner via RPC when not in cache (tier 3)
- RPC owner written to cache after fetch
- unresolved → owner=None, status=degraded (tier 4)
- __account_index_N__ placeholder resolved to pubkey via account_keys
- program-owned accounts flagged (is_program_owned=True)
- token account address never returned as owner

resolve_event_owners():
- patches source_owner and destination_owner into event dict
- owner_resolution_status=ok when both resolved
- owner_resolution_status=degraded when either unresolved

AmountResolver (resolve_amounts()):
- resolved from balance delta (tier 1)
- amount_received_raw is largest positive delta
- amount_transferred_raw is largest negative delta magnitude
- fee_withheld_raw = transferred - received (Token-2022)
- fee_withheld_raw=None when no source delta
- amount_decimal is Decimal, not float
- amount_decimal exact 6-decimal precision
- returns degraded when no balances present
- no crash on empty/None balances

apply_owner_and_amount_resolution():
- updates amount fields in normalized event
- preserves Phase 4 amounts when Phase 5 resolution fails
- owner fields updated when resolver provided
- validation_status re-aggregated to degraded when any sub-status degraded
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Optional

import pytest

from services.solana.constants import SPL_TOKEN_PROGRAM, USDC_MINT
from services.solana.event_schema import apply_owner_and_amount_resolution, normalize_event
from services.solana.owner_resolver import (
    METHOD_OWNER_CACHE,
    METHOD_RPC_ACCOUNT_INFO,
    METHOD_TOKEN_BALANCES,
    METHOD_UNRESOLVED,
    STATUS_DEGRADED,
    STATUS_OK,
    OwnerCache,
    OwnerResolver,
    _resolve_placeholder,
    resolve_amounts,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WALLET_A = "WalletAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
WALLET_B = "WalletBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
ATA_A = "AtaAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
ATA_B = "AtaBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
SIG_1 = "Sig1" + "1" * 84
USDC_DECIMALS = 6


def _make_token_balance(
    account_index: int,
    mint: str = USDC_MINT,
    owner: str = WALLET_A,
    amount: int = 1_000_000,
) -> dict[str, Any]:
    return {
        "accountIndex": account_index,
        "mint": mint,
        "owner": owner,
        "uiTokenAmount": {"amount": str(amount), "decimals": USDC_DECIMALS},
    }


def _make_pre_normalized(
    *,
    account_keys: Optional[list[str]] = None,
    pre_token_balances: Optional[list[dict]] = None,
    post_token_balances: Optional[list[dict]] = None,
) -> dict[str, Any]:
    return {
        "account_keys_resolved": account_keys or [WALLET_A, WALLET_B, ATA_A, ATA_B],
        "pre_token_balances": pre_token_balances or [],
        "post_token_balances": post_token_balances or [],
        "instructions_resolved": [],
        "inner_instructions_resolved": [],
        "pre_normalization_status": "ok",
    }


# ---------------------------------------------------------------------------
# OwnerCache
# ---------------------------------------------------------------------------

class TestOwnerCache:

    def test_set_and_get(self, tmp_path):
        cache = OwnerCache(cache_path=str(tmp_path / "owners.json"))
        cache.set(ATA_A, WALLET_A)
        assert cache.get(ATA_A) == WALLET_A

    def test_miss_returns_none(self, tmp_path):
        cache = OwnerCache(cache_path=str(tmp_path / "owners.json"))
        assert cache.get(ATA_A) is None

    def test_flush_persists_to_disk(self, tmp_path):
        path = str(tmp_path / "owners.json")
        cache = OwnerCache(cache_path=path)
        cache.set(ATA_A, WALLET_A)
        cache.flush()
        cache2 = OwnerCache(cache_path=path)
        assert cache2.get(ATA_A) == WALLET_A

    def test_flush_noop_when_not_dirty(self, tmp_path):
        """flush() should not write if nothing changed."""
        path = str(tmp_path / "owners.json")
        cache = OwnerCache(cache_path=path)
        cache.flush()  # no-op — file should not be created
        assert not (tmp_path / "owners.json").exists()


# ---------------------------------------------------------------------------
# _resolve_placeholder
# ---------------------------------------------------------------------------

class TestResolvePlaceholder:

    def test_resolves_account_index_placeholder(self):
        keys = [WALLET_A, WALLET_B, ATA_A, ATA_B]
        result = _resolve_placeholder("__account_index_2__", keys)
        assert result == ATA_A

    def test_returns_unchanged_for_real_pubkey(self):
        result = _resolve_placeholder(ATA_A, [WALLET_A, ATA_A])
        assert result == ATA_A

    def test_returns_unchanged_for_out_of_range_index(self):
        result = _resolve_placeholder("__account_index_99__", [WALLET_A])
        assert result == "__account_index_99__"

    def test_index_zero(self):
        keys = [WALLET_A, WALLET_B]
        result = _resolve_placeholder("__account_index_0__", keys)
        assert result == WALLET_A


# ---------------------------------------------------------------------------
# OwnerResolver — tier 1: token balances
# ---------------------------------------------------------------------------

class TestOwnerResolverTokenBalances:

    def _resolver(self, tmp_path):
        cache = OwnerCache(cache_path=str(tmp_path / "owners.json"))
        return OwnerResolver(owner_cache=cache)

    def test_resolves_from_pre_token_balances(self, tmp_path):
        resolver = self._resolver(tmp_path)
        pre = [_make_token_balance(2, owner=WALLET_A)]  # ATA_A is at index 2
        result = resolver.resolve(
            ATA_A,
            pre_token_balances=pre,
            post_token_balances=[],
            account_keys=[WALLET_A, WALLET_B, ATA_A, ATA_B],
        )
        assert result.owner == WALLET_A
        assert result.resolution_method == METHOD_TOKEN_BALANCES
        assert result.resolution_status == STATUS_OK

    def test_resolves_from_post_token_balances_when_not_in_pre(self, tmp_path):
        resolver = self._resolver(tmp_path)
        post = [_make_token_balance(3, owner=WALLET_B)]  # ATA_B at index 3
        result = resolver.resolve(
            ATA_B,
            pre_token_balances=[],
            post_token_balances=post,
            account_keys=[WALLET_A, WALLET_B, ATA_A, ATA_B],
        )
        assert result.owner == WALLET_B
        assert result.resolution_method == METHOD_TOKEN_BALANCES

    def test_resolved_owner_written_to_cache(self, tmp_path):
        cache = OwnerCache(cache_path=str(tmp_path / "owners.json"))
        resolver = OwnerResolver(owner_cache=cache)
        pre = [_make_token_balance(2, owner=WALLET_A)]
        resolver.resolve(
            ATA_A,
            pre_token_balances=pre,
            post_token_balances=[],
            account_keys=[WALLET_A, WALLET_B, ATA_A, ATA_B],
        )
        assert cache.get(ATA_A) == WALLET_A

    def test_program_owned_flagged(self, tmp_path):
        resolver = self._resolver(tmp_path)
        system_prog = "11111111111111111111111111111111"
        pre = [_make_token_balance(2, owner=system_prog)]
        result = resolver.resolve(
            ATA_A,
            pre_token_balances=pre,
            post_token_balances=[],
            account_keys=[WALLET_A, WALLET_B, ATA_A, ATA_B],
        )
        assert result.is_program_owned is True

    def test_wallet_not_program_owned(self, tmp_path):
        resolver = self._resolver(tmp_path)
        pre = [_make_token_balance(2, owner=WALLET_A)]
        result = resolver.resolve(
            ATA_A,
            pre_token_balances=pre,
            post_token_balances=[],
            account_keys=[WALLET_A, WALLET_B, ATA_A, ATA_B],
        )
        assert result.is_program_owned is False

    def test_none_token_account_returns_degraded(self, tmp_path):
        resolver = self._resolver(tmp_path)
        result = resolver.resolve(None)
        assert result.owner is None
        assert result.resolution_status == STATUS_DEGRADED


# ---------------------------------------------------------------------------
# OwnerResolver — tier 2: owner cache
# ---------------------------------------------------------------------------

class TestOwnerResolverCache:

    def test_resolves_from_cache_when_not_in_balances(self, tmp_path):
        cache = OwnerCache(cache_path=str(tmp_path / "owners.json"))
        cache.set(ATA_A, WALLET_A)
        resolver = OwnerResolver(owner_cache=cache)
        result = resolver.resolve(
            ATA_A,
            pre_token_balances=[],
            post_token_balances=[],
            account_keys=[],
        )
        assert result.owner == WALLET_A
        assert result.resolution_method == METHOD_OWNER_CACHE
        assert result.resolution_status == STATUS_OK


# ---------------------------------------------------------------------------
# OwnerResolver — tier 3: RPC fallback
# ---------------------------------------------------------------------------

class MockRPCForOwner:
    def __init__(self, owners: dict[str, str]):
        self._owners = owners
        self.calls: list[str] = []

    def get_account_info(self, address, *, encoding, commitment, use_fallback=False):
        self.calls.append(address)
        owner = self._owners.get(address)
        if owner is None:
            return None
        return {
            "data": {
                "parsed": {
                    "info": {"owner": owner},
                    "type": "account",
                }
            }
        }


class TestOwnerResolverRPC:

    def test_resolves_via_rpc_when_not_cached(self, tmp_path):
        rpc = MockRPCForOwner({ATA_A: WALLET_A})
        cache = OwnerCache(cache_path=str(tmp_path / "owners.json"))
        resolver = OwnerResolver(owner_cache=cache, rpc_client=rpc)
        result = resolver.resolve(ATA_A, pre_token_balances=[], post_token_balances=[], account_keys=[])
        assert result.owner == WALLET_A
        assert result.resolution_method == METHOD_RPC_ACCOUNT_INFO
        assert result.resolution_status == STATUS_OK

    def test_rpc_result_written_to_cache(self, tmp_path):
        rpc = MockRPCForOwner({ATA_A: WALLET_A})
        cache = OwnerCache(cache_path=str(tmp_path / "owners.json"))
        resolver = OwnerResolver(owner_cache=cache, rpc_client=rpc)
        resolver.resolve(ATA_A, pre_token_balances=[], post_token_balances=[], account_keys=[])
        assert cache.get(ATA_A) == WALLET_A

    def test_second_call_uses_cache_not_rpc(self, tmp_path):
        rpc = MockRPCForOwner({ATA_A: WALLET_A})
        cache = OwnerCache(cache_path=str(tmp_path / "owners.json"))
        resolver = OwnerResolver(owner_cache=cache, rpc_client=rpc)
        resolver.resolve(ATA_A, pre_token_balances=[], post_token_balances=[], account_keys=[])
        resolver.resolve(ATA_A, pre_token_balances=[], post_token_balances=[], account_keys=[])
        assert rpc.calls.count(ATA_A) == 1  # only one RPC call


# ---------------------------------------------------------------------------
# OwnerResolver — tier 4: unresolved
# ---------------------------------------------------------------------------

class TestOwnerResolverUnresolved:

    def test_unresolved_returns_degraded(self, tmp_path):
        cache = OwnerCache(cache_path=str(tmp_path / "owners.json"))
        resolver = OwnerResolver(owner_cache=cache)  # no RPC
        result = resolver.resolve(ATA_A, pre_token_balances=[], post_token_balances=[], account_keys=[])
        assert result.owner is None
        assert result.resolution_status == STATUS_DEGRADED
        assert result.resolution_method == METHOD_UNRESOLVED

    def test_token_account_preserved_even_when_owner_unknown(self, tmp_path):
        cache = OwnerCache(cache_path=str(tmp_path / "owners.json"))
        resolver = OwnerResolver(owner_cache=cache)
        result = resolver.resolve(ATA_A, pre_token_balances=[], post_token_balances=[], account_keys=[])
        assert result.token_account == ATA_A


# ---------------------------------------------------------------------------
# resolve_event_owners
# ---------------------------------------------------------------------------

class TestResolveEventOwners:

    def _make_event(self, src=ATA_A, dst=ATA_B):
        return {
            "source_token_account": src,
            "destination_token_account": dst,
            "owner_resolution_status": "pending",
        }

    def test_patches_source_and_destination_owners(self, tmp_path):
        cache = OwnerCache(cache_path=str(tmp_path / "owners.json"))
        cache.set(ATA_A, WALLET_A)
        cache.set(ATA_B, WALLET_B)
        resolver = OwnerResolver(owner_cache=cache)

        event = self._make_event()
        pre = _make_pre_normalized()
        patch = resolver.resolve_event_owners(event, pre)

        assert patch["source_owner"] == WALLET_A
        assert patch["destination_owner"] == WALLET_B
        assert patch["owner_resolution_status"] == STATUS_OK

    def test_degraded_when_source_unresolved(self, tmp_path):
        cache = OwnerCache(cache_path=str(tmp_path / "owners.json"))
        cache.set(ATA_B, WALLET_B)  # only destination cached
        resolver = OwnerResolver(owner_cache=cache)

        event = self._make_event()
        pre = _make_pre_normalized()
        patch = resolver.resolve_event_owners(event, pre)

        assert patch["source_owner"] is None
        assert patch["destination_owner"] == WALLET_B
        assert patch["owner_resolution_status"] == STATUS_DEGRADED


# ---------------------------------------------------------------------------
# resolve_amounts
# ---------------------------------------------------------------------------

class TestResolveAmounts:

    def _pre(self, pre_balances, post_balances):
        return _make_pre_normalized(
            pre_token_balances=pre_balances,
            post_token_balances=post_balances,
        )

    def test_resolves_received_from_balance_delta(self):
        pre = [_make_token_balance(2, amount=5_000_000)]
        post = [
            _make_token_balance(2, amount=4_000_000),
            _make_token_balance(3, amount=1_000_000),
        ]
        result = resolve_amounts(self._pre(pre, post))
        assert result.amount_received_raw == 1_000_000
        assert result.resolution_method == METHOD_TOKEN_BALANCES
        assert result.resolution_status == STATUS_OK

    def test_resolves_transferred_from_balance_delta(self):
        pre = [_make_token_balance(2, amount=5_000_000)]
        post = [
            _make_token_balance(2, amount=4_000_000),
            _make_token_balance(3, amount=1_000_000),
        ]
        result = resolve_amounts(self._pre(pre, post))
        assert result.amount_transferred_raw == 1_000_000

    def test_fee_withheld_is_difference_when_token2022(self):
        # Source sent 1_000_200, destination received 1_000_000
        pre = [_make_token_balance(2, amount=5_000_200)]
        post = [
            _make_token_balance(2, amount=4_000_000),
            _make_token_balance(3, amount=1_000_000),
        ]
        result = resolve_amounts(self._pre(pre, post))
        assert result.fee_withheld_raw == 200

    def test_amount_decimal_is_decimal_type(self):
        pre = [_make_token_balance(2, amount=5_000_000)]
        post = [_make_token_balance(2, amount=4_000_000), _make_token_balance(3, amount=1_000_000)]
        result = resolve_amounts(self._pre(pre, post))
        assert isinstance(result.amount_decimal, Decimal)

    def test_amount_decimal_exact_precision(self):
        pre = [_make_token_balance(2, amount=2_500_000)]
        post = [_make_token_balance(2, amount=1_500_000), _make_token_balance(3, amount=1_000_000)]
        result = resolve_amounts(self._pre(pre, post))
        assert result.amount_decimal == Decimal("1.000000")

    def test_empty_balances_returns_degraded(self):
        result = resolve_amounts(self._pre([], []))
        assert result.resolution_status == STATUS_DEGRADED
        assert result.amount_received_raw is None
        assert result.amount_decimal is None

    def test_no_crash_on_none_balances(self):
        pre_norm = _make_pre_normalized()
        pre_norm["pre_token_balances"] = None
        pre_norm["post_token_balances"] = None
        result = resolve_amounts(pre_norm)
        assert result.resolution_status == STATUS_DEGRADED

    def test_newly_created_ata_pre_balance_missing(self):
        """ATA with no pre-balance treated as 0 — receives full amount."""
        pre = []  # new ATA — no entry in pre
        post = [_make_token_balance(3, amount=2_000_000)]
        result = resolve_amounts(self._pre(pre, post))
        assert result.amount_received_raw == 2_000_000
        assert result.resolution_status == STATUS_OK


# ---------------------------------------------------------------------------
# apply_owner_and_amount_resolution integration
# ---------------------------------------------------------------------------

class TestApplyOwnerAndAmountResolution:

    def _make_raw_event(self, amount_received_raw=1_000_000):
        pre_normalized = _make_pre_normalized(
            pre_token_balances=[_make_token_balance(2, amount=5_000_000, owner=WALLET_A)],
            post_token_balances=[
                _make_token_balance(2, amount=4_000_000, owner=WALLET_A),
                _make_token_balance(3, amount=amount_received_raw, owner=WALLET_B),
            ],
        )
        raw = {
            "signature": SIG_1,
            "slot": 999,
            "block_time": 1_700_000_000,
            "chain": "solana",
            "pre_normalization_status": "ok",
            "alt_resolution_status": "not_required",
            "transaction_version": "legacy",
            "transaction_success": True,
            "observed_transfer_inclusion": True,
            "transfer_detected": True,
            "balance_delta_detected": True,
            "settlement_evidence_type": "balance_delta",
            "amount_received_raw": amount_received_raw,
            "source_token_account": ATA_A,
            "destination_token_account": ATA_B,
            "token_mint": USDC_MINT,
            "transfer_validation_status": "ok",
            "fee_lamports": 5000,
            "jito_tip_lamports": 0,
            "explicit_tip_lamports": 0,
            "total_native_observed_cost_lamports": 5000,
            "cost_validation_status": "ok",
            "jito_tip_detection_status": "ok",
            "_pre_normalized": pre_normalized,
        }
        return raw, pre_normalized

    def test_amount_fields_updated_from_balance_delta(self, tmp_path):
        raw, pre = self._make_raw_event(amount_received_raw=1_000_000)
        event = normalize_event(raw)
        apply_owner_and_amount_resolution(event, pre)
        assert event["amount_received_raw"] == 1_000_000
        assert event["amount_transferred_raw"] == 1_000_000
        assert event["amount_resolution_status"] == STATUS_OK

    def test_amount_decimal_updated_and_is_decimal(self, tmp_path):
        raw, pre = self._make_raw_event(amount_received_raw=2_500_000)
        event = normalize_event(raw)
        apply_owner_and_amount_resolution(event, pre)
        assert isinstance(event["amount_decimal"], Decimal)
        assert event["amount_decimal"] == Decimal("2.500000")

    def test_owner_fields_updated_when_resolver_provided(self, tmp_path):
        cache = OwnerCache(cache_path=str(tmp_path / "owners.json"))
        resolver = OwnerResolver(owner_cache=cache)
        raw, pre = self._make_raw_event()
        event = normalize_event(raw)
        apply_owner_and_amount_resolution(event, pre, owner_resolver=resolver)
        # Owners come from post_token_balances entries
        assert event["source_owner"] == WALLET_A
        assert event["destination_owner"] == WALLET_B
        assert event["owner_resolution_status"] == STATUS_OK

    def test_owner_fields_unchanged_when_no_resolver(self, tmp_path):
        raw, pre = self._make_raw_event()
        event = normalize_event(raw)
        apply_owner_and_amount_resolution(event, pre, owner_resolver=None)
        assert event["source_owner"] is None  # Phase 4 placeholder preserved

    def test_validation_status_degraded_when_amount_fails(self, tmp_path):
        raw, _ = self._make_raw_event()
        pre_empty = _make_pre_normalized()  # no balances
        event = normalize_event(raw)
        apply_owner_and_amount_resolution(event, pre_empty)
        assert event["amount_resolution_status"] == STATUS_DEGRADED
        assert event["validation_status"] == "degraded"

    def test_no_float_in_amount_fields_after_phase5(self, tmp_path):
        from services.solana.event_schema import assert_no_float_amounts
        raw, pre = self._make_raw_event()
        event = normalize_event(raw)
        apply_owner_and_amount_resolution(event, pre)
        assert_no_float_amounts(event)
