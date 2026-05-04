"""
Phase 5 — Owner + Amount Resolution.

Two responsibilities:

1. OwnerResolver — resolves the wallet owner of a source/destination token account.
2. AmountResolver — resolves transfer amounts from the pre-normalized event.

Owner Resolution Hierarchy (per build plan)
-------------------------------------------
1. preTokenBalances / postTokenBalances  (cheapest — already fetched)
2. Persistent token-account owner cache  (across runs, no RPC)
3. Fallback getAccountInfo RPC call      (last resort, single call)
4. null owner + degraded status          (never crash)

Owner Rules
-----------
- Never treat the token account address as the wallet owner.
- If owner is unresolved, set owner_resolution_status = "degraded".
- Preserve token_account even when owner is unknown.
- Program-owned accounts (SPL programs, system) are tagged separately.

Amount Resolution Hierarchy (per build plan)
--------------------------------------------
1. Token balance delta  (pre/post balances — strongest)
2. Parsed SPL transfer amount from instruction data
3. Token-2022 fee data (fee_withheld = sent - received)
4. null amount + degraded status

The balance-delta path is already implemented in transfer_truth.py and
token_program.py. AmountResolver composes those results and fills the
remaining fields (amount_transferred_raw, fee_withheld_raw) not yet
populated by Phase 1–4.

Decimal rule: all token math uses decimal.Decimal. float is prohibited.
"""

from __future__ import annotations

import logging
import os
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

logger = logging.getLogger("canopy.solana.owner_resolver")

# Resolution method tags
METHOD_TOKEN_BALANCES = "token_balances"
METHOD_OWNER_CACHE = "owner_cache"
METHOD_RPC_ACCOUNT_INFO = "rpc_account_info"
METHOD_UNRESOLVED = "unresolved"

# Resolution status tags
STATUS_OK = "ok"
STATUS_DEGRADED = "degraded"
STATUS_PENDING = "pending"

# Known program-owned account prefixes / addresses (non-exhaustive)
# These are System Program and token programs — owning them doesn't mean
# the address is a wallet.
_PROGRAM_OWNED_MARKERS = frozenset({
    "11111111111111111111111111111111",            # System Program
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",  # SPL Token
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",  # Token-2022
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJe8bv",  # Associated Token Program
})

OWNER_CACHE_PATH = os.path.join("data", "solana_owner_cache.json")


# ---------------------------------------------------------------------------
# Persistent owner cache (file-backed, loaded lazily)
# ---------------------------------------------------------------------------

class OwnerCache:
    """
    Persistent cache: token_account_pubkey → owner_pubkey.

    Written after each successful RPC resolution. Loaded on init.
    Atomic flush (tmp + os.replace) same pattern as ALTCache.
    """

    def __init__(self, cache_path: str = OWNER_CACHE_PATH) -> None:
        self._path = cache_path
        self._store: dict[str, str] = {}
        self._dirty = False
        self._load()

    def get(self, token_account: str) -> Optional[str]:
        return self._store.get(token_account)

    def set(self, token_account: str, owner: str) -> None:
        if self._store.get(token_account) != owner:
            self._store[token_account] = owner
            self._dirty = True

    def flush(self) -> None:
        if not self._dirty:
            return
        try:
            import json
            os.makedirs(os.path.dirname(os.path.abspath(self._path)), exist_ok=True)
            tmp = self._path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(self._store, fh, indent=2)
            os.replace(tmp, self._path)
            self._dirty = False
        except OSError as exc:
            logger.error("OwnerCache flush failed: %s", exc)

    def _load(self) -> None:
        if not os.path.exists(self._path):
            return
        try:
            import json
            with open(self._path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
            if isinstance(raw, dict):
                self._store = {k: v for k, v in raw.items()
                               if isinstance(k, str) and isinstance(v, str)}
        except Exception as exc:
            logger.warning("OwnerCache load failed: %s — starting empty", exc)


# ---------------------------------------------------------------------------
# OwnerResolutionResult
# ---------------------------------------------------------------------------

class OwnerResolutionResult:
    __slots__ = (
        "token_account", "owner", "is_program_owned",
        "resolution_method", "resolution_status",
    )

    def __init__(
        self,
        token_account: Optional[str],
        owner: Optional[str],
        *,
        is_program_owned: bool = False,
        resolution_method: str = METHOD_UNRESOLVED,
        resolution_status: str = STATUS_DEGRADED,
    ) -> None:
        self.token_account = token_account
        self.owner = owner
        self.is_program_owned = is_program_owned
        self.resolution_method = resolution_method
        self.resolution_status = resolution_status

    def to_dict(self) -> dict[str, Any]:
        return {
            "token_account": self.token_account,
            "owner": self.owner,
            "is_program_owned": self.is_program_owned,
            "resolution_method": self.resolution_method,
            "resolution_status": self.resolution_status,
        }


# ---------------------------------------------------------------------------
# OwnerResolver
# ---------------------------------------------------------------------------

class OwnerResolver:
    """
    Resolves wallet owners for source and destination token accounts.

    Inject an optional RPC client for fallback getAccountInfo lookups.
    Without RPC, resolution stops at the owner cache.

    Usage
    -----
    resolver = OwnerResolver(owner_cache=cache, rpc_client=rpc)

    source_result = resolver.resolve(
        token_account=source_token_account,
        pre_token_balances=pre_normalized["pre_token_balances"],
        post_token_balances=pre_normalized["post_token_balances"],
        account_keys=pre_normalized["account_keys_resolved"],
    )
    """

    def __init__(
        self,
        owner_cache: Optional[OwnerCache] = None,
        rpc_client=None,
    ) -> None:
        self._cache = owner_cache or OwnerCache()
        self._rpc = rpc_client

    def resolve(
        self,
        token_account: Optional[str],
        *,
        pre_token_balances: list[dict] = None,
        post_token_balances: list[dict] = None,
        account_keys: list[str] = None,
    ) -> OwnerResolutionResult:
        """
        Resolve the owner of a token account through the four-tier hierarchy.

        Returns OwnerResolutionResult — never raises.
        """
        if not token_account:
            return OwnerResolutionResult(
                token_account=None,
                owner=None,
                resolution_method=METHOD_UNRESOLVED,
                resolution_status=STATUS_DEGRADED,
            )

        # Resolve __account_index_N__ placeholders
        resolved_account = _resolve_placeholder(token_account, account_keys or [])

        # 1. Token balances (pre then post)
        owner = _owner_from_balances(
            resolved_account,
            pre_token_balances or [],
            post_token_balances or [],
            account_keys or [],
        )
        if owner:
            is_prog = owner in _PROGRAM_OWNED_MARKERS
            self._cache.set(resolved_account, owner)
            return OwnerResolutionResult(
                token_account=resolved_account,
                owner=owner,
                is_program_owned=is_prog,
                resolution_method=METHOD_TOKEN_BALANCES,
                resolution_status=STATUS_OK,
            )

        # 2. Owner cache
        cached_owner = self._cache.get(resolved_account)
        if cached_owner:
            is_prog = cached_owner in _PROGRAM_OWNED_MARKERS
            return OwnerResolutionResult(
                token_account=resolved_account,
                owner=cached_owner,
                is_program_owned=is_prog,
                resolution_method=METHOD_OWNER_CACHE,
                resolution_status=STATUS_OK,
            )

        # 3. RPC getAccountInfo fallback
        if self._rpc is not None:
            rpc_owner = self._fetch_owner_via_rpc(resolved_account)
            if rpc_owner:
                is_prog = rpc_owner in _PROGRAM_OWNED_MARKERS
                self._cache.set(resolved_account, rpc_owner)
                self._cache.flush()
                return OwnerResolutionResult(
                    token_account=resolved_account,
                    owner=rpc_owner,
                    is_program_owned=is_prog,
                    resolution_method=METHOD_RPC_ACCOUNT_INFO,
                    resolution_status=STATUS_OK,
                )

        # 4. Unresolved — degraded but not a crash
        logger.debug("Owner unresolved for token account %s", (resolved_account or "")[:16])
        return OwnerResolutionResult(
            token_account=resolved_account,
            owner=None,
            resolution_method=METHOD_UNRESOLVED,
            resolution_status=STATUS_DEGRADED,
        )

    def resolve_event_owners(
        self,
        normalized_event: dict[str, Any],
        pre_normalized: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Resolve source_owner and destination_owner for a normalized event.

        Returns a patch dict to merge into the normalized event.
        Mutates nothing — caller does event.update(patch).
        """
        pre_balances = pre_normalized.get("pre_token_balances") or []
        post_balances = pre_normalized.get("post_token_balances") or []
        account_keys = pre_normalized.get("account_keys_resolved") or []

        src_result = self.resolve(
            normalized_event.get("source_token_account"),
            pre_token_balances=pre_balances,
            post_token_balances=post_balances,
            account_keys=account_keys,
        )
        dst_result = self.resolve(
            normalized_event.get("destination_token_account"),
            pre_token_balances=pre_balances,
            post_token_balances=post_balances,
            account_keys=account_keys,
        )

        # Aggregate owner resolution status
        if src_result.resolution_status == STATUS_OK and dst_result.resolution_status == STATUS_OK:
            owner_resolution_status = STATUS_OK
        elif src_result.resolution_status == STATUS_OK or dst_result.resolution_status == STATUS_OK:
            owner_resolution_status = STATUS_DEGRADED  # partial
        else:
            owner_resolution_status = STATUS_DEGRADED

        # Determine overall owner_resolution_method
        methods = {src_result.resolution_method, dst_result.resolution_method}
        if METHOD_RPC_ACCOUNT_INFO in methods:
            owner_resolution_method = METHOD_RPC_ACCOUNT_INFO
        elif METHOD_OWNER_CACHE in methods:
            owner_resolution_method = METHOD_OWNER_CACHE
        elif METHOD_TOKEN_BALANCES in methods:
            owner_resolution_method = METHOD_TOKEN_BALANCES
        else:
            owner_resolution_method = METHOD_UNRESOLVED

        return {
            "source_owner": src_result.owner,
            "destination_owner": dst_result.owner,
            "owner_resolution_status": owner_resolution_status,
            "owner_resolution_method": owner_resolution_method,
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _fetch_owner_via_rpc(self, token_account: str) -> Optional[str]:
        """Fetch token account owner via getAccountInfo (jsonParsed encoding)."""
        try:
            info = self._rpc.get_account_info(
                token_account,
                encoding="jsonParsed",
                commitment="finalized",
                use_fallback=False,
            )
            if info is None:
                return None
            data = info.get("data", {})
            if isinstance(data, dict):
                parsed = data.get("parsed", {})
                info_inner = parsed.get("info", {})
                owner = info_inner.get("owner")
                return str(owner) if owner else None
        except Exception as exc:
            logger.warning("RPC owner fetch failed for %s: %s", token_account[:16], exc)
        return None


# ---------------------------------------------------------------------------
# AmountResolver
# ---------------------------------------------------------------------------

class AmountResolutionResult:
    __slots__ = (
        "amount_transferred_raw", "amount_received_raw",
        "fee_withheld_raw", "amount_decimal",
        "resolution_method", "resolution_status",
    )

    def __init__(
        self,
        *,
        amount_transferred_raw: Optional[int] = None,
        amount_received_raw: Optional[int] = None,
        fee_withheld_raw: Optional[int] = None,
        amount_decimal: Optional[Decimal] = None,
        resolution_method: str = METHOD_UNRESOLVED,
        resolution_status: str = STATUS_DEGRADED,
    ) -> None:
        self.amount_transferred_raw = amount_transferred_raw
        self.amount_received_raw = amount_received_raw
        self.fee_withheld_raw = fee_withheld_raw
        self.amount_decimal = amount_decimal
        self.resolution_method = resolution_method
        self.resolution_status = resolution_status

    def to_dict(self) -> dict[str, Any]:
        return {
            "amount_transferred_raw": self.amount_transferred_raw,
            "amount_received_raw": self.amount_received_raw,
            "fee_withheld_raw": self.fee_withheld_raw,
            "amount_decimal": self.amount_decimal,
            "amount_resolution_method": self.resolution_method,
            "amount_resolution_status": self.resolution_status,
        }


def resolve_amounts(
    pre_normalized: dict[str, Any],
    *,
    decimals: int = 6,
) -> AmountResolutionResult:
    """
    Resolve transfer amounts from a pre-normalized transaction.

    Hierarchy:
    1. Token balance delta (pre/post balances)
    2. Instruction data (already extracted in transfer_truth / token_program)
    3. Degraded — null amounts, no crash

    Parameters
    ----------
    pre_normalized:
        Output of pre_normalizer.normalize_transaction().
    decimals:
        Token decimal places (default 6 for USDC).
    """
    pre_balances = pre_normalized.get("pre_token_balances") or []
    post_balances = pre_normalized.get("post_token_balances") or []

    # Step 1: balance delta across all token accounts
    delta = _compute_full_balance_delta(pre_balances, post_balances)

    amount_received_raw = delta.get("received_raw")
    amount_transferred_raw = delta.get("transferred_raw")
    fee_withheld_raw: Optional[int] = None

    if amount_transferred_raw is not None and amount_received_raw is not None:
        raw_fee = amount_transferred_raw - amount_received_raw
        fee_withheld_raw = max(raw_fee, 0) if raw_fee >= 0 else None

    if amount_received_raw is not None:
        amount_decimal = _safe_decimal(amount_received_raw, decimals)
        return AmountResolutionResult(
            amount_transferred_raw=amount_transferred_raw,
            amount_received_raw=amount_received_raw,
            fee_withheld_raw=fee_withheld_raw,
            amount_decimal=amount_decimal,
            resolution_method=METHOD_TOKEN_BALANCES,
            resolution_status=STATUS_OK,
        )

    # Step 2: try instruction-level amounts from transfer_truth result
    # (stored in pre_normalized if available)
    instr_amount = pre_normalized.get("_transfer_truth_amount_received_raw")
    if instr_amount is not None:
        amount_decimal = _safe_decimal(int(instr_amount), decimals)
        return AmountResolutionResult(
            amount_received_raw=int(instr_amount),
            amount_decimal=amount_decimal,
            resolution_method="instruction",
            resolution_status=STATUS_OK,
        )

    # Step 3: degraded
    return AmountResolutionResult(
        resolution_method=METHOD_UNRESOLVED,
        resolution_status=STATUS_DEGRADED,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_placeholder(token_account: str, account_keys: list[str]) -> str:
    """
    Resolve __account_index_N__ placeholder to the actual pubkey.

    These placeholders are written by transfer_truth._token_account_key()
    when only the accountIndex is known. Phase 5 resolves them here.
    """
    if token_account.startswith("__account_index_") and token_account.endswith("__"):
        try:
            idx = int(token_account[len("__account_index_"):-2])
            if 0 <= idx < len(account_keys):
                return account_keys[idx]
        except (ValueError, IndexError):
            pass
    return token_account


def _owner_from_balances(
    token_account: str,
    pre_balances: list[dict],
    post_balances: list[dict],
    account_keys: list[str],
) -> Optional[str]:
    """
    Find the owner of token_account in pre/post token balance tables.

    The RPC token balance entry includes an `owner` field (wallet pubkey).
    We match by resolving the accountIndex to the pubkey and comparing.
    """
    key_to_idx = {k: i for i, k in enumerate(account_keys) if k}
    target_idx = key_to_idx.get(token_account)

    for balances in (pre_balances, post_balances):
        for entry in balances:
            idx = entry.get("accountIndex")
            if idx is None:
                continue
            # Match by index if we have it, or by exhaustive search
            if target_idx is not None and int(idx) != target_idx:
                continue
            owner = entry.get("owner")
            if owner and isinstance(owner, str):
                return owner

    return None


def _compute_full_balance_delta(
    pre_balances: list[dict],
    post_balances: list[dict],
) -> dict[str, Optional[int]]:
    """
    Compute the net send and receive amounts across all token accounts.

    Returns:
      received_raw  — largest positive delta (destination)
      transferred_raw — largest negative delta magnitude (source)
    """
    pre_by_idx: dict[int, dict] = {
        int(b["accountIndex"]): b for b in pre_balances
        if b.get("accountIndex") is not None
    }
    post_by_idx: dict[int, dict] = {
        int(b["accountIndex"]): b for b in post_balances
        if b.get("accountIndex") is not None
    }

    all_idx = set(pre_by_idx) | set(post_by_idx)
    best_received: Optional[int] = None
    best_transferred: Optional[int] = None

    for idx in all_idx:
        pre = pre_by_idx.get(idx, {})
        post = post_by_idx.get(idx, {})

        pre_amt = _raw_amount(pre)
        post_amt = _raw_amount(post)

        if pre_amt is None:
            pre_amt = 0
        if post_amt is None:
            post_amt = 0

        delta = post_amt - pre_amt

        if delta > 0:
            if best_received is None or delta > best_received:
                best_received = delta
        elif delta < 0:
            magnitude = abs(delta)
            if best_transferred is None or magnitude > best_transferred:
                best_transferred = magnitude

    return {"received_raw": best_received, "transferred_raw": best_transferred}


def _raw_amount(balance_entry: dict) -> Optional[int]:
    ui = balance_entry.get("uiTokenAmount") or {}
    amount_str = ui.get("amount")
    if amount_str is None:
        return None
    try:
        return int(amount_str)
    except (ValueError, TypeError):
        return None


def _safe_decimal(raw: int, decimals: int) -> Optional[Decimal]:
    try:
        return Decimal(raw) / Decimal(10 ** decimals)
    except (InvalidOperation, OverflowError):
        return None
