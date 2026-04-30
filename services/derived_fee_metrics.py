"""Derived fee metrics computed from measured BigQuery extraction rows."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, Optional


def _coerce_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_datetime(value: object) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _percentile_cont(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * percentile
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    fraction = position - lower_index
    lower_value = ordered[lower_index]
    upper_value = ordered[upper_index]
    return lower_value + (upper_value - lower_value) * fraction


def derive_fee_metrics(
    measured_rows: Iterable[dict],
    *,
    chain: str,
    token: str,
    token_contract: str,
    native_price_usd: float,
    window_label: str,
    queried_at: Optional[datetime] = None,
    min_payment_stablecoin: float = 1.0,
) -> Optional[dict]:
    """
    Convert measured transfer rows into derived fee metrics.

    Assumptions are explicit here rather than hidden in SQL:
    - payment-like activity means transfer value >= min_payment_stablecoin
    - adjusted-direct activity means the transaction's destination contract is the token contract
    - fee math converts native gas cost into USD using the caller-provided spot price
    """

    rows = [dict(row) for row in measured_rows]
    if not rows:
        return None

    tx_rollups: dict[str, dict] = {}
    token_contract_lc = token_contract.lower()

    for row in rows:
        transaction_hash = str(row.get("transaction_hash") or "").lower()
        if not transaction_hash:
            continue
        transfer_value = _coerce_float(row.get("transfer_value_token")) or 0.0
        block_timestamp = _coerce_datetime(row.get("block_timestamp"))
        tx_state = tx_rollups.setdefault(
            transaction_hash,
            {
                "transaction_hash": transaction_hash,
                "block_timestamp": block_timestamp,
                "transfer_logs": 0,
                "payment_like_transfer_logs": 0,
                "transfer_volume_usdc": 0.0,
                "payment_like_volume_usdc": 0.0,
                "tx_to_address": str(row.get("tx_to_address") or "").lower() or None,
                "token_address": str(row.get("token_address") or token_contract_lc).lower(),
                "status": row.get("status"),
                "gas_used": _coerce_float(row.get("gas_used")),
                "effective_gas_price": _coerce_float(row.get("effective_gas_price")),
            },
        )
        if block_timestamp and (
            tx_state["block_timestamp"] is None or block_timestamp > tx_state["block_timestamp"]
        ):
            tx_state["block_timestamp"] = block_timestamp
        tx_state["transfer_logs"] += 1
        tx_state["transfer_volume_usdc"] += transfer_value
        if transfer_value >= min_payment_stablecoin:
            tx_state["payment_like_transfer_logs"] += 1
            tx_state["payment_like_volume_usdc"] += transfer_value
        if tx_state["tx_to_address"] is None and row.get("tx_to_address"):
            tx_state["tx_to_address"] = str(row["tx_to_address"]).lower()
        if tx_state["gas_used"] is None:
            tx_state["gas_used"] = _coerce_float(row.get("gas_used"))
        if tx_state["effective_gas_price"] is None:
            tx_state["effective_gas_price"] = _coerce_float(row.get("effective_gas_price"))
        if tx_state["status"] is None:
            tx_state["status"] = row.get("status")

    classified_transactions: list[dict] = []
    for tx_state in tx_rollups.values():
        gas_used = tx_state["gas_used"]
        gas_price = tx_state["effective_gas_price"]
        status = tx_state["status"]
        transfer_logs = tx_state["transfer_logs"]
        if status != 1 or gas_used is None or gas_price is None or transfer_logs == 0:
            continue
        tx_fee_usd = ((gas_used * gas_price) / 1e18) * native_price_usd
        fee_per_transfer = tx_fee_usd / transfer_logs
        tx_state = {
            **tx_state,
            "tx_fee_usd": tx_fee_usd,
            "fee_usd_per_transfer": fee_per_transfer,
            "is_adjusted_direct": (
                tx_state["tx_to_address"] == token_contract_lc
                and tx_state["payment_like_transfer_logs"] > 0
            ),
        }
        classified_transactions.append(tx_state)

    if not classified_transactions:
        return None

    queried_at = queried_at or datetime.now(timezone.utc)
    fee_values = [item["fee_usd_per_transfer"] for item in classified_transactions]
    transfer_count = sum(item["transfer_logs"] for item in classified_transactions)
    total_fee_usd = sum(item["tx_fee_usd"] for item in classified_transactions)
    freshness_timestamp = max(
        (item["block_timestamp"] for item in classified_transactions if item["block_timestamp"] is not None),
        default=None,
    )

    adjusted_transactions = [item for item in classified_transactions if item["is_adjusted_direct"]]
    adjusted_freshness_timestamp = max(
        (item["block_timestamp"] for item in adjusted_transactions if item["block_timestamp"] is not None),
        default=None,
    )

    gap_minutes: list[float] = []
    ordered_adjusted_times = sorted(
        item["block_timestamp"] for item in adjusted_transactions if item["block_timestamp"] is not None
    )
    for previous, current in zip(ordered_adjusted_times, ordered_adjusted_times[1:]):
        gap_minutes.append((current - previous).total_seconds() / 60.0)

    return {
        "chain": chain,
        "token": token,
        "avg_fee_usd": round(total_fee_usd / transfer_count, 6) if transfer_count else 0.0,
        "median_fee_usd": round(_percentile_cont(fee_values, 0.5), 6),
        "p90_fee_usd": round(_percentile_cont(fee_values, 0.9), 6),
        "transfer_count": transfer_count,
        "volume_usdc": round(
            sum(item["transfer_volume_usdc"] for item in classified_transactions),
            6,
        ),
        "adjusted_transaction_count": len(adjusted_transactions),
        "adjusted_transfer_count": sum(
            item["payment_like_transfer_logs"] for item in adjusted_transactions
        ),
        "adjusted_volume_usdc": round(
            sum(item["payment_like_volume_usdc"] for item in adjusted_transactions),
            6,
        )
        if adjusted_transactions
        else None,
        "adjusted_freshness_timestamp": (
            adjusted_freshness_timestamp.isoformat() if adjusted_freshness_timestamp else None
        ),
        "minutes_since_last_adjusted_transfer": (
            int((queried_at - adjusted_freshness_timestamp).total_seconds() // 60)
            if adjusted_freshness_timestamp
            else None
        ),
        "avg_minutes_between_adjusted_transfers": (
            round(sum(gap_minutes) / len(gap_minutes), 2) if gap_minutes else None
        ),
        "activity_filter_method": (
            f"Direct {token} contract calls with transfer value >= $1; excludes obvious "
            "router-mediated flows, zero-value logs, and dust-like activity."
        ),
        "window_used": window_label,
        "freshness_timestamp": freshness_timestamp.isoformat() if freshness_timestamp else None,
        "native_price_used_usd": native_price_usd,
        "queried_at": queried_at.isoformat(),
    }
