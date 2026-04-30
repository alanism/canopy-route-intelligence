"""AMM swap simulation helpers."""

from __future__ import annotations

from typing import Dict


def simulate_amm_swap(
    *,
    amount_in: float,
    reserve_in: float,
    reserve_out: float,
    fee_bps: int,
) -> dict:
    if amount_in <= 0:
        raise ValueError("amount_in must be positive")
    if reserve_in <= 0 or reserve_out <= 0:
        raise ValueError("pool reserves must be positive")

    fee_multiplier = 10_000 - fee_bps
    amount_in_with_fee = amount_in * fee_multiplier
    numerator = amount_in_with_fee * reserve_out
    denominator = (reserve_in * 10_000) + amount_in_with_fee
    amount_out = numerator / denominator

    mid_price = reserve_out / reserve_in
    execution_price = amount_out / amount_in
    slippage = max(0.0, 1 - (execution_price / mid_price))
    trade_share = amount_in / reserve_in

    return {
        "amount_out": round(amount_out, 6),
        "slippage": round(slippage, 6),
        "execution_price": round(execution_price, 8),
        "trade_share": round(trade_share, 6),
        "mid_price": round(mid_price, 8),
    }


def simulate_swap_from_snapshot(
    *,
    snapshot: dict,
    pool_key: str,
    amount_in: float,
) -> dict:
    pool = snapshot["dex_pool_reserves"][pool_key]
    result = simulate_amm_swap(
        amount_in=amount_in,
        reserve_in=float(pool["reserve_in"]),
        reserve_out=float(pool["reserve_out"]),
        fee_bps=int(pool["fee_bps"]),
    )
    result["pool_key"] = pool_key
    result["fee_bps"] = int(pool["fee_bps"])
    result["pool_depth"] = float(pool["reserve_in"])
    return result
