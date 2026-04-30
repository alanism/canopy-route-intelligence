"""Bridge transfer simulation helpers."""

from __future__ import annotations


def simulate_bridge_transfer(
    *,
    snapshot: dict,
    bridge_name: str,
    source_chain: str,
    destination_chain: str,
    input_amount: float,
) -> dict:
    bridge_cfg = snapshot["bridge_config"][bridge_name]
    vault_balance = float(
        snapshot["bridge_vault_balances"].get(bridge_name, {}).get(
            f"{source_chain}:{destination_chain}",
            0.0,
        )
    )

    protocol_fee = input_amount * (float(bridge_cfg["protocol_fee_bps"]) / 10_000)
    bonder_fee = input_amount * (float(bridge_cfg["bonder_fee_bps"]) / 10_000)
    liquidity_fee = input_amount * (float(bridge_cfg["liquidity_fee_bps"]) / 10_000)
    fees_total = protocol_fee + bonder_fee + liquidity_fee
    incentive_usd = float(bridge_cfg.get("incentive_usd", 0.0))
    amount_out = max(input_amount - fees_total + incentive_usd, 0.0)

    return {
        "mechanism": "lock-mint" if source_chain != destination_chain else "same-chain",
        "amount_out": round(amount_out, 6),
        "protocol_fee": round(protocol_fee, 6),
        "bonder_fee": round(bonder_fee, 6),
        "liquidity_fee": round(liquidity_fee, 6),
        "fees_total_usd": round(fees_total, 6),
        "incentive_usd": round(incentive_usd, 6),
        "vault_balance": round(vault_balance, 2),
        "estimated_seconds": int(bridge_cfg["estimated_seconds"]),
        "safety_factor": float(bridge_cfg["safety_factor"]),
        "bridge_name": bridge_name,
    }
