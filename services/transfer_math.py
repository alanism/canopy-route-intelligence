"""Deterministic transfer economics for the Canopy corridor UI."""

from __future__ import annotations

from typing import Dict, Optional


TRANSFER_ASSUMPTIONS: Dict[str, Dict[str, float]] = {
    "Polygon": {
        "routing_bps": 0.0014,
        "routing_fixed_fee_usd": 0.20,
        "routing_min_fee_usd": 1.25,
        "reference_network_fee_usd": 0.01,
    },
    "Ethereum": {
        "routing_bps": 0.0011,
        "routing_fixed_fee_usd": 0.35,
        "routing_min_fee_usd": 1.75,
        "reference_network_fee_usd": 3.20,
    },
    "Stellar": {
        "routing_bps": 0.0010,
        "routing_fixed_fee_usd": 0.15,
        "routing_min_fee_usd": 0.75,
        "reference_network_fee_usd": 0.02,
    },
    "default": {
        "routing_bps": 0.0013,
        "routing_fixed_fee_usd": 0.20,
        "routing_min_fee_usd": 1.00,
        "reference_network_fee_usd": 0.05,
    },
}


def get_transfer_assumptions(rail: str) -> Dict[str, float]:
    return TRANSFER_ASSUMPTIONS.get(rail, TRANSFER_ASSUMPTIONS["default"]).copy()


def build_transfer_math(
    *,
    rail: str,
    amount_usdc: float,
    network_fee_usd: Optional[float],
    measured_fee_available: bool,
) -> dict:
    assumptions = get_transfer_assumptions(rail)
    amount_usdc = round(float(amount_usdc), 6)
    applied_network_fee_usd = float(
        network_fee_usd
        if network_fee_usd is not None
        else assumptions["reference_network_fee_usd"]
    )
    routing_fee_usd = max(
        (amount_usdc * assumptions["routing_bps"]) + assumptions["routing_fixed_fee_usd"],
        assumptions["routing_min_fee_usd"],
    )
    total_fee_usd = round(applied_network_fee_usd + routing_fee_usd, 6)
    landed_amount_usd = round(max(amount_usdc - total_fee_usd, 0.0), 6)

    return {
        "amount_usdc": amount_usdc,
        "network_fee_usd": round(applied_network_fee_usd, 6),
        "routing_bps": assumptions["routing_bps"],
        "routing_fixed_fee_usd": assumptions["routing_fixed_fee_usd"],
        "routing_min_fee_usd": assumptions["routing_min_fee_usd"],
        "routing_fee_usd": round(routing_fee_usd, 6),
        "total_fee_usd": total_fee_usd,
        "landed_amount_usd": landed_amount_usd,
        "provenance": {
            "network_fee_usd": "MEASURED" if measured_fee_available else "MODELED",
            "routing_fee_usd": "CALCULATED",
            "total_fee_usd": "CALCULATED",
            "landed_amount_usd": "CALCULATED",
        },
        "trace": {
            "routing_fee_formula": "max(amount * routing_bps + routing_fixed_fee, routing_min_fee)",
            "total_fee_formula": "network_fee + routing_fee",
            "landed_amount_formula": "amount - total_fee",
        },
    }
