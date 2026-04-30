"""Deterministic bridge solvency checks for Canopy v4."""

from __future__ import annotations

from typing import Dict

BRIDGE_BASELINES: Dict[str, Dict[str, float | str]] = {
    "Polygon": {
        "bridge_name": "Polygon bridge",
        "vault_reserves_usd": 1_204_000_000,
        "minted_supply_usd": 1_201_600_000,
    },
    "Ethereum": {
        "bridge_name": "Native settlement / no bridge dependency",
        "vault_reserves_usd": 1_000_000_000,
        "minted_supply_usd": 999_000_000,
    },
    "Stellar": {
        "bridge_name": "Historical reference rail",
        "vault_reserves_usd": 400_000_000,
        "minted_supply_usd": 400_200_000,
    },
}

CORRIDOR_STRESS = {
    "US-NG": {"Polygon": 0.0006, "Ethereum": 0.0002},
    "US-VN": {"Polygon": 0.0008},
    "SG-ID": {"Polygon": 0.0004},
    "US-PH": {"Polygon": 0.0002},
    "US-BR": {"Ethereum": 0.0001},
}


def check_bridge_solvency(rail: str, corridor_key: str) -> dict:
    baseline = BRIDGE_BASELINES.get(rail, BRIDGE_BASELINES["Ethereum"])
    reserves = float(baseline["vault_reserves_usd"])
    minted_supply = float(baseline["minted_supply_usd"])
    stress = CORRIDOR_STRESS.get(corridor_key, {}).get(rail, 0.0)
    ratio = max(0.0, round((reserves / max(minted_supply, 1.0)) - stress, 6))
    buffer = round(reserves - minted_supply, 2)

    if ratio < 0.999:
        alert_level = "HIGH_RISK"
    elif ratio < 1.0:
        alert_level = "WATCH"
    else:
        alert_level = "OK"

    return {
        "bridge_name": baseline["bridge_name"],
        "vault_reserves_usd": reserves,
        "minted_supply_usd": minted_supply,
        "solvency_ratio": ratio,
        "buffer_usd": buffer,
        "alert_level": alert_level,
    }
