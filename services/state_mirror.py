"""State mirror for deterministic Canopy execution simulations."""

from __future__ import annotations

import copy
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional
from uuid import uuid4

from api.cache import get_cache, get_cache_age_seconds

SNAPSHOT_TTL_SECONDS = 10

_snapshot_cache: Optional[dict] = None
_snapshot_cached_at: float = 0.0

GAS_MOVING_AVERAGES_GWEI = {
    "Ethereum": 24.0,
    "Polygon": 68.0,
    "Arbitrum": 0.12,
    "Base": 0.09,
}

BLOCK_TIMES_SECONDS = {
    "Ethereum": 15,
    "Polygon": 2,
    "Arbitrum": 1,
    "Base": 2,
}

CONFIRMATIONS = {
    "Ethereum": 12,
    "Polygon": 10,
    "Arbitrum": 20,
    "Base": 20,
}

POOL_BASELINES = {
    "Ethereum:ETH:USDC": {"reserve_in": 4_200, "reserve_out": 11_000_000, "fee_bps": 30},
    "Polygon:POL:USDC": {"reserve_in": 14_000_000, "reserve_out": 1_900_000, "fee_bps": 20},
    "Arbitrum:ETH:USDC": {"reserve_in": 3_600, "reserve_out": 8_400_000, "fee_bps": 24},
    "Base:ETH:USDC": {"reserve_in": 2_850, "reserve_out": 6_900_000, "fee_bps": 22},
    "Ethereum:USDC_BRIDGED:USDC": {"reserve_in": 4_900_000, "reserve_out": 4_860_000, "fee_bps": 15},
    "Polygon:USDC_BRIDGED:USDC": {"reserve_in": 6_400_000, "reserve_out": 6_320_000, "fee_bps": 18},
    "Arbitrum:USDC_BRIDGED:USDC": {"reserve_in": 5_600_000, "reserve_out": 5_520_000, "fee_bps": 18},
    "Base:USDC_BRIDGED:USDC": {"reserve_in": 4_200_000, "reserve_out": 4_140_000, "fee_bps": 18},
}

BRIDGE_CONFIG = {
    "Hop": {
        "protocol_fee_bps": 4,
        "bonder_fee_bps": 6,
        "liquidity_fee_bps": 3,
        "estimated_seconds": 150,
        "safety_factor": 0.9,
        "incentive_usd": 0.0,
    },
    "Stargate": {
        "protocol_fee_bps": 3,
        "bonder_fee_bps": 5,
        "liquidity_fee_bps": 2,
        "estimated_seconds": 135,
        "safety_factor": 0.92,
        "incentive_usd": 0.0,
    },
    "LayerZero": {
        "protocol_fee_bps": 2,
        "bonder_fee_bps": 4,
        "liquidity_fee_bps": 1,
        "estimated_seconds": 105,
        "safety_factor": 0.86,
        "incentive_usd": 5.0,
    },
    "PolygonBridge": {
        "protocol_fee_bps": 2,
        "bonder_fee_bps": 3,
        "liquidity_fee_bps": 1,
        "estimated_seconds": 220,
        "safety_factor": 0.95,
        "incentive_usd": 0.0,
    },
}

BRIDGE_VAULT_BALANCES = {
    "Hop": {
        "Ethereum:Polygon": 2_300_000.0,
        "Ethereum:Arbitrum": 1_950_000.0,
        "Ethereum:Base": 1_550_000.0,
    },
    "Stargate": {
        "Ethereum:Polygon": 3_600_000.0,
        "Ethereum:Arbitrum": 3_100_000.0,
        "Ethereum:Base": 2_650_000.0,
    },
    "LayerZero": {
        "Ethereum:Polygon": 1_400_000.0,
        "Ethereum:Arbitrum": 1_800_000.0,
        "Ethereum:Base": 1_600_000.0,
    },
    "PolygonBridge": {
        "Ethereum:Polygon": 4_200_000.0,
    },
}

BRIDGE_LIQUIDITY_VELOCITY = {
    "Hop:Ethereum:Polygon": {"current_per_min": 42_000.0, "historical_per_min": 14_000.0},
    "Hop:Ethereum:Arbitrum": {"current_per_min": 12_000.0, "historical_per_min": 10_000.0},
    "Stargate:Ethereum:Polygon": {"current_per_min": 11_000.0, "historical_per_min": 9_500.0},
    "Stargate:Ethereum:Arbitrum": {"current_per_min": 8_000.0, "historical_per_min": 9_000.0},
    "LayerZero:Ethereum:Arbitrum": {"current_per_min": 6_000.0, "historical_per_min": 3_200.0},
    "PolygonBridge:Ethereum:Polygon": {"current_per_min": 7_500.0, "historical_per_min": 6_800.0},
}


def _iso_from_timestamp(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _build_snapshot(now: float) -> dict:
    cache = get_cache()
    cache_age = get_cache_age_seconds() or 0
    eth_price = float(cache.get("eth_price_usd", 2100))
    polygon_price = float(cache.get("polygon_price_usd", 0.10))

    network_congestion = {
        "Ethereum": round(0.32 + min(cache_age / 3600, 0.25), 2),
        "Polygon": round(0.24 + min(cache_age / 5400, 0.2), 2),
        "Arbitrum": 0.21,
        "Base": 0.18,
    }
    gas_prices_gwei = {
        chain: round(base * (1 + (network_congestion.get(chain, 0.2) * 0.7)), 4)
        for chain, base in GAS_MOVING_AVERAGES_GWEI.items()
    }

    created_at = _iso_from_timestamp(now)
    expires_at = _iso_from_timestamp(now + SNAPSHOT_TTL_SECONDS)
    warnings = []
    if cache_age > SNAPSHOT_TTL_SECONDS:
        warnings.append("STALE_DATA_WARNING")

    return {
        "snapshot_id": f"snap_{uuid4().hex[:12]}",
        "created_at": created_at,
        "expiry_timestamp": expires_at,
        "ttl_seconds": SNAPSHOT_TTL_SECONDS,
        "warnings": warnings,
        "gas_prices_gwei": gas_prices_gwei,
        "gas_moving_average_gwei": dict(GAS_MOVING_AVERAGES_GWEI),
        "dex_pool_reserves": copy.deepcopy(POOL_BASELINES),
        "bridge_vault_balances": copy.deepcopy(BRIDGE_VAULT_BALANCES),
        "bridge_config": copy.deepcopy(BRIDGE_CONFIG),
        "bridge_liquidity_velocity": copy.deepcopy(BRIDGE_LIQUIDITY_VELOCITY),
        "network_congestion": network_congestion,
        "block_times": dict(BLOCK_TIMES_SECONDS),
        "confirmations": dict(CONFIRMATIONS),
        "native_token_prices_usd": {
            "Ethereum": eth_price,
            "Polygon": polygon_price,
            "Arbitrum": eth_price,
            "Base": eth_price,
        },
        "data_freshness": {
            "gas_age_sec": max(5, min(int(cache_age), 45)),
            "pool_age_sec": max(3, min(int(cache_age), 25)),
        },
    }


def get_state_snapshot(*, force_refresh: bool = False, allow_stale: bool = False) -> dict:
    global _snapshot_cache, _snapshot_cached_at

    now = time.time()
    should_refresh = (
        force_refresh
        or _snapshot_cache is None
        or (now - _snapshot_cached_at) >= SNAPSHOT_TTL_SECONDS
    )

    if should_refresh:
        _snapshot_cache = _build_snapshot(now)
        _snapshot_cached_at = now
    elif allow_stale and (now - _snapshot_cached_at) >= SNAPSHOT_TTL_SECONDS:
        warnings = list(_snapshot_cache.get("warnings", []))
        if "STALE_DATA_WARNING" not in warnings:
            warnings.append("STALE_DATA_WARNING")
        _snapshot_cache["warnings"] = warnings

    snapshot = copy.deepcopy(_snapshot_cache)
    snapshot_age = int(max(0, now - _snapshot_cached_at))
    snapshot["data_freshness"]["snapshot_age_sec"] = snapshot_age
    snapshot["data_freshness"]["snapshot_expires_in_sec"] = max(
        0, SNAPSHOT_TTL_SECONDS - snapshot_age
    )
    return snapshot
