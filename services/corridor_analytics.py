"""
Deterministic corridor analytics for Canopy v4.

The current app has live chain-level fee and activity reads, but not fully
resolved entity-level corridor attribution. This module turns those live reads
into corridor intelligence using stable corridor profiles plus adversarial
checks, which keeps the system deterministic and explainable.
"""

from __future__ import annotations

import os
from datetime import timedelta
from typing import Dict, List, Optional

import pandas as pd

from services.bridge_solvency import check_bridge_solvency
from services.corridor_config import get_corridor_analytics_profile
from services.bigquery_client import execute_sql
from services.integrity_flags import build_integrity_flags
from services.liquidity_engine import (
    calculate_liquidity_score,
    calculate_trust_score,
    clamp_score,
    cost_score_from_fee,
    depth_score_from_usd,
)
from services.request_context import get_request_id
from services.strategy_engine import build_strategy_assessment
from services.summary_store import encode_top_whale_flows, get_corridor_summary
from services.token_registry import get_token_contract

LOW_INTEGRITY_THRESHOLD = 0.32
LIVE_CORRIDOR_BIGQUERY = os.getenv("CANOPY_CORRIDOR_BIGQUERY", "false").lower() == "true"
CORRIDOR_QUERY_MAX_BYTES_BILLED = int(
    os.getenv("CANOPY_CORRIDOR_MAX_BYTES_PER_QUERY", "250000000")
)

CORRIDOR_PROFILES: Dict[str, Dict[str, object]] = {
    "US-MX": {
        "weekly_multiplier": 6.8,
        "concentration_risk": 0.31,
        "whale_bias": 0.22,
        "rails": {
            "Polygon": {"share": 0.072, "bridge_share": 0.28, "sender_ratio": 0.46, "receiver_ratio": 0.41, "depth_multiplier": 1.12},
            "Ethereum": {"share": 0.034, "bridge_share": 0.08, "sender_ratio": 0.37, "receiver_ratio": 0.34, "depth_multiplier": 0.82},
            "Stellar": {"share": 0.022, "bridge_share": 0.02, "sender_ratio": 0.33, "receiver_ratio": 0.3, "depth_multiplier": 0.7},
        },
    },
    "US-BR": {
        "weekly_multiplier": 7.4,
        "concentration_risk": 0.28,
        "whale_bias": 0.3,
        "rails": {
            "Polygon": {"share": 0.088, "bridge_share": 0.34, "sender_ratio": 0.45, "receiver_ratio": 0.39, "depth_multiplier": 1.18},
            "Ethereum": {"share": 0.041, "bridge_share": 0.1, "sender_ratio": 0.35, "receiver_ratio": 0.31, "depth_multiplier": 0.86},
            "Stellar": {"share": 0.018, "bridge_share": 0.03, "sender_ratio": 0.28, "receiver_ratio": 0.27, "depth_multiplier": 0.66},
        },
    },
    "US-PH": {
        "weekly_multiplier": 7.1,
        "concentration_risk": 0.34,
        "whale_bias": 0.19,
        "rails": {
            "Polygon": {"share": 0.058, "bridge_share": 0.31, "sender_ratio": 0.44, "receiver_ratio": 0.4, "depth_multiplier": 1.04},
            "Ethereum": {"share": 0.032, "bridge_share": 0.09, "sender_ratio": 0.34, "receiver_ratio": 0.31, "depth_multiplier": 0.78},
            "Stellar": {"share": 0.026, "bridge_share": 0.03, "sender_ratio": 0.36, "receiver_ratio": 0.34, "depth_multiplier": 0.74},
        },
    },
    "US-VN": {
        "weekly_multiplier": 6.1,
        "concentration_risk": 0.46,
        "whale_bias": 0.16,
        "rails": {
            "Polygon": {"share": 0.031, "bridge_share": 0.42, "sender_ratio": 0.29, "receiver_ratio": 0.25, "depth_multiplier": 0.74},
            "Ethereum": {"share": 0.029, "bridge_share": 0.14, "sender_ratio": 0.33, "receiver_ratio": 0.29, "depth_multiplier": 0.82},
            "Stellar": {"share": 0.012, "bridge_share": 0.04, "sender_ratio": 0.23, "receiver_ratio": 0.22, "depth_multiplier": 0.52},
        },
    },
    "SG-ID": {
        "weekly_multiplier": 6.7,
        "concentration_risk": 0.37,
        "whale_bias": 0.27,
        "rails": {
            "Polygon": {"share": 0.049, "bridge_share": 0.38, "sender_ratio": 0.41, "receiver_ratio": 0.36, "depth_multiplier": 1.02},
            "Ethereum": {"share": 0.027, "bridge_share": 0.12, "sender_ratio": 0.31, "receiver_ratio": 0.28, "depth_multiplier": 0.8},
            "Stellar": {"share": 0.015, "bridge_share": 0.03, "sender_ratio": 0.26, "receiver_ratio": 0.25, "depth_multiplier": 0.61},
        },
    },
    "JP-SG": {
        "weekly_multiplier": 6.4,
        "concentration_risk": 0.24,
        "whale_bias": 0.34,
        "rails": {
            "Polygon": {"share": 0.037, "bridge_share": 0.3, "sender_ratio": 0.39, "receiver_ratio": 0.33, "depth_multiplier": 0.96},
            "Ethereum": {"share": 0.046, "bridge_share": 0.05, "sender_ratio": 0.36, "receiver_ratio": 0.32, "depth_multiplier": 0.98},
            "Stellar": {"share": 0.011, "bridge_share": 0.02, "sender_ratio": 0.24, "receiver_ratio": 0.22, "depth_multiplier": 0.48},
        },
    },
}

DEFAULT_PROFILE = {
    "weekly_multiplier": 6.5,
    "concentration_risk": 0.35,
    "whale_bias": 0.2,
    "rails": {
        "Polygon": {"share": 0.04, "bridge_share": 0.3, "sender_ratio": 0.4, "receiver_ratio": 0.34, "depth_multiplier": 1.0},
        "Ethereum": {"share": 0.03, "bridge_share": 0.08, "sender_ratio": 0.33, "receiver_ratio": 0.3, "depth_multiplier": 0.82},
        "Stellar": {"share": 0.012, "bridge_share": 0.03, "sender_ratio": 0.24, "receiver_ratio": 0.23, "depth_multiplier": 0.56},
    },
}

MEASURED_CORRIDOR_EXTRACTION_SQL = """
SELECT
  LOWER(transaction_hash) AS transaction_hash,
  block_timestamp,
  LOWER(from_address) AS from_address,
  LOWER(to_address) AS to_address,
  LOWER(token_address) AS token_address,
  SAFE_CAST(value AS FLOAT64) / 1000000 AS transfer_value_token
FROM `token_transfers`
WHERE LOWER(token_address) = LOWER(@token_contract)
  AND DATE(block_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND from_address IS NOT NULL
  AND to_address IS NOT NULL
"""


def _get_profile(corridor_key: str, rail: str) -> dict:
    profile = get_corridor_analytics_profile(corridor_key)
    rail_profile = profile["rails"].get(rail, DEFAULT_PROFILE["rails"]["Ethereum"])
    return {
        "weekly_multiplier": profile["weekly_multiplier"],
        "concentration_risk": profile["concentration_risk"],
        "whale_bias": profile["whale_bias"],
        **rail_profile,
    }


def _safe_number(value: Optional[float], fallback: float = 0.0) -> float:
    try:
        if value is None:
            return fallback
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _build_freshness_score(rail_data: dict) -> float:
    freshness_score = rail_data.get("freshness_score")
    if freshness_score is not None:
        return clamp_score(float(freshness_score))
    confidence = rail_data.get("confidence", 0.5)
    return clamp_score(confidence)


def _estimate_volume(base_volume_usdc: float, rail_profile: dict) -> float:
    return round(base_volume_usdc * rail_profile["share"], 2)


def _estimate_tx_count(base_tx_count: float, rail_profile: dict, mode: str) -> int:
    estimated = int(max(base_tx_count * rail_profile["share"], 1))
    if mode == "historical_reference":
        estimated = max(estimated // 2, 1)
    return estimated


def _normalize_integrity_score(unique_senders: int, volume_24h: float) -> float:
    raw_ratio = unique_senders / max(volume_24h, 1.0)
    normalized = unique_senders / max(volume_24h / 10_000, 1.0)
    return clamp_score(normalized), round(raw_ratio, 8)

def _build_volume_series(volume_7d: float) -> List[dict]:
    weights = [0.8, 0.9, 0.88, 1.04, 1.12, 0.98, 1.0]
    total_weight = sum(weights)
    base_unit = volume_7d / total_weight if total_weight else 0.0
    return [
        {"day": f"D-{6 - index}", "volume_usd": round(base_unit * weight, 2)}
        for index, weight in enumerate(weights)
    ]


def _derive_corridor_volume_from_measured_rows(
    measured_rows,
    *,
    corridor_id: str,
    rail: str,
    token: str,
    time_range: str,
) -> Optional[dict]:
    if measured_rows.empty:
        return None
    timestamps = measured_rows.copy(deep=True)
    timestamps["block_timestamp"] = pd.to_datetime(
        timestamps["block_timestamp"],
        utc=True,
        errors="coerce",
    )
    timestamps = timestamps.dropna(subset=["block_timestamp"])
    if timestamps.empty:
        return None
    now = timestamps["block_timestamp"].max()
    cutoff_24h = now - timedelta(hours=24)
    volume_7d = float(measured_rows["transfer_value_token"].sum())
    volume_24h = float(
        timestamps.loc[
            timestamps["block_timestamp"] >= cutoff_24h,
            "transfer_value_token",
        ].sum()
    )
    return {
        "corridor_id": corridor_id,
        "rail": rail,
        "token": token,
        "time_range": time_range,
        "volume_24h": round(volume_24h, 2),
        "volume_7d": round(volume_7d, 2),
        "source": "derived_from_measured_batch",
        "data_layer": "derived",
        "serving_path": "batch_bigquery",
    }


def _build_unavailable_corridor_summary(
    corridor_id: str,
    rail: str,
    *,
    token: str,
    time_range: str,
) -> dict:
    return {
        "corridor_id": corridor_id,
        "rail": rail,
        "token": token,
        "time_range": time_range,
        "volume_24h": 0.0,
        "volume_7d": 0.0,
        "tx_count": 0,
        "unique_senders": 0,
        "unique_receivers": 0,
        "velocity_unique_capital": 0.0,
        "concentration_score": 0.0,
        "bridge_name": check_bridge_solvency(rail, corridor_id)["bridge_name"],
        "bridge_share": 0.0,
        "bridge_volume": 0.0,
        "bridge_transactions": 0,
        "whale_threshold_usd": 5000,
        "whale_activity_score": 0.0,
        "net_flow_7d": 0.0,
        "top_whale_flows": [],
        "source": "derived_summary_unavailable",
        "data_layer": "derived",
        "serving_path": "summary_fallback",
    }


def get_corridor_volume(
    corridor_id: str,
    rail: str,
    rail_data: dict,
    *,
    token: str = "USDC",
    time_range: str = "24h",
    allow_live_bigquery: bool = False,
) -> dict:
    if allow_live_bigquery and get_request_id():
        raise RuntimeError("Request-path corridor analytics must not trigger live BigQuery.")
    if allow_live_bigquery and LIVE_CORRIDOR_BIGQUERY:
        try:
            token_contract = get_token_contract(token, rail)
            if token_contract:
                dataframe = execute_sql(
                    MEASURED_CORRIDOR_EXTRACTION_SQL,
                    params={"token_contract": token_contract},
                    query_name=f"measured_corridor_extraction_{rail.lower()}_{token.lower()}",
                    query_family="corridor_volume",
                    maximum_bytes_billed=CORRIDOR_QUERY_MAX_BYTES_BILLED,
                    query_classification="measured",
                    enforce_validation=True,
                )
                measured_volume = _derive_corridor_volume_from_measured_rows(
                    dataframe,
                    corridor_id=corridor_id,
                    rail=rail,
                    token=token,
                    time_range=time_range,
                )
                if measured_volume is not None:
                    return measured_volume
        except Exception:
            pass

    profile = _get_profile(corridor_id, rail)
    base_volume = _safe_number(
        rail_data.get("adjusted_volume_usdc"),
        _safe_number(rail_data.get("volume_usdc"), 0.0),
    )
    volume_24h = _estimate_volume(base_volume, profile)
    volume_7d = round(volume_24h * profile["weekly_multiplier"], 2)
    return {
        "corridor_id": corridor_id,
        "rail": rail,
        "token": token,
        "time_range": time_range,
        "volume_24h": volume_24h,
        "volume_7d": volume_7d,
        "source": "derived_deterministic_profile",
        "data_layer": "derived",
        "serving_path": "deterministic_fallback",
    }


def get_activity_metrics(corridor_id: str, rail: str, rail_data: dict) -> dict:
    profile = _get_profile(corridor_id, rail)
    base_tx_count = _safe_number(
        rail_data.get("adjusted_transfer_count"),
        _safe_number(rail_data.get("transfer_count"), 0.0),
    )
    tx_count = _estimate_tx_count(base_tx_count, profile, rail_data.get("mode", "live_measured"))
    unique_senders = int(max(tx_count * profile["sender_ratio"], 1))
    unique_receivers = int(max(tx_count * profile["receiver_ratio"], 1))
    return {
        "tx_count": tx_count,
        "unique_senders": unique_senders,
        "unique_receivers": unique_receivers,
        "velocity_unique_capital": clamp_score(unique_senders / max(tx_count, 1)),
        "concentration_score": clamp_score(profile["concentration_risk"]),
    }


def get_bridge_usage(corridor_id: str, rail: str, volume_24h: float, tx_count: int) -> dict:
    profile = _get_profile(corridor_id, rail)
    bridge_share = clamp_score(profile["bridge_share"])
    bridge_volume = round(volume_24h * bridge_share, 2)
    bridge_transactions = int(round(tx_count * bridge_share))
    bridge_name = check_bridge_solvency(rail, corridor_id)["bridge_name"]
    return {
        "bridge_name": bridge_name,
        "bridge_share": bridge_share,
        "bridge_volume": bridge_volume,
        "bridge_transactions": bridge_transactions,
    }


def get_whale_flows(corridor_id: str, rail: str, volume_7d: float, tx_count: int) -> dict:
    profile = _get_profile(corridor_id, rail)
    whale_threshold_usd = 5_000
    whale_activity_score = clamp_score(profile["whale_bias"] + min(tx_count / 4_000, 0.25))
    net_flow_7d = round(volume_7d * (profile["whale_bias"] - 0.12), 2)
    cohorts = []
    for index, multiplier in enumerate((0.42, 0.27, 0.18, 0.08, 0.05), start=1):
        direction = 1 if index % 2 else -1
        cohorts.append(
            {
                "cluster": f"High-value cohort {index:02d}",
                "net_flow_7d": round(net_flow_7d * multiplier * direction, 2),
                "threshold_usd": whale_threshold_usd,
            }
        )
    return {
        "whale_threshold_usd": whale_threshold_usd,
        "whale_activity_score": whale_activity_score,
        "net_flow_7d": net_flow_7d,
        "top_flows": cohorts,
    }


def build_corridor_base_summary(
    corridor_id: str,
    rail_data: dict,
    *,
    rail: Optional[str] = None,
    token: str = "USDC",
    time_range: str = "24h",
    allow_live_bigquery: bool = False,
) -> dict:
    effective_rail = rail or rail_data["rail"]
    volume_metrics = get_corridor_volume(
        corridor_id,
        effective_rail,
        rail_data,
        token=token,
        time_range=time_range,
        allow_live_bigquery=allow_live_bigquery,
    )
    activity = get_activity_metrics(corridor_id, effective_rail, rail_data)
    bridge_usage = get_bridge_usage(
        corridor_id,
        effective_rail,
        volume_metrics["volume_24h"],
        activity["tx_count"],
    )
    whale_flows = get_whale_flows(
        corridor_id,
        effective_rail,
        volume_metrics["volume_7d"],
        activity["tx_count"],
    )
    return {
        "corridor_id": corridor_id,
        "rail": effective_rail,
        "token": token,
        "time_range": time_range,
        "volume_24h": volume_metrics["volume_24h"],
        "volume_7d": volume_metrics["volume_7d"],
        "tx_count": activity["tx_count"],
        "unique_senders": activity["unique_senders"],
        "unique_receivers": activity["unique_receivers"],
        "velocity_unique_capital": activity["velocity_unique_capital"],
        "concentration_score": activity["concentration_score"],
        "bridge_name": bridge_usage["bridge_name"],
        "bridge_share": bridge_usage["bridge_share"],
        "bridge_volume": bridge_usage["bridge_volume"],
        "bridge_transactions": bridge_usage["bridge_transactions"],
        "whale_threshold_usd": whale_flows["whale_threshold_usd"],
        "whale_activity_score": whale_flows["whale_activity_score"],
        "net_flow_7d": whale_flows["net_flow_7d"],
        "top_whale_flows": whale_flows["top_flows"],
        "top_whale_flows_json": encode_top_whale_flows(whale_flows["top_flows"]),
        "source": volume_metrics["source"],
        "data_layer": volume_metrics.get("data_layer", "derived"),
        "serving_path": volume_metrics.get("serving_path", "summary_store"),
    }


def build_rail_corridor_metrics(
    corridor_id: str,
    rail_data: dict,
    *,
    transfer_amount_usdc: float,
    fee_floor: float,
    fee_ceiling: float,
    token: str = "USDC",
    time_range: str = "24h",
) -> dict:
    rail = rail_data["rail"]
    base_summary = get_corridor_summary(
        corridor_id,
        rail,
        token=token,
        time_range=time_range,
    )
    if base_summary is None:
        base_summary = _build_unavailable_corridor_summary(
            corridor_id,
            rail,
            token=token,
            time_range=time_range,
        )
    solvency = check_bridge_solvency(rail, corridor_id)

    integrity_score, raw_integrity_ratio = _normalize_integrity_score(
        base_summary["unique_senders"],
        base_summary["volume_24h"],
    )
    accessible_depth_usd = round(
        base_summary["volume_24h"] * _get_profile(corridor_id, rail)["depth_multiplier"],
        2,
    )
    depth_score = depth_score_from_usd(accessible_depth_usd, transfer_amount_usdc)
    raw_liquidity_score = calculate_liquidity_score(
        depth_score,
        clamp_score(solvency["solvency_ratio"]),
        integrity_score,
    )
    raw_trust_score = calculate_trust_score(
        integrity=integrity_score,
        solvency=clamp_score(solvency["solvency_ratio"]),
        concentration_risk=base_summary["concentration_score"],
        freshness=_build_freshness_score(rail_data),
    )
    cost_score = cost_score_from_fee(
        _safe_number(rail_data.get("estimated_fee_usd"), 0.0),
        fee_floor,
        fee_ceiling,
    )
    flags = build_integrity_flags(
        mode=rail_data.get("mode", "live_measured"),
        integrity_score=integrity_score,
        volume_diversity_score=base_summary["velocity_unique_capital"],
        solvency_ratio=float(solvency["solvency_ratio"]),
        estimated_fee_usd=_safe_number(rail_data.get("estimated_fee_usd"), 0.0),
        fee_floor=fee_floor,
        fee_ceiling=fee_ceiling,
    )
    strategy_assessment = build_strategy_assessment(
        cost_score=cost_score,
        liquidity_score=raw_liquidity_score,
        trust_score=raw_trust_score,
        flags=flags,
    )
    status = "OK" if not flags else "FLAGGED_" + "_".join(flags)

    return {
        "volume_24h": base_summary["volume_24h"],
        "volume_7d": base_summary["volume_7d"],
        "volume_series_7d": _build_volume_series(base_summary["volume_7d"]),
        "tx_count": base_summary["tx_count"],
        "unique_senders": base_summary["unique_senders"],
        "unique_receivers": base_summary["unique_receivers"],
        "velocity_unique_capital": base_summary["velocity_unique_capital"],
        "concentration_score": base_summary["concentration_score"],
        "integrity_score": integrity_score,
        "integrity_ratio_raw": raw_integrity_ratio,
        "integrity_threshold": LOW_INTEGRITY_THRESHOLD,
        "bridge_name": base_summary["bridge_name"],
        "bridge_share": base_summary["bridge_share"],
        "bridge_volume": base_summary["bridge_volume"],
        "bridge_transactions": base_summary["bridge_transactions"],
        "solvency_ratio": solvency["solvency_ratio"],
        "solvency_buffer_usd": solvency["buffer_usd"],
        "solvency_alert_level": solvency["alert_level"],
        "accessible_depth_usd": accessible_depth_usd,
        "depth_score": depth_score,
        "liquidity_score_v4": strategy_assessment["liquidity_score"],
        "trust_score_v4": strategy_assessment["trust_score"],
        "raw_liquidity_score_v4": raw_liquidity_score,
        "raw_trust_score_v4": raw_trust_score,
        "cost_score_v4": cost_score,
        "strategy_score": strategy_assessment["strategy_score"],
        "strategy_score_label": strategy_assessment["strategy_score_label"],
        "evidence_confidence": strategy_assessment["evidence_confidence"],
        "evidence_confidence_label": strategy_assessment["evidence_confidence_label"],
        "strategy_assessment": strategy_assessment,
        "route_score": strategy_assessment["strategy_score"],
        "whale_threshold_usd": base_summary["whale_threshold_usd"],
        "whale_activity_score": base_summary["whale_activity_score"],
        "net_flow_7d": base_summary["net_flow_7d"],
        "top_whale_flows": base_summary["top_whale_flows"],
        "adversarial_flags": flags,
        "status": status,
        "source": base_summary["source"],
        "data_layer": base_summary.get("data_layer"),
        "serving_path": base_summary.get("serving_path"),
    }
