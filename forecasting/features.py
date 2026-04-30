"""Feature extraction for Canopy corridor forecasting."""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import pandas as pd

from api.corridor_metrics import build_corridor_analytics_response
from api.router import get_demo_presets, get_route

try:
    from statsmodels.tsa.stattools import adfuller
except ImportError:  # pragma: no cover - graceful fallback for lightweight envs
    adfuller = None

HISTORY_DAYS = 45

REGULATORY_RISK_BY_EXPOSURE = {
    "low": 0.22,
    "moderate": 0.48,
    "medium": 0.48,
    "medium-high": 0.58,
    "high": 0.74,
    "developing": 0.68,
    "emerging": 0.62,
    "unknown": 0.55,
}

FX_RISK_BY_CORRIDOR = {
    "US-MX": 0.34,
    "US-BR": 0.63,
    "US-NG": 0.71,
    "US-PH": 0.41,
    "US-ZA": 0.56,
    "US-VN": 0.58,
    "SG-ID": 0.52,
    "JP-SG": 0.24,
}


def clamp_probability(value: float) -> float:
    return round(max(0.0, min(float(value), 1.0)), 4)


def normalize_corridor_id(corridor_id: str) -> str:
    corridor_key = (corridor_id or "").strip().lower()
    for preset in get_demo_presets():
        if corridor_key in {
            str(preset.get("key", "")).lower(),
            str(preset.get("corridor_slug", "")).lower(),
            str(preset.get("label", "")).lower(),
        }:
            return str(preset["key"])
    raise ValueError(f"Unknown corridor id: {corridor_id}")


def _get_preset(corridor_key: str) -> dict:
    for preset in get_demo_presets():
        if str(preset.get("key")) == corridor_key:
            return preset
    raise ValueError(f"Unknown corridor key: {corridor_key}")


def _series_mean(values: List[float]) -> float:
    return sum(values) / max(len(values), 1)


def _series_std(values: List[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean_value = _series_mean(values)
    variance = sum((value - mean_value) ** 2 for value in values) / (len(values) - 1)
    return variance ** 0.5


def _safe_log_ratio(current: float, previous: float) -> float:
    if current <= 0 or previous <= 0:
        return 0.0
    return math.log(current / previous)


def _build_synthetic_history(
    *,
    corridor_key: str,
    analytics: dict,
    route_result: dict,
    history_days: int,
) -> pd.DataFrame:
    volume_24h = float(analytics.get("volume_24h", 1.0) or 1.0)
    tx_count = int(analytics.get("tx_count", 1) or 1)
    bridge_share = float(analytics.get("bridge_share", 0.2) or 0.2)
    liquidity_score = float(analytics.get("liquidity_score", 0.5) or 0.5)
    integrity_score = float(analytics.get("integrity_score", 0.5) or 0.5)
    route_score = float(route_result.get("route_score", 0.5) or 0.5)
    fee_usd = float(route_result.get("expected_fee_usd", 0.0) or 0.0)

    corridor_seed = sum(ord(char) for char in corridor_key)
    daily_rows: List[dict] = []
    base_date = datetime.now(timezone.utc).date() - timedelta(days=history_days - 1)

    for day_offset in range(history_days):
        date_value = base_date + timedelta(days=day_offset)
        weekly_cycle = 1.0 + (((day_offset % 7) - 3) * 0.028)
        momentum = 1.0 + (((day_offset / max(history_days - 1, 1)) - 0.5) * 0.14)
        corridor_bias = 1.0 + (((corridor_seed % 11) - 5) * 0.012)
        shock_wave = 1.0 + ((((day_offset + corridor_seed) % 9) - 4) * 0.017)

        daily_volume = max(volume_24h * 0.88 * weekly_cycle * momentum * corridor_bias * shock_wave, 1.0)
        daily_tx_count = max(int(tx_count * weekly_cycle * (0.96 + day_offset * 0.0025)), 1)
        unique_addresses = max(int(daily_tx_count * max(integrity_score, 0.08)), 1)
        gas_fee_usd = max(fee_usd * (0.82 + (day_offset % 5) * 0.045), 0.01)
        dex_liquidity = max(
            daily_volume * (0.9 + liquidity_score * 0.55) * (0.96 + ((day_offset + 2) % 6) * 0.016),
            1.0,
        )
        bridge_flow = max(daily_volume * bridge_share * (0.88 + ((day_offset + 1) % 4) * 0.055), 0.0)
        stablecoin_supply = max(daily_volume * (1.45 + route_score * 0.35), 1.0)

        daily_rows.append(
            {
                "date": pd.Timestamp(date_value),
                "volume_usd": round(daily_volume, 2),
                "fees_usd": round(gas_fee_usd, 6),
                "bridge_volume_usd": round(bridge_flow, 2),
                "tx_count": daily_tx_count,
                "unique_addresses": unique_addresses,
                "dex_liquidity_usd": round(dex_liquidity, 2),
                "stablecoin_supply_usd": round(stablecoin_supply, 2),
                "volume_time": round(sum(row["volume_usd"] for row in daily_rows) + daily_volume, 2),
            }
        )

    frame = pd.DataFrame(daily_rows)
    frame["volume_log_return"] = [
        0.0,
        *[
            _safe_log_ratio(frame.iloc[index]["volume_usd"], frame.iloc[index - 1]["volume_usd"])
            for index in range(1, len(frame))
        ],
    ]
    frame["fee_log_return"] = [
        0.0,
        *[
            _safe_log_ratio(frame.iloc[index]["fees_usd"], frame.iloc[index - 1]["fees_usd"])
            for index in range(1, len(frame))
        ],
    ]
    frame["liquidity_log_return"] = [
        0.0,
        *[
            _safe_log_ratio(
                frame.iloc[index]["dex_liquidity_usd"],
                frame.iloc[index - 1]["dex_liquidity_usd"],
            )
            for index in range(1, len(frame))
        ],
    ]
    frame["bridge_log_return"] = [
        0.0,
        *[
            _safe_log_ratio(
                max(frame.iloc[index]["bridge_volume_usd"], 1.0),
                max(frame.iloc[index - 1]["bridge_volume_usd"], 1.0),
            )
            for index in range(1, len(frame))
        ],
    ]
    frame["rolling_volatility_7d"] = frame["volume_log_return"].rolling(7, min_periods=2).std().fillna(0.0)
    frame["rolling_volatility_30d"] = frame["volume_log_return"].rolling(30, min_periods=2).std().fillna(0.0)
    frame["rolling_fee_volatility_7d"] = frame["fee_log_return"].rolling(7, min_periods=2).std().fillna(0.0)
    return frame


def _stationarity_check(series: pd.Series) -> dict:
    cleaned = pd.Series([float(value) for value in series.tolist() if pd.notna(value)])
    if len(cleaned) < 6 or cleaned.std() == 0:
        return {
            "stationary": True,
            "p_value": 0.01,
            "transformation": "none",
        }

    if adfuller is not None:
        try:
            p_value = float(adfuller(cleaned)[1])
        except ValueError:
            p_value = 0.01
    else:
        lagged = cleaned.shift(1).dropna()
        aligned = cleaned.iloc[1:]
        autocorr = 0.0
        if len(aligned) > 2 and aligned.std() > 0 and lagged.std() > 0:
            autocorr = float(aligned.corr(lagged) or 0.0)
        p_value = 0.12 if abs(float(autocorr or 0.0)) > 0.85 else 0.03

    stationary = p_value <= 0.05
    return {
        "stationary": stationary,
        "p_value": round(p_value, 6),
        "transformation": "difference" if not stationary else "none",
    }


def build_feature_bundle(corridor_id: str, *, history_days: int = HISTORY_DAYS) -> dict:
    corridor_key = normalize_corridor_id(corridor_id)
    preset = _get_preset(corridor_key)
    route_result = get_route(
        origin=preset["origin"],
        destination=preset["destination"],
        amount_usdc=float(preset.get("default_amount_usdc", 50_000)),
        time_sensitivity="standard",
        monthly_volume_usdc=float(preset.get("default_monthly_volume_usdc", 1_000_000)),
        current_rail_fee_pct=float(preset.get("default_baseline_fee_pct", 1.5)),
        current_rail_settlement_hours=float(preset.get("default_baseline_settlement_hours", 24)),
        current_setup=preset.get("default_current_setup", ""),
        compliance_sensitivity="medium",
        lens="strategy",
    )
    analytics = build_corridor_analytics_response(route_result, time_range="7d")
    feature_frame = _build_synthetic_history(
        corridor_key=corridor_key,
        analytics=analytics,
        route_result=route_result,
        history_days=history_days,
    )

    latest_row = feature_frame.iloc[-1]
    volume_stationarity = _stationarity_check(feature_frame["volume_log_return"])
    fee_stationarity = _stationarity_check(feature_frame["fee_log_return"])
    liquidity_stationarity = _stationarity_check(feature_frame["liquidity_log_return"])

    regulatory_exposure = str(preset.get("regulatory_exposure", "unknown")).strip().lower()
    fx_risk = FX_RISK_BY_CORRIDOR.get(corridor_key, 0.45)
    regulatory_risk = REGULATORY_RISK_BY_EXPOSURE.get(regulatory_exposure, 0.55)

    return {
        "corridor_id": corridor_key,
        "corridor_label": preset["label"],
        "preset": preset,
        "route_result": route_result,
        "analytics": analytics,
        "feature_frame": feature_frame,
        "current_state": {
            "volume_24h": float(analytics.get("volume_24h", 0.0) or 0.0),
            "volume_7d": float(analytics.get("volume_7d", 0.0) or 0.0),
            "tx_count": int(analytics.get("tx_count", 0) or 0),
            "unique_senders": int(analytics.get("unique_senders", 0) or 0),
            "unique_receivers": int(analytics.get("unique_receivers", 0) or 0),
            "integrity_score": float(analytics.get("integrity_score", 0.0) or 0.0),
            "liquidity_score": float(analytics.get("liquidity_score", 0.0) or 0.0),
            "trust_score": float(analytics.get("trust_score", 0.0) or 0.0),
            "bridge_volume": float(analytics.get("bridge_volume", 0.0) or 0.0),
            "bridge_share": float(analytics.get("bridge_share", 0.0) or 0.0),
            "solvency_ratio": float(
                analytics.get("best_route_detail", {}).get("solvency_ratio", 1.0) or 1.0
            ),
            "estimated_fee_usd": float(route_result.get("expected_fee_usd", 0.0) or 0.0),
            "transfer_amount_usdc": float(route_result.get("amount_usdc", 0.0) or 0.0),
            "latest_volume_log_return": float(latest_row["volume_log_return"]),
            "latest_fee_log_return": float(latest_row["fee_log_return"]),
            "latest_liquidity_log_return": float(latest_row["liquidity_log_return"]),
            "rolling_volatility_7d": float(latest_row["rolling_volatility_7d"]),
            "rolling_volatility_30d": float(latest_row["rolling_volatility_30d"]),
            "rolling_fee_volatility_7d": float(latest_row["rolling_fee_volatility_7d"]),
            "fx_volatility_signal": fx_risk,
            "regulatory_risk_index": regulatory_risk,
        },
        "stationarity": {
            "volume": volume_stationarity,
            "fees": fee_stationarity,
            "liquidity": liquidity_stationarity,
        },
        "freshness": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "feature_window_days": history_days,
        },
    }
