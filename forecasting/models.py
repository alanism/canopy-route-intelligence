"""Forecasting models for Canopy corridor health."""

from __future__ import annotations

from typing import List

import pandas as pd

from forecasting.features import clamp_probability

try:
    from statsmodels.tsa.regime_switching.markov_regression import MarkovRegression
except ImportError:  # pragma: no cover - optional dependency
    MarkovRegression = None

try:
    from statsmodels.tsa.statespace.structural import UnobservedComponents
except ImportError:  # pragma: no cover - optional dependency
    UnobservedComponents = None

try:
    from statsmodels.tsa.arima.model import ARIMA
except ImportError:  # pragma: no cover - optional dependency
    ARIMA = None


def _mean(values: List[float]) -> float:
    return sum(values) / max(len(values), 1)


def _std(values: List[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean_value = _mean(values)
    variance = sum((value - mean_value) ** 2 for value in values) / (len(values) - 1)
    return variance ** 0.5


def detect_regime_probability(feature_bundle: dict, volatility_result: dict) -> dict:
    frame: pd.DataFrame = feature_bundle["feature_frame"]
    series = [float(value) for value in frame["rolling_volatility_7d"].tolist() if pd.notna(value)]
    predicted_volatility = float(volatility_result["predicted_corridor_volatility"])

    if MarkovRegression is not None and len(series) >= 12:
        try:
            model = MarkovRegression(series, k_regimes=2, trend="c", switching_variance=True)
            fitted = model.fit(disp=False)
            smoothed = fitted.smoothed_marginal_probabilities
            if hasattr(smoothed, "iloc"):
                high_regime_probability = float(smoothed.iloc[-1].max())
            else:
                last_row = smoothed[-1] if getattr(smoothed, "ndim", 1) > 1 else [smoothed[-1]]
                high_regime_probability = float(max(last_row))
            model_used = "markov_switching"
        except Exception:
            baseline = _mean(series[-14:] or series)
            spread = _std(series[-14:] or series)
            high_regime_probability = min(
                max((predicted_volatility - baseline) / max(spread * 3, 1e-6), 0.0),
                1.0,
            )
            model_used = "regime_heuristic"
    else:
        baseline = _mean(series[-14:] or series)
        spread = _std(series[-14:] or series)
        high_regime_probability = min(
            max((predicted_volatility - baseline) / max(spread * 3, 1e-6), 0.0),
            1.0,
        )
        model_used = "regime_heuristic"

    return {
        "high_volatility_regime_probability": clamp_probability(high_regime_probability),
        "model_used": model_used,
    }


def filter_latent_health_signal(feature_bundle: dict) -> dict:
    frame: pd.DataFrame = feature_bundle["feature_frame"]
    observed = (
        (frame["dex_liquidity_usd"] / frame["dex_liquidity_usd"].max()) * 0.45
        + (frame["unique_addresses"] / frame["unique_addresses"].max()) * 0.25
        + (1.0 - frame["rolling_fee_volatility_7d"].clip(upper=1.0)) * 0.30
    ).fillna(0.0)

    if UnobservedComponents is not None and len(observed) >= 10:
        try:
            model = UnobservedComponents(observed, level="local level")
            fitted = model.fit(disp=False)
            latent_signal = float(fitted.filtered_state[0][-1])
            model_used = "kalman_filter"
        except Exception:
            latent_signal = float(observed.ewm(span=5, adjust=False).mean().iloc[-1])
            model_used = "ewma_filter"
    else:
        latent_signal = float(observed.ewm(span=5, adjust=False).mean().iloc[-1])
        model_used = "ewma_filter"

    return {
        "latent_corridor_health_signal": clamp_probability(latent_signal),
        "model_used": model_used,
    }


def forecast_demand_growth(feature_bundle: dict) -> dict:
    frame: pd.DataFrame = feature_bundle["feature_frame"]
    volume_series = [float(value) for value in frame["volume_usd"].tolist() if pd.notna(value)]

    if ARIMA is not None and len(volume_series) >= 20:
        try:
            model = ARIMA(volume_series, order=(1, 1, 1))
            fitted = model.fit()
            forecast_values = fitted.forecast(steps=30)
            forecast_mean = _mean([float(value) for value in forecast_values.tolist()])
            model_used = "arima_1_1_1"
        except Exception:
            recent = volume_series[-7:]
            baseline = _mean(volume_series[-30:] or volume_series)
            recent_mean = _mean(recent)
            daily_delta = (recent_mean - baseline) / max(len(recent), 1)
            forecast_mean = recent_mean + (daily_delta * 30)
            model_used = "trend_fallback"
    else:
        recent = volume_series[-7:]
        baseline = _mean(volume_series[-30:] or volume_series)
        recent_mean = _mean(recent)
        daily_delta = (recent_mean - baseline) / max(len(recent), 1)
        forecast_mean = recent_mean + (daily_delta * 30)
        model_used = "trend_fallback"

    recent_mean = _mean(volume_series[-7:] or volume_series)
    baseline = _mean(volume_series[-30:] or volume_series)
    growth_rate = ((forecast_mean - baseline) / max(baseline, 1.0)) * 100
    interval_width = max(abs(growth_rate) * 0.35, 4.0)

    return {
        "demand_growth_forecast": round(growth_rate, 2),
        "confidence_interval": {
            "lower": round(growth_rate - interval_width, 2),
            "upper": round(growth_rate + interval_width, 2),
            "confidence_level": 0.95,
        },
        "model_used": model_used,
    }


def run_forecast_models(feature_bundle: dict, volatility_result: dict, liquidity_result: dict) -> dict:
    current_state = feature_bundle["current_state"]
    regime_result = detect_regime_probability(feature_bundle, volatility_result)
    latent_result = filter_latent_health_signal(feature_bundle)
    growth_result = forecast_demand_growth(feature_bundle)

    stability_probability = clamp_probability(
        (volatility_result["corridor_stability_probability"] * 0.55)
        + ((1.0 - regime_result["high_volatility_regime_probability"]) * 0.25)
        + (latent_result["latent_corridor_health_signal"] * 0.20)
    )

    fx_volatility_signal = clamp_probability(float(current_state["fx_volatility_signal"]))
    regulatory_risk_index = clamp_probability(float(current_state["regulatory_risk_index"]))

    return {
        "corridor_stability_probability": stability_probability,
        "high_volatility_regime_probability": regime_result["high_volatility_regime_probability"],
        "latent_corridor_health_signal": latent_result["latent_corridor_health_signal"],
        "fx_volatility_signal": fx_volatility_signal,
        "regulatory_risk_index": regulatory_risk_index,
        "demand_growth_forecast": growth_result["demand_growth_forecast"],
        "demand_growth_confidence_interval": growth_result["confidence_interval"],
        "model_metadata": {
            "volatility_model": volatility_result["model_used"],
            "regime_model": regime_result["model_used"],
            "filter_model": latent_result["model_used"],
            "demand_model": growth_result["model_used"],
        },
    }
