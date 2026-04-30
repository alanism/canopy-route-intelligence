"""Volatility models for Canopy corridor forecasting."""

from __future__ import annotations

from typing import Dict, List

import pandas as pd

from forecasting.features import clamp_probability

try:
    from arch import arch_model
except ImportError:  # pragma: no cover - optional dependency
    arch_model = None


def _std(values: List[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean_value = sum(values) / len(values)
    variance = sum((value - mean_value) ** 2 for value in values) / (len(values) - 1)
    return variance ** 0.5


def forecast_corridor_volatility(feature_bundle: dict) -> dict:
    frame: pd.DataFrame = feature_bundle["feature_frame"]
    composite_returns = (
        (frame["volume_log_return"] * 0.45)
        + (frame["fee_log_return"] * 0.35)
        + (frame["bridge_log_return"] * 0.20)
    ).dropna()
    scaled_returns = [float(value) * 100 for value in composite_returns.tolist() if pd.notna(value)]

    if arch_model is not None and len(scaled_returns) >= 12:
        try:
            model = arch_model(scaled_returns, vol="Garch", p=1, q=1, dist="normal")
            fitted = model.fit(disp="off")
            forecast = fitted.forecast(horizon=1)
            predicted_volatility = float((forecast.variance.values[-1, :][0]) ** 0.5) / 100
            model_used = "garch_1_1"
        except Exception:
            trailing_window = scaled_returns[-14:] if len(scaled_returns) >= 14 else scaled_returns
            predicted_volatility = _std(trailing_window) / 100
            model_used = "ewma_fallback"
    else:
        trailing_window = scaled_returns[-14:] if len(scaled_returns) >= 14 else scaled_returns
        predicted_volatility = _std(trailing_window) / 100
        model_used = "ewma_fallback"

    trailing_30d = [float(value) for value in frame["rolling_volatility_30d"].tail(30).tolist() if pd.notna(value)]
    baseline_volatility = sum(trailing_30d) / max(len(trailing_30d), 1)
    volatility_std = _std(trailing_30d)
    sigma_multiplier = (
        (predicted_volatility - baseline_volatility) / volatility_std
        if volatility_std > 0
        else 0.0
    )
    stability_probability = clamp_probability(1.0 - min(predicted_volatility / 0.08, 0.95))

    return {
        "predicted_corridor_volatility": round(predicted_volatility, 6),
        "baseline_volatility": round(baseline_volatility, 6),
        "volatility_sigma_multiplier": round(sigma_multiplier, 4),
        "corridor_stability_probability": stability_probability,
        "model_used": model_used,
    }
