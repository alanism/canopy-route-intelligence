"""Forecasting API for probabilistic corridor health signals."""

from __future__ import annotations

from typing import Dict, List, Union

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from pydantic import ConfigDict

from forecasting.features import build_feature_bundle
from forecasting.gatekeeper import apply_risk_gatekeeper
from forecasting.liquidity import calculate_liquidity_metrics
from forecasting.models import run_forecast_models
from forecasting.scoring import compute_corridor_health_score
from forecasting.volatility import forecast_corridor_volatility

router = APIRouter(tags=["forecasting"])


class ConfidenceIntervalModel(BaseModel):
    lower: float
    upper: float
    confidence_level: float


class ForecastResponseModel(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    corridor_id: str
    corridor_label: str
    probabilistic_signal: str = "Forecast Models"
    corridor_stability_probability: float
    liquidity_shock_risk: float
    transfer_slippage_probability: float
    fx_volatility_signal: float
    regulatory_risk_index: float
    demand_growth_forecast: float
    corridor_health_score: float
    confidence_intervals: Dict[str, ConfidenceIntervalModel]
    kill_switch_triggered: bool
    kill_switches: List[str] = Field(default_factory=list)
    alerts: List[str] = Field(default_factory=list)
    status: str
    model_metadata: Dict[str, str] = Field(default_factory=dict)
    score_breakdown: Dict[str, float] = Field(default_factory=dict)
    stationarity: Dict[str, dict] = Field(default_factory=dict)
    forecast_freshness: Dict[str, Union[str, int]] = Field(default_factory=dict)


def run_corridor_forecast(corridor_id: str) -> ForecastResponseModel:
    feature_bundle = build_feature_bundle(corridor_id)
    volatility_result = forecast_corridor_volatility(feature_bundle)
    liquidity_result = calculate_liquidity_metrics(feature_bundle, volatility_result)
    model_result = run_forecast_models(feature_bundle, volatility_result, liquidity_result)

    combined_signals = {
        **volatility_result,
        **liquidity_result,
        **model_result,
    }
    score_result = compute_corridor_health_score(combined_signals)
    gated_result = apply_risk_gatekeeper(
        feature_bundle,
        {**combined_signals, **score_result},
    )

    demand_forecast = float(model_result["demand_growth_forecast"])
    stability_probability = float(model_result["corridor_stability_probability"])
    slippage_probability = float(liquidity_result["transfer_slippage_probability"])

    confidence_intervals = {
        "corridor_stability_probability": ConfidenceIntervalModel(
            lower=round(max(stability_probability - 0.12, 0.0), 4),
            upper=round(min(stability_probability + 0.11, 1.0), 4),
            confidence_level=0.95,
        ),
        "demand_growth_forecast": ConfidenceIntervalModel(**model_result["demand_growth_confidence_interval"]),
        "transfer_slippage_probability": ConfidenceIntervalModel(
            lower=round(max(slippage_probability - 0.09, 0.0), 4),
            upper=round(min(slippage_probability + 0.12, 1.0), 4),
            confidence_level=0.95,
        ),
    }

    return ForecastResponseModel(
        corridor_id=feature_bundle["corridor_id"],
        corridor_label=feature_bundle["corridor_label"],
        corridor_stability_probability=stability_probability,
        liquidity_shock_risk=float(liquidity_result["liquidity_shock_risk"]),
        transfer_slippage_probability=slippage_probability,
        fx_volatility_signal=float(model_result["fx_volatility_signal"]),
        regulatory_risk_index=float(model_result["regulatory_risk_index"]),
        demand_growth_forecast=demand_forecast,
        corridor_health_score=float(gated_result["corridor_health_score"]),
        confidence_intervals=confidence_intervals,
        kill_switch_triggered=bool(gated_result["kill_switch_triggered"]),
        kill_switches=list(gated_result["kill_switches"]),
        alerts=list(gated_result["alerts"]),
        status=str(gated_result["status"]),
        model_metadata={
            **model_result["model_metadata"],
            "forecast_layer": "independent_from_deterministic_routing",
        },
        score_breakdown=score_result["score_breakdown"],
        stationarity=feature_bundle["stationarity"],
        forecast_freshness=feature_bundle["freshness"],
    )


@router.get("/corridor/{corridor_id}/forecast", response_model=ForecastResponseModel)
async def corridor_forecast(
    corridor_id: str,
    probabilistic: bool = Query(default=True, description="Forecast layer only, never alters routing."),
) -> ForecastResponseModel:
    if not probabilistic:
        raise HTTPException(status_code=400, detail="Forecast endpoint only returns probabilistic signals")
    try:
        return run_corridor_forecast(corridor_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
