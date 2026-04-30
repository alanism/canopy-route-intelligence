"""Simulation API for Canopy execution quotes."""

from __future__ import annotations

from uuid import uuid4

from fastapi import APIRouter, HTTPException

from models.request_models import SimulateRequest
from models.response_models import SimulationResponseModel
from services.quote_engine import generate_execution_quote
from services.risk_engine import evaluate_risk_profile
from services.routing_engine import select_best_execution_route
from services.state_mirror import get_state_snapshot

router = APIRouter(tags=["simulation"])


async def run_simulation(request: SimulateRequest) -> SimulationResponseModel:
    snapshot = get_state_snapshot()
    route_result = select_best_execution_route(request, snapshot)
    risk_profile = evaluate_risk_profile(
        snapshot=snapshot,
        route_result=route_result,
        slippage_tolerance=request.slippage_tolerance,
    )
    quote = generate_execution_quote(
        request=request,
        route_result=route_result,
        risk_profile=risk_profile,
        snapshot=snapshot,
    )

    return SimulationResponseModel(
        simulation_id=f"sim_{uuid4().hex[:10]}",
        state_snapshot_id=snapshot["snapshot_id"],
        quote=quote,
        execution_plan={
            "total_received": route_result["total_received"],
            "total_fees_usd": route_result["total_fees_usd"],
            "estimated_time_seconds": route_result["estimated_time_seconds"],
            "settlement_time_confidence": route_result["settlement_time_confidence"],
            "settlement_range": route_result["settlement_range"],
            "algorithm_used": route_result["algorithm_used"],
            "route": route_result["route"],
            "steps": route_result["steps"],
        },
        risk_profile=risk_profile,
        data_freshness={
            "gas_age_sec": snapshot["data_freshness"]["gas_age_sec"],
            "pool_age_sec": snapshot["data_freshness"]["pool_age_sec"],
            "snapshot_age_sec": snapshot["data_freshness"]["snapshot_age_sec"],
            "snapshot_expires_in_sec": snapshot["data_freshness"]["snapshot_expires_in_sec"],
            "warnings": list(snapshot.get("warnings", [])),
        },
    )


@router.post("/v1/simulate", response_model=SimulationResponseModel)
async def simulate_transfer(request: SimulateRequest) -> SimulationResponseModel:
    if request.token.upper() != "USDC":
        raise HTTPException(
            status_code=400,
            detail="Simulation is currently limited to USDC while Canopy V5 validates multi-token route advising first.",
        )
    return await run_simulation(request)
