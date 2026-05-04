"""
api/main.py — FastAPI application with CORS, async lifespan, and routing endpoints.

Startup: the readiness surface initializes in an idle state.
Measured refresh is operator-triggered rather than automatic.
"""

import asyncio
import html
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional
from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.responses import HTMLResponse
from fastapi.responses import JSONResponse
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from typing import Literal

from api import cache
from api.corridor_metrics import build_corridor_analytics_response
from api.demo_store import (
    create_scenario,
    get_scenario,
    get_discovery_summary,
    list_discovery_events,
    list_scenarios,
    review_scenario,
    save_discovery_event,
    save_feedback,
)
from api.router import get_landscape, get_demo_presets, get_preview, get_route
from api.simulate import router as simulate_router
from api.x402 import (
    LEGACY_PAYMENT_HEADER,
    LEGACY_PAYMENT_RESPONSE_HEADER,
    PAYMENT_REQUIRED_HEADER,
    PAYMENT_RESPONSE_HEADER,
    PAYMENT_SIGNATURE_HEADER,
    X402_ALLOW_UNVERIFIED_PAYMENTS,
    X402_ENABLED,
    X402_FACILITATOR_URL,
    X402_NETWORK,
    X402_PRICE_USDC,
    build_payment_response_headers,
    extract_payment_header,
    payment_required_response,
    verify_and_settle_payment,
)
from models.response_models import ContextGraphResponseModel
from services.corridor_config import get_config_health, load_corridor_config
from services.solana.api_integration import get_solana_api_state
from services.context_graph import cache as context_graph_cache
from services.context_graph.queries import SUPPORTED_TIME_RANGES
from services.context_graph.service import build_response_payload, resolve_chain
from services.export_receipt import export_decision_receipt
from services.logging_utils import log_event
from services.payroll_demo import (
    build_receipt_context,
    evaluate_payroll_run,
    get_overview,
    get_payroll_run_detail,
    ingest_payroll_file,
    list_exceptions as list_payroll_exceptions,
    list_payroll_runs,
    record_run_decision,
    trigger_run_handoff,
)
from services.query_metrics import get_query_metrics_snapshot
from services.request_context import reset_request_id, set_request_id
from services.runtime_mode import (
    get_runtime_mode,
    get_runtime_mode_label,
    get_runtime_mode_note,
    is_demo_mode,
    is_real_mode,
)
from services.summary_store import init_summary_store
from services.token_registry import DEFAULT_TOKEN, get_supported_tokens, normalize_token

try:
    from forecasting.api import router as forecast_router
except ModuleNotFoundError:
    forecast_router = None

# ── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("sci-agent.main")
UI_DIR = Path(__file__).resolve().parent.parent / "ui"
INDEX_FILE = UI_DIR / "index.html"
APP_VERSION = "5.0.0"
REQUEST_ID_HEADER = "X-Request-Id"

# ── Lifespan (async startup — never blocking) ───────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    CRITICAL: assign task to variable — Python's asyncio holds only WEAK
    references to tasks. asyncio.create_task() with no assignment = task may
    be garbage-collected mid-execution. Cache silently stops updating.
    """
    logger.info("Starting Canopy Route Intelligence stablecoin route advisor")
    load_corridor_config(force=True)
    init_summary_store()
    if is_demo_mode():
        cache.seed_demo_cache()
        context_graph_cache.seed_demo_cache()
        logger.info("Canopy runtime mode=%s", get_runtime_mode_label())
    else:
        cache.initialize_manual_refresh_state()
    yield


# ── App ──────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Canopy Route Intelligence",
    description="Stablecoin route advisor with landscape summary, token-aware routing, and decision artifacts.",
    version=APP_VERSION,
    lifespan=lifespan,
)

# ── CORS (must be added LAST — FastAPI reverse-onion middleware stack) ───────
# Added last = runs outermost, wraps all responses with CORS headers.

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=[
        REQUEST_ID_HEADER,
        PAYMENT_REQUIRED_HEADER,
        PAYMENT_RESPONSE_HEADER,
        LEGACY_PAYMENT_RESPONSE_HEADER,
    ],
)
app.include_router(simulate_router)
if forecast_router is not None:
    app.include_router(forecast_router)


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    request_id = request.headers.get(REQUEST_ID_HEADER) or f"req_{uuid4().hex[:12]}"
    token = set_request_id(request_id)
    started_at = time.perf_counter()
    log_event(
        logger,
        "http.request.started",
        request_id=request_id,
        method=request.method,
        path=request.url.path,
        query=str(request.url.query or ""),
    )
    try:
        response = await call_next(request)
    except Exception as exc:
        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
        log_event(
            logger,
            "http.request.failed",
            request_id=request_id,
            method=request.method,
            path=request.url.path,
            duration_ms=duration_ms,
            error=str(exc),
        )
        reset_request_id(token)
        raise
    finally:
        if "response" in locals():
            duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
            reset_request_id(token)
    response.headers[REQUEST_ID_HEADER] = request_id
    log_event(
        logger,
        "http.request.completed",
        request_id=request_id,
        method=request.method,
        path=request.url.path,
        status_code=response.status_code,
        duration_ms=duration_ms,
    )
    return response


# ── Request Models ──────────────────────────────────────────────────────────


class RouteRequest(BaseModel):
    origin: str = "US"
    destination: str = "BR"
    amount_usdc: float = Field(default=50000, ge=1, le=10_000_000)
    time_sensitivity: str = "standard"
    monthly_volume_usdc: float = Field(default=3_000_000, ge=1, le=1_000_000_000)
    current_rail_fee_pct: float = Field(default=1.2, ge=0, le=100)
    current_rail_settlement_hours: float = Field(default=24, ge=0, le=720)
    current_setup: str = "SWIFT + FX provider + local payout counterparty"
    compliance_sensitivity: str = "medium"
    lens: str = "strategy"
    token: str = DEFAULT_TOKEN
    scenario_id: Optional[int] = None


class DemoFeedbackRequest(RouteRequest):
    feedback_decision: str = "Maybe"
    feedback_reviewers: str = ""
    feedback_notes: str = ""


class PublicRouteRequest(BaseModel):
    amount: float = Field(default=50_000, ge=1, le=10_000_000)
    token: str = "USDC"
    source_chain: str = "Ethereum"
    destination: str = "Philippines"
    corridor_id: Optional[str] = None
    time_range: str = "24h"


class ScenarioCreateRequest(RouteRequest):
    notes: str = ""
    follow_up_requested: bool = False


class ScenarioReviewRequest(BaseModel):
    review_state: str = Field(default="reviewed")
    reviewer: str = ""
    notes: str = ""
    follow_up_requested: bool = False


class DiscoveryEventRequest(BaseModel):
    event_name: str
    corridor_key: str = ""
    corridor_label: str = ""
    token: str = ""
    lens: str = ""
    metadata: dict = Field(default_factory=dict)


class PayrollDecisionRequest(BaseModel):
    action: Literal["APPROVE", "HOLD", "ESCALATE"]
    approver: str = "Operations"
    decision_reason: str = ""
    decision_reason_other: str = ""


class PayrollEvaluationRequest(BaseModel):
    transfer_amount_usd: float = Field(..., ge=1, le=1_000_000_000)
    required_arrival_at: Optional[str] = None
    payroll_currency: Literal["USD", "NGN", "ZAR", "BRL", "MXN", "PHP", "VND"]
    override_buffer_percent: Optional[float] = Field(default=None, ge=0, le=100)


class PayrollDataUploadRequest(BaseModel):
    source_type: Literal["demo", "upload", "api", "sftp", "manual"]
    file_name: str
    content_base64: str


def _find_preset(
    *,
    corridor_id: Optional[str] = None,
    destination: Optional[str] = None,
    allow_fallback: bool = True,
) -> Optional[dict]:
    presets = get_demo_presets()
    if corridor_id:
        corridor_key = corridor_id.strip().lower()
        for preset in presets:
            if corridor_key in {
                str(preset.get("key", "")).lower(),
                str(preset.get("corridor_slug", "")).lower(),
                str(preset.get("label", "")).lower(),
            }:
                return preset
    if destination:
        destination_key = destination.strip().lower()
        for preset in presets:
            if destination_key in {
                str(preset.get("destination_country", "")).lower(),
                str(preset.get("destination", "")).lower(),
            }:
                return preset
    return presets[0] if allow_fallback and presets else None


def _scenario_artifact_from_request(request: RouteRequest) -> Optional[dict]:
    if request.scenario_id is None:
        return None
    return get_scenario(int(request.scenario_id))


def _route_from_request(request: RouteRequest) -> dict:
    return get_route(
        origin=request.origin,
        destination=request.destination,
        amount_usdc=request.amount_usdc,
        time_sensitivity=request.time_sensitivity,
        monthly_volume_usdc=request.monthly_volume_usdc,
        current_rail_fee_pct=request.current_rail_fee_pct,
        current_rail_settlement_hours=request.current_rail_settlement_hours,
        current_setup=request.current_setup,
        compliance_sensitivity=request.compliance_sensitivity,
        lens=request.lens,
        token=request.token,
    )


def _normalized_token_or_400(token: str) -> str:
    try:
        return normalize_token(token)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported token. Supported tokens: {', '.join(get_supported_tokens())}",
        ) from exc


def _build_export_html(route_data: dict, feedback_request: Optional[DemoFeedbackRequest] = None) -> str:
    def esc(value: object) -> str:
        return html.escape(str(value or "—"))

    rails_html = "".join(
        f"""
        <section class="rail {'reference' if rail.get('mode') == 'historical_reference' else ''}">
            <div class="rail-top">
                <h3>{esc(rail.get('rail'))}</h3>
                <span class="pill">{esc(rail.get('status_badge'))}</span>
            </div>
            <p><strong>Mode:</strong> {esc(rail.get('mode', '')).replace('_', ' ')}</p>
            <p><strong>Cost signal:</strong> {esc(rail.get('estimated_fee_label'))}</p>
            <p><strong>Liquidity / comparator:</strong> {esc(rail.get('liquidity_proxy_label'))}</p>
            <p>{esc(rail.get('liquidity_proxy_detail'))}</p>
            <p><strong>Predictability:</strong> {esc(rail.get('settlement_timing'))}</p>
            <p><strong>Evidence confidence:</strong> {esc(rail.get('evidence_confidence_label') or rail.get('confidence_label'))}</p>
            <p><strong>Freshness:</strong> {esc(rail.get('freshness_timestamp') or 'Historical / batched')}</p>
            <p class="muted">{esc(rail.get('note'))}</p>
        </section>
        """
        for rail in route_data.get("rails", [])
    )

    why_html = "".join(
        f"<li>{esc(item)}</li>" for item in route_data.get("why_this_route", [])
    )
    solved_html = "".join(
        f"<li>{esc(item)}</li>" for item in route_data.get("solved_infrastructure", [])
    )
    open_html = "".join(
        f"<li>{esc(item)}</li>" for item in route_data.get("open_questions", [])
    )

    feedback_block = ""
    if feedback_request is not None:
        feedback_block = f"""
        <section class="panel">
            <h2>Demo Notes</h2>
            <div class="meta-grid">
                <div><span class="meta-label">Would this influence a decision?</span><span>{esc(feedback_request.feedback_decision)}</span></div>
                <div><span class="meta-label">Who else should review?</span><span>{esc(feedback_request.feedback_reviewers)}</span></div>
            </div>
            <p>{esc(feedback_request.feedback_notes)}</p>
        </section>
        """

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{esc(route_data.get('corridor'))} Committee Summary</title>
    <style>
        * {{ box-sizing: border-box; }}
        body {{ font-family: Inter, Arial, sans-serif; background: #f4f6fa; color: #1b241f; margin: 0; padding: 28px; }}
        .page {{ max-width: 980px; margin: 0 auto; display: grid; gap: 18px; }}
        .hero, .panel {{ background: #fff; border: 1px solid #d9e2dc; border-radius: 20px; padding: 22px; }}
        .eyebrow {{ font-size: 11px; text-transform: uppercase; letter-spacing: 0.16em; color: #617066; margin-bottom: 8px; }}
        h1 {{ margin: 0 0 10px; font-size: 34px; line-height: 1.05; }}
        h2 {{ margin: 0 0 12px; font-size: 20px; }}
        h3 {{ margin: 0; font-size: 18px; }}
        p, li {{ line-height: 1.6; color: #36433b; }}
        .meta-grid {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 12px; }}
        .meta-grid div {{ background: #f8fafc; border: 1px solid #e4ebe6; border-radius: 16px; padding: 14px; display: grid; gap: 6px; }}
        .meta-label {{ font-size: 10px; text-transform: uppercase; letter-spacing: 0.16em; color: #6d7a72; }}
        .rails {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; }}
        .rail {{ background: #fbfcfd; border: 1px solid #e4ebe6; border-radius: 18px; padding: 16px; }}
        .rail.reference {{ background: #f5f7ff; border-color: #dce3ff; }}
        .rail-top {{ display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 10px; gap: 10px; }}
        .pill {{ font-size: 10px; text-transform: uppercase; letter-spacing: 0.12em; color: #45564c; background: #edf2ef; border-radius: 999px; padding: 6px 8px; }}
        .muted {{ color: #66756b; }}
        ul {{ margin: 0; padding-left: 20px; }}
        .two-col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }}
        .summary {{ white-space: pre-wrap; }}
        .actions {{ display: flex; justify-content: space-between; align-items: center; gap: 12px; color: #66756b; font-size: 12px; }}
        @media print {{ body {{ background: #fff; padding: 0; }} .actions {{ display: none; }} .hero, .panel, .rail {{ box-shadow: none; }} }}
        @media (max-width: 860px) {{ .rails, .meta-grid, .two-col {{ grid-template-columns: 1fr; }} body {{ padding: 16px; }} }}
    </style>
</head>
<body>
    <div class="page">
        <section class="hero">
            <div class="eyebrow">Canopy Decision</div>
            <h1>{esc(route_data.get('corridor'))}</h1>
            <p>{esc(route_data.get('lens_summary') or route_data.get('corridor_note'))}</p>
            <p><strong>Operating model:</strong> {esc(route_data.get('api_workflow_note'))}</p>
            <div class="meta-grid">
                <div><span class="meta-label">Transfer Winner</span><span>{esc(route_data.get('transfer_winner'))}</span></div>
                <div><span class="meta-label">Canopy Recommendation</span><span>{esc(route_data.get('canopy_recommendation') or route_data.get('recommended_rail'))}</span></div>
                <div><span class="meta-label">Expected landed amount</span><span>{esc(route_data.get('expected_landed_amount_label'))}</span></div>
                <div><span class="meta-label">Strategy Score</span><span>{esc(route_data.get('strategy_score_label') or route_data.get('decision_score_label'))}</span></div>
                <div><span class="meta-label">Evidence Confidence</span><span>{esc(route_data.get('evidence_confidence_label') or route_data.get('confidence_label'))}</span></div>
                <div><span class="meta-label">Decision lens</span><span>{esc(route_data.get('active_lens', {}).get('label', 'Strategy'))}</span></div>
                <div><span class="meta-label">Primary question</span><span>{esc(route_data.get('active_lens', {}).get('key_question', ''))}</span></div>
                <div><span class="meta-label">Monthly corridor volume</span><span>{esc(route_data.get('monthly_volume_usdc'))} USDC</span></div>
                <div><span class="meta-label">Current rail assumption</span><span>{esc(route_data.get('scenario', {}).get('baseline_settlement_label'))}</span></div>
            </div>
        </section>

        <section class="panel">
            <h2>Committee Summary</h2>
            <p class="summary">{esc(route_data.get('committee_summary'))}</p>
            <div class="actions">
                <span>{esc(route_data.get('route_mode_note'))}</span>
                <button onclick="window.print()">Print / Save PDF</button>
            </div>
        </section>

        <section class="panel">
            <h2>Rail Comparison</h2>
            <div class="rails">{rails_html}</div>
        </section>

        <section class="panel two-col">
            <div>
                <h2>Why This Route</h2>
                <ul>{why_html}</ul>
            </div>
            <div>
                <h2>Scenario Assumptions</h2>
                <ul>
                    <li>Ticket size: {esc(route_data.get('amount_usdc'))} USDC</li>
                    <li>Monthly volume: {esc(route_data.get('monthly_volume_usdc'))} USDC</li>
                    <li>Current rail fee assumption: {esc(route_data.get('scenario', {}).get('current_rail_fee_pct'))}%</li>
                    <li>Current settlement assumption: {esc(route_data.get('scenario', {}).get('current_rail_settlement_hours'))} hours</li>
                    <li>Current setup: {esc(route_data.get('scenario', {}).get('current_setup'))}</li>
                    <li>Compliance sensitivity: {esc(route_data.get('scenario', {}).get('compliance_sensitivity'))}</li>
                </ul>
            </div>
        </section>

        <section class="panel two-col">
            <div>
                <h2>Solved Infrastructure</h2>
                <ul>{solved_html}</ul>
            </div>
            <div>
                <h2>Open Questions</h2>
                <ul>{open_html}</ul>
            </div>
        </section>

        {feedback_block}
    </div>
</body>
</html>"""


# ── Endpoints ───────────────────────────────────────────────────────────────


@app.get("/health")
async def health():
    """System health check with cache and data status."""
    c = cache.get_cache()
    polygon_state = c["chains"].get("Polygon", {})
    eth_state = c["chains"].get("Ethereum", {})
    polygon_data = polygon_state.get("data", {})
    eth_data = eth_state.get("data", {})
    config_health = get_config_health()

    return JSONResponse(
        content={
            "status": c.get("status", "initializing"),
            "version": APP_VERSION,
            "runtime_mode": get_runtime_mode(),
            "runtime_mode_label": get_runtime_mode_label(),
            "runtime_mode_note": get_runtime_mode_note(),
            "cache_age_seconds": cache.get_cache_age_seconds(),
            "eth_price_usd": c.get("eth_price_usd", 3500),
            "polygon_price_usd": c.get("polygon_price_usd", 0.10),
            "native_prices_live": c.get(
                "native_prices_live",
                c.get("eth_price_live", False),
            ),
            "eth_price_live": c.get("eth_price_live", False),
            "polygon_transfer_count_24h": (
                polygon_data.get("transfer_count") if polygon_data else None
            ),
            "eth_transfer_count_24h": (
                eth_data.get("transfer_count") if eth_data else None
            ),
            "chains": {
                "Polygon": {
                    "status": polygon_state.get("status", "initializing"),
                    "freshness_level": polygon_state.get("freshness_level", "unknown"),
                    "cache_age_seconds": polygon_state.get("age_seconds"),
                    "last_success_at": polygon_state.get("last_success_at"),
                    "last_attempt_at": polygon_state.get("last_attempt_at"),
                    "last_error": polygon_state.get("last_error"),
                },
                "Ethereum": {
                    "status": eth_state.get("status", "initializing"),
                    "freshness_level": eth_state.get("freshness_level", "unknown"),
                    "cache_age_seconds": eth_state.get("age_seconds"),
                    "last_success_at": eth_state.get("last_success_at"),
                    "last_attempt_at": eth_state.get("last_attempt_at"),
                    "last_error": eth_state.get("last_error"),
                },
                "Solana": get_solana_api_state().to_chain_health_dict(),
            },
            "corridor_config_status": config_health.get("status"),
            "corridor_config_source": config_health.get("source"),
            "corridor_config_last_loaded_at": config_health.get("last_loaded_at"),
            "corridor_config_last_error": config_health.get("last_error"),
            "corridor_config_refresh_seconds": config_health.get("refresh_seconds"),
            "last_error": c.get("last_error"),
            "poll_count": c.get("poll_count", 0),
            "context_graph_status": context_graph_cache.get_cache().get("status", "initializing"),
            "context_graph_age_seconds": context_graph_cache.get_cache_age_seconds(),
            "x402_enabled": X402_ENABLED,
            "x402_price_usdc": X402_PRICE_USDC,
            "x402_network": X402_NETWORK,
            "x402_facilitator_url": X402_FACILITATOR_URL,
            "x402_allow_unverified_payments": X402_ALLOW_UNVERIFIED_PAYMENTS,
        }
    )


@app.get("/")
async def index():
    """Serve the demo UI from the same service as the API."""
    return FileResponse(INDEX_FILE)


@app.get("/v1/client-config")
async def client_config():
    """Return non-secret runtime config needed by the browser UI."""
    return JSONResponse(
        content={
            "mapbox_token": os.getenv("VITE_MAPBOX_TOKEN", ""),
            "mapbox_token_present": bool(os.getenv("VITE_MAPBOX_TOKEN", "")),
            "app_version": app.version,
            "runtime_mode": get_runtime_mode(),
            "runtime_mode_label": get_runtime_mode_label(),
            "runtime_mode_note": get_runtime_mode_note(),
            "live_data_enabled": is_real_mode(),
            "data_sources": (
                ["BigQuery measured summaries", "Coinbase native prices"]
                if is_real_mode()
                else ["Seeded demo route snapshots", "Fallback native prices"]
            ),
            "default_token": DEFAULT_TOKEN,
            "supported_tokens": get_supported_tokens(),
        }
    )


@app.get("/v1/demo/presets")
async def demo_presets():
    """Return scenario presets for the Canopy V5 workspace."""
    return JSONResponse(content={"presets": get_demo_presets()})


@app.get("/v1/landscape")
async def landscape(
    corridor_id: str,
    amount: float = 50_000,
    lens: str = "strategy",
):
    preset = _find_preset(corridor_id=corridor_id, allow_fallback=False)
    if preset is None:
        raise HTTPException(status_code=404, detail="Corridor not found")

    payload = get_landscape(
        origin=preset["origin"],
        destination=preset["destination"],
        amount_usdc=amount,
        time_sensitivity="standard",
        monthly_volume_usdc=float(preset.get("default_monthly_volume_usdc", 1_000_000)),
        current_rail_fee_pct=float(preset.get("default_baseline_fee_pct", 1.5)),
        current_rail_settlement_hours=float(preset.get("default_baseline_settlement_hours", 24)),
        current_setup=preset.get("default_current_setup", ""),
        compliance_sensitivity="medium",
        lens=lens,
    )
    return JSONResponse(content=payload)


@app.post("/v1/demo/feedback")
async def demo_feedback(request: DemoFeedbackRequest):
    """Persist scenario assumptions and live feedback for the sample merchant demo."""
    request.token = _normalized_token_or_400(request.token)
    route_result = _route_from_request(request)
    saved = save_feedback(
        corridor_key=route_result.get("corridor_key", ""),
        corridor_label=route_result.get("corridor", ""),
        recommended_rail=route_result.get("recommended_rail", ""),
        scenario_payload=request.model_dump(),
        route_payload=route_result,
        feedback_decision=request.feedback_decision,
        feedback_reviewers=request.feedback_reviewers,
        feedback_notes=request.feedback_notes,
    )
    return JSONResponse(
        content={
            "status": "saved",
            "request_id": route_result.get("request_id"),
            "decision_id": route_result.get("decision_id"),
            "feedback_id": saved["id"],
            "created_at": saved["created_at"],
            "corridor": route_result.get("corridor"),
            "recommended_rail": route_result.get("recommended_rail"),
        }
    )


@app.post("/v1/demo/export")
async def demo_export(request: DemoFeedbackRequest):
    """Return a printable one-page committee summary for the current scenario."""
    request.token = _normalized_token_or_400(request.token)
    route_result = _route_from_request(request)
    log_event(
        logger,
        "route.export.generated",
        request_id=route_result.get("request_id"),
        decision_id=route_result.get("decision_id"),
        corridor=route_result.get("corridor"),
        lens=route_result.get("lens"),
    )
    return HTMLResponse(content=_build_export_html(route_result, request))


@app.post("/v1/demo/decision-receipt")
async def demo_decision_receipt(request: RouteRequest):
    """Return a plain-text decision receipt for the current scenario."""
    request.token = _normalized_token_or_400(request.token)
    route_result = _route_from_request(request)
    receipt = export_decision_receipt(
        corridor=route_result.get("corridor", f"{request.origin} -> {request.destination}"),
        lens=route_result.get("active_lens", {}).get("label", request.lens),
        route_result=route_result,
        scenario_artifact=_scenario_artifact_from_request(request),
    )
    log_event(
        logger,
        "route.receipt.generated",
        request_id=route_result.get("request_id"),
        decision_id=route_result.get("decision_id"),
        corridor=route_result.get("corridor"),
        lens=route_result.get("lens"),
    )
    return PlainTextResponse(
        content=receipt,
        headers={
            "Content-Disposition": (
                "attachment; "
                f"filename=\"{route_result.get('corridor_slug', 'corridor')}-{request.lens}-decision-receipt.txt\""
            )
        },
    )


@app.post("/v1/route")
async def route(
    request: RouteRequest,
    payment_signature: Optional[str] = Header(default=None, alias=PAYMENT_SIGNATURE_HEADER),
    legacy_payment: Optional[str] = Header(default=None, alias=LEGACY_PAYMENT_HEADER),
):
    """
    Full routing recommendation.

    NEVER returns 503. If cache is empty, returns bootstrap data.
    The UI should always show something.
    """
    payment_header, _header_name = extract_payment_header(payment_signature, legacy_payment)

    if X402_ENABLED and not payment_header:
        return payment_required_response("POST /v1/route")

    payment_response_headers = {}
    if X402_ENABLED and payment_header:
        payment_ok, settle_payload, error_message = verify_and_settle_payment(
            payment_header, "POST /v1/route"
        )
        if not payment_ok:
            return JSONResponse(
                status_code=402,
                content={"error": "payment_invalid", "message": error_message},
            )
        if settle_payload:
            payment_response_headers = build_payment_response_headers(settle_payload)

    request.token = _normalized_token_or_400(request.token)
    result = _route_from_request(request)
    log_event(
        logger,
        "route.api.response",
        request_id=result.get("request_id"),
        decision_id=result.get("decision_id"),
        corridor=result.get("corridor"),
        recommendation=result.get("recommended_rail"),
        global_data_status=result.get("global_data_status"),
    )
    return JSONResponse(content=result, headers=payment_response_headers)


@app.post("/v1/scenarios")
async def scenarios_create(request: ScenarioCreateRequest):
    request.token = _normalized_token_or_400(request.token)
    route_result = _route_from_request(request)
    saved = create_scenario(
        corridor_key=route_result.get("corridor_key", ""),
        corridor_label=route_result.get("corridor", ""),
        token=route_result.get("token", DEFAULT_TOKEN),
        recommended_rail=route_result.get("recommended_rail", ""),
        scenario_payload={
            **request.model_dump(),
            "notes": request.notes,
            "follow_up_requested": request.follow_up_requested,
        },
        review_notes=request.notes,
        route_payload=route_result,
        follow_up_requested=request.follow_up_requested,
    )
    return JSONResponse(
        content={
            "status": "saved",
            "scenario": get_scenario(saved["id"]) or saved,
            "route": route_result,
        }
    )


@app.get("/v1/scenarios")
async def scenarios_list(limit: int = 10, corridor_key: Optional[str] = None):
    return JSONResponse(content={"items": list_scenarios(limit=limit, corridor_key=corridor_key)})


@app.get("/v1/scenarios/{scenario_id}")
async def scenarios_get(scenario_id: int):
    scenario = get_scenario(scenario_id)
    if scenario is None:
        raise HTTPException(status_code=404, detail="Scenario not found")
    return JSONResponse(content=scenario)


@app.post("/v1/scenarios/{scenario_id}/review")
async def scenarios_review(scenario_id: int, request: ScenarioReviewRequest):
    scenario = review_scenario(
        scenario_id,
        review_state=request.review_state,
        reviewer=request.reviewer,
        review_notes=request.notes,
        follow_up_requested=request.follow_up_requested,
    )
    if scenario is None:
        raise HTTPException(status_code=404, detail="Scenario not found")
    return JSONResponse(content={"status": "reviewed", "scenario": scenario})


@app.post("/v1/discovery/events")
async def discovery_event(request: DiscoveryEventRequest):
    token = ""
    if request.token:
        token = _normalized_token_or_400(request.token)
    saved = save_discovery_event(
        event_name=request.event_name,
        corridor_key=request.corridor_key,
        corridor_label=request.corridor_label,
        token=token,
        lens=request.lens,
        metadata=request.metadata,
    )
    return JSONResponse(content={"status": "saved", "event": saved})


@app.get("/v1/discovery/events")
async def discovery_events(limit: int = 20, corridor_key: Optional[str] = None):
    return JSONResponse(content={"items": list_discovery_events(limit=limit, corridor_key=corridor_key)})


@app.get("/v1/discovery/summary")
async def discovery_summary(corridor_key: Optional[str] = None):
    return JSONResponse(content=get_discovery_summary(corridor_key=corridor_key))


@app.get("/v1/system/bigquery-metrics")
async def bigquery_metrics():
    return JSONResponse(content=get_query_metrics_snapshot())


@app.get("/v1/solana/health")
async def solana_health():
    """
    Solana data layer health and freshness state.

    Returns the current FreshnessMonitor state, last ingestion run metrics,
    and validation gate result. freshness_state is one of:
      "fresh"       — data is current (within SOLANA_FRESHNESS_THRESHOLD_SECONDS)
      "stale"       — ingestion lag exceeds threshold; data shown with warning
      "unavailable" — no Solana data has been ingested yet

    The ``chain_health`` sub-key mirrors the Polygon/Ethereum shape in /health
    so the dashboard can render a Solana row without bespoke handling.
    """
    state = get_solana_api_state()
    return JSONResponse(content={
        **state.to_dict(),
        "chain": "Solana",
        "chain_health": state.to_chain_health_dict(),
        "scope_disclaimer": (
            "Solana data reflects observed SPL token movements within "
            "configured watched sources and measured windows."
        ),
    })


@app.get("/v1/overview")
async def payroll_overview():
    return JSONResponse(content=get_overview())


@app.get("/v1/measured-refresh-status")
async def measured_refresh_status():
    return JSONResponse(content=cache.get_refresh_state())


@app.post("/v1/measured-refresh")
async def measured_refresh():
    payload = cache.trigger_manual_refresh()
    log_event(
        logger,
        "payroll.measured_refresh.triggered",
        refresh_status=payload.get("status"),
        last_measured_refresh=payload.get("last_measured_refresh"),
    )
    return JSONResponse(content=payload)


@app.get("/v1/payroll-runs")
async def payroll_runs():
    return JSONResponse(content={"items": list_payroll_runs()})


@app.get("/v1/payroll-runs/{run_id}")
async def payroll_run_detail(run_id: str):
    try:
        return JSONResponse(content=get_payroll_run_detail(run_id))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Payroll run not found") from exc


@app.post("/v1/payroll-runs/{run_id}/evaluate")
async def payroll_evaluate_run(run_id: str, request: PayrollEvaluationRequest):
    try:
        payload = evaluate_payroll_run(
            run_id,
            transfer_amount_usd=request.transfer_amount_usd,
            required_arrival_at=request.required_arrival_at,
            payroll_currency=request.payroll_currency,
            override_buffer_percent=request.override_buffer_percent,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Payroll run not found") from exc
    log_event(
        logger,
        "payroll.run.evaluated",
        payroll_run_id=run_id,
        transfer_amount_usd=request.transfer_amount_usd,
        payroll_currency=request.payroll_currency,
        required_arrival_at=request.required_arrival_at,
    )
    return JSONResponse(content=payload)


@app.post("/v1/payroll-runs/{run_id}/payroll-data-upload")
async def payroll_data_upload(run_id: str, request: PayrollDataUploadRequest):
    try:
        payload = ingest_payroll_file(
            run_id,
            source_type=request.source_type,
            file_name=request.file_name,
            content_base64=request.content_base64,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Payroll run not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    log_event(
        logger,
        "payroll.data_source.uploaded",
        payroll_run_id=run_id,
        source_type=request.source_type,
        file_name=request.file_name,
        record_count=payload["snapshot"].get("record_count"),
        beneficiary_change_count=payload["snapshot"].get("beneficiary_change_count"),
    )
    return JSONResponse(content=payload)


@app.get("/v1/exceptions")
async def payroll_exceptions(run_id: Optional[str] = None):
    try:
        return JSONResponse(content={"items": list_payroll_exceptions(run_id=run_id)})
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Payroll run not found") from exc


@app.post("/v1/payroll-runs/{run_id}/decision")
async def payroll_record_decision(run_id: str, request: PayrollDecisionRequest):
    try:
        payload = record_run_decision(
            run_id,
            action=request.action,
            approver=request.approver,
            decision_reason=request.decision_reason,
            decision_reason_other=request.decision_reason_other,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Payroll run not found") from exc
    log_event(
        logger,
        "payroll.decision.recorded",
        payroll_run_id=run_id,
        action=request.action,
        approver=request.approver,
    )
    return JSONResponse(content=payload)


@app.post("/v1/payroll-runs/{run_id}/handoff")
async def payroll_trigger_handoff(run_id: str):
    try:
        payload = trigger_run_handoff(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Payroll run not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    log_event(
        logger,
        "payroll.handoff.triggered",
        payroll_run_id=run_id,
        handoff_status=payload["handoff"]["status"],
    )
    return JSONResponse(content=payload)


def _payroll_receipt_response(run_id: str) -> PlainTextResponse:
    detail = get_payroll_run_detail(run_id)
    receipt = export_decision_receipt(
        corridor=detail["corridor"],
        lens="Payroll Readiness",
        route_result=detail["route_recommendation"]["route_payload"],
        payroll_context=build_receipt_context(run_id),
    )
    return PlainTextResponse(
        content=receipt,
        headers={
            "Content-Disposition": f"attachment; filename=\"{run_id}-decision-receipt.txt\""
        },
    )


@app.get("/v1/payroll-runs/{run_id}/decision-receipt")
async def payroll_decision_receipt_get(run_id: str):
    try:
        return _payroll_receipt_response(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Payroll run not found") from exc


@app.post("/v1/payroll-runs/{run_id}/decision-receipt")
async def payroll_decision_receipt_post(run_id: str):
    try:
        return _payroll_receipt_response(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Payroll run not found") from exc


@app.get("/corridor/analytics")
async def corridor_analytics(
    corridor_id: str,
    token: str = "USDC",
    time_range: str = "24h",
):
    preset = _find_preset(corridor_id=corridor_id)
    if preset is None:
        raise HTTPException(status_code=404, detail="Corridor not found")

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
        token=_normalized_token_or_400(token),
    )
    response = build_corridor_analytics_response(route_result, time_range=time_range)
    response["token"] = token
    response["request_id"] = route_result.get("request_id")
    response["decision_id"] = route_result.get("decision_id")
    response["global_data_status"] = route_result.get("global_data_status")
    log_event(
        logger,
        "corridor.analytics.generated",
        request_id=route_result.get("request_id"),
        decision_id=route_result.get("decision_id"),
        corridor=route_result.get("corridor"),
        corridor_id=corridor_id,
        time_range=time_range,
        best_route=response.get("best_route"),
    )
    return JSONResponse(content=response)


@app.get("/corridor/{corridor_id}/graph", response_model=ContextGraphResponseModel)
async def corridor_graph(
    corridor_id: str,
    token: str = "USDC",
    time_range: str = "24h",
    chain: str = "auto",
):
    normalized_token = _normalized_token_or_400(token)
    if normalized_token != "USDC":
        raise HTTPException(status_code=400, detail="Only USDC is supported in the current Context Graph Lite build")
    if time_range.lower() not in SUPPORTED_TIME_RANGES:
        raise HTTPException(status_code=400, detail="Unsupported time range")

    preset = _find_preset(corridor_id=corridor_id, allow_fallback=False)
    if preset is None:
        raise HTTPException(status_code=404, detail="Corridor not found")

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
        token=normalized_token,
    )
    selected_chain = resolve_chain(chain, route_result)
    if selected_chain not in {"Ethereum", "Polygon"}:
        raise HTTPException(status_code=400, detail="Only Ethereum and Polygon are supported in the current Context Graph Lite build")

    snapshot, resolved_time_range = context_graph_cache.get_best_snapshot(
        selected_chain,
        token=token.upper(),
        requested_time_range=time_range.lower(),
    )
    log_event(
        logger,
        "corridor.graph.generated",
        request_id=route_result.get("request_id"),
        decision_id=route_result.get("decision_id"),
        corridor=route_result.get("corridor"),
        corridor_id=corridor_id,
        chain=selected_chain,
        requested_time_range=time_range.lower(),
        resolved_time_range=resolved_time_range,
    )
    return build_response_payload(
        corridor_id=preset.get("corridor_slug", corridor_id),
        route_result=route_result,
        chain=selected_chain,
        token=normalized_token,
        requested_time_range=time_range.lower(),
        resolved_time_range=resolved_time_range,
        snapshot=snapshot,
        graph_cache_status=context_graph_cache.get_cache().get("status", "initializing"),
        graph_cache_age_seconds=context_graph_cache.get_cache_age_seconds(),
    )


@app.post("/route")
async def public_route(request: PublicRouteRequest):
    preset = _find_preset(corridor_id=request.corridor_id, destination=request.destination)
    if preset is None:
        raise HTTPException(status_code=404, detail="Corridor not found")
    normalized_token = _normalized_token_or_400(request.token)

    route_result = get_route(
        origin=preset["origin"],
        destination=preset["destination"],
        amount_usdc=request.amount,
        time_sensitivity="standard",
        monthly_volume_usdc=float(preset.get("default_monthly_volume_usdc", 1_000_000)),
        current_rail_fee_pct=float(preset.get("default_baseline_fee_pct", 1.5)),
        current_rail_settlement_hours=float(preset.get("default_baseline_settlement_hours", 24)),
        current_setup=preset.get("default_current_setup", ""),
        compliance_sensitivity="medium",
        lens="strategy",
        token=normalized_token,
    )
    return JSONResponse(
        content={
            "request_id": route_result.get("request_id"),
            "decision_id": route_result.get("decision_id"),
            "corridor": route_result.get("corridor"),
            "token": route_result.get("token"),
            "best_route": route_result.get("recommended_rail"),
            "cost": route_result.get("expected_fee_usd"),
            "liquidity_score": route_result.get("liquidity_score"),
            "trust_score": route_result.get("trust_score_v4"),
            "route_score": route_result.get("route_score"),
            "status": route_result.get("status"),
            "global_data_status": route_result.get("global_data_status"),
            "coverage_state": route_result.get("coverage_state"),
            "corridor_best_supported": route_result.get("corridor_best_supported"),
            "evidence_packet": route_result.get("evidence_packet"),
            "data_health_summary": route_result.get("data_health_summary"),
            "flags": route_result.get("adversarial_flags", []),
            "source_chain": request.source_chain,
            "destination": request.destination,
            "corridor_analytics": route_result.get("corridor_analytics"),
        }
    )


@app.get("/v1/preview/{origin}/{destination}")
async def preview(origin: str, destination: str):
    """
    Free preview endpoint.

    Returns a directionally useful recommendation without the premium route
    details such as confidence and full fee comparison.
    """
    result = get_preview(origin=origin.upper(), destination=destination.upper())
    return JSONResponse(content=result)


@app.get("/v1/corridor/{origin}/{destination}")
async def corridor(origin: str, destination: str):
    """
    Quick corridor lookup with defaults ($50,000, standard).
    Satisfies the "dataset export" ask from Google reviewers.
    """
    result = get_route(
        origin=origin.upper(),
        destination=destination.upper(),
        amount_usdc=50_000,
        time_sensitivity="standard",
        token=DEFAULT_TOKEN,
    )
    return JSONResponse(content=result)
