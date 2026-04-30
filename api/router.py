"""
api/router.py — Corridor decision logic for the sample merchant demo workspace.

The router reads cached rail data, combines it with corridor assumptions and a
light interpretation layer, then returns a decision-oriented payload for the UI.
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from uuid import uuid4

from api.cache import CRITICAL_SECONDS, FRESH_SECONDS, get_cache, get_cache_age_seconds
from api.corridor_metrics import attach_corridor_analytics
from services.corridor_config import (
    get_corridor_or_default,
    get_corridors,
    get_stellar_reference,
)
from services.logging_utils import log_event
from services.request_context import get_request_id
from services.corridor_analytics import build_rail_corridor_metrics
from services.token_registry import (
    COVERAGE_ACTIVE,
    COVERAGE_LIMITED,
    DEFAULT_TOKEN,
    EVIDENCE_LIVE_MEASURED,
    build_metric_evidence,
    get_supported_tokens,
    get_token_coverage_state,
    is_active_coverage,
    normalize_token,
)
from services.transfer_math import build_transfer_math

DEFAULT_CORRIDOR_NOTE = "Cross-border stablecoin transfer."
DEFAULT_LENS = "strategy"
API_WORKFLOW_NOTE = (
    "Canopy is not moving the money. It is telling you which road looks open."
)
logger = logging.getLogger("sci-agent.router")

LENS_CONFIGS = {
    "strategy": {
        "key": "strategy",
        "label": "Strategy",
        "persona": "Product / corridor launch decision makers",
        "key_question": "Should we launch or deepen this corridor?",
        "description": (
            "Emphasizes corridor viability, ecosystem maturity, and the recommendation "
            "narrative for internal launch decisions."
        ),
        "focus_areas": [
            "Corridor viability",
            "Ecosystem maturity",
            "Recommendation narrative",
        ],
        "comparison_note": "This lens frames the corridor as a launch or deepen decision.",
    },
    "treasury": {
        "key": "treasury",
        "label": "Treasury",
        "persona": "Treasury / liquidity operators",
        "key_question": "Can this rail reliably move our transaction size?",
        "description": (
            "Emphasizes liquidity proxy, transfer continuity, and settlement predictability "
            "for treasury and operations teams."
        ),
        "focus_areas": [
            "Liquidity proxy",
            "Transfer continuity",
            "Settlement predictability",
        ],
        "comparison_note": "This lens frames the corridor as a settlement and liquidity decision.",
    },
    "risk": {
        "key": "risk",
        "label": "Risk",
        "persona": "Compliance / legal reviewers",
        "key_question": "What risks remain unresolved?",
        "description": (
            "Emphasizes infrastructure maturity, regulatory exposure, and unresolved diligence "
            "items for internal review."
        ),
        "focus_areas": [
            "Infrastructure maturity",
            "Regulatory exposure",
            "Open diligence items",
        ],
        "comparison_note": "This lens frames the corridor as an unresolved-risk review.",
    },
    "expansion": {
        "key": "expansion",
        "label": "Expansion",
        "persona": "Growth / market expansion teams",
        "key_question": "Which corridors should we expand next?",
        "description": (
            "Emphasizes corridor comparison, market readiness, and ecosystem support across "
            "the sample merchant demo shortlist."
        ),
        "focus_areas": [
            "Corridor comparison",
            "Market readiness",
            "Ecosystem support",
        ],
        "comparison_note": (
            "This lens compares the demo corridors using each corridor's default sample merchant "
            "assumptions rather than the currently selected scenario."
        ),
    },
}

DEMO_CORRIDORS = {
    ("US", "MX"): {
        "key": "US-MX",
        "label": "US -> Mexico",
        "corridor_slug": "us-mexico",
        "source_country": "US",
        "destination_country": "Mexico",
        "destination_city": "Mexico City",
        "map_viewport": {
            "lat": 19.432608,
            "lon": -99.133209,
            "zoom": 10.9,
            "bearing": -6,
            "pitch": 0,
        },
        "corridor_note": (
            "This is the benchmark corridor. Huge remittance volume. Mature payout expectations. A very fair place to ask whether stablecoin rails are actually better."
        ),
        "default_amount_usdc": 10_000,
        "default_monthly_volume_usdc": 3_000_000,
        "default_baseline_fee_pct": 0.95,
        "default_baseline_settlement_hours": 12,
        "default_current_setup": "US funding counterparty + FX corridor counterparty + Mexico payout network",
        "polygon_maturity": "High",
        "stellar_maturity": "Medium",
        "market_readiness": "High",
        "ecosystem_support": "Strong",
        "regulatory_exposure": "Moderate",
        "launch_readiness": "Ready for serious demo evaluation",
        "launch_readiness_score": 0.88,
        "rail_route_fit": {"Polygon": 0.91, "Ethereum": 0.76, "Stellar": 0.68},
        "solved_infrastructure": [
            "Polygon: low-cost stablecoin settlement makes the competitive case easier to test in a corridor with real payment volume.",
            "Ethereum: trusted treasury and custody path if operator comfort matters more than raw cost.",
            "Stellar: still useful as a payments-native comparator, especially when the conversation turns to remittance rails.",
        ],
        "open_questions": [
            "Which Mexico payout counterparty gives sample merchant the cleanest operational path from stablecoin settlement into local disbursement?",
            "How much fee improvement is needed to beat mature remittance infrastructure in a corridor this competitive?",
            "What AML and transaction-monitoring evidence would sample merchant require before treating this as demo-ready at scale?",
        ],
    },
    ("US", "BR"): {
        "key": "US-BR",
        "label": "US -> Brazil",
        "corridor_slug": "us-brazil",
        "source_country": "US",
        "destination_country": "Brazil",
        "destination_city": "Sao Paulo",
        "map_viewport": {
            "lat": -23.55052,
            "lon": -46.633308,
            "zoom": 10.8,
            "bearing": -8,
            "pitch": 0,
        },
        "corridor_note": (
            "Brazil is the cleanest demo story in the bunch. Real stablecoin activity. Real PIX-adjacent narrative. Real reason to care."
        ),
        "default_amount_usdc": 50_000,
        "default_monthly_volume_usdc": 3_000_000,
        "default_baseline_fee_pct": 1.2,
        "default_baseline_settlement_hours": 24,
        "default_current_setup": "SWIFT + FX provider + Brazil payout counterparty",
        "polygon_maturity": "High",
        "stellar_maturity": "Medium",
        "market_readiness": "High",
        "ecosystem_support": "Strong",
        "regulatory_exposure": "Moderate",
        "launch_readiness": "Ready for structured demo",
        "launch_readiness_score": 0.86,
        "rail_route_fit": {"Polygon": 0.89, "Ethereum": 0.77, "Stellar": 0.64},
        "solved_infrastructure": [
            "Polygon: Brazil has strong stablecoin-to-PIX narratives and enterprise payment examples.",
            "Ethereum: institutionally familiar rail with broad custody and treasury support.",
            "Stellar: PIX-connected wallet and payout patterns exist, but enterprise route validation still needs diligence.",
        ],
        "open_questions": [
            "Which local payout or treasury counterparty would sample merchant trust first for production rollout?",
            "How much corridor volume must be executable before the demo is strategically meaningful?",
            "What internal evidence does sample merchant need beyond a live demo to move to a structured demo?",
        ],
    },
    ("US", "PH"): {
        "key": "US-PH",
        "label": "US -> Philippines",
        "corridor_slug": "us-philippines",
        "source_country": "US",
        "destination_country": "Philippines",
        "destination_city": "Manila",
        "map_viewport": {
            "lat": 14.599512,
            "lon": 120.984222,
            "zoom": 11.6,
            "bearing": -10,
            "pitch": 0,
        },
        "corridor_note": (
            "The Philippines is a serious corridor. Big remittance energy. Good payout story. Still needs counterparty diligence."
        ),
        "default_amount_usdc": 5_000,
        "default_monthly_volume_usdc": 1_500_000,
        "default_baseline_fee_pct": 1.4,
        "default_baseline_settlement_hours": 36,
        "default_current_setup": "SWIFT + local payout counterparty + treasury prefunding",
        "polygon_maturity": "High",
        "stellar_maturity": "Medium",
        "market_readiness": "Medium-high",
        "ecosystem_support": "Strong",
        "regulatory_exposure": "Moderate",
        "launch_readiness": "Validate next",
        "launch_readiness_score": 0.74,
        "rail_route_fit": {"Polygon": 0.82, "Ethereum": 0.75, "Stellar": 0.7},
        "solved_infrastructure": [
            "Polygon: well-understood low-cost payout rail narrative with institutional validators.",
            "Ethereum: broadly trusted infrastructure baseline if cost is secondary to familiarity.",
            "Stellar: strong remittance reputation and payment-native story for committee discussion.",
        ],
        "open_questions": [
            "What last-mile payout counterparty would sample merchant use to make stablecoin settlement operationally invisible?",
            "How would sample merchant handle attribution and sanctions monitoring across off-ramp counterpartys?",
            "Would sample merchant evaluate this as consumer remittance infrastructure or broader payout infrastructure?",
        ],
    },
    ("US", "VN"): {
        "key": "US-VN",
        "label": "US -> Vietnam",
        "corridor_slug": "us-vietnam",
        "source_country": "US",
        "destination_country": "Vietnam",
        "destination_city": "Ho Chi Minh City",
        "map_viewport": {
            "lat": 10.823099,
            "lon": 106.629662,
            "zoom": 11.3,
            "bearing": -12,
            "pitch": 0,
        },
        "corridor_note": (
            "Vietnam is interesting. Not ready for chest-thumping. More watchlist than victory lap right now."
        ),
        "default_amount_usdc": 10_000,
        "default_monthly_volume_usdc": 800_000,
        "default_baseline_fee_pct": 1.6,
        "default_baseline_settlement_hours": 48,
        "default_current_setup": "SWIFT + banking counterparty + manual treasury review",
        "polygon_maturity": "Medium",
        "stellar_maturity": "Medium",
        "market_readiness": "Developing",
        "ecosystem_support": "Emerging",
        "regulatory_exposure": "Elevated",
        "launch_readiness": "Watchlist until diligence clears",
        "launch_readiness_score": 0.58,
        "rail_route_fit": {"Polygon": 0.64, "Ethereum": 0.76, "Stellar": 0.58},
        "solved_infrastructure": [
            "Ethereum: strongest baseline for trust and broad provider compatibility.",
            "Polygon: attractive cost profile and strong stablecoin ecosystem if local payouts can be solved.",
            "Stellar: purpose-built payment story is relevant, but corridor-specific proof still matters.",
        ],
        "open_questions": [
            "Which regulated off-ramp or payout counterparty is strongest for Vietnam today?",
            "Is stablecoin settlement reducing a real sample merchant pain point here, or only changing the funding leg?",
            "Should this corridor be used for a demo now or only as a strategic watchlist corridor?",
        ],
    },
    ("SG", "ID"): {
        "key": "SG-ID",
        "label": "Singapore -> Indonesia",
        "corridor_slug": "singapore-indonesia",
        "source_country": "Singapore",
        "destination_country": "Indonesia",
        "destination_city": "Jakarta",
        "map_viewport": {
            "lat": -6.208763,
            "lon": 106.845599,
            "zoom": 10.8,
            "bearing": -7,
            "pitch": 0,
        },
        "corridor_note": (
            "This is Southeast Asia operator reality. Payroll, vendors, regional fintech settlement. Real demand, but infrastructure maturity is not evenly distributed."
        ),
        "default_amount_usdc": 10_000,
        "default_monthly_volume_usdc": 1_200_000,
        "default_baseline_fee_pct": 1.25,
        "default_baseline_settlement_hours": 18,
        "default_current_setup": "Singapore treasury account + regional FX desk + Indonesia payout counterparty",
        "polygon_maturity": "Medium-high",
        "stellar_maturity": "Medium",
        "market_readiness": "Medium-high",
        "ecosystem_support": "Strong",
        "regulatory_exposure": "Moderate",
        "launch_readiness": "Promising with counterparty diligence",
        "launch_readiness_score": 0.72,
        "rail_route_fit": {"Polygon": 0.84, "Ethereum": 0.74, "Stellar": 0.63},
        "solved_infrastructure": [
            "Polygon: regional fintech operators will like the cost profile if the last-mile payout stack is dependable.",
            "Ethereum: trusted baseline for operators that care more about institutional familiarity than cheap execution.",
            "Stellar: payments-oriented story is relevant, but corridor-specific operator proof is still thinner than the narrative.",
        ],
        "open_questions": [
            "Which Indonesia payout or treasury counterparty is mature enough to make regional settlement feel operationally boring?",
            "How much of the corridor read reflects real business settlement versus broader regional crypto activity?",
            "What regulatory and reporting controls would Singapore-based operators need before scaling this corridor confidently?",
        ],
    },
    ("JP", "SG"): {
        "key": "JP-SG",
        "label": "Japan -> Singapore",
        "corridor_slug": "japan-singapore",
        "source_country": "Japan",
        "destination_country": "Singapore",
        "destination_city": "Singapore",
        "map_viewport": {
            "lat": 1.352083,
            "lon": 103.819839,
            "zoom": 10.9,
            "bearing": -5,
            "pitch": 0,
        },
        "corridor_note": (
            "This is not a remittance story. It is a treasury and regional-operations story between two high-trust jurisdictions with very different infrastructure dynamics."
        ),
        "default_amount_usdc": 10_000,
        "default_monthly_volume_usdc": 2_000_000,
        "default_baseline_fee_pct": 0.85,
        "default_baseline_settlement_hours": 10,
        "default_current_setup": "Japan treasury bank + Singapore entity account + regional operating stack",
        "polygon_maturity": "Medium-high",
        "stellar_maturity": "Medium",
        "market_readiness": "High",
        "ecosystem_support": "Strong",
        "regulatory_exposure": "Low",
        "launch_readiness": "Strong for treasury exploration",
        "launch_readiness_score": 0.79,
        "rail_route_fit": {"Polygon": 0.78, "Ethereum": 0.82, "Stellar": 0.57},
        "solved_infrastructure": [
            "Ethereum: strongest fit when the buyer thinks in treasury controls, custody, and institutional comfort.",
            "Polygon: compelling if cost and transfer speed matter more than blue-chip familiarity.",
            "Stellar: still useful as a payments-native reference, though the corporate treasury case is less proven here.",
        ],
        "open_questions": [
            "Which treasury workflows in Japan-to-Singapore actually benefit from stablecoin settlement rather than existing banking rails?",
            "How would sample merchant evidence regulatory comfort and internal controls for a treasury-led corridor like this one?",
            "Is the opportunity here operating expense movement, treasury rebalancing, or venture-style capital deployment?",
        ],
    },
}

STELLAR_REFERENCE = {
    ("US", "MX"): {
        "maturity": "Medium",
        "liquidity_proxy": "Relevant payment-first comparator, but the current Canopy build still treats it as reference context rather than a live route.",
        "settlement": "Approx. 5s ledger close; current Canopy view uses batched analytics, not live rail monitoring.",
        "confidence": 0.58,
        "note": "Useful reference for remittance conversations. Still not today's measured winner.",
    },
    ("US", "BR"): {
        "maturity": "Medium",
        "liquidity_proxy": "Good anchor-led story, but still more comparator than live route in this build.",
        "settlement": "Approx. 5s ledger close; current Canopy view uses batched analytics, not live rail monitoring.",
        "confidence": 0.62,
        "note": "Good comparator. Not the live call today.",
    },
    ("US", "PH"): {
        "maturity": "Medium",
        "liquidity_proxy": "Good remittance story, but the corridor-grade proof still is not there yet.",
        "settlement": "Approx. 5s ledger close; current Canopy view uses batched analytics, not live rail monitoring.",
        "confidence": 0.6,
        "note": "Relevant context if you like payment-first rails. Still not today's live call.",
    },
    ("US", "VN"): {
        "maturity": "Medium",
        "liquidity_proxy": "Interesting on paper, but the corridor-specific maturity is still fuzzy.",
        "settlement": "Approx. 5s ledger close; current Canopy view uses batched analytics, not live rail monitoring.",
        "confidence": 0.52,
        "note": "Keep it in the conversation. Do not mistake it for a live recommendation.",
    },
    ("SG", "ID"): {
        "maturity": "Medium",
        "liquidity_proxy": "Compelling regional-payments comparator, though the corridor-specific operator evidence is still mostly directional.",
        "settlement": "Approx. 5s ledger close; current Canopy view uses batched analytics, not live rail monitoring.",
        "confidence": 0.55,
        "note": "Relevant for SEA settlement conversations. Still reference context in this demo.",
    },
    ("JP", "SG"): {
        "maturity": "Medium",
        "liquidity_proxy": "Interesting as a payment rail reference, but the treasury-grade corridor story remains thinner than Ethereum today.",
        "settlement": "Approx. 5s ledger close; current Canopy view uses batched analytics, not live rail monitoring.",
        "confidence": 0.5,
        "note": "Keep it as a comparator, not as the main live call for this treasury corridor.",
    },
}


def get_demo_presets() -> List[Dict]:
    """Return sample merchant corridor presets for the frontend scenario builder."""
    presets = []
    for cfg in get_corridors():
        presets.append(
            {
                "key": cfg["key"],
                "origin": cfg["origin"],
                "destination": cfg["destination"],
                "label": cfg["label"],
                "corridor_slug": cfg["corridor_slug"],
                "source_country": cfg["source_country"],
                "destination_country": cfg["destination_country"],
                "destination_city": cfg["destination_city"],
                "map_viewport": cfg["map_viewport"],
                "corridor_note": cfg["corridor_note"],
                "default_amount_usdc": cfg["default_amount_usdc"],
                "default_monthly_volume_usdc": cfg["default_monthly_volume_usdc"],
                "default_baseline_fee_pct": cfg["default_baseline_fee_pct"],
                "default_baseline_settlement_hours": cfg["default_baseline_settlement_hours"],
                "default_current_setup": cfg["default_current_setup"],
            }
        )
    return presets


def _normalize_lens(lens: Optional[str]) -> str:
    key = (lens or DEFAULT_LENS).strip().lower()
    return key if key in LENS_CONFIGS else DEFAULT_LENS


def _get_corridor_config(origin: str, destination: str) -> dict:
    return get_corridor_or_default(origin, destination)


def _derive_legacy_chain_status(cache: dict, fallback_cache_age: Optional[int]) -> Tuple[str, str, bool]:
    if cache.get("is_bootstrap", False):
        return ("initializing", "unknown", True)
    if fallback_cache_age is None:
        return ("initializing", "unknown", False)
    if fallback_cache_age > CRITICAL_SECONDS:
        return ("stale", "critical", False)
    if fallback_cache_age > FRESH_SECONDS:
        return ("stale", "stale", False)
    return ("fresh", "fresh", False)


def _get_chain_runtime(cache: dict, chain: str, fallback_cache_age: Optional[int], token: str = DEFAULT_TOKEN) -> dict:
    raw_chain = cache.get("chains", {}).get(chain, {})
    token_key = normalize_token(token)
    if isinstance(raw_chain.get("tokens"), dict):
        raw_token = raw_chain.get("tokens", {}).get(token_key, {})
        if raw_token:
            data = raw_token.get("data", {})
            status = raw_token.get("status", "initializing")
            freshness_level = raw_token.get("freshness_level", "unknown")
            using_bootstrap_data = bool(raw_token.get("using_bootstrap_data", False))
            age_seconds = raw_token.get("age_seconds")
            return {
                "chain": chain,
                "token": token_key,
                "data": data,
                "status": status,
                "freshness_level": freshness_level,
                "age_seconds": age_seconds,
                "last_success_at": raw_token.get("last_success_at"),
                "last_attempt_at": raw_token.get("last_attempt_at"),
                "last_error": raw_token.get("last_error"),
                "using_bootstrap_data": using_bootstrap_data,
                "is_recommendation_degraded": status != "fresh" or freshness_level != "fresh",
            }
        coverage_state = get_token_coverage_state(token_key, chain)
        return {
            "chain": chain,
            "token": token_key,
            "data": {"chain": chain, "token": token_key},
            "status": "unsupported" if coverage_state == "UNSUPPORTED" else "limited",
            "freshness_level": "unknown",
            "age_seconds": None,
            "last_success_at": None,
            "last_attempt_at": None,
            "last_error": None,
            "using_bootstrap_data": True,
            "is_recommendation_degraded": True,
        }
    if isinstance(raw_chain.get("data"), dict):
        data = raw_chain.get("data", {})
        status = raw_chain.get("status", "initializing")
        freshness_level = raw_chain.get("freshness_level", "unknown")
        using_bootstrap_data = bool(raw_chain.get("using_bootstrap_data", False))
        age_seconds = raw_chain.get("age_seconds")
        return {
            "chain": chain,
            "token": token_key,
            "data": data,
            "status": status,
            "freshness_level": freshness_level,
            "age_seconds": age_seconds,
            "last_success_at": raw_chain.get("last_success_at"),
            "last_attempt_at": raw_chain.get("last_attempt_at"),
            "last_error": raw_chain.get("last_error"),
            "using_bootstrap_data": using_bootstrap_data,
            "is_recommendation_degraded": status != "fresh" or freshness_level != "fresh",
        }

    status, freshness_level, using_bootstrap_data = _derive_legacy_chain_status(cache, fallback_cache_age)
    return {
        "chain": chain,
        "token": token_key,
        "data": raw_chain,
        "status": status,
        "freshness_level": freshness_level,
        "age_seconds": fallback_cache_age,
        "last_success_at": raw_chain.get("freshness_timestamp"),
        "last_attempt_at": raw_chain.get("queried_at"),
        "last_error": cache.get("last_error"),
        "using_bootstrap_data": using_bootstrap_data,
        "is_recommendation_degraded": status != "fresh" or freshness_level != "fresh",
    }


def _freshness_badge(chain_runtime: dict) -> str:
    if chain_runtime["using_bootstrap_data"]:
        return "Bootstrap estimate"
    if chain_runtime["status"] == "error":
        return "Live measured - refresh failed"
    if chain_runtime["freshness_level"] == "critical":
        return "Live measured - critically stale"
    if chain_runtime["freshness_level"] == "stale":
        return "Live measured - stale"
    return "Live measured"


def _fmt_percent(value: float) -> str:
    return f"{value:.1f}%"


def _fmt_usd(value: Optional[float]) -> str:
    if value is None:
        return "Reference only"
    if value >= 100:
        return f"${value:,.0f}"
    if value >= 1:
        return f"${value:,.2f}"
    return f"${value:,.4f}"


def _maturity_score(label: str) -> float:
    return {
        "strong": 0.88,
        "high": 0.84,
        "medium-high": 0.76,
        "medium": 0.66,
        "developing": 0.56,
        "emerging": 0.5,
        "directional": 0.48,
    }.get((label or "").strip().lower(), 0.5)


def _get_rail_confidence(
    transfer_count: int,
    adjusted_transfer_count: int,
    minutes_since_last_adjusted_transfer: Optional[int],
    cache_age: Optional[int],
    chain_status: str,
    freshness_level: str,
    native_prices_live: bool,
    is_bootstrap: bool,
) -> float:
    activity_count = adjusted_transfer_count or transfer_count

    if activity_count > 1_000:
        confidence = 0.96
    elif activity_count > 200:
        confidence = 0.88
    elif activity_count > 50:
        confidence = 0.82
    elif activity_count > 10:
        confidence = 0.68
    else:
        confidence = 0.58

    if adjusted_transfer_count < 50:
        confidence *= 0.8
    if minutes_since_last_adjusted_transfer is not None and minutes_since_last_adjusted_transfer > 240:
        confidence *= 0.72
    elif minutes_since_last_adjusted_transfer is not None and minutes_since_last_adjusted_transfer > 60:
        confidence *= 0.82
    if chain_status == "error":
        confidence *= 0.7
    elif freshness_level == "critical":
        confidence *= 0.65
    elif freshness_level == "stale":
        confidence *= 0.82
    elif cache_age is not None and cache_age > 3600:
        confidence *= 0.7
    if not native_prices_live:
        confidence *= 0.9
    if is_bootstrap:
        confidence *= 0.8

    return round(confidence, 2)


def _get_liquidity_score(
    adjusted_transfer_count: int,
    adjusted_volume_usdc: Optional[float],
    avg_gap_minutes: Optional[float],
    minutes_since_last_adjusted_transfer: Optional[int],
) -> float:
    score = 0.34

    if adjusted_transfer_count > 1_000:
        score += 0.34
    elif adjusted_transfer_count > 200:
        score += 0.26
    elif adjusted_transfer_count > 50:
        score += 0.18
    elif adjusted_transfer_count > 10:
        score += 0.1

    if adjusted_volume_usdc:
        if adjusted_volume_usdc > 100_000_000:
            score += 0.12
        elif adjusted_volume_usdc > 10_000_000:
            score += 0.08
        elif adjusted_volume_usdc > 1_000_000:
            score += 0.04

    if avg_gap_minutes is not None:
        if avg_gap_minutes <= 5:
            score += 0.12
        elif avg_gap_minutes <= 30:
            score += 0.08
        elif avg_gap_minutes <= 120:
            score += 0.04

    if minutes_since_last_adjusted_transfer is not None:
        if minutes_since_last_adjusted_transfer <= 15:
            score += 0.08
        elif minutes_since_last_adjusted_transfer <= 60:
            score += 0.05
        elif minutes_since_last_adjusted_transfer <= 240:
            score += 0.02
        else:
            score -= 0.05

    return max(0.12, min(round(score, 2), 0.98))


def _build_liquidity_proxy(
    token: str,
    adjusted_transfer_count: int,
    adjusted_volume_usdc: Optional[float],
    avg_gap_minutes: Optional[float],
    minutes_since_last_adjusted_transfer: Optional[int],
) -> Tuple[str, str]:
    if (
        adjusted_transfer_count > 1_000
        and (minutes_since_last_adjusted_transfer or 9999) <= 15
        and (avg_gap_minutes is None or avg_gap_minutes <= 5)
    ):
        return (
            "High adjusted activity",
            f"Filtered direct {token} activity is dense and recent, which is the strongest live liquidity proxy in this build.",
        )
    if (
        adjusted_transfer_count > 100
        and (minutes_since_last_adjusted_transfer or 9999) <= 60
        and (avg_gap_minutes is None or avg_gap_minutes <= 30)
    ):
        return (
            "Active adjusted flow",
            f"Filtered direct {token} activity is recent enough to support directional operator evaluation.",
        )
    if adjusted_transfer_count > 20 or (adjusted_volume_usdc or 0) > 1_000_000:
        return (
            "Moderate adjusted flow",
            f"Filtered direct {token} activity is present, but payout readiness and large-ticket execution still need diligence.",
        )
    return (
        "Thin adjusted flow",
        f"Filtered direct {token} activity is light or stale; treat this as a weak directional liquidity proxy.",
    )


def _build_transfer_continuity(
    avg_gap_minutes: Optional[float],
    minutes_since_last_adjusted_transfer: Optional[int],
) -> Tuple[str, str]:
    if avg_gap_minutes is None and minutes_since_last_adjusted_transfer is None:
        return (
            "Sparse direct activity",
            "Canopy has limited recent adjusted transfer continuity for this rail.",
        )
    if (
        avg_gap_minutes is not None
        and avg_gap_minutes <= 5
        and (minutes_since_last_adjusted_transfer or 9999) <= 15
    ):
        return (
            "Continuous flow",
            "Adjusted payment-like transfers are arriving with sub-5 minute average gaps and very recent recency.",
        )
    if (
        avg_gap_minutes is not None
        and avg_gap_minutes <= 30
        and (minutes_since_last_adjusted_transfer or 9999) <= 60
    ):
        return (
            "Consistent flow",
            "Adjusted payment-like transfers remain regular enough for directional treasury planning.",
        )
    if minutes_since_last_adjusted_transfer is not None and minutes_since_last_adjusted_transfer <= 240:
        return (
            "Intermittent flow",
            "Direct activity exists, but treasury should assume some timing variance in observed transfer continuity.",
        )
    return (
        "Stale flow",
        "Recent adjusted payment-like activity is limited, which weakens confidence in transfer continuity.",
    )


def _predictability_score(rail: str, mode: str) -> float:
    if mode == "historical_reference":
        return 0.58
    return {
        "Ethereum": 0.88,
        "Polygon": 0.79,
    }.get(rail, 0.62)


def _freshness_score(
    cache_age: Optional[int],
    minutes_since_last_adjusted_transfer: Optional[int],
    mode: str,
    chain_status: str = "fresh",
    freshness_level: str = "fresh",
) -> float:
    if mode == "historical_reference":
        return 0.42

    score = 0.82
    if chain_status == "error":
        score -= 0.28
    elif freshness_level == "critical":
        score -= 0.34
    elif freshness_level == "stale":
        score -= 0.16
    elif cache_age is not None and cache_age > 3600:
        score -= 0.28
    elif cache_age is not None and cache_age > 900:
        score -= 0.1

    if minutes_since_last_adjusted_transfer is not None and minutes_since_last_adjusted_transfer > 240:
        score -= 0.18
    elif minutes_since_last_adjusted_transfer is not None and minutes_since_last_adjusted_transfer > 60:
        score -= 0.08

    return max(0.24, round(score, 2))


def _score_level(score: float) -> str:
    if score >= 0.78:
        return "High"
    if score >= 0.58:
        return "Medium"
    return "Low"


def _build_trust_component(label: str, score: float, detail: str) -> dict:
    normalized = max(0.0, min(round(score, 2), 1.0))
    return {
        "label": label,
        "level": _score_level(normalized),
        "score": normalized,
        "detail": detail,
    }


def _build_freshness_component(
    mode: str,
    cache_age: Optional[int],
    minutes_since_last_adjusted_transfer: Optional[int],
    chain_status: str = "fresh",
    freshness_level: str = "fresh",
) -> dict:
    if mode == "historical_reference":
        return _build_trust_component(
            "Freshness",
            0.36,
            "Historical reference only. No live freshness read in the current stack.",
        )
    if chain_status == "error":
        return _build_trust_component(
            "Freshness",
            0.3,
            "Latest refresh failed, so Canopy is serving the last known chain read with an explicit degraded signal.",
        )
    if freshness_level == "critical":
        return _build_trust_component(
            "Freshness",
            0.28,
            "Signal is over 1 hour old, so trust is heavily decayed until the live cache refreshes.",
        )
    if cache_age is not None and cache_age > 3600:
        return _build_trust_component(
            "Freshness",
            0.32,
            "Signal stale — cache is over 1 hour old, so trust is automatically decayed.",
        )
    if minutes_since_last_adjusted_transfer is not None and minutes_since_last_adjusted_transfer > 240:
        return _build_trust_component(
            "Freshness",
            0.42,
            "Adjusted payment-like activity has gone quiet for over 4 hours.",
        )
    if cache_age is not None and cache_age > 900:
        return _build_trust_component(
            "Freshness",
            0.66,
            "Cache is older than 15 minutes, but still usable for directional reads.",
        )
    if minutes_since_last_adjusted_transfer is not None and minutes_since_last_adjusted_transfer > 60:
        return _build_trust_component(
            "Freshness",
            0.68,
            "Recent activity exists, but the last adjusted transfer is over an hour old.",
        )
    return _build_trust_component(
        "Freshness",
        0.92,
        "Recent measured activity and current cache make this a fresh signal.",
    )


def _build_signal_quality_component(
    mode: str,
    transfer_count: Optional[int],
    adjusted_transfer_count: Optional[int],
    activity_filter_method: Optional[str],
) -> Tuple[dict, float]:
    if mode == "historical_reference":
        return (
            _build_trust_component(
                "Signal quality",
                0.34,
                "Historical reference only. No DeFi-filtered payment stream is being measured here.",
            ),
            1.0,
        )

    raw_count = max(int(transfer_count or 0), 1)
    adjusted_count = max(int(adjusted_transfer_count or 0), 0)
    adjusted_ratio = adjusted_count / raw_count
    defi_noise_ratio = max(0.0, 1 - adjusted_ratio)

    filter_suffix = f" Filter rule: {activity_filter_method}" if activity_filter_method else ""

    if adjusted_ratio >= 0.72 and adjusted_count >= 100:
        detail = (
            "Most observed transfer logs survive the payment filter, so DeFi noise looks low."
            f"{filter_suffix}"
        )
        score = 0.9
    elif adjusted_ratio >= 0.38 and adjusted_count >= 20:
        detail = (
            "A usable share of observed transfer logs survive the payment filter, but protocol traffic is still present."
            f"{filter_suffix}"
        )
        score = 0.66
    else:
        detail = (
            "Activity contains significant protocol traffic; payment signal is weaker after filtering."
            f"{filter_suffix}"
        )
        score = 0.38

    return (_build_trust_component("Signal quality", score, detail), round(defi_noise_ratio, 2))


def _build_evidence_type_component(
    mode: str,
    native_prices_live: bool,
    is_bootstrap: bool,
) -> dict:
    if mode == "historical_reference":
        return _build_trust_component(
            "Evidence type",
            0.28,
            "Historical reference only; context signal, not a live operational recommendation.",
        )
    if is_bootstrap:
        return _build_trust_component(
            "Evidence type",
            0.48,
            "Bootstrap estimate while the live cache warms up. Useful, but not fully measured yet.",
        )
    if not native_prices_live:
        return _build_trust_component(
            "Evidence type",
            0.72,
            "Live measured chain activity with fallback pricing on the USD conversion layer.",
        )
    return _build_trust_component(
        "Evidence type",
        0.94,
        "Live measured chain activity from Google BigQuery with current market pricing.",
    )


def _build_attribution_component(rail: dict, corridor_cfg: dict, lens: str) -> dict:
    if rail["mode"] == "historical_reference":
        return _build_trust_component(
            "Attribution confidence",
            0.4,
            "Useful corridor context, but corridor-specific attribution is still inference-heavy.",
        )

    score = rail.get("route_fit_score", 0.62)
    regulatory_exposure = corridor_cfg.get("regulatory_exposure", "").strip().lower()
    market_readiness = corridor_cfg.get("market_readiness", "").strip().lower()

    if lens == "risk":
        if regulatory_exposure in {"high", "elevated"}:
            score *= 0.78
        elif regulatory_exposure in {"moderate", "medium"}:
            score *= 0.9

    if market_readiness in {"developing", "emerging"}:
        score *= 0.88

    score = max(0.24, min(round(score, 2), 0.95))

    if score >= 0.78:
        detail = "Corridor fit and market precedent are strong enough to support this lens."
    elif score >= 0.58:
        detail = "Some corridor fit is visible, but local payout attribution still relies on inference."
    else:
        detail = "Weak corridor-specific attribution; treat this as directional rather than fully defensible."

    return _build_trust_component("Attribution confidence", score, detail)


def _assumption_confidence(
    rail: dict,
    corridor_cfg: dict,
    lens: str,
    native_prices_live: bool,
    is_bootstrap: bool,
) -> Tuple[float, str]:
    if rail["mode"] == "historical_reference":
        return (
            0.24,
            "High assumption burden: this rail is shown mainly as contextual reference.",
        )

    score = 0.74
    regulatory_exposure = corridor_cfg.get("regulatory_exposure", "").strip().lower()

    if not native_prices_live:
        score -= 0.08
    if is_bootstrap:
        score -= 0.18
    if lens == "risk":
        score -= 0.08
    if regulatory_exposure in {"high", "elevated"}:
        score -= 0.08

    score = max(0.24, min(round(score, 2), 0.9))

    if score >= 0.72:
        detail = "Low assumption burden: most of this trust comes from measured signal rather than modeled context."
    elif score >= 0.52:
        detail = "Moderate assumption burden: measured signal is doing real work, but payout and routing assumptions still matter."
    else:
        detail = "High assumption burden: the recommendation leans heavily on modeled context around payout and routing."

    return (score, detail)


def _build_trust_profile(
    rail: dict,
    corridor_cfg: dict,
    lens: str,
    native_prices_live: bool,
    is_bootstrap: bool,
) -> dict:
    freshness = _build_freshness_component(
        rail["mode"],
        rail.get("cache_age_seconds"),
        rail.get("minutes_since_last_adjusted_transfer"),
        chain_status=rail.get("data_status", "fresh"),
        freshness_level=rail.get("freshness_level", "fresh"),
    )
    signal_quality, defi_noise_ratio = _build_signal_quality_component(
        rail["mode"],
        rail.get("transfer_count"),
        rail.get("adjusted_transfer_count"),
        rail.get("activity_filter_method"),
    )
    evidence_type = _build_evidence_type_component(
        rail["mode"],
        native_prices_live=native_prices_live,
        is_bootstrap=is_bootstrap,
    )
    attribution = _build_attribution_component(rail, corridor_cfg, lens)
    assumption_confidence, assumption_detail = _assumption_confidence(
        rail,
        corridor_cfg,
        lens,
        native_prices_live=native_prices_live,
        is_bootstrap=is_bootstrap,
    )

    trust_score = round(
        (
            (freshness["score"] * 0.26)
            + (signal_quality["score"] * 0.28)
            + (evidence_type["score"] * 0.22)
            + (attribution["score"] * 0.16)
            + (assumption_confidence * 0.08)
        )
        * 100
    )

    is_stale = (
        rail["mode"] != "historical_reference"
        and (
            rail.get("data_status") == "error"
            or rail.get("freshness_level") in {"stale", "critical"}
            or (rail.get("cache_age_seconds") is not None and rail.get("cache_age_seconds") > 3600)
        )
    )
    if is_stale:
        trust_score = round(trust_score * 0.7)
    if rail["mode"] == "historical_reference":
        trust_score = min(trust_score, 60)

    components = [freshness, signal_quality, evidence_type, attribution]
    weakest_component = min(components, key=lambda item: item["score"])

    if rail["mode"] == "historical_reference":
        summary = "Historical reference only; context signal, not a live recommendation."
    elif is_stale:
        summary = "Signal stale; trust is decayed automatically until the live cache refreshes."
    else:
        freshness_phrase = (
            "Strong recent measured activity"
            if freshness["level"] == "High"
            else "Fresh signal" if freshness["level"] == "Medium" else "Stale signal"
        )
        if signal_quality["level"] == "High":
            quality_phrase = "low DeFi noise"
        elif signal_quality["level"] == "Medium":
            quality_phrase = "moderate DeFi noise"
        else:
            quality_phrase = "payment signal mixed with protocol traffic"

        burden_phrase = (
            "low assumption burden"
            if assumption_confidence >= 0.72
            else "moderate assumption burden"
            if assumption_confidence >= 0.52
            else "high assumption burden"
        )
        summary = f"{freshness_phrase}; {quality_phrase}; {burden_phrase}."

    return {
        "trust_score": int(max(0, min(trust_score, 100))),
        "trust_score_label": f"{int(max(0, min(trust_score, 100)))} / 100",
        "trust_summary": summary,
        "trust_biggest_uncertainty": weakest_component["detail"],
        "trust_components": components,
        "assumption_confidence_score": round(assumption_confidence, 2),
        "assumption_confidence_label": f"{int(max(0, min(assumption_confidence * 100, 100)))} / 100",
        "assumption_burden_detail": assumption_detail,
        "defi_noise_ratio": defi_noise_ratio,
        "is_stale_signal": is_stale,
    }


def _cost_score(fee_usd: Optional[float], fee_values: List[float], mode: str) -> float:
    if mode == "historical_reference" or fee_usd is None:
        return 0.62
    if not fee_values:
        return 0.5
    low = min(fee_values)
    high = max(fee_values)
    if high == low:
        return 0.7
    normalized = 1 - ((fee_usd - low) / (high - low))
    return round(0.35 + (normalized * 0.55), 2)


def _build_live_rail_card(
    rail: str,
    token: str,
    chain_runtime: dict,
    fee_usd: float,
    native_prices_live: bool,
    maturity: str,
    settlement_timing: str,
    note: str,
) -> dict:
    chain_data = chain_runtime["data"]
    cache_age = chain_runtime.get("age_seconds")
    is_bootstrap = chain_runtime.get("using_bootstrap_data", False)
    transfer_count = int(chain_data.get("transfer_count", 0))
    volume_usdc = chain_data.get("volume_usdc")
    adjusted_transfer_count = int(chain_data.get("adjusted_transfer_count", 0))
    adjusted_volume_usdc = chain_data.get("adjusted_volume_usdc")
    avg_gap_minutes = chain_data.get("avg_minutes_between_adjusted_transfers")
    minutes_since_last_adjusted_transfer = chain_data.get(
        "minutes_since_last_adjusted_transfer"
    )
    confidence = _get_rail_confidence(
        transfer_count=transfer_count,
        adjusted_transfer_count=adjusted_transfer_count,
        minutes_since_last_adjusted_transfer=minutes_since_last_adjusted_transfer,
        cache_age=cache_age,
        chain_status=chain_runtime.get("status", "fresh"),
        freshness_level=chain_runtime.get("freshness_level", "fresh"),
        native_prices_live=native_prices_live,
        is_bootstrap=is_bootstrap,
    )
    liquidity_label, liquidity_detail = _build_liquidity_proxy(
        token=token,
        adjusted_transfer_count=adjusted_transfer_count,
        adjusted_volume_usdc=adjusted_volume_usdc,
        avg_gap_minutes=avg_gap_minutes,
        minutes_since_last_adjusted_transfer=minutes_since_last_adjusted_transfer,
    )
    continuity_label, continuity_detail = _build_transfer_continuity(
        avg_gap_minutes=avg_gap_minutes,
        minutes_since_last_adjusted_transfer=minutes_since_last_adjusted_transfer,
    )

    return {
        "rail": rail,
        "token": token,
        "coverage_state": get_token_coverage_state(token, rail),
        "evidence_state": EVIDENCE_LIVE_MEASURED,
        "mode": "live_measured",
        "status_badge": _freshness_badge(chain_runtime),
        "estimated_fee_usd": round(fee_usd, 6),
        "estimated_fee_label": _fmt_usd(fee_usd),
        "cost_signal": "Low" if fee_usd < 0.1 else ("Moderate" if fee_usd < 1 else "High"),
        "liquidity_proxy_label": liquidity_label,
        "liquidity_proxy_detail": liquidity_detail,
        "transfer_continuity_label": continuity_label,
        "transfer_continuity_detail": continuity_detail,
        "transfer_count": transfer_count,
        "volume_usdc": volume_usdc,
        "adjusted_transfer_count": adjusted_transfer_count,
        "adjusted_volume_usdc": adjusted_volume_usdc,
        "avg_minutes_between_adjusted_transfers": avg_gap_minutes,
        "minutes_since_last_adjusted_transfer": minutes_since_last_adjusted_transfer,
        "activity_filter_method": chain_data.get("activity_filter_method"),
        "freshness_timestamp": (
            chain_data.get("adjusted_freshness_timestamp")
            or chain_data.get("freshness_timestamp")
            or chain_runtime.get("last_success_at")
        ),
        "cache_age_seconds": cache_age,
        "data_status": chain_runtime.get("status", "initializing"),
        "freshness_level": chain_runtime.get("freshness_level", "unknown"),
        "last_success_at": chain_runtime.get("last_success_at"),
        "last_attempt_at": chain_runtime.get("last_attempt_at"),
        "last_error": chain_runtime.get("last_error"),
        "using_bootstrap_data": is_bootstrap,
        "confidence": confidence,
        "confidence_label": f"{int(confidence * 100)}%",
        "settlement_timing": settlement_timing,
        "maturity": maturity,
        "note": note,
        "liquidity_score": _get_liquidity_score(
            adjusted_transfer_count=adjusted_transfer_count,
            adjusted_volume_usdc=adjusted_volume_usdc,
            avg_gap_minutes=avg_gap_minutes,
            minutes_since_last_adjusted_transfer=minutes_since_last_adjusted_transfer,
        ),
        "predictability_score": _predictability_score(rail, "live_measured"),
        "freshness_score": _freshness_score(
            cache_age=cache_age,
            minutes_since_last_adjusted_transfer=minutes_since_last_adjusted_transfer,
            mode="live_measured",
            chain_status=chain_runtime.get("status", "fresh"),
            freshness_level=chain_runtime.get("freshness_level", "fresh"),
        ),
        "maturity_score": _maturity_score(maturity),
    }


def _build_stellar_card(origin: str, destination: str, corridor_cfg: dict) -> dict:
    profile = get_stellar_reference(origin, destination)
    continuity_label, continuity_detail = _build_transfer_continuity(
        avg_gap_minutes=None,
        minutes_since_last_adjusted_transfer=None,
    )
    return {
        "rail": "Stellar",
        "mode": "historical_reference",
        "status_badge": "Historical reference",
        "estimated_fee_usd": None,
        "estimated_fee_label": "Very low fee rail",
        "cost_signal": "Reference",
        "liquidity_proxy_label": "Liquidity Proxy",
        "liquidity_proxy_detail": profile["liquidity_proxy"],
        "transfer_continuity_label": continuity_label,
        "transfer_continuity_detail": continuity_detail,
        "transfer_count": None,
        "volume_usdc": None,
        "adjusted_transfer_count": None,
        "adjusted_volume_usdc": None,
        "avg_minutes_between_adjusted_transfers": None,
        "minutes_since_last_adjusted_transfer": None,
        "freshness_timestamp": None,
        "confidence": min(profile["confidence"], 0.65),
        "confidence_label": f"{int(min(profile['confidence'], 0.65) * 100)}%",
        "settlement_timing": profile["settlement"],
        "maturity": profile["maturity"],
        "note": profile["note"],
        "liquidity_score": 0.46,
        "predictability_score": _predictability_score("Stellar", "historical_reference"),
        "freshness_score": _freshness_score(None, None, "historical_reference"),
        "maturity_score": _maturity_score(corridor_cfg.get("stellar_maturity", profile["maturity"])),
    }


def _score_rails(
    rails: List[dict],
    corridor_cfg: dict,
    corridor_key: str,
    amount_usdc: float,
) -> Tuple[List[dict], dict, dict, dict]:
    fee_values = [
        card["estimated_fee_usd"]
        for card in rails
        if card["mode"] == "live_measured" and card.get("estimated_fee_usd") is not None
    ]

    scored = []
    fee_floor = min(fee_values) if fee_values else 0.0
    fee_ceiling = max(fee_values) if fee_values else 1.0
    for card in rails:
        analytics = build_rail_corridor_metrics(
            corridor_key,
            card,
            transfer_amount_usdc=amount_usdc,
            fee_floor=fee_floor,
            fee_ceiling=fee_ceiling,
        )
        route_fit = corridor_cfg.get("rail_route_fit", {}).get(card["rail"], 0.62)
        corridor_viability = round(
            ((corridor_cfg.get("launch_readiness_score", 0.5) * 0.55) + (route_fit * 0.45)),
            2,
        )
        ecosystem_validation = round(
            ((card.get("maturity_score", 0.5) * 0.55) + (_maturity_score(corridor_cfg.get("ecosystem_support", "Directional")) * 0.45)),
            2,
        )
        cost_advantage = _cost_score(card.get("estimated_fee_usd"), fee_values, card["mode"])
        legacy_decision_score = round(
            (corridor_viability * 0.24)
            + (analytics.get("liquidity_score_v4", card.get("liquidity_score", 0.4)) * 0.2)
            + (card.get("predictability_score", 0.5) * 0.17)
            + (route_fit * 0.14)
            + (cost_advantage * 0.1)
            + (ecosystem_validation * 0.08)
            + (card.get("freshness_score", 0.4) * 0.07),
            3,
        )
        freshness_penalty_factor = 1.0
        if card["mode"] == "live_measured":
            if card.get("data_status") == "error":
                freshness_penalty_factor = 0.72
            elif card.get("freshness_level") == "critical":
                freshness_penalty_factor = 0.68
            elif card.get("freshness_level") == "stale":
                freshness_penalty_factor = 0.84

        raw_strategy_score = analytics["strategy_score"]
        decision_score = round(
            ((legacy_decision_score * 0.46) + (analytics["route_score"] * 0.54))
            * freshness_penalty_factor,
            3,
        )
        strategy_score = round(raw_strategy_score * freshness_penalty_factor, 3)

        annotated = {
            **card,
            **analytics,
            "route_fit_score": round(route_fit, 2),
            "corridor_viability_score": corridor_viability,
            "ecosystem_validation_score": ecosystem_validation,
            "cost_advantage_score": cost_advantage,
            "legacy_decision_score": legacy_decision_score,
            "raw_strategy_score": raw_strategy_score,
            "strategy_score": strategy_score,
            "strategy_score_label": f"{int(strategy_score * 100)} / 100",
            "decision_score": decision_score,
            "decision_score_label": f"{int(decision_score * 100)} / 100",
            "evidence_confidence": analytics["evidence_confidence"],
            "evidence_confidence_label": analytics["evidence_confidence_label"],
            "freshness_penalty_factor": freshness_penalty_factor,
            "recommendation_eligible": (
                card["mode"] == "live_measured"
                and not analytics["adversarial_flags"]
                and card.get("data_status") == "fresh"
                and card.get("freshness_level") == "fresh"
            ),
            "transfer_math": build_transfer_math(
                rail=card["rail"],
                amount_usdc=amount_usdc,
                network_fee_usd=card.get("estimated_fee_usd"),
                measured_fee_available=(
                    card["mode"] == "live_measured"
                    and card.get("estimated_fee_usd") is not None
                ),
            ),
        }

        if card["mode"] == "historical_reference":
            annotated["strategy_score"] = min(annotated["strategy_score"], 0.65)
            annotated["strategy_score_label"] = f"{int(annotated['strategy_score'] * 100)} / 100"
            annotated["decision_score"] = annotated["strategy_score"]
            annotated["decision_score_label"] = annotated["strategy_score_label"]

        scored.append(annotated)

    transfer_ranked = sorted(
        [card for card in scored if card["mode"] == "live_measured"],
        key=lambda card: (
            -card["transfer_math"]["landed_amount_usd"],
            -card["strategy_score"],
            -card["evidence_confidence"],
        ),
    )
    live_ranked = sorted(
        [card for card in scored if card["recommendation_eligible"]],
        key=lambda card: (
            -card["strategy_score"],
            -card["liquidity_score_v4"],
            -card["evidence_confidence"],
            card["estimated_fee_usd"],
        ),
    )
    if len(live_ranked) < 2:
        live_ranked = sorted(
            [card for card in scored if card["mode"] == "live_measured"],
            key=lambda card: (
                0 if card["recommendation_eligible"] else 1,
                -card["strategy_score"],
                -card["liquidity_score_v4"],
                -card["evidence_confidence"],
                card["estimated_fee_usd"],
            ),
        )
    transfer_winner = transfer_ranked[0]
    recommended_card = live_ranked[0]
    alternative_card = live_ranked[1] if len(live_ranked) > 1 else live_ranked[0]
    return scored, transfer_winner, recommended_card, alternative_card


def _corridor_viability_label(score: float) -> str:
    if score >= 0.8:
        return "High"
    if score >= 0.68:
        return "Promising"
    return "Diligence-heavy"


def _risk_summary_label(open_questions: List[str], regulatory_exposure: str) -> str:
    base = len(open_questions)
    if regulatory_exposure.lower() in {"elevated", "high"}:
        return f"{base} open items, elevated regulatory review"
    if regulatory_exposure.lower() in {"moderate", "medium"}:
        return f"{base} open items, moderate review load"
    return f"{base} open items, lighter review load"


def _build_base_committee_summary(
    corridor_cfg: dict,
    recommended_card: dict,
    token: str,
    baseline_fee_pct: float,
    baseline_settlement_hours: float,
    current_setup: str,
    monthly_volume_usdc: float,
    tx_count_estimate: float,
    baseline_monthly_cost_usd: float,
    stablecoin_monthly_cost_usd: float,
) -> str:
    return (
        f"Canopy reviewed {corridor_cfg['label']} using sample merchant assumptions: "
        f"${monthly_volume_usdc:,.0f} monthly corridor volume, {tx_count_estimate:,.0f} transfers at the selected "
        f"ticket size, current rail cost around {_fmt_percent(baseline_fee_pct)}, and current settlement time around "
        f"{baseline_settlement_hours:.0f} hours. Under those assumptions, the strongest current rail in the workspace "
        f"for {token} is {recommended_card['rail']}. The current Canopy estimate for network-level cost is about "
        f"{recommended_card['estimated_fee_label']} per transfer, which implies roughly "
        f"{_fmt_usd(stablecoin_monthly_cost_usd)} monthly network cost versus a baseline corridor cost of about "
        f"{_fmt_usd(baseline_monthly_cost_usd)}. The right interpretation is not that the full payout stack is solved, "
        f"but that the funding and settlement leg may justify deeper diligence. Current setup assumed: {current_setup}."
    )


def _selected_token_coverage_state(token: str) -> str:
    if any(is_active_coverage(token, chain) for chain in ("Ethereum", "Polygon")):
        return COVERAGE_ACTIVE
    if any(get_token_coverage_state(token, chain) == COVERAGE_LIMITED for chain in ("Ethereum", "Polygon")):
        return COVERAGE_LIMITED
    return "UNSUPPORTED"


def _build_route_evidence(result: dict, recommended_card: dict) -> dict:
    return {
        "expected_fee_usd": build_metric_evidence(
            recommended_card.get("estimated_fee_usd"),
            evidence_state=recommended_card.get("evidence_state", EVIDENCE_LIVE_MEASURED),
            coverage_state=recommended_card.get("coverage_state", COVERAGE_ACTIVE),
            data_source="BigQuery cache",
            last_updated_at=recommended_card.get("last_success_at") or recommended_card.get("freshness_timestamp"),
            ttl_seconds=900,
            confidence_reason=recommended_card.get("confidence_label"),
        ),
        "recommendation_confidence": build_metric_evidence(
            result.get("evidence_confidence"),
            evidence_state=recommended_card.get("evidence_state", EVIDENCE_LIVE_MEASURED),
            coverage_state=recommended_card.get("coverage_state", COVERAGE_ACTIVE),
            data_source="Canopy strategy model",
            last_updated_at=recommended_card.get("last_success_at") or recommended_card.get("freshness_timestamp"),
            ttl_seconds=900,
            confidence_reason=recommended_card.get("evidence_confidence_label"),
        ),
        "trust_score": build_metric_evidence(
            recommended_card.get("trust_score_v4"),
            evidence_state=recommended_card.get("evidence_state", EVIDENCE_LIVE_MEASURED),
            coverage_state=recommended_card.get("coverage_state", COVERAGE_ACTIVE),
            data_source="Canopy trust profile",
            last_updated_at=recommended_card.get("last_success_at") or recommended_card.get("freshness_timestamp"),
            ttl_seconds=900,
            confidence_reason=recommended_card.get("trust_summary"),
        ),
        "accessible_depth_usd": build_metric_evidence(
            recommended_card.get("adjusted_volume_usdc"),
            evidence_state=recommended_card.get("evidence_state", EVIDENCE_LIVE_MEASURED),
            coverage_state=recommended_card.get("coverage_state", COVERAGE_ACTIVE),
            data_source="Direct transfer activity proxy",
            last_updated_at=recommended_card.get("last_success_at") or recommended_card.get("freshness_timestamp"),
            ttl_seconds=900,
            confidence_reason=recommended_card.get("liquidity_proxy_label"),
        ),
    }


def _annotate_rail_for_lens(rail: dict, lens: str) -> dict:
    if lens == "treasury":
        return {
            **rail,
            "lens_signal_label": "Transfer continuity",
            "lens_signal_value": rail.get("transfer_continuity_label", "Directional only"),
            "lens_signal_detail": rail.get("transfer_continuity_detail", rail.get("note", "")),
        }
    if lens == "risk":
        maturity_label = rail.get("maturity", "Directional")
        evidence_note = (
            "Historical reference only; not eligible for a live operational recommendation."
            if rail["mode"] == "historical_reference"
            else "Live measured evidence from the current Canopy cache."
        )
        return {
            **rail,
            "lens_signal_label": "Infrastructure maturity",
            "lens_signal_value": maturity_label,
            "lens_signal_detail": evidence_note,
        }
    if lens == "expansion":
        return {
            **rail,
            "lens_signal_label": "Strategy score",
            "lens_signal_value": rail.get("strategy_score_label", rail.get("decision_score_label", "—")),
            "lens_signal_detail": "Higher scores indicate better corridor fit under the current strategy model.",
        }
    return {
        **rail,
        "lens_signal_label": "Strategy score",
        "lens_signal_value": rail.get("strategy_score_label", rail.get("decision_score_label", "—")),
        "lens_signal_detail": "Blends cost, liquidity integrity, trust, and corridor fit for the Canopy recommendation.",
    }


def _build_lens_highlights(result: dict, corridor_cfg: dict, recommended_card: dict) -> List[dict]:
    lens = result["lens"]
    if lens == "treasury":
        continuity_value = recommended_card.get("transfer_continuity_label", "Directional only")
        density_detail = (
            f"{int(round(recommended_card.get('avg_minutes_between_adjusted_transfers', 0)))}m avg gap"
            if recommended_card.get("avg_minutes_between_adjusted_transfers") is not None
            else "No continuity estimate yet"
        )
        return [
            {
                "label": "Canopy recommendation",
                "value": recommended_card["rail"],
                "detail": "Best treasury fit in the current workspace.",
            },
            {
                "label": "Liquidity Proxy",
                "value": recommended_card["liquidity_proxy_label"],
                "detail": "Adjusted payment activity, not guaranteed executable liquidity.",
            },
            {
                "label": "Transfer continuity",
                "value": continuity_value,
                "detail": density_detail,
            },
            {
                "label": "Settlement framing",
                "value": "Predictable" if recommended_card["predictability_score"] >= 0.8 else "Directional",
                "detail": recommended_card["settlement_timing"],
            },
        ]
    if lens == "risk":
        return [
            {
                "label": "Canopy recommendation",
                "value": recommended_card["rail"],
                "detail": "Still the best live operational fit after risk weighting.",
            },
            {
                "label": "Regulatory exposure",
                "value": corridor_cfg["regulatory_exposure"],
                "detail": "Corridor-level diligence burden, not chain law advice.",
            },
            {
                "label": "Open diligence items",
                "value": str(len(result["open_questions"])),
                "detail": _risk_summary_label(result["open_questions"], corridor_cfg["regulatory_exposure"]),
            },
            {
                "label": "Evidence mode",
                "value": "Mixed",
                "detail": "This view combines live-supported rails with modeled corridor context and explicit evidence states.",
            },
        ]
    if lens == "expansion":
        rank = next(
            (
                item["rank"]
                for item in result.get("corridor_rankings", [])
                if item["corridor_key"] == result["corridor_key"]
            ),
            None,
        )
        return [
            {
                "label": "Expansion rank",
                "value": f"#{rank}" if rank else "—",
                "detail": "Rank among the sample merchant demo corridors.",
            },
            {
                "label": "Market readiness",
                "value": corridor_cfg["market_readiness"],
                "detail": corridor_cfg["launch_readiness"],
            },
            {
                "label": "Ecosystem support",
                "value": corridor_cfg["ecosystem_support"],
                "detail": "Blend of rail maturity and corridor precedent.",
            },
            {
                "label": "Canopy recommendation",
                "value": recommended_card["rail"],
                "detail": "Current best fit for this corridor inside the shortlist.",
            },
        ]
    return [
        {
            "label": "Canopy recommendation",
            "value": recommended_card["rail"],
            "detail": "Best current corridor fit across live measured rails.",
        },
        {
            "label": "Corridor viability",
            "value": _corridor_viability_label(recommended_card["corridor_viability_score"]),
            "detail": corridor_cfg["launch_readiness"],
        },
        {
            "label": "Ecosystem maturity",
            "value": corridor_cfg["ecosystem_support"],
            "detail": f"{recommended_card['rail']} shows the strongest current support story.",
        },
        {
            "label": "Evidence confidence",
            "value": recommended_card["evidence_confidence_label"],
            "detail": "Freshness, evidence quality, and flag penalties already reflected in the score.",
        },
    ]


def _build_lens_summary(result: dict, corridor_cfg: dict, recommended_card: dict) -> str:
    lens = result["lens"]
    if lens == "treasury":
        return (
            f"{result['corridor']} leans {recommended_card['rail']} for treasury work. "
            f"The flow signal looks {recommended_card['liquidity_proxy_label'].lower()} and the transfer rhythm looks "
            f"{recommended_card['transfer_continuity_label'].lower()}. Useful signal. Not a promise."
        )
    if lens == "risk":
        return (
            f"{result['corridor']} still points to {recommended_card['rail']}, but this is not a clean yes yet. "
            f"Read the corridor as {corridor_cfg['launch_readiness'].lower()} with {corridor_cfg['regulatory_exposure'].lower()} "
            f"regulatory exposure and {len(result['open_questions'])} real diligence items still open."
        )
    if lens == "expansion":
        return (
            f"{result['corridor']} is in the shortlist and {recommended_card['rail']} is its best current lane. "
            f"The real question is not who wins inside the corridor. It is whether this corridor deserves to move first."
        )
    return (
        f"{result['corridor']} leans {recommended_card['rail']}. Why? It does the best job balancing corridor fit, ecosystem support, "
        f"and live signal in this demo build."
    )


def _build_lens_why_route(result: dict, corridor_cfg: dict, recommended_card: dict, alternative_card: dict) -> List[str]:
    lens = result["lens"]
    if lens == "treasury":
        return [
            (
                f"{recommended_card['rail']} is the best treasury candidate because the flow signal reads as "
                f"{recommended_card['liquidity_proxy_label'].lower()} and the transfer rhythm reads as "
                f"{recommended_card['transfer_continuity_label'].lower()}."
            ),
            (
                f"The observed network cost is {recommended_card['estimated_fee_label']} per transfer versus "
                f"{alternative_card['estimated_fee_label']} on {alternative_card['rail']}. Nice. But treasury still cares more about whether the thing behaves."
            ),
            (
                "The liquidity proxy is cleaned-up payment activity. Routers, bridge-like flows, treasury loops, zero-value transfers, and dust are stripped out."
            ),
            (
                f"Before anyone gets cute, treasury still needs payout-counterparty validation at real ticket size."
            ),
        ]
    if lens == "risk":
        return [
            (
                f"{recommended_card['rail']} still ranks first, but this assumes {corridor_cfg['launch_readiness'].lower()}, not full production readiness."
            ),
            (
                f"Risk review should stare hard at the {len(result['open_questions'])} open items, especially payout-counterparty reliability and local regulatory interpretation."
            ),
            (
                "This recommendation is scoped to the currently supported live rails for the selected stablecoin."
            ),
            (
                "Canopy is not moving the money. It is helping people pick the road."
            ),
        ]
    if lens == "expansion":
        return [
            (
                f"{result['corridor']} is currently framed as {corridor_cfg['launch_readiness'].lower()}. That is the starting point for expansion."
            ),
            (
                f"{recommended_card['rail']} is the best rail in this corridor today, but the bigger question is whether this corridor should outrank the others."
            ),
            (
                f"Market readiness reads as {corridor_cfg['market_readiness'].lower()} with {corridor_cfg['ecosystem_support'].lower()} ecosystem support in the current model."
            ),
            (
                "Do not confuse winning one corridor with winning the roadmap."
            ),
        ]
    return [
        (
            (
                f"{recommended_card['rail']} is currently the only active live-supported rail for this stablecoin in the workspace."
                if recommended_card["rail"] == alternative_card["rail"]
                else f"{recommended_card['rail']} wins because it beats {alternative_card['rail']} on the whole scorecard, not just gas."
            )
        ),
        (
            f"This corridor reads as {corridor_cfg['launch_readiness'].lower()} with {corridor_cfg['ecosystem_support'].lower()} ecosystem support. Promising, yes. Finished, no."
        ),
        (
            "We care more about whether money lands cleanly than who has the sexier block time."
        ),
        (
            "Canopy is the routing layer. It is here to tell you which road looks open, not to cosplay as the whole payment stack."
        ),
    ]


def _build_lens_section_titles(lens: str) -> dict:
    if lens == "treasury":
        return {
            "why_eyebrow": "Treasury View",
            "why_title": "Why this rail fits treasury best",
            "scenario_eyebrow": "Settlement Inputs",
            "scenario_title": "Treasury assumptions in scope",
            "compliance_eyebrow": "Operational Coverage",
            "compliance_title": "What looks operational vs what still needs validation",
            "solved_title": "Operationally visible now",
            "open_title": "Treasury diligence still open",
            "corridor_eyebrow": "Corridor Comparison",
            "corridor_title": "How the demo corridors stack up",
        }
    if lens == "risk":
        return {
            "why_eyebrow": "Risk View",
            "why_title": "Why this rail survives the current risk lens",
            "scenario_eyebrow": "Review Inputs",
            "scenario_title": "Assumptions that drive the risk readout",
            "compliance_eyebrow": "Compliance Panel",
            "compliance_title": "Solved infrastructure vs unresolved diligence",
            "solved_title": "Solved infrastructure",
            "open_title": "Open questions",
            "corridor_eyebrow": "Corridor Comparison",
            "corridor_title": "How the demo corridors compare for risk",
        }
    if lens == "expansion":
        return {
            "why_eyebrow": "Expansion View",
            "why_title": "Why this corridor sits where it does",
            "scenario_eyebrow": "Scenario Inputs",
            "scenario_title": "Current assumptions behind this corridor readout",
            "compliance_eyebrow": "Expansion Risks",
            "compliance_title": "What looks ready vs what still blocks expansion",
            "solved_title": "Visible expansion support",
            "open_title": "What still blocks expansion",
            "corridor_eyebrow": "Demo Ranking",
            "corridor_title": "Which corridors should move next",
        }
    return {
        "why_eyebrow": "Strategy View",
        "why_title": "Why this rail leads the launch decision",
        "scenario_eyebrow": "Scenario Inputs",
        "scenario_title": "Current-state assumptions",
        "compliance_eyebrow": "Compliance & Operations",
        "compliance_title": "Solved infrastructure vs open questions",
        "solved_title": "Solved infrastructure",
        "open_title": "Open questions",
        "corridor_eyebrow": "Corridor Comparison",
        "corridor_title": "How the demo corridors compare",
    }


def _build_lens_committee_summary(
    lens: str,
    base_summary: str,
    result: dict,
    corridor_cfg: dict,
    recommended_card: dict,
) -> str:
    if lens == "treasury":
        return (
            f"{base_summary} Treasury interpretation: {recommended_card['rail']} currently looks strongest because the "
            f"liquidity proxy reads as {recommended_card['liquidity_proxy_label'].lower()} and transfer continuity reads as "
            f"{recommended_card['transfer_continuity_label'].lower()}. This should be interpreted as directional settlement "
            "capacity, not guaranteed executable size."
        )
    if lens == "risk":
        return (
            f"{base_summary} Risk interpretation: treat {corridor_cfg['label']} as {corridor_cfg['launch_readiness'].lower()} "
            f"with {corridor_cfg['regulatory_exposure'].lower()} regulatory exposure and {len(result['open_questions'])} open diligence items."
        )
    if lens == "expansion":
        rank = next(
            (
                item["rank"]
                for item in result.get("corridor_rankings", [])
                if item["corridor_key"] == result["corridor_key"]
            ),
            None,
        )
        return (
            f"{base_summary} Expansion interpretation: {corridor_cfg['label']} currently ranks "
            f"{'#' + str(rank) if rank else 'within the shortlist'} across the current discovery corridors, with "
            f"{corridor_cfg['market_readiness'].lower()} market readiness and {corridor_cfg['ecosystem_support'].lower()} ecosystem support."
        )
    return (
        f"{base_summary} Strategy interpretation: {corridor_cfg['label']} is best framed as "
        f"{corridor_cfg['launch_readiness'].lower()}, with {recommended_card['rail']} as the current leading rail."
    )


def _build_committee_summary(
    *,
    result: dict,
    corridor_cfg: dict,
    recommended_card: dict,
    alternative_card: dict,
    lens_summary: str,
    lens_committee_summary: str,
) -> str:
    transfer_winner_card = next(
        (rail for rail in result["rails"] if rail["rail"] == result["transfer_winner"]),
        recommended_card,
    )
    lines = [
        f"Decision lens: {result['active_lens']['label']}",
        f"Primary question: {result['active_lens']['key_question']}",
        "",
        "Scenario assumptions",
        f"- Corridor: {result['corridor']}",
        f"- Ticket size: {_fmt_usd(result['amount_usdc'])}",
        f"- Monthly corridor volume: {_fmt_usd(result['monthly_volume_usdc'])}",
        f"- Current rail cost assumption: {_fmt_percent(result['scenario']['current_rail_fee_pct'])}",
        f"- Current settlement assumption: {result['scenario']['current_rail_settlement_hours']:.0f} hours",
        f"- Current setup: {result['scenario']['current_setup']}",
        f"- Compliance sensitivity: {result['scenario']['compliance_sensitivity']}",
        "",
        "Dual-layer decision",
        f"- Transfer winner: {result['transfer_winner']} | landed amount {_fmt_usd(result['expected_landed_amount_usd'])}",
        (
            f"- Canopy recommendation: {recommended_card['rail']} | strategy score {recommended_card['strategy_score_label']} | "
            f"evidence confidence {recommended_card['evidence_confidence_label']}"
        ),
        "",
        "Rail comparison",
        (
            f"- Transfer winner rail: {transfer_winner_card['rail']} | network fee {transfer_winner_card['transfer_math']['provenance']['network_fee_usd'].lower()} "
            f"{_fmt_usd(transfer_winner_card['transfer_math']['network_fee_usd'])} | routing fee {_fmt_usd(transfer_winner_card['transfer_math']['routing_fee_usd'])} | "
            f"landed amount {_fmt_usd(transfer_winner_card['transfer_math']['landed_amount_usd'])}"
        ),
        (
            f"- Canopy recommendation rail: {recommended_card['rail']} | evidence mode {recommended_card['mode'].replace('_', ' ')} | "
            f"strategy score {recommended_card['strategy_score_label']} | Liquidity Proxy {recommended_card['liquidity_proxy_label']} | "
            f"evidence confidence {recommended_card['evidence_confidence_label']}"
        ),
        (
            f"- Alternative strategy rail: {alternative_card['rail']} | evidence mode {alternative_card['mode'].replace('_', ' ')} | "
            f"strategy score {alternative_card['strategy_score_label']} | evidence confidence {alternative_card['evidence_confidence_label']}"
            if alternative_card["rail"] != recommended_card["rail"]
            else f"- Alternative strategy rail: none | {recommended_card['rail']} is the only active live-supported option for this stablecoin right now"
        ),
        "",
        "Estimated cost delta",
        f"- Baseline monthly cost: {_fmt_usd(result['baseline_comparison']['baseline_monthly_cost_usd'])}",
        f"- Stablecoin monthly network cost: {_fmt_usd(result['baseline_comparison']['stablecoin_monthly_cost_usd'])}",
        f"- Estimated monthly savings vs current rail: {result['baseline_comparison']['savings_vs_baseline_label']}",
        "",
        "Key evidence",
        f"- {lens_summary}",
        f"- Canopy recommendation payout predictability framing: {recommended_card['settlement_timing']}",
        f"- {result['route_mode_note']}",
        f"- {API_WORKFLOW_NOTE}",
        "",
        "Unresolved risks",
    ]

    for item in result["open_questions"]:
        lines.append(f"- {item}")

    lines.extend(
        [
            "",
            "Committee framing",
            lens_committee_summary,
        ]
    )
    return "\n".join(lines)


def _build_corridor_rankings(lens: str) -> List[dict]:
    rankings = []
    for cfg in get_corridors():
        result = _get_route_core(
            origin=cfg["origin"],
            destination=cfg["destination"],
            amount_usdc=cfg["default_amount_usdc"],
            time_sensitivity="standard",
            monthly_volume_usdc=cfg["default_monthly_volume_usdc"],
            current_rail_fee_pct=cfg["default_baseline_fee_pct"],
            current_rail_settlement_hours=cfg["default_baseline_settlement_hours"],
            current_setup=cfg["default_current_setup"],
            compliance_sensitivity="medium",
            lens=lens,
            include_corridor_rankings=False,
        )
        rankings.append(
            {
                "corridor_key": result["corridor_key"],
                "corridor": result["corridor"],
                "recommended_rail": result["recommended_rail"],
                "market_readiness": result["market_readiness"],
                "ecosystem_support": result["ecosystem_support"],
                "regulatory_exposure": result["regulatory_exposure"],
                "launch_readiness": result["launch_readiness"],
                "decision_score": result["decision_score"],
                "decision_score_label": result["decision_score_label"],
                "confidence_label": result["confidence_label"],
                "key_reason": result["why_this_route"][0],
                "risk_note": result["open_questions"][0] if result["open_questions"] else "Additional diligence needed.",
            }
        )

    ranked = sorted(
        rankings,
        key=lambda item: (-item["decision_score"], -_maturity_score(item["market_readiness"]), -_maturity_score(item["ecosystem_support"])),
    )
    for idx, item in enumerate(ranked, start=1):
        item["rank"] = idx
    return ranked


def _get_route_core(
    origin: str = "US",
    destination: str = "BR",
    amount_usdc: float = 50_000,
    time_sensitivity: str = "standard",
    monthly_volume_usdc: Optional[float] = None,
    current_rail_fee_pct: Optional[float] = None,
    current_rail_settlement_hours: Optional[float] = None,
    current_setup: Optional[str] = None,
    compliance_sensitivity: str = "medium",
    lens: str = DEFAULT_LENS,
    token: str = DEFAULT_TOKEN,
    include_corridor_rankings: bool = True,
    include_corridor_best: bool = True,
) -> dict:
    lens = _normalize_lens(lens)
    token = normalize_token(token)
    cache = get_cache()
    cache_age = get_cache_age_seconds()
    native_prices_live = cache.get("native_prices_live", cache.get("eth_price_live", True))
    polygon_runtime = _get_chain_runtime(cache, "Polygon", cache_age, token)
    eth_runtime = _get_chain_runtime(cache, "Ethereum", cache_age, token)
    active_runtimes = [
        runtime
        for chain, runtime in (("Polygon", polygon_runtime), ("Ethereum", eth_runtime))
        if is_active_coverage(token, chain)
    ]
    is_bootstrap = bool(active_runtimes) and all(runtime["using_bootstrap_data"] for runtime in active_runtimes)
    decision_id = f"decision_{uuid4().hex[:12]}"
    request_id = get_request_id()

    corridor_cfg = _get_corridor_config(origin, destination)
    polygon_data = polygon_runtime["data"]
    eth_data = eth_runtime["data"]

    monthly_volume_usdc = monthly_volume_usdc or corridor_cfg["default_monthly_volume_usdc"]
    current_rail_fee_pct = (
        corridor_cfg["default_baseline_fee_pct"]
        if current_rail_fee_pct is None
        else current_rail_fee_pct
    )
    current_rail_settlement_hours = (
        corridor_cfg["default_baseline_settlement_hours"]
        if current_rail_settlement_hours is None
        else current_rail_settlement_hours
    )
    current_setup = current_setup or corridor_cfg["default_current_setup"]

    if time_sensitivity == "urgent":
        polygon_fee = polygon_data.get("p90_fee_usd", 0.03)
        eth_fee = eth_data.get("p90_fee_usd", 6.10)
        fee_basis_label = "p90 fee per transfer"
    else:
        polygon_fee = polygon_data.get("avg_fee_usd", 0.01)
        eth_fee = eth_data.get("avg_fee_usd", 3.20)
        fee_basis_label = "avg fee per transfer"

    live_cards = []
    if is_active_coverage(token, "Polygon"):
        live_cards.append(
            _build_live_rail_card(
                rail="Polygon",
                token=token,
                chain_runtime=polygon_runtime,
                fee_usd=polygon_fee,
                native_prices_live=native_prices_live,
                maturity=corridor_cfg["polygon_maturity"],
                settlement_timing="Actionable in seconds; payout predictability depends on treasury ops and local off-ramp readiness.",
                note=f"Low-cost {token} settlement on Polygon remains the cheapest measured route when the corridor supports it.",
            )
        )
    if is_active_coverage(token, "Ethereum"):
        live_cards.append(
            _build_live_rail_card(
                rail="Ethereum",
                token=token,
                chain_runtime=eth_runtime,
                fee_usd=eth_fee,
                native_prices_live=native_prices_live,
                maturity="High",
                settlement_timing="Higher-trust settlement baseline; slower and more expensive, but often easier to defend operationally.",
                note=f"Ethereum remains the institutionally familiar {token} baseline when trust or support matters more than raw cost.",
            )
        )

    rails, transfer_winner, recommended_card, alternative_card = _score_rails(
        live_cards,
        corridor_cfg=corridor_cfg,
        corridor_key=corridor_cfg["key"],
        amount_usdc=amount_usdc,
    )

    tx_count_estimate = max(monthly_volume_usdc / max(amount_usdc, 1), 1)
    stablecoin_monthly_cost_usd = recommended_card["estimated_fee_usd"] * tx_count_estimate
    baseline_monthly_cost_usd = monthly_volume_usdc * (current_rail_fee_pct / 100)
    savings_vs_baseline_usd = max(baseline_monthly_cost_usd - stablecoin_monthly_cost_usd, 0)
    baseline_settlement_label = (
        f"~{current_rail_settlement_hours:.0f}h current-state assumption"
    )

    base_committee_summary = _build_base_committee_summary(
        corridor_cfg=corridor_cfg,
        recommended_card=recommended_card,
        token=token,
        baseline_fee_pct=current_rail_fee_pct,
        baseline_settlement_hours=current_rail_settlement_hours,
        current_setup=current_setup,
        monthly_volume_usdc=monthly_volume_usdc,
        tx_count_estimate=tx_count_estimate,
        baseline_monthly_cost_usd=baseline_monthly_cost_usd,
        stablecoin_monthly_cost_usd=stablecoin_monthly_cost_usd,
    )

    caveats = []
    if is_bootstrap:
        caveats.append("The live traffic layer is still waking up, so you are seeing bootstrap numbers on the live rails.")
    if not native_prices_live:
        caveats.append("Native token prices are on fallback values right now.")
    if recommended_card.get("data_status") == "error":
        caveats.append("The recommended rail is currently using the last known good read because the latest refresh failed.")
    elif recommended_card.get("freshness_level") in {"stale", "critical"}:
        caveats.append("The recommended rail is not fully fresh right now, so treat the recommendation as degraded evidence.")
    if cache_age is not None and cache_age > 3600:
        caveats.append("The live rail read is over an hour old. Treat it carefully.")
    if recommended_card.get("adjusted_transfer_count", 0) < 50:
        caveats.append("The sample is light. Directional only, not gospel.")
    fresh_live_rails = [
        rail for rail in rails if rail.get("mode") == "live_measured" and rail.get("data_status") == "fresh"
    ]
    if len(fresh_live_rails) == 1:
        caveats.append("Only one live rail is currently fresh, so the comparison is effectively single-live-rail with a fallback comparator.")

    data_health_summary = {
        "global_status": cache.get("status", "initializing"),
        "fresh_live_rail_count": len(fresh_live_rails),
        "required_live_rail_count": len(live_cards),
        "chains": {
            "Polygon": {
                "coverage_state": get_token_coverage_state(token, "Polygon"),
                "status": polygon_runtime["status"],
                "freshness_level": polygon_runtime["freshness_level"],
                "cache_age_seconds": polygon_runtime["age_seconds"],
                "last_success_at": polygon_runtime["last_success_at"],
                "last_attempt_at": polygon_runtime["last_attempt_at"],
                "last_error": polygon_runtime["last_error"],
            },
            "Ethereum": {
                "coverage_state": get_token_coverage_state(token, "Ethereum"),
                "status": eth_runtime["status"],
                "freshness_level": eth_runtime["freshness_level"],
                "cache_age_seconds": eth_runtime["age_seconds"],
                "last_success_at": eth_runtime["last_success_at"],
                "last_attempt_at": eth_runtime["last_attempt_at"],
                "last_error": eth_runtime["last_error"],
            },
        },
    }

    result = {
        "workspace": "v5_route_advisor",
        "request_id": request_id,
        "decision_id": decision_id,
        "lens": lens,
        "lenses": list(LENS_CONFIGS.values()),
        "active_lens": LENS_CONFIGS[lens],
        "transfer_winner": transfer_winner["rail"],
        "canopy_recommendation": recommended_card["rail"],
        "recommended_rail": recommended_card["rail"],
        "alternative_rail": alternative_card["rail"],
        "token": token,
        "coverage_state": _selected_token_coverage_state(token),
        "corridor": corridor_cfg["label"],
        "corridor_key": corridor_cfg["key"],
        "corridor_slug": corridor_cfg["corridor_slug"],
        "source_country": corridor_cfg["source_country"],
        "destination_country": corridor_cfg["destination_country"],
        "destination_city": corridor_cfg["destination_city"],
        "map_viewport": corridor_cfg["map_viewport"],
        "corridor_note": corridor_cfg["corridor_note"],
        "amount_usdc": amount_usdc,
        "monthly_volume_usdc": monthly_volume_usdc,
        "time_sensitivity": time_sensitivity,
        "fee_basis_label": fee_basis_label,
        "expected_fee_usd": recommended_card["estimated_fee_usd"],
        "expected_fee_label": recommended_card["estimated_fee_label"],
        "alternative_fee_usd": alternative_card["estimated_fee_usd"],
        "alternative_fee_label": alternative_card["estimated_fee_label"],
        "expected_landed_amount_usd": transfer_winner["transfer_math"]["landed_amount_usd"],
        "expected_landed_amount_label": _fmt_usd(transfer_winner["transfer_math"]["landed_amount_usd"]),
        "confidence": recommended_card["confidence"],
        "confidence_label": recommended_card["confidence_label"],
        "evidence_confidence": recommended_card["evidence_confidence"],
        "evidence_confidence_label": recommended_card["evidence_confidence_label"],
        "strategy_score": recommended_card["strategy_score"],
        "strategy_score_label": recommended_card["strategy_score_label"],
        "decision_score": recommended_card["decision_score"],
        "decision_score_label": recommended_card["decision_score_label"],
        "corridor_viability": _corridor_viability_label(recommended_card["corridor_viability_score"]),
        "market_readiness": corridor_cfg["market_readiness"],
        "ecosystem_support": corridor_cfg["ecosystem_support"],
        "regulatory_exposure": corridor_cfg["regulatory_exposure"],
        "launch_readiness": corridor_cfg["launch_readiness"],
        "api_workflow_note": API_WORKFLOW_NOTE,
        "cache_age_seconds": cache_age,
        "global_data_status": cache.get("status", "initializing"),
        "data_health_summary": data_health_summary,
        "degraded_recommendation_warning": (
            "Recommended rail is not fully fresh; Canopy is serving a degraded recommendation."
            if recommended_card.get("mode") == "live_measured"
            and (
                recommended_card.get("data_status") == "error"
                or recommended_card.get("freshness_level") in {"stale", "critical"}
            )
            else None
        ),
        "is_bootstrap": is_bootstrap,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "caveat": " | ".join(caveats),
        "scenario": {
            "current_setup": current_setup,
            "current_rail_fee_pct": current_rail_fee_pct,
            "current_rail_settlement_hours": current_rail_settlement_hours,
            "baseline_settlement_label": baseline_settlement_label,
            "compliance_sensitivity": compliance_sensitivity,
            "tx_count_estimate": round(tx_count_estimate, 1),
        },
        "baseline_comparison": {
            "baseline_monthly_cost_usd": round(baseline_monthly_cost_usd, 2),
            "stablecoin_monthly_cost_usd": round(stablecoin_monthly_cost_usd, 2),
            "savings_vs_baseline_usd": round(savings_vs_baseline_usd, 2),
            "savings_vs_baseline_label": _fmt_usd(savings_vs_baseline_usd),
        },
        "rails": rails,
        "why_this_route": [],
        "solved_infrastructure": corridor_cfg["solved_infrastructure"],
        "open_questions": corridor_cfg["open_questions"],
        "committee_summary": base_committee_summary,
        "base_committee_summary": base_committee_summary,
        "liquidity_method_note": recommended_card.get("activity_filter_method"),
        "route_mode_note": (
            f"This recommendation compares only live-supported {token} rails in the current Canopy stack."
        ),
        "data_freshness_polygon": polygon_runtime.get("last_success_at") or polygon_data.get("freshness_timestamp"),
        "data_freshness_eth": eth_runtime.get("last_success_at") or eth_data.get("freshness_timestamp"),
        "volume_usdc_polygon": polygon_data.get("volume_usdc"),
        "eth_price_usd": cache.get("eth_price_usd", 3500),
        "polygon_price_usd": cache.get("polygon_price_usd", 0.10),
        "native_prices_live": native_prices_live,
        "eth_price_live": cache.get("eth_price_live", False),
        "transfer_winner_transfer_math": transfer_winner["transfer_math"],
    }

    if include_corridor_rankings:
        result["corridor_rankings"] = _build_corridor_rankings(lens)
        result["corridor_comparison_note"] = LENS_CONFIGS[lens]["comparison_note"]
    else:
        result["corridor_rankings"] = []
        result["corridor_comparison_note"] = LENS_CONFIGS[lens]["comparison_note"]

    result["rails"] = [
            {
                **annotated_rail,
                **_build_trust_profile(
                    annotated_rail,
                    corridor_cfg=corridor_cfg,
                    lens=lens,
                    native_prices_live=native_prices_live,
                    is_bootstrap=is_bootstrap,
                ),
        }
        for annotated_rail in (_annotate_rail_for_lens(rail, lens) for rail in result["rails"])
    ]
    result["lens_highlights"] = _build_lens_highlights(result, corridor_cfg, recommended_card)
    result["lens_summary"] = _build_lens_summary(result, corridor_cfg, recommended_card)
    result["why_this_route"] = _build_lens_why_route(result, corridor_cfg, recommended_card, alternative_card)
    result["section_titles"] = _build_lens_section_titles(lens)
    lens_committee_summary = _build_lens_committee_summary(
        lens=lens,
        base_summary=base_committee_summary,
        result=result,
        corridor_cfg=corridor_cfg,
        recommended_card=recommended_card,
    )
    result["committee_summary"] = _build_committee_summary(
        result=result,
        corridor_cfg=corridor_cfg,
        recommended_card=recommended_card,
        alternative_card=alternative_card,
        lens_summary=result["lens_summary"],
        lens_committee_summary=lens_committee_summary,
    )

    result["evidence_packet"] = _build_route_evidence(result, recommended_card)

    if include_corridor_best:
        landscape = get_landscape(
            origin=origin,
            destination=destination,
            amount_usdc=amount_usdc,
            time_sensitivity=time_sensitivity,
            monthly_volume_usdc=monthly_volume_usdc,
            current_rail_fee_pct=current_rail_fee_pct,
            current_rail_settlement_hours=current_rail_settlement_hours,
            current_setup=current_setup,
            compliance_sensitivity=compliance_sensitivity,
            lens=lens,
        )
        corridor_best = landscape.get("corridor_best_supported")
        result["corridor_best_supported"] = {
            **corridor_best,
            "is_selected_token": corridor_best.get("token") == token,
            "is_selected_route": (
                corridor_best.get("token") == token
                and corridor_best.get("rail") == result.get("recommended_rail")
            ),
        }
    else:
        result["corridor_best_supported"] = None

    return attach_corridor_analytics(result)


def get_landscape(
    origin: str = "US",
    destination: str = "BR",
    amount_usdc: float = 50_000,
    time_sensitivity: str = "standard",
    monthly_volume_usdc: Optional[float] = None,
    current_rail_fee_pct: Optional[float] = None,
    current_rail_settlement_hours: Optional[float] = None,
    current_setup: Optional[str] = None,
    compliance_sensitivity: str = "medium",
    lens: str = DEFAULT_LENS,
) -> dict:
    token_routes = []
    for token in get_supported_tokens():
        route = _get_route_core(
            origin=origin,
            destination=destination,
            amount_usdc=amount_usdc,
            time_sensitivity=time_sensitivity,
            monthly_volume_usdc=monthly_volume_usdc,
            current_rail_fee_pct=current_rail_fee_pct,
            current_rail_settlement_hours=current_rail_settlement_hours,
            current_setup=current_setup,
            compliance_sensitivity=compliance_sensitivity,
            lens=lens,
            token=token,
            include_corridor_rankings=False,
            include_corridor_best=False,
        )
        token_routes.append(route)

    def _landscape_rank_key(item: dict) -> tuple:
        recommended_rail = item.get("recommended_rail")
        recommended_card = next(
            (rail for rail in item.get("rails", []) if rail.get("rail") == recommended_rail),
            {},
        )
        return (
            item.get("strategy_score", 0),
            item.get("evidence_confidence", 0),
            -len(recommended_card.get("adversarial_flags", []) or []),
            recommended_card.get("unique_senders", 0),
            -(recommended_card.get("bridge_share", 1.0) or 1.0),
            -(item.get("expected_fee_usd") or 999999),
        )

    corridor_best_route = max(
        token_routes,
        key=_landscape_rank_key,
    )
    corridor_best_supported = {
        "token": corridor_best_route.get("token"),
        "rail": corridor_best_route.get("recommended_rail"),
        "strategy_score": corridor_best_route.get("strategy_score"),
        "strategy_score_label": corridor_best_route.get("strategy_score_label"),
        "coverage_state": corridor_best_route.get("coverage_state"),
        "label": "Corridor Best Among Supported Routes",
    }

    tiles = []
    for route in token_routes:
        tiles.append(
            {
                "token": route.get("token"),
                "coverage_state": route.get("coverage_state"),
                "recommended_rail": route.get("recommended_rail"),
                "recommended_rail_status": next(
                    (
                        rail.get("data_status")
                        for rail in route.get("rails", [])
                        if rail.get("rail") == route.get("recommended_rail")
                    ),
                    "unknown",
                ),
                "support_label": "Active Coverage" if route.get("coverage_state") == COVERAGE_ACTIVE else "Limited Coverage",
                "estimated_fee": route.get("evidence_packet", {}).get("expected_fee_usd"),
                "recommendation_confidence": route.get("evidence_packet", {}).get("recommendation_confidence"),
                "trust_score": route.get("evidence_packet", {}).get("trust_score"),
                "corridor_best_supported": route.get("token") == corridor_best_supported["token"]
                and route.get("recommended_rail") == corridor_best_supported["rail"],
            }
        )

    return {
        "corridor": token_routes[0].get("corridor") if token_routes else f"{origin} -> {destination}",
        "corridor_key": token_routes[0].get("corridor_key") if token_routes else f"{origin}-{destination}",
        "corridor_slug": token_routes[0].get("corridor_slug") if token_routes else f"{origin.lower()}-{destination.lower()}",
        "lens": lens,
        "amount_usdc": amount_usdc,
        "default_token": DEFAULT_TOKEN,
        "tiles": tiles,
        "corridor_best_supported": corridor_best_supported,
        "global_data_status": token_routes[0].get("global_data_status") if token_routes else "initializing",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_route(
    origin: str = "US",
    destination: str = "BR",
    amount_usdc: float = 50_000,
    time_sensitivity: str = "standard",
    monthly_volume_usdc: Optional[float] = None,
    current_rail_fee_pct: Optional[float] = None,
    current_rail_settlement_hours: Optional[float] = None,
    current_setup: Optional[str] = None,
    compliance_sensitivity: str = "medium",
    lens: str = DEFAULT_LENS,
    token: str = DEFAULT_TOKEN,
) -> dict:
    """Return the sample merchant demo route response."""
    result = _get_route_core(
        origin=origin,
        destination=destination,
        amount_usdc=amount_usdc,
        time_sensitivity=time_sensitivity,
        monthly_volume_usdc=monthly_volume_usdc,
        current_rail_fee_pct=current_rail_fee_pct,
        current_rail_settlement_hours=current_rail_settlement_hours,
        current_setup=current_setup,
        compliance_sensitivity=compliance_sensitivity,
        lens=lens,
        token=token,
    )
    log_event(
        logger,
        "route.decision.generated",
        request_id=result.get("request_id"),
        decision_id=result.get("decision_id"),
        corridor=result.get("corridor"),
        corridor_key=result.get("corridor_key"),
        lens=result.get("lens"),
        token=result.get("token"),
        recommendation=result.get("recommended_rail"),
        alternative_rail=result.get("alternative_rail"),
        global_data_status=result.get("global_data_status"),
        degraded_recommendation_warning=result.get("degraded_recommendation_warning"),
        scenario=result.get("scenario"),
        rails=[
            {
                "rail": rail.get("rail"),
                "data_status": rail.get("data_status"),
                "freshness_level": rail.get("freshness_level"),
                "cache_age_seconds": rail.get("cache_age_seconds"),
                "estimated_fee_usd": rail.get("estimated_fee_usd"),
                "liquidity_score": rail.get("liquidity_score_v4"),
                "trust_score": rail.get("trust_score_v4"),
                "integrity_score": rail.get("integrity_score"),
                "strategy_score": rail.get("strategy_score"),
                "freshness_penalty_factor": rail.get("freshness_penalty_factor"),
                "adversarial_flags": rail.get("adversarial_flags"),
            }
            for rail in result.get("rails", [])
        ],
    )
    return result


def get_preview(origin: str = "US", destination: str = "BR") -> dict:
    result = _get_route_core(
        origin=origin,
        destination=destination,
        amount_usdc=5_000,
        time_sensitivity="standard",
        lens=DEFAULT_LENS,
        token=DEFAULT_TOKEN,
        include_corridor_rankings=False,
        include_corridor_best=False,
    )
    return {
        "recommended_rail": result["recommended_rail"],
        "token": result["token"],
        "corridor": result["corridor"],
        "corridor_note": result["corridor_note"],
        "is_bootstrap": result["is_bootstrap"],
        "timestamp": result["timestamp"],
    }
