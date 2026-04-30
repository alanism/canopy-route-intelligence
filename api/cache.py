"""
api/cache.py — Async background poller with per-chain cache health.

Cache schema keeps one global dict in memory with independent chain states.
Reference reassignment (_cache = new_dict) remains the only mutation pattern.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from api.eth_price import get_native_prices
from data.query import CHAIN_CONFIGS, run_chain_query, run_chain_token_query
from services.corridor_analytics import build_corridor_base_summary
from services.corridor_config import get_corridors
from services.logging_utils import log_event
from services.query_metrics import get_query_metrics_snapshot
from services.runtime_mode import get_runtime_mode_label, is_demo_mode
from services.summary_store import (
    fetch_fee_activity_summaries,
    init_summary_store,
    upsert_corridor_summary,
    upsert_fee_activity_summary,
)
from services.token_registry import DEFAULT_TOKEN, get_active_combinations, iter_active_tokens_for_chain

logger = logging.getLogger("sci-agent.cache")

FRESH_SECONDS = 15 * 60
CRITICAL_SECONDS = 60 * 60
POLL_INTERVAL = int(os.getenv("CANOPY_POLL_INTERVAL_SECONDS", "300"))
BACKOFF_INTERVAL = int(os.getenv("CANOPY_POLL_BACKOFF_SECONDS", "30"))
LIVE_CHAINS = ("Polygon", "Ethereum")


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _age_seconds(timestamp: Optional[str]) -> Optional[int]:
    parsed = _parse_iso(timestamp)
    if parsed is None:
        return None
    return int((datetime.now(timezone.utc) - parsed).total_seconds())


def _freshness_level(age_seconds: Optional[int]) -> str:
    if age_seconds is None:
        return "unknown"
    if age_seconds > CRITICAL_SECONDS:
        return "critical"
    if age_seconds > FRESH_SECONDS:
        return "stale"
    return "fresh"


def _bootstrap_chain_data(chain: str, token: str = DEFAULT_TOKEN) -> dict:
    if chain == "Polygon":
        return {
            "chain": "Polygon",
            "token": token,
            "avg_fee_usd": 0.01,
            "median_fee_usd": 0.008,
            "p90_fee_usd": 0.03,
            "transfer_count": 0,
            "volume_usdc": None,
            "adjusted_transaction_count": 0,
            "adjusted_transfer_count": 0,
            "adjusted_volume_usdc": None,
            "adjusted_freshness_timestamp": None,
            "minutes_since_last_adjusted_transfer": None,
            "avg_minutes_between_adjusted_transfers": None,
            "activity_filter_method": None,
            "window_used": "N/A",
            "freshness_timestamp": None,
            "native_price_used_usd": 0.10,
            "queried_at": None,
        }
    return {
        "chain": "Ethereum",
        "token": token,
        "avg_fee_usd": 3.20,
        "median_fee_usd": 2.80,
        "p90_fee_usd": 6.10,
        "transfer_count": 0,
        "volume_usdc": None,
        "adjusted_transaction_count": 0,
        "adjusted_transfer_count": 0,
        "adjusted_volume_usdc": None,
        "adjusted_freshness_timestamp": None,
        "minutes_since_last_adjusted_transfer": None,
        "avg_minutes_between_adjusted_transfers": None,
        "activity_filter_method": None,
        "window_used": "N/A",
        "freshness_timestamp": None,
        "native_price_used_usd": 3500,
        "queried_at": None,
    }


def _initial_token_state(chain: str, token: str) -> dict:
    return {
        "data": _bootstrap_chain_data(chain, token),
        "last_success_at": None,
        "last_attempt_at": None,
        "status": "initializing",
        "last_error": None,
        "poll_count": 0,
        "age_seconds": None,
        "freshness_level": "unknown",
        "using_bootstrap_data": True,
    }


def _demo_chain_data(chain: str, token: str = DEFAULT_TOKEN) -> dict:
    seeded_at = _utcnow_iso()
    if chain == "Polygon":
        return {
            "chain": "Polygon",
            "token": token,
            "avg_fee_usd": 0.02,
            "median_fee_usd": 0.01,
            "p90_fee_usd": 0.05,
            "transfer_count": 12384,
            "volume_usdc": 81250000.0,
            "adjusted_transaction_count": 10984,
            "adjusted_transfer_count": 10436,
            "adjusted_volume_usdc": 74820000.0,
            "adjusted_freshness_timestamp": seeded_at,
            "minutes_since_last_adjusted_transfer": 3,
            "avg_minutes_between_adjusted_transfers": 0.41,
            "activity_filter_method": "seeded_demo_summary",
            "window_used": "seeded_demo_3h",
            "freshness_timestamp": seeded_at,
            "native_price_used_usd": 0.10,
            "queried_at": seeded_at,
        }
    return {
        "chain": "Ethereum",
        "token": token,
        "avg_fee_usd": 2.94,
        "median_fee_usd": 2.52,
        "p90_fee_usd": 5.48,
        "transfer_count": 4612,
        "volume_usdc": 53400000.0,
        "adjusted_transaction_count": 4238,
        "adjusted_transfer_count": 4010,
        "adjusted_volume_usdc": 49750000.0,
        "adjusted_freshness_timestamp": seeded_at,
        "minutes_since_last_adjusted_transfer": 6,
        "avg_minutes_between_adjusted_transfers": 1.12,
        "activity_filter_method": "seeded_demo_summary",
        "window_used": "seeded_demo_3h",
        "freshness_timestamp": seeded_at,
        "native_price_used_usd": 3500.0,
        "queried_at": seeded_at,
    }


def _demo_token_state(chain: str, token: str) -> dict:
    data = _demo_chain_data(chain, token)
    age_seconds = _age_seconds(data["freshness_timestamp"])
    return {
        "data": data,
        "last_success_at": data["queried_at"],
        "last_attempt_at": data["queried_at"],
        "status": "fresh",
        "last_error": None,
        "poll_count": 1,
        "age_seconds": age_seconds,
        "freshness_level": _freshness_level(age_seconds),
        "using_bootstrap_data": False,
    }


def _derive_chain_status(tokens: dict) -> tuple[str, Optional[int], str, Optional[str], Optional[str], bool]:
    token_states = list(tokens.values())
    if not token_states:
        return ("initializing", None, "unknown", None, None, True)

    statuses = [state.get("status", "initializing") for state in token_states]
    age_values = [state.get("age_seconds") for state in token_states if state.get("age_seconds") is not None]
    freshness_values = [state.get("freshness_level", "unknown") for state in token_states]
    success_values = [state.get("last_success_at") for state in token_states if state.get("last_success_at")]
    attempt_values = [state.get("last_attempt_at") for state in token_states if state.get("last_attempt_at")]
    errors = [state.get("last_error") for state in token_states if state.get("last_error")]

    if all(status == "initializing" for status in statuses):
        status = "initializing"
    elif any(status == "fresh" for status in statuses):
        status = "fresh"
    elif any(status == "initializing" for status in statuses):
        status = "initializing"
    else:
        status = "error"

    freshness_level = "unknown"
    if any(level == "critical" for level in freshness_values):
        freshness_level = "critical"
    elif any(level == "stale" for level in freshness_values):
        freshness_level = "stale"
    elif any(level == "fresh" for level in freshness_values):
        freshness_level = "fresh"

    return (
        status,
        max(age_values) if age_values else None,
        freshness_level,
        max(success_values) if success_values else None,
        max(attempt_values) if attempt_values else None,
        all(state.get("using_bootstrap_data", False) for state in token_states),
    )


def _initial_chain_state(chain: str) -> dict:
    tokens = {token: _initial_token_state(chain, token) for token in iter_active_tokens_for_chain(chain)}
    (
        status,
        age_seconds,
        freshness_level,
        last_success_at,
        last_attempt_at,
        using_bootstrap_data,
    ) = _derive_chain_status(tokens)
    default_state = tokens.get(DEFAULT_TOKEN) or next(iter(tokens.values()))
    return {
        "data": default_state["data"],
        "tokens": tokens,
        "last_success_at": last_success_at,
        "last_attempt_at": last_attempt_at,
        "status": status,
        "last_error": None,
        "poll_count": 0,
        "age_seconds": age_seconds,
        "freshness_level": freshness_level,
        "using_bootstrap_data": using_bootstrap_data,
    }


def seed_demo_cache() -> dict:
    global _cache

    init_summary_store()
    seeded_at = _utcnow_iso()
    prices = {
        "ethereum": float(os.getenv("ETH_PRICE_FALLBACK", "3500")),
        "polygon": float(os.getenv("POLYGON_PRICE_FALLBACK", "0.10")),
    }
    next_chains = {}
    grouped_results = {chain: {} for chain in LIVE_CHAINS}

    for chain in LIVE_CHAINS:
        tokens = {token: _demo_token_state(chain, token) for token in iter_active_tokens_for_chain(chain)}
        status, age_seconds, freshness_level, last_success_at, last_attempt_at, _ = _derive_chain_status(tokens)
        default_state = tokens.get(DEFAULT_TOKEN) or next(iter(tokens.values()))
        next_chains[chain] = {
            "data": default_state["data"],
            "tokens": tokens,
            "last_success_at": last_success_at,
            "last_attempt_at": last_attempt_at,
            "status": status,
            "last_error": None,
            "poll_count": 1,
            "age_seconds": age_seconds,
            "freshness_level": freshness_level,
            "using_bootstrap_data": False,
        }
        grouped_results[chain] = {token: token_state["data"] for token, token_state in tokens.items()}

    _materialize_fee_activity(grouped_results, materialized_at=seeded_at)
    _cache = _build_global_cache(next_chains, prices=prices, prices_live=False, poll_count=1)
    _cache["mode"] = "demo"
    _materialize_corridor_summaries(cache_payload=_cache, materialized_at=seeded_at)
    log_event(
        logger,
        "cache.demo.seeded",
        mode=get_runtime_mode_label(),
        poll_count=_cache["poll_count"],
        chain_statuses={chain: _cache["chains"][chain]["status"] for chain in LIVE_CHAINS},
    )
    return _cache


def _derive_global_status(chains: dict) -> str:
    statuses = [chains[chain]["status"] for chain in LIVE_CHAINS]
    if all(status == "initializing" for status in statuses):
        return "initializing"
    fresh_count = sum(1 for chain in LIVE_CHAINS if chains[chain]["status"] == "fresh")
    if fresh_count == len(LIVE_CHAINS):
        return "ok"
    if fresh_count > 0:
        return "degraded"
    return "error"


def _max_success_timestamp(chains: dict) -> Optional[str]:
    timestamps = [chains[chain]["last_success_at"] for chain in LIVE_CHAINS if chains[chain]["last_success_at"]]
    return max(timestamps) if timestamps else None


def _worst_cache_age(chains: dict) -> Optional[int]:
    ages = [chains[chain]["age_seconds"] for chain in LIVE_CHAINS if chains[chain]["age_seconds"] is not None]
    return max(ages) if ages else None


def _build_global_cache(chains: dict, *, prices: dict, prices_live: bool, poll_count: int) -> dict:
    errors = [f"{chain}: {chains[chain]['last_error']}" for chain in LIVE_CHAINS if chains[chain]["last_error"]]
    status = _derive_global_status(chains)
    return {
        "chains": chains,
        "eth_price_usd": prices["ethereum"],
        "polygon_price_usd": prices["polygon"],
        "native_prices_live": prices_live,
        "eth_price_live": prices_live,
        "last_updated": _max_success_timestamp(chains),
        "status": status,
        "poll_count": poll_count,
        "last_error": " | ".join(errors) if errors else None,
        "is_bootstrap": all(chains[chain]["using_bootstrap_data"] for chain in LIVE_CHAINS),
        "cache_age_seconds": _worst_cache_age(chains),
    }


BOOTSTRAP = _build_global_cache(
    {chain: _initial_chain_state(chain) for chain in LIVE_CHAINS},
    prices={"ethereum": 3500, "polygon": 0.10},
    prices_live=False,
    poll_count=0,
)

_cache: dict = {**BOOTSTRAP}
_refresh_state: dict = {
    "status": "idle",
    "label": "Not measured",
    "indicator": "gray",
    "last_measured_refresh": None,
    "last_error": None,
}
_refresh_task: Optional[asyncio.Task] = None


def _set_refresh_state(
    *,
    status: str,
    label: str,
    indicator: str,
    last_measured_refresh: Optional[str] = None,
    last_error: Optional[str] = None,
) -> None:
    global _refresh_state
    next_refresh = dict(_refresh_state)
    next_refresh.update(
        {
            "status": status,
            "label": label,
            "indicator": indicator,
            "last_error": last_error,
        }
    )
    if last_measured_refresh is not None:
        next_refresh["last_measured_refresh"] = last_measured_refresh
    _refresh_state = next_refresh


def initialize_manual_refresh_state() -> None:
    global _cache, _refresh_task
    _cache = {**BOOTSTRAP}
    _refresh_task = None
    _set_refresh_state(
        status="idle",
        label="Not measured",
        indicator="gray",
        last_measured_refresh=None,
        last_error=None,
    )


def _materialize_fee_activity(results: dict, *, materialized_at: str) -> None:
    rows = []
    for token_results in results.values():
        for result in token_results.values():
            if isinstance(result, Exception) or result is None:
                continue
            rows.append({**result, "materialized_at": materialized_at})
    upsert_fee_activity_summary(rows)


def _build_cache_from_materialized_summaries(
    *,
    previous_cache: dict,
    prices: dict,
    prices_live: bool,
    poll_count: int,
    polled_at: str,
) -> dict:
    materialized_rows = {
        (row["chain"], row["token"]): row for row in fetch_fee_activity_summaries()
    }
    previous_chains = previous_cache["chains"]
    next_chains = {}
    grouped_results = {chain: {} for chain in LIVE_CHAINS}

    for chain, token in get_active_combinations():
        row = materialized_rows.get((chain, token))
        grouped_results[chain][token] = (
            {
                key: row[key]
                for key in row.keys()
                if key != "materialized_at"
            }
            if row is not None
            else RuntimeError("Missing materialized fee summary")
        )

    for chain in LIVE_CHAINS:
        next_chains[chain] = _build_chain_state(
            previous_chains[chain],
            chain=chain,
            token_results=grouped_results.get(chain, {}),
            polled_at=polled_at,
        )

    return _build_global_cache(
        next_chains,
        prices=prices,
        prices_live=prices_live,
        poll_count=poll_count,
    )


def _rail_seed_from_cache(cache_payload: dict, rail: str, token: str) -> dict:
    if rail in {"Polygon", "Ethereum"}:
        chain_state = cache_payload.get("chains", {}).get(rail, {})
        token_state = chain_state.get("tokens", {}).get(token)
        if token_state:
            data = token_state.get("data", {})
            return {
                "rail": rail,
                "mode": "live_measured",
                "adjusted_volume_usdc": data.get("adjusted_volume_usdc"),
                "volume_usdc": data.get("volume_usdc"),
                "adjusted_transfer_count": data.get("adjusted_transfer_count"),
                "transfer_count": data.get("transfer_count"),
                "confidence": 0.85 if token_state.get("status") == "fresh" else 0.55,
                "freshness_score": 1.0 if token_state.get("status") == "fresh" else 0.6,
            }
    return {
        "rail": rail,
        "mode": "historical_reference",
        "adjusted_volume_usdc": 0.0,
        "volume_usdc": 0.0,
        "adjusted_transfer_count": 0,
        "transfer_count": 0,
        "confidence": 0.45,
        "freshness_score": 0.45,
    }


def _materialize_corridor_summaries(*, cache_payload: dict, materialized_at: str) -> None:
    rows = []
    for corridor in get_corridors():
        corridor_id = corridor["key"]
        for token in {combo_token for _, combo_token in get_active_combinations()}:
            for rail in ("Polygon", "Ethereum", "Stellar"):
                base_summary = build_corridor_base_summary(
                    corridor_id,
                    _rail_seed_from_cache(cache_payload, rail, token),
                    rail=rail,
                    token=token,
                    time_range="24h",
                    allow_live_bigquery=False,
                )
                rows.append({**base_summary, "materialized_at": materialized_at})
    upsert_corridor_summary(rows)


def _build_token_state(previous: dict, *, result: Optional[dict], error: Optional[str], polled_at: str) -> dict:
    poll_count = int(previous.get("poll_count", 0)) + 1
    if result is not None:
        age_seconds = _age_seconds(polled_at)
        return {
            "data": result,
            "last_success_at": polled_at,
            "last_attempt_at": polled_at,
            "status": "fresh",
            "last_error": None,
            "poll_count": poll_count,
            "age_seconds": age_seconds,
            "freshness_level": _freshness_level(age_seconds),
            "using_bootstrap_data": False,
        }

    age_seconds = _age_seconds(previous.get("last_success_at"))
    status = "initializing" if previous.get("last_success_at") is None else "error"
    return {
        **previous,
        "last_attempt_at": polled_at,
        "status": status,
        "last_error": error,
        "poll_count": poll_count,
        "age_seconds": age_seconds,
        "freshness_level": _freshness_level(age_seconds),
    }


def _build_chain_state(previous: dict, *, chain: str, token_results: dict, polled_at: str) -> dict:
    previous_tokens = previous.get("tokens", {})
    next_tokens = {}
    errors = []

    for token in iter_active_tokens_for_chain(chain):
        previous_token = previous_tokens.get(token, _initial_token_state(chain, token))
        raw_result = token_results.get(token)
        if isinstance(raw_result, Exception):
            next_tokens[token] = _build_token_state(
                previous_token,
                result=None,
                error=str(raw_result),
                polled_at=polled_at,
            )
            errors.append(f"{token}: {raw_result}")
            continue

        next_tokens[token] = _build_token_state(
            previous_token,
            result=raw_result,
            error=None,
            polled_at=polled_at,
        )

    (
        status,
        age_seconds,
        freshness_level,
        last_success_at,
        last_attempt_at,
        using_bootstrap_data,
    ) = _derive_chain_status(next_tokens)
    default_state = next_tokens.get(DEFAULT_TOKEN) or next(iter(next_tokens.values()))
    return {
        "data": default_state["data"],
        "tokens": next_tokens,
        "last_success_at": last_success_at,
        "last_attempt_at": last_attempt_at or polled_at,
        "status": status,
        "last_error": " | ".join(errors) if errors else None,
        "poll_count": int(previous.get("poll_count", 0)) + 1,
        "age_seconds": age_seconds,
        "freshness_level": freshness_level,
        "using_bootstrap_data": using_bootstrap_data,
    }


async def start_poller():
    global _cache
    init_summary_store()
    if is_demo_mode():
        seed_demo_cache()
        return
    _cache = _build_cache_from_materialized_summaries(
        previous_cache=_cache,
        prices={"ethereum": BOOTSTRAP["eth_price_usd"], "polygon": BOOTSTRAP["polygon_price_usd"]},
        prices_live=False,
        poll_count=int(_cache.get("poll_count", 0)),
        polled_at=_utcnow_iso(),
    )
    while True:
        try:
            await _poll()
            await asyncio.sleep(POLL_INTERVAL)
        except asyncio.CancelledError:
            logger.info("Poller cancelled - shutting down gracefully")
            break
        except Exception as exc:
            log_event(logger, "cache.poll.cycle.failed", error=str(exc))
            await asyncio.sleep(BACKOFF_INTERVAL)


async def _run_manual_refresh() -> None:
    global _refresh_task
    try:
        await _poll()
    except Exception as exc:
        _set_refresh_state(
            status="failed",
            label="Query failed",
            indicator="red",
            last_error=str(exc),
        )
        log_event(logger, "cache.manual_refresh.failed", error=str(exc))
        raise
    else:
        _set_refresh_state(
            status="ready",
            label="Evidence ready",
            indicator="green",
            last_measured_refresh=_cache.get("last_updated"),
            last_error=None,
        )
        log_event(
            logger,
            "cache.manual_refresh.succeeded",
            last_measured_refresh=_cache.get("last_updated"),
            cache_status=_cache.get("status"),
        )
    finally:
        _refresh_task = None


def trigger_manual_refresh() -> dict:
    global _refresh_task
    if is_demo_mode():
        seed_demo_cache()
        _set_refresh_state(
            status="ready",
            label="Evidence ready",
            indicator="green",
            last_measured_refresh=_cache.get("last_updated"),
            last_error=None,
        )
        return get_refresh_state()

    if _refresh_task is not None and not _refresh_task.done():
        return get_refresh_state()

    _set_refresh_state(
        status="querying",
        label="Querying BigQuery",
        indicator="yellow",
        last_error=None,
    )
    _refresh_task = asyncio.create_task(_run_manual_refresh())
    return get_refresh_state()


async def _poll():
    global _cache

    prices, prices_live = await asyncio.to_thread(get_native_prices)
    polled_at = _utcnow_iso()
    poll_count = int(_cache.get("poll_count", 0)) + 1

    active_combinations = get_active_combinations()
    results = await asyncio.gather(
        *[
            asyncio.to_thread(
                run_chain_token_query,
                CHAIN_CONFIGS[chain],
                prices[chain.lower()],
                token,
            )
            for chain, token in active_combinations
        ],
        return_exceptions=True,
    )

    grouped_results = {chain: {} for chain in LIVE_CHAINS}

    for (chain, token), raw_result in zip(active_combinations, results):
        if isinstance(raw_result, Exception):
            log_event(
                logger,
                "cache.poll.token.failed",
                chain=chain,
                token=token,
                poll_count=poll_count,
                error=str(raw_result),
            )
            grouped_results[chain][token] = raw_result
            continue

        if raw_result is None:
            empty_error = "No transfer data returned for active coverage token"
            log_event(
                logger,
                "cache.poll.token.empty",
                chain=chain,
                token=token,
                poll_count=poll_count,
                error=empty_error,
            )
            grouped_results[chain][token] = RuntimeError(empty_error)
            continue

        grouped_results[chain][token] = raw_result
        log_event(
            logger,
            "cache.poll.token.ok",
            chain=chain,
            token=token,
            poll_count=poll_count,
            avg_fee_usd=raw_result.get("avg_fee_usd"),
            transfer_count=raw_result.get("transfer_count"),
        )

    _materialize_fee_activity(grouped_results, materialized_at=polled_at)
    _cache = _build_cache_from_materialized_summaries(
        previous_cache=_cache,
        prices=prices,
        prices_live=prices_live,
        poll_count=poll_count,
        polled_at=polled_at,
    )
    _materialize_corridor_summaries(cache_payload=_cache, materialized_at=polled_at)

    for chain in LIVE_CHAINS:
        chain_state = _cache["chains"][chain]
        log_event(
            logger,
            "cache.poll.chain.ok",
            chain=chain,
            poll_count=poll_count,
            chain_status=chain_state["status"],
            freshness_level=chain_state["freshness_level"],
            age_seconds=chain_state["age_seconds"],
            active_tokens=list(chain_state.get("tokens", {}).keys()),
        )

    log_event(
        logger,
        "cache.poll.complete",
        poll_count=poll_count,
        status=_cache["status"],
        cache_age_seconds=_cache["cache_age_seconds"],
        native_prices_live=prices_live,
        chain_statuses={chain: _cache["chains"][chain]["status"] for chain in LIVE_CHAINS},
    )
    metrics = get_query_metrics_snapshot()
    log_event(
        logger,
        "bigquery.metrics.digest",
        source="fee_poller",
        poll_count=poll_count,
        execution_count=metrics["overall"]["execution_count"],
        execution_bytes=metrics["overall"]["execution_bytes"],
        dry_run_count=metrics["overall"]["dry_run_count"],
        dry_run_bytes=metrics["overall"]["dry_run_bytes"],
        families={
            family: {
                "query_count": item["query_count"],
                "avg_bytes_per_query": item["avg_bytes_per_query"],
                "max_budget_utilization": item["max_budget_utilization"],
            }
            for family, item in metrics["families"].items()
        },
    )


def get_cache() -> dict:
    return _cache


def get_chain_state(chain: str) -> Optional[dict]:
    return _cache.get("chains", {}).get(chain)


def get_chain_cache_age_seconds(chain: str) -> Optional[int]:
    chain_state = get_chain_state(chain)
    if not chain_state:
        return None
    return chain_state.get("age_seconds")


def get_cache_age_seconds() -> Optional[int]:
    return _cache.get("cache_age_seconds")


def get_refresh_state() -> dict:
    refresh = dict(_refresh_state)
    refresh["last_measured_refresh"] = refresh.get("last_measured_refresh") or _cache.get("last_updated")
    refresh["is_querying"] = refresh["status"] == "querying"
    return refresh
