"""Refreshable corridor configuration with local fallback and optional GCS source."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from threading import Lock
from time import time
from typing import Dict, List, Optional, Tuple

from services.logging_utils import log_event

logger = logging.getLogger("sci-agent.corridor_config")

CONFIG_ENV_VAR = "CANOPY_CORRIDOR_CONFIG_URI"
REFRESH_SECONDS_ENV_VAR = "CANOPY_CORRIDOR_CONFIG_REFRESH_SECONDS"
DEFAULT_REFRESH_SECONDS = 60
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "configs" / "corridors.v1.json"

REQUIRED_CORRIDOR_FIELDS = {
    "key",
    "origin",
    "destination",
    "label",
    "corridor_slug",
    "source_country",
    "destination_country",
    "destination_city",
    "map_viewport",
    "corridor_note",
    "default_amount_usdc",
    "default_monthly_volume_usdc",
    "default_baseline_fee_pct",
    "default_baseline_settlement_hours",
    "default_current_setup",
    "polygon_maturity",
    "stellar_maturity",
    "market_readiness",
    "ecosystem_support",
    "regulatory_exposure",
    "launch_readiness",
    "launch_readiness_score",
    "rail_route_fit",
    "solved_infrastructure",
    "open_questions",
    "analytics_profile",
    "stellar_reference",
}

_state_lock = Lock()
_state = {
    "config": None,
    "status": "initializing",
    "source": None,
    "last_loaded_at": None,
    "last_error": None,
}


def _parse_gcs_uri(uri: str) -> Tuple[str, str]:
    stripped = uri[5:]
    bucket, _, blob = stripped.partition("/")
    if not bucket or not blob:
        raise ValueError("GCS URI must look like gs://bucket/path.json")
    return bucket, blob


def _load_from_gcs(uri: str) -> dict:
    try:
        from google.cloud import storage
    except ImportError as exc:  # pragma: no cover - dependency-driven guard
        raise RuntimeError("google-cloud-storage is required for GCS corridor config loading") from exc

    bucket_name, blob_name = _parse_gcs_uri(uri)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    content = blob.download_as_text()
    return json.loads(content)


def _load_from_file(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _validate_config(payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Corridor config payload must be a JSON object")
    corridors = payload.get("corridors")
    if not isinstance(corridors, list) or not corridors:
        raise ValueError("Corridor config must include a non-empty corridors list")
    for corridor in corridors:
        missing = sorted(REQUIRED_CORRIDOR_FIELDS - corridor.keys())
        if missing:
            raise ValueError(f"Corridor {corridor.get('key', 'unknown')} missing fields: {', '.join(missing)}")
        if not isinstance(corridor.get("analytics_profile"), dict):
            raise ValueError(f"Corridor {corridor['key']} must include an analytics_profile object")
        if not isinstance(corridor.get("stellar_reference"), dict):
            raise ValueError(f"Corridor {corridor['key']} must include a stellar_reference object")
    if not isinstance(payload.get("default_corridor"), dict):
        raise ValueError("Corridor config must include default_corridor")
    if not isinstance(payload.get("default_analytics_profile"), dict):
        raise ValueError("Corridor config must include default_analytics_profile")
    if not isinstance(payload.get("default_stellar_reference"), dict):
        raise ValueError("Corridor config must include default_stellar_reference")
    return payload


def _build_indexes(payload: dict) -> dict:
    corridors = payload["corridors"]
    by_pair = {
        (str(corridor["origin"]).upper(), str(corridor["destination"]).upper()): corridor
        for corridor in corridors
    }
    by_key = {str(corridor["key"]).upper(): corridor for corridor in corridors}
    by_slug = {str(corridor["corridor_slug"]).lower(): corridor for corridor in corridors}
    return {
        **payload,
        "_by_pair": by_pair,
        "_by_key": by_key,
        "_by_slug": by_slug,
    }


def _refresh_locked(force: bool = False) -> dict:
    now = time()
    refresh_seconds = int(os.getenv(REFRESH_SECONDS_ENV_VAR, str(DEFAULT_REFRESH_SECONDS)))
    current = _state.get("config")
    last_loaded_at = _state.get("last_loaded_at")
    if (
        not force
        and current is not None
        and last_loaded_at is not None
        and now - last_loaded_at < refresh_seconds
    ):
        return current

    config_uri = os.getenv(CONFIG_ENV_VAR, "").strip()
    source = config_uri or str(DEFAULT_CONFIG_PATH)

    try:
        if config_uri:
            payload = _load_from_gcs(config_uri)
        else:
            payload = _load_from_file(DEFAULT_CONFIG_PATH)
        config = _build_indexes(_validate_config(payload))
        _state.update(
            {
                "config": config,
                "status": "ok",
                "source": source,
                "last_loaded_at": now,
                "last_error": None,
            }
        )
        log_event(
            logger,
            "corridor_config.refresh.ok",
            config_source=source,
            corridor_count=len(config["corridors"]),
            refresh_seconds=refresh_seconds,
        )
        return config
    except Exception as exc:
        _state.update(
            {
                "status": "error" if current is None else "degraded",
                "source": source,
                "last_loaded_at": now,
                "last_error": str(exc),
            }
        )
        log_event(
            logger,
            "corridor_config.refresh.failed",
            config_source=source,
            refresh_seconds=refresh_seconds,
            error=str(exc),
        )
        if current is not None:
            return current
        fallback = _build_indexes(_validate_config(_load_from_file(DEFAULT_CONFIG_PATH)))
        _state.update(
            {
                "config": fallback,
                "status": "degraded",
                "source": str(DEFAULT_CONFIG_PATH),
            }
        )
        return fallback


def load_corridor_config(*, force: bool = False) -> dict:
    with _state_lock:
        return _refresh_locked(force=force)


def get_config_health() -> dict:
    load_corridor_config(force=False)
    return {
        "status": _state.get("status", "initializing"),
        "source": _state.get("source"),
        "last_loaded_at": _state.get("last_loaded_at"),
        "last_error": _state.get("last_error"),
        "refresh_seconds": int(os.getenv(REFRESH_SECONDS_ENV_VAR, str(DEFAULT_REFRESH_SECONDS))),
    }


def get_corridors() -> List[dict]:
    return list(load_corridor_config(force=False)["corridors"])


def get_corridor_by_key(corridor_key: str) -> Optional[dict]:
    return load_corridor_config(force=False)["_by_key"].get(str(corridor_key).upper())


def get_corridor_by_slug(corridor_slug: str) -> Optional[dict]:
    return load_corridor_config(force=False)["_by_slug"].get(str(corridor_slug).lower())


def get_corridor(origin: str, destination: str) -> Optional[dict]:
    return load_corridor_config(force=False)["_by_pair"].get((origin.upper(), destination.upper()))


def get_default_corridor(origin: str, destination: str) -> dict:
    default = dict(load_corridor_config(force=False)["default_corridor"])
    return {
        **default,
        "key": f"{origin.upper()}-{destination.upper()}",
        "origin": origin.upper(),
        "destination": destination.upper(),
        "label": f"{origin.upper()} -> {destination.upper()}",
        "corridor_slug": f"{origin.lower()}-{destination.lower()}",
        "source_country": origin.upper(),
        "destination_country": destination.upper(),
        "destination_city": destination.upper(),
    }


def get_corridor_or_default(origin: str, destination: str) -> dict:
    return get_corridor(origin, destination) or get_default_corridor(origin, destination)


def get_corridor_analytics_profile(corridor_key: str) -> dict:
    corridor = get_corridor_by_key(corridor_key)
    if corridor:
        return corridor["analytics_profile"]
    return load_corridor_config(force=False)["default_analytics_profile"]


def get_stellar_reference(origin: str, destination: str) -> dict:
    corridor = get_corridor(origin, destination)
    if corridor:
        return corridor["stellar_reference"]
    return load_corridor_config(force=False)["default_stellar_reference"]
