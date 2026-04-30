"""Quote generation for the Canopy execution engine."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from models.request_models import SimulateRequest

QUOTE_TTL_SECONDS = 60


def generate_execution_quote(
    *,
    request: SimulateRequest,
    route_result: dict,
    risk_profile: dict,
    snapshot: dict,
) -> dict:
    expected_received = float(route_result["total_received"])
    min_received = expected_received * (1 - float(request.slippage_tolerance))
    valid_until = datetime.now(timezone.utc) + timedelta(seconds=QUOTE_TTL_SECONDS)

    return {
        "quote_id": f"qt_{uuid4().hex[:10]}",
        "expected_received": round(expected_received, 6),
        "min_received": round(min_received, 6),
        "valid_until": valid_until.isoformat().replace("+00:00", "Z"),
        "confidence_score": float(risk_profile["confidence_score"]),
        "quote_ttl_seconds": QUOTE_TTL_SECONDS,
        "status": "LOCKED_60S" if not risk_profile["alerts"] else "LOCKED_WITH_WARNINGS",
        "snapshot_id": snapshot["snapshot_id"],
    }
