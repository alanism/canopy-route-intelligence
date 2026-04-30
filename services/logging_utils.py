"""Structured logging helpers for demo-critical Canopy flows."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from services.request_context import get_request_id


def _json_default(value: Any) -> str:
    return str(value)


def log_event(logger: logging.Logger, event: str, **fields: Any) -> None:
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event": event,
        **fields,
    }
    request_id = get_request_id()
    if request_id and "request_id" not in payload:
        payload["request_id"] = request_id
    logger.info(json.dumps(payload, default=_json_default, sort_keys=True))
