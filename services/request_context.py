"""Request-scoped context helpers for demo traceability."""

from __future__ import annotations

from contextvars import ContextVar, Token
from typing import Optional


_REQUEST_ID: ContextVar[Optional[str]] = ContextVar("canopy_request_id", default=None)


def set_request_id(request_id: Optional[str]) -> Token:
    return _REQUEST_ID.set(request_id)


def get_request_id() -> Optional[str]:
    return _REQUEST_ID.get()


def reset_request_id(token: Token) -> None:
    _REQUEST_ID.reset(token)
