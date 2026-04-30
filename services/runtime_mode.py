"""Runtime mode helpers for demo vs live Canopy behavior."""

from __future__ import annotations

import os

DEMO_MODE = "demo"
REAL_MODE = "real"


def get_runtime_mode() -> str:
    value = os.getenv("CANOPY_RUNTIME_MODE", REAL_MODE).strip().lower()
    if value == DEMO_MODE:
        return DEMO_MODE
    return REAL_MODE


def is_demo_mode() -> bool:
    return get_runtime_mode() == DEMO_MODE


def is_real_mode() -> bool:
    return get_runtime_mode() == REAL_MODE


def get_runtime_mode_label() -> str:
    return "Demo Mode" if is_demo_mode() else "Real Mode"


def get_runtime_mode_note() -> str:
    if is_demo_mode():
        return "Seeded demo snapshots only. No BigQuery or Coinbase calls are made."
    return "Live measured refresh from BigQuery and Coinbase background polling."
