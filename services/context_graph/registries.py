"""Load repo-managed protocol and bridge registries for the context graph."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

REGISTRY_DIR = Path(__file__).resolve().parent / "registries"


def _normalize_entries(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for entry in entries:
        normalized.append(
            {
                **entry,
                "chain": str(entry.get("chain", "")).strip(),
                "contract_address": str(entry.get("contract_address", "")).strip().lower(),
            }
        )
    return normalized


@lru_cache(maxsize=4)
def _load_registry(filename: str) -> List[Dict[str, Any]]:
    path = REGISTRY_DIR / filename
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return _normalize_entries(payload)


def get_protocol_registry(chain: str) -> List[Dict[str, Any]]:
    return [
        entry
        for entry in _load_registry("protocol_registry.json")
        if entry["chain"].lower() == chain.lower()
    ]


def get_bridge_registry(chain: str) -> List[Dict[str, Any]]:
    return [
        entry
        for entry in _load_registry("bridge_registry.json")
        if entry["chain"].lower() == chain.lower()
    ]


def match_protocol_address(chain: str, address: Optional[str]) -> Optional[Dict[str, Any]]:
    if not address:
        return None
    normalized = address.lower()
    for entry in get_protocol_registry(chain):
        if entry["contract_address"] == normalized:
            return entry
    return None


def match_bridge_address(chain: str, address: Optional[str]) -> Optional[Dict[str, Any]]:
    if not address:
        return None
    normalized = address.lower()
    for entry in get_bridge_registry(chain):
        if entry["contract_address"] == normalized:
            return entry
    return None

