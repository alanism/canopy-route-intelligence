"""Canonical stablecoin metadata and support rules for Canopy v5."""

from __future__ import annotations

import os
from typing import Dict, Iterable, List, Optional, Tuple

EVIDENCE_LIVE_MEASURED = "LIVE_MEASURED"
EVIDENCE_MODELED = "MODELED"
EVIDENCE_STRATEGIC_REFERENCE = "STRATEGIC_REFERENCE"
EVIDENCE_UNSUPPORTED = "UNSUPPORTED"

COVERAGE_ACTIVE = "ACTIVE_COVERAGE"
COVERAGE_LIMITED = "LIMITED_COVERAGE"
COVERAGE_UNSUPPORTED = "UNSUPPORTED"

TOKEN_REGISTRY: Dict[str, dict] = {
    "USDC": {
        "symbol": "USDC",
        "display_name": "USD Coin",
        "issuer": "Circle",
        "decimals": 6,
        "coverage_state": COVERAGE_ACTIVE,
        "active_chains": ("Ethereum", "Polygon"),
        "limited_chains": (),
        "contracts": {
            "Ethereum": os.getenv(
                "USDC_ETH_CONTRACT",
                "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            ),
            "Polygon": os.getenv(
                "USDC_POLYGON_CONTRACT",
                "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
            ),
        },
        "noise_filter_profile": "stablecoin_standard",
        "summary_note": "Measured on Ethereum and Polygon in the current Canopy stack.",
    },
    "USDT": {
        "symbol": "USDT",
        "display_name": "Tether",
        "issuer": "Tether",
        "decimals": 6,
        "coverage_state": COVERAGE_LIMITED,
        "active_chains": ("Ethereum",),
        "limited_chains": ("Polygon",),
        "contracts": {
            "Ethereum": os.getenv(
                "USDT_ETH_CONTRACT",
                "0xdac17f958d2ee523a2206206994597c13d831ec7",
            ),
        },
        "noise_filter_profile": "stablecoin_tether",
        "summary_note": "Measured on Ethereum; Polygon remains limited coverage until verified.",
    },
    "PYUSD": {
        "symbol": "PYUSD",
        "display_name": "PayPal USD",
        "issuer": "PayPal / Paxos",
        "decimals": 6,
        "coverage_state": COVERAGE_LIMITED,
        "active_chains": ("Ethereum",),
        "limited_chains": ("Polygon",),
        "contracts": {
            "Ethereum": os.getenv(
                "PYUSD_ETH_CONTRACT",
                "0x6c3ea9036406852006290770bedfc107f9136a59",
            ),
        },
        "noise_filter_profile": "stablecoin_standard",
        "summary_note": "Measured on Ethereum; broader support is intentionally deferred.",
    },
}

DEFAULT_TOKEN = "USDC"


def _runtime_active_tokens() -> Tuple[str, ...]:
    requested = tuple(
        token.strip().upper()
        for token in os.getenv("CANOPY_ACTIVE_TOKENS", "").split(",")
        if token.strip()
    )
    if not requested:
        return tuple(TOKEN_REGISTRY.keys())
    valid = tuple(token for token in requested if token in TOKEN_REGISTRY)
    return valid or tuple(TOKEN_REGISTRY.keys())


def normalize_token(token: str | None) -> str:
    token_key = str(token or DEFAULT_TOKEN).strip().upper()
    if token_key not in TOKEN_REGISTRY:
        raise ValueError(token_key)
    return token_key


def get_token_config(token: str | None = None) -> dict:
    return TOKEN_REGISTRY[normalize_token(token)]


def get_supported_tokens() -> List[str]:
    return list(TOKEN_REGISTRY.keys())


def get_token_contract(token: str, chain: str) -> Optional[str]:
    cfg = get_token_config(token)
    return cfg["contracts"].get(chain)


def get_token_coverage_state(token: str, chain: Optional[str] = None) -> str:
    if chain is None:
        return get_token_config(token)["coverage_state"]
    if is_active_coverage(token, chain):
        return COVERAGE_ACTIVE
    if is_limited_coverage(token, chain):
        return COVERAGE_LIMITED
    return COVERAGE_UNSUPPORTED


def get_active_chains(token: str) -> Tuple[str, ...]:
    return tuple(get_token_config(token)["active_chains"])


def get_limited_chains(token: str) -> Tuple[str, ...]:
    return tuple(get_token_config(token)["limited_chains"])


def is_active_coverage(token: str, chain: str) -> bool:
    return chain in get_active_chains(token)


def is_limited_coverage(token: str, chain: str) -> bool:
    return chain in get_limited_chains(token)


def get_active_combinations() -> List[Tuple[str, str]]:
    combos: List[Tuple[str, str]] = []
    for token in _runtime_active_tokens():
        cfg = TOKEN_REGISTRY[token]
        combos.extend((chain, token) for chain in cfg["active_chains"])
    return combos


def get_supported_combinations(token: str) -> List[Tuple[str, str, str]]:
    token_key = normalize_token(token)
    cfg = get_token_config(token_key)
    combos: List[Tuple[str, str, str]] = []
    combos.extend((chain, token_key, COVERAGE_ACTIVE) for chain in cfg["active_chains"])
    combos.extend((chain, token_key, COVERAGE_LIMITED) for chain in cfg["limited_chains"])
    return combos


def iter_active_tokens_for_chain(chain: str) -> Iterable[str]:
    for token in _runtime_active_tokens():
        cfg = TOKEN_REGISTRY[token]
        if chain in cfg["active_chains"]:
            yield token


def build_metric_evidence(
    value,
    evidence_state: str,
    coverage_state: str,
    data_source: str,
    last_updated_at: Optional[str],
    ttl_seconds: int,
    confidence_reason: Optional[str] = None,
) -> dict:
    return {
        "value": value,
        "evidence_state": evidence_state,
        "coverage_state": coverage_state,
        "data_source": data_source,
        "last_updated_at": last_updated_at,
        "ttl_seconds": ttl_seconds,
        "confidence_reason": confidence_reason,
    }
