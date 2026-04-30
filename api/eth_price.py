"""
api/eth_price.py — Native token price fetchers used by the routing engine.

The module name is kept for compatibility with existing imports, but it now
returns both Ethereum and Polygon native token prices.
"""

import logging
import os
from typing import Dict, Tuple

import requests
from dotenv import load_dotenv
from services.runtime_mode import get_runtime_mode_label, is_demo_mode

load_dotenv()

logger = logging.getLogger("sci-agent.prices")

COINBASE_TICKER_URLS = {
    "ethereum": "https://api.exchange.coinbase.com/products/ETH-USD/ticker",
    "polygon": "https://api.exchange.coinbase.com/products/POL-USD/ticker",
}
TIMEOUT_SECONDS = 5
REQUEST_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "canopy-decision/0.4.0",
}


def _fetch_coinbase_price(asset_key: str) -> float:
    response = requests.get(
        COINBASE_TICKER_URLS[asset_key],
        headers=REQUEST_HEADERS,
        timeout=TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    data = response.json()
    return float(data["price"])


def get_native_prices() -> Tuple[Dict[str, float], bool]:
    """
    Fetch Ethereum and Polygon native token prices from Coinbase Exchange.

    Returns:
        ({'ethereum': float, 'polygon': float}, is_live)
    """
    fallback_prices = {
        "ethereum": float(os.getenv("ETH_PRICE_FALLBACK", "3500")),
        "polygon": float(os.getenv("POLYGON_PRICE_FALLBACK", "0.10")),
    }

    if is_demo_mode():
        logger.info(
            "Native prices source=%s | ETH: $%s | POL: $%s",
            get_runtime_mode_label().lower(),
            f"{fallback_prices['ethereum']:,.2f}",
            f"{fallback_prices['polygon']:,.4f}",
        )
        return (dict(fallback_prices), False)

    prices = dict(fallback_prices)
    live_flags = {}

    for asset_key in ("ethereum", "polygon"):
        try:
            prices[asset_key] = _fetch_coinbase_price(asset_key)
            live_flags[asset_key] = True
        except Exception as exc:
            live_flags[asset_key] = False
            logger.warning(
                "Coinbase %s ticker fetch failed: %s. Using fallback price %s",
                asset_key,
                exc,
                fallback_prices[asset_key],
            )

    is_live = all(live_flags.values())
    logger.info(
        "Native prices source=%s | ETH: $%s | POL: $%s",
        "coinbase" if is_live else "coinbase+fallback",
        f"{prices['ethereum']:,.2f}",
        f"{prices['polygon']:,.4f}",
    )
    return (prices, is_live)


def get_eth_price() -> Tuple[float, bool]:
    """
    Backward-compatible helper returning only Ethereum price.
    """
    prices, is_live = get_native_prices()
    return (prices["ethereum"], is_live)
