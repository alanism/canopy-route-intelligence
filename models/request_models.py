"""Request models for the Canopy execution engine."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, field_validator

CHAIN_ALIASES = {
    "ethereum": "Ethereum",
    "polygon": "Polygon",
    "arbitrum": "Arbitrum",
    "base": "Base",
}


class SimulateRequest(BaseModel):
    amount: float = Field(..., ge=1, le=10_000_000)
    token: str = Field(default="USDC", min_length=2, max_length=16)
    source_chain: str = Field(default="Ethereum")
    destination_chain: str = Field(default="Polygon")
    slippage_tolerance: float = Field(default=0.0075, ge=0.0, le=0.05)
    preference: Literal["balanced", "cheapest", "fastest", "safest"] = "balanced"
    gas_speed: Literal["slow", "medium", "fast"] = "medium"

    @field_validator("token")
    @classmethod
    def _normalize_token(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("source_chain", "destination_chain")
    @classmethod
    def _normalize_chain(cls, value: str) -> str:
        chain_key = value.strip().lower()
        return CHAIN_ALIASES.get(chain_key, value.strip().title())
