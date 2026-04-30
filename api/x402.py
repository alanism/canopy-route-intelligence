"""
api/x402.py — x402 protocol helpers with v2-compatible HTTP headers.

This module keeps payment metadata out of 200 OK route bodies while providing:
- `PAYMENT-REQUIRED` challenge headers for premium endpoints
- `PAYMENT-SIGNATURE` / legacy `X-Payment` request support
- Optional facilitator verify/settle calls when enabled
"""

import base64
import json
import logging
import os
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Optional, Tuple

import requests
from fastapi.responses import JSONResponse

logger = logging.getLogger("sci-agent.x402")

PAYMENT_SIGNATURE_HEADER = "PAYMENT-SIGNATURE"
PAYMENT_REQUIRED_HEADER = "PAYMENT-REQUIRED"
PAYMENT_RESPONSE_HEADER = "PAYMENT-RESPONSE"
LEGACY_PAYMENT_HEADER = "X-Payment"
LEGACY_PAYMENT_RESPONSE_HEADER = "X-Payment-Response"

DEFAULT_FACILITATOR_URL = "https://www.x402.org/facilitator"
DEFAULT_USDC_POLYGON_ASSET = os.getenv(
    "USDC_POLYGON_CONTRACT", "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
)

X402_PRICE_USDC = os.getenv("X402_PRICE_USDC", "0.01")
X402_PAYMENT_ADDRESS = os.getenv(
    "X402_PAYMENT_ADDRESS", "0x0000000000000000000000000000000000000000"
)
X402_ENABLED = os.getenv("X402_ENABLED", "false").lower() == "true"
X402_ALLOW_UNVERIFIED_PAYMENTS = (
    os.getenv("X402_ALLOW_UNVERIFIED_PAYMENTS", "false").lower() == "true"
)
X402_FACILITATOR_URL = os.getenv("X402_FACILITATOR_URL", DEFAULT_FACILITATOR_URL).rstrip(
    "/"
)
X402_NETWORK = os.getenv("X402_NETWORK", "eip155:137")
X402_ASSET = os.getenv("X402_ASSET", DEFAULT_USDC_POLYGON_ASSET)
X402_SCHEME = os.getenv("X402_SCHEME", "exact")
X402_TIMEOUT_SECONDS = float(os.getenv("X402_TIMEOUT_SECONDS", "10"))


def _safe_base64_encode(data: str) -> str:
    return base64.b64encode(data.encode("utf-8")).decode("utf-8")


def _safe_base64_decode(data: str) -> str:
    return base64.b64decode(data.encode("utf-8")).decode("utf-8")


def _price_to_base_units(price_usdc: str) -> str:
    """Convert USDC-denominated dollars into 6-decimal base units."""
    micros = (
        Decimal(price_usdc) * Decimal("1000000")
    ).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return str(int(micros))


def get_payment_requirements_v2(resource: str, description: Optional[str] = None) -> Dict[str, Any]:
    """Build x402 v2 payment requirements."""
    return {
        "x402Version": 2,
        "error": "payment required",
        "resource": {
            "url": resource,
            "description": description or "Premium SCI-Agent route recommendation",
            "mimeType": "application/json",
        },
        "accepts": [
            {
                "scheme": X402_SCHEME,
                "network": X402_NETWORK,
                "asset": X402_ASSET,
                "amount": _price_to_base_units(X402_PRICE_USDC),
                "payTo": X402_PAYMENT_ADDRESS,
                "maxTimeoutSeconds": 300,
                "extra": {},
            }
        ],
    }


def get_payment_requirements_v1(resource: str, description: Optional[str] = None) -> Dict[str, Any]:
    """Build legacy v1 payment requirements for compatibility."""
    return {
        "x402Version": 1,
        "error": "payment required",
        "accepts": [
            {
                "scheme": X402_SCHEME,
                "network": X402_NETWORK,
                "maxAmountRequired": _price_to_base_units(X402_PRICE_USDC),
                "resource": resource,
                "description": description or "Premium SCI-Agent route recommendation",
                "mimeType": "application/json",
                "payTo": X402_PAYMENT_ADDRESS,
                "maxTimeoutSeconds": 300,
                "asset": X402_ASSET,
                "extra": {},
            }
        ],
    }


def payment_required_response(resource: str) -> JSONResponse:
    """
    Return a 402 challenge with v2 and legacy-compatible payment instructions.
    """
    payment_required_v2 = get_payment_requirements_v2(resource)
    payment_required_v1 = get_payment_requirements_v1(resource)
    return JSONResponse(
        status_code=402,
        content={
            "error": "payment_required",
            "resource": resource,
            "message": "This endpoint requires x402 payment.",
            "payment_requirements": payment_required_v2,
            "legacy_payment_requirements": payment_required_v1,
        },
        headers={
            PAYMENT_REQUIRED_HEADER: _safe_base64_encode(json.dumps(payment_required_v2)),
            "X-Payment-Required": "true",
            "X-Payment-Amount": X402_PRICE_USDC,
            "X-Payment-Asset": X402_ASSET,
            "X-Payment-Network": X402_NETWORK,
            "X-Payment-Address": X402_PAYMENT_ADDRESS,
        },
    )


def decode_payment_header(header_value: str) -> Dict[str, Any]:
    """Decode a base64-encoded v2 or legacy payment signature header."""
    json_str = _safe_base64_decode(header_value)
    return json.loads(json_str)


def extract_payment_header(
    payment_signature: Optional[str], legacy_payment: Optional[str]
) -> Tuple[Optional[str], Optional[str]]:
    """Prefer v2 `PAYMENT-SIGNATURE`, fall back to legacy `X-Payment`."""
    if payment_signature:
        return payment_signature, PAYMENT_SIGNATURE_HEADER
    if legacy_payment:
        return legacy_payment, LEGACY_PAYMENT_HEADER
    return None, None


def _build_facilitator_request_body(
    payment_payload: Dict[str, Any],
    payment_requirements: Dict[str, Any],
) -> Dict[str, Any]:
    version = payment_payload.get("x402Version", 2)
    return {
        "x402Version": version,
        "paymentPayload": payment_payload,
        "paymentRequirements": payment_requirements,
    }


def verify_and_settle_payment(
    payment_header: str, resource: str
) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """
    Verify and settle an x402 payment via facilitator.

    Returns:
        (success, settlement_response, error_message)
    """
    try:
        payment_payload = decode_payment_header(payment_header)
    except Exception as exc:
        return (False, None, f"Invalid payment header encoding: {exc}")

    if X402_ALLOW_UNVERIFIED_PAYMENTS:
        logger.warning("x402 running in unverifed-dev mode; skipping facilitator calls")
        return (
            True,
            {
                "success": True,
                "transaction": "unverified-dev-mode",
                "network": X402_NETWORK,
                "payer": "unknown",
            },
            None,
        )

    if payment_payload.get("x402Version", 2) == 1:
        payment_requirements = get_payment_requirements_v1(resource)["accepts"][0]
    else:
        payment_requirements = get_payment_requirements_v2(resource)["accepts"][0]
    request_body = _build_facilitator_request_body(payment_payload, payment_requirements)

    try:
        verify_response = requests.post(
            f"{X402_FACILITATOR_URL}/verify",
            json=request_body,
            timeout=X402_TIMEOUT_SECONDS,
        )
        verify_response.raise_for_status()
        verify_payload = verify_response.json()
    except Exception as exc:
        return (False, None, f"Facilitator verify failed: {exc}")

    if not verify_payload.get("isValid", False):
        reason = verify_payload.get("invalidReason") or verify_payload.get("invalidMessage")
        return (False, None, f"Payment verification failed: {reason or 'invalid payment'}")

    try:
        settle_response = requests.post(
            f"{X402_FACILITATOR_URL}/settle",
            json=request_body,
            timeout=X402_TIMEOUT_SECONDS,
        )
        settle_response.raise_for_status()
        settle_payload = settle_response.json()
    except Exception as exc:
        return (False, None, f"Facilitator settle failed: {exc}")

    if not settle_payload.get("success", False):
        reason = settle_payload.get("errorReason") or settle_payload.get("errorMessage")
        return (False, settle_payload, f"Payment settlement failed: {reason or 'unknown'}")

    return (True, settle_payload, None)


def build_payment_response_headers(settle_payload: Dict[str, Any]) -> Dict[str, str]:
    """Build response headers confirming settlement."""
    encoded = _safe_base64_encode(json.dumps(settle_payload))
    return {
        PAYMENT_RESPONSE_HEADER: encoded,
        LEGACY_PAYMENT_RESPONSE_HEADER: encoded,
    }
