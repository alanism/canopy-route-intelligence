"""
Solana parser constants — mints, program IDs, confirmation settings, Jito tip accounts.

All values must be verified against issuer/protocol documentation before enabling
production ingestion. Do not edit without updating JITO_TIP_ACCOUNTS_REVIEWER.
"""

from __future__ import annotations

import os

# ---------------------------------------------------------------------------
# Token mints
# ---------------------------------------------------------------------------

USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDC_DECIMALS = 6

# Reference fixture only — do not expose dashboard claims unless actually ingested
PYUSD_MINT = "2b1kV6DkPAnxd5ixfnxCpjxmKwqjjaYmCZfHsFu24GXo"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"

LEAD_TOKEN_MINT = USDC_MINT

# ---------------------------------------------------------------------------
# Program IDs
# ---------------------------------------------------------------------------

SPL_TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
ASSOCIATED_TOKEN_PROGRAM = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
SYSTEM_PROGRAM = "11111111111111111111111111111111"

TOKEN_PROGRAMS: frozenset[str] = frozenset({SPL_TOKEN_PROGRAM, TOKEN_2022_PROGRAM})

# ---------------------------------------------------------------------------
# Confirmation constants
# ---------------------------------------------------------------------------

COMMITMENT = "finalized"
# Slots behind chain head before a slot is considered safe to ingest.
# ~32 slots ≈ 12.8 seconds at ~2 slots/sec.
CONFIRMATION_BUFFER_SLOTS = 32

# ---------------------------------------------------------------------------
# Jito tip accounts
#
# Source : https://jito-labs.gitbook.io/mev/searchers-resources/tip-payment-program
# Retrieved : 2024-Q4 (pinned)
# Reviewer  : set JITO_TIP_ACCOUNTS_REVIEWER env var before enabling production ingestion
#
# These are the 8 Block Engine tip accounts managed by Jito Labs.
# Verify against the source URL above before any production promotion.
# If the env var JITO_TIP_ACCOUNTS is set, it overrides this default set.
# ---------------------------------------------------------------------------

JITO_TIP_ACCOUNTS_SOURCE = (
    "https://jito-labs.gitbook.io/mev/searchers-resources/tip-payment-program"
)
JITO_TIP_ACCOUNTS_RETRIEVED_AT = "2024-Q4"
JITO_TIP_ACCOUNTS_REVIEWER = os.getenv("JITO_TIP_ACCOUNTS_REVIEWER", "unset")

_JITO_TIP_ACCOUNTS_DEFAULT: frozenset[str] = frozenset({
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt13ij6vjB",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
})


def get_jito_tip_accounts() -> frozenset[str]:
    """
    Return the active Jito tip account set.

    If JITO_TIP_ACCOUNTS env var is set, parse it as a comma-separated list.
    Otherwise return the pinned default set.
    """
    env_val = os.getenv("JITO_TIP_ACCOUNTS", "").strip()
    if env_val:
        return frozenset(a.strip() for a in env_val.split(",") if a.strip())
    return _JITO_TIP_ACCOUNTS_DEFAULT


JITO_TIP_ACCOUNTS: frozenset[str] = get_jito_tip_accounts()
