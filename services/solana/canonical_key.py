"""
Phase 0F — Canonical Key + Collision Defense.

Canonical key structure for Solana events:

    chain + signature + instruction_index + inner_instruction_index

Top-level rule: inner_instruction_index = -1 for all top-level instructions.
Never default a missing inner index to 0 — that collapses a top-level instruction
into the first inner CPI, silently overwriting data.

Prohibited identity pattern (EVM): tx_hash + log_index.
Solana events must never use log_index as part of their identity.

Collision defense:
- Two transfer effects sharing the same base key but different fingerprints
  are not overwritten — they receive a transfer_ordinal and collision_detected=True.
- validation_status is set to "degraded" unless the collision is fixture-proven safe.
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any, Optional

logger = logging.getLogger("canopy.solana.canonical_key")

CHAIN = "solana"
TOP_LEVEL_INNER_INDEX = -1


def build_raw_event_id(
    signature: str,
    instruction_index: int,
    inner_instruction_index: int,
) -> str:
    """
    Build the raw event ID for a Solana transfer event.

    Format: solana:{signature}:{instruction_index}:{inner_instruction_index}

    For top-level instructions, inner_instruction_index MUST be -1.
    Passing 0 for a top-level instruction is a caller bug.
    """
    if inner_instruction_index == 0:
        logger.debug(
            "build_raw_event_id called with inner_instruction_index=0 for sig=%s ix=%d; "
            "verify this is truly an inner instruction and not a top-level one.",
            signature, instruction_index,
        )
    return f"{CHAIN}:{signature}:{instruction_index}:{inner_instruction_index}"


def build_normalized_event_id(
    signature: str,
    instruction_index: int,
    inner_instruction_index: int,
    transfer_ordinal: int = 0,
) -> str:
    """
    Build the normalized event ID. Includes transfer_ordinal for collision resolution.

    For top-level instructions, inner_instruction_index MUST be -1.
    """
    base = build_raw_event_id(signature, instruction_index, inner_instruction_index)
    if transfer_ordinal == 0:
        return base
    return f"{base}:{transfer_ordinal}"


def build_event_fingerprint(
    program_id: str,
    token_mint: Optional[str],
    source_token_account: Optional[str],
    destination_token_account: Optional[str],
    amount_raw: int,
    instruction_data_hash: str,
) -> str:
    """
    Build a deterministic fingerprint for collision detection.

    Two events with the same base canonical key but different fingerprints
    represent distinct transfers and must not overwrite each other.
    """
    parts = "|".join([
        program_id or "",
        token_mint or "",
        source_token_account or "",
        destination_token_account or "",
        str(amount_raw),
        instruction_data_hash or "",
    ])
    return hashlib.sha256(parts.encode("utf-8")).hexdigest()


def assign_canonical_keys(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Assign canonical keys and detect collisions across a list of transfer events.

    Each event dict must have:
        - signature
        - instruction_index
        - inner_instruction_index   (-1 for top-level)
        - program_id
        - token_mint
        - source_token_account
        - destination_token_account
        - amount_raw
        - data_hash

    Mutates events in-place (adds raw_event_id, normalized_event_id,
    event_fingerprint, transfer_ordinal, collision_detected).

    Returns the updated list.
    """
    # Group by base key to detect collisions
    base_key_groups: dict[str, list[int]] = {}

    for i, event in enumerate(events):
        sig = event.get("signature", "")
        ix_idx = event.get("instruction_index", 0)
        inner_idx = event.get("inner_instruction_index", TOP_LEVEL_INNER_INDEX)

        raw_id = build_raw_event_id(sig, ix_idx, inner_idx)

        fingerprint = build_event_fingerprint(
            program_id=event.get("program_id", ""),
            token_mint=event.get("token_mint"),
            source_token_account=event.get("source_token_account"),
            destination_token_account=event.get("destination_token_account"),
            amount_raw=event.get("amount_raw", 0),
            instruction_data_hash=event.get("data_hash", ""),
        )

        event["raw_event_id"] = raw_id
        event["event_fingerprint"] = fingerprint

        if raw_id not in base_key_groups:
            base_key_groups[raw_id] = []
        base_key_groups[raw_id].append(i)

    # Assign ordinals and mark collisions
    for raw_id, indexes in base_key_groups.items():
        if len(indexes) == 1:
            events[indexes[0]]["transfer_ordinal"] = 0
            events[indexes[0]]["collision_detected"] = False
            events[indexes[0]]["normalized_event_id"] = build_normalized_event_id(
                events[indexes[0]]["signature"],
                events[indexes[0]]["instruction_index"],
                events[indexes[0]]["inner_instruction_index"],
                transfer_ordinal=0,
            )
        else:
            # Collision: multiple events share the same base key
            # Check if they also share the same fingerprint (true duplicates)
            fingerprints = [events[i]["event_fingerprint"] for i in indexes]
            all_same_fingerprint = len(set(fingerprints)) == 1

            for ordinal, i in enumerate(indexes):
                events[i]["transfer_ordinal"] = ordinal
                events[i]["normalized_event_id"] = build_normalized_event_id(
                    events[i]["signature"],
                    events[i]["instruction_index"],
                    events[i]["inner_instruction_index"],
                    transfer_ordinal=ordinal,
                )
                if all_same_fingerprint:
                    # True duplicate (same content) — mark collision but not degraded
                    events[i]["collision_detected"] = True
                    if events[i].get("validation_status") != "degraded":
                        events[i]["validation_status"] = "collision_duplicate"
                else:
                    # Different fingerprints sharing same base key — genuine collision
                    events[i]["collision_detected"] = True
                    events[i]["validation_status"] = "degraded"
                    logger.warning(
                        "Canonical key collision with differing fingerprints: %s "
                        "(ordinal=%d). Mark degraded until fixture-proven safe.",
                        raw_id, ordinal,
                    )

    return events


def validate_no_evm_identity(event: dict[str, Any]) -> None:
    """
    Assert that the event does not use the EVM tx_hash + log_index identity pattern.

    Raises ValueError if either field is present with non-Solana semantics.
    This is a hard guard — Solana events must never inherit EVM identity fields.
    """
    if "log_index" in event and event.get("chain") == "solana":
        raise ValueError(
            f"Solana event must not carry log_index. "
            f"Use instruction_index + inner_instruction_index instead. "
            f"Event: sig={event.get('signature')} ix={event.get('instruction_index')}"
        )
    if "tx_hash" in event and event.get("chain") == "solana":
        raise ValueError(
            f"Solana event must not carry tx_hash. "
            f"Use signature instead. "
            f"Event: sig={event.get('signature')}"
        )
