"""Helpers for normalizing corridor decision scores."""

from __future__ import annotations


def normalize_unit_score(value: float) -> float:
    """Clamp a numeric score to the 0..1 range."""
    return max(0.0, min(round(float(value), 4), 1.0))


def score_out_of_100(value: float) -> int:
    return int(round(normalize_unit_score(value) * 100))


def score_label(value: float) -> str:
    return f"{score_out_of_100(value)} / 100"
