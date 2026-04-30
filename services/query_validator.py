"""Lightweight query validation for Canopy BigQuery layer ownership."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal, Optional

QueryClassification = Literal["measured", "derived", "dev_only"]

_PARTITION_PATTERNS = (
    re.compile(r"block_timestamp\s*>=\s*TIMESTAMP_SUB", re.IGNORECASE),
    re.compile(r"DATE\s*\(\s*block_timestamp\s*\)\s*>=\s*DATE_SUB", re.IGNORECASE),
)
_SELECT_STAR_PATTERN = re.compile(r"SELECT\s+\*", re.IGNORECASE)
_LIMIT_PATTERN = re.compile(r"\bLIMIT\s+\d+\b", re.IGNORECASE)
_JOIN_PATTERN = re.compile(r"\bJOIN\b", re.IGNORECASE)
_WITH_PATTERN = re.compile(r"^\s*WITH\b", re.IGNORECASE)
_INTERVAL_PATTERN = re.compile(r"\bINTERVAL\s+\d+\s+(HOUR|DAY)\b", re.IGNORECASE)
_DERIVED_REQUEST_PATH_PATTERNS = (
    re.compile(r"\brequest\b", re.IGNORECASE),
    re.compile(r"\broute\b", re.IGNORECASE),
    re.compile(r"\bresponse\b", re.IGNORECASE),
)
_FORBIDDEN_MEASURED_PATTERNS = (
    (re.compile(r"\bpayment_like\b", re.IGNORECASE), "measured query includes payment-like heuristic logic"),
    (re.compile(r"\badjusted_", re.IGNORECASE), "measured query includes adjusted heuristic fields"),
    (re.compile(r"\bscore\b", re.IGNORECASE), "measured query includes score-like fields"),
    (re.compile(r"\blabel\b", re.IGNORECASE), "measured query includes label-like fields"),
    (re.compile(r"\bconfidence\b", re.IGNORECASE), "measured query includes confidence fields"),
    (re.compile(r"\bclassification\b", re.IGNORECASE), "measured query includes classification fields"),
    (re.compile(r"\brecommendation\b", re.IGNORECASE), "measured query includes recommendation fields"),
    (re.compile(r"\brank\b", re.IGNORECASE), "measured query includes ranking fields"),
    (re.compile(r"\broute\b", re.IGNORECASE), "measured query includes route/business interpretation"),
    (re.compile(r"\bPERCENTILE_CONT\s*\(", re.IGNORECASE), "measured query includes percentile aggregation"),
    (re.compile(r"\bAVG\s*\(", re.IGNORECASE), "measured query includes AVG aggregation"),
    (re.compile(r"\bSUM\s*\(", re.IGNORECASE), "measured query includes SUM aggregation"),
    (re.compile(r"\bCOUNTIF\s*\(", re.IGNORECASE), "measured query includes COUNTIF aggregation"),
)


@dataclass(frozen=True)
class QueryValidationIssue:
    code: str
    message: str


def validate_query(
    sql: str,
    *,
    classification: QueryClassification,
    query_name: Optional[str] = None,
    query_family: Optional[str] = None,
    maximum_bytes_billed: Optional[int] = None,
    request_scoped: bool = False,
) -> list[QueryValidationIssue]:
    issues: list[QueryValidationIssue] = []
    if not query_name:
        issues.append(
            QueryValidationIssue(
                code="missing_query_name",
                message="BigQuery query must declare a query_name.",
            )
        )
    if not query_family:
        issues.append(
            QueryValidationIssue(
                code="missing_query_family",
                message="BigQuery query must declare a query_family.",
            )
        )
    if maximum_bytes_billed is None:
        issues.append(
            QueryValidationIssue(
                code="missing_max_bytes_billed",
                message="BigQuery query must declare maximum_bytes_billed.",
            )
        )
    if classification == "derived" and request_scoped:
        issues.append(
            QueryValidationIssue(
                code="derived_request_path_forbidden",
                message="Derived BigQuery queries must not execute on the request path.",
            )
        )
    if classification == "dev_only" and request_scoped:
        issues.append(
            QueryValidationIssue(
                code="dev_only_request_path_forbidden",
                message="Dev-only BigQuery queries must not execute on the request path.",
            )
        )
    if classification == "derived" and query_name:
        if any(pattern.search(query_name) for pattern in _DERIVED_REQUEST_PATH_PATTERNS):
            issues.append(
                QueryValidationIssue(
                    code="derived_request_hint",
                    message="Derived BigQuery query name suggests request-path execution and should stay batch/dev only.",
                )
            )
        return issues
    if classification == "dev_only":
        return issues
    if classification != "measured":
        return issues

    if _SELECT_STAR_PATTERN.search(sql):
        issues.append(
            QueryValidationIssue(
                code="select_star",
                message="Measured query must not use SELECT *.",
            )
        )
    if not any(pattern.search(sql) for pattern in _PARTITION_PATTERNS):
        issues.append(
            QueryValidationIssue(
                code="missing_partition_filter",
                message="Measured query should include a block_timestamp partition filter.",
            )
        )
    if _LIMIT_PATTERN.search(sql):
        issues.append(
            QueryValidationIssue(
                code="limit_present",
                message="Measured query should not use LIMIT as a cost-control pattern.",
            )
        )
    if not _INTERVAL_PATTERN.search(sql):
        issues.append(
            QueryValidationIssue(
                code="unbounded_window",
                message="Measured query should use an explicit bounded time window.",
            )
        )
    if _JOIN_PATTERN.search(sql) and not _WITH_PATTERN.search(sql):
        issues.append(
            QueryValidationIssue(
                code="join_before_filter",
                message="Measured query should pre-filter in CTEs or subqueries before JOINs.",
            )
        )
    if _JOIN_PATTERN.search(sql) and "transfer_hashes AS" not in sql and "tracked_transfer_hashes AS" not in sql:
        issues.append(
            QueryValidationIssue(
                code="missing_prefiltered_join_keys",
                message="Measured query with JOINs should narrow joined tables through pre-filtered hash CTEs.",
            )
        )
    for pattern, message in _FORBIDDEN_MEASURED_PATTERNS:
        if pattern.search(sql):
            issues.append(
                QueryValidationIssue(
                    code="forbidden_measured_pattern",
                    message=message,
                )
            )
    return issues
