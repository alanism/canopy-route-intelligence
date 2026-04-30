"""
BigQuery helper for Canopy corridor analytics.

This module is intentionally lightweight:
- one cached client
- one small in-memory dataframe cache
- pandas dataframe return type for downstream analytics work
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from typing import Any, Dict, Optional, Tuple

import google.auth
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from services.query_metrics import record_query_metric
from services.query_validator import QueryClassification, validate_query
from services.request_context import get_request_id
from services.runtime_mode import is_demo_mode

_client: Optional[bigquery.Client] = None
_query_cache: Dict[str, Tuple[float, pd.DataFrame]] = {}
logger = logging.getLogger("sci-agent.bigquery")
DEFAULT_MAX_BYTES_BILLED = int(os.getenv("CANOPY_BIGQUERY_MAX_BYTES_BILLED", "1000000000"))
DEV_ONLY_BIGQUERY_ENABLED = os.getenv("CANOPY_ENABLE_DEV_BIGQUERY", "false").lower() == "true"


def _cache_key(sql: str, params: Optional[Dict[str, Any]]) -> str:
    payload = f"{sql}::{sorted((params or {}).items())}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def get_client() -> bigquery.Client:
    global _client
    if _client is not None:
        return _client

    project = os.getenv("GCP_PROJECT_ID")
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    if credentials_path and os.path.exists(credentials_path):
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        _client = bigquery.Client(project=project, credentials=credentials)
        return _client

    original_credentials_path = os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
    try:
        credentials, discovered_project = google.auth.default()
    finally:
        if original_credentials_path is not None:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = original_credentials_path

    effective_project = (
        project
        or os.getenv("GOOGLE_CLOUD_PROJECT")
        or os.getenv("GCLOUD_PROJECT")
        or discovered_project
    )
    _client = bigquery.Client(project=effective_project, credentials=credentials)
    return _client


def _build_query_parameters(
    params: Optional[Dict[str, Any]],
) -> list[bigquery.ScalarQueryParameter]:
    return [
        bigquery.ScalarQueryParameter(name, "STRING", value)
        for name, value in (params or {}).items()
    ]


def _build_job_config(
    *,
    params: Optional[Dict[str, Any]] = None,
    dry_run: bool = False,
    use_query_cache: bool = True,
    maximum_bytes_billed: Optional[int] = None,
) -> bigquery.QueryJobConfig:
    return bigquery.QueryJobConfig(
        query_parameters=_build_query_parameters(params),
        dry_run=dry_run,
        use_query_cache=use_query_cache,
        maximum_bytes_billed=maximum_bytes_billed or DEFAULT_MAX_BYTES_BILLED,
    )


def dry_run_sql(
    sql: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    query_name: str = "unnamed_query",
    query_family: Optional[str] = None,
    maximum_bytes_billed: Optional[int] = None,
    query_classification: QueryClassification = "derived",
    enforce_validation: bool = False,
    allow_request_scoped: bool = False,
) -> int:
    _validate_query(
        sql,
        query_name=query_name,
        query_family=query_family,
        maximum_bytes_billed=maximum_bytes_billed or DEFAULT_MAX_BYTES_BILLED,
        query_classification=query_classification,
        enforce_validation=enforce_validation,
        allow_request_scoped=allow_request_scoped,
    )
    client = get_client()
    dry_job = client.query(
        sql,
        job_config=_build_job_config(
            params=params,
            dry_run=True,
            use_query_cache=False,
            maximum_bytes_billed=maximum_bytes_billed,
        ),
    )
    bytes_processed = int(dry_job.total_bytes_processed or 0)
    logger.info(
        "BigQuery dry run complete",
        extra={
            "query_name": query_name,
            "bytes_processed": bytes_processed,
            "maximum_bytes_billed": maximum_bytes_billed or DEFAULT_MAX_BYTES_BILLED,
        },
    )
    record_query_metric(
        phase="dry_run",
        query_name=query_name,
        query_family=query_family or "unknown",
        query_classification=query_classification,
        bytes_processed=bytes_processed,
        maximum_bytes_billed=maximum_bytes_billed or DEFAULT_MAX_BYTES_BILLED,
    )
    return bytes_processed


def run_query(
    sql: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    query_name: str = "unnamed_query",
    query_family: Optional[str] = None,
    maximum_bytes_billed: Optional[int] = None,
    use_query_cache: bool = True,
    query_classification: QueryClassification = "derived",
    enforce_validation: bool = False,
    allow_request_scoped: bool = False,
) -> tuple[bigquery.QueryJob, Any]:
    dry_run_sql(
        sql,
        params=params,
        query_name=query_name,
        query_family=query_family,
        maximum_bytes_billed=maximum_bytes_billed,
        query_classification=query_classification,
        enforce_validation=enforce_validation,
        allow_request_scoped=allow_request_scoped,
    )

    started_at = time.perf_counter()
    client = get_client()
    query_job = client.query(
        sql,
        job_config=_build_job_config(
            params=params,
            dry_run=False,
            use_query_cache=use_query_cache,
            maximum_bytes_billed=maximum_bytes_billed,
        ),
    )
    rows = query_job.result()
    execution_time = round(time.perf_counter() - started_at, 3)
    logger.info(
        "BigQuery query complete",
        extra={
            "query_name": query_name,
            "bytes_processed": int(query_job.total_bytes_processed or 0),
            "execution_time": execution_time,
        },
    )
    record_query_metric(
        phase="execution",
        query_name=query_name,
        query_family=query_family or "unknown",
        query_classification=query_classification,
        bytes_processed=int(query_job.total_bytes_processed or 0),
        maximum_bytes_billed=maximum_bytes_billed or DEFAULT_MAX_BYTES_BILLED,
        execution_time=execution_time,
    )
    return query_job, rows


def execute_sql(
    sql: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    ttl_seconds: int = 300,
    use_cache: bool = True,
    query_name: str = "unnamed_query",
    query_family: Optional[str] = None,
    maximum_bytes_billed: Optional[int] = None,
    query_classification: QueryClassification = "derived",
    enforce_validation: bool = False,
    allow_request_scoped: bool = False,
) -> pd.DataFrame:
    cache_key = _cache_key(sql, params)
    now = time.time()

    if use_cache and cache_key in _query_cache:
        cached_at, cached_df = _query_cache[cache_key]
        if now - cached_at <= ttl_seconds:
            return cached_df.copy(deep=True)

    _, rows = run_query(
        sql,
        params=params,
        query_name=query_name,
        query_family=query_family,
        maximum_bytes_billed=maximum_bytes_billed,
        use_query_cache=use_cache,
        query_classification=query_classification,
        enforce_validation=enforce_validation,
        allow_request_scoped=allow_request_scoped,
    )
    dataframe = rows.to_dataframe(create_bqstorage_client=False)

    if use_cache:
        _query_cache[cache_key] = (now, dataframe.copy(deep=True))

    return dataframe


def _validate_query(
    sql: str,
    *,
    query_name: str,
    query_family: Optional[str],
    maximum_bytes_billed: int,
    query_classification: QueryClassification,
    enforce_validation: bool,
    allow_request_scoped: bool,
) -> None:
    if is_demo_mode():
        raise RuntimeError(
            f"BigQuery execution is disabled in demo mode for {query_name}."
        )
    request_scoped = get_request_id() is not None
    if request_scoped and not allow_request_scoped:
        raise RuntimeError(
            f"BigQuery execution is not allowed on the request path for {query_name}."
        )
    if query_classification == "dev_only" and not DEV_ONLY_BIGQUERY_ENABLED:
        raise RuntimeError(
            f"Dev-only BigQuery query {query_name} is disabled. Set CANOPY_ENABLE_DEV_BIGQUERY=true "
            "only for audit or parity workflows."
        )

    issues = validate_query(
        sql,
        classification=query_classification,
        query_name=query_name,
        query_family=query_family,
        maximum_bytes_billed=maximum_bytes_billed,
        request_scoped=request_scoped,
    )
    for issue in issues:
        logger.warning(
            "BigQuery query validation warning",
            extra={
                "query_name": query_name,
                "query_family": query_family,
                "query_classification": query_classification,
                "validation_code": issue.code,
                "validation_message": issue.message,
            },
        )
    if issues and enforce_validation:
        details = "; ".join(issue.message for issue in issues)
        raise ValueError(f"Query validation failed for {query_name}: {details}")
