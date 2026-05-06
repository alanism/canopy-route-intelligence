"""Phase 16.5 internal-only shadow S3 signal validation helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from services.query_validator import QueryClassification
from services.solana.bigquery_writer import S3_READINESS_FIELDS

SHADOW_PROJECT_ID = "canopy-main"
SHADOW_DATASET = "solana_measured_sandbox"
SHADOW_TABLE = "solana_transfers_phase16_test"
SHADOW_MAX_BYTES_BILLED = 50_000_000
SHADOW_QUERY_FAMILY = "solana_shadow_validation"
SHADOW_QUERY_NAMES = (
    "shadow_success_purity",
    "shadow_mev_protection_rate",
    "shadow_settlement_velocity",
    "shadow_fee_efficiency",
    "shadow_missing_field_report",
)


class ShadowValidationTargetError(ValueError):
    """Raised when Phase 16.5 attempts to use non-sandbox resources."""


@dataclass(frozen=True)
class ShadowTarget:
    project_id: str
    dataset: str
    table: str

    @property
    def table_fqn(self) -> str:
        return f"`{self.project_id}.{self.dataset}.{self.table}`"

    @property
    def table_path(self) -> str:
        return f"{self.project_id}.{self.dataset}.{self.table}"


@dataclass(frozen=True)
class ShadowQueryDefinition:
    name: str
    sql: str
    query_family: str = SHADOW_QUERY_FAMILY
    classification: QueryClassification = "dev_only"
    maximum_bytes_billed: int = SHADOW_MAX_BYTES_BILLED


def resolve_shadow_target(
    *,
    project_id: str | None = None,
    dataset: str | None = None,
    table: str | None = None,
) -> ShadowTarget:
    target = ShadowTarget(
        project_id=project_id or SHADOW_PROJECT_ID,
        dataset=dataset or SHADOW_DATASET,
        table=table or SHADOW_TABLE,
    )
    validate_shadow_target(target)
    return target


def validate_shadow_target(target: ShadowTarget) -> None:
    expected = (SHADOW_PROJECT_ID, SHADOW_DATASET, SHADOW_TABLE)
    actual = (target.project_id, target.dataset, target.table)
    if actual != expected:
        raise ShadowValidationTargetError(
            "Phase 16.5 sandbox guard failed: expected "
            f"{SHADOW_PROJECT_ID}.{SHADOW_DATASET}.{SHADOW_TABLE}, got "
            f"{target.project_id}.{target.dataset}.{target.table}"
        )


def build_slot_bounds_sql(target: ShadowTarget) -> str:
    return f"""
SELECT
  MIN(slot) AS slot_min,
  MAX(slot) AS slot_max,
  COUNT(*) AS total_rows,
  MIN(TIMESTAMP_SECONDS(block_time)) AS min_block_time,
  MAX(TIMESTAMP_SECONDS(block_time)) AS max_block_time
FROM {target.table_fqn}
WHERE slot IS NOT NULL
""".strip()


def validate_slot_bounds(slot_min: int | None, slot_max: int | None) -> tuple[int, int]:
    if slot_min is None or slot_max is None:
        raise ValueError("slot_min and slot_max must both be provided for shadow validation queries.")
    if slot_min > slot_max:
        raise ValueError(f"slot_min {slot_min} must be <= slot_max {slot_max}.")
    return slot_min, slot_max


def shadow_query_definitions(
    *,
    target: ShadowTarget,
    slot_min: int,
    slot_max: int,
    unavailable_due_to_cold_start: bool,
    cold_start_window_seconds: int | None,
    present_fields: set[str],
) -> list[ShadowQueryDefinition]:
    validate_shadow_target(target)
    slot_min, slot_max = validate_slot_bounds(slot_min, slot_max)
    definitions = [
        ShadowQueryDefinition(
            name="shadow_success_purity",
            sql=_build_shadow_success_purity_sql(
                target=target,
                slot_min=slot_min,
                slot_max=slot_max,
                unavailable_due_to_cold_start=unavailable_due_to_cold_start,
                cold_start_window_seconds=cold_start_window_seconds,
            ),
        ),
        ShadowQueryDefinition(
            name="shadow_mev_protection_rate",
            sql=_build_shadow_mev_protection_rate_sql(target=target, slot_min=slot_min, slot_max=slot_max),
        ),
        ShadowQueryDefinition(
            name="shadow_settlement_velocity",
            sql=_build_shadow_settlement_velocity_sql(target=target, slot_min=slot_min, slot_max=slot_max),
        ),
        ShadowQueryDefinition(
            name="shadow_fee_efficiency",
            sql=_build_shadow_fee_efficiency_sql(target=target, slot_min=slot_min, slot_max=slot_max),
        ),
        ShadowQueryDefinition(
            name="shadow_missing_field_report",
            sql=_build_shadow_missing_field_report_sql(
                target=target,
                slot_min=slot_min,
                slot_max=slot_max,
                present_fields=present_fields,
            ),
        ),
    ]
    actual_names = tuple(definition.name for definition in definitions)
    if actual_names != SHADOW_QUERY_NAMES:
        raise AssertionError(f"Unexpected shadow query names: {actual_names!r}")
    return definitions


def shadow_missing_fields(table_fields: set[str]) -> list[str]:
    return sorted(S3_READINESS_FIELDS - set(table_fields))


def cold_start_window_status(bounds_row: dict[str, Any]) -> tuple[bool, int | None]:
    min_block_time = _coerce_timestamp(bounds_row.get("min_block_time"))
    max_block_time = _coerce_timestamp(bounds_row.get("max_block_time"))
    if min_block_time is None or max_block_time is None:
        return True, None
    window_seconds = int((max_block_time - min_block_time).total_seconds())
    return window_seconds < 7 * 24 * 60 * 60, window_seconds


def _coerce_timestamp(value: Any) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    if isinstance(value, str):
        normalized = value.replace(" ", "T")
        if normalized.endswith(" UTC"):
            normalized = normalized[:-4] + "+00:00"
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        return datetime.fromisoformat(normalized)
    raise TypeError(f"Unsupported timestamp value for shadow validation: {value!r}")


def _bounded_source_cte(target: ShadowTarget, slot_min: int, slot_max: int, columns: list[str]) -> str:
    column_sql = ",\n    ".join(columns)
    return f"""
WITH bounded_source AS (
  SELECT
    {column_sql}
  FROM {target.table_fqn}
  WHERE slot BETWEEN {slot_min} AND {slot_max}
)
""".strip()


def _build_shadow_success_purity_sql(
    *,
    target: ShadowTarget,
    slot_min: int,
    slot_max: int,
    unavailable_due_to_cold_start: bool,
    cold_start_window_seconds: int | None,
) -> str:
    cte = _bounded_source_cte(
        target,
        slot_min,
        slot_max,
        [
            "slot",
            "transaction_success",
            "validation_status",
            "observed_transfer_inclusion",
        ],
    )
    cold_start_seconds = "NULL" if cold_start_window_seconds is None else str(cold_start_window_seconds)
    cold_start_flag = "TRUE" if unavailable_due_to_cold_start else "FALSE"
    return f"""
{cte}
SELECT
  'shadow_success_purity' AS shadow_query_name,
  'internal_only' AS output_scope,
  'shadow_success_purity is an internal validation metric, not a product KPI.' AS shadow_note,
  {slot_min} AS slot_min,
  {slot_max} AS slot_max,
  COUNT(*) AS total_rows,
  COUNTIF(
    transaction_success IS NOT NULL
    AND validation_status IS NOT NULL
    AND observed_transfer_inclusion IS NOT NULL
  ) AS qualifying_rows,
  COUNTIF(
    transaction_success = TRUE
    AND validation_status = 'ok'
    AND observed_transfer_inclusion = TRUE
  ) AS success_rows,
  SAFE_DIVIDE(
    COUNTIF(
      transaction_success = TRUE
      AND validation_status = 'ok'
      AND observed_transfer_inclusion = TRUE
    ),
    NULLIF(COUNTIF(
      transaction_success IS NOT NULL
      AND validation_status IS NOT NULL
      AND observed_transfer_inclusion IS NOT NULL
    ), 0)
  ) AS success_purity_rate,
  {cold_start_flag} AS seven_day_unavailable_due_to_cold_start,
  {cold_start_seconds} AS observed_window_seconds
FROM bounded_source
""".strip()


def _build_shadow_mev_protection_rate_sql(*, target: ShadowTarget, slot_min: int, slot_max: int) -> str:
    cte = _bounded_source_cte(
        target,
        slot_min,
        slot_max,
        [
            "slot",
            "jito_tip_lamports",
            "explicit_tip_lamports",
            "settlement_evidence_type",
            "transaction_success",
        ],
    )
    return f"""
{cte}
SELECT
  'shadow_mev_protection_rate' AS shadow_query_name,
  'internal_only' AS output_scope,
  'shadow_mev_protection_rate is a shadow proxy, not product truth.' AS shadow_note,
  {slot_min} AS slot_min,
  {slot_max} AS slot_max,
  COUNT(*) AS total_rows,
  COUNTIF(COALESCE(jito_tip_lamports, 0) > 0) AS jito_tip_positive_rows,
  COUNTIF(
    COALESCE(jito_tip_lamports, 0) > 0
    OR COALESCE(explicit_tip_lamports, 0) > 0
    OR settlement_evidence_type = 'both'
  ) AS protected_proxy_rows,
  SAFE_DIVIDE(
    COUNTIF(
      COALESCE(jito_tip_lamports, 0) > 0
      OR COALESCE(explicit_tip_lamports, 0) > 0
      OR settlement_evidence_type = 'both'
    ),
    NULLIF(COUNTIF(transaction_success = TRUE), 0)
  ) AS mev_protection_proxy_rate
FROM bounded_source
""".strip()


def _build_shadow_settlement_velocity_sql(*, target: ShadowTarget, slot_min: int, slot_max: int) -> str:
    cte = _bounded_source_cte(
        target,
        slot_min,
        slot_max,
        [
            "slot",
            "block_time",
            "ingested_at",
        ],
    )
    return f"""
{cte},
latency_source AS (
  SELECT
    slot,
    TIMESTAMP_DIFF(ingested_at, TIMESTAMP_SECONDS(block_time), SECOND) AS latency_seconds
  FROM bounded_source
  WHERE block_time IS NOT NULL
    AND ingested_at IS NOT NULL
)
SELECT
  'shadow_settlement_velocity' AS shadow_query_name,
  'internal_only' AS output_scope,
  'shadow_settlement_velocity summarizes observed ingestion lag only.' AS shadow_note,
  {slot_min} AS slot_min,
  {slot_max} AS slot_max,
  COUNT(*) AS latency_rows,
  MIN(latency_seconds) AS min_latency_seconds,
  MAX(latency_seconds) AS max_latency_seconds,
  AVG(latency_seconds) AS avg_latency_seconds,
  APPROX_QUANTILES(latency_seconds, 100)[OFFSET(50)] AS p50_latency_seconds,
  APPROX_QUANTILES(latency_seconds, 100)[OFFSET(95)] AS p95_latency_seconds
FROM latency_source
""".strip()


def _build_shadow_fee_efficiency_sql(*, target: ShadowTarget, slot_min: int, slot_max: int) -> str:
    cte = _bounded_source_cte(
        target,
        slot_min,
        slot_max,
        [
            "slot",
            "amount_transferred_raw",
            "total_native_observed_cost_lamports",
        ],
    )
    return f"""
{cte},
efficiency_source AS (
  SELECT
    slot,
    CAST(amount_transferred_raw AS BIGNUMERIC) AS amount_transferred_raw_bn,
    CAST(total_native_observed_cost_lamports AS BIGNUMERIC) AS total_native_observed_cost_lamports_bn,
    SAFE_DIVIDE(
      CAST(amount_transferred_raw AS BIGNUMERIC),
      NULLIF(CAST(total_native_observed_cost_lamports AS BIGNUMERIC), 0)
    ) AS transfer_per_lamport
  FROM bounded_source
  WHERE amount_transferred_raw IS NOT NULL
    AND total_native_observed_cost_lamports IS NOT NULL
)
SELECT
  'shadow_fee_efficiency' AS shadow_query_name,
  'internal_only' AS output_scope,
  'shadow_fee_efficiency is an internal-only cost relationship summary.' AS shadow_note,
  {slot_min} AS slot_min,
  {slot_max} AS slot_max,
  COUNT(*) AS qualified_rows,
  AVG(amount_transferred_raw_bn) AS avg_amount_transferred_raw,
  AVG(total_native_observed_cost_lamports_bn) AS avg_total_native_observed_cost_lamports,
  AVG(transfer_per_lamport) AS avg_transfer_per_lamport,
  MIN(transfer_per_lamport) AS min_transfer_per_lamport,
  MAX(transfer_per_lamport) AS max_transfer_per_lamport
FROM efficiency_source
""".strip()


def _build_shadow_missing_field_report_sql(
    *,
    target: ShadowTarget,
    slot_min: int,
    slot_max: int,
    present_fields: set[str],
) -> str:
    validate_shadow_target(target)
    validate_slot_bounds(slot_min, slot_max)
    selects: list[str] = []
    for field_name in sorted(S3_READINESS_FIELDS & set(present_fields)):
        field_expr = f"`{field_name}`"
        selects.append(
            f"""SELECT
  'shadow_missing_field_report' AS shadow_query_name,
  'internal_only' AS output_scope,
  '{field_name}' AS field_name,
  'present' AS field_state,
  COUNT(*) AS total_rows,
  COUNTIF({field_expr} IS NULL) AS null_count,
  SAFE_DIVIDE(COUNTIF({field_expr} IS NULL), NULLIF(COUNT(*), 0)) AS null_rate
FROM {target.table_fqn}
WHERE slot BETWEEN {slot_min} AND {slot_max}"""
        )
    if not selects:
        raise ValueError("shadow_missing_field_report requires at least one present S3 readiness field.")
    return "\nUNION ALL\n".join(selects)
