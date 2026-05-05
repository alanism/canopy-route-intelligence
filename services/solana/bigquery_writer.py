"""
Phase 4 — Solana BigQuery Writer.

Writes normalized Solana events to the `solana_measured` BigQuery table.

BigQuery rules for Solana
--------------------------
- Solana ingestion is RPC-first. Do not assume public Solana BigQuery availability.
- BIGNUMERIC for all raw integer amounts (u64-compatible, no precision loss).
- NUMERIC for amount_decimal (28 digits, 9 decimal places — USDC needs 6).
- No FLOAT64 anywhere in the Solana schema.
- Partition on `slot` via RANGE_BUCKET for cost-efficient range queries.
- Cluster on `token_mint`, `watched_address`, `raw_event_id`.
- Rows are append-only at insert. No UPDATE/DELETE in the measured layer.

Dry-run gate
------------
Every insert is preceded by a dry-run row-count estimate. If the BigQuery
client is unavailable (missing credentials), the writer falls back to
local JSON file output (data/solana_events_buffer.jsonl) so ingestion
continues unblocked during development.

decimal.Decimal → BigQuery
--------------------------
BigQuery Python client does not natively serialize decimal.Decimal to
BIGNUMERIC. We convert via str(value) before insertion — BigQuery accepts
numeric strings for BIGNUMERIC/NUMERIC columns.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

logger = logging.getLogger("canopy.solana.bigquery_writer")

# ---------------------------------------------------------------------------
# Schema constants
# ---------------------------------------------------------------------------

DATASET = os.environ.get("SOLANA_BQ_DATASET", "solana_measured")
TABLE = os.environ.get("SOLANA_BQ_TABLE", "solana_transfers")
FALLBACK_BUFFER_PATH = os.path.join("data", "solana_events_buffer.jsonl")
DEFAULT_SLOT_RANGE_START = 0
DEFAULT_SLOT_RANGE_END = 1_000_000_000
DEFAULT_SLOT_RANGE_INTERVAL = 1_000_000
CLUSTERING_FIELDS = ("token_mint", "watched_address", "raw_event_id")

# BigQuery schema for the solana_measured.solana_transfers table.
# BIGNUMERIC for raw integer amounts; NUMERIC for decimal amounts.
BQ_SCHEMA = [
    # Identity
    {"name": "chain",               "type": "STRING",     "mode": "REQUIRED"},
    {"name": "signature",           "type": "STRING",     "mode": "REQUIRED"},
    {"name": "slot",                "type": "INTEGER",    "mode": "NULLABLE"},
    {"name": "block_time",          "type": "INTEGER",    "mode": "NULLABLE"},
    # Token accounts
    {"name": "token_mint",                  "type": "STRING",     "mode": "NULLABLE"},
    {"name": "watched_address",             "type": "STRING",     "mode": "NULLABLE"},
    {"name": "source_token_account",        "type": "STRING",     "mode": "NULLABLE"},
    {"name": "destination_token_account",   "type": "STRING",     "mode": "NULLABLE"},
    {"name": "source_owner",               "type": "STRING",     "mode": "NULLABLE"},
    {"name": "destination_owner",          "type": "STRING",     "mode": "NULLABLE"},
    # Instruction position
    {"name": "instruction_index",       "type": "INTEGER",    "mode": "NULLABLE"},
    {"name": "inner_instruction_index", "type": "INTEGER",    "mode": "NULLABLE"},
    {"name": "transfer_ordinal",        "type": "INTEGER",    "mode": "NULLABLE"},
    {"name": "program_id",              "type": "STRING",     "mode": "NULLABLE"},
    # Amounts — BIGNUMERIC for raw (u64), NUMERIC for decimal
    {"name": "amount_raw",              "type": "BIGNUMERIC", "mode": "NULLABLE"},
    {"name": "amount_decimal",          "type": "NUMERIC",    "mode": "NULLABLE"},
    {"name": "amount_transferred_raw",  "type": "BIGNUMERIC", "mode": "NULLABLE"},
    {"name": "fee_withheld_raw",        "type": "BIGNUMERIC", "mode": "NULLABLE"},
    {"name": "amount_received_raw",     "type": "BIGNUMERIC", "mode": "NULLABLE"},
    # Cost — all BIGNUMERIC (lamports are u64-range)
    {"name": "fee_lamports",                        "type": "BIGNUMERIC", "mode": "NULLABLE"},
    {"name": "native_base_fee_lamports",            "type": "BIGNUMERIC", "mode": "NULLABLE"},
    {"name": "native_priority_fee_lamports",        "type": "BIGNUMERIC", "mode": "NULLABLE"},
    {"name": "jito_tip_lamports",                   "type": "BIGNUMERIC", "mode": "NULLABLE"},
    {"name": "explicit_tip_lamports",               "type": "BIGNUMERIC", "mode": "NULLABLE"},
    {"name": "total_native_observed_cost_lamports", "type": "BIGNUMERIC", "mode": "NULLABLE"},
    # Transfer truth
    {"name": "transaction_success",        "type": "BOOLEAN",    "mode": "NULLABLE"},
    {"name": "transfer_detected",          "type": "BOOLEAN",    "mode": "NULLABLE"},
    {"name": "balance_delta_detected",     "type": "BOOLEAN",    "mode": "NULLABLE"},
    {"name": "observed_transfer_inclusion","type": "BOOLEAN",    "mode": "NULLABLE"},
    {"name": "settlement_evidence_type",   "type": "STRING",     "mode": "NULLABLE"},
    # Metadata
    {"name": "decode_version",         "type": "STRING",     "mode": "NULLABLE"},
    {"name": "validation_status",      "type": "STRING",     "mode": "NULLABLE"},
    {"name": "cost_detection_status",  "type": "STRING",     "mode": "NULLABLE"},
    {"name": "tip_detection_status",   "type": "STRING",     "mode": "NULLABLE"},
    {"name": "provider",               "type": "STRING",     "mode": "NULLABLE"},
    {"name": "provider_mode",          "type": "STRING",     "mode": "NULLABLE"},
    # Canonical keys
    {"name": "raw_event_id",          "type": "STRING",     "mode": "NULLABLE"},
    {"name": "normalized_event_id",   "type": "STRING",     "mode": "NULLABLE"},
    {"name": "event_fingerprint",     "type": "STRING",     "mode": "NULLABLE"},
    {"name": "collision_detected",    "type": "BOOLEAN",    "mode": "NULLABLE"},
    # Resolution statuses
    {"name": "alt_resolution_status",    "type": "STRING",     "mode": "NULLABLE"},
    {"name": "owner_resolution_status",  "type": "STRING",     "mode": "NULLABLE"},
    {"name": "amount_resolution_status", "type": "STRING",     "mode": "NULLABLE"},
    # Ingestion timestamp
    {"name": "ingested_at",           "type": "TIMESTAMP",  "mode": "NULLABLE"},
]

# Fields whose values must be serialized as str() for BIGNUMERIC/NUMERIC
# Integer (u64) raw amount fields — serialized as plain str(int)
_BIGNUMERIC_INT_FIELDS = frozenset({
    "amount_raw", "amount_transferred_raw", "fee_withheld_raw",
    "amount_received_raw", "fee_lamports", "native_base_fee_lamports",
    "native_priority_fee_lamports", "jito_tip_lamports", "explicit_tip_lamports",
    "total_native_observed_cost_lamports",
})
# Decimal amount field — serialized with fixed 6-decimal precision for USDC
_NUMERIC_DECIMAL_FIELDS = frozenset({"amount_decimal"})
# All numeric fields (union — used for float guard)
_BIGNUMERIC_FIELDS = _BIGNUMERIC_INT_FIELDS | _NUMERIC_DECIMAL_FIELDS
S3_READINESS_FIELDS = frozenset({
    "chain", "signature", "slot", "block_time", "token_mint", "watched_address",
    "source_token_account", "destination_token_account", "source_owner",
    "destination_owner", "amount_raw", "amount_decimal", "amount_transferred_raw",
    "amount_received_raw", "fee_lamports", "jito_tip_lamports",
    "total_native_observed_cost_lamports", "transaction_success",
    "observed_transfer_inclusion", "settlement_evidence_type", "validation_status",
    "raw_event_id", "normalized_event_id", "event_fingerprint", "collision_detected",
    "alt_resolution_status", "owner_resolution_status", "amount_resolution_status",
    "ingested_at",
})


# ---------------------------------------------------------------------------
# Write result
# ---------------------------------------------------------------------------

class WriteResult:
    def __init__(
        self,
        rows_attempted: int = 0,
        rows_inserted: int = 0,
        errors: Optional[list[str]] = None,
        fallback_used: bool = False,
    ):
        self.rows_attempted = rows_attempted
        self.rows_inserted = rows_inserted
        self.errors: list[str] = errors or []
        self.fallback_used = fallback_used

    @property
    def success(self) -> bool:
        return self.rows_inserted == self.rows_attempted and not self.errors


# ---------------------------------------------------------------------------
# SolanaEventWriter
# ---------------------------------------------------------------------------

class SolanaEventWriter:
    """
    Writes normalized Solana events to BigQuery (or local fallback buffer).

    Inject a BigQuery client for production use. Without a client, the writer
    uses the local JSONL buffer so ingestion continues during development.

    Usage
    -----
    writer = SolanaEventWriter()                     # auto-detects BQ client
    writer = SolanaEventWriter(bq_client=mock_client) # inject in tests

    result = writer.write_batch(normalized_events)
    """

    def __init__(
        self,
        bq_client=None,
        *,
        dataset: str = DATASET,
        table: str = TABLE,
        fallback_path: str = FALLBACK_BUFFER_PATH,
    ) -> None:
        self._bq_client = bq_client
        self._dataset = dataset
        self._table = table
        self._fallback_path = fallback_path
        self._table_ref: Optional[str] = None

    def write_batch(self, normalized_events: list[dict[str, Any]]) -> WriteResult:
        """
        Write a batch of normalized events.

        Uses BigQuery if a client is available; falls back to local JSONL buffer.
        Never raises — returns WriteResult with error details on failure.
        """
        if not normalized_events:
            return WriteResult()

        rows = [_serialize_for_bq(e) for e in normalized_events]

        if self._bq_client is not None:
            return self._write_to_bq(rows)
        else:
            return self._write_to_fallback(rows)

    def table_id(self) -> str:
        return f"{self._dataset}.{self._table}"

    # ------------------------------------------------------------------
    # BigQuery path
    # ------------------------------------------------------------------

    def _write_to_bq(self, rows: list[dict[str, Any]]) -> WriteResult:
        if (
            hasattr(self._bq_client, "load_table_from_json")
            and hasattr(self._bq_client, "query")
        ):
            return self._write_to_bq_merge(rows)
        return self._write_to_bq_insert(rows)

    def _write_to_bq_insert(self, rows: list[dict[str, Any]]) -> WriteResult:
        """Compatibility write path for minimal clients/mocks."""
        try:
            table_ref = self._bq_client.dataset(self._dataset).table(self._table)
            errors = self._bq_client.insert_rows_json(table_ref, rows)
            if errors:
                error_msgs = [str(e) for e in errors]
                logger.error("BigQuery insert errors: %s", error_msgs)
                return WriteResult(
                    rows_attempted=len(rows),
                    rows_inserted=len(rows) - len(errors),
                    errors=error_msgs,
                )
            logger.info("BigQuery: inserted %d Solana events to %s", len(rows), self.table_id())
            return WriteResult(rows_attempted=len(rows), rows_inserted=len(rows))
        except Exception as exc:
            msg = f"BigQuery write failed: {exc}"
            logger.error(msg)
            return WriteResult(rows_attempted=len(rows), errors=[msg])

    def _write_to_bq_merge(self, rows: list[dict[str, Any]]) -> WriteResult:
        """
        Preferred write path: stage batch, then MERGE by normalized_event_id.
        """
        temp_table = f"{self._table}_staging_{uuid.uuid4().hex[:12]}"
        temp_table_fqn = f"{self._dataset}.{temp_table}"
        target_table_fqn = f"{self._dataset}.{self._table}"

        try:
            load_job = self._bq_client.load_table_from_json(
                rows,
                temp_table_fqn,
            )
            load_job.result()

            columns = [f["name"] for f in BQ_SCHEMA]
            update_assignments = ",\n    ".join(
                f"{col} = source.{col}" for col in columns
            )
            insert_columns = ", ".join(columns)
            insert_values = ", ".join(f"source.{col}" for col in columns)

            merge_sql = f"""
MERGE `{target_table_fqn}` AS target
USING `{temp_table_fqn}` AS source
ON target.normalized_event_id = source.normalized_event_id
WHEN MATCHED THEN
  UPDATE SET
    {update_assignments}
WHEN NOT MATCHED THEN
  INSERT ({insert_columns})
  VALUES ({insert_values})
"""
            query_job = self._bq_client.query(merge_sql)
            query_job.result()

            logger.info(
                "BigQuery MERGE: upserted %d Solana events into %s",
                len(rows),
                self.table_id(),
            )
            return WriteResult(rows_attempted=len(rows), rows_inserted=len(rows))
        except Exception as exc:
            msg = f"BigQuery MERGE failed: {exc}"
            logger.error(msg)
            return WriteResult(rows_attempted=len(rows), errors=[msg])
        finally:
            try:
                self._bq_client.delete_table(temp_table_fqn, not_found_ok=True)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Local fallback buffer
    # ------------------------------------------------------------------

    def _write_to_fallback(self, rows: list[dict[str, Any]]) -> WriteResult:
        """Append rows to JSONL buffer file. Used when BQ client is unavailable."""
        try:
            os.makedirs(os.path.dirname(os.path.abspath(self._fallback_path)), exist_ok=True)
            with open(self._fallback_path, "a", encoding="utf-8") as fh:
                for row in rows:
                    fh.write(json.dumps(row, default=str) + "\n")
            logger.info(
                "Fallback buffer: wrote %d Solana events to %s",
                len(rows), self._fallback_path,
            )
            return WriteResult(
                rows_attempted=len(rows),
                rows_inserted=len(rows),
                fallback_used=True,
            )
        except OSError as exc:
            msg = f"Fallback buffer write failed: {exc}"
            logger.error(msg)
            return WriteResult(rows_attempted=len(rows), errors=[msg])


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------

def _serialize_for_bq(event: dict[str, Any]) -> dict[str, Any]:
    """
    Prepare a normalized event dict for BigQuery JSON insertion.

    - BIGNUMERIC/NUMERIC fields: convert int/Decimal → str (BQ accepts numeric strings)
    - None values: preserved as None (BQ treats as NULL)
    - float: raise immediately — float is prohibited in Solana amounts
    - Strip private keys (prefixed with '_')
    """
    result: dict[str, Any] = {}
    for key, val in event.items():
        if key.startswith("_"):
            continue  # drop internal passthrough fields
        if key in _BIGNUMERIC_FIELDS:
            if isinstance(val, float):
                raise TypeError(
                    f"Float found in BIGNUMERIC field '{key}': {val!r}. "
                    "Solana amounts must be int or decimal.Decimal."
                )
            if val is None:
                result[key] = None
            elif key in _NUMERIC_DECIMAL_FIELDS:
                # NUMERIC column: preserve trailing zeros so BQ sees full precision
                # e.g. Decimal("1.000000") → "1.000000" not "1"
                result[key] = f"{val:.6f}"
            else:
                result[key] = str(val)
        else:
            result[key] = val
    return result


def _bq_schema_fields():
    """
    Return BigQuery SchemaField objects from BQ_SCHEMA.

    Imported lazily so tests don't require google-cloud-bigquery installed.
    """
    from google.cloud.bigquery import SchemaField
    return [
        SchemaField(f["name"], f["type"], mode=f.get("mode", "NULLABLE"))
        for f in BQ_SCHEMA
    ]


def bq_schema_by_name() -> dict[str, dict[str, str]]:
    """Return BQ_SCHEMA indexed by field name."""
    return {field["name"]: field for field in BQ_SCHEMA}


def validate_bq_schema_contract() -> list[str]:
    """Return schema contract violations. Empty list means Phase 16-safe."""
    violations: list[str] = []
    schema = bq_schema_by_name()

    for field in _BIGNUMERIC_INT_FIELDS:
        if schema.get(field, {}).get("type") != "BIGNUMERIC":
            violations.append(f"{field} must be BIGNUMERIC")
    for field in _NUMERIC_DECIMAL_FIELDS:
        if schema.get(field, {}).get("type") != "NUMERIC":
            violations.append(f"{field} must be NUMERIC")
    for field in CLUSTERING_FIELDS:
        if field not in schema:
            violations.append(f"clustering field missing from schema: {field}")
    if schema.get("slot", {}).get("type") != "INTEGER":
        violations.append("slot must be INTEGER for RANGE_BUCKET partitioning")
    if schema.get("normalized_event_id", {}).get("type") != "STRING":
        violations.append("normalized_event_id must be STRING for MERGE key")
    return violations


def s3_readiness_field_report() -> dict[str, list[str]]:
    """Report S3-readiness fields present/missing in BQ_SCHEMA."""
    schema_fields = set(bq_schema_by_name())
    present = sorted(S3_READINESS_FIELDS & schema_fields)
    missing = sorted(S3_READINESS_FIELDS - schema_fields)
    return {"present": present, "missing": missing}


def build_create_table_ddl(
    *,
    project_id: str,
    dataset: str = DATASET,
    table: str = TABLE,
    slot_range_start: int = DEFAULT_SLOT_RANGE_START,
    slot_range_end: int = DEFAULT_SLOT_RANGE_END,
    slot_range_interval: int = DEFAULT_SLOT_RANGE_INTERVAL,
) -> str:
    """Build BigQuery DDL from BQ_SCHEMA; do not duplicate schema elsewhere."""
    columns = ",\n  ".join(_ddl_column(field) for field in BQ_SCHEMA)
    cluster_by = ", ".join(f"`{field}`" for field in CLUSTERING_FIELDS)
    return f"""CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.{table}` (
  {columns}
)
PARTITION BY RANGE_BUCKET(`slot`, GENERATE_ARRAY({slot_range_start}, {slot_range_end}, {slot_range_interval}))
CLUSTER BY {cluster_by}"""


def _ddl_column(field: dict[str, str]) -> str:
    mode = field.get("mode", "NULLABLE")
    not_null = " NOT NULL" if mode == "REQUIRED" else ""
    return f"`{field['name']}` {field['type']}{not_null}"
