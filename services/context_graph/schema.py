"""BigQuery schema discovery for context graph extraction."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable, Optional

from services.bigquery_client import get_client


@dataclass(frozen=True)
class ChainSchema:
    chain: str
    dataset: str
    transfer_table: str
    transfer_source: str
    transactions_table: str
    receipts_table: str
    logs_table: str
    traces_table: Optional[str]
    transfer_contract_field: str
    transfer_value_field: str
    transfer_transaction_hash_field: str
    transactions_hash_field: str
    receipts_transaction_hash_field: str
    receipt_gas_price_field: str
    gas_used_field: str
    logs_address_field: str
    logs_transaction_hash_field: str
    traces_to_address_field: Optional[str]
    traces_from_address_field: Optional[str]
    traces_transaction_hash_field: Optional[str]

    def to_dict(self) -> dict:
        return asdict(self)


CHAIN_DATASETS = {
    "Ethereum": "bigquery-public-data.goog_blockchain_ethereum_mainnet_us",
    "Polygon": "bigquery-public-data.goog_blockchain_polygon_mainnet_us",
}


def _pick_field(fields: set[str], candidates: Iterable[str], fallback: str) -> str:
    for candidate in candidates:
        if candidate in fields:
            return candidate
    return fallback


def _pick_trace_address_field(fields: set[str], key: str) -> Optional[str]:
    if key in fields:
        return key
    if "action" in fields:
        return f"action.{key}"
    return None


def _get_table_fields(client, table_id: str) -> set[str]:
    table = client.get_table(table_id)
    return {field.name for field in table.schema}


def discover_chain_schema(chain: str, *, client=None) -> ChainSchema:
    client = client or get_client()
    dataset = CHAIN_DATASETS[chain]
    receipts_table = f"{dataset}.receipts"
    token_transfers_table = f"{dataset}.token_transfers"
    decoded_events_table = f"{dataset}.decoded_events"
    transactions_table = f"{dataset}.transactions"
    logs_table = f"{dataset}.logs"
    traces_table = f"{dataset}.traces"

    try:
        receipt_fields = _get_table_fields(client, receipts_table)
        transaction_fields = _get_table_fields(client, transactions_table)
        log_fields = _get_table_fields(client, logs_table)
        try:
            trace_fields = _get_table_fields(client, traces_table)
            effective_traces_table = traces_table
        except Exception:
            trace_fields = set()
            effective_traces_table = None
        if chain == "Ethereum":
            transfer_fields = _get_table_fields(client, token_transfers_table)
            transfer_table = token_transfers_table
            transfer_source = "token_transfers"
        else:
            transfer_fields = _get_table_fields(client, decoded_events_table)
            transfer_table = decoded_events_table
            transfer_source = "decoded_events"
    except Exception:
        receipt_fields = {"transaction_hash", "gas_used", "effective_gas_price"}
        transaction_fields = {"transaction_hash", "gas_price"}
        log_fields = {"transaction_hash", "address"}
        trace_fields = {"transaction_hash", "to_address", "from_address"} if chain == "Ethereum" else set()
        effective_traces_table = traces_table if chain == "Ethereum" else None
        if chain == "Ethereum":
            transfer_fields = {"transaction_hash", "address", "quantity"}
            transfer_table = token_transfers_table
            transfer_source = "token_transfers"
        else:
            transfer_fields = {"transaction_hash", "address", "args"}
            transfer_table = decoded_events_table
            transfer_source = "decoded_events"

    return ChainSchema(
        chain=chain,
        dataset=dataset,
        transfer_table=transfer_table,
        transfer_source=transfer_source,
        transactions_table=transactions_table,
        receipts_table=receipts_table,
        logs_table=logs_table,
        traces_table=effective_traces_table,
        transfer_contract_field=_pick_field(
            transfer_fields,
            ("token_address", "address"),
            "address",
        ),
        transfer_value_field=_pick_field(
            transfer_fields,
            ("value", "quantity", "args"),
            "quantity" if chain == "Ethereum" else "args",
        ),
        transfer_transaction_hash_field=_pick_field(
            transfer_fields,
            ("transaction_hash", "hash"),
            "transaction_hash",
        ),
        transactions_hash_field=_pick_field(
            transaction_fields,
            ("transaction_hash", "hash"),
            "transaction_hash",
        ),
        receipts_transaction_hash_field=_pick_field(
            receipt_fields,
            ("transaction_hash",),
            "transaction_hash",
        ),
        receipt_gas_price_field=_pick_field(
            receipt_fields,
            ("effective_gas_price", "gas_price"),
            "effective_gas_price",
        ),
        gas_used_field=_pick_field(
            receipt_fields,
            ("receipt_gas_used", "gas_used"),
            "gas_used",
        ),
        logs_address_field=_pick_field(
            log_fields,
            ("address", "contract_address"),
            "address",
        ),
        logs_transaction_hash_field=_pick_field(
            log_fields,
            ("transaction_hash", "hash"),
            "transaction_hash",
        ),
        traces_to_address_field=_pick_trace_address_field(trace_fields, "to_address")
        if trace_fields
        else None,
        traces_from_address_field=_pick_trace_address_field(trace_fields, "from_address")
        if trace_fields
        else None,
        traces_transaction_hash_field=_pick_field(
            trace_fields,
            ("transaction_hash", "hash"),
            "transaction_hash",
        )
        if trace_fields
        else None,
    )


def discover_supported_schemas(
    chains: Iterable[str] = ("Ethereum", "Polygon"),
    *,
    client=None,
) -> dict[str, ChainSchema]:
    client = client or get_client()
    return {
        chain: discover_chain_schema(chain, client=client)
        for chain in chains
    }
