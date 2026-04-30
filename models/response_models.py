"""Response models for the Canopy execution engine."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ExecutionStepModel(BaseModel):
    operation: str
    chain: str
    amount_in: float
    amount_out: float
    fee_usd: float
    estimated_seconds: int
    details: Dict[str, Any] = Field(default_factory=dict)


class QuoteModel(BaseModel):
    quote_id: str
    expected_received: float
    min_received: float
    valid_until: str
    confidence_score: float
    quote_ttl_seconds: int
    status: str


class ExecutionPlanModel(BaseModel):
    total_received: float
    total_fees_usd: float
    estimated_time_seconds: int
    settlement_time_confidence: str
    settlement_range: str
    algorithm_used: str
    route: List[str]
    steps: List[ExecutionStepModel]


class RiskProfileModel(BaseModel):
    confidence_score: float
    liquidity_score: float
    safety_score: float
    flags: List[str]
    alerts: List[str]


class DataFreshnessModel(BaseModel):
    gas_age_sec: int
    pool_age_sec: int
    snapshot_age_sec: int
    snapshot_expires_in_sec: int
    warnings: List[str]


class SimulationResponseModel(BaseModel):
    simulation_id: str
    state_snapshot_id: str
    quote: QuoteModel
    execution_plan: ExecutionPlanModel
    risk_profile: RiskProfileModel
    data_freshness: DataFreshnessModel


class GraphNodeModel(BaseModel):
    node_id: str
    label: str
    node_type: str
    total_volume: float
    transaction_count: int
    degree: int


class GraphEdgeModel(BaseModel):
    source_node: str
    destination_node: str
    source_type: str
    destination_type: str
    edge_type: str
    token: str
    total_volume: float
    transaction_count: int
    last_seen: str
    avg_gas_fee: float
    sample_transaction_hash: str
    evidence_type: str


class TopologySignalModel(BaseModel):
    name: str
    value: float
    label: str


class EvidenceItemModel(BaseModel):
    kind: str
    source: str
    destination: str
    transaction_count: int
    total_volume: float
    last_seen: str
    sample_transaction_hash: str
    evidence_type: str


class ContextGraphResponseModel(BaseModel):
    corridor_id: str
    corridor: Optional[str] = None
    corridor_key: str
    chain: str
    token: str
    time_range: str
    requested_time_range: Optional[str] = None
    status: str
    graph_generated_at: Optional[str] = None
    graph_cache_status: str
    graph_cache_age_seconds: Optional[int] = None
    query_mode: Optional[str] = None
    topology: str
    topology_classification: str
    liquidity_hubs: List[GraphNodeModel] = Field(default_factory=list)
    nodes: List[GraphNodeModel] = Field(default_factory=list)
    edges: List[GraphEdgeModel] = Field(default_factory=list)
    signals: List[TopologySignalModel] = Field(default_factory=list)
    flow_density: float
    protocol_noise_ratio: float
    bridge_usage_rate: float
    counterparty_entropy: float
    liquidity_gap: float
    confidence_score: float
    evidence_stack: List[EvidenceItemModel] = Field(default_factory=list)
