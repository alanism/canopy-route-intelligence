# BigQuery Live Audit Report

Generated at: `2026-03-20T18:14:02.958215+00:00`

Overall status: **PASS**

## corridor_live_parity

Status: **PASS**

- Corridor parity uses materialized summaries versus fresh recomputation from current batch poll inputs.
- No request-path BigQuery execution occurs during this audit check.

### Scope: `{"corridor_id": "US-MX", "rail": "Ethereum", "time_range": "24h", "token": "USDC"}`
- Status: `pass`
- Fields compared: `volume_24h, volume_7d, tx_count, unique_senders, unique_receivers, velocity_unique_capital, concentration_score, bridge_name, bridge_share, bridge_volume, bridge_transactions, whale_threshold_usd, whale_activity_score, net_flow_7d, top_whale_flows, source, data_layer, serving_path`
- Notes: `Materialized corridor summary compared against fresh recomputation from cache poll inputs.`
- Mismatches: none

### Scope: `{"corridor_id": "US-MX", "rail": "Polygon", "time_range": "24h", "token": "USDC"}`
- Status: `pass`
- Fields compared: `volume_24h, volume_7d, tx_count, unique_senders, unique_receivers, velocity_unique_capital, concentration_score, bridge_name, bridge_share, bridge_volume, bridge_transactions, whale_threshold_usd, whale_activity_score, net_flow_7d, top_whale_flows, source, data_layer, serving_path`
- Notes: `Materialized corridor summary compared against fresh recomputation from cache poll inputs.`
- Mismatches: none

### Scope: `{"corridor_id": "US-BR", "rail": "Ethereum", "time_range": "24h", "token": "USDC"}`
- Status: `pass`
- Fields compared: `volume_24h, volume_7d, tx_count, unique_senders, unique_receivers, velocity_unique_capital, concentration_score, bridge_name, bridge_share, bridge_volume, bridge_transactions, whale_threshold_usd, whale_activity_score, net_flow_7d, top_whale_flows, source, data_layer, serving_path`
- Notes: `Materialized corridor summary compared against fresh recomputation from cache poll inputs.`
- Mismatches: none

### Scope: `{"corridor_id": "US-BR", "rail": "Polygon", "time_range": "24h", "token": "USDC"}`
- Status: `pass`
- Fields compared: `volume_24h, volume_7d, tx_count, unique_senders, unique_receivers, velocity_unique_capital, concentration_score, bridge_name, bridge_share, bridge_volume, bridge_transactions, whale_threshold_usd, whale_activity_score, net_flow_7d, top_whale_flows, source, data_layer, serving_path`
- Notes: `Materialized corridor summary compared against fresh recomputation from cache poll inputs.`
- Mismatches: none

## context_graph_live_parity

Status: **PASS**

- Context-graph parity uses live BigQuery edge extraction in non-request-path audit mode.
- The legacy comparator reconstructs grouped-edge semantics from the same raw fact rows.

### Scope: `{"chain": "Ethereum", "requested_time_range": "1h", "resolved_time_range": "1h", "token": "USDC"}`
- Status: `pass`
- Fields compared: `topology, topology_classification, flow_density, protocol_noise_ratio, bridge_usage_rate, counterparty_entropy, liquidity_gap, total_transactions, edges, evidence_stack`
- Intentional deltas: `Registry matching and entity labeling now occur in Python rather than SQL.`
- Notes: `Current snapshot built from live raw edge facts queried from BigQuery.; Legacy-equivalent snapshot reconstructed by grouping those facts before graph assembly.; Materialized snapshot exists for this sampled scope.`
- Mismatches: none

### Scope: `{"chain": "Ethereum", "requested_time_range": "24h", "resolved_time_range": "24h", "token": "USDC"}`
- Status: `pass`
- Fields compared: `topology, topology_classification, flow_density, protocol_noise_ratio, bridge_usage_rate, counterparty_entropy, liquidity_gap, total_transactions, edges, evidence_stack`
- Intentional deltas: `Registry matching and entity labeling now occur in Python rather than SQL.`
- Notes: `Current snapshot built from live raw edge facts queried from BigQuery.; Legacy-equivalent snapshot reconstructed by grouping those facts before graph assembly.`
- Mismatches: none

### Scope: `{"chain": "Polygon", "requested_time_range": "1h", "resolved_time_range": "1h", "token": "USDC"}`
- Status: `pass`
- Fields compared: `topology, topology_classification, flow_density, protocol_noise_ratio, bridge_usage_rate, counterparty_entropy, liquidity_gap, total_transactions, edges, evidence_stack`
- Intentional deltas: `Registry matching and entity labeling now occur in Python rather than SQL.`
- Notes: `Current snapshot built from live raw edge facts queried from BigQuery.; Legacy-equivalent snapshot reconstructed by grouping those facts before graph assembly.`
- Mismatches: none

### Scope: `{"chain": "Polygon", "requested_time_range": "24h", "resolved_time_range": "1h", "token": "USDC"}`
- Status: `pass`
- Fields compared: `topology, topology_classification, flow_density, protocol_noise_ratio, bridge_usage_rate, counterparty_entropy, liquidity_gap, total_transactions, edges, evidence_stack`
- Intentional deltas: `Registry matching and entity labeling now occur in Python rather than SQL.`
- Notes: `Current snapshot built from live raw edge facts queried from BigQuery.; Legacy-equivalent snapshot reconstructed by grouping those facts before graph assembly.; Budget-safe fallback resolved requested 24h to 1h for Polygon.`
- Mismatches: none
