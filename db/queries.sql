-- Canopy v4 corridor analytics query sketches.
-- These are reference templates for the deterministic analytics layer.

-- corridor_volume_daily
SELECT
  DATE(block_timestamp) AS date,
  SUM(value_usd) AS volume_usd,
  COUNT(*) AS tx_count,
  COUNT(DISTINCT from_address) AS unique_senders,
  COUNT(DISTINCT to_address) AS unique_receivers
FROM `token_transfers`
WHERE token_symbol = @token
GROUP BY date
ORDER BY date DESC;

-- whale_flows_7d
WITH holder_flows AS (
  SELECT
    owner_address,
    SUM(CASE WHEN direction = 'in' THEN value_usd ELSE 0 END) AS total_in,
    SUM(CASE WHEN direction = 'out' THEN value_usd ELSE 0 END) AS total_out
  FROM `token_transfers`
  WHERE block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    AND value_usd >= 5000
  GROUP BY owner_address
)
SELECT
  owner_address,
  total_in - total_out AS net_flow_7d
FROM holder_flows
ORDER BY net_flow_7d DESC
LIMIT 25;

-- bridge_usage_placeholder
SELECT
  bridge_name,
  COUNT(*) AS bridge_transactions,
  SUM(value_usd) AS bridge_volume
FROM `bridge_message_logs`
WHERE token_symbol = @token
GROUP BY bridge_name
ORDER BY bridge_volume DESC;
