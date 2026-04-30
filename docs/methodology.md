# Methodology

Canopy Route Intelligence uses conservative benchmark language. The dashboard should be read as route intelligence from observed and configured data, not as proof of production payment outcomes.

## Observed Route Cost

Observed route cost means cost visible or computable from available chain data and configured route metadata. It is not the same as true facilitator margin.

Examples include network-level cost estimates, configured baseline assumptions, and costs derived from available transfer evidence. The system does not claim to know private commercial spread, off-chain fees, or facilitator margin.

## Route Share

Route share is a benchmark distribution across supported routes in the available dataset or configured sample context. It should be interpreted as observed or sample route behavior, not as a complete market-share claim.

## Observed On-Chain Settlement Health

Observed on-chain settlement health means whether available on-chain settlement evidence appears complete, timely, and internally consistent. It does not prove off-chain API delivery, merchant fulfillment, or full x402 resource delivery.

## Freshness

Freshness measures how recently the route intelligence cache or source-derived records were updated. Freshness gates help keep stale data from being presented as current.

## Directly Measured

- supported chain/token transfer evidence where source data is available
- timestamps or block/slot positions from source records
- configured route metadata
- cache age and refresh state
- validation and reconciliation outputs

## Inferred

- route attractiveness from observed cost, freshness, and configured risk factors
- route share summaries in demo/sample contexts
- settlement-health status from available on-chain evidence

## Not Claimed

- custody of funds
- payment execution
- true facilitator margin
- full production autonomous routing
- complete off-chain x402 payment visibility
- proof of merchant fulfillment or resource delivery
- production SLA
