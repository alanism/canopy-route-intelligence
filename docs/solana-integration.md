# Solana Integration

Solana requires first-class chain-specific handling because stablecoin movement is represented through Solana transaction and account structure, not Ethereum-style ERC-20 logs.

Solana stablecoin transfer data is not treated as Ethereum-style logs. Project DG normalizes Solana-native transaction structure -- signatures, slots, token mints, accounts, instructions, and inner instructions -- into a comparable stablecoin route intelligence model.

## Ethereum vs Solana

Ethereum ERC-20 analysis commonly starts from `Transfer(address,address,uint256)` logs emitted by token contracts. Those logs have a familiar contract address, topics, data payload, block number, transaction hash, and log index.

Solana analysis starts from transaction records. A normalized transfer may require the transaction signature, slot, block time, token mint, source token account, destination token account, instruction index, and inner instruction index when available.

## Solana Primitives

- `signature`: transaction identifier used for traceability
- `slot`: ledger position used for ordering
- `block_time`: timestamp when available
- `token_mint`: stablecoin mint address, such as a USDC mint
- `source_account`: source token account
- `destination_account`: destination token account
- `instruction_index`: outer instruction position
- `inner_instruction_index`: inner instruction position when available

## Normalized Model

Project DG maps Solana transfer evidence into the same route intelligence concepts used by the rest of the app:

- chain
- token
- route or corridor key
- transfer amount
- observed fee/cost fields when computable
- timestamp
- source/destination evidence
- freshness state
- validation state
- trace identifiers

## Currently Implemented

The current public hackathon snapshot includes the working EVM/BigQuery route intelligence path and the Solana normalization contract in documentation. The dashboard and API are structured so Solana-normalized records can be compared with Ethereum records once a Solana ingestion adapter is connected.

## Demo Or Limited

Solana-specific dashboard values in this repository should be treated as demo or design-level unless backed by a connected Solana indexer in the local environment. The repo does not claim complete Solana production coverage.

## Cut From Hackathon Version

- complete Solana indexer implementation
- production-grade Solana replay/backfill tooling
- full SPL token account owner attribution
- off-chain x402 delivery verification
- autonomous payment execution
