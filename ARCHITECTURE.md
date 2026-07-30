
# MTF_V4 Mass-Market Engine

MTF_V4 replaces the monolithic REST polling loop with an event-driven pipeline.
The production service contract remains `quant_bot`, while the internal engine
is split into independently testable modules.

## Pipeline

1. `UniverseManager` refreshes all tradeable USD-M perpetual contracts.
2. `MarketStream` consumes all-market ticker and best-book WebSocket events.
3. `SymbolStore` keeps one bounded, latest-state record per symbol.
4. `FeatureEngine` updates cheap features for every symbol in constant time.
5. `Ranker` publishes a coalesced hot set; stale work never forms a FIFO queue.
6. `DeepAnalyzer` computes structure, momentum, volume and liquidity scores.
7. `RiskEngine` sizes positions from risk, stop distance, liquidity and confidence.
8. `PositionEngine` owns the complete simulated position lifecycle.
9. `PersistenceWriter` serializes SQLite writes without blocking market ingestion.
10. `Panel` reads immutable snapshots and never locks the market-data hot path.

## Removal rule

The legacy blocking REST scanner is removed in the same commit that switches
`main.rs` to MTF_V4. MTF_V4 must pass format, check, Clippy, unit tests, replay
tests and a WebSocket soak test before that commit can merge.

## Data policy

All symbols are observed continuously. Expensive depth and structural analysis
is applied only to the current hot set. The hot set is coalesced: a symbol has
at most one pending analysis record and newer state replaces older state.

On-chain data is exposed through an `OnChainProvider` trait. With no configured
provider its contribution is neutral; missing data is never treated as bullish
or bearish and never blocks the market-data pipeline.
