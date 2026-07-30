
# MTF_V4 Mass-Market Engine

MTF_V4 replaces the monolithic REST polling loop with an event-driven pipeline.
The production service contract remains `quant_bot`, while the internal engine
is split into independently testable modules.

## Frozen V4 scope

V4 deliberately uses a small set of independent, measurable inputs:

- return z-score over short and medium horizons;
- volume impulse z-score;
- spread and executable top-book liquidity;
- realized volatility;
- top-book imbalance.

ADX/EMA may be derived for display and regime grouping, but they do not receive
extra votes when their information is already represented by normalized returns.
Fibonacci, subjective order blocks and on-chain feeds are outside the first
production scope. They can only be added after an out-of-sample ablation test
shows that they improve net expectancy after fees and slippage.

## Pipeline

1. `UniverseManager` refreshes all tradeable USD-M perpetual contracts.
2. `MarketStream` consumes all-market ticker and best-book WebSocket events.
3. `SymbolStore` keeps one bounded, latest-state record per symbol.
4. `FeatureEngine` updates cheap features for every symbol in constant time.
5. `Ranker` publishes a coalesced hot set; stale work never forms a FIFO queue.
6. `ProbabilityModel` estimates a calibrated TP-before-SL probability.
7. `RiskEngine` requires positive net expectancy and sizes from stop distance,
   liquidity and confidence.
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

## Probability and learning

Every hot candidate, including rejected candidates, is persisted with its
feature snapshot. Outcomes are labeled at fixed horizons with maximum favorable
and adverse excursion.

The first model is a Beta-Binomial estimator grouped by regime and score band:

`p = (wins + alpha) / (samples + alpha + beta)`

An entry is allowed only when:

`EV = p * average_win - (1 - p) * average_loss - costs > 0`

A single losing trade is not considered a learned mistake. Parameter promotion
requires a minimum sample, walk-forward validation and a challenger that beats
the active model after costs. Failed challengers are discarded automatically.
