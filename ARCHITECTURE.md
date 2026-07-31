# MTF_V4 Mass-Market Engine

MTF_V4 replaces the monolithic REST polling loop with an event-driven simulation
pipeline. The binary remains `quant_bot`; the internal engine is split into
independently testable modules.

## Implemented signal scope

The first V4 version intentionally uses a small, measurable feature set:

- 15-second, 60-second and 300-second percentage returns;
- quote-volume impulse;
- executable best bid/ask spread and top-book liquidity;
- realized short-window volatility;
- top-book imbalance.

The features are normalized into a cheap ranking score. A symbol must also pass
freshness, warm-up, volume, spread, direction and vote-consistency gates. LONG
and SHORT are decided per symbol; BTC is not a global direction lock.

ADX, EMA, Fibonacci, subjective order blocks, SMC labels and on-chain feeds are
not part of the first V4 decision path. They should only be promoted after an
out-of-sample ablation test improves net expectancy after costs.

## Runtime pipeline

1. `UniverseManager` refreshes tradeable USD-M perpetual contracts without
   exposing an empty universe during refresh.
2. `MarketStream` consumes all-market mini-ticker, best-book and mark-price
   WebSocket events with reconnect/backoff.
3. `SymbolStore` keeps one bounded latest-state record per symbol.
4. `FeatureEngine` updates cheap features for every tracked symbol once per
   second.
5. `Ranker` publishes a coalesced hot set; stale work never forms a FIFO queue.
6. `RiskEngine` requires valid liquidity, stop distance, free margin and
   portfolio risk, then caps leverage at 3x and per-trade margin at 10%.
7. `PositionEngine` owns cost-aware 40/40/20 exits, monotonic stops and the
   open-ended runner.
8. `PortfolioEngine` is the single owner of mutable account state.
9. `SqliteStore` commits snapshots, executions and signal decisions atomically.
10. `Panel` reads immutable dashboard snapshots and sends emergency commands to
    the portfolio owner.

Market ingestion does not take the SQLite mutex. SQLite writes are short,
batched transactions performed by the portfolio actor; WAL and `FULL`
synchronous mode protect restart recovery.

## Cost and accounting model

Entry and exit fills include configured slippage. Net PnL includes allocated
entry fees, exit fees and funding. Break-even and stop locks therefore protect
cost-adjusted rather than raw price PnL. Margin is reserved from equity without
being treated as a realized expense.

## Learning data

The database records:

- gate-rejection transitions with their feature snapshot;
- portfolio-level accept/reject decisions;
- accepted decisions linked directly to `position_id`;
- every open, stop movement, partial exit, funding accrual and final close.

This is training-ready raw evidence, not an online self-modifying model.
`stats.rs` contains conservative expectancy helpers, but V4 does not
automatically change production parameters. A future challenger must use
minimum samples, walk-forward/out-of-sample validation and cost-adjusted
expectancy before promotion.

## Deployment rule

The V3 service is not upgraded while it owns open V3 positions. V4 first runs
beside V3 with a separate database and port. After restart, emergency-exit and
soak validation, V3 entries are stopped, its positions drain, and only then is
the service switched to V4 with a fresh 10,000 USDT simulation balance.
