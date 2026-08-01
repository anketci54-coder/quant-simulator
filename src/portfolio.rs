use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, Result};
use serde_json::json;

use crate::{
    cost::CostModel,
    engine::GateRejection,
    market::SymbolMeta,
    model::{Candidate, Side, STRATEGY_VERSION},
    position::{ExitReason, Position, PositionEvent, PositionPolicy, PositionQuote, PositionStage},
    risk::{cost_adjusted_risk_to_stop, size_candidate, PositionSize, RiskLimits},
    storage::{LedgerEvent, PortfolioSnapshot, SignalDecision, SqliteStore, TrackedPosition},
};

#[derive(Clone, Copy, Debug)]
pub struct Quote {
    pub bid: f64,
    pub ask: f64,
    pub atr: f64,
    pub structure_stop: Option<f64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmergencyExitSummary {
    pub closed_positions: usize,
    pub fallback_quotes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EntryReject {
    EntriesPaused,
    Capacity,
    SameSideCapacity,
    DuplicateSymbol,
    InvalidQuote,
    InvalidSize,
    InsufficientFreeMargin,
    PortfolioRisk,
    TotalMargin,
    NotionalTooSmall,
    ExpectedProfitTooSmall,
}

impl EntryReject {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::EntriesPaused => "ENTRIES_PAUSED",
            Self::Capacity => "CAPACITY",
            Self::SameSideCapacity => "SAME_SIDE_CAPACITY",
            Self::DuplicateSymbol => "DUPLICATE_SYMBOL",
            Self::InvalidQuote => "INVALID_QUOTE",
            Self::InvalidSize => "INVALID_SIZE",
            Self::InsufficientFreeMargin => "INSUFFICIENT_FREE_MARGIN",
            Self::PortfolioRisk => "PORTFOLIO_RISK",
            Self::TotalMargin => "TOTAL_MARGIN",
            Self::NotionalTooSmall => "NOTIONAL_TOO_SMALL",
            Self::ExpectedProfitTooSmall => "EXPECTED_PROFIT_TOO_SMALL",
        }
    }
}

#[derive(Clone, Debug)]
pub struct PortfolioLimits {
    pub max_positions: usize,
    pub max_same_side_positions: usize,
    pub leverage: f64,
    pub risk_per_trade: f64,
    pub max_portfolio_risk: f64,
    pub max_trade_allocation: f64,
    pub max_total_margin_fraction: f64,
    pub min_trade_notional: f64,
    pub min_expected_net_profit: f64,
}

pub struct PortfolioEngine {
    snapshot: PortfolioSnapshot,
    store: Arc<SqliteStore>,
    costs: CostModel,
    policy: PositionPolicy,
    limits: PortfolioLimits,
    gate_rejection_cache: HashMap<(String, String), i64>,
}

impl PortfolioEngine {
    pub fn new(
        snapshot: PortfolioSnapshot,
        store: Arc<SqliteStore>,
        costs: CostModel,
        policy: PositionPolicy,
        limits: PortfolioLimits,
    ) -> Result<Self> {
        if !costs.validate()
            || !policy.validate()
            || limits.max_positions == 0
            || limits.max_positions > 10
            || limits.max_same_side_positions == 0
            || limits.max_same_side_positions > limits.max_positions
            || !(1.0..=3.0).contains(&limits.leverage)
            || !(0.0..=0.02).contains(&limits.risk_per_trade)
            || !(0.0..=0.10).contains(&limits.max_portfolio_risk)
            || !(0.01..=0.10).contains(&limits.max_trade_allocation)
            || !(0.05..=1.0).contains(&limits.max_total_margin_fraction)
            || limits.min_trade_notional <= 0.0
            || limits.min_expected_net_profit < 0.0
        {
            anyhow::bail!("PortfolioEngine yapılandırması geçersiz");
        }
        Ok(Self {
            snapshot,
            store,
            costs,
            policy,
            limits,
            gate_rejection_cache: HashMap::new(),
        })
    }

    pub fn snapshot(&self) -> &PortfolioSnapshot {
        &self.snapshot
    }

    pub fn costs(&self) -> CostModel {
        self.costs
    }

    pub fn used_margin(&self) -> f64 {
        self.snapshot
            .positions
            .iter()
            .map(|tracked| {
                tracked.initial_margin * tracked.position.remaining_quantity
                    / tracked.position.original_quantity
            })
            .sum()
    }

    pub fn account_equity(&self) -> f64 {
        self.snapshot.balance
            + self
                .snapshot
                .positions
                .iter()
                .map(|tracked| tracked.position.unrealized_net_pnl(self.costs))
                .sum::<f64>()
    }

    pub fn free_margin(&self) -> f64 {
        (self.account_equity() - self.used_margin()).max(0.0)
    }

    pub fn portfolio_downside_risk(&self) -> Option<f64> {
        let mut total = 0.0;
        for tracked in &self.snapshot.positions {
            let position = &tracked.position;
            total += cost_adjusted_risk_to_stop(
                position.side,
                position.entry_fill,
                position.stop,
                position.remaining_quantity,
                position.entry_fee_remaining,
                position.funding_cost,
                self.costs,
            )?;
        }
        Some(total)
    }

    pub fn try_open(
        &mut self,
        candidate: &Candidate,
        meta: &SymbolMeta,
        quote: Quote,
        market_regime: &str,
        now: i64,
    ) -> Result<std::result::Result<u64, EntryReject>> {
        let before = self.snapshot.clone();
        let outcome = self.evaluate_open(candidate, meta, quote);
        let mut decision = SignalDecision {
            decision_key: format!(
                "{}:{}:{}",
                candidate.symbol,
                candidate.observed_at,
                candidate.side.direction()
            ),
            position_id: None,
            symbol: candidate.symbol.clone(),
            side: Some(candidate.side),
            score: candidate.score,
            confidence: candidate.confidence,
            accepted: outcome.is_ok(),
            reject_reason: outcome
                .as_ref()
                .err()
                .map(|reason| reason.as_str().to_string()),
            features: candidate.features,
            observed_at: candidate.observed_at,
        };

        let result = match outcome {
            Ok((size, entry_fill, expected_net_profit)) => {
                let risk_distance = candidate.stop_distance;
                let initial_stop = entry_fill - candidate.side.direction() * risk_distance;
                let tp1 = entry_fill + candidate.side.direction() * risk_distance;
                let tp2 = entry_fill + candidate.side.direction() * risk_distance * 2.0;
                let Some(position) = Position::new(
                    candidate.side,
                    entry_fill,
                    size.quantity,
                    initial_stop,
                    tp1,
                    tp2,
                    self.costs,
                ) else {
                    return Ok(Err(EntryReject::InvalidSize));
                };
                let id = self.snapshot.next_position_id;
                self.snapshot.next_position_id = self.snapshot.next_position_id.saturating_add(1);
                decision.position_id = Some(id);
                let initial_margin = size.quantity * entry_fill / self.limits.leverage;
                self.snapshot.positions.push(TrackedPosition {
                    id,
                    symbol: candidate.symbol.clone(),
                    strategy_version: STRATEGY_VERSION.to_string(),
                    market_regime: market_regime.to_string(),
                    opened_at: now,
                    initial_margin,
                    position,
                });
                let entry_fee = self.costs.entry_fee(entry_fill, size.quantity);
                let ledger = LedgerEvent {
                    event_key: format!("{id}:{now}:OPEN"),
                    position_id: id,
                    symbol: candidate.symbol.clone(),
                    side: candidate.side,
                    stage: PositionStage::BeforeTp1,
                    event_type: "OPEN".to_string(),
                    quantity: size.quantity,
                    price: entry_fill,
                    net_pnl: 0.0,
                    fee: entry_fee,
                    funding: 0.0,
                    exit_reason: None,
                    payload_json: json!({
                        "risk": size.risk,
                        "margin": initial_margin,
                        "stop": initial_stop,
                        "tp1": tp1,
                        "tp2": tp2,
                        "expected_net_profit": expected_net_profit,
                        "policy": "40/40/20_RUNNER"
                    })
                    .to_string(),
                    created_at: now,
                };
                if let Err(error) =
                    self.store
                        .persist_atomic(&self.snapshot, &[ledger], &[decision], now)
                {
                    self.snapshot = before;
                    return Err(error).context("OPEN atomik olarak kaydedilemedi");
                }
                Ok(id)
            }
            Err(reason) => {
                self.store
                    .persist_atomic(&self.snapshot, &[], &[decision], now)
                    .context("Reddedilen sinyal kaydedilemedi")?;
                Err(reason)
            }
        };
        Ok(result)
    }

    pub fn record_gate_rejections(&mut self, rejections: &[GateRejection], now: i64) -> Result<()> {
        if rejections.is_empty() {
            return Ok(());
        }
        let decisions: Vec<SignalDecision> = rejections
            .iter()
            .filter(|rejection| {
                let reason = rejection.reason.as_str();
                let cache_key = (rejection.symbol.clone(), reason.to_string());
                let should_record = self
                    .gate_rejection_cache
                    .get(&cache_key)
                    .is_none_or(|previous_at| now.saturating_sub(*previous_at) >= 3_600_000);
                if should_record {
                    self.gate_rejection_cache.insert(cache_key, now);
                }
                should_record
            })
            .map(|rejection| SignalDecision {
                decision_key: format!(
                    "GATE:{}:{}:{}",
                    rejection.symbol,
                    rejection.observed_at,
                    rejection.reason.as_str()
                ),
                position_id: None,
                symbol: rejection.symbol.clone(),
                side: None,
                score: rejection.features.cheap_score,
                confidence: (rejection.features.cheap_score / 100.0).clamp(0.0, 1.0),
                accepted: false,
                reject_reason: Some(rejection.reason.as_str().to_string()),
                features: rejection.features,
                observed_at: rejection.observed_at,
            })
            .collect();
        if decisions.is_empty() {
            return Ok(());
        }
        self.store
            .persist_atomic(&self.snapshot, &[], &decisions, now)
            .context("Sinyal kapısı retleri atomik kaydedilemedi")
    }

    fn evaluate_open(
        &self,
        candidate: &Candidate,
        meta: &SymbolMeta,
        quote: Quote,
    ) -> std::result::Result<(PositionSize, f64, f64), EntryReject> {
        if self.snapshot.entries_paused {
            return Err(EntryReject::EntriesPaused);
        }
        if self.snapshot.positions.len() >= self.limits.max_positions {
            return Err(EntryReject::Capacity);
        }
        if self
            .snapshot
            .positions
            .iter()
            .filter(|position| position.position.side == candidate.side)
            .count()
            >= self.limits.max_same_side_positions
        {
            return Err(EntryReject::SameSideCapacity);
        }
        if self
            .snapshot
            .positions
            .iter()
            .any(|position| position.symbol == candidate.symbol)
        {
            return Err(EntryReject::DuplicateSymbol);
        }
        let entry_fill = self
            .costs
            .estimated_entry_fill(candidate.side, quote.bid, quote.ask)
            .ok_or(EntryReject::InvalidQuote)?;
        let expected_cost_rate = self.costs.entry_fee_rate
            + self.costs.exit_fee_rate
            + (self.costs.expected_entry_slippage_bps
                + self.costs.expected_exit_slippage_bps
                + self.costs.safety_buffer_bps)
                / 10_000.0;
        let size = size_candidate(
            self.account_equity(),
            candidate,
            RiskLimits {
                risk_fraction: self.limits.risk_per_trade,
                allocation_fraction: self.limits.max_trade_allocation,
                leverage: self.limits.leverage,
                step_size: meta.step_size,
                min_quantity: meta.min_quantity,
                expected_round_trip_cost_rate: expected_cost_rate,
            },
        )
        .ok_or(EntryReject::InvalidSize)?;
        let actual_margin = size.quantity * entry_fill / self.limits.leverage;
        let notional = size.quantity * entry_fill;
        if notional < meta.min_notional.max(self.limits.min_trade_notional) {
            return Err(EntryReject::NotionalTooSmall);
        }
        // Planned lifecycle realizes 40% at 1R, 40% at 2R and leaves 20%
        // for a runner. Value the runner conservatively at 3R, then discount
        // the gross reward by signal confidence and subtract full round-trip
        // fees/slippage. This matches the actual 40/40/20 policy instead of
        // incorrectly judging the whole trade only by its TP1 slice.
        let lifecycle_reward_r = 0.40 * 1.0 + 0.40 * 2.0 + 0.20 * 3.0;
        let expected_net_profit =
            size.quantity * candidate.stop_distance * lifecycle_reward_r * candidate.confidence
                - notional * expected_cost_rate;
        if expected_net_profit < self.limits.min_expected_net_profit {
            return Err(EntryReject::ExpectedProfitTooSmall);
        }
        if actual_margin > self.free_margin() {
            return Err(EntryReject::InsufficientFreeMargin);
        }
        if self.used_margin() + actual_margin
            > self.account_equity() * self.limits.max_total_margin_fraction
        {
            return Err(EntryReject::TotalMargin);
        }
        let risk_limit = self.account_equity() * self.limits.max_portfolio_risk;
        if self
            .portfolio_downside_risk()
            .ok_or(EntryReject::PortfolioRisk)?
            + size.risk
            > risk_limit
        {
            return Err(EntryReject::PortfolioRisk);
        }
        Ok((size, entry_fill, expected_net_profit))
    }

    pub fn process_quote(
        &mut self,
        symbol: &str,
        quote: Quote,
        step_size: f64,
        now: i64,
    ) -> Result<Vec<PositionEvent>> {
        let Some(index) = self
            .snapshot
            .positions
            .iter()
            .position(|tracked| tracked.symbol == symbol)
        else {
            return Ok(Vec::new());
        };
        let before = self.snapshot.clone();
        let (position_id, side, events) = {
            let tracked = &mut self.snapshot.positions[index];
            let events = tracked.position.on_quote(
                PositionQuote {
                    bid: quote.bid,
                    ask: quote.ask,
                    atr: quote.atr,
                    structure_stop: quote.structure_stop,
                    step_size,
                },
                self.costs,
                self.policy,
            );
            (tracked.id, tracked.position.side, events)
        };
        if events.is_empty() {
            return Ok(events);
        }
        let ledger = self.apply_events(position_id, symbol, side, &events, now);
        self.snapshot
            .positions
            .retain(|tracked| tracked.position.stage != PositionStage::Closed);
        if let Err(error) = self.store.persist_atomic(&self.snapshot, &ledger, &[], now) {
            self.snapshot = before;
            return Err(error).context("Pozisyon güncellemesi atomik kaydedilemedi");
        }
        Ok(events)
    }

    pub fn apply_funding(
        &mut self,
        symbol: &str,
        mark_price: f64,
        funding_rate: f64,
        now: i64,
    ) -> Result<Option<f64>> {
        let Some(index) = self
            .snapshot
            .positions
            .iter()
            .position(|tracked| tracked.symbol == symbol)
        else {
            return Ok(None);
        };
        if mark_price <= 0.0 || !funding_rate.is_finite() {
            return Ok(None);
        }
        let before = self.snapshot.clone();
        let tracked = &mut self.snapshot.positions[index];
        let funding_cost = tracked.position.remaining_quantity
            * mark_price
            * funding_rate
            * tracked.position.side.direction();
        tracked.position.add_funding_cost(funding_cost);
        let event = LedgerEvent {
            event_key: format!("{}:{now}:FUNDING", tracked.id),
            position_id: tracked.id,
            symbol: symbol.to_string(),
            side: tracked.position.side,
            stage: tracked.position.stage,
            event_type: "FUNDING".to_string(),
            quantity: tracked.position.remaining_quantity,
            price: mark_price,
            // Funding is accrued into the position and recognized exactly
            // once in its eventual exit PnL.
            net_pnl: 0.0,
            fee: 0.0,
            funding: funding_cost,
            exit_reason: None,
            payload_json: json!({"funding_rate": funding_rate}).to_string(),
            created_at: now,
        };
        if let Err(error) = self
            .store
            .persist_atomic(&self.snapshot, &[event], &[], now)
        {
            self.snapshot = before;
            return Err(error).context("Funding atomik kaydedilemedi");
        }
        Ok(Some(funding_cost))
    }

    /// Closes every position at the latest executable quote and disables new
    /// entries in the same SQLite transaction.
    pub fn emergency_close_all(
        &mut self,
        quotes: &HashMap<String, Quote>,
        now: i64,
    ) -> Result<EmergencyExitSummary> {
        let before = self.snapshot.clone();
        self.snapshot.entries_paused = true;
        self.snapshot.pause_reason = Some("EMERGENCY_EXIT".to_string());
        let mut ledger = Vec::new();
        let mut closed = 0usize;
        let mut fallback_quotes = 0usize;

        for tracked in &mut self.snapshot.positions {
            let quote = match quotes.get(&tracked.symbol).copied() {
                Some(quote) => quote,
                None => {
                    fallback_quotes += 1;
                    let last = tracked.position.last_exit_fill.max(f64::EPSILON);
                    Quote {
                        bid: last,
                        ask: last,
                        atr: f64::EPSILON,
                        structure_stop: None,
                    }
                }
            };
            let events = tracked.position.force_close(
                quote.bid,
                quote.ask,
                self.costs,
                ExitReason::EmergencyExit,
            );
            for (sequence, event) in events.iter().enumerate() {
                if let PositionEvent::Closed { net_pnl, .. } = event {
                    self.snapshot.balance += net_pnl;
                    self.snapshot.realized_net_pnl += net_pnl;
                    closed += 1;
                }
                ledger.push(ledger_from_event(
                    tracked.id,
                    &tracked.symbol,
                    tracked.position.side,
                    event,
                    now,
                    sequence,
                ));
            }
        }
        self.snapshot
            .positions
            .retain(|tracked| tracked.position.stage != PositionStage::Closed);
        if let Err(error) = self.store.persist_atomic(&self.snapshot, &ledger, &[], now) {
            self.snapshot = before;
            return Err(error).context("Acil çıkış atomik kaydedilemedi");
        }
        Ok(EmergencyExitSummary {
            closed_positions: closed,
            fallback_quotes,
        })
    }

    fn apply_events(
        &mut self,
        position_id: u64,
        symbol: &str,
        side: Side,
        events: &[PositionEvent],
        now: i64,
    ) -> Vec<LedgerEvent> {
        let mut ledger = Vec::with_capacity(events.len());
        for (sequence, event) in events.iter().enumerate() {
            match event {
                PositionEvent::PartialExit { net_pnl, .. }
                | PositionEvent::Closed { net_pnl, .. } => {
                    self.snapshot.balance += net_pnl;
                    self.snapshot.realized_net_pnl += net_pnl;
                }
                PositionEvent::StopMoved { .. } => {}
            }
            ledger.push(ledger_from_event(
                position_id,
                symbol,
                side,
                event,
                now,
                sequence,
            ));
        }
        ledger
    }
}

fn ledger_from_event(
    position_id: u64,
    symbol: &str,
    side: Side,
    event: &PositionEvent,
    now: i64,
    sequence: usize,
) -> LedgerEvent {
    match event {
        PositionEvent::StopMoved { old, new, stage } => LedgerEvent {
            event_key: format!("{position_id}:{now}:{sequence}:STOP"),
            position_id,
            symbol: symbol.to_string(),
            side,
            stage: *stage,
            event_type: "STOP_MOVED".to_string(),
            quantity: 0.0,
            price: *new,
            net_pnl: 0.0,
            fee: 0.0,
            funding: 0.0,
            exit_reason: None,
            payload_json: json!({"old_stop": old, "new_stop": new}).to_string(),
            created_at: now,
        },
        PositionEvent::PartialExit {
            quantity,
            exit_fill,
            entry_fee_allocated,
            exit_fee,
            funding,
            net_pnl,
            stage,
            fraction_of_original,
        } => LedgerEvent {
            event_key: format!("{position_id}:{now}:{sequence}:PARTIAL"),
            position_id,
            symbol: symbol.to_string(),
            side,
            stage: *stage,
            event_type: "PARTIAL_EXIT".to_string(),
            quantity: *quantity,
            price: *exit_fill,
            net_pnl: *net_pnl,
            fee: *exit_fee,
            funding: *funding,
            exit_reason: None,
            payload_json: json!({
                "fraction_of_original": fraction_of_original,
                "entry_fee_allocated": entry_fee_allocated
            })
            .to_string(),
            created_at: now,
        },
        PositionEvent::Closed {
            quantity,
            exit_fill,
            entry_fee_allocated,
            exit_fee,
            funding,
            net_pnl,
            reason,
        } => LedgerEvent {
            event_key: format!("{position_id}:{now}:{sequence}:CLOSE"),
            position_id,
            symbol: symbol.to_string(),
            side,
            stage: PositionStage::Closed,
            event_type: "CLOSE".to_string(),
            quantity: *quantity,
            price: *exit_fill,
            net_pnl: *net_pnl,
            fee: *exit_fee,
            funding: *funding,
            exit_reason: Some(*reason),
            payload_json: json!({"entry_fee_allocated": entry_fee_allocated}).to_string(),
            created_at: now,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;
    use crate::{
        model::FeatureSnapshot,
        storage::{PortfolioSnapshot, SqliteStore},
    };

    fn temp_database() -> (std::path::PathBuf, Arc<SqliteStore>) {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("quant-v4-portfolio-{unique}.db"));
        let store = Arc::new(SqliteStore::open(&path).unwrap());
        (path, store)
    }

    fn engine(store: Arc<SqliteStore>) -> PortfolioEngine {
        PortfolioEngine::new(
            PortfolioSnapshot::fresh(10_000.0),
            store,
            CostModel::default(),
            PositionPolicy::default(),
            PortfolioLimits {
                max_positions: 10,
                max_same_side_positions: 3,
                leverage: 3.0,
                risk_per_trade: 0.005,
                max_portfolio_risk: 0.02,
                max_trade_allocation: 0.10,
                max_total_margin_fraction: 0.40,
                min_trade_notional: 100.0,
                min_expected_net_profit: 1.0,
            },
        )
        .unwrap()
    }

    fn candidate(symbol: &str, side: Side, observed_at: i64) -> Candidate {
        Candidate {
            symbol: symbol.to_string(),
            side,
            price: 100.0,
            score: 80.0,
            confidence: 0.8,
            stop_distance: 1.0,
            liquidity_notional: 1_000_000.0,
            observed_at,
            features: FeatureSnapshot::default(),
        }
    }

    fn meta() -> SymbolMeta {
        SymbolMeta {
            tick_size: 0.01,
            step_size: 0.1,
            min_quantity: 0.1,
            min_notional: 5.0,
        }
    }

    fn cleanup(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }

    #[test]
    fn emergency_exit_closes_every_position_and_persists_pause_across_restart() {
        let (path, store) = temp_database();
        let mut portfolio = engine(store.clone());
        let entry_quote = Quote {
            bid: 99.99,
            ask: 100.01,
            atr: 0.5,
            structure_stop: None,
        };
        portfolio
            .try_open(
                &candidate("LONGUSDT", Side::Long, 1),
                &meta(),
                entry_quote,
                "SYMBOL_BULL",
                1,
            )
            .unwrap()
            .unwrap();
        portfolio
            .try_open(
                &candidate("SHORTUSDT", Side::Short, 2),
                &meta(),
                entry_quote,
                "SYMBOL_BEAR",
                2,
            )
            .unwrap()
            .unwrap();

        let quotes = HashMap::from([(
            "LONGUSDT".to_string(),
            Quote {
                bid: 101.0,
                ask: 101.01,
                atr: 0.5,
                structure_stop: None,
            },
        )]);
        let summary = portfolio.emergency_close_all(&quotes, 3).unwrap();
        assert_eq!(
            summary,
            EmergencyExitSummary {
                closed_positions: 2,
                fallback_quotes: 1,
            }
        );
        assert!(portfolio.snapshot().positions.is_empty());
        assert!(portfolio.snapshot().entries_paused);

        drop(portfolio);
        let restored = store.load_or_create(1.0, 4).unwrap();
        assert!(restored.positions.is_empty());
        assert!(restored.entries_paused);
        assert_eq!(restored.pause_reason.as_deref(), Some("EMERGENCY_EXIT"));
        drop(store);
        cleanup(&path);
    }
}
