use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::{sync::watch, time};

use crate::{
    config::Config,
    market::SymbolStore,
    model::{Candidate, FeatureSnapshot, Side, SymbolState},
};

#[derive(Clone, Debug, Default)]
pub struct EngineFrame {
    pub candidates: Vec<Candidate>,
    pub gate_rejections: Vec<GateRejection>,
}

#[derive(Clone, Debug)]
pub struct GateRejection {
    pub symbol: String,
    pub reason: CandidateReject,
    pub features: FeatureSnapshot,
    pub observed_at: i64,
}

pub type HotSet = Arc<watch::Sender<EngineFrame>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CandidateReject {
    Stale,
    Warmup,
    LowVolume,
    InvalidSpread,
    WeakScore,
    NeutralDirection,
    ConflictingSignals,
}

impl CandidateReject {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stale => "STALE",
            Self::Warmup => "WARMUP",
            Self::LowVolume => "LOW_VOLUME",
            Self::InvalidSpread => "INVALID_SPREAD",
            Self::WeakScore => "WEAK_SCORE",
            Self::NeutralDirection => "NEUTRAL_DIRECTION",
            Self::ConflictingSignals => "CONFLICTING_SIGNALS",
        }
    }
}

pub async fn run_feature_engine(
    config: Config,
    store: SymbolStore,
    hot_set: HotSet,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut interval = time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut last_rejections: HashMap<String, CandidateReject> = HashMap::new();
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
            _ = interval.tick() => {
                let now = now_millis();
                let mut candidates = Vec::new();
                let mut gate_rejections = Vec::new();
                for mut entry in store.iter_mut() {
                    let features = calculate_features(&entry, now);
                    entry.features = features;
                    match candidate_from(&config, &entry, now) {
                        Ok(candidate) => {
                            last_rejections.remove(&candidate.symbol);
                            candidates.push(candidate);
                        }
                        Err(reason) => {
                            let symbol = entry.symbol.clone();
                            if last_rejections.insert(symbol.clone(), reason) != Some(reason) {
                                gate_rejections.push(GateRejection {
                                    symbol,
                                    reason,
                                    features,
                                    observed_at: now,
                                });
                            }
                        }
                    }
                }
                candidates.sort_by(|left, right| right.score.total_cmp(&left.score));
                candidates.truncate(config.hot_set_size);
                hot_set.send_replace(EngineFrame {
                    candidates,
                    gate_rejections,
                });
            }
        }
    }
}

fn calculate_features(state: &SymbolState, now: i64) -> FeatureSnapshot {
    let return_15s = return_since(state, now - 15_000);
    let return_60s = return_since(state, now - 60_000);
    let return_300s = return_since(state, now - 300_000);
    let volume_impulse = volume_impulse(state, now - 60_000);
    let volatility = realized_volatility(state, now - 60_000);
    let spread_percent = state.spread_percent().unwrap_or(100.0);
    let book_imbalance = state.top_book_imbalance().unwrap_or_default();
    let momentum = normalized(return_15s.abs(), 0.05, 0.80);
    let volume = normalized(volume_impulse, 0.0, 0.002);
    let liquidity = if spread_percent.is_finite() {
        1.0 - normalized(spread_percent, 0.02, 0.20)
    } else {
        0.0
    };
    let book = normalized(book_imbalance.abs(), 0.05, 0.60);
    let cheap_score =
        (momentum * 25.0 + volume * 25.0 + liquidity * 25.0 + book * 25.0).clamp(0.0, 100.0);
    FeatureSnapshot {
        return_15s,
        return_60s,
        return_300s,
        volume_impulse,
        volatility,
        spread_percent,
        book_imbalance,
        cheap_score,
        updated_at: state.event_time,
    }
}

fn candidate_from(
    config: &Config,
    state: &SymbolState,
    now: i64,
) -> Result<Candidate, CandidateReject> {
    let features = state.features;
    if now.saturating_sub(state.event_time) > config.stale_after.as_millis() as i64
        || state.last_price <= 0.0
    {
        return Err(CandidateReject::Stale);
    }
    let window_ready = state.samples.len() >= 30
        && state
            .samples
            .front()
            .is_some_and(|sample| sample.event_time <= now - 30_000);
    if !window_ready {
        return Err(CandidateReject::Warmup);
    }
    if state.quote_volume < config.min_quote_volume {
        return Err(CandidateReject::LowVolume);
    }
    if !features.spread_percent.is_finite() || features.spread_percent > config.max_spread_percent {
        return Err(CandidateReject::InvalidSpread);
    }
    if features.cheap_score < 55.0 {
        return Err(CandidateReject::WeakScore);
    }
    let direction =
        features.return_15s * 0.5 + features.return_60s * 0.35 + features.book_imbalance * 0.15;
    let side = if direction >= 0.03 {
        Side::Long
    } else if direction <= -0.03 {
        Side::Short
    } else {
        return Err(CandidateReject::NeutralDirection);
    };
    let signed_votes = [
        features.return_15s.signum(),
        features.return_60s.signum(),
        features.book_imbalance.signum(),
    ]
    .into_iter()
    .filter(|sign| *sign == side.direction())
    .count();
    if signed_votes < 2 {
        return Err(CandidateReject::ConflictingSignals);
    }
    let confidence = (features.cheap_score / 100.0).clamp(0.35, 1.0);
    // realized_volatility is in percentage points; convert to a decimal
    // fraction before applying the ATR-like stop multiplier.
    let stop_percent = (features.volatility / 100.0 * 2.2).clamp(0.004, 0.018);
    Ok(Candidate {
        symbol: state.symbol.clone(),
        side,
        price: state.last_price,
        score: features.cheap_score,
        confidence,
        stop_distance: state.last_price * stop_percent,
        liquidity_notional: (state.bid * state.bid_quantity).min(state.ask * state.ask_quantity),
        observed_at: now,
        features,
    })
}

fn return_since(state: &SymbolState, threshold: i64) -> f64 {
    let Some(latest) = state.samples.back() else {
        return 0.0;
    };
    let base = state
        .samples
        .iter()
        .find(|sample| sample.event_time >= threshold)
        .or_else(|| state.samples.front());
    base.filter(|sample| sample.price > 0.0)
        .map(|sample| (latest.price / sample.price - 1.0) * 100.0)
        .unwrap_or_default()
}

fn volume_impulse(state: &SymbolState, threshold: i64) -> f64 {
    let Some(latest) = state.samples.back() else {
        return 0.0;
    };
    let base = state
        .samples
        .iter()
        .find(|sample| sample.event_time >= threshold)
        .or_else(|| state.samples.front());
    base.map(|sample| {
        ((latest.quote_volume - sample.quote_volume) / latest.quote_volume.max(1.0)).max(0.0)
    })
    .unwrap_or_default()
}

fn realized_volatility(state: &SymbolState, threshold: i64) -> f64 {
    let mut previous = None;
    let mut returns = Vec::new();
    for sample in state
        .samples
        .iter()
        .filter(|sample| sample.event_time >= threshold)
    {
        if let Some(price) = previous {
            returns.push((sample.price / price - 1.0) * 100.0);
        }
        previous = Some(sample.price);
    }
    if returns.len() < 2 {
        return 0.0;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let variance = returns
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (returns.len() - 1) as f64;
    variance.sqrt()
}

fn normalized(value: f64, low: f64, high: f64) -> f64 {
    if high <= low {
        return 0.0;
    }
    ((value - low) / (high - low)).clamp(0.0, 1.0)
}

pub fn coalesce_candidates(candidates: impl IntoIterator<Item = Candidate>) -> Vec<Candidate> {
    let mut latest: HashMap<String, Candidate> = HashMap::new();
    for candidate in candidates {
        match latest.get(&candidate.symbol) {
            Some(current) if current.observed_at > candidate.observed_at => {}
            _ => {
                latest.insert(candidate.symbol.clone(), candidate);
            }
        }
    }
    let mut values: Vec<_> = latest.into_values().collect();
    values.sort_by(|left, right| right.score.total_cmp(&left.score));
    values
}

pub fn symbols_in_hot_set(candidates: &[Candidate]) -> HashSet<String> {
    candidates
        .iter()
        .map(|candidate| candidate.symbol.clone())
        .collect()
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(symbol: &str, score: f64, observed_at: i64) -> Candidate {
        Candidate {
            symbol: symbol.to_string(),
            side: Side::Long,
            price: 1.0,
            score,
            confidence: 0.5,
            stop_distance: 0.01,
            liquidity_notional: 10_000.0,
            observed_at,
            features: FeatureSnapshot::default(),
        }
    }

    fn config() -> Config {
        Config {
            rest_base_url: "https://example.test".to_string(),
            websocket_url: "wss://example.test".to_string(),
            database_path: "test.db".to_string(),
            panel_bind: "127.0.0.1:8080".parse().unwrap(),
            panel_action_token: "0123456789abcdef0123456789abcdef".to_string(),
            initial_balance: 10_000.0,
            entry_enabled: true,
            max_positions: 10,
            leverage: 3.0,
            risk_per_trade: 0.005,
            max_portfolio_risk: 0.02,
            max_trade_allocation: 0.10,
            taker_fee_rate: 0.0004,
            expected_entry_slippage_bps: 1.0,
            expected_exit_slippage_bps: 2.0,
            break_even_safety_bps: 1.0,
            min_quote_volume: 20_000_000.0,
            max_spread_percent: 0.10,
            hot_set_size: 64,
            stale_after: Duration::from_secs(5),
        }
    }

    #[test]
    fn coalescing_keeps_latest_symbol_state_without_fifo_backlog() {
        let result = coalesce_candidates([
            candidate("AUSDT", 90.0, 1),
            candidate("AUSDT", 70.0, 2),
            candidate("BUSDT", 80.0, 1),
        ]);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].symbol, "BUSDT");
        assert_eq!(result[1].observed_at, 2);
    }

    #[test]
    fn bearish_symbol_can_generate_short_without_btc_regime_gate() {
        let now = 100_000;
        let mut state = SymbolState {
            symbol: "ALTUSDT".to_string(),
            last_price: 99.0,
            bid: 98.99,
            ask: 99.01,
            bid_quantity: 1_000.0,
            ask_quantity: 2_000.0,
            quote_volume: 30_000_000.0,
            event_time: now,
            features: FeatureSnapshot {
                return_15s: -0.20,
                return_60s: -0.40,
                return_300s: -0.80,
                volume_impulse: 0.01,
                volatility: 0.20,
                spread_percent: 0.02,
                book_imbalance: -0.40,
                cheap_score: 80.0,
                updated_at: now,
            },
            ..SymbolState::default()
        };
        for second in 0..=30 {
            state.push_sample(crate::model::MarketSample {
                event_time: now - (30 - second) * 1_000,
                price: 100.0 - second as f64 * 0.03,
                quote_volume: 30_000_000.0,
            });
        }
        let result = candidate_from(&config(), &state, now).unwrap();
        assert_eq!(result.side, Side::Short);
    }
}
