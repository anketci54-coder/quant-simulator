
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

pub type HotSet = Arc<watch::Sender<Vec<Candidate>>>;

pub async fn run_feature_engine(
    config: Config,
    store: SymbolStore,
    hot_set: HotSet,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut interval = time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
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
                for mut entry in store.iter_mut() {
                    let features = calculate_features(&entry, now);
                    entry.features = features;
                    if let Some(candidate) = candidate_from(&config, &entry, now) {
                        candidates.push(candidate);
                    }
                }
                candidates.sort_by(|left, right| right.score.total_cmp(&left.score));
                candidates.truncate(config.hot_set_size);
                hot_set.send_replace(candidates);
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
    let spread_percent = state.spread_percent().unwrap_or(f64::INFINITY);
    let book_imbalance = state.top_book_imbalance().unwrap_or_default();
    let momentum = normalized(return_15s.abs(), 0.05, 0.80);
    let volume = normalized(volume_impulse, 0.0, 0.002);
    let liquidity = if spread_percent.is_finite() {
        1.0 - normalized(spread_percent, 0.02, 0.20)
    } else {
        0.0
    };
    let book = normalized(book_imbalance.abs(), 0.05, 0.60);
    let cheap_score = (momentum * 25.0 + volume * 25.0 + liquidity * 25.0 + book * 25.0)
        .clamp(0.0, 100.0);
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

fn candidate_from(config: &Config, state: &SymbolState, now: i64) -> Option<Candidate> {
    let features = state.features;
    if now.saturating_sub(state.event_time) > config.stale_after.as_millis() as i64
        || state.quote_volume < config.min_quote_volume
        || features.spread_percent > config.max_spread_percent
        || features.cheap_score < 55.0
        || state.last_price <= 0.0
    {
        return None;
    }
    let direction = features.return_15s * 0.5
        + features.return_60s * 0.35
        + features.book_imbalance * 0.15;
    let side = if direction > 0.0 {
        Side::Long
    } else {
        Side::Short
    };
    let confidence = (features.cheap_score / 100.0).clamp(0.35, 1.0);
    let stop_percent = (features.volatility * 2.2).clamp(0.004, 0.018);
    Some(Candidate {
        symbol: state.symbol.clone(),
        side,
        price: state.last_price,
        score: features.cheap_score,
        confidence,
        stop_distance: state.last_price * stop_percent,
        liquidity_notional: (state.bid * state.bid_quantity)
            .min(state.ask * state.ask_quantity),
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
}
