use std::{collections::HashMap, sync::Arc, time::Duration};

use futures_util::future::join_all;
use serde_json::Value;
use tokio::sync::{watch, Semaphore};

use crate::{
    config::Config,
    engine::{EngineFrame, HotSet},
    model::{Candidate, Side},
};

const VALIDATION_LIMIT: usize = 12;
const INDICATOR_CACHE_MS: i64 = 60_000;
const MAX_USABLE_INDICATOR_AGE_MS: i64 = 120_000;
const BTC_REGIME_CACHE_MS: i64 = 300_000;
const MAX_CONCURRENT_PACK_FETCHES: usize = 3;

#[derive(Clone, Copy, Debug)]
struct Indicators {
    ema_fast: f64,
    ema_slow: f64,
    rsi: f64,
    atr: f64,
    adx: f64,
}

#[derive(Clone, Copy, Debug)]
struct MtfPack {
    one: Indicators,
    five: Indicators,
    fifteen: Indicators,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Regime {
    Bull,
    Bear,
    Sideways,
}

pub async fn run_hybrid_filter(
    config: Config,
    mut raw_rx: watch::Receiver<EngineFrame>,
    output: HotSet,
    mut shutdown: watch::Receiver<bool>,
) {
    let client = match reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(8))
        .user_agent("quant-simulator/0.3 MTF_HYBRID")
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            eprintln!("Hybrid HTTP client error: {error}");
            return;
        }
    };
    let fetch_limit = Arc::new(Semaphore::new(MAX_CONCURRENT_PACK_FETCHES));
    let mut cache: HashMap<String, (i64, MtfPack)> = HashMap::new();
    let mut btc_cache: Option<(i64, Regime)> = None;

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
            changed = raw_rx.changed() => {
                if changed.is_err() {
                    return;
                }
                let raw = raw_rx.borrow().clone();
                let now = now_millis();
                let regime = match btc_cache {
                    Some((cached_at, regime)) if now.saturating_sub(cached_at) < BTC_REGIME_CACHE_MS => regime,
                    _ => match fetch_indicators(&client, &config.rest_base_url, "BTCUSDT", "1h").await {
                        Ok(indicators) => {
                            let regime = classify_regime(indicators);
                            btc_cache = Some((now, regime));
                            regime
                        }
                        Err(error) => {
                            eprintln!("Hybrid BTC regime error: {error}");
                            btc_cache.map(|(_, regime)| regime).unwrap_or(Regime::Sideways)
                        }
                    },
                };

                let selected: Vec<_> = raw.candidates.into_iter().take(VALIDATION_LIMIT).collect();
                let missing: Vec<_> = selected
                    .iter()
                    .filter(|candidate| {
                        !cache.get(&candidate.symbol).is_some_and(|(cached_at, _)| {
                            now.saturating_sub(*cached_at) < INDICATOR_CACHE_MS
                        })
                    })
                    .map(|candidate| candidate.symbol.clone())
                    .collect();
                let base_url = config.rest_base_url.clone();
                let fetched = join_all(missing.into_iter().map(|symbol| {
                    let client = client.clone();
                    let base_url = base_url.clone();
                    let fetch_limit = fetch_limit.clone();
                    async move {
                        let permit = fetch_limit.acquire_owned().await;
                        let result = match permit {
                            Ok(_permit) => fetch_pack(&client, &base_url, &symbol).await,
                            Err(_) => Err("MTF fetch limiter closed".to_string()),
                        };
                        (symbol, result)
                    }
                })).await;
                for (symbol, result) in fetched {
                    match result {
                        Ok(pack) => { cache.insert(symbol, (now, pack)); }
                        Err(error) => eprintln!("Hybrid MTF error symbol={symbol}: {error}"),
                    }
                }

                let mut candidates = Vec::new();
                for candidate in selected {
                    let Some((cached_at, pack)) = cache.get(&candidate.symbol).copied() else {
                        continue;
                    };
                    if now.saturating_sub(cached_at) > MAX_USABLE_INDICATOR_AGE_MS {
                        continue;
                    }
                    if let Some(candidate) = validate_candidate(candidate, pack, regime) {
                        candidates.push(candidate);
                    }
                }
                candidates.sort_by(|left, right| right.score.total_cmp(&left.score));
                output.send_replace(EngineFrame {
                    candidates,
                    gate_rejections: raw.gate_rejections,
                });
                cache.retain(|_, (cached_at, _)| now.saturating_sub(*cached_at) < 10 * INDICATOR_CACHE_MS);
            }
        }
    }
}

async fn fetch_pack(
    client: &reqwest::Client,
    base_url: &str,
    symbol: &str,
) -> Result<MtfPack, String> {
    let (one, five, fifteen) = tokio::join!(
        fetch_indicators(client, base_url, symbol, "1m"),
        fetch_indicators(client, base_url, symbol, "5m"),
        fetch_indicators(client, base_url, symbol, "15m"),
    );
    Ok(MtfPack {
        one: one?,
        five: five?,
        fifteen: fifteen?,
    })
}

async fn fetch_indicators(
    client: &reqwest::Client,
    base_url: &str,
    symbol: &str,
    interval: &str,
) -> Result<Indicators, String> {
    let url = format!("{}/fapi/v1/klines", base_url.trim_end_matches('/'));
    let mut last_error = String::new();
    for attempt in 0..2 {
        let response = client
            .get(&url)
            .query(&[("symbol", symbol), ("interval", interval), ("limit", "60")])
            .send()
            .await
            .and_then(reqwest::Response::error_for_status);
        match response {
            Ok(response) => match response.json::<Vec<Vec<Value>>>().await {
                Ok(rows) => {
                    let mut highs = Vec::with_capacity(rows.len());
                    let mut lows = Vec::with_capacity(rows.len());
                    let mut closes = Vec::with_capacity(rows.len());
                    for row in rows {
                        let parse = |index: usize| {
                            row.get(index)
                                .and_then(Value::as_str)
                                .and_then(|value| value.parse::<f64>().ok())
                        };
                        if let (Some(high), Some(low), Some(close)) =
                            (parse(2), parse(3), parse(4))
                        {
                            highs.push(high);
                            lows.push(low);
                            closes.push(close);
                        }
                    }
                    return calculate_indicators(&highs, &lows, &closes)
                        .ok_or_else(|| format!("{interval} insufficient indicator data"));
                }
                Err(error) => last_error = format!("{interval} parse failed: {error}"),
            },
            Err(error) => last_error = format!("{interval} request failed: {error}"),
        }
        if attempt == 0 {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    Err(last_error)
}

fn validate_candidate(
    mut candidate: Candidate,
    pack: MtfPack,
    regime: Regime,
) -> Option<Candidate> {
    let normalized_trend = (pack.fifteen.ema_fast - pack.fifteen.ema_slow) / pack.fifteen.atr;
    let obi = candidate.features.book_imbalance;
    let long = candidate.side == Side::Long;
    let trend_confirmed = match (regime, candidate.side) {
        (Regime::Bull, Side::Long) => {
            obi >= 0.16
                && normalized_trend >= 0.08
                && pack.fifteen.adx >= 22.0
                && pack.five.ema_fast > pack.five.ema_slow
                && pack.five.adx >= 20.0
                && (54.0..=70.0).contains(&pack.five.rsi)
                && pack.one.ema_fast >= pack.one.ema_slow
                && (48.0..=75.0).contains(&pack.one.rsi)
        }
        (Regime::Bear, Side::Short) => {
            obi <= -0.12
                && normalized_trend <= -0.05
                && pack.fifteen.adx >= 20.0
                && pack.five.ema_fast < pack.five.ema_slow
                && pack.five.adx >= 18.0
                && (28.0..=48.0).contains(&pack.five.rsi)
                && pack.one.ema_fast <= pack.one.ema_slow
                && (25.0..=52.0).contains(&pack.one.rsi)
        }
        _ => false,
    };
    let scalp_confirmed = regime == Regime::Sideways
        && if long {
            obi >= 0.22
                && normalized_trend >= 0.12
                && pack.fifteen.adx >= 27.0
                && pack.five.ema_fast > pack.five.ema_slow
                && pack.five.adx >= 22.0
                && (55.0..=76.0).contains(&pack.five.rsi)
                && pack.one.ema_fast >= pack.one.ema_slow
                && (52.0..=78.0).contains(&pack.one.rsi)
        } else {
            obi <= -0.18
                && normalized_trend <= -0.10
                && pack.fifteen.adx >= 25.0
                && pack.five.ema_fast < pack.five.ema_slow
                && pack.five.adx >= 22.0
                && (24.0..=45.0).contains(&pack.five.rsi)
                && pack.one.ema_fast <= pack.one.ema_slow
                && (22.0..=48.0).contains(&pack.one.rsi)
        };
    if !trend_confirmed && !scalp_confirmed {
        return None;
    }
    let stop_distance = if scalp_confirmed {
        (pack.fifteen.atr * 1.25).clamp(candidate.price * 0.004, candidate.price * 0.018)
    } else {
        (pack.fifteen.atr * 1.8).clamp(candidate.price * 0.006, candidate.price * 0.03)
    };
    let trend_quality = ((pack.fifteen.adx - 18.0) / 22.0).clamp(0.0, 1.0);
    let obi_quality = (obi.abs() / 0.30).clamp(0.0, 1.0);
    let confirmation_quality = ((pack.five.adx - 18.0) / 18.0).clamp(0.0, 1.0);
    candidate.confidence =
        (trend_quality * 0.45 + obi_quality * 0.35 + confirmation_quality * 0.20)
            .clamp(if scalp_confirmed { 0.35 } else { 0.55 }, 1.0);
    candidate.stop_distance = stop_distance;
    candidate.score = (candidate.score * 0.40 + candidate.confidence * 60.0).clamp(0.0, 100.0);
    candidate.market_regime = if scalp_confirmed {
        "HYBRID_SIDEWAYS_SCALP".to_string()
    } else if long {
        "HYBRID_BULL".to_string()
    } else {
        "HYBRID_BEAR".to_string()
    };
    Some(candidate)
}

fn calculate_indicators(highs: &[f64], lows: &[f64], closes: &[f64]) -> Option<Indicators> {
    if closes.len() < 22 || highs.len() != closes.len() || lows.len() != closes.len() {
        return None;
    }
    let ema_fast = ema(closes, 8)?;
    let ema_slow = ema(closes, 21)?;
    let changes: Vec<_> = closes
        .windows(2)
        .map(|window| window[1] - window[0])
        .collect();
    let recent = &changes[changes.len() - changes.len().min(7)..];
    let gains: f64 = recent.iter().map(|change| change.max(0.0)).sum();
    let losses: f64 = recent.iter().map(|change| (-change).max(0.0)).sum();
    let rsi = if losses <= f64::EPSILON {
        100.0
    } else {
        100.0 - 100.0 / (1.0 + gains / losses)
    };
    let true_ranges: Vec<_> = (1..closes.len())
        .map(|index| {
            (highs[index] - lows[index])
                .max((highs[index] - closes[index - 1]).abs())
                .max((lows[index] - closes[index - 1]).abs())
        })
        .collect();
    let atr_period = true_ranges.len().min(14);
    let atr = true_ranges[true_ranges.len() - atr_period..]
        .iter()
        .sum::<f64>()
        / atr_period as f64;
    if atr <= 0.0 || !atr.is_finite() {
        return None;
    }
    let period = (closes.len() - 1).min(14);
    let start = closes.len() - period;
    let (mut positive_dm, mut negative_dm) = (0.0, 0.0);
    for index in start..closes.len() {
        let upward = highs[index] - highs[index - 1];
        let downward = lows[index - 1] - lows[index];
        if upward > downward && upward > 0.0 {
            positive_dm += upward;
        }
        if downward > upward && downward > 0.0 {
            negative_dm += downward;
        }
    }
    let positive_di = 100.0 * (positive_dm / period as f64) / atr;
    let negative_di = 100.0 * (negative_dm / period as f64) / atr;
    let adx = if positive_di + negative_di > f64::EPSILON {
        100.0 * (positive_di - negative_di).abs() / (positive_di + negative_di)
    } else {
        0.0
    };
    Some(Indicators {
        ema_fast,
        ema_slow,
        rsi,
        atr,
        adx,
    })
}

fn ema(values: &[f64], period: usize) -> Option<f64> {
    let first = *values.first()?;
    let multiplier = 2.0 / (period as f64 + 1.0);
    Some(values.iter().skip(1).fold(first, |current, value| {
        (value - current) * multiplier + current
    }))
}

fn classify_regime(indicators: Indicators) -> Regime {
    if indicators.adx < 18.0 {
        Regime::Sideways
    } else if indicators.ema_fast > indicators.ema_slow && indicators.rsi >= 52.0 {
        Regime::Bull
    } else if indicators.ema_fast < indicators.ema_slow && indicators.rsi <= 48.0 {
        Regime::Bear
    } else {
        Regime::Sideways
    }
}

fn now_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indicator_stack_detects_rising_market() {
        let closes: Vec<_> = (0..60).map(|index| 100.0 + index as f64 * 0.5).collect();
        let highs: Vec<_> = closes.iter().map(|close| close + 0.3).collect();
        let lows: Vec<_> = closes.iter().map(|close| close - 0.3).collect();
        let indicators = calculate_indicators(&highs, &lows, &closes).unwrap();
        assert!(indicators.ema_fast > indicators.ema_slow);
        assert!(indicators.adx > 0.0);
    }
}
