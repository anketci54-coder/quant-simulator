use std::{
    collections::HashSet,
    str,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use dashmap::DashMap;
use futures_util::StreamExt;
use serde::Deserialize;
use tokio::{sync::watch, time};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{
    config::Config,
    model::{BookTicker, CombinedEvent, MarkPrice, MarketSample, MiniTicker, SymbolState},
};

pub type SymbolStore = Arc<DashMap<String, SymbolState>>;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ApplyStats {
    mini_tickers: usize,
    book_tickers: usize,
    mark_prices: usize,
}

impl ApplyStats {
    fn add(&mut self, other: Self) {
        self.mini_tickers += other.mini_tickers;
        self.book_tickers += other.book_tickers;
        self.mark_prices += other.mark_prices;
    }

    fn total(self) -> usize {
        self.mini_tickers + self.book_tickers + self.mark_prices
    }
}

#[derive(Clone)]
pub struct Universe {
    symbols: Arc<DashMap<String, SymbolMeta>>,
}

#[derive(Clone, Copy, Debug)]
pub struct SymbolMeta {
    pub tick_size: f64,
    pub step_size: f64,
    pub min_quantity: f64,
}

impl Universe {
    pub fn new() -> Self {
        Self {
            symbols: Arc::new(DashMap::new()),
        }
    }

    pub fn contains(&self, symbol: &str) -> bool {
        self.symbols.contains_key(symbol)
    }

    pub fn len(&self) -> usize {
        self.symbols.len()
    }

    pub fn is_empty(&self) -> bool {
        self.symbols.is_empty()
    }

    pub fn symbols(&self) -> HashSet<String> {
        self.symbols
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    pub fn meta(&self, symbol: &str) -> Option<SymbolMeta> {
        self.symbols.get(symbol).map(|entry| *entry)
    }

    pub async fn refresh(&self, client: &reqwest::Client, config: &Config) -> Result<()> {
        let url = format!("{}/fapi/v1/exchangeInfo", config.rest_base_url);
        let response: ExchangeInfo = client
            .get(url)
            .send()
            .await
            .context("exchangeInfo request failed")?
            .error_for_status()
            .context("exchangeInfo returned an error")?
            .json()
            .await
            .context("exchangeInfo JSON is invalid")?;

        let mut next = Vec::new();
        for symbol in response.symbols {
            if symbol.status != "TRADING"
                || symbol.contract_type != "PERPETUAL"
                || symbol.quote_asset != "USDT"
                || !is_safe_symbol(&symbol.symbol)
            {
                continue;
            }
            let mut meta = SymbolMeta {
                tick_size: 0.0,
                step_size: 0.0,
                min_quantity: 0.0,
            };
            for filter in symbol.filters {
                match filter.filter_type.as_str() {
                    "PRICE_FILTER" => meta.tick_size = parse_or_zero(&filter.tick_size),
                    "LOT_SIZE" => {
                        meta.step_size = parse_or_zero(&filter.step_size);
                        meta.min_quantity = parse_or_zero(&filter.min_qty);
                    }
                    _ => {}
                }
            }
            if meta.tick_size > 0.0 && meta.step_size > 0.0 && meta.min_quantity > 0.0 {
                next.push((symbol.symbol, meta));
            }
        }
        if next.is_empty() {
            anyhow::bail!("exchangeInfo kullanılabilir USD-M perpetual sembol döndürmedi");
        }
        let next_symbols: HashSet<String> = next.iter().map(|(symbol, _)| symbol.clone()).collect();
        for (symbol, meta) in next {
            self.symbols.insert(symbol, meta);
        }
        self.symbols
            .retain(|symbol, _| next_symbols.contains(symbol));
        Ok(())
    }
}

impl Default for Universe {
    fn default() -> Self {
        Self::new()
    }
}

pub async fn run_universe_manager(
    universe: Universe,
    client: reqwest::Client,
    config: Config,
    ready: watch::Sender<bool>,
) {
    loop {
        match universe.refresh(&client, &config).await {
            Ok(()) => {
                ready.send_replace(true);
                time::sleep(Duration::from_secs(900)).await;
            }
            Err(error) => {
                eprintln!("Universe refresh error: {error:#}");
                ready.send_replace(false);
                time::sleep(Duration::from_secs(10)).await;
            }
        }
    }
}

pub async fn run_market_stream(
    source: &'static str,
    websocket_url: String,
    universe: Universe,
    store: SymbolStore,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut backoff = 1u64;
    loop {
        if *shutdown.borrow() {
            return;
        }
        match connect_async(&websocket_url).await {
            Ok((stream, _)) => {
                backoff = 1;
                println!("Market stream connected source={source}");
                let (_, mut reader) = stream.split();
                let mut applied_events = ApplyStats::default();
                let mut health_interval = time::interval(Duration::from_secs(60));
                health_interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
                health_interval.tick().await;
                loop {
                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                return;
                            }
                        }
                        _ = health_interval.tick() => {
                            let now = now_millis();
                            let price_ready = store
                                .iter()
                                .filter(|state| {
                                    state.last_price > 0.0
                                        && now.saturating_sub(state.price_received_at) <= 5_000
                                })
                                .count();
                            let book_ready = store
                                .iter()
                                .filter(|state| {
                                    state.bid > 0.0
                                        && state.ask > state.bid
                                        && now.saturating_sub(state.book_received_at) <= 15_000
                                })
                                .count();
                            println!(
                                "market_stream_health source={} total_60s={} mini_60s={} book_60s={} mark_60s={} active_symbols={} price_ready={} book_ready={}",
                                source,
                                applied_events.total(),
                                applied_events.mini_tickers,
                                applied_events.book_tickers,
                                applied_events.mark_prices,
                                store.len(),
                                price_ready,
                                book_ready
                            );
                            applied_events = ApplyStats::default();
                        }
                        message = reader.next() => {
                            match message {
                                Some(Ok(Message::Text(text))) => {
                                    match apply_message(&text, &universe, &store) {
                                        Ok(applied) => applied_events.add(applied),
                                        Err(error) => eprintln!("Market event rejected: {error:#}"),
                                    }
                                }
                                Some(Ok(Message::Binary(bytes))) => {
                                    match str::from_utf8(&bytes)
                                        .map_err(anyhow::Error::from)
                                        .and_then(|text| apply_message(text, &universe, &store))
                                    {
                                        Ok(applied) => applied_events.add(applied),
                                        Err(error) => {
                                            eprintln!("Binary market event rejected: {error:#}")
                                        }
                                    }
                                }
                                Some(Ok(Message::Close(_))) | None => break,
                                Some(Err(error)) => {
                                    eprintln!("Market stream error: {error}");
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            Err(error) => eprintln!("WebSocket connect error: {error}"),
        }
        time::sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(30);
    }
}

fn apply_message(text: &str, universe: &Universe, store: &SymbolStore) -> Result<ApplyStats> {
    apply_message_at(text, universe, store, now_millis())
}

fn apply_message_at(
    text: &str,
    universe: &Universe,
    store: &SymbolStore,
    received_at: i64,
) -> Result<ApplyStats> {
    let event: CombinedEvent = serde_json::from_str(text)?;
    let stream_name = event.stream.to_ascii_lowercase();
    let mut applied = ApplyStats::default();
    if stream_name.contains("miniticker") {
        let tickers: Vec<MiniTicker> = serde_json::from_value(event.data)?;
        for ticker in tickers {
            if !universe.contains(&ticker.symbol)
                || ticker.symbol_type.is_some_and(|kind| kind != 1)
            {
                continue;
            }
            let mut state = store
                .entry(ticker.symbol.clone())
                .or_insert_with(|| SymbolState {
                    symbol: ticker.symbol.clone(),
                    ..SymbolState::default()
                });
            state.last_price = ticker.close;
            state.quote_volume = ticker.quote_volume;
            state.price_received_at = received_at;
            state.push_sample(MarketSample {
                event_time: received_at,
                price: ticker.close,
                quote_volume: ticker.quote_volume,
            });
            applied.mini_tickers += 1;
        }
    } else if stream_name.contains("bookticker") {
        let tickers: Vec<BookTicker> = if event.data.is_array() {
            serde_json::from_value(event.data)?
        } else {
            vec![serde_json::from_value(event.data)?]
        };
        for ticker in tickers {
            if universe.contains(&ticker.symbol) && ticker.symbol_type.is_none_or(|kind| kind == 1)
            {
                let mut state = store
                    .entry(ticker.symbol.clone())
                    .or_insert_with(|| SymbolState {
                        symbol: ticker.symbol.clone(),
                        ..SymbolState::default()
                    });
                state.bid = ticker.bid;
                state.ask = ticker.ask;
                state.bid_quantity = ticker.bid_quantity;
                state.ask_quantity = ticker.ask_quantity;
                state.book_received_at = received_at;
                applied.book_tickers += 1;
            }
        }
    } else if stream_name.contains("markprice") {
        let prices: Vec<MarkPrice> = if event.data.is_array() {
            serde_json::from_value(event.data)?
        } else {
            vec![serde_json::from_value(event.data)?]
        };
        for price in prices {
            if universe.contains(&price.symbol) && price.symbol_type.is_none_or(|kind| kind == 1) {
                let mut state = store
                    .entry(price.symbol.clone())
                    .or_insert_with(|| SymbolState {
                        symbol: price.symbol.clone(),
                        ..SymbolState::default()
                    });
                state.mark_price = price.mark_price;
                state.funding_rate = price.funding_rate;
                state.next_funding_time = price.next_funding_time;
                state.mark_received_at = received_at;
                applied.mark_prices += 1;
            }
        }
    }
    Ok(applied)
}

fn is_safe_symbol(symbol: &str) -> bool {
    symbol.ends_with("USDT")
        && symbol.len() > 4
        && symbol
            .bytes()
            .all(|value| value.is_ascii_uppercase() || value.is_ascii_digit())
}

fn parse_or_zero(value: &str) -> f64 {
    value.parse().unwrap_or(0.0)
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[derive(Deserialize)]
struct ExchangeInfo {
    symbols: Vec<ExchangeSymbol>,
}

#[derive(Deserialize)]
struct ExchangeSymbol {
    symbol: String,
    status: String,
    #[serde(rename = "contractType")]
    contract_type: String,
    #[serde(rename = "quoteAsset")]
    quote_asset: String,
    filters: Vec<ExchangeFilter>,
}

#[derive(Deserialize)]
struct ExchangeFilter {
    #[serde(rename = "filterType")]
    filter_type: String,
    #[serde(rename = "tickSize", default)]
    tick_size: String,
    #[serde(rename = "stepSize", default)]
    step_size: String,
    #[serde(rename = "minQty", default)]
    min_qty: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbol_filter_rejects_unsafe_names() {
        assert!(is_safe_symbol("BTCUSDT"));
        assert!(is_safe_symbol("1000PEPEUSDT"));
        assert!(!is_safe_symbol("BTC-USDT"));
        assert!(!is_safe_symbol("我来了USDT"));
    }

    #[test]
    fn lowercase_combined_stream_names_update_symbol_state() {
        let universe = Universe::new();
        universe.symbols.insert(
            "BTCUSDT".to_string(),
            SymbolMeta {
                tick_size: 0.1,
                step_size: 0.001,
                min_quantity: 0.001,
            },
        );
        let store: SymbolStore = Arc::new(DashMap::new());
        let message = r#"{
            "stream":"!miniticker@arr",
            "data":[{
                "E":1000,
                "s":"BTCUSDT",
                "c":"65000.5",
                "q":"25000000"
            }]
        }"#;

        assert_eq!(
            apply_message_at(message, &universe, &store, 50_000).unwrap(),
            ApplyStats {
                mini_tickers: 1,
                ..ApplyStats::default()
            }
        );
        let state = store.get("BTCUSDT").unwrap();
        assert_eq!(state.last_price, 65_000.5);
        assert_eq!(state.price_received_at, 50_000);
        assert_eq!(state.samples.back().unwrap().event_time, 50_000);
        assert_eq!(state.samples.len(), 1);
    }

    #[test]
    fn split_book_stream_completes_trade_ready_state() {
        let universe = Universe::new();
        universe.symbols.insert(
            "BTCUSDT".to_string(),
            SymbolMeta {
                tick_size: 0.1,
                step_size: 0.001,
                min_quantity: 0.001,
            },
        );
        let store: SymbolStore = Arc::new(DashMap::new());
        let mini = r#"{
            "stream":"!miniTicker@arr",
            "data":[{"E":1000,"s":"BTCUSDT","c":"65000","q":"25000000","st":1}]
        }"#;
        let book = r#"{
            "stream":"!bookTicker",
            "data":{"E":1001,"s":"BTCUSDT","b":"64999","B":"10","a":"65001","A":"12","st":1}
        }"#;

        apply_message_at(mini, &universe, &store, 50_000).unwrap();
        let stats = apply_message_at(book, &universe, &store, 50_100).unwrap();

        assert_eq!(stats.book_tickers, 1);
        let state = store.get("BTCUSDT").unwrap();
        assert_eq!(state.price_received_at, 50_000);
        assert_eq!(state.book_received_at, 50_100);
        assert_eq!(state.bid, 64_999.0);
        assert_eq!(state.ask, 65_001.0);
    }
}
