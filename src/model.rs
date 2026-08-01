use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

pub const STRATEGY_VERSION: &str = "MTF_V4";
pub const MAX_SAMPLES: usize = 1_800;

#[derive(Clone, Debug, Default)]
pub struct SymbolState {
    pub symbol: String,
    pub last_price: f64,
    pub bid: f64,
    pub ask: f64,
    pub bid_quantity: f64,
    pub ask_quantity: f64,
    pub quote_volume: f64,
    pub mark_price: f64,
    pub funding_rate: f64,
    pub next_funding_time: i64,
    pub price_received_at: i64,
    pub book_received_at: i64,
    pub mark_received_at: i64,
    pub samples: VecDeque<MarketSample>,
    pub features: FeatureSnapshot,
}

impl SymbolState {
    pub fn push_sample(&mut self, sample: MarketSample) {
        if self
            .samples
            .back()
            .is_some_and(|last| last.event_time == sample.event_time)
        {
            self.samples.pop_back();
        }
        self.samples.push_back(sample);
        while self.samples.len() > MAX_SAMPLES {
            self.samples.pop_front();
        }
    }

    pub fn spread_percent(&self) -> Option<f64> {
        if self.bid <= 0.0 || self.ask <= self.bid {
            return None;
        }
        let midpoint = (self.bid + self.ask) / 2.0;
        Some((self.ask - self.bid) / midpoint * 100.0)
    }

    pub fn top_book_imbalance(&self) -> Option<f64> {
        let total = self.bid_quantity + self.ask_quantity;
        (total > 0.0).then_some((self.bid_quantity - self.ask_quantity) / total)
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct MarketSample {
    pub event_time: i64,
    pub price: f64,
    pub quote_volume: f64,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct FeatureSnapshot {
    pub return_15s: f64,
    pub return_60s: f64,
    pub return_300s: f64,
    pub volume_impulse: f64,
    pub volatility: f64,
    pub spread_percent: f64,
    pub book_imbalance: f64,
    pub cheap_score: f64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Serialize)]
pub struct Candidate {
    pub symbol: String,
    pub side: Side,
    pub price: f64,
    pub score: f64,
    pub confidence: f64,
    pub stop_distance: f64,
    pub liquidity_notional: f64,
    pub observed_at: i64,
    pub features: FeatureSnapshot,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Long,
    Short,
}

impl Side {
    pub fn direction(self) -> f64 {
        match self {
            Self::Long => 1.0,
            Self::Short => -1.0,
        }
    }

    pub fn favorable_price(self, bid: f64, ask: f64) -> f64 {
        match self {
            Self::Long => bid,
            Self::Short => ask,
        }
    }

    pub fn stop_is_hit(self, bid: f64, ask: f64, stop: f64) -> bool {
        match self {
            Self::Long => bid <= stop,
            Self::Short => ask >= stop,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CombinedEvent {
    pub stream: String,
    pub data: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MiniTicker {
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "c", deserialize_with = "de_f64")]
    pub close: f64,
    #[serde(rename = "q", deserialize_with = "de_f64")]
    pub quote_volume: f64,
    #[serde(rename = "st", default)]
    pub symbol_type: Option<u8>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct BookTicker {
    #[serde(rename = "E", default)]
    pub event_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "b", deserialize_with = "de_f64")]
    pub bid: f64,
    #[serde(rename = "B", deserialize_with = "de_f64")]
    pub bid_quantity: f64,
    #[serde(rename = "a", deserialize_with = "de_f64")]
    pub ask: f64,
    #[serde(rename = "A", deserialize_with = "de_f64")]
    pub ask_quantity: f64,
    #[serde(rename = "st", default)]
    pub symbol_type: Option<u8>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MarkPrice {
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "p", deserialize_with = "de_f64")]
    pub mark_price: f64,
    #[serde(rename = "r", deserialize_with = "de_f64")]
    pub funding_rate: f64,
    #[serde(rename = "T")]
    pub next_funding_time: i64,
    #[serde(rename = "st", default)]
    pub symbol_type: Option<u8>,
}

fn de_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    value.parse().map_err(serde::de::Error::custom)
}
