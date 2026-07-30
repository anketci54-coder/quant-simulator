use std::{env, net::SocketAddr, time::Duration};

use anyhow::{bail, Context, Result};

#[derive(Clone, Debug)]
pub struct Config {
    pub rest_base_url: String,
    pub websocket_url: String,
    pub panel_bind: SocketAddr,
    pub initial_balance: f64,
    pub entry_enabled: bool,
    pub max_positions: usize,
    pub risk_per_trade: f64,
    pub max_portfolio_risk: f64,
    pub max_trade_allocation: f64,
    pub min_quote_volume: f64,
    pub max_spread_percent: f64,
    pub hot_set_size: usize,
    pub stale_after: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let config = Self {
            rest_base_url: env::var("BINANCE_BASE_URL")
                .unwrap_or_else(|_| "https://fapi.binance.com".to_string()),
            websocket_url: env::var("BINANCE_WS_URL").unwrap_or_else(|_| {
                "wss://fstream.binance.com/stream?streams=!miniTicker@arr/!bookTicker".to_string()
            }),
            panel_bind: env::var("PANEL_BIND")
                .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
                .parse()
                .context("PANEL_BIND must be an IP:PORT address")?,
            initial_balance: parse("INITIAL_BALANCE_USDT", 10_000.0)?,
            entry_enabled: env::var("ENTRY_ENABLED")
                .map(|value| value.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
            max_positions: parse("MAX_POSITIONS", 10usize)?.clamp(1, 10),
            risk_per_trade: parse("RISK_PER_TRADE", 0.005f64)?,
            max_portfolio_risk: parse("MAX_PORTFOLIO_RISK", 0.02f64)?,
            max_trade_allocation: parse("MAX_TRADE_ALLOCATION", 0.10f64)?,
            min_quote_volume: parse("MIN_QUOTE_VOLUME", 20_000_000f64)?,
            max_spread_percent: parse("MAX_SPREAD_PERCENT", 0.10f64)?,
            hot_set_size: parse("HOT_SET_SIZE", 64usize)?.clamp(8, 256),
            stale_after: Duration::from_millis(
                parse("MARKET_STALE_AFTER_MS", 5_000u64)?.clamp(1_000, 60_000),
            ),
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.initial_balance <= 0.0 {
            bail!("INITIAL_BALANCE_USDT must be positive");
        }
        if !(0.0..=0.02).contains(&self.risk_per_trade) {
            bail!("RISK_PER_TRADE must be between 0 and 0.02");
        }
        if !(0.0..=0.10).contains(&self.max_portfolio_risk) {
            bail!("MAX_PORTFOLIO_RISK must be between 0 and 0.10");
        }
        if !(0.01..=0.10).contains(&self.max_trade_allocation) {
            bail!("MAX_TRADE_ALLOCATION must be between 0.01 and 0.10");
        }
        Ok(())
    }
}

fn parse<T>(name: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|error| anyhow::anyhow!("{name}: {error}")),
        Err(_) => Ok(default),
    }
}
