use dotenvy::dotenv;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::env;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tiny_http::{Response, Server};

const MAX_POSITION_MARGIN_USDT: f64 = 100.0;
const FEE_RATE_PERCENT: f64 = 0.04;
const BREAKEVEN_BUFFER_RATE: f64 = 0.001;

#[derive(Clone, Debug)]
struct SymbolFilters {
    min_qty: f64,
    max_qty: f64,
    step_size: f64,
}

#[derive(Deserialize, Debug)]
struct ExchangeInfo {
    symbols: Vec<ExchangeSymbol>,
}

#[derive(Deserialize, Debug)]
struct ExchangeSymbol {
    symbol: String,
    filters: Vec<ExchangeFilter>,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "filterType")]
enum ExchangeFilter {
    #[serde(rename = "LOT_SIZE")]
    LotSize {
        #[serde(rename = "minQty")]
        min_qty: String,
        #[serde(rename = "maxQty")]
        max_qty: String,
        #[serde(rename = "stepSize")]
        step_size: String,
    },
    #[serde(other)]
    Other,
}

fn calculate_pnl_percent(side: &str, entry: f64, current: f64, leverage: f64) -> Option<f64> {
    if entry <= 0.0 || current <= 0.0 || leverage <= 0.0 {
        return None;
    }
    let raw_diff = if side == "LONG" {
        (current - entry) / entry
    } else if side == "SHORT" {
        (entry - current) / entry
    } else {
        return None;
    };
    Some((raw_diff * leverage * 100.0) - (FEE_RATE_PERCENT * leverage * 2.0))
}

fn break_even_stop(side: &str, entry: f64) -> Option<f64> {
    match side {
        "LONG" => Some(entry * (1.0 + BREAKEVEN_BUFFER_RATE)),
        "SHORT" => Some(entry * (1.0 - BREAKEVEN_BUFFER_RATE)),
        _ => None,
    }
}

fn calculate_position_size(
    balance: f64,
    entry: f64,
    stop: f64,
    leverage: f64,
    risk_fraction: f64,
    filters: &SymbolFilters,
) -> Option<(f64, f64, f64)> {
    if balance <= 0.0 || entry <= 0.0 || leverage <= 0.0 || !(0.0..=1.0).contains(&risk_fraction) {
        return None;
    }
    let stop_distance = (entry - stop).abs();
    if stop_distance <= 0.0 {
        return None;
    }
    let risk_qty = (balance * risk_fraction) / stop_distance;
    let margin_capped_qty = (MAX_POSITION_MARGIN_USDT * leverage) / entry;
    let raw_qty = risk_qty.min(margin_capped_qty);
    let qty = quantize_quantity(raw_qty, filters)?;
    let margin = (qty * entry) / leverage;
    let risk = qty * stop_distance;
    Some((qty, margin, risk))
}

fn position_risk(position: &ActivePosition) -> f64 {
    position
        .quantity
        .parse::<f64>()
        .ok()
        .map(|qty| qty * (position.entry_price - position.stop_loss).abs())
        .unwrap_or(0.0)
}

fn quantize_quantity(raw_qty: f64, filters: &SymbolFilters) -> Option<f64> {
    if !raw_qty.is_finite() || raw_qty <= 0.0 || filters.step_size <= 0.0 {
        return None;
    }
    let qty = (raw_qty / filters.step_size).floor() * filters.step_size;
    if qty + f64::EPSILON < filters.min_qty || qty > filters.max_qty {
        None
    } else {
        Some(qty)
    }
}

#[derive(Clone, Serialize, Debug, PartialEq)]
enum PositionLifecycle {
    PendingOpen,
    Open,
    Closed,

}

#[derive(Clone)]
struct Config {
    base_url: String,
    entry_enabled: bool,
    max_positions: usize,
    max_same_side_positions: usize,
    max_signal_candidates: usize,
    risk_per_trade: f64,
    max_portfolio_risk: f64,
    session_loss_limit: f64,
    max_consecutive_losses: usize,
    cooldown: Duration,
    min_quote_volume: f64,
    max_spread_percent: f64,
    obi_confirmation_samples: usize,
}

fn is_tradeable_usdt_symbol(symbol: &str) -> bool {
    let base = symbol.strip_suffix("USDT").unwrap_or_default();
    (2..=16).contains(&base.len())
        && base
            .bytes()
            .all(|character| character.is_ascii_uppercase() || character.is_ascii_digit())
}

impl Config {
    fn futures_url(&self, path: &str) -> String {
        format!(
            "{}/{}",
            self.base_url.trim_end_matches('/'),
            path.trim_start_matches('/')
        )
    }
}

#[derive(Deserialize, Debug, Clone)]
struct Ticker {
    symbol: String,
    #[serde(rename = "priceChangePercent")]
    price_change_percent: String,
    #[serde(rename = "quoteVolume")]
    quote_volume: String,
    #[serde(rename = "lastPrice")]
    last_price: String,
}

#[derive(Deserialize, Debug)]
struct Depth {
    bids: Vec<Vec<String>>,
    asks: Vec<Vec<String>>,
}

#[derive(Deserialize, Debug)]
struct BookTicker {
    symbol: String,
    #[serde(rename = "bidPrice")]
    bid_price: String,
    #[serde(rename = "askPrice")]
    ask_price: String,
}

fn format_tr_number(value: f64, precision: usize, show_plus: bool) -> String {
    if !value.is_finite() {
        return "—".to_string();
    }
    let sign = if value < 0.0 {
        "-"
    } else if show_plus && value > 0.0 {
        "+"
    } else {
        ""
    };
    let raw = format!("{:.*}", precision, value.abs());
    let (integer, decimals) = raw.split_once('.').unwrap_or((&raw, ""));
    let mut grouped = String::with_capacity(integer.len() + integer.len() / 3);
    for (index, character) in integer.chars().enumerate() {
        if index > 0 && (integer.len() - index) % 3 == 0 {
            grouped.push('.');
        }
        grouped.push(character);
    }
    if precision == 0 {
        format!("{sign}{grouped}")
    } else {
        format!("{sign}{grouped},{decimals}")
    }
}

fn format_money(value: f64, show_plus: bool) -> String {
    format_tr_number(value, 2, show_plus)
}

fn format_percent(value: f64, precision: usize, show_plus: bool) -> String {
    format!("{}%", format_tr_number(value, precision, show_plus))
}

fn format_price(value: f64) -> String {
    let precision = if value.abs() >= 1_000.0 {
        2
    } else if value.abs() >= 1.0 {
        4
    } else if value.abs() >= 0.01 {
        6
    } else {
        8
    };
    let scale = 10_f64.powi(precision as i32);
    let rounded = (value * scale).round() / scale;
    format_tr_number(rounded, precision, false)
}

fn format_quantity(value: &str) -> String {
    value
        .parse::<f64>()
        .map(|parsed| {
            if parsed.abs() >= 1_000.0 {
                let rounded = (parsed * 100.0).round() / 100.0;
                format_tr_number(rounded, 2, false)

            } else {
                let trimmed = format_tr_number(parsed, 8, false)
                    .trim_end_matches('0')
                    .trim_end_matches(',')
                    .to_string();
                if trimmed.is_empty() {
                    "0".to_string()
                } else {
                    trimmed
                }
            }
        })
        .unwrap_or_else(|_| value.to_string())
}

fn spread_percent(book: &BookTicker) -> Option<f64> {
    let bid = book.bid_price.parse::<f64>().ok()?;
    let ask = book.ask_price.parse::<f64>().ok()?;
    let mid = (bid + ask) / 2.0;
    if bid <= 0.0 || ask < bid || mid <= 0.0 {
        None
    } else {
        Some(((ask - bid) / mid) * 100.0)
    }
}

#[derive(Clone, Serialize, Debug)]
struct ActivePosition {
    id: usize,
    symbol: String,
    side: String,
    entry_price: f64,
    current_price: f64,
    stop_loss: f64,
    take_profit: f64,
    best_price: f64,
    peak_pnl_percent: f64,
    leverage: f64,
    margin_usdt: f64,
    lifecycle: PositionLifecycle,
    status: String,
    pnl_percent: f64,
    pnl_usd: f64,
    quantity: String,
}

#[derive(Clone, Serialize, Debug)]
struct ClosedPosition {
    id: usize,
    symbol: String,
    side: String,
    entry_price: f64,
    exit_price: f64,
    status: String,
    pnl_percent: f64,
    pnl_usd: f64,
}

#[derive(Clone, Serialize, Debug)]
struct DailyAccounting {
    date: String,
    starting_balance: f64,
    current_balance: f64,
    total_roi: f64,
    closed_trades_count: usize,
    successful_trades: usize,
}

#[derive(Clone, Default)]
struct PerformanceStats {
    profit_factor: f64,
    expectancy: f64,
}

#[derive(Clone)]
struct AppState {
    positions: Vec<ActivePosition>,
    history: Vec<ClosedPosition>,
    accounting: DailyAccounting,
    next_position_id: usize,
    last_error: String,
    stats: PerformanceStats,
}

type SharedState = Arc<Mutex<AppState>>;

fn init_db() -> Result<Connection, String> {
    let conn = Connection::open("quant_history.db")
        .map_err(|e| format!("SQLite veritabanı açılamadı: {e}"))?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000;",
    )
    .map_err(|e| format!("SQLite PRAGMA ayarlanamadı: {e}"))?;
    conn.execute("CREATE TABLE IF NOT EXISTS closed_trades (id INTEGER PRIMARY KEY, symbol TEXT NOT NULL, side TEXT NOT NULL, entry_price REAL NOT NULL, exit_price REAL NOT NULL, status TEXT NOT NULL, pnl_percent REAL NOT NULL, pnl_usd REAL NOT NULL)", [])
        .map_err(|e| format!("closed_trades şeması oluşturulamadı: {e}"))?;
    conn.execute("CREATE TABLE IF NOT EXISTS active_positions (id INTEGER PRIMARY KEY, symbol TEXT NOT NULL, side TEXT NOT NULL, entry_price REAL NOT NULL, current_price REAL NOT NULL, stop_loss REAL NOT NULL, take_profit REAL NOT NULL, best_price REAL NOT NULL, peak_pnl_percent REAL NOT NULL, leverage REAL NOT NULL, margin_usdt REAL NOT NULL, lifecycle TEXT NOT NULL, status TEXT NOT NULL, pnl_percent REAL NOT NULL, pnl_usd REAL NOT NULL, quantity TEXT NOT NULL)", [])
        .map_err(|e| format!("active_positions şeması oluşturulamadı: {e}"))?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS kv_store (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
        [],
    )
    .map_err(|e| format!("kv_store şeması oluşturulamadı: {e}"))?;
    Ok(conn)
}

fn get_max_closed_id(conn: &Mutex<Connection>) -> usize {
    if let Ok(c) = conn.lock() {
        let max_id: i64 = c
            .query_row(
                "SELECT COALESCE(MAX(id), 0) FROM closed_trades",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);
        max_id as usize
    } else {
        0
    }
}


fn get_max_active_id(conn: &Mutex<Connection>) -> usize {
    if let Ok(c) = conn.lock() {
        let max_id: i64 = c
            .query_row(
                "SELECT COALESCE(MAX(id), 0) FROM active_positions",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);
        max_id as usize
    } else {
        0
    }
}

fn atomic_batch_save(
    conn: &Mutex<Connection>,
    closed_list: &[ClosedPosition],
    active_list: &[ActivePosition],
    acc: &DailyAccounting,
) -> Result<(), String> {
    let mut c = conn.lock().map_err(|e| e.to_string())?;
    let tx = c.transaction().map_err(|e| e.to_string())?;

    for h in closed_list {
        tx.execute(
            "INSERT OR IGNORE INTO closed_trades (id, symbol, side, entry_price, exit_price, status, pnl_percent, pnl_usd) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![h.id as i64, h.symbol.clone(), h.side.clone(), h.entry_price, h.exit_price, h.status.clone(), h.pnl_percent, h.pnl_usd],
        ).map_err(|e| e.to_string())?;
    }

    tx.execute("DELETE FROM active_positions", [])
        .map_err(|e| e.to_string())?;
    for p in active_list {
        let lc_str = match p.lifecycle {
            PositionLifecycle::PendingOpen => "PendingOpen",
            PositionLifecycle::Open => "Open",
            PositionLifecycle::Closed => "Closed",
        };
        tx.execute(
            "INSERT INTO active_positions (id, symbol, side, entry_price, current_price, stop_loss, take_profit, best_price, peak_pnl_percent, leverage, margin_usdt, lifecycle, status, pnl_percent, pnl_usd, quantity) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            params![p.id as i64, p.symbol.clone(), p.side.clone(), p.entry_price, p.current_price, p.stop_loss, p.take_profit, p.best_price, p.peak_pnl_percent, p.leverage, p.margin_usdt, lc_str, p.status.clone(), p.pnl_percent, p.pnl_usd, p.quantity.clone()],
        ).map_err(|e| e.to_string())?;
    }

    tx.execute(
        "INSERT OR REPLACE INTO kv_store (key, value) VALUES ('current_balance', ?1)",
        params![acc.current_balance.to_string()],
    )
    .map_err(|e| e.to_string())?;
    tx.execute(
        "INSERT OR REPLACE INTO kv_store (key, value) VALUES ('starting_balance', ?1)",
        params![acc.starting_balance.to_string()],
    )
    .map_err(|e| e.to_string())?;

    tx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

fn load_active_positions_from_db(conn: &Mutex<Connection>) -> Result<Vec<ActivePosition>, String> {
    let mut positions = Vec::new();
    let c = conn.lock().map_err(|e| e.to_string())?;
    let mut stmt = c.prepare("SELECT id, symbol, side, entry_price, current_price, stop_loss, take_profit, best_price, peak_pnl_percent, leverage, margin_usdt, lifecycle, status, pnl_percent, pnl_usd, quantity FROM active_positions").map_err(|e| e.to_string())?;
    let iter = stmt
        .query_map([], |row| {
            let lc_str: String = row.get(11)?;
            let lifecycle = match lc_str.as_str() {
                "PendingOpen" => PositionLifecycle::PendingOpen,
                "Open" => PositionLifecycle::Open,
                "Closed" => PositionLifecycle::Closed,
                _ => PositionLifecycle::Closed,
            };
            Ok(ActivePosition {
                id: row.get::<_, i64>(0)? as usize,
                symbol: row.get(1)?,
                side: row.get(2)?,
                entry_price: row.get(3)?,
                current_price: row.get(4)?,
                stop_loss: row.get(5)?,
                take_profit: row.get(6)?,
                best_price: row.get(7)?,
                peak_pnl_percent: row.get(8)?,
                leverage: row.get(9)?,
                margin_usdt: row.get(10)?,
                lifecycle,
                status: row.get(12)?,
                pnl_percent: row.get(13)?,
                pnl_usd: row.get(14)?,
                quantity: row.get(15)?,
            })
        })
        .map_err(|e| e.to_string())?;

    for row in iter {
        positions.push(row.map_err(|e| format!("Aktif pozisyon satırı okunamadı: {e}"))?);
    }
    Ok(positions)
}

fn load_history_from_db(
    conn: &Mutex<Connection>,
) -> Result<(Vec<ClosedPosition>, usize, usize), String> {
    let mut history = Vec::new();
    let c = conn.lock().map_err(|e| e.to_string())?;
    let total_count: usize = c
        .query_row("SELECT COUNT(*) FROM closed_trades", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap_or(0) as usize;
    let successful_count: usize = c
        .query_row(
            "SELECT COUNT(*) FROM closed_trades WHERE pnl_percent > 0",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0) as usize;

    let mut stmt = c.prepare("SELECT id, symbol, side, entry_price, exit_price, status, pnl_percent, pnl_usd FROM closed_trades ORDER BY id DESC LIMIT 50").map_err(|e| e.to_string())?;
    let iter = stmt

        .query_map([], |row| {
            Ok(ClosedPosition {
                id: row.get::<_, i64>(0)? as usize,
                symbol: row.get(1)?,
                side: row.get(2)?,
                entry_price: row.get(3)?,
                exit_price: row.get(4)?,
                status: row.get(5)?,
                pnl_percent: row.get(6)?,
                pnl_usd: row.get(7)?,
            })
        })
        .map_err(|e| e.to_string())?;

    for row in iter {
        history.push(row.map_err(|e| format!("Geçmiş işlem satırı okunamadı: {e}"))?);
    }
    history.reverse();
    Ok((history, total_count, successful_count))
}

fn load_performance_stats(conn: &Mutex<Connection>) -> Result<PerformanceStats, String> {
    let c = conn.lock().map_err(|e| e.to_string())?;
    let (gross_profit, gross_loss, trade_count): (f64, f64, i64) = c
        .query_row(
            "SELECT COALESCE(SUM(CASE WHEN pnl_usd > 0 THEN pnl_usd ELSE 0 END), 0.0), COALESCE(SUM(CASE WHEN pnl_usd < 0 THEN -pnl_usd ELSE 0 END), 0.0), COUNT(*) FROM closed_trades",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(|e| e.to_string())?;
    let profit_factor = if gross_loss > 0.0 {
        gross_profit / gross_loss
    } else if gross_profit > 0.0 {
        f64::INFINITY
    } else {
        0.0
    };
    let expectancy = if trade_count > 0 {
        (gross_profit - gross_loss) / trade_count as f64
    } else {
        0.0
    };
    Ok(PerformanceStats {
        profit_factor,
        expectancy,
    })
}

fn load_accounting_from_db(conn: &Mutex<Connection>, default_starting: f64) -> DailyAccounting {
    let mut starting = default_starting;
    let mut ledger_pnl = 0.0;
    let mut date = "unknown".to_string();
    if let Ok(c) = conn.lock() {
        if let Ok(val) = c.query_row(
            "SELECT value FROM kv_store WHERE key='starting_balance'",
            [],
            |row| row.get::<_, String>(0),
        ) {
            if let Ok(parsed) = val.parse::<f64>() {
                starting = parsed;
            }
        }
        ledger_pnl = c
            .query_row(
                "SELECT COALESCE(SUM(pnl_usd), 0.0) FROM closed_trades",
                [],
                |row| row.get::<_, f64>(0),
            )
            .unwrap_or(0.0);
        date = c
            .query_row("SELECT date('now')", [], |row| row.get::<_, String>(0))
            .unwrap_or_else(|_| "unknown".to_string());
    }
    let current = starting + ledger_pnl;
    if let Ok(c) = conn.lock() {
        if let Err(e) = c.execute(
            "INSERT OR REPLACE INTO kv_store (key, value) VALUES ('current_balance', ?1)",
            params![current.to_string()],
        ) {
            eprintln!("Ledger bakiye uzlaştırması kaydedilemedi: {e}");
        }
    }
    let total_roi = if starting > 0.0 {
        ((current - starting) / starting) * 100.0
    } else {
        0.0
    };
    DailyAccounting {
        date,
        starting_balance: starting,
        current_balance: current,
        total_roi,
        closed_trades_count: 0,
        successful_trades: 0,
    }
}

fn fetch_symbol_filters(
    client: &reqwest::blocking::Client,
    config: &Config,
) -> Result<HashMap<String, SymbolFilters>, String> {
    let url = config.futures_url("fapi/v1/exchangeInfo");
    let info = client
        .get(&url)
        .send()
        .and_then(|r| r.error_for_status())
        .map_err(|e| format!("exchangeInfo request failed: {e}"))?
        .json::<ExchangeInfo>()
        .map_err(|e| format!("exchangeInfo parse failed: {e}"))?;

    let mut result = HashMap::new();
    for symbol in info.symbols {
        for filter in symbol.filters {
            if let ExchangeFilter::LotSize {
                min_qty,
                max_qty,
                step_size,
            } = filter
            {
                if let (Ok(min_qty), Ok(max_qty), Ok(step_size)) = (

                    min_qty.parse::<f64>(),
                    max_qty.parse::<f64>(),
                    step_size.parse::<f64>(),
                ) {
                    result.insert(
                        symbol.symbol.clone(),
                        SymbolFilters {
                            min_qty,
                            max_qty,
                            step_size,
                        },
                    );
                }
                break;
            }
        }
    }
    if result.is_empty() {
        Err("exchangeInfo returned no usable LOT_SIZE filters".to_string())
    } else {
        Ok(result)
    }
}

fn calculate_obi(
    client: &reqwest::blocking::Client,
    config: &Config,
    symbol: &str,
) -> Result<f64, String> {
    let url = format!(
        "{}?symbol={}&limit=5",
        config.futures_url("fapi/v1/depth"),
        symbol
    );
    let resp = client
        .get(&url)
        .send()
        .and_then(|r| r.error_for_status())
        .map_err(|e| format!("depth request failed for {symbol}: {e}"))?;
    let depth = resp
        .json::<Depth>()
        .map_err(|e| format!("depth parse failed for {symbol}: {e}"))?;
    let bid_vol: f64 = depth
        .bids
        .iter()
        .filter_map(|b| b.get(1).and_then(|v| v.parse::<f64>().ok()))
        .sum();
    let ask_vol: f64 = depth
        .asks
        .iter()
        .filter_map(|a| a.get(1).and_then(|v| v.parse::<f64>().ok()))
        .sum();
    if bid_vol + ask_vol > 0.0 {
        Ok((bid_vol - ask_vol) / (bid_vol + ask_vol))
    } else {
        Ok(0.0)
    }
}

fn run_engine(config: Config, shared_state: SharedState, db_conn: Arc<Mutex<Connection>>) {
    let client = match reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(10))
        .build()
    {
        Ok(client) => client,
        Err(e) => {
            if let Ok(mut state) = shared_state.lock() {
                state.last_error = format!("HTTP istemcisi oluşturulamadı: {e}");
            }
            return;
        }
    };
    let mut symbol_filters: HashMap<String, SymbolFilters> = HashMap::new();
    let mut obi_samples: HashMap<String, VecDeque<f64>> = HashMap::new();
    let mut symbol_cooldowns: HashMap<String, Instant> = HashMap::new();
    let mut consecutive_losses = 0usize;
    let session_starting_balance = shared_state
        .lock()
        .map(|state| state.accounting.current_balance)
        .unwrap_or(0.0);
    loop {
        if symbol_filters.is_empty() {
            match fetch_symbol_filters(&client, &config) {
                Ok(filters) => symbol_filters = filters,
                Err(e) => {
                    if let Ok(mut state) = shared_state.lock() {
                        state.last_error = e;
                    }
                    thread::sleep(Duration::from_secs(5));
                    continue;
                }
            }
        }
        let url = config.futures_url("fapi/v1/ticker/24hr");
        let tickers_result = client
            .get(&url)
            .send()
            .and_then(|r| r.error_for_status())
            .and_then(|r| r.json::<Vec<Ticker>>());
        match tickers_result {
            Ok(tickers) => {
                let book_map: HashMap<String, BookTicker> = client
                    .get(config.futures_url("fapi/v1/ticker/bookTicker"))
                    .send()
                    .and_then(|r| r.error_for_status())
                    .and_then(|r| r.json::<Vec<BookTicker>>())
                    .map(|books| {
                        books
                            .into_iter()
                            .map(|book| (book.symbol.clone(), book))
                            .collect()
                    })
                    .unwrap_or_default();
                let mut state = match shared_state.lock() {
                    Ok(s) => s.clone(),
                    Err(_) => {
                        thread::sleep(Duration::from_secs(1));
                        continue;
                    }

                };
                let mut newly_closed = Vec::new();
                let mut still_active = Vec::new();
                let mut current_err =
                    String::from("Sistem Kararlı Çalışıyor (Risk Kontrollü Simülasyon Modu)");
                let mut batch_realized_pnl = 0.0;

                for mut pos in state.positions {
                    if pos.lifecycle == PositionLifecycle::PendingOpen {
                        pos.lifecycle = PositionLifecycle::Open;

                        pos.status = format!(
                            "Simüle Edilmiş Emir Gerçekleşti ({}x / ${:.0}) [Gelişmiş Koruma]",
                            pos.side, pos.margin_usdt
                        );
                    }
                    if let Some(t) = tickers.iter().find(|x| x.symbol == pos.symbol) {
                        if let Ok(curr_price) = t.last_price.parse::<f64>() {
                            pos.current_price = curr_price;
                            if pos.side == "LONG" {
                                if curr_price > pos.best_price {
                                    pos.best_price = curr_price;
                                }
                            } else {
                                if curr_price < pos.best_price {
                                    pos.best_price = curr_price;
                                }
                            }
                            let Some(pnl_percent) = calculate_pnl_percent(
                                &pos.side,
                                pos.entry_price,
                                curr_price,
                                pos.leverage,
                            ) else {
                                still_active.push(pos);
                                continue;
                            };
                            pos.pnl_percent = pnl_percent;
                            pos.peak_pnl_percent = pos.peak_pnl_percent.max(pos.pnl_percent);

                            if pos.peak_pnl_percent >= 2.5 && pos.peak_pnl_percent < 5.0 {
                                if let Some(be_stop) = break_even_stop(&pos.side, pos.entry_price) {
                                    if (pos.side == "LONG" && pos.stop_loss < be_stop)
                                        || (pos.side == "SHORT" && pos.stop_loss > be_stop)
                                    {
                                        pos.stop_loss = be_stop;
                                    }
                                }
                            } else if pos.peak_pnl_percent >= 5.0 {
                                if pos.side == "LONG" {
                                    let trailed_sl = pos.best_price * 0.992;
                                    if trailed_sl > pos.stop_loss {
                                        pos.stop_loss = trailed_sl;
                                    }
                                } else {
                                    let trailed_sl = pos.best_price * 1.008;
                                    if trailed_sl < pos.stop_loss {
                                        pos.stop_loss = trailed_sl;
                                    }
                                }
                            }

                            pos.pnl_usd = pos.margin_usdt * (pos.pnl_percent / 100.0);
                            let mut close_reason = None;
                            if pos.side == "LONG" {
                                if curr_price <= pos.stop_loss {
                                    close_reason = Some("SL / Break-Even Hit".to_string());
                                } else if curr_price >= pos.take_profit {
                                    close_reason = Some("TP Hit".to_string());
                                }
                            } else {
                                if curr_price >= pos.stop_loss {
                                    close_reason = Some("SL / Break-Even Hit".to_string());
                                } else if curr_price <= pos.take_profit {
                                    close_reason = Some("TP Hit".to_string());
                                }
                            }

                            if let Some(reason) = close_reason {
                                pos.lifecycle = PositionLifecycle::Closed;
                                batch_realized_pnl += pos.pnl_usd;
                                newly_closed.push(ClosedPosition {
                                    id: pos.id,
                                    symbol: pos.symbol.clone(),
                                    side: pos.side.clone(),
                                    entry_price: pos.entry_price,
                                    exit_price: curr_price,
                                    status: reason,
                                    pnl_percent: pos.pnl_percent,
                                    pnl_usd: pos.pnl_usd,
                                });
                            } else {
                                still_active.push(pos);
                            }
                        } else {
                            still_active.push(pos);
                        }
                    } else {
                        still_active.push(pos);
                    }
                }

                state.accounting.current_balance += batch_realized_pnl;

                let session_drawdown = if session_starting_balance > 0.0 {
                    ((session_starting_balance - state.accounting.current_balance)
                        / session_starting_balance)
                        .max(0.0)
                } else {
                    0.0
                };
                let safety_allows_entries = config.entry_enabled
                    && session_drawdown < config.session_loss_limit
                    && consecutive_losses < config.max_consecutive_losses;
                if !safety_allows_entries {
                    current_err = if !config.entry_enabled {
                        "Yeni girişler ENTRY_ENABLED ile durduruldu".to_string()
                    } else if consecutive_losses >= config.max_consecutive_losses {
                        format!(
                            "Ardışık {consecutive_losses} kayıp sonrası yeni girişler kilitlendi"

                        )
                    } else {
                        format!(
                            "Seans zarar limiti aşıldı: %{:.2}",
                            session_drawdown * 100.0
                        )
                    };
                }

                if safety_allows_entries {
                    let mut candidates: Vec<&Ticker> = tickers
                        .iter()
                        .filter(|ticker| is_tradeable_usdt_symbol(&ticker.symbol))
                        .filter(|ticker| {
                            ticker
                                .quote_volume
                                .parse::<f64>()
                                .is_ok_and(|volume| volume >= config.min_quote_volume)
                                && ticker
                                    .price_change_percent
                                    .parse::<f64>()
                                    .is_ok_and(|change| change.abs() > 2.0)
                        })
                        .collect();
                    candidates.sort_by(|left, right| {
                        let left_volume = left.quote_volume.parse::<f64>().unwrap_or(0.0);
                        let right_volume = right.quote_volume.parse::<f64>().unwrap_or(0.0);
                        right_volume.total_cmp(&left_volume)
                    });
                    candidates.truncate(config.max_signal_candidates);

                    for t in candidates {
                        let (Ok(change), Ok(vol), Ok(price)) = (
                            t.price_change_percent.parse::<f64>(),
                            t.quote_volume.parse::<f64>(),
                            t.last_price.parse::<f64>(),
                        ) else {
                            continue;
                        };
                        if price <= 0.0 || vol < config.min_quote_volume || change.abs() <= 2.0 {
                            continue;
                        }
                        let Some(book) = book_map.get(&t.symbol) else {
                            continue;
                        };
                        let Some(spread) = spread_percent(book) else {
                            continue;
                        };
                        if spread > config.max_spread_percent {
                            continue;
                        }
                        let Ok(obi) = calculate_obi(&client, &config, &t.symbol) else {
                            continue;
                        };
                        let samples = obi_samples.entry(t.symbol.clone()).or_default();
                        samples.push_back(obi);
                        while samples.len() > config.obi_confirmation_samples {
                            samples.pop_front();
                        }
                        if samples.len() < config.obi_confirmation_samples {
                            continue;
                        }
                        let long_confirmed =
                            change > 2.0 && samples.iter().all(|value| *value >= 0.20);
                        let short_confirmed =
                            change < -2.0 && samples.iter().all(|value| *value <= -0.20);
                        let (side, leverage) = if long_confirmed {
                            ("LONG", 3.0)
                        } else if short_confirmed {
                            ("SHORT", 3.0)
                        } else {
                            continue;
                        };
                        let already_active = still_active.iter().any(|p| p.symbol == t.symbol);
                        let cooling_down = symbol_cooldowns
                            .get(&t.symbol)
                            .is_some_and(|closed_at| closed_at.elapsed() < config.cooldown);
                        if already_active
                            || cooling_down
                            || still_active.len() >= config.max_positions
                        {
                            continue;
                        }
                        let same_side_count = still_active
                            .iter()
                            .filter(|position| position.side == side)
                            .count();
                        if same_side_count >= config.max_same_side_positions {
                            continue;
                        }
                        let committed_margin: f64 = still_active
                            .iter()
                            .map(|position| position.margin_usdt)
                            .sum();
                        let free_margin = state.accounting.current_balance - committed_margin;
                        if free_margin <= 0.0 {
                            continue;
                        }
                        let (stop_loss, take_profit) = if side == "LONG" {
                            (price * 0.985, price * 1.035)
                        } else {
                            (price * 1.015, price * 0.965)
                        };
                        let Some(filters) = symbol_filters.get(&t.symbol) else {
                            continue;
                        };
                        let Some((quantity, margin_usdt, new_risk)) = calculate_position_size(
                            state.accounting.current_balance,
                            price,
                            stop_loss,
                            leverage,
                            config.risk_per_trade,
                            filters,
                        ) else {
                            continue;
                        };
                        let portfolio_risk: f64 = still_active.iter().map(position_risk).sum();
                        let max_risk_usd =
                            state.accounting.current_balance * config.max_portfolio_risk;
                        if portfolio_risk + new_risk > max_risk_usd || margin_usdt > free_margin {

                            continue;
                        }
                        let precision = filters
                            .step_size
                            .to_string()
                            .split('.')
                            .nth(1)
                            .map_or(0, |value| value.trim_end_matches('0').len());
                        let quantity_text = format!("{:.*}", precision, quantity);
                        let current_id = state.next_position_id;
                        state.next_position_id += 1;
                        still_active.push(ActivePosition {
                            id: current_id,
                            symbol: t.symbol.clone(),
                            side: side.to_string(),
                            entry_price: price,
                            current_price: price,
                            stop_loss,
                            take_profit,
                            best_price: price,
                            peak_pnl_percent: -FEE_RATE_PERCENT * leverage,
                            leverage,
                            margin_usdt,
                            lifecycle: PositionLifecycle::PendingOpen,
                            status: format!(
                                "Risk kontrollü simüle emir: {} / risk USD {:.2}",
                                side, new_risk
                            ),
                            pnl_percent: -FEE_RATE_PERCENT * leverage,
                            pnl_usd: -(margin_usdt * FEE_RATE_PERCENT * leverage / 100.0),
                            quantity: quantity_text,
                        });
                    }
                }

                if state.accounting.starting_balance > 0.0 {
                    state.accounting.total_roi = ((state.accounting.current_balance
                        - state.accounting.starting_balance)
                        / state.accounting.starting_balance)
                        * 100.0;
                }

                if let Err(e) =
                    atomic_batch_save(&db_conn, &newly_closed, &still_active, &state.accounting)
                {
                    if let Ok(mut locked_state) = shared_state.lock() {
                        locked_state.last_error =
                            format!("DB Batch Transaction Hatası: {e}; durum ilerletilmedi");
                    }
                    thread::sleep(Duration::from_secs(1));
                    continue;
                }

                for closed in &newly_closed {
                    symbol_cooldowns.insert(closed.symbol.clone(), Instant::now());
                    if closed.pnl_usd < 0.0 {
                        consecutive_losses += 1;
                    } else {
                        consecutive_losses = 0;
                    }
                }

                state.positions = still_active;

                if let Ok((hist, tot_count, succ_count)) = load_history_from_db(&db_conn) {
                    state.history = hist;
                    state.accounting.closed_trades_count = tot_count;
                    state.accounting.successful_trades = succ_count;
                }
                if let Ok(stats) = load_performance_stats(&db_conn) {
                    state.stats = stats;
                }

                state.last_error = current_err;
                if let Ok(mut locked_state) = shared_state.lock() {
                    *locked_state = state;
                }
            }
            Err(e) => {
                if let Ok(mut state) = shared_state.lock() {
                    state.last_error = format!("Ağ Hatası: {}", e);
                }
            }
        }
        thread::sleep(Duration::from_secs(3));
    }
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn render_dashboard(state: &AppState) -> String {
    let roi_class = if state.accounting.total_roi >= 0.0 {
        "positive"
    } else {
        "negative"
    };
    let win_rate = if state.accounting.closed_trades_count > 0 {
        state.accounting.successful_trades as f64 / state.accounting.closed_trades_count as f64
            * 100.0
    } else {
        0.0
    };
    let used_margin: f64 = state
        .positions
        .iter()
        .map(|position| position.margin_usdt)
        .sum();
    let open_pnl: f64 = state
        .positions
        .iter()
        .map(|position| position.pnl_usd)
        .sum();
    let status_text = if state.last_error.contains("ENTRY_ENABLED") {

        "Yeni işlemler kapalı"
    } else {
        &state.last_error
    };
    let status_class = if state.last_error.contains("Hata") {
        "status danger"
    } else if state.last_error.contains("ENTRY_ENABLED") {
        "status warning"
    } else {
        "status healthy"
    };
    let balance_text = format_money(state.accounting.current_balance, false);
    let roi_text = format_percent(state.accounting.total_roi, 2, true);
    let open_pnl_text = format_money(open_pnl, true);
    let used_margin_text = format_money(used_margin, false);
    let win_rate_text = format_percent(win_rate, 1, false);
    let profit_factor_text = format_tr_number(state.stats.profit_factor, 2, false);

    let mut html = format!(
        r#"<!doctype html>
<html lang="tr"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="3"><title>Quant Futures</title>
<style>
:root{{--bg:#05070b;--glass:#111722d9;--glass2:#171f2d;--line:#293346;--text:#f4f7fb;--muted:#8290a7;--gold:#f8c246;--green:#14e6a0;--red:#ff4d6d;--blue:#5ba8ff}}
*{{box-sizing:border-box}}html{{min-height:100%}}body{{margin:0;min-height:100vh;background:#05070b;color:var(--text);font-family:Inter,ui-sans-serif,system-ui,-apple-system,Segoe UI,sans-serif;overflow-x:hidden}}
body:before{{content:"";position:fixed;inset:0;z-index:-2;background:radial-gradient(circle at 8% 0%,#17315970 0,transparent 32%),radial-gradient(circle at 92% 8%,#5b421f54 0,transparent 28%),linear-gradient(145deg,#05070b 0%,#0a1019 48%,#05070b 100%)}}
body:after{{content:"";position:fixed;inset:0;z-index:-1;opacity:.22;background-image:linear-gradient(#ffffff08 1px,transparent 1px),linear-gradient(90deg,#ffffff08 1px,transparent 1px);background-size:42px 42px;mask-image:linear-gradient(to bottom,#000,transparent 78%)}}
.shell{{max-width:1540px;margin:auto;padding:28px 30px 44px;perspective:1400px}}.topbar{{display:flex;align-items:center;justify-content:space-between;gap:18px;margin-bottom:24px;padding:14px 16px;border:1px solid #ffffff12;border-radius:18px;background:linear-gradient(135deg,#131a26dc,#090d14c7);box-shadow:0 18px 55px #0008,inset 0 1px #ffffff10;backdrop-filter:blur(18px)}}
.brand{{display:flex;align-items:center;gap:14px}}.logo{{position:relative;width:46px;height:46px;border-radius:14px;background:linear-gradient(145deg,#ffe27a,#e7a900);color:#111;display:grid;place-items:center;font-weight:950;font-size:21px;box-shadow:0 12px 24px #0008,0 0 28px #f8c24638,inset 0 2px 2px #fff9,inset 0 -3px 5px #9e680077;transform:rotate(-3deg)}}
.brand h1{{font-size:21px;margin:0;letter-spacing:.25px;text-shadow:0 2px 16px #000}}.brand p{{margin:4px 0 0;color:var(--muted);font-size:12px}}.top-actions{{display:flex;gap:10px;align-items:center}}
.mode{{font-size:10px;color:#161000;background:linear-gradient(180deg,#ffd966,#e9aa08);font-weight:900;padding:8px 12px;border-radius:9px;letter-spacing:1px;box-shadow:0 7px 16px #0007,inset 0 1px #fff8}}
.status{{display:flex;align-items:center;gap:9px;padding:11px 14px;border:1px solid #ffffff12;background:#0b111bd9;border-radius:11px;color:#9eabc0;font-size:12px;box-shadow:inset 0 1px #ffffff08,0 8px 20px #0005}}
.status:before{{content:"";width:9px;height:9px;border-radius:50%;background:var(--green);box-shadow:0 0 8px var(--green),0 0 18px var(--green)}}.status.warning:before{{background:var(--gold);box-shadow:0 0 8px var(--gold),0 0 18px var(--gold)}}.status.danger:before{{background:var(--red);box-shadow:0 0 8px var(--red),0 0 18px var(--red)}}
.stats{{display:grid;grid-template-columns:repeat(6,minmax(130px,1fr));gap:14px;margin-bottom:30px}}.stat{{position:relative;overflow:hidden;background:linear-gradient(145deg,#192231e8,#0d131de8);border:1px solid #ffffff13;border-radius:16px;padding:17px 16px 18px;box-shadow:0 16px 32px #0007,inset 0 1px #ffffff10;transform:translateZ(0);transition:.25s ease}}
.stat:before{{content:"";position:absolute;inset:0;background:linear-gradient(115deg,#ffffff0d,transparent 34%);pointer-events:none}}.stat:after{{content:"";position:absolute;left:15px;right:15px;bottom:0;height:2px;background:linear-gradient(90deg,transparent,var(--blue),transparent);opacity:.55}}.stat:hover{{transform:translateY(-5px) rotateX(3deg);border-color:#5ba8ff55;box-shadow:0 24px 42px #0009,0 0 22px #5ba8ff18,inset 0 1px #ffffff18}}
.stat span{{display:block;color:#8696ad;font-size:10px;text-transform:uppercase;letter-spacing:1px;margin-bottom:10px}}.stat strong{{font-size:20px;font-variant-numeric:tabular-nums;text-shadow:0 2px 14px #000}}

.positive{{color:var(--green)!important;text-shadow:0 0 18px #14e6a033!important}}.negative{{color:var(--red)!important;text-shadow:0 0 18px #ff4d6d33!important}}.section-head{{display:flex;align-items:end;justify-content:space-between;margin:24px 2px 13px}}.section-head h2{{font-size:16px;margin:0;letter-spacing:.2px}}.section-head h2:after{{content:"";display:block;width:42px;height:3px;margin-top:8px;border-radius:3px;background:linear-gradient(90deg,var(--gold),transparent);box-shadow:0 0 12px #f8c24655}}.section-head span{{font-size:11px;color:var(--muted)}}
.position-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(350px,1fr));gap:16px}}.position{{position:relative;overflow:hidden;background:linear-gradient(145deg,#182230f2,#0b111af2);border:1px solid #ffffff14;border-radius:18px;padding:19px;box-shadow:0 20px 38px #0009,inset 0 1px #ffffff12;transform-style:preserve-3d;transition:.25s ease}}
.position:before{{content:"";position:absolute;inset:0;pointer-events:none;background:radial-gradient(circle at 90% 0%,var(--glow),transparent 40%),linear-gradient(115deg,#ffffff0c,transparent 35%)}}.position:after{{content:"";position:absolute;left:0;right:0;bottom:0;height:4px;background:linear-gradient(90deg,transparent,var(--accent),transparent);box-shadow:0 0 18px var(--accent)}}.position:hover{{transform:translateY(-6px) rotateX(2deg) rotateY(-1deg);box-shadow:0 30px 50px #000b,0 0 28px var(--glow),inset 0 1px #ffffff18}}.position.long{{--accent:var(--green);--glow:#14e6a018;border-color:#14e6a044}}.position.short{{--accent:var(--red);--glow:#ff4d6d18;border-color:#ff4d6d44}}
.position-top{{position:relative;display:flex;justify-content:space-between;align-items:start;margin-bottom:19px}}.symbol{{font-size:20px;font-weight:900;letter-spacing:.3px}}.side{{font-size:10px;font-weight:950;padding:6px 10px;border:1px solid currentColor;border-radius:7px;box-shadow:0 6px 16px #0007}}
.long .side{{color:var(--green);background:#14e6a012}}.short .side{{color:var(--red);background:#ff4d6d12}}.position-meta{{color:var(--muted);font-size:11px;margin-top:5px}}.pnl{{text-align:right}}.pnl strong{{display:block;font-size:23px;font-variant-numeric:tabular-nums}}.pnl small{{color:var(--muted)}}.long .pnl strong{{color:var(--green);text-shadow:0 0 18px #14e6a040}}.short .pnl strong{{color:var(--red);text-shadow:0 0 18px #ff4d6d40}}
.price-grid{{position:relative;display:grid;grid-template-columns:repeat(3,1fr);gap:9px;padding-top:14px;border-top:1px solid #ffffff10}}.price-grid div{{padding:9px 10px;border-radius:10px;background:#05091070;box-shadow:inset 0 1px 5px #0008,inset 0 1px #ffffff08}}.price-grid span{{color:var(--muted);font-size:9px;display:block;margin-bottom:5px;text-transform:uppercase;letter-spacing:.65px}}.price-grid b{{font-size:13px;font-variant-numeric:tabular-nums}}
.risk-row{{position:relative;display:flex;justify-content:space-between;gap:8px;margin-top:13px;padding-top:12px;border-top:1px solid #ffffff10;font-size:11px;color:var(--muted)}}.risk-row b{{color:var(--text)}}.empty{{grid-column:1/-1;position:relative;overflow:hidden;background:linear-gradient(145deg,#111925cc,#090e16cc);border:1px solid #ffffff12;border-radius:18px;padding:48px;text-align:center;color:var(--muted);box-shadow:0 20px 42px #0008,inset 0 1px #ffffff0c}}.empty:before{{content:"◈";display:block;color:var(--gold);font-size:28px;margin-bottom:10px;text-shadow:0 0 22px #f8c24688}}
.table-wrap{{overflow:auto;border:1px solid #ffffff12;border-radius:18px;background:linear-gradient(145deg,#131b27e8,#090e16e8);box-shadow:0 20px 42px #0008,inset 0 1px #ffffff0d}}table{{width:100%;border-collapse:collapse;min-width:800px}}th{{font-size:9px;text-transform:uppercase;letter-spacing:.9px;color:var(--muted);text-align:left;background:#ffffff05}}th,td{{padding:13px 15px;border-bottom:1px solid #ffffff0b}}td{{font-size:12px;font-variant-numeric:tabular-nums}}tbody tr{{transition:.18s}}tbody tr:hover{{background:#ffffff05}}tr:last-child td{{border-bottom:0}}.side-text.long{{color:var(--green);font-weight:850}}.side-text.short{{color:var(--red);font-weight:850}}footer{{color:#647087;font-size:9px;text-align:center;margin-top:25px}}
@media(max-width:1050px){{.stats{{grid-template-columns:repeat(3,1fr)}}}}@media(max-width:700px){{.shell{{padding:14px}}.topbar{{align-items:flex-start;flex-direction:column}}.top-actions{{width:100%;flex-wrap:wrap}}.status{{flex:1}}.stats{{grid-template-columns:repeat(2,1fr);gap:10px}}.stat{{padding:14px}}.position-grid{{grid-template-columns:1fr}}.risk-row{{flex-wrap:wrap}}}}
</style></head><body><main class="shell">
<header class="topbar"><div class="brand"><div class="logo">Q</div><div><h1>Quant Futures</h1><p>Risk kontrollü piyasa simülasyonu</p></div></div>
<div class="top-actions"><div class="mode">SIMÜLASYON</div><div class="{status_class}">{status}</div></div></header>
<section class="stats">
<div class="stat"><span>Toplam Bakiye</span><strong>{balance} USDT</strong></div>
<div class="stat"><span>Toplam Getiri</span><strong class="{roi_class}">{roi}</strong></div>
<div class="stat"><span>Açık Kâr / Zarar</span><strong class="{open_class}">{open_pnl} USDT</strong></div>
<div class="stat"><span>Kullanılan Marjin</span><strong>{used_margin} USDT</strong></div>
<div class="stat"><span>Başarı Oranı</span><strong>{win_rate}</strong></div>
<div class="stat"><span>Kâr Faktörü</span><strong>{pf}</strong></div></section>
<div class="section-head"><h2>Açık Pozisyonlar</h2><span>{position_count} pozisyon</span></div><section class="position-grid">"#,
        status_class = status_class,
        status = escape_html(status_text),
        balance = balance_text,
        roi_class = roi_class,
        roi = roi_text,
        open_class = if open_pnl >= 0.0 {
            "positive"
        } else {
            "negative"
        },
        open_pnl = open_pnl_text,
        used_margin = used_margin_text,
        win_rate = win_rate_text,
        pf = profit_factor_text,
        position_count = state.positions.len()
    );

    if state.positions.is_empty() {
        html.push_str(r#"<div class="empty">Henüz açık pozisyon yok.</div>"#);
    } else {
        for position in &state.positions {
            let side_class = if position.side == "LONG" {
                "long"
            } else {
                "short"
            };
            let notional = position.margin_usdt * position.leverage;
            let leverage_text = format_tr_number(position.leverage, 0, false);
            let notional_text = format_money(notional, false);
            let pnl_usd_text = format_money(position.pnl_usd, true);
            let pnl_percent_text = format_percent(position.pnl_percent, 2, true);
            let margin_text = format_money(position.margin_usdt, false);
            let pnl_class = if position.pnl_usd >= 0.0 {
                "positive"
            } else {
                "negative"
            };
            html.push_str(&format!(
                r#"<article class="position {side_class}"><div class="position-top">
<div><div class="symbol">{symbol}</div><div class="position-meta">#{id} · {leverage}x kaldıraç · {notional} USDT pozisyon</div></div>
<div style="display:flex;gap:12px;align-items:start"><span class="side">{side}</span><div class="pnl"><strong class="{pnl_class}">{pnl_usd} USDT</strong><small>{pnl_percent}</small></div></div></div>
<div class="price-grid"><div><span>Giriş</span><b>{entry}</b></div><div><span>Anlık</span><b>{current}</b></div><div><span>Miktar</span><b>{quantity}</b></div></div>
<div class="risk-row"><span>Marjin <b>{margin} USDT</b></span><span>Stop <b>{stop}</b></span><span>Hedef <b>{take_profit}</b></span></div></article>"#,
                side_class=side_class,symbol=escape_html(&position.symbol),id=position.id,leverage=leverage_text,notional=notional_text,pnl_class=pnl_class,
                side=escape_html(&position.side),pnl_usd=pnl_usd_text,pnl_percent=pnl_percent_text,entry=format_price(position.entry_price),
                current=format_price(position.current_price),quantity=format_quantity(&position.quantity),margin=margin_text,
                stop=format_price(position.stop_loss),take_profit=format_price(position.take_profit)
            ));
        }
    }

    html.push_str(&format!(
        r#"</section><div class="section-head"><h2>İşlem Geçmişi</h2><span>{closed} kapanan · İşlem başı {expectancy} USDT</span></div>
<div class="table-wrap"><table><thead><tr><th>ID</th><th>Parite</th><th>Yön</th><th>Giriş</th><th>Çıkış</th><th>Sonuç</th><th>PnL</th><th>PnL &#36;</th></tr></thead><tbody>"#,
        closed=state.accounting.closed_trades_count,expectancy=format_money(state.stats.expectancy, true)
    ));
    if state.history.is_empty() {
        html.push_str(r#"<tr><td colspan="8" style="text-align:center;color:var(--muted)">Henüz kapanan işlem yok.</td></tr>"#);
    } else {
        for trade in &state.history {
            let side_class = if trade.side == "LONG" {
                "long"
            } else {

                "short"
            };
            let pnl_class = if trade.pnl_usd >= 0.0 {
                "positive"
            } else {
                "negative"
            };
            html.push_str(&format!(
                r#"<tr><td>#{id}</td><td><b>{symbol}</b></td><td class="side-text {side_class}">{side}</td><td>{entry}</td><td>{exit}</td><td>{status}</td><td class="{pnl_class}">{pnl_percent}</td><td class="{pnl_class}">{pnl_usd} USDT</td></tr>"#,
                id=trade.id,symbol=escape_html(&trade.symbol),side_class=side_class,side=escape_html(&trade.side),
                entry=format_price(trade.entry_price),exit=format_price(trade.exit_price),status=escape_html(&trade.status),
                pnl_class=pnl_class,pnl_percent=format_percent(trade.pnl_percent, 2, true),pnl_usd=format_money(trade.pnl_usd, true)
            ));
        }
    }
    html.push_str("</tbody></table></div><footer>Veriler 3 saniyede bir yenilenir · Simülasyon sonuçları gerçek piyasa performansı garantisi değildir.</footer></main></body></html>");
    html
}

fn main() {
    dotenv().ok();
    let config = Config {
        base_url: env::var("BINANCE_BASE_URL")
            .unwrap_or_else(|_| "https://testnet.binancefuture.com".into()),
        entry_enabled: env::var("ENTRY_ENABLED")
            .map(|value| value.eq_ignore_ascii_case("true"))
            .unwrap_or(false),
        max_positions: env::var("MAX_POSITIONS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(10)
            .clamp(1, 10),
        max_same_side_positions: env::var("MAX_SAME_SIDE_POSITIONS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(3)
            .clamp(1, 5),
        max_signal_candidates: env::var("MAX_SIGNAL_CANDIDATES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(20)
            .clamp(5, 50),
        risk_per_trade: env::var("RISK_PER_TRADE")
            .ok()
            .and_then(|value| value.parse().ok())
            .filter(|value| (0.0..=0.02).contains(value))
            .unwrap_or(0.005),
        max_portfolio_risk: env::var("MAX_PORTFOLIO_RISK")
            .ok()
            .and_then(|value| value.parse().ok())
            .filter(|value| (0.0..=0.10).contains(value))
            .unwrap_or(0.02),
        session_loss_limit: env::var("SESSION_LOSS_LIMIT")
            .ok()
            .and_then(|value| value.parse().ok())
            .filter(|value| (0.0..=0.10).contains(value))
            .unwrap_or(0.02),
        max_consecutive_losses: env::var("MAX_CONSECUTIVE_LOSSES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(3)
            .clamp(1, 10),
        cooldown: Duration::from_secs(
            env::var("SYMBOL_COOLDOWN_SECONDS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(3600),
        ),
        min_quote_volume: env::var("MIN_QUOTE_VOLUME")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(20_000_000.0),
        max_spread_percent: env::var("MAX_SPREAD_PERCENT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(0.10),
        obi_confirmation_samples: env::var("OBI_CONFIRMATION_SAMPLES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(3)
            .clamp(1, 10),
    };
    let db = match init_db() {
        Ok(db) => db,
        Err(e) => {
            eprintln!("{e}");
            return;
        }
    };
    let db_conn = Arc::new(Mutex::new(db));
    let next_id = get_max_closed_id(&db_conn).max(get_max_active_id(&db_conn)) + 1;

    let initial_positions = load_active_positions_from_db(&db_conn).unwrap_or_default();
    let (initial_history, total_closed, total_succ) =
        load_history_from_db(&db_conn).unwrap_or_default();
    let initial_balance = env::var("INITIAL_BALANCE_USDT")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(10_000.0);
    let initial_accounting = load_accounting_from_db(&db_conn, initial_balance);
    let initial_stats = load_performance_stats(&db_conn).unwrap_or_default();

    let shared_state: SharedState = Arc::new(Mutex::new(AppState {
        positions: initial_positions,
        history: initial_history,
        accounting: DailyAccounting {
            date: initial_accounting.date,
            starting_balance: initial_accounting.starting_balance,
            current_balance: initial_accounting.current_balance,
            total_roi: initial_accounting.total_roi,
            closed_trades_count: total_closed,
            successful_trades: total_succ,
        },
        next_position_id: next_id,
        last_error: "Sistem Başlatıldı".to_string(),
        stats: initial_stats,
    }));

    let s_clone = Arc::clone(&shared_state);

    let db_clone = Arc::clone(&db_conn);
    let cfg_clone = config.clone();
    thread::spawn(move || {
        run_engine(cfg_clone, s_clone, db_clone);
    });

    let panel_bind = env::var("PANEL_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
    let server = match Server::http(&panel_bind) {
        Ok(server) => server,
        Err(e) => {
            eprintln!("Panel sunucusu başlatılamadı ({panel_bind}): {e}");
            return;
        }
    };
    println!("🚀 Quant paneli {panel_bind} adresinde yayında");

    for request in server.incoming_requests() {
        let state = match shared_state.lock() {
            Ok(state) => state.clone(),
            Err(_) => continue,
        };
        let html = render_dashboard(&state);
        let content_type = match tiny_http::Header::from_bytes(
            &b"Content-Type"[..],
            &b"text/html; charset=utf-8"[..],
        ) {
            Ok(header) => header,
            Err(_) => continue,
        };
        let response = Response::from_string(html).with_header(content_type);
        if let Err(error) = request.respond(response) {
            eprintln!("Panel yanıtı gönderilemedi: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pnl_is_symmetric_for_long_and_short() {
        let long = calculate_pnl_percent("LONG", 100.0, 101.0, 3.0).unwrap();
        let short = calculate_pnl_percent("SHORT", 100.0, 99.0, 3.0).unwrap();
        assert!((long - short).abs() < 1e-9);
        assert!((long - 2.76).abs() < 1e-9);
    }

    #[test]
    fn break_even_covers_fee_and_slippage_buffer() {
        assert!(break_even_stop("LONG", 100.0).unwrap() > 100.0);
        assert!(break_even_stop("SHORT", 100.0).unwrap() < 100.0);
    }

    #[test]
    fn quantity_respects_exchange_filters() {
        let filters = SymbolFilters {
            min_qty: 0.001,

            max_qty: 100.0,
            step_size: 0.001,
        };
        assert_eq!(quantize_quantity(1.23456, &filters), Some(1.234));
        assert_eq!(quantize_quantity(0.0005, &filters), None);
    }

    #[test]
    fn risk_sizing_caps_margin_and_loss() {
        let filters = SymbolFilters {
            min_qty: 0.001,
            max_qty: 100_000.0,
            step_size: 0.001,
        };
        let (quantity, margin, risk) =
            calculate_position_size(1_000.0, 100.0, 98.5, 3.0, 0.005, &filters).unwrap();
        assert!(margin <= MAX_POSITION_MARGIN_USDT);
        assert!(risk <= 5.0 + 1e-9);
        assert!(quantity > 0.0);
    }

    #[test]
    fn spread_filter_rejects_invalid_books() {
        let liquid = BookTicker {
            symbol: "TESTUSDT".to_string(),
            bid_price: "99.95".to_string(),
            ask_price: "100.05".to_string(),
        };
        let crossed = BookTicker {
            symbol: "TESTUSDT".to_string(),
            bid_price: "101".to_string(),
            ask_price: "100".to_string(),
        };
        assert!((spread_percent(&liquid).unwrap() - 0.1).abs() < 1e-9);
        assert_eq!(spread_percent(&crossed), None);
    }

    #[test]
    fn display_numbers_use_bounded_precision() {
        assert_eq!(format_price(12_345.678_9), "12.345,68");
        assert_eq!(format_price(12.345678), "12,3457");
        assert_eq!(format_price(0.123456789), "0,123457");
        assert_eq!(format_price(0.00123456789), "0,00123457");
        assert_eq!(format_quantity("199468.08500000"), "199.468,09");
        assert_eq!(format_money(10_000.0, false), "10.000,00");
        assert_eq!(format_money(-1_234.5, true), "-1.234,50");
        assert_eq!(format_percent(12.5, 2, true), "+12,50%");
    }

    #[test]
    fn symbol_filter_rejects_testnet_noise() {
        assert!(is_tradeable_usdt_symbol("BTCUSDT"));
        assert!(is_tradeable_usdt_symbol("1000PEPEUSDT"));
        assert!(!is_tradeable_usdt_symbol("我踏马来了USDT"));
        assert!(!is_tradeable_usdt_symbol("BTC-USDT"));
        assert!(!is_tradeable_usdt_symbol("USDT"));
        assert!(!is_tradeable_usdt_symbol("btcusdt"));
    }
}
