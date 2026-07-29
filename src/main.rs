use dotenvy::dotenv;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::env;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tiny_http::{Response, Server};

const FEE_RATE_PERCENT: f64 = 0.04;
const BREAKEVEN_BUFFER_RATE: f64 = 0.001;
const STRATEGY_VERSION: &str = "MTF_V3";

fn unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}

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

fn calculate_trade_pnl(side: &str, entry: f64, exit: f64, quantity: f64) -> Option<f64> {
    if entry <= 0.0 || exit <= 0.0 || quantity <= 0.0 {
        return None;
    }
    let gross = match side {
        "LONG" => (exit - entry) * quantity,
        "SHORT" => (entry - exit) * quantity,
        _ => return None,
    };
    let fees = (entry + exit) * quantity * (FEE_RATE_PERCENT / 100.0);
    Some(gross - fees)
}

fn break_even_stop(side: &str, entry: f64) -> Option<f64> {
    match side {
        "LONG" => Some(entry * (1.0 + BREAKEVEN_BUFFER_RATE)),
        "SHORT" => Some(entry * (1.0 - BREAKEVEN_BUFFER_RATE)),
        _ => None,
    }
}

fn protected_profit_stop(
    side: &str,
    entry: f64,
    leverage: f64,
    target_margin_percent: f64,
) -> Option<f64> {
    if entry <= 0.0 || leverage <= 0.0 {
        return None;
    }
    let round_trip_fee_percent = FEE_RATE_PERCENT * leverage * 2.0;
    let raw_move = (target_margin_percent + round_trip_fee_percent) / (leverage * 100.0);
    match side {
        "LONG" => Some(entry * (1.0 + raw_move)),
        "SHORT" => Some(entry * (1.0 - raw_move)),
        _ => None,
    }
}

fn calculate_position_size(
    balance: f64,
    entry: f64,
    stop: f64,
    leverage: f64,
    risk_fraction: f64,
    max_allocation_fraction: f64,
    filters: &SymbolFilters,
) -> Option<(f64, f64, f64)> {
    if balance <= 0.0
        || entry <= 0.0
        || leverage <= 0.0
        || !(0.0..=1.0).contains(&risk_fraction)
        || !(0.0..=1.0).contains(&max_allocation_fraction)
    {
        return None;
    }
    let stop_distance = (entry - stop).abs();

    if stop_distance <= 0.0 {
        return None;
    }
    let risk_qty = (balance * risk_fraction) / stop_distance;
    let margin_capped_qty = (balance * max_allocation_fraction * leverage) / entry;
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

fn partial_quantity(
    initial_quantity: f64,
    remaining_quantity: f64,
    fraction: f64,
    filters: &SymbolFilters,
) -> Option<f64> {
    let requested = (initial_quantity * fraction).min(remaining_quantity);
    quantize_quantity(requested, filters)
}

fn exchange_quantity_text(quantity: f64, filters: &SymbolFilters) -> String {
    let precision = filters
        .step_size
        .to_string()
        .split('.')
        .nth(1)
        .map_or(0, |value| value.trim_end_matches('0').len());
    format!("{quantity:.precision$}")
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
    max_trade_allocation: f64,
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
    initial_quantity: String,
    tp1_price: f64,
    tp2_price: f64,
    tp_stage: u8,
    realized_pnl_usd: f64,
    atr: f64,
    opened_at: i64,
    strategy_version: String,
    entry_ema_fast: f64,
    entry_ema_slow: f64,
    entry_rsi: f64,
    entry_adx: f64,
    entry_obi: f64,
    entry_spread: f64,
    market_regime: String,
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
    max_pnl_percent: f64,
    exit_stage: String,
    opened_at: i64,
    closed_at: i64,
    strategy_version: String,
    entry_rsi: f64,
    entry_adx: f64,
    entry_obi: f64,
    market_regime: String,
}

#[derive(Clone, Debug)]
struct FastIndicators {
    ema_fast: f64,
    ema_slow: f64,
    rsi: f64,
    atr: f64,
    adx: f64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum MarketRegime {
    Bull,
    Bear,
    Sideways,
}

impl MarketRegime {
    fn as_str(self) -> &'static str {
        match self {
            Self::Bull => "BULL",
            Self::Bear => "BEAR",
            Self::Sideways => "SIDEWAYS",
        }
    }
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
    strategy_trade_count: usize,
    strategy_pnl: f64,
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
    conn.execute("CREATE TABLE IF NOT EXISTS closed_trades (id INTEGER PRIMARY KEY, symbol TEXT NOT NULL, side TEXT NOT NULL, entry_price REAL NOT NULL, exit_price REAL NOT NULL, status TEXT NOT NULL, pnl_percent REAL NOT NULL, pnl_usd REAL NOT NULL, max_pnl_percent REAL NOT NULL DEFAULT 0, exit_stage TEXT NOT NULL DEFAULT 'LEGACY')", [])
        .map_err(|e| format!("closed_trades şeması oluşturulamadı: {e}"))?;
    conn.execute("CREATE TABLE IF NOT EXISTS active_positions (id INTEGER PRIMARY KEY, symbol TEXT NOT NULL, side TEXT NOT NULL, entry_price REAL NOT NULL, current_price REAL NOT NULL, stop_loss REAL NOT NULL, take_profit REAL NOT NULL, best_price REAL NOT NULL, peak_pnl_percent REAL NOT NULL, leverage REAL NOT NULL, margin_usdt REAL NOT NULL, lifecycle TEXT NOT NULL, status TEXT NOT NULL, pnl_percent REAL NOT NULL, pnl_usd REAL NOT NULL, quantity TEXT NOT NULL, initial_quantity TEXT NOT NULL DEFAULT '0', tp1_price REAL NOT NULL DEFAULT 0, tp2_price REAL NOT NULL DEFAULT 0, tp_stage INTEGER NOT NULL DEFAULT 0, realized_pnl_usd REAL NOT NULL DEFAULT 0, atr REAL NOT NULL DEFAULT 0)", [])
        .map_err(|e| format!("active_positions şeması oluşturulamadı: {e}"))?;
    for migration in [
        "ALTER TABLE closed_trades ADD COLUMN max_pnl_percent REAL NOT NULL DEFAULT 0",
        "ALTER TABLE closed_trades ADD COLUMN exit_stage TEXT NOT NULL DEFAULT 'LEGACY'",
        "ALTER TABLE active_positions ADD COLUMN initial_quantity TEXT NOT NULL DEFAULT '0'",
        "ALTER TABLE active_positions ADD COLUMN tp1_price REAL NOT NULL DEFAULT 0",
        "ALTER TABLE active_positions ADD COLUMN tp2_price REAL NOT NULL DEFAULT 0",
        "ALTER TABLE active_positions ADD COLUMN tp_stage INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE active_positions ADD COLUMN realized_pnl_usd REAL NOT NULL DEFAULT 0",
        "ALTER TABLE active_positions ADD COLUMN atr REAL NOT NULL DEFAULT 0",
        "ALTER TABLE active_positions ADD COLUMN opened_at INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE active_positions ADD COLUMN strategy_version TEXT NOT NULL DEFAULT 'LEGACY'",
        "ALTER TABLE active_positions ADD COLUMN entry_ema_fast REAL NOT NULL DEFAULT 0",
        "ALTER TABLE active_positions ADD COLUMN entry_ema_slow REAL NOT NULL DEFAULT 0",
        "ALTER TABLE active_positions ADD COLUMN entry_rsi REAL NOT NULL DEFAULT 0",
        "ALTER TABLE active_positions ADD COLUMN entry_adx REAL NOT NULL DEFAULT 0",
        "ALTER TABLE active_positions ADD COLUMN entry_obi REAL NOT NULL DEFAULT 0",
        "ALTER TABLE active_positions ADD COLUMN entry_spread REAL NOT NULL DEFAULT 0",
        "ALTER TABLE active_positions ADD COLUMN market_regime TEXT NOT NULL DEFAULT 'LEGACY'",
        "ALTER TABLE closed_trades ADD COLUMN opened_at INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE closed_trades ADD COLUMN closed_at INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE closed_trades ADD COLUMN strategy_version TEXT NOT NULL DEFAULT 'LEGACY'",
        "ALTER TABLE closed_trades ADD COLUMN entry_rsi REAL NOT NULL DEFAULT 0",
        "ALTER TABLE closed_trades ADD COLUMN entry_adx REAL NOT NULL DEFAULT 0",
        "ALTER TABLE closed_trades ADD COLUMN entry_obi REAL NOT NULL DEFAULT 0",
        "ALTER TABLE closed_trades ADD COLUMN market_regime TEXT NOT NULL DEFAULT 'LEGACY'",
    ] {
        let _ = conn.execute(migration, []);
    }
    conn.execute(
        "CREATE TABLE IF NOT EXISTS kv_store (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
        [],
    )
    .map_err(|e| format!("kv_store şeması oluşturulamadı: {e}"))?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS symbol_cooldowns (symbol TEXT PRIMARY KEY, closed_at INTEGER NOT NULL)",
        [],
    )
    .map_err(|e| format!("symbol_cooldowns şeması oluşturulamadı: {e}"))?;
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

fn load_consecutive_losses(conn: &Mutex<Connection>) -> usize {
    let Ok(c) = conn.lock() else {
        return 0;
    };
    let Ok(mut statement) =
        c.prepare("SELECT pnl_usd FROM closed_trades ORDER BY id DESC LIMIT 20")
    else {
        return 0;
    };
    let Ok(rows) = statement.query_map([], |row| row.get::<_, f64>(0)) else {
        return 0;
    };
    rows.filter_map(Result::ok)
        .take_while(|pnl| *pnl < 0.0)
        .count()
}

fn load_or_create_daily_starting_balance(conn: &Mutex<Connection>, current_balance: f64) -> f64 {
    let Ok(c) = conn.lock() else {
        return current_balance;
    };
    let today = c
        .query_row("SELECT date('now')", [], |row| row.get::<_, String>(0))
        .unwrap_or_else(|_| "unknown".to_string());
    let stored_date = c
        .query_row(
            "SELECT value FROM kv_store WHERE key='risk_session_date'",
            [],
            |row| row.get::<_, String>(0),
        )
        .ok();
    let stored_balance = c
        .query_row(
            "SELECT value FROM kv_store WHERE key='risk_session_starting_balance'",
            [],
            |row| row.get::<_, String>(0),
        )
        .ok()
        .and_then(|value| value.parse::<f64>().ok());
    if stored_date.as_deref() == Some(today.as_str()) {
        return stored_balance.unwrap_or(current_balance);
    }
    if let Err(error) = c.execute(
        "INSERT OR REPLACE INTO kv_store (key, value) VALUES ('risk_session_date', ?1)",
        params![today],
    ) {
        eprintln!("Günlük risk tarihi kaydedilemedi: {error}");
    }
    if let Err(error) = c.execute(
        "INSERT OR REPLACE INTO kv_store (key, value) VALUES ('risk_session_starting_balance', ?1)",
        params![current_balance.to_string()],
    ) {
        eprintln!("Günlük başlangıç bakiyesi kaydedilemedi: {error}");
    }
    current_balance
}

fn load_symbol_cooldowns(conn: &Mutex<Connection>) -> HashMap<String, i64> {
    let Ok(c) = conn.lock() else {
        return HashMap::new();
    };
    let Ok(mut statement) = c.prepare("SELECT symbol, closed_at FROM symbol_cooldowns") else {
        return HashMap::new();
    };
    let Ok(rows) = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    }) else {
        return HashMap::new();
    };
    rows.filter_map(Result::ok).collect()
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
            "INSERT OR IGNORE INTO closed_trades (id, symbol, side, entry_price, exit_price, status, pnl_percent, pnl_usd, max_pnl_percent, exit_stage, opened_at, closed_at, strategy_version, entry_rsi, entry_adx, entry_obi, market_regime) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            params![h.id as i64, h.symbol.clone(), h.side.clone(), h.entry_price, h.exit_price, h.status.clone(), h.pnl_percent, h.pnl_usd, h.max_pnl_percent, h.exit_stage.clone(), h.opened_at, h.closed_at, h.strategy_version.clone(), h.entry_rsi, h.entry_adx, h.entry_obi, h.market_regime.clone()],
        ).map_err(|e| e.to_string())?;
        tx.execute(
            "INSERT OR REPLACE INTO symbol_cooldowns (symbol, closed_at) VALUES (?1, ?2)",
            params![h.symbol.clone(), h.closed_at],
        )
        .map_err(|e| e.to_string())?;
    }

    tx.execute("DELETE FROM active_positions", [])
        .map_err(|e| e.to_string())?;
    for p in active_list {
        let lc_str = match p.lifecycle {
            PositionLifecycle::PendingOpen => "PendingOpen",
            PositionLifecycle::Open => "Open",
            PositionLifecycle::Closed => "Closed",
   …14063 tokens truncated…teY(-1deg);box-shadow:0 30px 50px #000b,0 0 28px var(--glow),inset 0 1px #ffffff18}}.position.long{{--accent:var(--green);--glow:#14e6a018;border-color:#14e6a044}}.position.short{{--accent:var(--red);--glow:#ff4d6d18;border-color:#ff4d6d44}}
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
<div class="section-head"><h2>Açık Pozisyonlar</h2><span>{strategy} · {strategy_count}/100 işlem · PnL {strategy_pnl} USDT · {position_count} pozisyon</span></div><section class="position-grid">"#,
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
        strategy = STRATEGY_VERSION,
        strategy_count = state.stats.strategy_trade_count,
        strategy_pnl = strategy_pnl_text,
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
<div><div class="symbol">{symbol}</div><div class="position-meta">#{id} · {leverage}x kaldıraç · {notional} USDT · TP aşaması {tp_stage}/3 · {strategy} · {regime}</div></div>
<div style="display:flex;gap:12px;align-items:start"><span class="side">{side}</span><div class="pnl"><strong class="{pnl_class}">{pnl_usd} USDT</strong><small>{pnl_percent}</small></div></div></div>
<div class="price-grid"><div><span>Giriş</span><b>{entry}</b></div><div><span>Anlık</span><b>{current}</b></div><div><span>Miktar</span><b>{quantity}</b></div></div>
<div class="risk-row"><span>Marjin <b>{margin} USDT</b></span><span>Stop <b>{stop}</b></span><span>TP1 <b>{tp1}</b></span><span>TP2 <b>{tp2}</b></span><span>Trend hedefi <b>{take_profit}</b></span></div></article>"#,
                side_class=side_class,symbol=escape_html(&position.symbol),id=position.id,leverage=leverage_text,notional=notional_text,pnl_class=pnl_class,tp_stage=position.tp_stage,
                strategy=escape_html(&position.strategy_version),regime=escape_html(&position.market_regime),
                side=escape_html(&position.side),pnl_usd=pnl_usd_text,pnl_percent=pnl_percent_text,entry=format_price(position.entry_price),
                current=format_price(position.current_price),quantity=format_quantity(&position.quantity),margin=margin_text,
                stop=format_price(position.stop_loss),tp1=format_price(position.tp1_price),tp2=format_price(position.tp2_price),take_profit=format_price(position.take_profit)
            ));
        }
    }

    html.push_str(&format!(
        r#"</section><div class="section-head"><h2>İşlem Geçmişi</h2><span>{closed} kapanan · İşlem başı {expectancy} USDT</span></div>
<div class="table-wrap"><table><thead><tr><th>ID</th><th>Parite</th><th>Yön</th><th>Strateji</th><th>Giriş</th><th>Çıkış</th><th>Kapanış</th><th>Maks. PnL</th><th>PnL</th><th>PnL &#36;</th></tr></thead><tbody>"#,
        closed=state.accounting.closed_trades_count,expectancy=format_money(state.stats.expectancy, true)
    ));
    if state.history.is_empty() {
        html.push_str(r#"<tr><td colspan="10" style="text-align:center;color:var(--muted)">Henüz kapanan işlem yok.</td></tr>"#);
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
                r#"<tr><td>#{id}</td><td><b>{symbol}</b></td><td class="side-text {side_class}">{side}</td><td>{strategy}<br><small>{regime}</small></td><td>{entry}</td><td>{exit}</td><td>{exit_stage}: {status}</td><td>{max_pnl}</td><td class="{pnl_class}">{pnl_percent}</td><td class="{pnl_class}">{pnl_usd} USDT</td></tr>"#,
                id=trade.id,symbol=escape_html(&trade.symbol),side_class=side_class,side=escape_html(&trade.side),
                strategy=escape_html(&trade.strategy_version),regime=escape_html(&trade.market_regime),
                entry=format_price(trade.entry_price),exit=format_price(trade.exit_price),status=escape_html(&trade.status),
                exit_stage=escape_html(&trade.exit_stage),max_pnl=format_percent(trade.max_pnl_percent, 2, true),
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
            .unwrap_or_else(|_| "https://fapi.binance.com".into()),
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
            .unwrap_or(12)
            .clamp(5, 50),
        max_trade_allocation: env::var("MAX_TRADE_ALLOCATION")
            .ok()
            .and_then(|value| value.parse().ok())
            .filter(|value| (0.01..=0.10).contains(value))
            .unwrap_or(0.10),
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
    fn protected_profit_stop_locks_both_directions() {
        let long_stop = protected_profit_stop("LONG", 100.0, 3.0, 0.40).unwrap();
        let short_stop = protected_profit_stop("SHORT", 100.0, 3.0, 0.40).unwrap();
        assert!(long_stop > break_even_stop("LONG", 100.0).unwrap());
        assert!(short_stop < break_even_stop("SHORT", 100.0).unwrap());
        assert_eq!(protected_profit_stop("INVALID", 100.0, 3.0, 0.40), None);
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
            calculate_position_size(1_000.0, 100.0, 98.5, 3.0, 0.005, 0.10, &filters).unwrap();
        assert!(margin <= 100.0);
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

    #[test]
    fn trade_pnl_includes_round_trip_fees() {
        let long = calculate_trade_pnl("LONG", 100.0, 102.0, 10.0).unwrap();
        let short = calculate_trade_pnl("SHORT", 100.0, 98.0, 10.0).unwrap();
        assert!(long > 0.0);
        assert!(short > 0.0);
        assert!(long < 20.0);
        assert!(short < 20.0);
    }

    #[test]
    fn fast_indicators_detect_uptrend() {
        let closes: Vec<f64> = (0..60).map(|index| 100.0 + index as f64 * 0.2).collect();
        let highs: Vec<f64> = closes.iter().map(|close| close + 0.1).collect();
        let lows: Vec<f64> = closes.iter().map(|close| close - 0.1).collect();
        let indicators = calculate_fast_indicators(&highs, &lows, &closes).unwrap();
        assert!(indicators.ema_fast > indicators.ema_slow);
        assert!(indicators.rsi > 50.0);
        assert!(indicators.atr > 0.0);
        assert!(indicators.adx > 0.0);
        assert_eq!(classify_market_regime(&indicators), MarketRegime::Bull);
    }

    #[test]
    fn weak_adx_is_sideways_regime() {
        let indicators = FastIndicators {
            ema_fast: 101.0,
            ema_slow: 100.0,
            rsi: 60.0,
            atr: 1.0,
            adx: 10.0,
        };
        assert_eq!(classify_market_regime(&indicators), MarketRegime::Sideways);
    }

    #[test]
    fn partial_take_profit_respects_lot_step() {
        let filters = SymbolFilters {
            min_qty: 0.001,
            max_qty: 100.0,
            step_size: 0.001,
        };
        assert_eq!(partial_quantity(10.0, 10.0, 0.30, &filters), Some(3.0));
        assert_eq!(partial_quantity(10.0, 1.0, 0.30, &filters), Some(1.0));
    }
}
