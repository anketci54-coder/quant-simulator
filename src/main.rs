use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tiny_http::{Server, Response};
use dotenvy::dotenv;
use std::env;
use rusqlite::{Connection, params};

const MAX_POSITIONS: usize = 10;
const POSITION_MARGIN_USDT: f64 = 100.0;

#[derive(Clone, Serialize, Debug, PartialEq)]
enum PositionLifecycle {
    PendingOpen,
    Open,
    Closed,
}

#[derive(Clone)]
struct Config {
    base_url: String,
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

#[derive(Clone)]
struct AppState {
    positions: Vec<ActivePosition>,
    history: Vec<ClosedPosition>,
    accounting: DailyAccounting,
    next_position_id: usize,
    last_error: String,
}

type SharedState = Arc<Mutex<AppState>>;

fn init_db() -> Connection {
    let conn = Connection::open("quant_history.db").expect("SQLite veritabanı açılamadı!");
    let _ = conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000;");
    let _ = conn.execute("CREATE TABLE IF NOT EXISTS closed_trades (id INTEGER PRIMARY KEY, symbol TEXT NOT NULL, side TEXT NOT NULL, entry_price REAL NOT NULL, exit_price REAL NOT NULL, status TEXT NOT NULL, pnl_percent REAL NOT NULL, pnl_usd REAL NOT NULL)", []);
    let _ = conn.execute("CREATE TABLE IF NOT EXISTS active_positions (id INTEGER PRIMARY KEY, symbol TEXT NOT NULL, side TEXT NOT NULL, entry_price REAL NOT NULL, current_price REAL NOT NULL, stop_loss REAL NOT NULL, take_profit REAL NOT NULL, best_price REAL NOT NULL, peak_pnl_percent REAL NOT NULL, leverage REAL NOT NULL, margin_usdt REAL NOT NULL, lifecycle TEXT NOT NULL, status TEXT NOT NULL, pnl_percent REAL NOT NULL, pnl_usd REAL NOT NULL, quantity TEXT NOT NULL)", []);
    let _ = conn.execute("CREATE TABLE IF NOT EXISTS kv_store (key TEXT PRIMARY KEY, value TEXT NOT NULL)", []);
    
    let column_exists: Result<i64, _> = conn.query_row(
        "SELECT COUNT(*) FROM pragma_table_info('active_positions') WHERE name='highest_price'",
        [],
        |row| row.get(0)
    );
    if let Ok(count) = column_exists {
        if count > 0 {
            let _ = conn.execute("ALTER TABLE active_positions RENAME COLUMN highest_price TO best_price", []);
        }
    }
    conn
}

fn get_max_closed_id(conn: &Mutex<Connection>) -> usize {
    if let Ok(c) = conn.lock() {
        let mut stmt = c.prepare("SELECT COALESCE(MAX(id), 0) FROM closed_trades").unwrap();
        let max_id: i64 = stmt.query_row([], |row| row.get(0)).unwrap_or(0);
        max_id as usize
    } else { 0 }
}

fn get_max_active_id(conn: &Mutex<Connection>) -> usize {
    if let Ok(c) = conn.lock() {
        let mut stmt = c.prepare("SELECT COALESCE(MAX(id), 0) FROM active_positions").unwrap();
        let max_id: i64 = stmt.query_row([], |row| row.get(0)).unwrap_or(0);
        max_id as usize
    } else { 0 }
}

fn atomic_close_and_save_position(conn: &Mutex<Connection>, h: &ClosedPosition, positions: &[ActivePosition], acc: &DailyAccounting) -> Result<(), String> {
    let mut c = conn.lock().map_err(|e| e.to_string())?;
    let tx = c.transaction().map_err(|e| e.to_string())?;

    tx.execute(
        "INSERT INTO closed_trades (id, symbol, side, entry_price, exit_price, status, pnl_percent, pnl_usd) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![h.id as i64, h.symbol.clone(), h.side.clone(), h.entry_price, h.exit_price, h.status.clone(), h.pnl_percent, h.pnl_usd],
    ).map_err(|e| e.to_string())?;

    tx.execute("DELETE FROM active_positions", []).map_err(|e| e.to_string())?;
    for p in positions {
        let lc_str = match p.lifecycle { PositionLifecycle::PendingOpen => "PendingOpen", PositionLifecycle::Open => "Open", PositionLifecycle::Closed => "Closed" };
        tx.execute(
            "INSERT INTO active_positions (id, symbol, side, entry_price, current_price, stop_loss, take_profit, best_price, peak_pnl_percent, leverage, margin_usdt, lifecycle, status, pnl_percent, pnl_usd, quantity) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            params![p.id as i64, p.symbol.clone(), p.side.clone(), p.entry_price, p.current_price, p.stop_loss, p.take_profit, p.best_price, p.peak_pnl_percent, p.leverage, p.margin_usdt, lc_str, p.status.clone(), p.pnl_percent, p.pnl_usd, p.quantity.clone()],
        ).map_err(|e| e.to_string())?;
    }

    tx.execute("INSERT OR REPLACE INTO kv_store (key, value) VALUES ('current_balance', ?1)", params![acc.current_balance.to_string()]).map_err(|e| e.to_string())?;
    tx.execute("INSERT OR REPLACE INTO kv_store (key, value) VALUES ('starting_balance', ?1)", params![acc.starting_balance.to_string()]).map_err(|e| e.to_string())?;

    tx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

fn atomic_save_active_positions(conn: &Mutex<Connection>, positions: &[ActivePosition], acc: &DailyAccounting) -> Result<(), String> {
    let mut c = conn.lock().map_err(|e| e.to_string())?;
    let tx = c.transaction().map_err(|e| e.to_string())?;

    tx.execute("DELETE FROM active_positions", []).map_err(|e| e.to_string())?;
    for p in positions {
        let lc_str = match p.lifecycle { PositionLifecycle::PendingOpen => "PendingOpen", PositionLifecycle::Open => "Open", PositionLifecycle::Closed => "Closed" };
        tx.execute(
            "INSERT INTO active_positions (id, symbol, side, entry_price, current_price, stop_loss, take_profit, best_price, peak_pnl_percent, leverage, margin_usdt, lifecycle, status, pnl_percent, pnl_usd, quantity) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            params![p.id as i64, p.symbol.clone(), p.side.clone(), p.entry_price, p.current_price, p.stop_loss, p.take_profit, p.best_price, p.peak_pnl_percent, p.leverage, p.margin_usdt, lc_str, p.status.clone(), p.pnl_percent, p.pnl_usd, p.quantity.clone()],
        ).map_err(|e| e.to_string())?;
    }

    tx.execute("INSERT OR REPLACE INTO kv_store (key, value) VALUES ('current_balance', ?1)", params![acc.current_balance.to_string()]).map_err(|e| e.to_string())?;
    tx.execute("INSERT OR REPLACE INTO kv_store (key, value) VALUES ('starting_balance', ?1)", params![acc.starting_balance.to_string()]).map_err(|e| e.to_string())?;

    tx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

fn load_active_positions_from_db(conn: &Mutex<Connection>) -> Result<Vec<ActivePosition>, String> {
    let mut positions = Vec::new();
    let c = conn.lock().map_err(|e| e.to_string())?;
    let mut stmt = c.prepare("SELECT id, symbol, side, entry_price, current_price, stop_loss, take_profit, best_price, peak_pnl_percent, leverage, margin_usdt, lifecycle, status, pnl_percent, pnl_usd, quantity FROM active_positions").map_err(|e| e.to_string())?;
    let iter = stmt.query_map([], |row| {
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
    }).map_err(|e| e.to_string())?;

    for r in iter {
        if let Ok(pos) = r {
            positions.push(pos);
        }
    }
    Ok(positions)
}

fn load_history_from_db(conn: &Mutex<Connection>) -> Result<(Vec<ClosedPosition>, usize, usize), String> {
    let mut history = Vec::new();
    let c = conn.lock().map_err(|e| e.to_string())?;
    
    let total_count: usize = c.query_row("SELECT COUNT(*) FROM closed_trades", [], |row| row.get::<_, i64>(0)).unwrap_or(0) as usize;
    let successful_count: usize = c.query_row("SELECT COUNT(*) FROM closed_trades WHERE pnl_percent > 0", [], |row| row.get::<_, i64>(0)).unwrap_or(0) as usize;

    let mut stmt = c.prepare("SELECT id, symbol, side, entry_price, exit_price, status, pnl_percent, pnl_usd FROM closed_trades ORDER BY id DESC LIMIT 50").map_err(|e| e.to_string())?;
    let iter = stmt.query_map([], |row| {
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
    }).map_err(|e| e.to_string())?;

    for r in iter {
        if let Ok(h) = r {
            history.push(h);
        }
    }
    history.reverse();
    Ok((history, total_count, successful_count))
}

fn load_accounting_from_db(conn: &Mutex<Connection>, default_starting: f64) -> DailyAccounting {
    let mut starting = default_starting;
    let mut current = default_starting;
    if let Ok(c) = conn.lock() {
        if let Ok(val) = c.query_row("SELECT value FROM kv_store WHERE key='starting_balance'", [], |row| row.get::<_, String>(0)) { if let Ok(p) = val.parse::<f64>() { starting = p; } }
        if let Ok(val) = c.query_row("SELECT value FROM kv_store WHERE key='current_balance'", [], |row| row.get::<_, String>(0)) { if let Ok(p) = val.parse::<f64>() { current = p; } }
    }
    let total_roi = if starting > 0.0 { ((current - starting) / starting) * 100.0 } else { 0.0 };
    DailyAccounting { date: "2026-07-28".to_string(), starting_balance: starting, current_balance: current, total_roi, closed_trades_count: 0, successful_trades: 0 }
}

fn calculate_obi(client: &reqwest::blocking::Client, config: &Config, symbol: &str) -> Result<f64, String> {
    let url = format!("{}?symbol={}&limit=5", config.futures_url("fapi/v1/depth"), symbol);
    let resp = client.get(&url).send().map_err(|e| e.to_string())?;
    let depth = resp.json::<Depth>().map_err(|e| e.to_string())?;
    let bid_vol: f64 = depth.bids.iter().filter_map(|b| b.get(1).and_then(|v| v.parse::<f64>().ok())).sum();
    let ask_vol: f64 = depth.asks.iter().filter_map(|a| a.get(1).and_then(|v| v.parse::<f64>().ok())).sum();
    if bid_vol + ask_vol > 0.0 { Ok((bid_vol - ask_vol) / (bid_vol + ask_vol)) } else { Ok(0.0) }
}

fn run_engine(config: Config, shared_state: SharedState, db_conn: Arc<Mutex<Connection>>) {
    let client = reqwest::blocking::Client::builder().connect_timeout(Duration::from_secs(3)).timeout(Duration::from_secs(10)).build().unwrap();
    loop {
        let url = config.futures_url("fapi/v1/ticker/24hr");
        let tickers_result = client.get(&url).send().and_then(|r| r.json::<Vec<Ticker>>());
        match tickers_result {
            Ok(tickers) => {
                let mut state = match shared_state.lock() { Ok(s) => s.clone(), Err(_) => { thread::sleep(Duration::from_secs(1)); continue; } };
                let mut realized_pnl_usd = 0.0;
                let mut still_active = Vec::new();
                let mut current_err = String::from("Sistem Kararlı Çalışıyor (Simülasyon Modu)");

                for mut pos in state.positions {
                    if pos.lifecycle == PositionLifecycle::PendingOpen {
                        pos.lifecycle = PositionLifecycle::Open;
                        pos.status = format!("Simüle Edilmiş Emir Gerçekleşti ({}x / ${:.0}) [ReduceOnly Destekli]", pos.side, pos.margin_usdt);
                    }
                    if let Some(t) = tickers.iter().find(|x| x.symbol == pos.symbol) {
                        if let Ok(curr_price) = t.last_price.parse::<f64>() {
                            pos.current_price = curr_price;
                            if pos.side == "LONG" { if curr_price > pos.best_price { pos.best_price = curr_price; } }
                            else { if curr_price < pos.best_price { pos.best_price = curr_price; } }
                            let raw_diff = if pos.side == "LONG" { (curr_price - pos.entry_price) / pos.entry_price } else { (pos.entry_price - curr_price) / pos.entry_price };
                            let fee_roi = 0.04 * pos.leverage * 2.0;
                            pos.pnl_percent = (raw_diff * pos.leverage * 100.0) - fee_roi;
                            pos.peak_pnl_percent = pos.peak_pnl_percent.max(pos.pnl_percent);

                            if pos.peak_pnl_percent >= 5.0 {
                                if pos.side == "LONG" {
                                    let trailed_sl = pos.best_price * 0.99;
                                    if trailed_sl > pos.stop_loss { pos.stop_loss = trailed_sl; }
                                } else {
                                    let trailed_sl = pos.best_price * 1.01;
                                    if trailed_sl < pos.stop_loss { pos.stop_loss = trailed_sl; }
                                }
                            }
                            pos.pnl_usd = pos.margin_usdt * (pos.pnl_percent / 100.0);
                            let mut close_reason = None;
                            if pos.side == "LONG" {
                                if curr_price <= pos.stop_loss { close_reason = Some("SL Hit - ReduceOnly".to_string()); }
                                else if curr_price >= pos.take_profit { close_reason = Some("TP Hit - ReduceOnly".to_string()); }
                            } else {
                                if curr_price >= pos.stop_loss { close_reason = Some("SL Hit - ReduceOnly".to_string()); }
                                else if curr_price <= pos.take_profit { close_reason = Some("TP Hit - ReduceOnly".to_string()); }
                            }
                            if let Some(reason) = close_reason {
                                pos.lifecycle = PositionLifecycle::Closed;
                                realized_pnl_usd += pos.pnl_usd;
                                
                                state.accounting.current_balance += realized_pnl_usd;
                                if state.accounting.starting_balance > 0.0 { 
                                    state.accounting.total_roi = ((state.accounting.current_balance - state.accounting.starting_balance) / state.accounting.starting_balance) * 100.0; 
                                }

                                let closed_trade = ClosedPosition { id: pos.id, symbol: pos.symbol.clone(), side: pos.side.clone(), entry_price: pos.entry_price, exit_price: curr_price, status: reason, pnl_percent: pos.pnl_percent, pnl_usd: pos.pnl_usd };
                                
                                if let Err(e) = atomic_close_and_save_position(&db_conn, &closed_trade, &still_active, &state.accounting) {
                                    current_err = format!("DB Transaction Hatası: {}", e);
                                }
                            } else { still_active.push(pos); }
                        } else { still_active.push(pos); }
                    } else { still_active.push(pos); }
                }

                if realized_pnl_usd == 0.0 {
                    state.accounting.current_balance += realized_pnl_usd;
                    if state.accounting.starting_balance > 0.0 { 
                        state.accounting.total_roi = ((state.accounting.current_balance - state.accounting.starting_balance) / state.accounting.starting_balance) * 100.0; 
                    }
                    if let Err(e) = atomic_save_active_positions(&db_conn, &still_active, &state.accounting) {
                        current_err = format!("DB Active Positions Kayıt Hatası: {}", e);
                    }
                }

                for t in &tickers {
                    if t.symbol.ends_with("USDT") {
                        if let (Ok(change), Ok(vol), Ok(_price)) = (t.price_change_percent.parse::<f64>(), t.quote_volume.parse::<f64>(), t.last_price.parse::<f64>()) {
                            if vol > 1_000_000.0 && change.abs() > 2.0 {
                                if let Ok(obi) = calculate_obi(&client, &config, &t.symbol) {
                                    let (should_open, side, lev) = match (change, obi) {
                                        (c, o) if c > 2.0 && o >= 0.20 => (true, "LONG", 3.0),
                                        (c, o) if c < -2.0 && o <= -0.20 => (true, "SHORT", 3.0),
                                        _ => (false, "", 0.0),
                                    };
                                    if should_open {
                                        let already_active = still_active.iter().any(|p| p.symbol == t.symbol);
                                        let committed_margin: f64 = still_active.iter().map(|p| p.margin_usdt).sum();
                                        if !already_active && still_active.len() < MAX_POSITIONS && (state.accounting.current_balance - committed_margin) >= POSITION_MARGIN_USDT {
                                            let price = t.last_price.parse::<f64>().unwrap_or(0.0);
                                            if price > 0.0 {
                                                let raw_qty = (POSITION_MARGIN_USDT * lev) / price;
                                                let step_size = 0.001;
                                                let stepped_qty = (raw_qty / step_size).floor() * step_size;
                                                let qty_str = format!("{:.3}", stepped_qty.max(step_size));
                                                let (stop_loss, take_profit) = if side == "LONG" { (price * 0.985, price * 1.035) } else { (price * 1.015, price * 0.965) };
                                                let current_id = state.next_position_id;
                                                state.next_position_id += 1;
                                                still_active.push(ActivePosition {
                                                    id: current_id, symbol: t.symbol.clone(), side: side.to_string(), entry_price: price, current_price: price,
                                                    stop_loss, take_profit, best_price: price, peak_pnl_percent: -0.04 * lev, leverage: lev, margin_usdt: POSITION_MARGIN_USDT,
                                                    lifecycle: PositionLifecycle::PendingOpen, status: format!("Simüle Emir Gönderildi ({})", side), pnl_percent: -0.04 * lev, pnl_usd: -(POSITION_MARGIN_USDT * 0.0004 * lev), quantity: qty_str,
                                                });
                                                let _ = atomic_save_active_positions(&db_conn, &still_active, &state.accounting);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                state.positions = still_active;
                
                if let Ok((hist, tot_count, succ_count)) = load_history_from_db(&db_conn) {
                    state.history = hist;
                    state.accounting.closed_trades_count = tot_count;
                    state.accounting.successful_trades = succ_count;
                }

                state.last_error = current_err;
                if let Ok(mut locked_state) = shared_state.lock() { *locked_state = state; }
            },
            Err(e) => { if let Ok(mut state) = shared_state.lock() { state.last_error = format!("Ağ Hatası: {}", e); } }
        }
        thread::sleep(Duration::from_secs(3));
    }
}

fn main() {
    dotenv().ok();
    let config = Config { base_url: env::var("BINANCE_BASE_URL").unwrap_or_else(|_| "https://testnet.binancefuture.com".into()) };
    let db = init_db();
    let db_conn = Arc::new(Mutex::new(db));
    let next_id = get_max_closed_id(&db_conn).max(get_max_active_id(&db_conn)) + 1;
    
    let initial_positions = load_active_positions_from_db(&db_conn).unwrap_or_default();
    let (initial_history, total_closed, total_succ) = load_history_from_db(&db_conn).unwrap_or_default();
    let initial_accounting = load_accounting_from_db(&db_conn, 1000.0);

    let shared_state: SharedState = Arc::new(Mutex::new(AppState {
        positions: initial_positions, history: initial_history,
        accounting: DailyAccounting { 
            date: initial_accounting.date, 
            starting_balance: initial_accounting.starting_balance, 
            current_balance: initial_accounting.current_balance, 
            total_roi: initial_accounting.total_roi, 
            closed_trades_count: total_closed, 
            successful_trades: total_succ 
        },
        next_position_id: next_id, last_error: "Sistem Başlatıldı".to_string(),
    }));

    let s_clone = Arc::clone(&shared_state);
    let db_clone = Arc::clone(&db_conn);
    let cfg_clone = config.clone();
    thread::spawn(move || { run_engine(cfg_clone, s_clone, db_clone); });

    let server = Server::http("0.0.0.0:8080").unwrap();
    println!("🚀 Kararlı Quant Paneli Yayında!");

    for request in server.incoming_requests() {
        let state = match shared_state.lock() { Ok(s) => s.clone(), Err(_) => continue };
        let mut html = String::from(r#"<!DOCTYPE html>
        <html lang="tr"><head><meta charset="UTF-8"><title>Quant Paneli</title><meta http-equiv="refresh" content="3">
        <style>
            body { background-color: #0d1117; color: #c9d1d9; font-family: Arial, sans-serif; padding: 20px; }
            h1, h2 { color: #58a6ff; font-size: 18px; }
            table { width: 100%; border-collapse: collapse; margin-top: 10px; background: #161b22; margin-bottom: 30px; }
            th, td { padding: 8px; border: 1px solid #30363d; text-align: left; font-size: 12px; }
            th { background-color: #21262d; color: #f0f6fc; }
            .pos { color: #3fb950; font-weight: bold; }
            .neg { color: #f85149; font-weight: bold; }
            .err { background: #163d16; border: 1px solid #3fb950; color: #3fb950; padding: 10px; border-radius: 6px; margin-bottom: 20px; font-weight: bold; }
            .card { background: #161b22; border: 1px solid #30363d; padding: 12px; border-radius: 6px; margin-bottom: 20px; font-size: 14px; }
        </style></head>
        <body>
            <h1>⚡ Quant İşlem Paneli (Simülasyon Modu)</h1>
            <div class="err">🚨 Durum: --LAST_ERR--</div>
            <div class="card"><b>Bakiye:</b> $--CURRENT_BAL-- | <b>ROI:</b> <span class="--ROI_CLASS--">--ROI_VAL--%</span> | <b>Kapatılan:</b> --CLOSED-- | <b>Başarılı:</b> --SUCC--</div>
            <h2>Aktif Pozisyonlar</h2>
            <table><tr><th>ID</th><th>Parite</th><th>Yön</th><th>Lifecycle</th><th>Margin</th><th>Miktar</th><th>Giriş</th><th>Anlık</th><th>Peak PnL %</th><th>Stop Loss</th><th>Take Profit</th><th>PnL %</th><th>PnL $</th></tr>"#);

        let roi_class = if state.accounting.total_roi >= 0.0 { "pos" } else { "neg" };
        html = html.replace("--LAST_ERR--", &state.last_error)
                   .replace("--CURRENT_BAL--", &format!("{:.2}", state.accounting.current_balance))
                   .replace("--ROI_CLASS--", roi_class)
                   .replace("--ROI_VAL--", &format!("{:+.2}", state.accounting.total_roi))
                   .replace("--CLOSED--", &state.accounting.closed_trades_count.to_string())
                   .replace("--SUCC--", &state.accounting.successful_trades.to_string());

        if state.positions.is_empty() {
            html.push_str("<tr><td colspan=\"13\" style=\"text-align: center;\">Aktif pozisyon yok.</td></tr>");
        } else {
            for p in state.positions {
                let pnl_class = if p.pnl_percent >= 0.0 { "pos" } else { "neg" };
                let lc_str = match p.lifecycle { PositionLifecycle::PendingOpen => "<span style='color:orange;'>PendingOpen</span>", PositionLifecycle::Open => "<span style='color:lightgreen;'>Open</span>", PositionLifecycle::Closed => "Closed" };
                html.push_str(&format!("<tr><td>#{}</td><td><b>{}</b></td><td>{}</td><td>{}</td><td>${:.0}</td><td>{}</td><td>{}</td><td>{}</td><td class=\"pos\">{:+.2}%</td><td class=\"neg\">{}</td><td class=\"pos\">{}</td><td class=\"{}\">{:+.2}%</td><td class=\"{}\">${:+.2}</td></tr>", p.id, p.symbol, p.side, lc_str, p.margin_usdt, p.quantity, p.entry_price, p.current_price, p.peak_pnl_percent, p.stop_loss, p.take_profit, pnl_class, p.pnl_percent, pnl_class, p.pnl_usd));
            }
        }

        html.push_str(r#"</table><h2>Kapatılan İşlemler (SQLite - Atomic Transactions)</h2><table><tr><th>ID</th><th>Parite</th><th>Yön</th><th>Giriş</th><th>Çıkış</th><th>Sonuç</th><th>PnL %</th><th>PnL $</th></tr>"#);
        if state.history.is_empty() {
            html.push_str("<tr><td colspan=\"8\" style=\"text-align: center;\">Kapatılan işlem yok.</td></tr>");
        } else {
            for h in state.history {
                let pnl_class = if h.pnl_percent >= 0.0 { "pos" } else { "neg" };
                html.push_str(&format!("<tr><td>#{}</td><td><b>{}</b></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td class=\"{}\">{:+.2}%</td><td class=\"{}\">${:+.2}</td></tr>", h.id, h.symbol, h.side, h.entry_price, h.exit_price, h.status, pnl_class, h.pnl_percent, pnl_class, h.pnl_usd));
            }
        }
        html.push_str("</table></body></html>");
        let response = Response::from_string(html).with_header(tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/html; charset=utf-8"[..]).unwrap());
        let _ = request.respond(response);
    }
}
