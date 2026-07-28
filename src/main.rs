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

#[derive(Clone, Serialize)]
struct SignalRow {
    symbol: String,
    change: f64,
    price: f64,
    obi: f64,
    action: String,
    pnl_sim: String,
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
    highest_price: f64,
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
    signals: Vec<SignalRow>,
    positions: Vec<ActivePosition>,
    history: Vec<ClosedPosition>,
    accounting: DailyAccounting,
    next_position_id: usize,
    last_error: String,
}

type SharedState = Arc<Mutex<AppState>>;

fn init_db() -> Connection {
    let conn = Connection::open("quant_history.db").expect("SQLite veritabanı açılamadı!");
    conn.execute(
        "CREATE TABLE IF NOT EXISTS closed_trades (
            id INTEGER PRIMARY KEY,
            symbol TEXT,
            side TEXT,
            entry_price REAL,
            exit_price REAL,
            status TEXT,
            pnl_percent REAL,
            pnl_usd REAL
        )",
        [],
    ).expect("Tablo oluşturulamadı!");
    conn
}

fn save_trade_to_db(conn: &Mutex<Connection>, h: &ClosedPosition) {
    if let Ok(c) = conn.lock() {
        let _ = c.execute(
            "INSERT INTO closed_trades (id, symbol, side, entry_price, exit_price, status, pnl_percent, pnl_usd) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![h.id as i64, h.symbol, h.side, h.entry_price, h.exit_price, h.status, h.pnl_percent, h.pnl_usd],
        );
    }
}

fn load_history_from_db(conn: &Mutex<Connection>) -> Vec<ClosedPosition> {
    let mut history = Vec::new();
    if let Ok(c) = conn.lock() {
        if let Ok(mut stmt) = c.prepare("SELECT id, symbol, side, entry_price, exit_price, status, pnl_percent, pnl_usd FROM closed_trades ORDER BY id DESC LIMIT 50") {
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
            });
            if let Ok(rows) = iter {
                for r in rows.flatten() {
                    history.push(r);
                }
            }
        }
    }
    history.reverse();
    history
}

fn calculate_obi(client: &reqwest::blocking::Client, config: &Config, symbol: &str) -> Result<f64, String> {
    let url = format!("{}?symbol={}&limit=5", config.futures_url("fapi/v1/depth"), symbol);
    let resp = client.get(&url).send().map_err(|e| e.to_string())?;
    let depth = resp.json::<Depth>().map_err(|e| e.to_string())?;
    
    let bid_vol: f64 = depth.bids.iter().filter_map(|b| b.get(1).and_then(|v| v.parse::<f64>().ok())).sum();
    let ask_vol: f64 = depth.asks.iter().filter_map(|a| a.get(1).and_then(|v| v.parse::<f64>().ok())).sum();
    
    if bid_vol + ask_vol > 0.0 {
        Ok((bid_vol - ask_vol) / (bid_vol + ask_vol))
    } else {
        Ok(0.0)
    }
}

fn run_engine(config: Config, shared_state: SharedState, db_conn: Arc<Mutex<Connection>>) {
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    loop {
        let url = config.futures_url("fapi/v1/ticker/24hr");
        let tickers_result = client.get(&url).send().and_then(|r| r.json::<Vec<Ticker>>());

        match tickers_result {
            Ok(tickers) => {
                let mut state = match shared_state.lock() {
                    Ok(s) => s.clone(),
                    Err(_) => {
                        thread::sleep(Duration::from_secs(1));
                        continue;
                    }
                };

                let mut realized_pnl_usd = 0.0;
                let mut closed_count = 0;
                let mut success_count = 0;
                let mut still_active = Vec::new();
                let mut new_closed = Vec::new();
                let current_err = String::from("Adım 5: SQLite Kalıcı Bellek ve Modüler Mimari Aktif");

                for mut pos in state.positions {
                    if pos.lifecycle == PositionLifecycle::PendingOpen {
                        pos.lifecycle = PositionLifecycle::Open;
                        pos.status = format!("Aktif {} ({}x / ${:.0}) [Doğrulandı]", pos.side, pos.leverage, pos.margin_usdt);
                    }

                    if let Some(t) = tickers.iter().find(|x| x.symbol == pos.symbol) {
                        if let Ok(curr_price) = t.last_price.parse::<f64>() {
                            pos.current_price = curr_price;
                            if pos.side == "LONG" && curr_price > pos.highest_price { pos.highest_price = curr_price; }
                            else if pos.side == "SHORT" && curr_price < pos.highest_price { pos.highest_price = curr_price; }
                            
                            let raw_diff = if pos.side == "LONG" { (curr_price - pos.entry_price) / pos.entry_price } else { (pos.entry_price - curr_price) / pos.entry_price };
                            let fee_roi = 0.04 * pos.leverage * 2.0;
                            pos.pnl_percent = (raw_diff * pos.leverage * 100.0) - fee_roi;
                            pos.peak_pnl_percent = pos.peak_pnl_percent.max(pos.pnl_percent);

                            pos.pnl_usd = pos.margin_usdt * (pos.pnl_percent / 100.0);

                            let mut close_reason = None;
                            if pos.side == "LONG" {
                                if curr_price <= pos.stop_loss {
                                    close_reason = Some("Zarar Kes (SL Hit - ReduceOnly)".to_string());
                                } else if curr_price >= pos.take_profit {
                                    close_reason = Some("Hedef Alındı (TP Hit - ReduceOnly)".to_string());
                                    success_count += 1;
                                }
                            } else {
                                if curr_price >= pos.stop_loss {
                                    close_reason = Some("Zarar Kes (SL Hit - ReduceOnly)".to_string());
                                } else if curr_price <= pos.take_profit {
                                    close_reason = Some("Hedef Alındı (TP Hit - ReduceOnly)".to_string());
                                    success_count += 1;
                                }
                            }

                            if let Some(reason) = close_reason {
                                pos.lifecycle = PositionLifecycle::Closed;
                                realized_pnl_usd += pos.pnl_usd;
                                closed_count += 1;
                                
                                let closed_trade = ClosedPosition {
                                    id: pos.id,
                                    symbol: pos.symbol,
                                    side: pos.side,
                                    entry_price: pos.entry_price,
                                    exit_price: curr_price,
                                    status: reason,
                                    pnl_percent: pos.pnl_percent,
                                    pnl_usd: pos.pnl_usd,
                                };

                                save_trade_to_db(&db_conn, &closed_trade);
                                new_closed.push(closed_trade);
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

                state.accounting.current_balance += realized_pnl_usd;
                state.accounting.closed_trades_count += closed_count;
                state.accounting.successful_trades += success_count;
                if state.accounting.starting_balance > 0.0 {
                    state.accounting.total_roi = ((state.accounting.current_balance - state.accounting.starting_balance) / state.accounting.starting_balance) * 100.0;
                }

                let mut new_signals = Vec::new();
                for t in tickers {
                    if t.symbol.ends_with("USDT") {
                        if let (Ok(change), Ok(vol), Ok(price)) = (
                            t.price_change_percent.parse::<f64>(),
                            t.quote_volume.parse::<f64>(),
                            t.last_price.parse::<f64>()
                        ) {
                            if vol > 1_000_000.0 && change.abs() > 2.0 {
                                let obi = calculate_obi(&client, &config, &t.symbol).unwrap_or(0.0);

                                let (action, should_open, side, lev) = if change > 2.0 {
                                    ("Momentum Artış (LONG)", true, "LONG", 3.0)
                                } else if change < -2.0 {
                                    ("Momentum Düşüş (SHORT)", true, "SHORT", 3.0)
                                } else {
                                    ("İzlemede", false, "", 0.0)
                                };

                                new_signals.push(SignalRow { 
                                    symbol: t.symbol.clone(), 
                                    change, 
                                    price, 
                                    obi, 
                                    action: action.to_string(), 
                                    pnl_sim: "Aktif Sinyal".to_string() 
                                });

                                if should_open {
                                    let already_active = still_active.iter().any(|p| p.symbol == t.symbol);
                                    
                                    let committed_margin: f64 = still_active.iter().map(|p| p.margin_usdt).sum();
                                    let available_margin = state.accounting.current_balance - committed_margin;
                                    let has_capacity = still_active.len() < MAX_POSITIONS;
                                    let has_sufficient_funds = available_margin >= POSITION_MARGIN_USDT;

                                    if !already_active && has_capacity && has_sufficient_funds {
                                        let raw_qty = (POSITION_MARGIN_USDT * lev) / price;
                                        let step_size = 0.001;
                                        let stepped_qty = (raw_qty / step_size).floor() * step_size;
                                        let qty_str = format!("{:.3}", stepped_qty.max(step_size));

                                        let (stop_loss, take_profit) = if side == "LONG" {
                                            (price * (1.0 - 0.015), price * (1.0 + 0.035))
                                        } else {
                                            (price * (1.0 + 0.015), price * (1.0 - 0.035))
                                        };

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
                                            highest_price: price, 
                                            peak_pnl_percent: -0.04 * lev,
                                            leverage: lev, 
                                            margin_usdt: POSITION_MARGIN_USDT,
                                            lifecycle: PositionLifecycle::PendingOpen,
                                            status: format!("Emir Bekliyor (Pending {})", side),
                                            pnl_percent: -0.04 * lev, 
                                            pnl_usd: -(POSITION_MARGIN_USDT * 0.0004 * lev),
                                            quantity: qty_str,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }

                state.positions = still_active;
                // Veritabanından güncel geçmişi yükleyip senkronize ediyoruz
                state.history = load_history_from_db(&db_conn);
                state.signals = new_signals;
                state.last_error = current_err;

                if let Ok(mut locked_state) = shared_state.lock() {
                    *locked_state = state;
                }
            },
            Err(e) => {
                if let Ok(mut state) = shared_state.lock() {
                    state.last_error = format!("Ağ Hatası / Ticker Alınamadı: {}", e);
                }
            }
        }
        thread::sleep(Duration::from_secs(3));
    }
}

fn main() {
    dotenv().ok();
    let config = Config {
        base_url: env::var("BINANCE_BASE_URL").unwrap_or_else(|_| "https://testnet.binancefuture.com".into()),
    };

    let db = init_db();
    let db_conn = Arc::new(Mutex::new(db));
    let initial_history = load_history_from_db(&db_conn);
    let closed_count = initial_history.len();

    let shared_state: SharedState = Arc::new(Mutex::new(AppState {
        signals: Vec::new(),
        positions: Vec::new(),
        history: initial_history,
        accounting: DailyAccounting {
            date: "2026-07-28".to_string(), 
            starting_balance: 1000.0,
            current_balance: 1000.0, 
            total_roi: 0.0, 
            closed_trades_count: closed_count, 
            successful_trades: 0,
        },
        next_position_id: 1,
        last_error: "Adım 5: SQLite Kalıcı Bellek Başlatıldı".to_string(),
    }));

    let s_clone = Arc::clone(&shared_state);
    let db_clone = Arc::clone(&db_conn);
    let cfg_clone = config.clone();
    thread::spawn(move || { run_engine(cfg_clone, s_clone, db_clone); });

    let server = Server::http("0.0.0.0:8080").unwrap();
    println!("🚀 Adım 5 Tamamlandı: SQLite Destekli Kalıcı Quant Paneli Yayında!");

    for request in server.incoming_requests() {
        let state = match shared_state.lock() {
            Ok(s) => s.clone(),
            Err(_) => continue,
        };
        
        let mut html = String::from(r#"<!DOCTYPE html>
        <html lang="tr">
        <head>
            <meta charset="UTF-8">
            <title>Quant Paneli</title>
            <meta http-equiv="refresh" content="3">
            <style>
                body { background-color: #0d1117; color: #c9d1d9; font-family: Arial, sans-serif; padding: 20px; }
                h1, h2 { color: #58a6ff; font-size: 18px; }
                table { width: 100%; border-collapse: collapse; margin-top: 10px; background: #161b22; margin-bottom: 30px; }
                th, td { padding: 8px; border: 1px solid #30363d; text-align: left; font-size: 12px; }
                th { background-color: #21262d; color: #f0f6fc; }
                tr:hover { background-color: #30363d; }
                .pos { color: #3fb950; font-weight: bold; }
                .neg { color: #f85149; font-weight: bold; }
                .err { background: #163d16; border: 1px solid #3fb950; color: #3fb950; padding: 10px; border-radius: 6px; margin-bottom: 20px; font-weight: bold; }
                .card { background: #161b22; border: 1px solid #30363d; padding: 12px; border-radius: 6px; margin-bottom: 20px; font-size: 14px; }
            </style>
        </head>
        <body>
            <h1>⚡ SQLite Kalıcı Bellekli Quant Paneli</h1>
            <div class="err">🚨 Durum: --LAST_ERR--</div>
            <div class="card">
                <b>Bakiye:</b> $--CURRENT_BAL-- | 
                <b>ROI:</b> <span class="--ROI_CLASS--">--ROI_VAL--%</span> | 
                <b>Kapatılan (DB Kayıtlı):</b> --CLOSED-- | 
                <b>Başarılı:</b> --SUCC--
            </div>
            <h2>Aktif Pozisyonlar ve Emir Döngüsü</h2>
            <table>
                <tr><th>ID</th><th>Parite</th><th>Yön</th><th>Lifecycle</th><th>Margin</th><th>Miktar</th><th>Giriş</th><th>Anlık</th><th>Stop Loss</th><th>Take Profit</th><th>PnL %</th><th>PnL $</th></tr>"#);

        let roi_class = if state.accounting.total_roi >= 0.0 { "pos" } else { "neg" };
        html = html
            .replace("--LAST_ERR--", &state.last_error)
            .replace("--CURRENT_BAL--", &format!("{:.2}", state.accounting.current_balance))
            .replace("--ROI_CLASS--", roi_class)
            .replace("--ROI_VAL--", &format!("{:+.2}", state.accounting.total_roi))
            .replace("--CLOSED--", &state.accounting.closed_trades_count.to_string())
            .replace("--SUCC--", &state.accounting.successful_trades.to_string());

        if state.positions.is_empty() {
            html.push_str("<tr><td colspan=\"12\" style=\"text-align: center;\">Aktif pozisyon yok.</td></tr>");
        } else {
            for p in state.positions {
                let pnl_class = if p.pnl_percent >= 0.0 { "pos" } else { "neg" };
                let lifecycle_str = match p.lifecycle {
                    PositionLifecycle::PendingOpen => "<span style='color:orange;'>PendingOpen</span>",
                    PositionLifecycle::Open => "<span style='color:lightgreen;'>Open</span>",
                    PositionLifecycle::Closed => "Closed",
                };
                html.push_str(&format!("<tr><td>#{}</td><td><b>{}</b></td><td>{}</td><td>{}</td><td>${:.0}</td><td>{}</td><td>{}</td><td>{}</td><td class=\"neg\">{}</td><td class=\"pos\">{}</td><td class=\"{}\">{:+.2}%</td><td class=\"{}\">${:+.2}</td></tr>", p.id, p.symbol, p.side, lifecycle_str, p.margin_usdt, p.quantity, p.entry_price, p.current_price, p.stop_loss, p.take_profit, pnl_class, p.pnl_percent, pnl_class, p.pnl_usd));
            }
        }

        html.push_str(r#"</table>
            <h2>Kapatılan İşlemler Geçmişi (SQLite Veritabanından)</h2>
            <table>
                <tr><th>ID</th><th>Parite</th><th>Yön</th><th>Giriş</th><th>Çıkış</th><th>Sonuç</th><th>PnL %</th><th>PnL $</th></tr>"#);

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
