use serde::Deserialize;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tiny_http::{Server, Response};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use hex;
use dotenvy::dotenv;
use std::env;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
struct Config {
    api_key: String,
    secret_key: String,
    base_url: String,
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
    #[serde(rename = "highPrice")]
    high_price: String,
    #[serde(rename = "lowPrice")]
    low_price: String,
}

#[derive(Deserialize, Debug)]
struct Depth {
    bids: Vec<Vec<String>>,
    asks: Vec<Vec<String>>,
}

#[derive(Clone, serde::Serialize)]
struct SignalRow {
    symbol: String,
    change: f64,
    price: f64,
    obi: f64,
    action: String,
    pnl_sim: String,
}

#[derive(Clone, serde::Serialize, Debug)]
struct ActivePosition {
    id: usize,
    symbol: String,
    side: String,
    entry_price: f64,
    current_price: f64,
    highest_price: f64,
    peak_pnl_percent: f64,
    leverage: f64,
    status: String,
    pnl_percent: f64,
    pnl_usd: f64,
}

#[derive(Clone, serde::Serialize, Debug)]
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

#[derive(Clone, serde::Serialize, Debug)]
struct DailyAccounting {
    date: String,
    starting_balance: f64,
    current_balance: f64,
    total_roi: f64,
    closed_trades_count: usize,
    successful_trades: usize,
}

fn sign_query(secret_key: &str, query_string: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret_key.as_bytes()).expect("HMAC can take key of any size");
    mac.update(query_string.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn send_testnet_order(client: &reqwest::blocking::Client, config: &Config, symbol: &str, side: &str, quantity: &str) {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
    let query = format!("symbol={}&side={}&type=MARKET&quantity={}&timestamp={}", symbol, side, quantity, timestamp);
    let signature = sign_query(&config.secret_key, &query);
    let full_query = format!("{}&signature={}", query, signature);
    let url = format!("{}/fapi/v1/order?{}", config.base_url, full_query);
    let _ = client.post(&url).header("X-MBX-APIKEY", &config.api_key).send();
}

fn calculate_obi(client: &reqwest::blocking::Client, symbol: &str) -> f64 {
    let url = format!("https://testnet.binancefuture.com/fapi/v1/depth?symbol={}&limit=5", symbol);
    if let Ok(resp) = client.get(&url).send() {
        if let Ok(depth) = resp.json::<Depth>() {
            let bid_vol: f64 = depth.bids.iter().filter_map(|b| b.get(1).and_then(|v| v.parse::<f64>().ok())).sum();
            let ask_vol: f64 = depth.asks.iter().filter_map(|a| a.get(1).and_then(|v| v.parse::<f64>().ok())).sum();
            if bid_vol + ask_vol > 0.0 { return (bid_vol - ask_vol) / (bid_vol + ask_vol); }
        }
    }
    0.0
}

fn run_engine(config: Config, signals_cache: Arc<Mutex<Vec<SignalRow>>>, positions_cache: Arc<Mutex<Vec<ActivePosition>>>, history_cache: Arc<Mutex<Vec<ClosedPosition>>>, accounting_cache: Arc<Mutex<DailyAccounting>>) {
    let mut next_id = 1;
    let initial_capital = 1000.0;
    
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    loop {
        let url = "https://testnet.binancefuture.com/fapi/v1/ticker/24hr";
        if let Ok(resp) = client.get(url).send() {
            if let Ok(tickers) = resp.json::<Vec<Ticker>>() {
                
                // 1. Snapshot al (Mutex kilit süresini minimuma indir)
                let mut positions = positions_cache.lock().unwrap().clone();
                let mut history = history_cache.lock().unwrap().clone();
                let mut acc = accounting_cache.lock().unwrap().clone();

                let mut realized_pnl_usd = 0.0;
                let mut closed_count = 0;
                let mut success_count = 0;
                let mut still_active = Vec::new();
                let mut new_signals = Vec::new();
                let mut orders_to_send = Vec::new();

                for mut pos in positions {
                    if let Some(t) = tickers.iter().find(|x| x.symbol == pos.symbol) {
                        if let Ok(curr_price) = t.last_price.parse::<f64>() {
                            pos.current_price = curr_price;
                            if pos.side == "LONG" && curr_price > pos.highest_price { pos.highest_price = curr_price; }
                            else if pos.side == "SHORT" && curr_price < pos.highest_price { pos.highest_price = curr_price; }
                            
                            let raw_diff = if pos.side == "LONG" { (curr_price - pos.entry_price) / pos.entry_price } else { (pos.entry_price - curr_price) / pos.entry_price };
                            pos.pnl_percent = (raw_diff * pos.leverage * 100.0) - 0.20;
                            pos.peak_pnl_percent = pos.peak_pnl_percent.max(pos.pnl_percent);

                            let position_budget = initial_capital / 5.0;
                            pos.pnl_usd = position_budget * (pos.pnl_percent / 100.0);

                            let stop_floor = match pos.peak_pnl_percent {
                                peak if peak >= 5.0 => 2.5,
                                peak if peak >= 3.5 => 1.5,
                                peak if peak >= 2.0 => 0.5,
                                _ => -1.5,
                            };

                            let mut close_reason = None;
                            if pos.pnl_percent <= stop_floor {
                                if stop_floor > 0.0 { close_reason = Some("Kârla Kapatıldı (Trailing)".to_string()); success_count += 1; }
                                else { close_reason = Some("Zarar Kes (Stop Loss)".to_string()); }
                            } else if pos.pnl_percent >= 7.0 {
                                close_reason = Some("Hedef Kapatıldı (Max Hit)".to_string());
                                success_count += 1;
                            }

                            if let Some(reason) = close_reason {
                                realized_pnl_usd += pos.pnl_usd;
                                closed_count += 1;
                                history.push(ClosedPosition {
                                    id: pos.id,
                                    symbol: pos.symbol,
                                    side: pos.side,
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

                acc.current_balance += realized_pnl_usd;
                acc.closed_trades_count += closed_count;
                acc.successful_trades += success_count;
                if acc.starting_balance > 0.0 {
                    acc.total_roi = ((acc.current_balance - acc.starting_balance) / acc.starting_balance) * 100.0;
                }

                for t in tickers {
                    if t.symbol.ends_with("USDT") {
                        if let (Ok(change), Ok(vol), Ok(price), Ok(high), Ok(low)) = (
                            t.price_change_percent.parse::<f64>(),
                            t.quote_volume.parse::<f64>(),
                            t.last_price.parse::<f64>(),
                            t.high_price.parse::<f64>(),
                            t.low_price.parse::<f64>()
                        ) {
                            if vol > 5_000_000.0 && change.abs() > 4.0 {
                                let obi = calculate_obi(&client, &t.symbol);
                                let is_breakout_high = price >= high * 0.995;
                                let is_breakout_low = price <= low * 1.005;

                                let (action, pnl_sim, should_open, side, api_side, lev) = if is_breakout_high && obi < -0.2 {
                                    ("PA Likidite Alımı / BoS (SHORT)", "Testnet Emir Gönderildi", true, "SHORT", "SELL", 5.0)
                                } else if is_breakout_low && obi > 0.2 {
                                    ("PA Dip Tepkisi / Destek (LONG)", "Testnet Emir Gönderildi", true, "LONG", "BUY", 5.0)
                                } else if change > 0.0 && obi > 0.3 {
                                    ("Order Block / Alım Baskısı (LONG)", "Testnet Emir Gönderildi", true, "LONG", "BUY", 3.0)
                                } else {
                                    ("Gürültü / İzlemede", "İşlem Yok", false, "", "", 0.0)
                                };

                                new_signals.push(SignalRow { symbol: t.symbol.clone(), change, price, obi, action: action.to_string(), pnl_sim: pnl_sim.to_string() });

                                if should_open {
                                    let already_active = still_active.iter().any(|p| p.symbol == t.symbol);
                                    if !already_active {
                                        // Dinamik quantity hesabı (bütçe / fiyat)
                                        let position_budget = initial_capital / 5.0;
                                        let raw_qty = (position_budget * lev) / price;
                                        let qty_str = format!("{:.3}", raw_qty.max(0.001));

                                        orders_to_send.push((t.symbol.clone(), api_side.to_string(), qty_str));

                                        still_active.push(ActivePosition {
                                            id: next_id, symbol: t.symbol.clone(), side: side.to_string(),
                                            entry_price: price, current_price: price, highest_price: price, peak_pnl_percent: -0.20,
                                            leverage: lev, status: format!("Aktif {} ({}x)", side, lev),
                                            pnl_percent: -0.20, pnl_usd: -((initial_capital / 5.0) * 0.0020),
                                        });
                                        next_id += 1;
                                    }
                                }
                            }
                        }
                    }
                }

                // 2. Ağ çağrıları bittikten sonra sonuçları kısa bir kilit altında kaydet
                {
                    let mut pos_lock = positions_cache.lock().unwrap();
                    let mut hist_lock = history_cache.lock().unwrap();
                    let mut acc_lock = accounting_cache.lock().unwrap();
                    let mut sig_lock = signals_cache.lock().unwrap();

                    *pos_lock = still_active;
                    *hist_lock = history;
                    *acc_lock = acc;
                    *sig_lock = new_signals;
                }

                // Ağ emirlerini kilitsiz gönder
                for (sym, side, qty) in orders_to_send {
                    send_testnet_order(&client, &config, &sym, &side, &qty);
                }
            }
        }
        thread::sleep(Duration::from_secs(3));
    }
}

fn main() {
    dotenv().ok();
    let config = Config {
        api_key: env::var("BINANCE_API_KEY").expect("BINANCE_API_KEY eksik"),
        secret_key: env::var("BINANCE_SECRET_KEY").expect("BINANCE_SECRET_KEY eksik"),
        base_url: env::var("BINANCE_BASE_URL").unwrap_or_else(|_| "https://testnet.binancefuture.com".into()),
    };

    let signals_cache = Arc::new(Mutex::new(Vec::new()));
    let positions_cache = Arc::new(Mutex::new(Vec::new()));
    let history_cache = Arc::new(Mutex::new(Vec::new()));
    let accounting_cache = Arc::new(Mutex::new(DailyAccounting {
        date: "2026-07-28".to_string(), starting_balance: 1000.0,
        current_balance: 1000.0, total_roi: 0.0, closed_trades_count: 0, successful_trades: 0,
    }));

    let s_clone = Arc::clone(&signals_cache);
    let p_clone = Arc::clone(&positions_cache);
    let h_clone = Arc::clone(&history_cache);
    let a_clone = Arc::clone(&accounting_cache);
    let cfg_clone = config.clone();

    thread::spawn(move || { run_engine(cfg_clone, s_clone, p_clone, h_clone, a_clone); });

    let server = Server::http("0.0.0.0:8080").unwrap();
    println!("🚀 Optimize & Güvenli Quant Paneli Yayında!");

    for request in server.incoming_requests() {
        let positions = positions_cache.lock().unwrap().clone();
        let history = history_cache.lock().unwrap().clone();
        let acc = accounting_cache.lock().unwrap().clone();
        
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
                .card { background: #161b22; border: 1px solid #30363d; padding: 12px; border-radius: 6px; margin-bottom: 20px; font-size: 14px; }
            </style>
        </head>
        <body>
            <h1>⚡ Optimize Quant Paneli</h1>
            <div class="card">
                <b>Bakiye:</b> $--CURRENT_BAL-- | 
                <b>ROI:</b> <span class="--ROI_CLASS--">--ROI_VAL--%</span> | 
                <b>Kapatılan:</b> --CLOSED-- | 
                <b>Başarılı:</b> --SUCC--
            </div>
            <h2>Aktif Pozisyonlar</h2>
            <table>
                <tr><th>ID</th><th>Parite</th><th>Yön</th><th>Giriş</th><th>Anlık</th><th>Tepe PnL</th><th>PnL %</th><th>PnL $</th></tr>"#);

        let roi_class = if acc.total_roi >= 0.0 { "pos" } else { "neg" };
        html = html
            .replace("--CURRENT_BAL--", &format!("{:.2}", acc.current_balance))
            .replace("--ROI_CLASS--", roi_class)
            .replace("--ROI_VAL--", &format!("{:+.2}", acc.total_roi))
            .replace("--CLOSED--", &acc.closed_trades_count.to_string())
            .replace("--SUCC--", &acc.successful_trades.to_string());

        if positions.is_empty() {
            html.push_str("<tr><td colspan=\"8\" style=\"text-align: center;\">Aktif pozisyon yok.</td></tr>");
        } else {
            for p in positions {
                let pnl_class = if p.pnl_percent >= 0.0 { "pos" } else { "neg" };
                html.push_str(&format!("<tr><td>#{}</td><td><b>{}</b></td><td>{}</td><td>{}</td><td>{}</td><td>{:+.2}%</td><td class=\"{}\">{:+.2}%</td><td class=\"{}\">${:+.2}</td></tr>", p.id, p.symbol, p.side, p.entry_price, p.current_price, p.peak_pnl_percent, pnl_class, p.pnl_percent, pnl_class, p.pnl_usd));
            }
        }

        html.push_str(r#"</table>
            <h2>Kapatılan İşlemler Geçmişi</h2>
            <table>
                <tr><th>ID</th><th>Parite</th><th>Yön</th><th>Giriş</th><th>Çıkış</th><th>Sonuç</th><th>PnL %</th><th>PnL $</th></tr>"#);

        if history.is_empty() {
            html.push_str("<tr><td colspan=\"8\" style=\"text-align: center;\">Kapatılan işlem yok.</td></tr>");
        } else {
            for h in history {
                let pnl_class = if h.pnl_percent >= 0.0 { "pos" } else { "neg" };
                html.push_str(&format!("<tr><td>#{}</td><td><b>{}</b></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td class=\"{}\">{:+.2}%</td><td class=\"{}\">${:+.2}</td></tr>", h.id, h.symbol, h.side, h.entry_price, h.exit_price, h.status, pnl_class, h.pnl_percent, pnl_class, h.pnl_usd));
            }
        }

        html.push_str("</table></body></html>");
        let response = Response::from_string(html).with_header(tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/html; charset=utf-8"[..]).unwrap());
        let _ = request.respond(response);
    }
}
