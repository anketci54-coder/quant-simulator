use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Serialize;
use tokio::sync::{mpsc, oneshot, watch};

use crate::{
    model::{Side, STRATEGY_VERSION},
    portfolio::EmergencyExitSummary,
};

#[derive(Clone, Debug, Default, Serialize)]
pub struct DashboardSnapshot {
    pub strategy_version: String,
    pub status: String,
    pub entries_paused: bool,
    pub balance: f64,
    pub used_margin: f64,
    pub free_margin: f64,
    pub realized_net_pnl: f64,
    pub unrealized_net_pnl: f64,
    pub tracked_symbols: usize,
    pub updated_at: i64,
    pub positions: Vec<PositionView>,
}

#[derive(Clone, Debug, Serialize)]
pub struct PositionView {
    pub id: u64,
    pub symbol: String,
    pub side: Side,
    pub stage: String,
    pub leverage: f64,
    pub entry: f64,
    pub current: f64,
    pub stop: f64,
    pub tp1: f64,
    pub tp2: f64,
    pub original_quantity: f64,
    pub remaining_quantity: f64,
    pub remaining_margin: f64,
    pub realized_net_pnl: f64,
    pub unrealized_net_pnl: f64,
    pub funding_cost: f64,
    pub opened_at: i64,
}

pub struct EmergencyCommand {
    pub reply: oneshot::Sender<std::result::Result<EmergencyExitSummary, String>>,
}

#[derive(Clone)]
struct PanelState {
    dashboard: watch::Receiver<DashboardSnapshot>,
    emergency: mpsc::Sender<EmergencyCommand>,
    action_token: Arc<str>,
}

pub async fn run_panel(
    bind: SocketAddr,
    dashboard: watch::Receiver<DashboardSnapshot>,
    emergency: mpsc::Sender<EmergencyCommand>,
    action_token: String,
) -> Result<()> {
    let state = PanelState {
        dashboard,
        emergency,
        action_token: Arc::from(action_token),
    };
    let app = Router::new()
        .route("/", get(index))
        .route("/api/dashboard", get(dashboard_api))
        .route("/api/emergency-exit", post(emergency_exit))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("Panel {bind} adresine bağlanamadı"))?;
    println!("MTF_V4 panel listening on {bind}");
    axum::serve(listener, app)
        .await
        .context("Panel sunucusu durdu")
}

async fn dashboard_api(State(state): State<PanelState>) -> Json<DashboardSnapshot> {
    Json(state.dashboard.borrow().clone())
}

async fn emergency_exit(State(state): State<PanelState>, headers: HeaderMap) -> Response {
    let supplied = headers
        .get("x-action-token")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if !constant_time_eq(supplied.as_bytes(), state.action_token.as_bytes()) {
        return (StatusCode::FORBIDDEN, "Geçersiz işlem anahtarı").into_response();
    }
    let (reply_tx, reply_rx) = oneshot::channel();
    if state
        .emergency
        .send(EmergencyCommand { reply: reply_tx })
        .await
        .is_err()
    {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Portföy motoruna ulaşılamadı",
        )
            .into_response();
    }
    match reply_rx.await {
        Ok(Ok(summary)) => Json(serde_json::json!({
            "ok": true,
            "closed_positions": summary.closed_positions,
            "fallback_quotes": summary.fallback_quotes,
            "entries_paused": true
        }))
        .into_response(),
        Ok(Err(error)) => (StatusCode::INTERNAL_SERVER_ERROR, error).into_response(),
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            "Acil çıkış yanıtı alınamadı",
        )
            .into_response(),
    }
}

async fn index(State(state): State<PanelState>) -> Html<String> {
    let token =
        serde_json::to_string(state.action_token.as_ref()).unwrap_or_else(|_| "\"\"".to_string());
    Html(
        PANEL_HTML
            .replace("__ACTION_TOKEN__", &token)
            .replace("__STRATEGY_VERSION__", STRATEGY_VERSION),
    )
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    let length = left.len().max(right.len());
    for index in 0..length {
        let left_byte = left.get(index).copied().unwrap_or_default();
        let right_byte = right.get(index).copied().unwrap_or_default();
        difference |= usize::from(left_byte ^ right_byte);
    }
    difference == 0
}

const PANEL_HTML: &str = r#"<!doctype html>
<html lang="tr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Quant Futures · __STRATEGY_VERSION__</title>
<style>
:root{color-scheme:dark;--bg:#05080d;--panel:#0e1623;--panel2:#101c2c;--line:#213149;--muted:#8291a8;--text:#f6f8fc;--gold:#f5c542;--green:#26d99a;--red:#ff5277;--blue:#4c8dff}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 15% -10%,#10213b 0,transparent 34%),radial-gradient(circle at 95% 0,#2b2210 0,transparent 25%),var(--bg);color:var(--text);font:14px Inter,ui-sans-serif,system-ui,-apple-system,Segoe UI,sans-serif;min-height:100vh}
.shell{max-width:1580px;margin:auto;padding:22px}.topbar,.metric,.position,.empty,.history{background:linear-gradient(145deg,rgba(19,30,47,.97),rgba(8,14,23,.97));border:1px solid var(--line);box-shadow:0 22px 60px rgba(0,0,0,.28),inset 0 1px rgba(255,255,255,.035)}
.topbar{border-radius:20px;padding:16px 18px;display:flex;align-items:center;gap:14px}.logo{width:48px;height:48px;border-radius:14px;display:grid;place-items:center;background:linear-gradient(145deg,#ffe069,#e7a900);color:#090b0f;font-weight:1000;font-size:21px;box-shadow:0 8px 22px #f5c54233}.brand h1{margin:0;font-size:20px}.brand p{margin:4px 0 0;color:var(--muted);font-size:12px}.spacer{flex:1}.badge{padding:8px 12px;border-radius:10px;background:#f5c542;color:#161204;font-size:11px;font-weight:900;letter-spacing:.08em}.status{color:#b6c2d5;padding:10px 13px;border:1px solid var(--line);border-radius:12px}.dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--green);box-shadow:0 0 13px var(--green);margin-right:8px}
.panic{border:1px solid #ff527788;background:linear-gradient(145deg,#441426,#230914);color:#fff;border-radius:12px;padding:11px 15px;font-weight:900;cursor:pointer;box-shadow:0 8px 20px #ff527722}.panic:hover{transform:translateY(-1px);filter:brightness(1.12)}.panic:disabled{opacity:.55;cursor:wait}
.metrics{display:grid;grid-template-columns:repeat(6,1fr);gap:12px;margin:18px 0 26px}.metric{border-radius:16px;padding:16px;min-height:92px}.label{color:var(--muted);font-size:10px;letter-spacing:.11em;text-transform:uppercase}.value{font-size:21px;font-weight:850;margin-top:12px}.positive{color:var(--green)}.negative{color:var(--red)}
.section-head{display:flex;align-items:end;margin:0 2px 12px}.section-head h2{font-size:16px;margin:0}.section-head small{margin-left:auto;color:var(--muted)}.positions{display:grid;grid-template-columns:repeat(3,1fr);gap:14px}.position{position:relative;border-radius:20px;padding:18px;overflow:hidden;min-height:260px}.position.long{border-color:#26d99a55}.position.short{border-color:#ff527755}.position:after{content:"";position:absolute;left:0;right:0;bottom:0;height:4px;background:var(--side)}.position.long{--side:var(--green)}.position.short{--side:var(--red)}.phead{display:flex;gap:10px;align-items:start}.symbol{font-size:22px;font-weight:950}.meta{color:var(--muted);font-size:12px;margin-top:5px}.side{margin-left:auto;border:1px solid var(--side);color:var(--side);border-radius:9px;padding:7px 10px;font-weight:900}.pnl{text-align:right;font-size:22px;font-weight:900;color:var(--side);min-width:110px}.grid3{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;border-top:1px solid var(--line);border-bottom:1px solid var(--line);padding:14px 0;margin:17px 0}.cell{background:#080e17;border:1px solid #162237;border-radius:10px;padding:10px}.cell b{display:block;margin-top:7px}.levels{display:grid;grid-template-columns:repeat(3,1fr);gap:9px;color:var(--muted);font-size:11px}.levels b{display:block;color:var(--text);margin-top:4px}.empty{border-radius:18px;min-height:180px;display:grid;place-items:center;color:var(--muted);grid-column:1/-1}.footer{text-align:center;color:#526079;font-size:10px;margin-top:26px}
@media(max-width:1150px){.metrics{grid-template-columns:repeat(3,1fr)}.positions{grid-template-columns:repeat(2,1fr)}}@media(max-width:720px){.shell{padding:12px}.topbar{flex-wrap:wrap}.spacer{display:none}.status{order:4;width:100%}.metrics{grid-template-columns:repeat(2,1fr)}.positions{grid-template-columns:1fr}.panic{margin-left:auto}.badge{display:none}}
</style>
</head>
<body>
<main class="shell">
  <header class="topbar">
    <div class="logo">Q</div><div class="brand"><h1>Quant Futures</h1><p>MTF_V4 · maliyet kontrollü piyasa simülasyonu</p></div>
    <div class="spacer"></div><span class="badge">SİMÜLASYON</span>
    <div class="status"><span class="dot"></span><span id="status">Motor başlatılıyor</span></div>
    <button class="panic" id="panic">ACİL ÇIKIŞ · TÜMÜNÜ KAPAT</button>
  </header>
  <section class="metrics">
    <div class="metric"><div class="label">Toplam Bakiye</div><div class="value" id="balance">—</div></div>
    <div class="metric"><div class="label">Gerçekleşen Net K/Z</div><div class="value" id="realized">—</div></div>
    <div class="metric"><div class="label">Açık Net K/Z</div><div class="value" id="unrealized">—</div></div>
    <div class="metric"><div class="label">Kullanılan Marjin</div><div class="value" id="used">—</div></div>
    <div class="metric"><div class="label">Serbest Marjin</div><div class="value" id="free">—</div></div>
    <div class="metric"><div class="label">Takip Edilen Piyasa</div><div class="value" id="tracked">—</div></div>
  </section>
  <div class="section-head"><h2>Açık Pozisyonlar</h2><small id="summary">—</small></div>
  <section class="positions" id="positions"><div class="empty">Piyasa verisi bekleniyor…</div></section>
  <div class="footer">Komisyon, slippage ve fonlama net PnL içindedir · Son bacak sabit TP olmadan trendi izler.</div>
</main>
<script>
const ACTION_TOKEN=__ACTION_TOKEN__;
const money=new Intl.NumberFormat('tr-TR',{minimumFractionDigits:2,maximumFractionDigits:2});
const qty=new Intl.NumberFormat('tr-TR',{maximumFractionDigits:6});
const price=v=>new Intl.NumberFormat('tr-TR',{minimumFractionDigits:v>=100?2:4,maximumFractionDigits:v>=100?2:8}).format(v);
const cash=v=>`${money.format(v)} USDT`;
const tone=(el,v)=>{el.classList.toggle('positive',v>0);el.classList.toggle('negative',v<0)};
function setMoney(id,v){const e=document.getElementById(id);e.textContent=cash(v);tone(e,v)}
function render(d){
 document.getElementById('status').textContent=d.status;
 document.querySelector('.dot').style.background=d.entries_paused?'#ff5277':'#26d99a';
 setMoney('balance',d.balance);setMoney('realized',d.realized_net_pnl);setMoney('unrealized',d.unrealized_net_pnl);
 document.getElementById('used').textContent=cash(d.used_margin);document.getElementById('free').textContent=cash(d.free_margin);
 document.getElementById('tracked').textContent=money.format(d.tracked_symbols);
 document.getElementById('summary').textContent=`${d.positions.length} pozisyon · ${d.strategy_version}`;
 const root=document.getElementById('positions');root.replaceChildren();
 if(!d.positions.length){const e=document.createElement('div');e.className='empty';e.textContent='Henüz açık pozisyon yok.';root.append(e);return}
 for(const p of d.positions){
  const card=document.createElement('article');card.className=`position ${p.side==='LONG'?'long':'short'}`;
  const total=p.realized_net_pnl+p.unrealized_net_pnl;
  card.innerHTML=`<div class="phead"><div><div class="symbol"></div><div class="meta"></div></div><span class="side"></span><div class="pnl"></div></div>
  <div class="grid3"><div class="cell"><span class="label">Giriş</span><b>${price(p.entry)}</b></div><div class="cell"><span class="label">Anlık</span><b>${price(p.current)}</b></div><div class="cell"><span class="label">Kalan Miktar</span><b>${qty.format(p.remaining_quantity)}</b></div></div>
  <div class="levels"><span>Stop<b>${price(p.stop)}</b></span><span>TP1 · %40<b>${price(p.tp1)}</b></span><span>TP2 · %40<b>${price(p.tp2)}</b></span><span>Kalan Marjin<b>${cash(p.remaining_margin)}</b></span><span>Gerçekleşen<b>${cash(p.realized_net_pnl)}</b></span><span>Fonlama<b>${cash(p.funding_cost)}</b></span></div>`;
  card.querySelector('.symbol').textContent=p.symbol;card.querySelector('.meta').textContent=`#${p.id} · ${p.leverage}x · ${p.stage} · runner %20`;
  card.querySelector('.side').textContent=p.side;card.querySelector('.pnl').textContent=(total>=0?'+':'')+cash(total);root.append(card);
 }
}
async function refresh(){try{const r=await fetch('/api/dashboard',{cache:'no-store'});if(r.ok)render(await r.json())}catch{}}
document.getElementById('panic').onclick=async function(){this.disabled=true;this.textContent='KAPATILIYOR…';try{const r=await fetch('/api/emergency-exit',{method:'POST',headers:{'x-action-token':ACTION_TOKEN}});const x=await r.json();if(!r.ok)throw new Error(x);this.textContent=`KAPATILDI · ${x.closed_positions} POZİSYON`;await refresh()}catch(e){this.disabled=false;this.textContent='HATA · TEKRAR DENE'}};
refresh();setInterval(refresh,1000);
</script>
</body>
</html>"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn action_token_comparison_is_exact() {
        assert!(constant_time_eq(b"same", b"same"));
        assert!(!constant_time_eq(b"same", b"different"));
        assert!(!constant_time_eq(b"same", b"same-longer"));
    }
}
