use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use dashmap::DashMap;
use dotenvy::dotenv;
use quant_bot::{
    config::Config,
    cost::CostModel,
    engine::{run_feature_engine, HotSet},
    market::{run_market_stream, run_universe_manager, SymbolStore, Universe},
    model::Side,
    panel::{run_panel, DashboardSnapshot, EmergencyCommand, PositionView},
    portfolio::{PortfolioEngine, PortfolioLimits, Quote},
    position::{PositionPolicy, PositionStage},
    storage::SqliteStore,
};
use tokio::{
    sync::{mpsc, watch},
    time,
};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    let config = Config::from_env()?;
    let costs = CostModel {
        entry_fee_rate: config.taker_fee_rate,
        exit_fee_rate: config.taker_fee_rate,
        expected_entry_slippage_bps: config.expected_entry_slippage_bps,
        expected_exit_slippage_bps: config.expected_exit_slippage_bps,
        safety_buffer_bps: config.break_even_safety_bps,
    };
    let client = reqwest::Client::builder()
        .user_agent("quant-simulator/0.2 MTF_V4")
        .build()?;
    let universe = Universe::new();
    let store: SymbolStore = Arc::new(DashMap::new());
    let (universe_ready_tx, mut universe_ready_rx) = watch::channel(false);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (hot_tx, mut hot_rx) = watch::channel(Vec::new());
    let hot_set: HotSet = Arc::new(hot_tx);
    let (dashboard_tx, dashboard_rx) = watch::channel(DashboardSnapshot::default());
    let (emergency_tx, mut emergency_rx) = mpsc::channel::<EmergencyCommand>(4);

    tokio::spawn(run_universe_manager(
        universe.clone(),
        client,
        config.clone(),
        universe_ready_tx,
    ));
    universe_ready_rx.wait_for(|ready| *ready).await?;

    let database =
        Arc::new(SqliteStore::open(&config.database_path).context("MTF_V4 SQLite başlatılamadı")?);
    let portfolio_snapshot = database
        .load_or_create(config.initial_balance, now_millis())
        .context("MTF_V4 AppState geri yüklenemedi")?;
    let mut portfolio = PortfolioEngine::new(
        portfolio_snapshot,
        database.clone(),
        costs,
        PositionPolicy::default(),
        PortfolioLimits {
            max_positions: config.max_positions,
            leverage: config.leverage,
            risk_per_trade: config.risk_per_trade,
            max_portfolio_risk: config.max_portfolio_risk,
            max_trade_allocation: config.max_trade_allocation,
        },
    )?;

    tokio::spawn(run_market_stream(
        config.clone(),
        universe.clone(),
        store.clone(),
        shutdown_rx.clone(),
    ));
    tokio::spawn(run_feature_engine(
        config.clone(),
        store.clone(),
        hot_set,
        shutdown_rx,
    ));
    let panel_bind = config.panel_bind;
    let panel_action_token = config.panel_action_token.clone();
    tokio::spawn(async move {
        if let Err(error) =
            run_panel(panel_bind, dashboard_rx, emergency_tx, panel_action_token).await
        {
            eprintln!("Panel error: {error:#}");
        }
    });

    println!(
        "MTF_V4 mass-market engine started with {} Futures symbols; restored_positions={}",
        universe.len(),
        portfolio.snapshot().positions.len()
    );

    let mut position_interval = time::interval(Duration::from_millis(250));
    position_interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut dashboard_interval = time::interval(Duration::from_secs(1));
    dashboard_interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut last_entry_attempt: HashMap<String, i64> = HashMap::new();
    let mut funding_schedule: HashMap<String, (i64, f64)> = HashMap::new();
    let shutdown_signal = shutdown_signal();
    tokio::pin!(shutdown_signal);

    loop {
        tokio::select! {
            _ = &mut shutdown_signal => {
                shutdown_tx.send_replace(true);
                database.checkpoint()?;
                break;
            }
            _ = position_interval.tick() => {
                let now = now_millis();
                let symbols: Vec<String> = portfolio
                    .snapshot()
                    .positions
                    .iter()
                    .map(|position| position.symbol.clone())
                    .collect();
                for symbol in symbols {
                    let Some(state) = store.get(&symbol) else {
                        continue;
                    };
                    let Some(meta) = universe.meta(&symbol) else {
                        continue;
                    };
                    let quote = Quote {
                        bid: state.bid,
                        ask: state.ask,
                        atr: (state.last_price * state.features.volatility / 100.0)
                            .max(state.last_price * 0.001),
                        structure_stop: None,
                    };
                    let next_funding = state.next_funding_time;
                    let funding_rate = state.funding_rate;
                    let mark_price = state.mark_price;
                    drop(state);

                    portfolio
                        .process_quote(&symbol, quote, meta.step_size, now)
                        .with_context(|| {
                            format!("{symbol} pozisyon güncellemesi başarısız")
                        })?;

                    if next_funding > 0 {
                        match funding_schedule.insert(
                            symbol.clone(),
                            (next_funding, funding_rate),
                        ) {
                            Some((previous_time, previous_rate))
                                if previous_time != next_funding =>
                            {
                                portfolio
                                    .apply_funding(
                                        &symbol,
                                        mark_price,
                                        previous_rate,
                                        now,
                                    )
                                    .with_context(|| {
                                        format!("{symbol} funding kaydı başarısız")
                                    })?;
                            }
                            _ => {}
                        }
                    }
                }
            }
            _ = dashboard_interval.tick() => {
                dashboard_tx.send_replace(build_dashboard(
                    &portfolio,
                    &store,
                    config.leverage,
                    config.entry_enabled,
                    store.len(),
                    now_millis(),
                ));
            }
            command = emergency_rx.recv() => {
                let Some(command) = command else {
                    continue;
                };
                let symbols: Vec<String> = portfolio
                    .snapshot()
                    .positions
                    .iter()
                    .map(|position| position.symbol.clone())
                    .collect();
                let mut quotes = HashMap::new();
                for symbol in symbols {
                    if let Some(state) = store.get(&symbol) {
                        quotes.insert(
                            symbol,
                            Quote {
                                bid: state.bid,
                                ask: state.ask,
                                atr: (state.last_price * 0.001).max(f64::EPSILON),
                                structure_stop: None,
                            },
                        );
                    }
                }
                let result = portfolio
                    .emergency_close_all(&quotes, now_millis())
                    .map_err(|error| format!("{error:#}"));
                let _ = command.reply.send(result);
                dashboard_tx.send_replace(build_dashboard(
                    &portfolio,
                    &store,
                    config.leverage,
                    config.entry_enabled,
                    store.len(),
                    now_millis(),
                ));
            }
            changed = hot_rx.changed() => {
                if changed.is_err() {
                    break;
                }
                let hot = hot_rx.borrow().clone();
                if let Some(best) = hot.first() {
                    println!(
                        "hot_set={} best={} score={:.1} tracked={}",
                        hot.len(),
                        best.symbol,
                        best.score,
                        store.len()
                    );
                }
                if config.entry_enabled && !portfolio.snapshot().entries_paused {
                    let now = now_millis();
                    for candidate in &hot {
                        if portfolio.snapshot().positions.len() >= config.max_positions {
                            break;
                        }
                        if last_entry_attempt
                            .get(&candidate.symbol)
                            .is_some_and(|last| now.saturating_sub(*last) < 60_000)
                        {
                            continue;
                        }
                        let (Some(state), Some(meta)) = (
                            store.get(&candidate.symbol),
                            universe.meta(&candidate.symbol),
                        ) else {
                            continue;
                        };
                        let quote = Quote {
                            bid: state.bid,
                            ask: state.ask,
                            atr: candidate.stop_distance / 2.2,
                            structure_stop: None,
                        };
                        drop(state);
                        last_entry_attempt.insert(candidate.symbol.clone(), now);
                        let regime = match candidate.side {
                            Side::Long => "SYMBOL_BULL",
                            Side::Short => "SYMBOL_BEAR",
                        };
                        let result = portfolio
                            .try_open(candidate, &meta, quote, regime, now)
                            .with_context(|| {
                                format!("{} giriş kararı kaydedilemedi", candidate.symbol)
                            })?;
                        if let Ok(position_id) = result {
                            println!(
                                "OPEN id={} symbol={} side={:?} confidence={:.2}",
                                position_id,
                                candidate.symbol,
                                candidate.side,
                                candidate.confidence
                            );
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut terminate =
            signal(SignalKind::terminate()).expect("SIGTERM handler oluşturulamadı");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn build_dashboard(
    portfolio: &PortfolioEngine,
    store: &SymbolStore,
    leverage: f64,
    runtime_entry_enabled: bool,
    tracked_symbols: usize,
    now: i64,
) -> DashboardSnapshot {
    let costs = portfolio.costs();
    let mut unrealized_net_pnl = 0.0;
    let mut positions = Vec::new();
    for tracked in &portfolio.snapshot().positions {
        let state = store.get(&tracked.symbol);
        let (current, open_pnl) = state
            .as_ref()
            .and_then(|state| {
                let current = tracked.position.side.favorable_price(state.bid, state.ask);
                let exit_fill =
                    costs.estimated_exit_fill(tracked.position.side, state.bid, state.ask)?;
                let pnl = costs.net_pnl(
                    tracked.position.side,
                    tracked.position.entry_fill,
                    exit_fill,
                    tracked.position.remaining_quantity,
                    tracked.position.entry_fee_remaining,
                    tracked.position.funding_cost,
                );
                Some((current, pnl))
            })
            .unwrap_or((tracked.position.entry_fill, 0.0));
        unrealized_net_pnl += open_pnl;
        let stage = match tracked.position.stage {
            PositionStage::BeforeTp1 => "TP1 BEKLİYOR",
            PositionStage::AfterTp1 => "TP1 GERÇEKLEŞTİ",
            PositionStage::Runner => "RUNNER",
            PositionStage::Closed => "KAPALI",
        };
        positions.push(PositionView {
            id: tracked.id,
            symbol: tracked.symbol.clone(),
            side: tracked.position.side,
            stage: stage.to_string(),
            leverage,
            entry: tracked.position.entry_fill,
            current,
            stop: tracked.position.stop,
            tp1: tracked.position.tp1,
            tp2: tracked.position.tp2,
            original_quantity: tracked.position.original_quantity,
            remaining_quantity: tracked.position.remaining_quantity,
            remaining_margin: tracked.initial_margin * tracked.position.remaining_quantity
                / tracked.position.original_quantity,
            realized_net_pnl: tracked.position.realized_net_pnl,
            unrealized_net_pnl: open_pnl,
            funding_cost: tracked.position.funding_cost,
            opened_at: tracked.opened_at,
        });
    }
    let entries_paused = !runtime_entry_enabled || portfolio.snapshot().entries_paused;
    DashboardSnapshot {
        strategy_version: quant_bot::model::STRATEGY_VERSION.to_string(),
        status: if !runtime_entry_enabled {
            "ENTRY_ENABLED=false: yeni girişler durduruldu".to_string()
        } else if entries_paused {
            portfolio
                .snapshot()
                .pause_reason
                .clone()
                .unwrap_or_else(|| "Yeni girişler durduruldu".to_string())
        } else {
            "Tüm USD-M Futures pariteleri eşzamanlı taranıyor".to_string()
        },
        entries_paused,
        balance: portfolio.snapshot().balance + unrealized_net_pnl,
        used_margin: portfolio.used_margin(),
        free_margin: portfolio.free_margin(),
        realized_net_pnl: portfolio.snapshot().realized_net_pnl,
        unrealized_net_pnl,
        tracked_symbols,
        updated_at: now,
        positions,
    }
}
