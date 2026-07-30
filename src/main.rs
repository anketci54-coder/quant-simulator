use std::sync::Arc;

use anyhow::Result;
use dashmap::DashMap;
use dotenvy::dotenv;
use quant_bot::{
    config::Config,
    engine::{run_feature_engine, HotSet},
    market::{run_market_stream, run_universe_manager, SymbolStore, Universe},
};
use tokio::sync::watch;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    let config = Config::from_env()?;
    let client = reqwest::Client::builder()
        .user_agent("quant-simulator/0.2 MTF_V4")
        .build()?;
    let universe = Universe::new();
    let store: SymbolStore = Arc::new(DashMap::new());
    let (universe_ready_tx, mut universe_ready_rx) = watch::channel(false);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (hot_tx, mut hot_rx) = watch::channel(Vec::new());
    let hot_set: HotSet = Arc::new(hot_tx);

    tokio::spawn(run_universe_manager(
        universe.clone(),
        client,
        config.clone(),
        universe_ready_tx,
    ));
    universe_ready_rx.wait_for(|ready| *ready).await?;

    tokio::spawn(run_market_stream(
        config.clone(),
        universe.clone(),
        store.clone(),
        shutdown_rx.clone(),
    ));
    tokio::spawn(run_feature_engine(
        config,
        store.clone(),
        hot_set,
        shutdown_rx,
    ));

    println!(
        "MTF_V4 mass-market engine started with {} Futures symbols",
        universe.len()
    );

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                shutdown_tx.send_replace(true);
                break;
            }
            changed = hot_rx.changed() => {
                if changed.is_err() {
                    break;
                }
                let hot = hot_rx.borrow();
                if let Some(best) = hot.first() {
                    println!(
                        "hot_set={} best={} score={:.1} tracked={}",
                        hot.len(),
                        best.symbol,
                        best.score,
                        store.len()

                    );
                }
            }
        }
    }
    Ok(())
}
