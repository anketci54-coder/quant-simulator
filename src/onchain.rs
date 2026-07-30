use std::future::Future;

use anyhow::Result;

#[derive(Clone, Copy, Debug, Default)]
pub struct OnChainContext {
    pub score: f64,
    pub confidence: f64,
    pub observed_at: i64,
}

pub trait OnChainProvider: Send + Sync {
    fn context(&self, symbol: &str) -> impl Future<Output = Result<Option<OnChainContext>>> + Send;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NeutralOnChain;

impl OnChainProvider for NeutralOnChain {
    async fn context(&self, _symbol: &str) -> Result<Option<OnChainContext>> {
        Ok(None)
    }
}
