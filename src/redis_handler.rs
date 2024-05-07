use crate::{BalanceChangeSwap, PoolChangeEvent, RawPoolSwap, TradeContext, TradeEventHandler};
use async_trait::async_trait;
use redis::{streams::StreamMaxlen, AsyncCommands};

pub struct PushToRedisStream<C: AsyncCommands + Sync> {
    connection: C,
    max_stream_size: usize,
}

impl<C: AsyncCommands + Sync> PushToRedisStream<C> {
    pub fn new(connection: C, max_stream_size: usize) -> Self {
        Self {
            connection,
            max_stream_size,
        }
    }
}

#[async_trait]
impl<C: AsyncCommands + Sync + 'static> TradeEventHandler for PushToRedisStream<C> {
    async fn on_raw_pool_swap(&mut self, context: &TradeContext, swap: &RawPoolSwap) {
        let response: String = self
            .connection
            .xadd_maxlen(
                "trade_pool",
                StreamMaxlen::Approx(self.max_stream_size),
                format!("{}-*", context.block_height),
                &[
                    ("swap", serde_json::to_string(&swap).unwrap()),
                    ("context", serde_json::to_string(&context).unwrap()),
                ],
            )
            .await
            .unwrap();
        log::debug!("Adding to stream: {response}");
    }

    async fn on_balance_change_swap(
        &mut self,
        context: &TradeContext,
        balance_changes: &BalanceChangeSwap,
    ) {
        let response: String = self
            .connection
            .xadd_maxlen(
                "trade_swap",
                StreamMaxlen::Approx(self.max_stream_size),
                format!("{}-*", context.block_height),
                &[
                    (
                        "balance_change",
                        serde_json::to_string(&balance_changes).unwrap(),
                    ),
                    ("context", serde_json::to_string(&context).unwrap()),
                ],
            )
            .await
            .unwrap();
        log::debug!("Adding to stream: {response}");
    }

    async fn on_pool_change(&mut self, event: &PoolChangeEvent) {
        let response: String = self
            .connection
            .xadd_maxlen(
                "trade_pool_change",
                StreamMaxlen::Approx(self.max_stream_size),
                format!("{}-*", event.block_height),
                &[("pool_change", serde_json::to_string(&event).unwrap())],
            )
            .await
            .unwrap();
        log::debug!("Adding to stream: {response}");
    }
}
