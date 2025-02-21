use std::collections::HashMap;

use crate::ref_finance_state;
use crate::{
    BalanceChangeSwap, PoolChangeEvent, PoolId, PoolType, RawPoolSwap, TradeContext,
    TradeEventHandler,
};
use async_trait::async_trait;
use inevents_redis::RedisEventStream;
use inindexer::near_indexer_primitives::types::{AccountId, BlockHeight};
use intear_events::events::trade::liquidity_pool::LiquidityPoolEvent;
use intear_events::events::trade::memecooking_deposit::MemeCookingDepositEvent;
use intear_events::events::trade::memecooking_withdraw::MemeCookingWithdrawEvent;
use intear_events::events::trade::trade_pool::TradePoolEvent;
use intear_events::events::trade::trade_pool_change::TradePoolChangeEvent;
use intear_events::events::trade::trade_pool_change::{
    RefPool, RefRatedSwapPool, RefSimplePool, RefStableSwapPool, RefSwapVolume,
};
use intear_events::events::trade::trade_swap::TradeSwapEvent;
use redis::aio::ConnectionManager;

pub struct PushToRedisStream {
    pool_stream: RedisEventStream<TradePoolEvent>,
    swap_stream: RedisEventStream<TradeSwapEvent>,
    pool_change_stream: RedisEventStream<TradePoolChangeEvent>,
    meme_cooking_deposit_stream: RedisEventStream<MemeCookingDepositEvent>,
    meme_cooking_withdraw_stream: RedisEventStream<MemeCookingWithdrawEvent>,
    liquidity_pool_stream: RedisEventStream<LiquidityPoolEvent>,
    max_stream_size: usize,
}

impl PushToRedisStream {
    pub async fn new(connection: ConnectionManager, max_stream_size: usize) -> Self {
        Self {
            pool_stream: RedisEventStream::new(connection.clone(), TradePoolEvent::ID.to_string()),
            swap_stream: RedisEventStream::new(connection.clone(), TradeSwapEvent::ID.to_string()),
            pool_change_stream: RedisEventStream::new(
                connection.clone(),
                TradePoolChangeEvent::ID.to_string(),
            ),
            meme_cooking_deposit_stream: RedisEventStream::new(
                connection.clone(),
                MemeCookingDepositEvent::ID.to_string(),
            ),
            meme_cooking_withdraw_stream: RedisEventStream::new(
                connection.clone(),
                MemeCookingWithdrawEvent::ID.to_string(),
            ),
            liquidity_pool_stream: RedisEventStream::new(
                connection.clone(),
                LiquidityPoolEvent::ID.to_string(),
            ),
            max_stream_size,
        }
    }
}

#[async_trait]
impl TradeEventHandler for PushToRedisStream {
    async fn on_raw_pool_swap(&mut self, context: TradeContext, swap: RawPoolSwap) {
        self.pool_stream.add_event(TradePoolEvent {
            pool: swap.pool,
            token_in: swap.token_in,
            token_out: swap.token_out,
            amount_in: swap.amount_in,
            amount_out: swap.amount_out,
            trader: context.trader,
            block_height: context.block_height,
            block_timestamp_nanosec: context.block_timestamp_nanosec,
            transaction_id: context.transaction_id,
            receipt_id: context.receipt_id,
        });
    }

    async fn on_balance_change_swap(
        &mut self,
        context: TradeContext,
        balance_changes: BalanceChangeSwap,
    ) {
        self.swap_stream.add_event(TradeSwapEvent {
            balance_changes: balance_changes.balance_changes,
            trader: context.trader,
            block_height: context.block_height,
            block_timestamp_nanosec: context.block_timestamp_nanosec,
            transaction_id: context.transaction_id,
            receipt_id: context.receipt_id,
        });
    }

    async fn on_pool_change(&mut self, event: PoolChangeEvent) {
        self.pool_change_stream.add_event(TradePoolChangeEvent {
            pool_id: event.pool_id.clone(),
            pool: match event.pool {
                PoolType::Ref(pool) => {
                    intear_events::events::trade::trade_pool_change::PoolType::Ref(match pool {
                        ref_finance_state::Pool::SimplePool(pool) => {
                            RefPool::SimplePool(RefSimplePool {
                                token_account_ids: pool
                                    .token_account_ids
                                    .into_iter()
                                    .map(|account_id| account_id.parse().unwrap())
                                    .collect(),
                                amounts: pool.amounts,
                                volumes: pool
                                    .volumes
                                    .into_iter()
                                    .map(|volume| RefSwapVolume {
                                        input: volume.input,
                                        output: volume.output,
                                    })
                                    .collect(),
                                total_fee: pool.total_fee,
                                exchange_fee: pool.exchange_fee,
                                referral_fee: pool.referral_fee,
                                shares_total_supply: pool.shares_total_supply,
                            })
                        }
                        ref_finance_state::Pool::StableSwapPool(pool) => {
                            RefPool::StableSwapPool(RefStableSwapPool {
                                token_account_ids: pool
                                    .token_account_ids
                                    .into_iter()
                                    .map(|account_id| account_id.parse().unwrap())
                                    .collect(),
                                token_decimals: pool.token_decimals,
                                c_amounts: pool.c_amounts,
                                volumes: pool
                                    .volumes
                                    .into_iter()
                                    .map(|volume| RefSwapVolume {
                                        input: volume.input,
                                        output: volume.output,
                                    })
                                    .collect(),
                                total_fee: pool.total_fee,
                                shares_total_supply: pool.shares_total_supply,
                                init_amp_factor: pool.init_amp_factor,
                                target_amp_factor: pool.target_amp_factor,
                                init_amp_time: pool.init_amp_time,
                                stop_amp_time: pool.stop_amp_time,
                            })
                        }
                        ref_finance_state::Pool::RatedSwapPool(pool) => {
                            RefPool::RatedSwapPool(RefRatedSwapPool {
                                token_account_ids: pool
                                    .token_account_ids
                                    .into_iter()
                                    .map(|account_id| account_id.parse().unwrap())
                                    .collect(),
                                token_decimals: pool.token_decimals,
                                c_amounts: pool.c_amounts,
                                volumes: pool
                                    .volumes
                                    .into_iter()
                                    .map(|volume| RefSwapVolume {
                                        input: volume.input,
                                        output: volume.output,
                                    })
                                    .collect(),
                                total_fee: pool.total_fee,
                                shares_total_supply: pool.shares_total_supply,
                                init_amp_factor: pool.init_amp_factor,
                                target_amp_factor: pool.target_amp_factor,
                                init_amp_time: pool.init_amp_time,
                                stop_amp_time: pool.stop_amp_time,
                            })
                        }
                    })
                }
                PoolType::Aidols(pool) => {
                    intear_events::events::trade::trade_pool_change::PoolType::Aidols(pool)
                }
                PoolType::GraFun(pool) => {
                    intear_events::events::trade::trade_pool_change::PoolType::GraFun(pool)
                }
            },
            block_height: event.block_height,
            block_timestamp_nanosec: event.block_timestamp_nanosec,
            receipt_id: event.receipt_id,
        });
    }

    async fn on_liquidity_pool(
        &mut self,
        context: TradeContext,
        pool_id: PoolId,
        tokens: HashMap<AccountId, i128>,
    ) {
        self.liquidity_pool_stream.add_event(LiquidityPoolEvent {
            pool: pool_id,
            tokens,
            provider_account_id: context.trader,
            block_height: context.block_height,
            block_timestamp_nanosec: context.block_timestamp_nanosec,
            transaction_id: context.transaction_id,
            receipt_id: context.receipt_id,
        });
    }

    async fn flush_events(&mut self, block_height: BlockHeight) {
        self.pool_stream
            .flush_events(block_height, self.max_stream_size)
            .await
            .expect("Failed to flush pool stream");
        self.swap_stream
            .flush_events(block_height, self.max_stream_size)
            .await
            .expect("Failed to flush swap stream");
        self.pool_change_stream
            .flush_events(block_height, self.max_stream_size)
            .await
            .expect("Failed to flush pool change stream");
        self.meme_cooking_deposit_stream
            .flush_events(block_height, self.max_stream_size)
            .await
            .expect("Failed to flush meme cooking deposit stream");
        self.meme_cooking_withdraw_stream
            .flush_events(block_height, self.max_stream_size)
            .await
            .expect("Failed to flush meme cooking withdraw stream");
        self.liquidity_pool_stream
            .flush_events(block_height, self.max_stream_size)
            .await
            .expect("Failed to flush liquidity pool stream");
    }
}
