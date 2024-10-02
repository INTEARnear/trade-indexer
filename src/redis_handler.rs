use std::collections::HashMap;

use crate::meme_cooking_deposit_detection::{DepositEvent, WithdrawEvent};
use crate::{
    ref_finance_state, BalanceChangeSwap, PoolChangeEvent, PoolId, PoolType, RawPoolSwap,
    TradeContext, TradeEventHandler,
};
use async_trait::async_trait;
use inevents_redis::RedisEventStream;
use inindexer::near_indexer_primitives::types::AccountId;
use intear_events::events::trade::liquidity_pool::{LiquidityPoolEvent, LiquidityPoolEventData};
use intear_events::events::trade::memecooking_deposit::MemeCookingDepositEvent;
use intear_events::events::trade::memecooking_withdraw::MemeCookingWithdrawEvent;
use intear_events::events::trade::trade_pool::TradePoolEvent;
use intear_events::events::trade::trade_pool_change::TradePoolChangeEvent;
use intear_events::events::trade::trade_swap::TradeSwapEvent;
use intear_events::events::trade::{
    memecooking_deposit::MemeCookingDepositEventData,
    memecooking_withdraw::MemeCookingWithdrawEventData,
    trade_pool::TradePoolEventData,
    trade_pool_change::{
        RefPool, RefRatedSwapPool, RefSimplePool, RefStableSwapPool, RefSwapVolume,
        TradePoolChangeEventData,
    },
    trade_swap::TradeSwapEventData,
};
use redis::aio::ConnectionManager;

pub struct PushToRedisStream {
    pool_stream: RedisEventStream<TradePoolEventData>,
    swap_stream: RedisEventStream<TradeSwapEventData>,
    pool_change_stream: RedisEventStream<TradePoolChangeEventData>,
    meme_cooking_deposit_stream: RedisEventStream<MemeCookingDepositEventData>,
    meme_cooking_withdraw_stream: RedisEventStream<MemeCookingWithdrawEventData>,
    liquidity_pool_stream: RedisEventStream<LiquidityPoolEventData>,
    max_stream_size: usize,
}

impl PushToRedisStream {
    pub async fn new(
        connection: ConnectionManager,
        max_stream_size: usize,
        is_testnet: bool,
    ) -> Self {
        Self {
            pool_stream: RedisEventStream::new(
                connection.clone(),
                if is_testnet {
                    format!("{}_testnet", TradePoolEvent::ID)
                } else {
                    TradePoolEvent::ID.to_string()
                },
            ),
            swap_stream: RedisEventStream::new(
                connection.clone(),
                if is_testnet {
                    format!("{}_testnet", TradeSwapEvent::ID)
                } else {
                    TradeSwapEvent::ID.to_string()
                },
            ),
            pool_change_stream: RedisEventStream::new(
                connection.clone(),
                if is_testnet {
                    format!("{}_testnet", TradePoolChangeEvent::ID)
                } else {
                    TradePoolChangeEvent::ID.to_string()
                },
            ),
            meme_cooking_deposit_stream: RedisEventStream::new(
                connection.clone(),
                if is_testnet {
                    format!("{}_testnet", MemeCookingDepositEvent::ID)
                } else {
                    MemeCookingDepositEvent::ID.to_string()
                },
            ),
            meme_cooking_withdraw_stream: RedisEventStream::new(
                connection.clone(),
                if is_testnet {
                    format!("{}_testnet", MemeCookingWithdrawEvent::ID)
                } else {
                    MemeCookingWithdrawEvent::ID.to_string()
                },
            ),
            liquidity_pool_stream: RedisEventStream::new(
                connection.clone(),
                if is_testnet {
                    format!("{}_testnet", LiquidityPoolEvent::ID)
                } else {
                    LiquidityPoolEvent::ID.to_string()
                },
            ),
            max_stream_size,
        }
    }
}

#[async_trait]
impl TradeEventHandler for PushToRedisStream {
    async fn on_raw_pool_swap(&mut self, context: TradeContext, swap: RawPoolSwap) {
        self.pool_stream
            .emit_event(
                context.block_height,
                TradePoolEventData {
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
                },
                self.max_stream_size,
            )
            .await
            .expect("Failed to emit pool event");
    }

    async fn on_balance_change_swap(
        &mut self,
        context: TradeContext,
        balance_changes: BalanceChangeSwap,
    ) {
        self.swap_stream
            .emit_event(
                context.block_height,
                TradeSwapEventData {
                    balance_changes: balance_changes.balance_changes,

                    trader: context.trader,
                    block_height: context.block_height,
                    block_timestamp_nanosec: context.block_timestamp_nanosec,
                    transaction_id: context.transaction_id,
                    receipt_id: context.receipt_id,
                },
                self.max_stream_size,
            )
            .await
            .expect("Failed to emit swap event");
    }

    async fn on_pool_change(&mut self, event: PoolChangeEvent) {
        self.pool_change_stream
            .emit_event(
                event.block_height,
                TradePoolChangeEventData {
                    pool_id: event.pool_id,
                    pool: match event.pool {
                        PoolType::Ref(pool) => {
                            intear_events::events::trade::trade_pool_change::PoolType::Ref(
                                match pool {
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
                                },
                            )
                        }
                    },
                    block_height: event.block_height,
                    block_timestamp_nanosec: event.block_timestamp_nanosec,
                    receipt_id: event.receipt_id,
                },
                self.max_stream_size,
            )
            .await
            .expect("Failed to emit pool change event");
    }

    async fn on_memecooking_deposit(&mut self, context: TradeContext, deposit: DepositEvent) {
        self.meme_cooking_deposit_stream
            .emit_event(
                context.block_height,
                MemeCookingDepositEventData {
                    meme_id: deposit.meme_id,
                    amount: deposit.amount,
                    protocol_fee: deposit.protocol_fee,
                    referrer: deposit.referrer,
                    referrer_fee: deposit.referrer_fee,

                    trader: context.trader,
                    block_height: context.block_height,
                    block_timestamp_nanosec: context.block_timestamp_nanosec,
                    transaction_id: context.transaction_id,
                    receipt_id: context.receipt_id,
                },
                self.max_stream_size,
            )
            .await
            .expect("Failed to emit meme cooking deposit event");
    }

    async fn on_memecooking_withdraw(&mut self, context: TradeContext, withdraw: WithdrawEvent) {
        self.meme_cooking_withdraw_stream
            .emit_event(
                context.block_height,
                MemeCookingWithdrawEventData {
                    meme_id: withdraw.meme_id,
                    amount: withdraw.amount,
                    fee: withdraw.fee,

                    trader: context.trader,
                    block_height: context.block_height,
                    block_timestamp_nanosec: context.block_timestamp_nanosec,
                    transaction_id: context.transaction_id,
                    receipt_id: context.receipt_id,
                },
                self.max_stream_size,
            )
            .await
            .expect("Failed to emit meme cooking withdraw event");
    }

    async fn on_liquidity_pool(
        &mut self,
        context: TradeContext,
        pool_id: PoolId,
        tokens: HashMap<AccountId, i128>,
    ) {
        self.liquidity_pool_stream
            .emit_event(
                context.block_height,
                LiquidityPoolEventData {
                    pool: pool_id,
                    tokens,
                    provider_account_id: context.trader,
                    block_height: context.block_height,
                    block_timestamp_nanosec: context.block_timestamp_nanosec,
                    transaction_id: context.transaction_id,
                    receipt_id: context.receipt_id,
                },
                self.max_stream_size,
            )
            .await
            .expect("Failed to emit liquidity pool event");
    }
}
