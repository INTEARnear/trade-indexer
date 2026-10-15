use std::collections::HashMap;

use aidols_trade_detection::AIDOLS_CONTRACT_ID;
use async_trait::async_trait;
use borsh::BorshDeserialize;
use inindexer::near_utils::FtBalance;
use inindexer::transaction_trees::TransactionTree;
use inindexer::{
    Indexer, IndexerExecutionOutcomeWithReceipt, StreamerMessage,
    near_indexer_primitives::{
        CryptoHash,
        types::{AccountId, BlockHeight},
        views::{StateChangeCauseView, StateChangeValueView},
    },
};
use intear_events::events::trade::trade_pool_change::{AidolsPool, IntearPlachPool, RefDclPool};
use ref_trade_detection::{REF_CONTRACT_ID, TESTNET_REF_CONTRACT_ID};
use refdcl_trade_detection::REFDCL_CONTRACT_ID;

pub mod aggregatedex;
pub mod aidols_state;
pub mod aidols_trade_detection;
pub mod intear_plach_trade_detection;
#[cfg(feature = "redis")]
pub mod redis_handler;
pub mod ref_finance_state;
pub mod ref_trade_detection;
pub mod refdcl_state;
pub mod refdcl_trade_detection;

#[cfg(test)]
mod tests;

type PoolId = String;

pub struct TradeIndexer<T: TradeEventHandler> {
    pub handler: T,
    pub is_testnet: bool,
}

#[async_trait]
pub trait TradeEventHandler: Send + Sync + 'static {
    async fn on_raw_pool_swap(&mut self, context: TradeContext, swap: RawPoolSwap);
    async fn on_balance_change_swap(
        &mut self,
        context: TradeContext,
        balance_changes: BalanceChangeSwap,
        referrer: Option<String>,
    );
    async fn on_pool_change(&mut self, pool: PoolChangeEvent);
    async fn on_liquidity_pool(
        &mut self,
        context: TradeContext,
        pool_id: PoolId,
        tokens: HashMap<AccountId, i128>,
    );
    /// The same liquidity changes as `on_liquidity_pool`, for pools of exactly two tokens
    async fn on_liquidity_change(&mut self, context: TradeContext, change: LiquidityChange);
    async fn flush_events(&mut self, block_height: BlockHeight);
}

#[async_trait]
impl<T: TradeEventHandler> Indexer for TradeIndexer<T> {
    type Error = String;

    async fn process_block(&mut self, block: &StreamerMessage) -> Result<(), Self::Error> {
        let ref_contract_id = if self.is_testnet {
            TESTNET_REF_CONTRACT_ID
        } else {
            REF_CONTRACT_ID
        };
        for shard in block.shards.iter() {
            for state_change in shard.state_changes.iter() {
                if let StateChangeValueView::DataUpdate {
                    account_id,
                    key,
                    value,
                } = &state_change.value
                {
                    if account_id == ref_contract_id {
                        let receipt_id =
                            if let StateChangeCauseView::ReceiptProcessing { receipt_hash } =
                                &state_change.cause
                            {
                                receipt_hash
                            } else {
                                log::warn!(
                                    "Update not caused by a receipt in block {}",
                                    block.block.header.height
                                );
                                continue;
                            };
                        let key = key.as_slice();
                        // Prefix changed from b"p" to 0x00 in https://github.com/ref-finance/ref-contracts/commit/a196f4a18368f0c3d62e80ba2788c350c94e85b2
                        #[allow(clippy::if_same_then_else)]
                        let without_prefix = if key.starts_with(&[0]) {
                            &key[1..]
                        } else if key.starts_with(b"p") {
                            &key[1..]
                        } else {
                            continue;
                        };
                        if without_prefix.len() != 8 {
                            log::warn!("Invalid pool key: {:02x?}", key);
                            continue;
                        }
                        let pool_id = u64::from_le_bytes(without_prefix.try_into().unwrap());
                        log::debug!("Pool changed: {pool_id}");
                        // A layout this doesn't know would leave the pool's price behind for good
                        let pool = borsh::from_slice::<ref_finance_state::Pool>(value.as_slice())
                            .map_err(|err| {
                            format!(
                                "Ref pool {pool_id} in block {} doesn't decode: {err}",
                                block.block.header.height
                            )
                        })?;
                        if pool_id > 420_000 {
                            log::warn!(
                                "Pool ID too high, probably a bug: {pool_id}. If Ref actually has that many pools, increase the number in {}:{} to a reasonable amount",
                                file!(),
                                line!() - 1
                            );
                            continue;
                        }

                        let pool = PoolChangeEvent {
                            pool_id: ref_trade_detection::create_ref_pool_id(pool_id),
                            receipt_id: *receipt_id,
                            block_timestamp_nanosec: block.block.header.timestamp_nanosec as u128,
                            block_height: block.block.header.height,
                            pool: PoolType::Ref(pool),
                        };
                        self.handler.on_pool_change(pool).await;
                    } else if account_id == AIDOLS_CONTRACT_ID && !self.is_testnet {
                        let receipt_id =
                            if let StateChangeCauseView::ReceiptProcessing { receipt_hash } =
                                &state_change.cause
                            {
                                receipt_hash
                            } else {
                                log::warn!(
                                    "Update not caused by a receipt in block {}",
                                    block.block.header.height
                                );
                                continue;
                            };
                        let key = key.as_slice();
                        #[allow(clippy::if_same_then_else)]
                        let mut without_prefix = if let Some(data) = key.strip_prefix(&[0x00]) {
                            data
                        } else {
                            continue;
                        };
                        let Ok(token_id) =
                            <AccountId as BorshDeserialize>::deserialize(&mut without_prefix)
                        else {
                            log::warn!("Invalid account id: {:02x?}", key);
                            continue;
                        };
                        log::debug!("Pool changed: {token_id}");
                        let pool = borsh::from_slice::<aidols_state::AidolsPoolState>(
                            value.as_slice(),
                        )
                        .map_err(|err| {
                            format!(
                                "Aidols pool of {token_id} in block {} doesn't decode: {err}",
                                block.block.header.height
                            )
                        })?;
                        self.handler
                            .on_pool_change(PoolChangeEvent {
                                pool_id: aidols_trade_detection::create_aidols_pool_id(&token_id),
                                receipt_id: *receipt_id,
                                block_timestamp_nanosec: block.block.header.timestamp_nanosec
                                    as u128,
                                block_height: block.block.header.height,
                                pool: PoolType::Aidols(AidolsPool {
                                    token_id: token_id.clone(),
                                    token_hold: pool.token_hold,
                                    wnear_hold: pool.wnear_hold,
                                    is_deployed: pool.is_deployed,
                                    is_tradable: pool.is_tradable,
                                }),
                            })
                            .await;
                    } else if account_id == REFDCL_CONTRACT_ID && !self.is_testnet {
                        let receipt_id =
                            if let StateChangeCauseView::ReceiptProcessing { receipt_hash } =
                                &state_change.cause
                            {
                                receipt_hash
                            } else {
                                log::warn!(
                                    "Update not caused by a receipt in block {}",
                                    block.block.header.height
                                );
                                continue;
                            };
                        // Pools are an UnorderedMap with prefix 0x01, its values are stored at
                        // 0x01 'v' ++ u64 index
                        let key = key.as_slice();
                        if key.len() != 10 || !key.starts_with(&[0x01, b'v']) {
                            continue;
                        }
                        // Only the record's leading fields are read
                        let pool =
                            <refdcl_state::RefDclPoolPrefix as BorshDeserialize>::deserialize(
                                &mut value.as_slice(),
                            )
                            .map_err(|err| {
                                format!(
                                    "DCL pool record {key:02x?} in block {} doesn't decode: {err}",
                                    block.block.header.height
                                )
                            })?;
                        if pool.version > 1 {
                            return Err(format!(
                                "DCL pool {} in block {} has a record version this doesn't know: {}",
                                pool.pool_id, block.block.header.height, pool.version
                            ));
                        }
                        let (Ok(token_x), Ok(token_y)) = (
                            pool.token_x.parse::<AccountId>(),
                            pool.token_y.parse::<AccountId>(),
                        ) else {
                            log::warn!("Invalid DCL pool tokens: {}", pool.pool_id);
                            continue;
                        };
                        log::debug!("Pool changed: {}", pool.pool_id);
                        self.handler
                            .on_pool_change(PoolChangeEvent {
                                pool_id: refdcl_trade_detection::create_refdcl_pool_id(
                                    &pool.pool_id,
                                ),
                                receipt_id: *receipt_id,
                                block_timestamp_nanosec: block.block.header.timestamp_nanosec
                                    as u128,
                                block_height: block.block.header.height,
                                pool: PoolType::RefDcl(RefDclPool {
                                    token_x,
                                    token_y,
                                    fee: pool.fee,
                                    point_delta: pool.point_delta,
                                    current_point: pool.current_point,
                                    liquidity: pool.liquidity,
                                    liquidity_x: pool.liquidity_x,
                                    total_x: pool.total_x,
                                    total_y: pool.total_y,
                                }),
                            })
                            .await;
                    }
                }
            }
        }
        Ok(())
    }

    async fn process_receipt(
        &mut self,
        receipt: &IndexerExecutionOutcomeWithReceipt,
        transaction: &TransactionTree,
        block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        let receipt = transaction
            .receipt(&receipt.receipt.receipt_id)
            .expect("The tree has the receipts of the block it comes with");
        ref_trade_detection::detect(
            receipt,
            transaction,
            block,
            &mut self.handler,
            self.is_testnet,
        )
        .await;
        aidols_trade_detection::detect(
            receipt,
            transaction,
            block,
            &mut self.handler,
            self.is_testnet,
        )
        .await;
        refdcl_trade_detection::detect(
            receipt,
            transaction,
            block,
            &mut self.handler,
            self.is_testnet,
        )
        .await;
        intear_plach_trade_detection::detect(
            receipt,
            transaction,
            block,
            &mut self.handler,
            self.is_testnet,
        )
        .await;
        Ok(())
    }

    async fn process_block_end(&mut self, block: &StreamerMessage) -> Result<(), Self::Error> {
        self.handler.flush_events(block.block.header.height).await;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct TradeContext {
    pub trader: AccountId,
    pub block_height: BlockHeight,
    pub block_timestamp_nanosec: u128,
    pub transaction_id: CryptoHash,
    pub receipt_id: CryptoHash,
}

#[derive(Debug, PartialEq, Clone)]
pub struct RawPoolSwap {
    pub pool: PoolId,
    pub token_in: AccountId,
    pub token_out: AccountId,
    pub amount_in: FtBalance,
    pub amount_out: FtBalance,
    pub referrer: Option<AccountId>,
}

#[derive(Debug, PartialEq)]
pub struct BalanceChangeSwap {
    pub balance_changes: HashMap<AccountId, i128>,
    pub pool_swaps: Vec<RawPoolSwap>,
}

impl BalanceChangeSwap {
    pub fn new(pool_swaps: Vec<RawPoolSwap>) -> Option<Self> {
        let mut balance_changes = HashMap::new();
        for swap in &pool_swaps {
            let (Ok(amount_in), Ok(amount_out)) = (
                i128::try_from(swap.amount_in),
                i128::try_from(swap.amount_out),
            ) else {
                log::warn!("Amount overflow in swap: {swap:?}");
                continue;
            };
            *balance_changes.entry(swap.token_in.clone()).or_insert(0) -= amount_in;
            *balance_changes.entry(swap.token_out.clone()).or_insert(0) += amount_out;
        }
        balance_changes.retain(|_, change| *change != 0);
        (!balance_changes.is_empty()).then_some(Self {
            balance_changes,
            pool_swaps,
        })
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum LiquidityChangeKind {
    Add,
    Remove,
}

#[derive(Debug, PartialEq, Clone)]
pub struct LiquidityChange {
    pub pool: PoolId,
    pub kind: LiquidityChangeKind,
    pub token_a: AccountId,
    pub amount_a: FtBalance,
    pub token_b: AccountId,
    pub amount_b: FtBalance,
}

/// Reports a liquidity change to both `on_liquidity_pool` and, for a pool of two tokens,
/// `on_liquidity_change`. on_liquidity_pool is not reported if an amount doesn't fit in i128.
pub(crate) async fn report_liquidity(
    handler: &mut impl TradeEventHandler,
    context: TradeContext,
    pool: PoolId,
    kind: LiquidityChangeKind,
    tokens: Vec<(AccountId, FtBalance)>,
) {
    if let Ok([(token_a, amount_a), (token_b, amount_b)]) = <[_; 2]>::try_from(tokens.clone()) {
        let change = LiquidityChange {
            pool: pool.clone(),
            kind,
            token_a,
            amount_a,
            token_b,
            amount_b,
        };
        handler.on_liquidity_change(context.clone(), change).await;
    }

    let mut signed = HashMap::new();
    for (token, amount) in &tokens {
        let Ok(amount) = i128::try_from(*amount) else {
            log::warn!("Amount overflow in liquidity change of {pool}: {amount}");
            return;
        };
        let amount = match kind {
            LiquidityChangeKind::Add => amount,
            LiquidityChangeKind::Remove => -amount,
        };
        signed.insert(token.clone(), amount);
    }
    handler
        .on_liquidity_pool(context.clone(), pool.clone(), signed)
        .await;
}

#[derive(Debug, PartialEq)]
pub struct PoolChangeEvent {
    pub pool_id: PoolId,
    pub receipt_id: CryptoHash,
    pub block_timestamp_nanosec: u128,
    pub block_height: BlockHeight,
    pub pool: PoolType,
}

#[derive(Debug, PartialEq)]
pub enum PoolType {
    Ref(ref_finance_state::Pool),
    Aidols(AidolsPool),
    IntearPlach(IntearPlachPool),
    RefDcl(RefDclPool),
}
