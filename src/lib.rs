use std::collections::HashMap;

use aidols_trade_detection::AIDOLS_CONTRACT_ID;
use async_trait::async_trait;
use borsh::BorshDeserialize;
use grafun_trade_detection::GRAFUN_CONTRACT_ID;
use inindexer::{
    near_indexer_primitives::{
        types::{AccountId, Balance, BlockHeight},
        views::{StateChangeCauseView, StateChangeValueView},
        CryptoHash, StreamerMessage,
    },
    IncompleteTransaction, Indexer, TransactionReceipt,
};
use intear_events::events::trade::trade_pool_change::GraFunPool;
use intear_events::events::trade::trade_pool_change::{AidolsPool, VeaxPool};
use ref_trade_detection::REF_CONTRACT_ID;
use ref_trade_detection::TESTNET_REF_CONTRACT_ID;

mod aidols_state;
mod aidols_trade_detection;
mod grafun_state;
mod grafun_trade_detection;
pub mod redis_handler;
mod ref_finance_state;
mod ref_trade_detection;
mod refdcl_trade_detection;
#[cfg(test)]
mod tests;
mod veax_state;
mod veax_trade_detection;

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
    );
    async fn on_pool_change(&mut self, pool: PoolChangeEvent);
    async fn on_liquidity_pool(
        &mut self,
        context: TradeContext,
        pool_id: PoolId,
        tokens: HashMap<AccountId, i128>,
    );
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
        let aidols_contract_id = AIDOLS_CONTRACT_ID;
        let grafun_contract_id = GRAFUN_CONTRACT_ID;
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
                        if let Ok(pool) = <ref_finance_state::Pool as BorshDeserialize>::deserialize(
                            &mut value.as_slice(),
                        ) {
                            if pool_id > 420_000 {
                                log::warn!("Pool ID too high, probably a bug: {pool_id}. If Ref actually has that many pools, increase the number in {}:{} to a reasonable amount", file!(), line!() - 1);
                                continue;
                            }

                            let pool = PoolChangeEvent {
                                pool_id: ref_trade_detection::create_ref_pool_id(pool_id),
                                receipt_id: *receipt_id,
                                block_timestamp_nanosec: block.block.header.timestamp_nanosec
                                    as u128,
                                block_height: block.block.header.height,
                                pool: PoolType::Ref(pool),
                            };
                            self.handler.on_pool_change(pool).await;
                        }
                    } else if account_id == aidols_contract_id {
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
                        if let Ok(pool) =
                            <aidols_state::AidolsPoolState as BorshDeserialize>::deserialize(
                                &mut value.as_slice(),
                            )
                        {
                            self.handler
                                .on_pool_change(PoolChangeEvent {
                                    pool_id: aidols_trade_detection::create_aidols_pool_id(
                                        &token_id,
                                    ),
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
                        }
                    } else if account_id == grafun_contract_id {
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
                        let mut without_prefix = if let Some(data) = key.strip_prefix(b"s") {
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
                        if let Ok(pool) =
                            <grafun_state::GraFunPoolState as BorshDeserialize>::deserialize(
                                &mut value.as_slice(),
                            )
                        {
                            self.handler
                                .on_pool_change(PoolChangeEvent {
                                    pool_id: grafun_trade_detection::create_grafun_pool_id(
                                        &token_id,
                                    ),
                                    receipt_id: *receipt_id,
                                    block_timestamp_nanosec: block.block.header.timestamp_nanosec
                                        as u128,
                                    block_height: block.block.header.height,
                                    pool: PoolType::GraFun(GraFunPool {
                                        token_id: token_id.clone(),
                                        token_hold: pool.token_hold,
                                        wnear_hold: pool.wnear_hold,
                                        is_deployed: pool.is_deployed,
                                        is_tradable: pool.is_tradable,
                                    }),
                                })
                                .await;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn on_receipt(
        &mut self,
        receipt: &TransactionReceipt,
        transaction: &IncompleteTransaction,
        block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
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
        grafun_trade_detection::detect(
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
        veax_trade_detection::detect(
            receipt,
            transaction,
            block,
            &mut self.handler,
            self.is_testnet,
        )
        .await;
        veax_state::detect_changes(
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
    trader: AccountId,
    block_height: BlockHeight,
    pub block_timestamp_nanosec: u128,
    transaction_id: CryptoHash,
    receipt_id: CryptoHash,
}

#[derive(Debug, PartialEq, Clone)]
pub struct RawPoolSwap {
    pool: PoolId,
    token_in: AccountId,
    token_out: AccountId,
    amount_in: Balance,
    amount_out: Balance,
}

#[derive(Debug, PartialEq)]
pub struct BalanceChangeSwap {
    balance_changes: HashMap<AccountId, i128>,
    pool_swaps: Vec<RawPoolSwap>,
}

#[derive(Debug, PartialEq)]
pub struct PoolChangeEvent {
    pool_id: PoolId,
    receipt_id: CryptoHash,
    block_timestamp_nanosec: u128,
    block_height: BlockHeight,
    pool: PoolType,
}

#[derive(Debug, PartialEq)]
pub enum PoolType {
    Ref(ref_finance_state::Pool),
    Aidols(AidolsPool),
    GraFun(GraFunPool),
    Veax(VeaxPool),
}

pub(crate) fn find_parent_receipt<'a>(
    transaction: &'a IncompleteTransaction,
    receipt: &TransactionReceipt,
) -> Option<&'a TransactionReceipt> {
    transaction.receipts.iter().find_map(|r| {
        if let Some(r) = r.1 {
            if r.receipt
                .execution_outcome
                .outcome
                .receipt_ids
                .contains(&receipt.receipt.receipt.receipt_id)
            {
                return Some(r);
            }
        }
        None
    })
}
