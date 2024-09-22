use std::collections::HashMap;

use async_trait::async_trait;
use borsh::BorshDeserialize;
use inindexer::{
    near_indexer_primitives::{
        types::{AccountId, Balance, BlockHeight},
        views::{StateChangeCauseView, StateChangeValueView},
        CryptoHash, StreamerMessage,
    },
    IncompleteTransaction, Indexer, TransactionReceipt,
};
use ref_trade_detection::REF_CONTRACT_ID;
use ref_trade_detection::TESTNET_REF_CONTRACT_ID;

use crate::meme_cooking_deposit_detection::{DepositEvent, WithdrawEvent};

mod meme_cooking_deposit_detection;
pub mod redis_handler;
mod ref_finance_state;
mod ref_trade_detection;
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
    );
    async fn on_pool_change(&mut self, pool: PoolChangeEvent);
    async fn on_memecooking_deposit(&mut self, context: TradeContext, deposit: DepositEvent);
    async fn on_memecooking_withdraw(&mut self, context: TradeContext, withdraw: WithdrawEvent);
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
                        if let Ok(pool) = <ref_finance_state::Pool as BorshDeserialize>::deserialize(
                            &mut value.as_slice(),
                        ) {
                            if pool_id > 25_000 {
                                log::warn!("Pool ID too high, probably a bug: {pool_id}. If Ref actually has that many pools, increase this number to a reasonable amount");
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
        meme_cooking_deposit_detection::detect(
            receipt,
            transaction,
            block,
            &mut self.handler,
            self.is_testnet,
        )
        .await;
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
