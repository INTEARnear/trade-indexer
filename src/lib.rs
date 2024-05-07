use std::collections::HashMap;

use async_trait::async_trait;
use inindexer::{
    near_indexer_primitives::{
        types::{AccountId, Balance, BlockHeight},
        views::StateChangeValueView,
        CryptoHash, StreamerMessage,
    },
    near_utils::dec_format,
    IncompleteTransaction, Indexer, TransactionReceipt,
};
use near_sdk::borsh::BorshDeserialize;
use ref_trade_detection::REF_CONTRACT_ID;
use serde::{Deserialize, Serialize};

#[cfg(feature = "redis-handler")]
pub mod redis_handler;
mod ref_finance_state;
mod ref_trade_detection;
#[cfg(test)]
mod tests;

type PoolId = String;

pub struct TradeIndexer<T: TradeEventHandler>(pub T);

#[async_trait]
pub trait TradeEventHandler: Send + Sync + 'static {
    async fn on_raw_pool_swap(&mut self, context: &TradeContext, swap: &RawPoolSwap);
    async fn on_balance_change_swap(
        &mut self,
        context: &TradeContext,
        balance_changes: &BalanceChangeSwap,
    );
    async fn on_pool_change(&mut self, pool: &Pool, block_height: BlockHeight);
}

#[async_trait]
impl<T: TradeEventHandler> Indexer for TradeIndexer<T> {
    type Error = String;

    async fn process_block(&mut self, block: &StreamerMessage) -> Result<(), Self::Error> {
        for shard in block.shards.iter() {
            for state_change in shard.state_changes.iter() {
                if let StateChangeValueView::DataUpdate {
                    account_id,
                    key,
                    value,
                } = &state_change.value
                {
                    if account_id == REF_CONTRACT_ID {
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

                            let pool = Pool {
                                id: ref_trade_detection::create_ref_pool_id(pool_id),
                                pool: PoolType::Ref(pool),
                            };
                            self.0
                                .on_pool_change(&pool, block.block.header.height)
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
        ref_trade_detection::detect(receipt, transaction, block, &mut self.0).await;
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct TradeContext {
    trader: AccountId,
    block_height: BlockHeight,
    #[serde(with = "dec_format")]
    pub block_timestamp_nanosec: u128,
    transaction_id: CryptoHash,
    receipt_id: CryptoHash,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct RawPoolSwap {
    pool: PoolId,
    token_in: AccountId,
    token_out: AccountId,
    #[serde(with = "dec_format")]
    amount_in: Balance,
    #[serde(with = "dec_format")]
    amount_out: Balance,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct BalanceChangeSwap {
    #[serde(with = "balance_changes_serializer")]
    balance_changes: HashMap<AccountId, i128>,
    pool_swaps: Vec<RawPoolSwap>,
}

mod balance_changes_serializer {
    use serde::{Deserializer, Serializer};

    use super::*;

    pub fn serialize<S>(
        balance_changes: &HashMap<AccountId, i128>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let balance_changes_stringified: HashMap<&AccountId, String> = balance_changes
            .iter()
            .map(|(k, v)| (k, v.to_string()))
            .collect();
        balance_changes_stringified.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<HashMap<AccountId, i128>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let balance_changes_stringified: HashMap<AccountId, String> =
            Deserialize::deserialize(deserializer)?;
        let mut balance_changes: HashMap<AccountId, i128> = HashMap::new();
        for (k, v) in balance_changes_stringified {
            balance_changes.insert(k, v.parse().map_err(serde::de::Error::custom)?);
        }
        Ok(balance_changes)
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct Pool {
    id: PoolId,
    pool: PoolType,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum PoolType {
    Ref(ref_finance_state::Pool),
}
