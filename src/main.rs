mod redis_handler;
mod ref_finance_state;
mod ref_trade_detection;
#[cfg(test)]
mod tests;

use redis_handler::PushToRedisStream;
use ref_trade_detection::{create_ref_pool_id, REF_CONTRACT_ID};

use std::collections::HashMap;

use async_trait::async_trait;
use inindexer::fastnear_data_server::FastNearDataServerProvider;
use inindexer::near_indexer_primitives::near_primitives::serialize::dec_format;
use inindexer::near_indexer_primitives::types::{AccountId, Balance, BlockHeight};
use inindexer::near_indexer_primitives::views::StateChangeValueView;
use inindexer::near_indexer_primitives::{CryptoHash, StreamerMessage};
use inindexer::{
    run_indexer, AutoContinue, BlockIterator, IncompleteTransaction, Indexer, IndexerOptions,
    PreprocessTransactionsSettings, TransactionReceipt,
};
use near_sdk::borsh::BorshDeserialize;
use redis::aio::ConnectionManager;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

type PoolId = String;

struct TradeIndexer<T: TradeEventHandler>(T);

#[async_trait]
pub trait TradeEventHandler: Send + Sync + 'static {
    async fn on_raw_pool_swap(&mut self, context: &TradeContext, swap: &RawPoolSwap);
    async fn on_balance_change_swap(
        &mut self,
        context: &TradeContext,
        balance_changes: &BalanceChangeSwap,
    );
    async fn on_pool_change(&mut self, pool: &Pool);
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
                                id: create_ref_pool_id(pool_id),
                                pool: PoolType::Ref(pool),
                            };
                            self.0.on_pool_change(&pool).await;
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
    #[serde(with = "dec_format")]
    block_height: BlockHeight,
    txid: CryptoHash,
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

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .with_module_level("inindexer::performance", log::LevelFilter::Debug)
        .init()
        .unwrap();

    let client = redis::Client::open(
        std::env::var("REDIS_URL").expect("No $REDIS_URL environment variable set"),
    )
    .unwrap();
    let connection = ConnectionManager::new(client).await.unwrap();

    let mut indexer = TradeIndexer(PushToRedisStream::new(connection, 100_000));

    #[cfg(feature = "parallel")]
    let streamer = inindexer::message_provider::ParallelProviderStreamer::new(
        FastNearDataServerProvider::mainnet(),
        10,
    );
    #[cfg(not(feature = "parallel"))]
    let streamer = FastNearDataServerProvider::mainnet();

    run_indexer(
        &mut indexer,
        streamer,
        IndexerOptions {
            range: if std::env::args().len() > 1 {
                // For debugging
                let msg = "Usage: `trade-indexer` or `trade-indexer [start-block] [end-block]`";
                BlockIterator::iterator(
                    std::env::args()
                        .nth(1)
                        .expect(msg)
                        .replace(['_', ',', ' ', '.'], "")
                        .parse()
                        .expect(msg)
                        ..=std::env::args()
                            .nth(2)
                            .expect(msg)
                            .replace(['_', ',', ' ', '.'], "")
                            .parse()
                            .expect(msg),
                )
            } else {
                BlockIterator::AutoContinue(AutoContinue::default())
            },
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: if cfg!(debug_assertions) { 0 } else { 100 },
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .expect("Indexer run failed");
}
