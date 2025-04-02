use std::collections::HashMap;

use inindexer::near_utils::EventLogData;
use inindexer::{
    near_indexer_primitives::{
        types::{AccountId, Balance},
        StreamerMessage,
    },
    IncompleteTransaction, TransactionReceipt,
};
use serde::{Deserialize, Deserializer};

use crate::veax_state::create_veax_pool_id;
use crate::{BalanceChangeSwap, PoolId, RawPoolSwap, TradeContext, TradeEventHandler};

pub const VEAX_CONTRACT_ID: &str = "veax.near";

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct SwapEvent {
    user: AccountId,
    /// (in, out)
    tokens: (AccountId, AccountId),
    /// (in, out)
    #[serde(deserialize_with = "deserialize_tuple_dec_format")]
    amounts: (Balance, Balance),
}

fn deserialize_tuple_dec_format<'de, D>(deserializer: D) -> Result<(Balance, Balance), D::Error>
where
    D: Deserializer<'de>,
{
    let tuple: (String, String) = Deserialize::deserialize(deserializer)?;
    Ok((
        tuple
            .0
            .parse::<Balance>()
            .map_err(serde::de::Error::custom)?,
        tuple
            .1
            .parse::<Balance>()
            .map_err(serde::de::Error::custom)?,
    ))
}

pub async fn detect(
    receipt: &TransactionReceipt,
    transaction: &IncompleteTransaction,
    block: &StreamerMessage,
    handler: &mut impl TradeEventHandler,
    is_testnet: bool,
) {
    if is_testnet {
        return;
    }
    if receipt.is_successful(false) && receipt.receipt.receipt.receiver_id == VEAX_CONTRACT_ID {
        for log in &receipt.receipt.execution_outcome.outcome.logs {
            if let Ok(event) = EventLogData::<SwapEvent>::deserialize(log) {
                if event.event == "swap" && event.standard == "veax" {
                    let context = TradeContext {
                        trader: event.data.user,
                        block_height: block.block.header.height,
                        block_timestamp_nanosec: block.block.header.timestamp_nanosec as u128,
                        transaction_id: transaction.transaction.transaction.hash,
                        receipt_id: receipt.receipt.receipt.receipt_id,
                    };
                    handler
                        .on_raw_pool_swap(
                            context.clone(),
                            RawPoolSwap {
                                pool: create_veax_pool_id(&event.data.tokens),
                                token_in: event.data.tokens.0.clone(),
                                token_out: event.data.tokens.1.clone(),
                                amount_in: event.data.amounts.0,
                                amount_out: event.data.amounts.1,
                            },
                        )
                        .await;
                    handler
                        .on_balance_change_swap(
                            context,
                            BalanceChangeSwap {
                                balance_changes: HashMap::from_iter([
                                    (event.data.tokens.0.clone(), -(event.data.amounts.0 as i128)),
                                    (event.data.tokens.1.clone(), event.data.amounts.1 as i128),
                                ]),
                                pool_swaps: vec![RawPoolSwap {
                                    pool: create_veax_pool_id(&event.data.tokens),
                                    token_in: event.data.tokens.0.clone(),
                                    token_out: event.data.tokens.1.clone(),
                                    amount_in: event.data.amounts.0,
                                    amount_out: event.data.amounts.1,
                                }],
                            },
                        )
                        .await;
                }
            }
        }
    }
}
