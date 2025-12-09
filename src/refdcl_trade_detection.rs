use std::collections::HashMap;

use inindexer::near_utils::{EventLogData, FtBalance};
use inindexer::{
    near_indexer_primitives::{types::AccountId, StreamerMessage},
    near_utils::dec_format,
    IncompleteTransaction, TransactionReceipt,
};
use serde::Deserialize;

use crate::{BalanceChangeSwap, PoolId, RawPoolSwap, TradeContext, TradeEventHandler};

pub const REFDCL_CONTRACT_ID: &str = "dclv2.ref-labs.near";

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct SwapEvent {
    #[serde(with = "dec_format")]
    amount_in: FtBalance,
    #[serde(with = "dec_format")]
    amount_out: FtBalance,
    pool_id: String,
    #[serde(with = "dec_format")]
    protocol_fee: FtBalance,
    swapper: AccountId,
    token_in: AccountId,
    token_out: AccountId,
    #[serde(with = "dec_format")]
    total_fee: FtBalance,
}

pub async fn detect(
    receipt: &TransactionReceipt,
    transaction: &IncompleteTransaction,
    block: &StreamerMessage,
    handler: &mut impl TradeEventHandler,
    is_testnet: bool,
) {
    if is_testnet {
        // CA is unknown on testnet
        return;
    }
    if receipt.is_successful(false) && receipt.receipt.receipt.receiver_id == REFDCL_CONTRACT_ID {
        for log in &receipt.receipt.execution_outcome.outcome.logs {
            if let Ok(event) = EventLogData::<Vec<SwapEvent>>::deserialize(log) {
                if event.event == "swap" && event.standard == "dcl.ref" {
                    for swap in event.data {
                        let context = TradeContext {
                            trader: swap.swapper,
                            block_height: block.block.header.height,
                            block_timestamp_nanosec: block.block.header.timestamp_nanosec as u128,
                            transaction_id: transaction.transaction.transaction.hash,
                            receipt_id: receipt.receipt.receipt.receipt_id,
                        };
                        handler
                            .on_raw_pool_swap(
                                context.clone(),
                                RawPoolSwap {
                                    pool: create_refdcl_pool_id(&swap.pool_id),
                                    token_in: swap.token_in.clone(),
                                    token_out: swap.token_out.clone(),
                                    amount_in: swap.amount_in,
                                    amount_out: swap.amount_out,
                                },
                            )
                            .await;
                        handler
                            .on_balance_change_swap(
                                context,
                                BalanceChangeSwap {
                                    balance_changes: HashMap::from_iter([
                                        (swap.token_in.clone(), -(swap.amount_in as i128)),
                                        (swap.token_out.clone(), swap.amount_out as i128),
                                    ]),
                                    pool_swaps: vec![RawPoolSwap {
                                        pool: create_refdcl_pool_id(&swap.pool_id),
                                        token_in: swap.token_in.clone(),
                                        token_out: swap.token_out.clone(),
                                        amount_in: swap.amount_in,
                                        amount_out: swap.amount_out,
                                    }],
                                },
                            )
                            .await;
                    }
                }
            }
        }
    }
}

pub fn create_refdcl_pool_id(pool_id: &str) -> PoolId {
    format!("REFDCL-{pool_id}")
}
