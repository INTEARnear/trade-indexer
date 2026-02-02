use std::collections::HashMap;

use inindexer::near_utils::EventLogData;
use inindexer::{
    IncompleteTransaction, TransactionReceipt,
    near_indexer_primitives::{StreamerMessage, types::AccountId},
    near_utils::{FtBalance, dec_format},
};
use serde::Deserialize;

use crate::{BalanceChangeSwap, PoolId, RawPoolSwap, TradeContext, TradeEventHandler};

pub const AIDOLS_CONTRACT_ID: &str = "aidols.near";

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct SwapEvent {
    #[serde(with = "dec_format")]
    input_amount: FtBalance,
    input_token: AccountId,
    #[serde(with = "dec_format")]
    output_amount: FtBalance,
    output_token: AccountId,
    referral_id: Option<AccountId>,
    #[serde(with = "dec_format")]
    token_hold: FtBalance,
    user_id: AccountId,
    #[serde(with = "dec_format")]
    wnear_commission: FtBalance,
    #[serde(with = "dec_format")]
    wnear_hold: FtBalance,
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
    if receipt.is_successful(false) && receipt.receipt.receipt.receiver_id == AIDOLS_CONTRACT_ID {
        for log in &receipt.receipt.execution_outcome.outcome.logs {
            if let Ok(event) = EventLogData::<Vec<SwapEvent>>::deserialize(log)
                && event.event == "token_swap"
            {
                for swap in event.data {
                    let context = TradeContext {
                        trader: swap.user_id.clone(),
                        block_height: block.block.header.height,
                        block_timestamp_nanosec: block.block.header.timestamp_nanosec as u128,
                        transaction_id: transaction.transaction.transaction.hash,
                        receipt_id: receipt.receipt.receipt.receipt_id,
                    };
                    let token = if swap.input_token == "wrap.near" {
                        swap.output_token.clone()
                    } else {
                        swap.input_token.clone()
                    };
                    handler
                        .on_raw_pool_swap(
                            context.clone(),
                            RawPoolSwap {
                                pool: create_aidols_pool_id(&token),
                                token_in: swap.input_token.clone(),
                                token_out: swap.output_token.clone(),
                                amount_in: swap.input_amount,
                                amount_out: swap.output_amount,
                            },
                        )
                        .await;
                    handler
                        .on_balance_change_swap(
                            context,
                            BalanceChangeSwap {
                                balance_changes: HashMap::from_iter([
                                    (swap.input_token.clone(), -(swap.input_amount as i128)),
                                    (swap.output_token.clone(), swap.output_amount as i128),
                                ]),
                                pool_swaps: vec![RawPoolSwap {
                                    pool: create_aidols_pool_id(&token),
                                    token_in: swap.input_token.clone(),
                                    token_out: swap.output_token.clone(),
                                    amount_in: swap.input_amount,
                                    amount_out: swap.output_amount,
                                }],
                            },
                            swap.referral_id.map(|id| id.to_string()),
                        )
                        .await;
                }
            }
        }
    }
}

pub fn create_aidols_pool_id(token_id: &AccountId) -> PoolId {
    format!("AIDOLS-{token_id}")
}
