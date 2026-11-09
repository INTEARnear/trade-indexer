use inindexer::near_utils::{EventLogData, is_receipt_successful};
use inindexer::{
    near_indexer_primitives::{CryptoHash, types::AccountId},
    near_utils::{FtBalance, dec_format},
    transaction_trees::ExecutedReceipt,
};
use serde::Deserialize;

use crate::{BalanceChangeSwap, PoolId, RawPoolSwap, ReceiptEvent, TradeContext};

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

pub(crate) fn detect(
    receipt: &ExecutedReceipt,
    transaction_id: CryptoHash,
    is_testnet: bool,
) -> Vec<ReceiptEvent> {
    let mut events = Vec::new();
    if is_testnet {
        return events;
    }
    if is_receipt_successful(&receipt.receipt) == Some(true)
        && receipt.receipt.receipt.receiver_id == AIDOLS_CONTRACT_ID
    {
        for log in &receipt.receipt.execution_outcome.outcome.logs {
            if let Ok(event) = EventLogData::<Vec<SwapEvent>>::deserialize(log)
                && event.event == "token_swap"
            {
                for swap in event.data {
                    let context = TradeContext {
                        trader: swap.user_id.clone(),
                        block_height: receipt.block_height,
                        block_timestamp_nanosec: receipt.block_timestamp_nanosec,
                        transaction_id,
                        receipt_id: receipt.receipt.receipt.receipt_id,
                    };
                    let token = if swap.input_token == "wrap.near" {
                        swap.output_token.clone()
                    } else {
                        swap.input_token.clone()
                    };
                    let referrer = swap.referral_id.as_ref().map(|id| id.to_string());
                    let raw_pool_swap = RawPoolSwap {
                        pool: create_aidols_pool_id(&token),
                        token_in: swap.input_token,
                        token_out: swap.output_token,
                        amount_in: swap.input_amount,
                        amount_out: swap.output_amount,
                        referrer: swap.referral_id,
                    };
                    events.push(ReceiptEvent::PoolSwap {
                        context: context.clone(),
                        swap: raw_pool_swap.clone(),
                    });
                    if let Some(balance_changes) = BalanceChangeSwap::new(vec![raw_pool_swap]) {
                        events.push(ReceiptEvent::BalanceChangeSwap {
                            context,
                            balance_changes,
                            referrer,
                        });
                    }
                }
            }
        }
    }
    events
}

pub fn create_aidols_pool_id(token_id: &AccountId) -> PoolId {
    format!("AIDOLS-{token_id}")
}
