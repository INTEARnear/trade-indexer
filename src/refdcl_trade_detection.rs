use std::collections::HashMap;

use inindexer::near_indexer_primitives::views::{ActionView, ReceiptEnumView};
use inindexer::near_utils::{EventLogData, FtBalance};
use inindexer::{
    IncompleteTransaction, TransactionReceipt,
    near_indexer_primitives::{StreamerMessage, types::AccountId},
    near_utils::dec_format,
};
use serde::Deserialize;

use crate::{
    BalanceChangeSwap, PoolId, RawPoolSwap, TradeContext, TradeEventHandler, find_parent_receipt,
};

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

#[derive(Deserialize, Debug)]
struct FtOnTransferArgs {
    msg: String,
}

/// Variants of the ft_on_transfer msg that accept a referral_id
#[derive(Deserialize, Debug)]
enum TokenReceiverMessage {
    Swap {
        #[serde(default)]
        referral_id: Option<AccountId>,
    },
    SwapByOutput {
        #[serde(default)]
        referral_id: Option<AccountId>,
    },
    LimitOrderWithSwap {
        #[serde(default)]
        referral_id: Option<AccountId>,
    },
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
    if receipt.is_successful(false)
        && receipt.receipt.receipt.receiver_id == REFDCL_CONTRACT_ID
        && let ReceiptEnumView::Action { actions, .. } = &receipt.receipt.receipt.receipt
    {
        let referrer = actions.iter().find_map(|action| {
            let ActionView::FunctionCall {
                method_name, args, ..
            } = action
            else {
                return None;
            };
            if method_name != "ft_on_transfer" {
                return None;
            }
            let call = serde_json::from_slice::<FtOnTransferArgs>(args).ok()?;
            match serde_json::from_str::<TokenReceiverMessage>(&call.msg).ok()? {
                TokenReceiverMessage::Swap { referral_id }
                | TokenReceiverMessage::SwapByOutput { referral_id }
                | TokenReceiverMessage::LimitOrderWithSwap { referral_id } => {
                    referral_id.map(|id| id.to_string())
                }
            }
        });
        for log in &receipt.receipt.execution_outcome.outcome.logs {
            if let Ok(event) = EventLogData::<Vec<SwapEvent>>::deserialize(log)
                && (event.event == "swap" || event.event == "swap_desire")
                && event.standard == "dcl.ref"
            {
                for swap in event.data {
                    let mut trader = swap.swapper;
                    if trader == "aggregatedex.near" {
                        let mut last_transfer_call = receipt;
                        let mut last_parent = receipt;
                        while let Some(parent) = find_parent_receipt(transaction, last_parent) {
                            last_parent = parent;
                            if let ReceiptEnumView::Action { actions, .. } =
                                &parent.receipt.receipt.receipt
                                && actions.iter().any(|a| {
                                    matches!(
                                        a,
                                        ActionView::FunctionCall { method_name, .. }
                                            if method_name == "ft_transfer_call"
                                    )
                                })
                            {
                                last_transfer_call = parent;
                            }
                        }
                        trader = last_transfer_call.receipt.receipt.predecessor_id.clone();
                    }
                    let context = TradeContext {
                        trader,
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
                    let Ok(amount_in_i128) = i128::try_from(swap.amount_in) else {
                        log::warn!("Amount in overflow in swap event: {}", swap.amount_in);
                        continue;
                    };
                    let Ok(amount_out_i128) = i128::try_from(swap.amount_out) else {
                        log::warn!("Amount out overflow in swap event: {}", swap.amount_out);
                        continue;
                    };
                    handler
                        .on_balance_change_swap(
                            context,
                            BalanceChangeSwap {
                                balance_changes: HashMap::from_iter([
                                    (swap.token_in.clone(), -amount_in_i128),
                                    (swap.token_out.clone(), amount_out_i128),
                                ]),
                                pool_swaps: vec![RawPoolSwap {
                                    pool: create_refdcl_pool_id(&swap.pool_id),
                                    token_in: swap.token_in.clone(),
                                    token_out: swap.token_out.clone(),
                                    amount_in: swap.amount_in,
                                    amount_out: swap.amount_out,
                                }],
                            },
                            referrer.clone(),
                        )
                        .await;
                }
            }
        }
    }
}

pub fn create_refdcl_pool_id(pool_id: &str) -> PoolId {
    format!("REFDCL-{pool_id}")
}
