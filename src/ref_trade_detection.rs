use std::collections::HashMap;

use inindexer::{
    near_indexer_primitives::{
        types::{AccountId, Balance},
        views::{ActionView, ReceiptEnumView},
        StreamerMessage,
    },
    near_utils::dec_format,
    IncompleteTransaction, TransactionReceipt,
};
use serde::Deserialize;

use crate::{
    find_parent_receipt, BalanceChangeSwap, PoolId, RawPoolSwap, TradeContext, TradeEventHandler,
};

pub const TESTNET_REF_CONTRACT_ID: &str = "ref-finance-101.testnet";
pub const REF_CONTRACT_ID: &str = "v2.ref-finance.near";

pub async fn detect(
    receipt: &TransactionReceipt,
    transaction: &IncompleteTransaction,
    block: &StreamerMessage,
    handler: &mut impl TradeEventHandler,
    is_testnet: bool,
) {
    let ref_contract_id = if is_testnet {
        TESTNET_REF_CONTRACT_ID
    } else {
        REF_CONTRACT_ID
    };
    if receipt.is_successful(false) && receipt.receipt.receipt.receiver_id == ref_contract_id {
        let mut raw_pool_swaps = vec![];
        let mut balance_changes = HashMap::new();
        let mut trader = receipt.receipt.receipt.predecessor_id.clone();
        let mut swap_actions = vec![];
        let mut swap_logs_in_receipt = Vec::new();
        if let ReceiptEnumView::Action { actions, .. } = &receipt.receipt.receipt.receipt {
            for action in actions {
                if let ActionView::FunctionCall {
                    method_name, args, ..
                } = action
                {
                    if method_name == "ft_on_transfer" {
                        if let Some(caller_receipt) = transaction
                            .receipts
                            .iter()
                            .filter_map(|(_, r)| r.as_ref())
                            .find(|r| {
                                r.receipt
                                    .execution_outcome
                                    .outcome
                                    .receipt_ids
                                    .contains(&receipt.receipt.receipt.receipt_id)
                            })
                        {
                            trader = caller_receipt.receipt.receipt.predecessor_id.clone();
                        }
                        if let Ok(call) = serde_json::from_slice::<FtTransferCall>(args) {
                            if let Ok(call) =
                                serde_json::from_str::<FtTransferCallExecute>(&call.msg)
                            {
                                swap_actions.extend(call.actions);
                            } else if let Ok(call) =
                                serde_json::from_str::<FtTransferCallHotZap>(&call.msg)
                            {
                                swap_actions.extend(call.hot_zap_actions);
                            }
                        }
                    } else if method_name == "swap" {
                        if let Ok(call) = serde_json::from_slice::<MethodSwap>(args) {
                            swap_actions.extend(call.actions);
                        }
                    } else if method_name == "execute_actions" {
                        if let Ok(call) = serde_json::from_slice::<MethodExecuteActions>(args) {
                            swap_actions.extend(call.actions);
                        }
                    }
                    // There could be some edge cases with both "swap" and "ft_transfer_call" as
                    // separate actions in one transaction (if it's possible to have 2 function
                    // call actions in 1 transaction), but since the ft_transfer_call caller
                    // must be the same as swap caller, it should be handled correctly by the
                    // statement above.
                }
            }
        }

        if trader == "ref.hot.tg" {
            if let Some(receipt) = find_parent_receipt(transaction, receipt) {
                if let Some(receipt) = find_parent_receipt(transaction, receipt) {
                    trader = receipt.receipt.receipt.predecessor_id.clone();
                } else {
                    log::warn!(
                        "Could not find the parent receipt of the parent receipt of the ref.hot.tg trade {:?}",
                        transaction.transaction.transaction.hash
                    );
                    return;
                }
            } else {
                log::warn!(
                    "Could not find the parent receipt of the ref.hot.tg trade {:?}",
                    transaction.transaction.transaction.hash
                );
                return;
            }
        }

        for log in &receipt.receipt.execution_outcome.outcome.logs {
            if let Some(log) = log.strip_prefix("Swapped ") {
                if let Some((token_in, token_out)) = log.split_once(" for ") {
                    let token_out = token_out.split(',').next().unwrap();
                    let (amount_in, token_in) = token_in.split_once(' ').unwrap();
                    let (amount_out, token_out) = token_out.split_once(' ').unwrap();
                    if let (Ok(token_in), Ok(token_out), Ok(amount_in), Ok(amount_out)) = (
                        token_in.parse::<AccountId>(),
                        token_out.parse::<AccountId>(),
                        amount_in.parse::<Balance>(),
                        amount_out.parse::<Balance>(),
                    ) {
                        log::info!(
                            "{} exchanged {} {} for {} {}",
                            trader,
                            amount_in,
                            token_in,
                            amount_out,
                            token_out
                        );
                        *balance_changes.entry(token_in.clone()).or_insert(0) -= amount_in as i128;
                        *balance_changes.entry(token_out.clone()).or_insert(0) +=
                            amount_out as i128;
                        swap_logs_in_receipt.push(RawPoolSwap {
                            pool: "NONE".to_string(),
                            token_in,
                            token_out,
                            amount_in,
                            amount_out,
                        });
                    }
                }
            }
        }

        if swap_actions.len() != swap_logs_in_receipt.len() {
            log::warn!(
                    "Invalid number of actions found in receipt {:?} for transaction {:?}: {swap_actions:?}",
                    receipt.receipt.receipt.receipt,
                    transaction.transaction.transaction.hash
                );
            return;
        }

        raw_pool_swaps.extend(
            swap_logs_in_receipt
                .into_iter()
                .enumerate()
                .map(|(i, swap)| RawPoolSwap {
                    pool: create_ref_pool_id(swap_actions[i].pool_id),
                    token_in: swap.token_in,
                    token_out: swap.token_out,
                    amount_in: swap.amount_in,
                    amount_out: swap.amount_out,
                }),
        );

        if raw_pool_swaps.is_empty() {
            return;
        }

        let context = TradeContext {
            trader,
            block_height: block.block.header.height,
            block_timestamp_nanosec: block.block.header.timestamp_nanosec as u128,
            transaction_id: transaction.transaction.transaction.hash,
            receipt_id: receipt.receipt.receipt.receipt_id,
        };
        for raw_pool_swap in raw_pool_swaps.clone() {
            handler
                .on_raw_pool_swap(context.clone(), raw_pool_swap)
                .await;
        }
        balance_changes.retain(|_, v| *v != 0);
        if !balance_changes.is_empty() {
            let balance_changes = BalanceChangeSwap {
                balance_changes,
                pool_swaps: raw_pool_swaps,
            };
            handler
                .on_balance_change_swap(context, balance_changes)
                .await;
        }
    }
}

pub fn create_ref_pool_id(pool_id: u64) -> PoolId {
    format!("REF-{}", pool_id)
}

#[derive(Deserialize, Debug)]
struct MethodSwap {
    actions: Vec<Action>,
}

#[derive(Deserialize, Debug)]
struct MethodExecuteActions {
    actions: Vec<Action>,
}

#[derive(Deserialize, Debug)]
struct FtTransferCall {
    /// Json string that represents either FtTransferCallExecute or FtTransferCallHotZap
    msg: String,
}

#[derive(Deserialize, Debug)]
struct FtTransferCallExecute {
    actions: Vec<Action>,
}

#[derive(Deserialize, Debug)]
struct FtTransferCallHotZap {
    hot_zap_actions: Vec<Action>,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct Action {
    pool_id: u64,
    token_in: AccountId,
    #[serde(with = "dec_format", default)]
    amount_in: Option<Balance>,
    token_out: AccountId,
    #[serde(with = "dec_format")]
    min_amount_out: Balance,
}
