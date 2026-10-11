use inindexer::near_utils::dec_format_vec;
use inindexer::{
    IncompleteTransaction, TransactionReceipt,
    near_indexer_primitives::{
        StreamerMessage,
        types::AccountId,
        views::{ActionView, ReceiptEnumView},
    },
    near_utils::{FtBalance, dec_format},
};
use serde::Deserialize;

use crate::{
    BalanceChangeSwap, LiquidityChangeKind, PoolId, RawPoolSwap, TradeContext, TradeEventHandler,
    find_parent_receipt, report_liquidity,
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
        let mut trader = receipt.receipt.receipt.predecessor_id.clone();
        let mut swap_action_pools = vec![];
        let mut referrer = None;
        let mut liquidity_changes = Vec::new();
        if let ReceiptEnumView::Action { actions, .. } = &receipt.receipt.receipt.receipt {
            for action in actions {
                if let ActionView::FunctionCall {
                    method_name, args, ..
                } = action
                {
                    if method_name == "ft_on_transfer" {
                        if let Some(caller_receipt) = transaction
                            .receipts
                            .values()
                            .filter_map(|r| r.as_ref())
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
                        if let Ok(call) = serde_json::from_slice::<FtTransferCallArgs>(args) {
                            if let Ok(call) =
                                serde_json::from_str::<FtTransferCallArgsExecute>(&call.msg)
                            {
                                if let Some(referral_id) = call.referral_id {
                                    referrer = Some(referral_id);
                                }
                                swap_action_pools
                                    .extend(call.actions.into_iter().map(|a| a.pool_id))
                            } else if let Ok(call) =
                                serde_json::from_str::<FtTransferCallArgsHotZap>(&call.msg)
                            {
                                swap_action_pools
                                    .extend(call.hot_zap_actions.into_iter().map(|a| a.pool_id));
                            }
                        }
                    } else if method_name == "swap" {
                        if let Ok(call) = serde_json::from_slice::<MethodSwap>(args) {
                            if let Some(referral_id) = call.referral_id {
                                referrer = Some(referral_id);
                            }
                            swap_action_pools.extend(call.actions.into_iter().map(|a| a.pool_id));
                        }
                    } else if method_name == "swap_by_output" {
                        if let Ok(call) = serde_json::from_slice::<MethodSwap>(args) {
                            if let Some(referral_id) = call.referral_id {
                                referrer = Some(referral_id);
                            }
                            swap_action_pools.extend(call.actions.into_iter().map(|a| a.pool_id));
                        }
                    } else if method_name == "execute_actions" {
                        if let Ok(call) = serde_json::from_slice::<MethodExecuteActions>(args) {
                            if let Some(referral_id) = call.referral_id {
                                referrer = Some(referral_id);
                            }
                            swap_action_pools.extend(call.actions.into_iter().map(|a| a.pool_id));
                        }
                    } else if method_name == "add_liquidity"
                        && let Ok(call) =
                            serde_json::from_slice::<FtTransferCallArgsAddLiquidity>(args)
                    {
                        liquidity_changes.extend(
                            receipt
                                .receipt
                                .execution_outcome
                                .outcome
                                .logs
                                .iter()
                                .filter_map(|log| {
                                    // format: "Liquidity added ["999999999999999915648607 wrap.near", "15869989324782287999975226 intel.tkn.near"], minted 514844781930897970949 shares"
                                    let log = log
                                        .strip_prefix("Liquidity added [\"")?
                                        .strip_suffix(" shares")?;
                                    let (amounts, shares) = log.split_once("\"], minted ")?;
                                    shares.parse::<FtBalance>().ok()?;
                                    parse_amounts(amounts)
                                })
                                .map(|tokens| {
                                    (
                                        create_ref_pool_id(call.pool_id),
                                        LiquidityChangeKind::Add,
                                        tokens,
                                    )
                                }),
                        );
                    } else if method_name == "remove_liquidity"
                        && let Ok(call) = serde_json::from_slice::<RemoveLiquidity>(args)
                    {
                        liquidity_changes.extend(
                            receipt
                                .receipt
                                .execution_outcome
                                .outcome
                                .logs
                                .iter()
                                .filter_map(|log| {
                                    // format: "514844781930897970949 shares of liquidity removed: receive back ["1000312838374558764552331 wrap.near", "15865198314126424586378752 intel.tkn.near"]"
                                    let (shares, amounts) = log.split_once(
                                        " shares of liquidity removed: receive back [\"",
                                    )?;
                                    shares.parse::<FtBalance>().ok()?;
                                    parse_amounts(amounts.strip_suffix("\"]")?)
                                })
                                .map(|tokens| {
                                    (
                                        create_ref_pool_id(call.pool_id),
                                        LiquidityChangeKind::Remove,
                                        tokens,
                                    )
                                }),
                        );
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

        if trader == "aggregatedex.near" {
            let mut last_transfer_call = receipt;
            let mut last_parent = receipt;
            while let Some(parent) = find_parent_receipt(transaction, last_parent) {
                last_parent = parent;
                if let ReceiptEnumView::Action { actions, .. } = &parent.receipt.receipt.receipt
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

        for (pool, kind, tokens) in liquidity_changes {
            report_liquidity(handler, context.clone(), pool, kind, tokens).await;
        }

        let mut swap_logs_in_receipt = Vec::new();
        for log in &receipt.receipt.execution_outcome.outcome.logs {
            if let (Some(log), _) | (_, Some(log)) = (
                log.strip_prefix("Swapped "),
                log.strip_prefix("Swap_by_output "),
            ) && let Some((token_in, token_out)) = log.split_once(" for ")
            {
                let token_out = token_out.split(',').next().unwrap();
                let (amount_in, token_in) = token_in.split_once(' ').unwrap();
                let (amount_out, token_out) = token_out.split_once(' ').unwrap();
                if let (Ok(token_in), Ok(token_out), Ok(amount_in), Ok(amount_out)) = (
                    token_in.parse::<AccountId>(),
                    token_out.parse::<AccountId>(),
                    amount_in.parse::<FtBalance>(),
                    amount_out.parse::<FtBalance>(),
                ) {
                    log::info!(
                        "{} exchanged {} {} for {} {}",
                        context.trader,
                        amount_in,
                        token_in,
                        amount_out,
                        token_out
                    );
                    swap_logs_in_receipt.push((token_in, token_out, amount_in, amount_out));
                }
            }
        }

        if swap_action_pools.len() != swap_logs_in_receipt.len() {
            log::warn!(
                "Invalid number of actions found in receipt {:?} for transaction {:?}: {swap_action_pools:?}",
                receipt.receipt.receipt.receipt,
                transaction.transaction.transaction.hash
            );
            return;
        }

        let raw_pool_swaps = swap_action_pools
            .into_iter()
            .zip(swap_logs_in_receipt)
            .map(
                |(pool_id, (token_in, token_out, amount_in, amount_out))| RawPoolSwap {
                    pool: create_ref_pool_id(pool_id),
                    token_in,
                    token_out,
                    amount_in,
                    amount_out,
                    referrer: referrer.clone(),
                },
            )
            .collect::<Vec<_>>();
        for raw_pool_swap in raw_pool_swaps.clone() {
            handler
                .on_raw_pool_swap(context.clone(), raw_pool_swap)
                .await;
        }
        if let Some(balance_changes) = BalanceChangeSwap::new(raw_pool_swaps) {
            handler
                .on_balance_change_swap(context, balance_changes, referrer.map(|id| id.to_string()))
                .await;
        }
    }
}

/// `amount token", "amount token", ...`, without the outer quotes
fn parse_amounts(amounts: &str) -> Option<Vec<(AccountId, FtBalance)>> {
    amounts
        .split("\", \"")
        .map(|amount| {
            let (amount, token) = amount.split_once(' ')?;
            Some((token.parse().ok()?, amount.parse().ok()?))
        })
        .collect()
}

pub fn create_ref_pool_id(pool_id: u64) -> PoolId {
    format!("REF-{}", pool_id)
}

#[derive(Deserialize, Debug)]
struct MethodSwap {
    actions: Vec<Action>,
    #[serde(default)]
    referral_id: Option<AccountId>,
}

#[derive(Deserialize, Debug)]
struct MethodExecuteActions {
    actions: Vec<Action>,
    #[serde(default)]
    referral_id: Option<AccountId>,
}

#[derive(Deserialize, Debug)]
struct FtTransferCallArgs {
    /// Json string that represents either FtTransferCallExecute or FtTransferCallHotZap
    msg: String,
}

#[derive(Deserialize, Debug)]
struct FtTransferCallArgsExecute {
    actions: Vec<Action>,
    #[serde(default)]
    referral_id: Option<AccountId>,
}

#[derive(Deserialize, Debug)]
struct FtTransferCallArgsHotZap {
    hot_zap_actions: Vec<Action>,
}

#[derive(Deserialize, Debug)]
struct FtTransferCallArgsAddLiquidity {
    pool_id: u64,
    #[serde(with = "dec_format_vec")]
    #[allow(dead_code)]
    amounts: Vec<FtBalance>,
}

#[derive(Deserialize, Debug)]
struct RemoveLiquidity {
    pool_id: u64,
    #[serde(with = "dec_format")]
    #[allow(dead_code)]
    shares: FtBalance,
    #[serde(with = "dec_format_vec")]
    #[allow(dead_code)]
    min_amounts: Vec<FtBalance>,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct Action {
    pool_id: u64,
    token_in: AccountId,
    token_out: AccountId,
    #[serde(with = "dec_format", default)]
    amount_in: Option<FtBalance>,
    #[serde(with = "dec_format", default)]
    min_amount_out: Option<FtBalance>,
    #[serde(with = "dec_format", default)]
    amount_out: Option<FtBalance>,
    #[serde(with = "dec_format", default)]
    max_amount_in: Option<FtBalance>,
}
