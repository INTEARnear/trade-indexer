use inindexer::near_indexer_primitives::views::{ActionView, ReceiptEnumView};
use inindexer::near_utils::{EventLogData, FtBalance, is_receipt_successful};
use inindexer::{
    StreamerMessage,
    near_indexer_primitives::types::AccountId,
    near_utils::dec_format,
    transaction_trees::{ExecutedReceipt, TransactionTree},
};
use serde::Deserialize;

use crate::{
    BalanceChangeSwap, LiquidityChangeKind, PoolId, RawPoolSwap, TradeContext, TradeEventHandler,
    aggregatedex::{AGGREGATEDEX_CONTRACT_ID, aggregatedex_trade},
    report_liquidity,
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

/// `liquidity_added` and `liquidity_append`, a new position and more liquidity in an existing one
#[derive(Deserialize, Debug)]
struct LiquidityAddedEvent {
    owner_id: AccountId,
    pool_id: String,
    #[serde(with = "dec_format")]
    paid_token_x: FtBalance,
    #[serde(with = "dec_format")]
    paid_token_y: FtBalance,
}

#[derive(Deserialize, Debug)]
struct LiquidityRemovedEvent {
    owner_id: AccountId,
    pool_id: String,
    /// 0 when the position's fees are only claimed
    #[serde(with = "dec_format")]
    removed_amount: u128,
    /// What was sent back, the removed liquidity together with the claimed fees
    #[serde(with = "dec_format")]
    refund_token_x: FtBalance,
    #[serde(with = "dec_format")]
    refund_token_y: FtBalance,
    #[serde(with = "dec_format")]
    claim_fee_token_x: FtBalance,
    #[serde(with = "dec_format")]
    claim_fee_token_y: FtBalance,
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
    receipt: &ExecutedReceipt,
    transaction: &TransactionTree,
    block: &StreamerMessage,
    handler: &mut impl TradeEventHandler,
    is_testnet: bool,
) {
    if is_testnet {
        // CA is unknown on testnet
        return;
    }
    if is_receipt_successful(&receipt.receipt) == Some(true)
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
                | TokenReceiverMessage::LimitOrderWithSwap { referral_id } => referral_id,
            }
        });
        let mut merged_pool_swaps = Vec::new();
        let mut merged_context = None;
        let mut merged_referrer = None;
        for log in &receipt.receipt.execution_outcome.outcome.logs {
            if let Ok(event) = EventLogData::<Vec<SwapEvent>>::deserialize(log)
                && (event.event == "swap" || event.event == "swap_desire")
                && event.standard == "dcl.ref"
            {
                for swap in event.data {
                    // A LimitOrderWithSwap that found nothing to swap before its point, so the
                    // whole deposit became an order
                    if swap.amount_in == 0 && swap.amount_out == 0 {
                        continue;
                    }
                    let (trader, referrer) = if swap.swapper == AGGREGATEDEX_CONTRACT_ID {
                        let (ad_trader, ad_referrer) = aggregatedex_trade(receipt, transaction);
                        (ad_trader, ad_referrer.or(referrer.clone()))
                    } else {
                        (swap.swapper, referrer.clone())
                    };
                    let context = TradeContext {
                        trader,
                        block_height: block.block.header.height,
                        block_timestamp_nanosec: block.block.header.timestamp_nanosec as u128,
                        transaction_id: transaction.transaction.transaction.hash,
                        receipt_id: receipt.receipt.receipt.receipt_id,
                    };
                    let raw_pool_swap = RawPoolSwap {
                        pool: create_refdcl_pool_id(&swap.pool_id),
                        token_in: swap.token_in,
                        token_out: swap.token_out,
                        amount_in: swap.amount_in,
                        amount_out: swap.amount_out,
                        referrer: referrer.clone(),
                    };
                    handler
                        .on_raw_pool_swap(context.clone(), raw_pool_swap.clone())
                        .await;
                    if merged_context.is_none() {
                        merged_context = Some(context);
                    }
                    merged_referrer = merged_referrer.or(referrer);
                    merged_pool_swaps.push(raw_pool_swap);
                }
            }

            let changes = if let Ok(event) =
                EventLogData::<Vec<LiquidityAddedEvent>>::deserialize(log)
                && (event.event == "liquidity_added" || event.event == "liquidity_append")
                && event.standard == "dcl.ref"
            {
                event
                    .data
                    .into_iter()
                    .map(|added| {
                        (
                            added.owner_id,
                            added.pool_id,
                            LiquidityChangeKind::Add,
                            added.paid_token_x,
                            added.paid_token_y,
                        )
                    })
                    .collect()
            } else if let Ok(event) = EventLogData::<Vec<LiquidityRemovedEvent>>::deserialize(log)
                && event.event == "liquidity_removed"
                && event.standard == "dcl.ref"
            {
                event
                    .data
                    .into_iter()
                    .filter(|removed| removed.removed_amount != 0)
                    .filter_map(|removed| {
                        let (Some(amount_x), Some(amount_y)) = (
                            removed
                                .refund_token_x
                                .checked_sub(removed.claim_fee_token_x),
                            removed
                                .refund_token_y
                                .checked_sub(removed.claim_fee_token_y),
                        ) else {
                            log::warn!("DCL refund smaller than its fees: {removed:?}");
                            return None;
                        };
                        Some((
                            removed.owner_id,
                            removed.pool_id,
                            LiquidityChangeKind::Remove,
                            amount_x,
                            amount_y,
                        ))
                    })
                    .collect()
            } else {
                Vec::new()
            };
            for (owner_id, pool_id, kind, amount_x, amount_y) in changes {
                // Pool ids are `token_x|token_y|fee`
                let mut tokens = pool_id.split('|').map(str::parse::<AccountId>);
                let (Some(Ok(token_x)), Some(Ok(token_y))) = (tokens.next(), tokens.next()) else {
                    log::warn!("Invalid DCL pool id: {pool_id}");
                    continue;
                };
                let context = TradeContext {
                    trader: owner_id,
                    block_height: block.block.header.height,
                    block_timestamp_nanosec: block.block.header.timestamp_nanosec as u128,
                    transaction_id: transaction.transaction.transaction.hash,
                    receipt_id: receipt.receipt.receipt.receipt_id,
                };
                report_liquidity(
                    handler,
                    context,
                    create_refdcl_pool_id(&pool_id),
                    kind,
                    vec![(token_x, amount_x), (token_y, amount_y)],
                )
                .await;
            }
        }
        if let Some(context) = merged_context
            && let Some(balance_changes) = BalanceChangeSwap::new(merged_pool_swaps)
        {
            handler
                .on_balance_change_swap(
                    context,
                    balance_changes,
                    merged_referrer.map(|id| id.to_string()),
                )
                .await;
        }
    }
}

pub fn create_refdcl_pool_id(pool_id: &str) -> PoolId {
    format!("REFDCL-{pool_id}")
}
