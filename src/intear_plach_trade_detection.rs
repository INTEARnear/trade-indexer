use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use inindexer::near_indexer_primitives::types::AccountId;
use inindexer::near_utils::EventLogData;
use inindexer::{
    IncompleteTransaction, TransactionReceipt, near_indexer_primitives::StreamerMessage,
};
use intear_dex_types::{AssetId, DexEventData, IntearDexEvent};
use intear_events::events::trade::trade_pool_change::{
    IntearAssetId, IntearAssetWithBalance, IntearPlachFeeConfiguration, IntearPlachFeeReceiver,
    IntearPlachPool,
};
use xyk_dex_types::{AssetWithBalance, CurrentFees, FeeReceiver, PoolView, XykDexEvent};

use crate::{
    BalanceChangeSwap, LiquidityChangeKind, PoolChangeEvent, PoolId, PoolType, RawPoolSwap,
    TradeContext, TradeEventHandler, find_parent_receipt, report_liquidity,
};

pub const INTEAR_CONTRACT_ID: &str = "dex.intear.near";
pub const INTEAR_LAUNCH_CONTRACT_IDS: &[&str] = &["launch.intear.near", "launch-v2.intear.near"];
pub const PLACH_DEX_ID: &str = "slimedragon.near/xyk";

pub async fn detect(
    receipt: &TransactionReceipt,
    transaction: &IncompleteTransaction,
    block: &StreamerMessage,
    handler: &mut impl TradeEventHandler,
    is_testnet: bool,
) {
    if is_testnet {
        // Not deployed on testnet
        return;
    }
    if receipt.is_successful(false) && receipt.receipt.receipt.receiver_id == INTEAR_CONTRACT_ID {
        let mut merged_pool_swaps = Vec::new();
        let mut merged_context = None;
        let mut merged_referrer = None;

        let dex_events = receipt
            .receipt
            .execution_outcome
            .outcome
            .logs
            .iter()
            .flat_map(|log| {
                if let Ok(event) = EventLogData::<DexEventData<serde_json::Value>>::deserialize(log)
                    && event.standard == "inteardex"
                    && event.event == "dex_event"
                {
                    return vec![event.data];
                }
                if let Some(json) = log.strip_prefix("EVENT_JSON:")
                    && let Ok(IntearDexEvent::DexEvents(encoded)) = serde_json::from_str(json)
                {
                    let decoded = BASE64_STANDARD
                        .decode(&encoded)
                        .map_err(|err| err.to_string())
                        .and_then(|compressed| {
                            lz4_flex::decompress_size_prepended(&compressed)
                                .map_err(|err| err.to_string())
                        })
                        .and_then(|json| {
                            serde_json::from_slice(&json).map_err(|err| err.to_string())
                        });
                    return decoded.unwrap_or_else(|err| {
                        log::error!(
                            "Couldn't decode dex_events in receipt {}: {err}",
                            receipt.receipt.receipt.receipt_id
                        );
                        Vec::new()
                    });
                }
                Vec::new()
            });
        for DexEventData {
            dex_id,
            event,
            referrer,
            user,
        } in dex_events
        {
            if dex_id != PLACH_DEX_ID.parse().unwrap() {
                continue;
            }
            let context = |trader: AccountId| TradeContext {
                trader,
                block_height: block.block.header.height,
                block_timestamp_nanosec: block.block.header.timestamp_nanosec as u128,
                transaction_id: transaction.transaction.transaction.hash,
                receipt_id: receipt.receipt.receipt.receipt_id,
            };

            let event = serde_json::from_value::<XykDexEvent>(event).unwrap_or_else(|err| {
                panic!(
                    "Couldn't parse plach event in receipt {}: {err}",
                    receipt.receipt.receipt.receipt_id
                );
            });

            match event {
                XykDexEvent::Swap {
                    pool_id,
                    request,
                    amount_in,
                    amount_out,
                    ..
                } => {
                    let Some(mut user) = user else { continue };
                    if INTEAR_LAUNCH_CONTRACT_IDS.contains(&user.as_str()) {
                        let mut ancestor = receipt;
                        loop {
                            let Some(parent) = find_parent_receipt(transaction, ancestor) else {
                                log::warn!(
                                    "Could not find who called {user} for the plach swap {:?}",
                                    transaction.transaction.transaction.hash
                                );
                                break;
                            };
                            ancestor = parent;
                            if ancestor.receipt.receipt.receiver_id == user {
                                user = ancestor.receipt.receipt.predecessor_id.clone();
                                break;
                            }
                        }
                    }
                    let Some(asset_in) = nep141_or_near(request.asset_in) else {
                        continue;
                    };
                    let Some(asset_out) = nep141_or_near(request.asset_out) else {
                        continue;
                    };
                    let context = context(user);
                    if merged_context.is_none() {
                        merged_context = Some(context.clone());
                    }
                    merged_referrer = merged_referrer.or(referrer.clone());
                    let raw_pool_swap = RawPoolSwap {
                        pool: create_plach_pool_id(pool_id),
                        token_in: asset_in,
                        token_out: asset_out,
                        amount_in: amount_in.0,
                        amount_out: amount_out.0,
                        referrer,
                    };
                    handler
                        .on_raw_pool_swap(context, raw_pool_swap.clone())
                        .await;
                    merged_pool_swaps.push(raw_pool_swap);
                }
                XykDexEvent::LiquidityAdded {
                    pool_id,
                    asset_0,
                    asset_1,
                    added_amount_0,
                    added_amount_1,
                    ..
                } => {
                    let Some(user) = user else { continue };
                    let Some(asset_0) = nep141_or_near(asset_0) else {
                        continue;
                    };
                    let Some(asset_1) = nep141_or_near(asset_1) else {
                        continue;
                    };
                    report_liquidity(
                        handler,
                        context(user),
                        create_plach_pool_id(pool_id),
                        LiquidityChangeKind::Add,
                        vec![(asset_0, added_amount_0.0), (asset_1, added_amount_1.0)],
                    )
                    .await;
                }
                XykDexEvent::LiquidityRemoved {
                    pool_id,
                    asset_0,
                    asset_1,
                    removed_amount_0,
                    removed_amount_1,
                    ..
                } => {
                    let Some(user) = user else { continue };
                    let Some(asset_0) = nep141_or_near(asset_0) else {
                        continue;
                    };
                    let Some(asset_1) = nep141_or_near(asset_1) else {
                        continue;
                    };
                    report_liquidity(
                        handler,
                        context(user),
                        create_plach_pool_id(pool_id),
                        LiquidityChangeKind::Remove,
                        vec![(asset_0, removed_amount_0.0), (asset_1, removed_amount_1.0)],
                    )
                    .await;
                }
                XykDexEvent::PoolUpdated { pool_id, pool } => {
                    let pool = match pool {
                        PoolView::Private {
                            assets,
                            fees,
                            owner_id,
                            ..
                        } => IntearPlachPool::Private {
                            assets: (intear_asset(assets.0), intear_asset(assets.1)),
                            fees: intear_fees(fees),
                            owner_id,
                        },
                        PoolView::Public {
                            assets,
                            fees,
                            total_shares,
                            ..
                        } => IntearPlachPool::Public {
                            assets: (intear_asset(assets.0), intear_asset(assets.1)),
                            fees: intear_fees(fees),
                            total_shares: total_shares.map(|shares| shares.0),
                        },
                        PoolView::Launch {
                            near_amount,
                            launched_asset,
                            fees,
                            phantom_liquidity_near,
                            ..
                        } => IntearPlachPool::Launch {
                            near_amount: near_amount.0,
                            launched_asset: intear_asset(launched_asset),
                            fees: intear_fees(fees),
                            phantom_liquidity_near: phantom_liquidity_near.0,
                        },
                        PoolView::LaunchV2 {
                            quote_asset,
                            launched_asset,
                            fees,
                            phantom_liquidity,
                            ..
                        } => IntearPlachPool::LaunchV2 {
                            quote_asset: intear_asset(quote_asset),
                            launched_asset: intear_asset(launched_asset),
                            fees: intear_fees(fees),
                            phantom_liquidity: phantom_liquidity.0,
                        },
                    };
                    handler
                        .on_pool_change(PoolChangeEvent {
                            pool_id: create_plach_pool_id(pool_id),
                            receipt_id: receipt.receipt.receipt.receipt_id,
                            block_timestamp_nanosec: block.block.header.timestamp_nanosec as u128,
                            block_height: block.block.header.height,
                            pool: PoolType::IntearPlach(pool),
                        })
                        .await;
                }
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

/// NEP-141 tokens and NEAR are what's indexed here; NEP-245 and NEP-171 assets aren't.
fn nep141_or_near(asset: AssetId) -> Option<AccountId> {
    match asset {
        AssetId::Nep141(id) => Some(id),
        AssetId::Near => Some("near".parse().unwrap()),
        AssetId::Nep245(_, _) | AssetId::Nep171(_, _) => None,
    }
}

fn intear_asset(asset: AssetWithBalance) -> IntearAssetWithBalance {
    IntearAssetWithBalance {
        asset_id: match asset.asset_id {
            AssetId::Near => IntearAssetId::Near,
            AssetId::Nep141(contract_id) => IntearAssetId::Nep141(contract_id),
            AssetId::Nep245(contract_id, token_id) => IntearAssetId::Nep245(contract_id, token_id),
            AssetId::Nep171(contract_id, token_id) => IntearAssetId::Nep171(contract_id, token_id),
        },
        balance: asset.balance.0,
    }
}

fn intear_fees(fees: CurrentFees) -> IntearPlachFeeConfiguration {
    IntearPlachFeeConfiguration {
        receivers: fees
            .receivers
            .into_iter()
            .map(|(receiver, fee)| {
                let receiver = match receiver {
                    FeeReceiver::Account(account_id) => IntearPlachFeeReceiver::Account(account_id),
                    FeeReceiver::Pool => IntearPlachFeeReceiver::Pool,
                    FeeReceiver::Community(account_id) => {
                        IntearPlachFeeReceiver::Community(account_id)
                    }
                };
                (receiver, fee)
            })
            .collect(),
    }
}

pub fn create_plach_pool_id(pool_id: u32) -> PoolId {
    format!("INTEARPLACH-{pool_id}")
}
