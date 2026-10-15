use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use inindexer::near_indexer_primitives::types::AccountId;
use inindexer::near_utils::{EventLogData, is_receipt_successful};
use inindexer::{
    StreamerMessage,
    transaction_trees::{ExecutedReceipt, TransactionTree},
};
use intear_dex_types::{AssetId, DexEventData, DexId, IntearDexEvent};
use intear_events::events::trade::trade_pool_change::{
    IntearAssetId, IntearAssetWithBalance, IntearPlachFeeConfiguration, IntearPlachFeeReceiver,
    IntearPlachPool,
};
use serde::Deserialize;
use xyk_dex_types::{
    AssetWithBalance, CurrentFees, FeeReceiver, PoolView, XykDexEvent, XykDexEventV1,
};

use crate::{
    BalanceChangeSwap, LiquidityChangeKind, PoolChangeEvent, PoolId, PoolType, RawPoolSwap,
    TradeContext, TradeEventHandler, report_liquidity,
};

pub const INTEAR_CONTRACT_ID: &str = "dex.intear.near";
pub const INTEAR_LAUNCH_CONTRACT_IDS: &[&str] = &["launch.intear.near", "launch-v2.intear.near"];
pub const PLACH_DEX_ID: &str = "slimedragon.near/xyk";

/// The dex events of a `dex_events_borsh` log: base64 of an lz4 block prefixed with its
/// uncompressed length, of the borsh of the events
fn decode_borsh_dex_events(encoded: &str) -> Result<Vec<DexEventData>, String> {
    let compressed = BASE64_STANDARD
        .decode(encoded)
        .map_err(|err| err.to_string())?;
    let decompressed =
        lz4_flex::decompress_size_prepended(&compressed).map_err(|err| err.to_string())?;
    near_sdk::borsh::from_slice(&decompressed).map_err(|err| err.to_string())
}

pub async fn detect(
    receipt: &ExecutedReceipt,
    transaction: &TransactionTree,
    block: &StreamerMessage,
    handler: &mut impl TradeEventHandler,
    is_testnet: bool,
) {
    if is_testnet {
        // Not deployed on testnet
        return;
    }
    if is_receipt_successful(&receipt.receipt) == Some(true)
        && receipt.receipt.receipt.receiver_id == INTEAR_CONTRACT_ID
    {
        let mut merged_pool_swaps = Vec::new();
        let mut merged_context = None;
        let mut merged_referrer = None;

        enum LoggedDexEvent {
            Borsh(Vec<u8>),
            // parsed later, in order to panic if not parseable
            Json(serde_json::Value),
        }
        struct LoggedDexEventData {
            dex_id: DexId,
            event: LoggedDexEvent,
            referrer: Option<AccountId>,
            user: Option<AccountId>,
        }

        #[derive(Deserialize)]
        struct OldJsonDexEventData {
            dex_id: DexId,
            event: serde_json::Value,
            referrer: Option<AccountId>,
            user: Option<AccountId>,
        }

        let logged_dex_events = receipt
            .receipt
            .execution_outcome
            .outcome
            .logs
            .iter()
            .flat_map(|log| {
                if let Ok(event) = EventLogData::<OldJsonDexEventData>::deserialize(log)
                    && event.standard == "inteardex"
                    && event.event == "dex_event"
                {
                    return vec![LoggedDexEventData {
                        dex_id: event.data.dex_id,
                        event: LoggedDexEvent::Json(event.data.event),
                        referrer: event.data.referrer,
                        user: event.data.user,
                    }];
                }
                if let Some(json) = log.strip_prefix("EVENT_JSON:")
                    && let Ok(IntearDexEvent::DexEventsBorsh(encoded)) = serde_json::from_str(json)
                {
                    return match decode_borsh_dex_events(&encoded) {
                        Ok(dex_events) => dex_events
                            .into_iter()
                            .map(|dex_event| LoggedDexEventData {
                                dex_id: dex_event.dex_id,
                                event: LoggedDexEvent::Borsh(dex_event.event),
                                referrer: dex_event.referrer,
                                user: dex_event.user,
                            })
                            .collect(),
                        Err(err) => {
                            log::error!(
                                "Couldn't decode dex_events_borsh in receipt {}: {err}",
                                receipt.receipt.receipt.receipt_id
                            );
                            Vec::new()
                        }
                    };
                }
                Vec::new()
            });
        for LoggedDexEventData {
            dex_id,
            event,
            referrer,
            user,
        } in logged_dex_events
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

            let event = match event {
                LoggedDexEvent::Borsh(bytes) => near_sdk::borsh::from_slice::<XykDexEvent>(&bytes)
                    .map_err(|err| err.to_string()),
                LoggedDexEvent::Json(value) => serde_json::from_value::<XykDexEventV1>(value)
                    .map(XykDexEvent::from)
                    .map_err(|err| err.to_string()),
            };
            let event = match event {
                Ok(event) => event,
                Err(err) => {
                    if block.block.header.height > 187015239 {
                        panic!(
                            "Couldn't parse plach event in receipt {}: {err}",
                            receipt.receipt.receipt.receipt_id
                        );
                    } else {
                        // old contract, mainly testing
                        continue;
                    }
                }
            };

            match event {
                XykDexEvent::Swap {
                    pool_id,
                    asset_in,
                    asset_out,
                    amount_in,
                    amount_out,
                    ..
                } => {
                    let Some(mut user) = user else { continue };
                    if INTEAR_LAUNCH_CONTRACT_IDS.contains(&user.as_str()) {
                        match transaction
                            .ancestors(&receipt.receipt.receipt.receipt_id)
                            .find(|ancestor| ancestor.receipt.receipt.receiver_id == user)
                        {
                            Some(launch_call) => {
                                user = launch_call.receipt.receipt.predecessor_id.clone()
                            }
                            None => log::warn!(
                                "Could not find who called {user} for the plach swap {:?}",
                                transaction.transaction.transaction.hash
                            ),
                        }
                    }
                    let Some(asset_in) = nep141_or_near(asset_in) else {
                        continue;
                    };
                    let Some(asset_out) = nep141_or_near(asset_out) else {
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
