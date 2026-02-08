use std::collections::HashMap;

use inindexer::near_utils::EventLogData;
use inindexer::{
    IncompleteTransaction, TransactionReceipt, near_indexer_primitives::StreamerMessage,
};
use intear_events::events::trade::trade_pool_change::IntearPlachPool;
use serde::Deserialize;

use crate::intear_dex_types::{AssetId, DexEvent, SwapRequest, U128};
use crate::{
    BalanceChangeSwap, PoolChangeEvent, PoolId, PoolType, RawPoolSwap, TradeContext,
    TradeEventHandler,
};

pub const INTEAR_CONTRACT_ID: &str = "dex.intear.near";
pub const PLACH_DEX_ID: &str = "slimedragon.near/xyk";

#[derive(Deserialize, Debug)]
struct PlachSwapEvent {
    pool_id: u32,
    request: SwapRequest,
    amount_in: U128,
    amount_out: U128,
}

#[derive(Deserialize, Debug)]
struct PlachPoolUpdatedEvent {
    pool_id: u32,
    pool: IntearPlachPool,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct PlachLiquidityAddedEvent {
    pool_id: u32,
    asset_0: AssetId,
    asset_1: AssetId,
    added_amount_0: U128,
    added_amount_1: U128,
    minted_shares: U128,
    new_owned_asset_0: U128,
    new_owned_asset_1: U128,
    new_owned_shares: U128,
    new_total_asset_0: U128,
    new_total_asset_1: U128,
    new_total_shares: U128,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct PlachLiquidityRemovedEvent {
    pool_id: u32,
    asset_0: AssetId,
    asset_1: AssetId,
    removed_amount_0: U128,
    removed_amount_1: U128,
    burned_shares: U128,
    new_owned_asset_0: U128,
    new_owned_asset_1: U128,
    new_owned_shares: U128,
    new_total_asset_0: U128,
    new_total_asset_1: U128,
    new_total_shares: U128,
}

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
        for log in &receipt.receipt.execution_outcome.outcome.logs {
            if let Ok(event) = EventLogData::<DexEvent<PlachSwapEvent>>::deserialize(log)
                && event.event == "dex_event"
                && event.standard == "inteardex"
                && event.data.event.event == "swap"
                && event.data.dex_id == PLACH_DEX_ID.parse().unwrap()
                && let Some(user) = event.data.user
            {
                let asset_in = match event.data.event.data.request.asset_in {
                    AssetId::Nep141(id) => id,
                    AssetId::Nep245(_, _) => continue,
                    AssetId::Nep171(_, _) => continue,
                    AssetId::Near => "near".parse().unwrap(),
                };
                let asset_out = match event.data.event.data.request.asset_out {
                    AssetId::Nep141(id) => id,
                    AssetId::Nep245(_, _) => continue,
                    AssetId::Nep171(_, _) => continue,
                    AssetId::Near => "near".parse().unwrap(),
                };
                let context = TradeContext {
                    trader: user.clone(),
                    block_height: block.block.header.height,
                    block_timestamp_nanosec: block.block.header.timestamp_nanosec as u128,
                    transaction_id: transaction.transaction.transaction.hash,
                    receipt_id: receipt.receipt.receipt.receipt_id,
                };
                handler
                    .on_raw_pool_swap(
                        context.clone(),
                        RawPoolSwap {
                            pool: create_plach_pool_id(event.data.event.data.pool_id),
                            token_in: asset_in.clone(),
                            token_out: asset_out.clone(),
                            amount_in: event.data.event.data.amount_in.0,
                            amount_out: event.data.event.data.amount_out.0,
                        },
                    )
                    .await;
                let Ok(amount_in_i128) = i128::try_from(event.data.event.data.amount_in.0) else {
                    log::warn!(
                        "Amount in overflow in swap event: {}",
                        event.data.event.data.amount_in.0
                    );
                    continue;
                };
                let Ok(amount_out_i128) = i128::try_from(event.data.event.data.amount_out.0) else {
                    log::warn!(
                        "Amount out overflow in swap event: {}",
                        event.data.event.data.amount_out.0
                    );
                    continue;
                };
                handler
                    .on_balance_change_swap(
                        context,
                        BalanceChangeSwap {
                            balance_changes: HashMap::from_iter([
                                (asset_in.clone(), -amount_in_i128),
                                (asset_out.clone(), amount_out_i128),
                            ]),
                            pool_swaps: vec![RawPoolSwap {
                                pool: create_plach_pool_id(event.data.event.data.pool_id),
                                token_in: asset_in.clone(),
                                token_out: asset_out.clone(),
                                amount_in: event.data.event.data.amount_in.0,
                                amount_out: event.data.event.data.amount_out.0,
                            }],
                        },
                        event.data.referrer.map(|id| id.to_string()),
                    )
                    .await;
            }

            if let Ok(event) = EventLogData::<DexEvent<PlachPoolUpdatedEvent>>::deserialize(log)
                && event.event == "dex_event"
                && event.standard == "inteardex"
                && event.data.event.event == "pool_updated"
                && event.data.dex_id == PLACH_DEX_ID.parse().unwrap()
            {
                handler
                    .on_pool_change(PoolChangeEvent {
                        pool_id: create_plach_pool_id(event.data.event.data.pool_id),
                        receipt_id: receipt.receipt.receipt.receipt_id,
                        block_timestamp_nanosec: block.block.header.timestamp_nanosec as u128,
                        block_height: block.block.header.height,
                        pool: PoolType::IntearPlach(event.data.event.data.pool),
                    })
                    .await;
            }

            if let Ok(event) = EventLogData::<DexEvent<PlachLiquidityAddedEvent>>::deserialize(log)
                && event.event == "dex_event"
                && event.standard == "inteardex"
                && event.data.event.event == "liquidity_added"
                && event.data.dex_id == PLACH_DEX_ID.parse().unwrap()
                && let Some(user) = event.data.user
            {
                let asset_0 = match event.data.event.data.asset_0 {
                    AssetId::Nep141(id) => id,
                    AssetId::Nep245(_, _) => continue,
                    AssetId::Nep171(_, _) => continue,
                    AssetId::Near => "near".parse().unwrap(),
                };
                let asset_1 = match event.data.event.data.asset_1 {
                    AssetId::Nep141(id) => id,
                    AssetId::Nep245(_, _) => continue,
                    AssetId::Nep171(_, _) => continue,
                    AssetId::Near => "near".parse().unwrap(),
                };

                let context = TradeContext {
                    trader: user.clone(),
                    block_height: block.block.header.height,
                    block_timestamp_nanosec: block.block.header.timestamp_nanosec as u128,
                    transaction_id: transaction.transaction.transaction.hash,
                    receipt_id: receipt.receipt.receipt.receipt_id,
                };

                let Ok(added_amount_0) = i128::try_from(event.data.event.data.added_amount_0.0)
                else {
                    log::warn!(
                        "Amount overflow in liquidity_added event: {}",
                        event.data.event.data.added_amount_0.0
                    );
                    continue;
                };
                let Ok(added_amount_1) = i128::try_from(event.data.event.data.added_amount_1.0)
                else {
                    log::warn!(
                        "Amount overflow in liquidity_added event: {}",
                        event.data.event.data.added_amount_1.0
                    );
                    continue;
                };

                handler
                    .on_liquidity_pool(
                        context,
                        create_plach_pool_id(event.data.event.data.pool_id),
                        HashMap::from_iter([(asset_0, added_amount_0), (asset_1, added_amount_1)]),
                    )
                    .await;
            }

            if let Ok(event) =
                EventLogData::<DexEvent<PlachLiquidityRemovedEvent>>::deserialize(log)
                && event.event == "dex_event"
                && event.standard == "inteardex"
                && event.data.event.event == "liquidity_removed"
                && event.data.dex_id == PLACH_DEX_ID.parse().unwrap()
                && let Some(user) = event.data.user
            {
                let asset_0 = match event.data.event.data.asset_0 {
                    AssetId::Nep141(id) => id,
                    AssetId::Nep245(_, _) => continue,
                    AssetId::Nep171(_, _) => continue,
                    AssetId::Near => "near".parse().unwrap(),
                };
                let asset_1 = match event.data.event.data.asset_1 {
                    AssetId::Nep141(id) => id,
                    AssetId::Nep245(_, _) => continue,
                    AssetId::Nep171(_, _) => continue,
                    AssetId::Near => "near".parse().unwrap(),
                };

                let context = TradeContext {
                    trader: user.clone(),
                    block_height: block.block.header.height,
                    block_timestamp_nanosec: block.block.header.timestamp_nanosec as u128,
                    transaction_id: transaction.transaction.transaction.hash,
                    receipt_id: receipt.receipt.receipt.receipt_id,
                };

                let Ok(removed_amount_0) = i128::try_from(event.data.event.data.removed_amount_0.0)
                else {
                    log::warn!(
                        "Amount overflow in liquidity_removed event: {}",
                        event.data.event.data.removed_amount_0.0
                    );
                    continue;
                };
                let Ok(removed_amount_1) = i128::try_from(event.data.event.data.removed_amount_1.0)
                else {
                    log::warn!(
                        "Amount overflow in liquidity_removed event: {}",
                        event.data.event.data.removed_amount_1.0
                    );
                    continue;
                };

                handler
                    .on_liquidity_pool(
                        context,
                        create_plach_pool_id(event.data.event.data.pool_id),
                        HashMap::from_iter([
                            (asset_0, -removed_amount_0),
                            (asset_1, -removed_amount_1),
                        ]),
                    )
                    .await;
            }
        }
    }
}

pub fn create_plach_pool_id(pool_id: u32) -> PoolId {
    format!("INTEARPLACH-{pool_id}")
}
