use inindexer::near_utils::EventLogData;
use inindexer::{
    near_indexer_primitives::{
        types::{AccountId, Balance},
        StreamerMessage,
    },
    near_utils::dec_format,
    IncompleteTransaction, TransactionReceipt,
};
use serde::Deserialize;

use crate::{TradeContext, TradeEventHandler};

pub const TESTNET_FACTORY_CONTRACT_ID: &str = "factory.v10.meme-cooking.testnet";
pub const FACTORY_CONTRACT_ID: &str = "meme-cooking.near";

#[derive(Clone, Deserialize, Debug, PartialEq)]
pub struct DepositEvent {
    pub meme_id: u64,
    pub account_id: AccountId,
    #[serde(with = "dec_format")]
    pub amount: Balance,
    #[serde(with = "dec_format")]
    pub protocol_fee: Balance,
    pub referrer: Option<AccountId>,
    #[serde(with = "dec_format", default)]
    pub referrer_fee: Option<Balance>,
}

#[derive(Clone, Deserialize, Debug, PartialEq)]
pub struct WithdrawEvent {
    pub meme_id: u64,
    pub account_id: AccountId,
    #[serde(with = "dec_format")]
    pub amount: Balance,
    #[serde(with = "dec_format")]
    pub fee: Balance,
}

pub async fn detect(
    receipt: &TransactionReceipt,
    transaction: &IncompleteTransaction,
    block: &StreamerMessage,
    handler: &mut impl TradeEventHandler,
    is_testnet: bool,
) {
    let factory_contract_id = if is_testnet {
        TESTNET_FACTORY_CONTRACT_ID
    } else {
        FACTORY_CONTRACT_ID
    };
    if receipt.is_successful(false) && receipt.receipt.receipt.receiver_id == factory_contract_id {
        for log in receipt.receipt.execution_outcome.outcome.logs.iter() {
            if let Ok(deposit) = EventLogData::<DepositEvent>::deserialize(log) {
                if deposit.standard != "meme-cooking" || deposit.event != "deposit" {
                    continue;
                }
                handler
                    .on_memecooking_deposit(
                        TradeContext {
                            trader: deposit.data.account_id.clone(),
                            block_height: block.block.header.height,
                            block_timestamp_nanosec: block.block.header.timestamp as u128,
                            receipt_id: receipt.receipt.receipt.receipt_id,
                            transaction_id: transaction.transaction.transaction.hash,
                        },
                        deposit.data,
                    )
                    .await;
            }
            if let Ok(withdraw) = EventLogData::<WithdrawEvent>::deserialize(log) {
                if withdraw.standard != "meme-cooking" || withdraw.event != "withdraw" {
                    continue;
                }
                handler
                    .on_memecooking_withdraw(
                        TradeContext {
                            trader: withdraw.data.account_id.clone(),
                            block_height: block.block.header.height,
                            block_timestamp_nanosec: block.block.header.timestamp as u128,
                            receipt_id: receipt.receipt.receipt.receipt_id,
                            transaction_id: transaction.transaction.transaction.hash,
                        },
                        withdraw.data,
                    )
                    .await;
            }
        }
    }
}
