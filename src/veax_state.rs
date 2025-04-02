use inindexer::near_utils::EventLogData;
use inindexer::{
    near_indexer_primitives::{types::AccountId, StreamerMessage},
    IncompleteTransaction, TransactionReceipt,
};

use crate::{PoolChangeEvent, PoolId, PoolType, TradeEventHandler};
use intear_events::events::trade::trade_pool_change::VeaxPool;

pub const VEAX_CONTRACT_ID: &str = "veax.near";

pub async fn detect_changes(
    receipt: &TransactionReceipt,
    _transaction: &IncompleteTransaction,
    block: &StreamerMessage,
    handler: &mut impl TradeEventHandler,
    is_testnet: bool,
) {
    if is_testnet {
        return;
    }
    if receipt.is_successful(false) && receipt.receipt.receipt.receiver_id == VEAX_CONTRACT_ID {
        for log in &receipt.receipt.execution_outcome.outcome.logs {
            if let Ok(event) = EventLogData::<VeaxPool>::deserialize(log) {
                if event.event == "update_pool_state" && event.standard == "veax" {
                    handler
                        .on_pool_change(PoolChangeEvent {
                            pool_id: create_veax_pool_id(&event.data.pool),
                            receipt_id: receipt.receipt.receipt.receipt_id,
                            block_timestamp_nanosec: block.block.header.timestamp_nanosec as u128,
                            block_height: block.block.header.height,
                            pool: PoolType::Veax(event.data),
                        })
                        .await;
                }
            }
        }
    }
}

pub fn create_veax_pool_id(tokens: &(AccountId, AccountId)) -> PoolId {
    format!("VEAX-{}-{}", tokens.0, tokens.1)
}
