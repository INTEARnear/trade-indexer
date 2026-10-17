use base64::{Engine, prelude::BASE64_STANDARD};
use inindexer::near_indexer_primitives::{
    types::AccountId,
    views::{ActionView, ReceiptEnumView},
};
use inindexer::transaction_trees::{ExecutedReceipt, TransactionTree};
use serde::Deserialize;

pub const AGGREGATEDEX_CONTRACT_ID: &str = "aggregatedex.near";

#[derive(Deserialize)]
struct FtTransferCall {
    receiver_id: AccountId,
    msg: String,
}

#[derive(Deserialize)]
struct Message {
    msg: String,
}

#[derive(Deserialize)]
struct SwapRequest {
    referral: Option<AccountId>,
    app_fee_recipient: Option<AccountId>,
}

/// Who made a trade that aggregatedex.near swapped for, and their referral. Their call is the
/// topmost `ft_transfer_call` above the receipt, which sends the tokens to aggregatedex.near.
pub(crate) fn aggregatedex_trade(
    receipt: &ExecutedReceipt,
    transaction: &TransactionTree,
) -> (AccountId, Option<AccountId>) {
    let Some((user_call, args)) = transaction
        .ancestors(&receipt.receipt.receipt.receipt_id)
        .filter_map(|ancestor| {
            let ReceiptEnumView::Action { actions, .. } = &ancestor.receipt.receipt.receipt else {
                return None;
            };
            actions.iter().find_map(|action| match action {
                ActionView::FunctionCall {
                    method_name, args, ..
                } if method_name == "ft_transfer_call" => Some((ancestor, args)),
                _ => None,
            })
        })
        .last()
    else {
        return (receipt.receipt.receipt.predecessor_id.clone(), None);
    };
    let trader = user_call.receipt.receipt.predecessor_id.clone();
    let Ok(call) = serde_json::from_slice::<FtTransferCall>(args) else {
        return (trader, None);
    };
    if call.receiver_id != AGGREGATEDEX_CONTRACT_ID {
        return (trader, None);
    }
    // The swap request is base64 of its JSON with every byte shifted by 7
    let request = serde_json::from_str::<Message>(&call.msg)
        .ok()
        .and_then(|message| BASE64_STANDARD.decode(message.msg).ok())
        .map(|shifted| {
            shifted
                .into_iter()
                .map(|byte| byte.wrapping_sub(7))
                .collect::<Vec<_>>()
        })
        .and_then(|request| serde_json::from_slice::<SwapRequest>(&request).ok());
    let Some(request) = request else {
        log::warn!(
            "Couldn't decode the aggregatedex.near swap request {}",
            call.msg
        );
        return (trader, None);
    };
    (trader, request.app_fee_recipient.or(request.referral))
}
