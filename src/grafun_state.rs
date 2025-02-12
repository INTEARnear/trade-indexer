use borsh::{BorshDeserialize, BorshSerialize};
use inindexer::near_indexer_primitives::types::Balance;

#[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq)]
pub struct GraFunPoolState {
    pub metadata: String,
    pub token_hold: Balance,
    pub wnear_hold: Balance,
    pub is_deployed: bool,
    pub is_tradable: bool,
}
