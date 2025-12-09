use borsh::{BorshDeserialize, BorshSerialize};
use inindexer::near_utils::FtBalance;

#[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq)]
pub struct AidolsPoolState {
    pub token_hold: FtBalance,
    pub wnear_hold: FtBalance,
    pub is_deployed: bool,
    pub is_tradable: bool,
}
