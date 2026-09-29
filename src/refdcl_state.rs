use borsh::{BorshDeserialize, BorshSerialize};
use inindexer::near_utils::FtBalance;

// Leading fields of a pool record, the rest of it is ignored
#[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq)]
pub struct RefDclPoolPrefix {
    pub version: u8,
    pub pool_id: String,
    pub token_x: String,
    pub token_y: String,
    pub fee: u32,
    pub point_delta: u32,
    pub current_point: i32,
    pub sqrt_price: [u8; 32],
    pub liquidity: FtBalance,
    pub liquidity_x: FtBalance,
}
