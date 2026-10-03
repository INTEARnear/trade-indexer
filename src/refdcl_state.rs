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
    pub max_liquidity_per_point: u128,
    pub fee_scale_x_128: [u8; 32],
    pub fee_scale_y_128: [u8; 32],
    pub total_fee_x_charged: FtBalance,
    pub total_fee_y_charged: FtBalance,
    pub volume_x_in: [u8; 32],
    pub volume_y_in: [u8; 32],
    pub volume_x_out: [u8; 32],
    pub volume_y_out: [u8; 32],
    pub total_liquidity: u128,
    pub total_order_x: FtBalance,
    pub total_order_y: FtBalance,
    pub total_x: FtBalance,
    pub total_y: FtBalance,
}
