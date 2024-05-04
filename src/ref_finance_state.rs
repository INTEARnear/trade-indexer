use inindexer::near_indexer_primitives::types::Balance;
use inindexer::near_utils::{dec_format, dec_format_vec};
use near_sdk::borsh::{self, BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

type SdkTimestamp = u64;
type SdkAccountId = String;

#[allow(clippy::enum_variant_names)]
#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum Pool {
    SimplePool(SimplePool),
    StableSwapPool(StableSwapPool),
    RatedSwapPool(RatedSwapPool),
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct SimplePool {
    /// List of tokens in the pool.
    pub token_account_ids: Vec<SdkAccountId>,
    /// How much NEAR this contract has.
    #[serde(with = "dec_format_vec")]
    pub amounts: Vec<Balance>,
    /// Volumes accumulated by this pool.
    pub volumes: Vec<SwapVolume>,
    /// Fee charged for swap (gets divided by FEE_DIVISOR).
    pub total_fee: u32,
    /// Obsolete, reserve to simplify upgrade.
    pub exchange_fee: u32,
    /// Obsolete, reserve to simplify upgrade.
    pub referral_fee: u32,
    /// Shares of the pool by liquidity providers.
    pub shares_prefix: Vec<u8>, // actual type: pub shares: LookupMap<SdkAccountId, Balance>,
    /// Total number of shares.
    #[serde(with = "dec_format")]
    pub shares_total_supply: Balance,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct SwapVolume {
    #[serde(with = "dec_format")]
    pub input: u128,
    #[serde(with = "dec_format")]
    pub output: u128,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct StableSwapPool {
    /// List of tokens in the pool.
    pub token_account_ids: Vec<SdkAccountId>,
    /// Each decimals for tokens in the pool
    pub token_decimals: Vec<u8>,
    /// token amounts in comparable decimal.
    #[serde(with = "dec_format_vec")]
    pub c_amounts: Vec<Balance>,
    /// Volumes accumulated by this pool.
    pub volumes: Vec<SwapVolume>,
    /// Fee charged for swap (gets divided by FEE_DIVISOR).
    pub total_fee: u32,
    /// Shares of the pool by liquidity providers.
    pub shares_prefix: Vec<u8>, // actual type: pub shares: LookupMap<SdkAccountId, Balance>,
    /// Total number of shares.
    #[serde(with = "dec_format")]
    pub shares_total_supply: Balance,
    /// Initial amplification coefficient.
    #[serde(with = "dec_format")]
    pub init_amp_factor: u128,
    /// Target for ramping up amplification coefficient.
    #[serde(with = "dec_format")]
    pub target_amp_factor: u128,
    /// Initial amplification time.
    #[serde(with = "dec_format")]
    pub init_amp_time: SdkTimestamp,
    /// Stop ramp up amplification time.
    #[serde(with = "dec_format")]
    pub stop_amp_time: SdkTimestamp,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct RatedSwapPool {
    /// List of tokens in the pool.
    pub token_account_ids: Vec<SdkAccountId>,
    /// Each decimals for tokens in the pool
    pub token_decimals: Vec<u8>,
    /// token amounts in comparable decimal.
    #[serde(with = "dec_format_vec")]
    pub c_amounts: Vec<Balance>,
    /// Volumes accumulated by this pool.
    pub volumes: Vec<SwapVolume>,
    /// Fee charged for swap (gets divided by FEE_DIVISOR).
    pub total_fee: u32,
    /// Shares of the pool by liquidity providers.
    pub shares_prefix: Vec<u8>, // actual type: pub shares: LookupMap<SdkAccountId, Balance>,
    /// Total number of shares.
    #[serde(with = "dec_format")]
    pub shares_total_supply: Balance,
    /// Initial amplification coefficient.
    #[serde(with = "dec_format")]
    pub init_amp_factor: u128,
    /// Target for ramping up amplification coefficient.
    #[serde(with = "dec_format")]
    pub target_amp_factor: u128,
    /// Initial amplification time.
    #[serde(with = "dec_format")]
    pub init_amp_time: SdkTimestamp,
    /// Stop ramp up amplification time.
    #[serde(with = "dec_format")]
    pub stop_amp_time: SdkTimestamp,
}
