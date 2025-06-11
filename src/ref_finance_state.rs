use borsh::{BorshDeserialize, BorshSerialize};
use inindexer::near_indexer_primitives::types::Balance;

type SdkTimestamp = u64;
type SdkAccountId = String;

#[allow(clippy::enum_variant_names)]
#[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq)]
pub enum Pool {
    SimplePool(SimplePool),
    StableSwapPool(StableSwapPool),
    RatedSwapPool(RatedSwapPool),
    DegenSwapPool(DegenSwapPool),
}

#[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq)]
pub struct SimplePool {
    /// List of tokens in the pool.
    pub token_account_ids: Vec<SdkAccountId>,
    /// How much NEAR this contract has.
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
    pub shares_total_supply: Balance,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq)]
pub struct SwapVolume {
    pub input: u128,
    pub output: u128,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq)]
pub struct StableSwapPool {
    /// List of tokens in the pool.
    pub token_account_ids: Vec<SdkAccountId>,
    /// Each decimals for tokens in the pool
    pub token_decimals: Vec<u8>,
    /// token amounts in comparable decimal.
    pub c_amounts: Vec<Balance>,
    /// Volumes accumulated by this pool.
    pub volumes: Vec<SwapVolume>,
    /// Fee charged for swap (gets divided by FEE_DIVISOR).
    pub total_fee: u32,
    /// Shares of the pool by liquidity providers.
    pub shares_prefix: Vec<u8>, // actual type: pub shares: LookupMap<SdkAccountId, Balance>,
    /// Total number of shares.
    pub shares_total_supply: Balance,
    /// Initial amplification coefficient.
    pub init_amp_factor: u128,
    /// Target for ramping up amplification coefficient.
    pub target_amp_factor: u128,
    /// Initial amplification time.
    pub init_amp_time: SdkTimestamp,
    /// Stop ramp up amplification time.
    pub stop_amp_time: SdkTimestamp,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq)]
pub struct RatedSwapPool {
    /// List of tokens in the pool.
    pub token_account_ids: Vec<SdkAccountId>,
    /// Each decimals for tokens in the pool
    pub token_decimals: Vec<u8>,
    /// token amounts in comparable decimal.
    pub c_amounts: Vec<Balance>,
    /// Volumes accumulated by this pool.
    pub volumes: Vec<SwapVolume>,
    /// Fee charged for swap (gets divided by FEE_DIVISOR).
    pub total_fee: u32,
    /// Shares of the pool by liquidity providers.
    pub shares_prefix: Vec<u8>, // actual type: pub shares: LookupMap<SdkAccountId, Balance>,
    /// Total number of shares.
    pub shares_total_supply: Balance,
    /// Initial amplification coefficient.
    pub init_amp_factor: u128,
    /// Target for ramping up amplification coefficient.
    pub target_amp_factor: u128,
    /// Initial amplification time.
    pub init_amp_time: SdkTimestamp,
    /// Stop ramp up amplification time.
    pub stop_amp_time: SdkTimestamp,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq)]
pub struct DegenSwapPool {
    /// List of tokens in the pool.
    pub token_account_ids: Vec<SdkAccountId>,
    /// Each decimals for tokens in the pool
    pub token_decimals: Vec<u8>,
    /// token amounts in comparable decimal.
    pub c_amounts: Vec<Balance>,
    /// Volumes accumulated by this pool.
    pub volumes: Vec<SwapVolume>,
    /// Fee charged for swap (gets divided by FEE_DIVISOR).
    pub total_fee: u32,
    /// Shares of the pool by liquidity providers.
    pub shares_prefix: Vec<u8>, // actual type: pub shares: LookupMap<SdkAccountId, Balance>,
    /// Total number of shares.
    pub shares_total_supply: Balance,
    /// Initial amplification coefficient.
    pub init_amp_factor: u128,
    /// Target for ramping up amplification coefficient.
    pub target_amp_factor: u128,
    /// Initial amplification time.
    pub init_amp_time: SdkTimestamp,
    /// Stop ramp up amplification time.
    pub stop_amp_time: SdkTimestamp,
}
