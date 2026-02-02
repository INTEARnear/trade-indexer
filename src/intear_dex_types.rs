use std::{fmt::Display, str::FromStr};

use borsh::{BorshDeserialize, BorshSerialize};
use inindexer::{near_indexer_primitives::types::AccountId, near_utils::EventLogData};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(PartialEq, Eq, Hash, Clone, PartialOrd, Ord, Debug, BorshSerialize, BorshDeserialize)]
pub enum AssetId {
    Near,
    Nep141(AccountId),
    Nep245(AccountId, String),
    Nep171(AccountId, String),
}

impl Display for AssetId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Near => write!(f, "near"),
            Self::Nep141(contract_id) => write!(f, "nep141:{contract_id}"),
            Self::Nep245(contract_id, token_id) => write!(f, "nep245:{contract_id}:{token_id}"),
            Self::Nep171(contract_id, token_id) => write!(f, "nep171:{contract_id}:{token_id}"),
        }
    }
}

impl FromStr for AssetId {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "near" => Ok(Self::Near),
            _ => match s.split_once(':') {
                Some(("nep141", contract_id)) => {
                    Ok(Self::Nep141(contract_id.parse().map_err(|e| {
                        format!("Invalid account id {contract_id}: {e}")
                    })?))
                }
                Some(("nep245", rest)) => {
                    if let Some((contract_id, token_id)) = rest.split_once(':') {
                        Ok(Self::Nep245(
                            contract_id
                                .parse()
                                .map_err(|e| format!("Invalid account id {contract_id}: {e}"))?,
                            token_id.to_string(),
                        ))
                    } else {
                        Err(format!("Invalid asset id: {s}"))
                    }
                }
                Some(("nep171", rest)) => {
                    if let Some((contract_id, token_id)) = rest.split_once(':') {
                        Ok(Self::Nep171(
                            contract_id
                                .parse()
                                .map_err(|e| format!("Invalid account id {contract_id}: {e}"))?,
                            token_id.to_string(),
                        ))
                    } else {
                        Err(format!("Invalid asset id: {s}"))
                    }
                }
                _ => Err(format!("Invalid asset id: {s}")),
            },
        }
    }
}

impl Serialize for AssetId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serde::Serialize::serialize(&self.to_string(), serializer)
    }
}

impl<'de> Deserialize<'de> for AssetId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(PartialEq, Eq, Hash, Clone, PartialOrd, Ord, BorshSerialize, BorshDeserialize, Debug)]
pub struct DexId {
    pub deployer: AccountId,
    pub id: String,
}

impl Display for DexId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.deployer, self.id)
    }
}

impl FromStr for DexId {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (deployer, id) = s.split_once('/').ok_or(format!("Invalid dex id: {s}"))?;
        Ok(Self {
            deployer: deployer
                .parse()
                .map_err(|e| format!("Invalid deployer id {deployer}: {e}"))?,
            id: id.to_string(),
        })
    }
}

impl Serialize for DexId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Serialize::serialize(&self.to_string(), serializer)
    }
}

impl<'de> Deserialize<'de> for DexId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <String as Deserialize<'de>>::deserialize(deserializer)?;
        Ok(Self::from_str(&s).unwrap())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct SwapRequest {
    pub message: Base64VecU8,
    pub asset_in: AssetId,
    pub asset_out: AssetId,
    pub amount: SwapRequestAmount,
}

type Base64VecU8 = String;

#[derive(
    Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
pub enum SwapRequestAmount {
    ExactIn(U128),
    ExactOut(U128),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, BorshSerialize, BorshDeserialize)]
pub struct U128(pub u128);

impl Serialize for U128 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Serialize::serialize(&self.0.to_string(), serializer)
    }
}

impl<'de> Deserialize<'de> for U128 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <String as Deserialize<'de>>::deserialize(deserializer)?;
        Ok(Self(s.parse().map_err(serde::de::Error::custom)?))
    }
}

#[derive(Deserialize, Debug)]
pub struct DexEvent<T> {
    pub dex_id: DexId,
    pub event: EventLogData<T>,
    pub referrer: Option<AccountId>,
    pub user: Option<AccountId>,
}
