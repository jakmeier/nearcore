use crate::hash::CryptoHash;
use crate::types::Balance;
use borsh::{BorshDeserialize, BorshSerialize};
use near_account_id::AccountId;
use near_schema_checker_lib::ProtocolSchema;

#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum ContractContext {
    /// The root context is the default context, used when running in the main
    /// namespace of an account.
    Root,
    /// Running under a sharded contract context, defined by a globally deployed
    /// code by account id.
    ShardedByAccountId { account_id: AccountId },
    /// Running under a sharded contract context, defined by a globally deployed
    /// code by code hash.
    ShardedByCodeHash { code_hash: CryptoHash },
}

/// Defines permissions for a subcontract context.
#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum SubcontractPermission {
    FullAccess,
    Limited { reserved_balance: Balance },
}
