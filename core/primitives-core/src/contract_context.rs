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
    /// Running under a sharded contract context, defined by a globally deployed code.
    ///
    /// TODO(sharded_contract): Do we need to support by account and by code? Or should it just be by name?
    // Sharded { code_id: GlobalContractIdentifier },
    Sharded { account_id: AccountId },
}

/// Defines permissions for a contract context.
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
pub enum ContextPermission {
    FullAccess,
    Limited { reserved_balance: Balance },
    Blocked,
}
