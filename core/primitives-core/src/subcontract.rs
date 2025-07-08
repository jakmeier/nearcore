use crate::hash::CryptoHash;
use crate::types::{Balance, StorageUsage};
use borsh::{BorshDeserialize, BorshSerialize};
use near_account_id::AccountId;
use near_schema_checker_lib::ProtocolSchema;

/// Meta data for a subcontract.
///
/// A subcontract is a isolated module that belongs to an account but runs a
/// different contract code than the main module.
///
/// The smart contract storage space is always isolated for a subcontract.
///
/// Balance and storage limits may be shared with the main account if the
/// subcontract has full access. For limited modules, the storage limits are
/// separate and the balance is generally not accessible.
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
pub enum Subcontract {
    V1(SubcontractV1),
}

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
pub struct SubcontractV1 {
    /// Defines what a subcontract can do on the account in which it has been deployed.
    pub permission: SubcontractPermission,
    /// Number of bytes used in the trie for storing this subcontract.
    pub storage_usage: StorageUsage,
    /// Amount of NEAR tokens that have been burnt for storage of this subcontract.
    pub storage_allowance: Balance,
}

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
    Limited,
    // Note: When adding structured variants, the storage requirements need to
    // be recomputed when `SetSubcontractPermissionAction` changes permissions.
}

impl Subcontract {
    /// Create a subcontract meta data struct with correct storage usage.
    pub fn new(
        permission: SubcontractPermission,
        key_size: u64,
        num_extra_bytes_record: StorageUsage,
    ) -> Self {
        let mut this = Subcontract::V1(SubcontractV1 {
            permission,
            storage_allowance: 0,
            // real value is set right below
            storage_usage: 0,
        });

        let usage = this.base_storage_usage(key_size, num_extra_bytes_record);
        this.set_storage_usage(usage);
        this
    }

    pub fn permission(&self) -> &SubcontractPermission {
        match self {
            Subcontract::V1(subcontract) => &subcontract.permission,
        }
    }

    pub fn set_permission(&mut self, new_permission: SubcontractPermission) {
        // Note: At the time, all permissions use the same storage amount, no
        // need to update storage requirements. But let's add a check to ensure
        // this stays this way.
        debug_assert_eq!(
            borsh::object_length(self.permission()).unwrap(),
            borsh::object_length(&new_permission).unwrap(),
            "need to handle different permission sizes in storage usage"
        );
        match self {
            Subcontract::V1(subcontract) => {
                subcontract.permission = new_permission;
            }
        }
    }

    /// How much storage is actually used in the trie, including meta data and
    /// contract data.
    ///
    /// This value is not included in the main account `storage_usage`. Tracking
    /// is done separately for the main account and subcontracts.
    ///
    /// To compute the required balance to be locked, this number needs to be
    /// multiplied by `nonrefundable_storage_amount_per_byte`.
    pub fn storage_usage(&self) -> StorageUsage {
        match self {
            Subcontract::V1(subcontract) => subcontract.storage_usage,
        }
    }

    /// How much balance has been burnt to cover nonrefundable storage costs of
    /// the subcontract.
    ///
    /// To compute the allowed bytes, this number needs to be divided by
    /// `nonrefundable_storage_amount_per_byte`.
    pub fn storage_allowance(&self) -> Balance {
        match self {
            Subcontract::V1(subcontract) => subcontract.storage_allowance,
        }
    }

    pub fn add_storage_allowance(&mut self, added: Balance) {
        match self {
            Subcontract::V1(subcontract) => {
                subcontract.storage_allowance = subcontract
                    .storage_allowance
                    .checked_add(added)
                    .expect("can't have more than u128::MAX balance")
            }
        }
    }

    pub fn set_storage_usage(&mut self, usage: StorageUsage) {
        match self {
            Subcontract::V1(subcontract) => subcontract.storage_usage = usage,
        }
    }

    /// How many bytes are required for storing the subcontract without its WASM
    /// contract state.
    fn base_storage_usage(&self, key_size: u64, num_extra_bytes_record: StorageUsage) -> u64 {
        let meta_data_size = borsh::object_length(self).expect("borsh must not fail") as u64;
        let record_overhead = num_extra_bytes_record;

        [key_size, meta_data_size, record_overhead]
            .iter()
            .try_fold(0u64, |acc, &value| acc.checked_add(value))
            .expect("storage must not be > u64::MAX")
    }
}
