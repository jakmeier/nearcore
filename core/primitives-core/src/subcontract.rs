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
    Limited { reserved_balance: Balance },
}

impl Subcontract {
    /// Create a subcontract meta data struct with correct storage usage.
    ///
    /// Returns `None` if any of the storage related math operations failed.
    pub fn new(
        permission: SubcontractPermission,
        key_size: u64,
        num_extra_bytes_record: StorageUsage,
        storage_amount_per_byte: Balance,
    ) -> Option<Self> {
        let mut this = Subcontract::V1(SubcontractV1 {
            permission,
            // real value is set right below
            storage_usage: 0,
        });

        let usage =
            this.compute_storage_usage(key_size, num_extra_bytes_record, storage_amount_per_byte)?;
        this.set_storage_usage(usage);
        Some(this)
    }

    pub fn permission(&self) -> &SubcontractPermission {
        match self {
            Subcontract::V1(subcontract) => &subcontract.permission,
        }
    }

    /// How much storage is actually used in the trie, including meta data and
    /// contract data.
    ///
    /// This value is always included in the main account `storage_usage`.
    /// Tracking it separately is necessary for permissions changes or deletion
    /// of a submodule.
    ///
    /// Note that for limited access subcontracts, the full reserved amount is
    /// added to the `Account` storage_usage.
    ///
    /// To compute the required balance to be locked, the ZBA limit must
    /// subtracted from this field.
    pub fn storage_usage(&self) -> StorageUsage {
        match self {
            Subcontract::V1(subcontract) => subcontract.storage_usage,
        }
    }

    /// How much storage usage needs to be added to the main account for this subcontract.
    pub fn storage_requirement(&self, subcontract_zba_limit: u64) -> StorageUsage {
        self.storage_usage().saturating_sub(subcontract_zba_limit)
    }

    fn set_storage_usage(&mut self, usage: StorageUsage) {
        match self {
            Subcontract::V1(subcontract) => subcontract.storage_usage = usage,
        }
    }

    fn compute_storage_usage(
        &self,
        key_size: u64,
        num_extra_bytes_record: StorageUsage,
        storage_amount_per_byte: Balance,
    ) -> Option<u64> {
        let data_store_usage = match self.permission() {
            // full access subcontracts use shared storage limits with parent
            // hence, they use the actual storage usage
            SubcontractPermission::FullAccess => self.storage_usage(),
            SubcontractPermission::Limited { reserved_balance } => {
                if storage_amount_per_byte == 0 {
                    // free storage config => no need to lock tokens
                    0
                } else {
                    // limited subcontracts require enough token to be locked for the full reserved amount
                    reserved_balance
                        .checked_div(Balance::from(storage_amount_per_byte))
                        .expect("storage_amount_per_byte is not 0") as u64
                }
            }
        };
        let meta_data_size = borsh::object_length(self).expect("borsh must not fail") as u64;
        let record_overhead = num_extra_bytes_record;

        [data_store_usage, key_size, meta_data_size, record_overhead]
            .iter()
            .try_fold(0u64, |acc, &value| acc.checked_add(value))
    }
}
