//! State viewer functions to list and filter accounts that have contracts
//! deployed.

use anyhow::Context;
use borsh::BorshDeserialize;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{Receipt, ReceiptEnum};
use near_primitives::transaction::{Action, ExecutionOutcomeWithProof};
use near_primitives::trie_key::trie_key_parsers::parse_account_id_from_contract_code_key;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::AccountId;
use near_store::{DBCol, NibbleSlice, StorageError, Store, Trie, TrieTraversalItem};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

/// Output type for contract account queries with all relevant data around a
/// single contract.
pub(crate) struct ContractAccount {
    pub(crate) account_id: AccountId,
    pub(crate) source_wasm: Arc<[u8]>,
    // /// Actions that have been observed to be triggered by the contract.
    // pub(crate) actions: BTreeSet<ActionType>,
}

#[derive(Debug, thiserror::Error)]
pub enum ContractAccountError {
    #[error("could not parse key {1:?}")]
    InvalidKey(#[source] std::io::Error, Vec<u8>),
    #[error("failed loading contract code for account {1}")]
    NoCode(#[source] StorageError, AccountId),
}

/// List of supported actions to filter for.
///
/// When filtering for an action, only those contracts will be listed that have
/// executed that action from within a recorded function call.
#[derive(clap::ArgEnum, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[repr(u8)]
pub(crate) enum ActionType {
    CreateAccount,
    DeployContract,
    FunctionCall,
    Transfer,
    Stake,
    AddKey,
    DeleteKey,
    DeleteAccount,
    DataReceipt,
}

impl std::fmt::Display for ContractAccount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:<64} {:>9}", self.account_id, self.source_wasm.len())
    }
}

impl ContractAccount {
    /// Iterate over all contracts stored in the given trie, in lexicographic
    /// order of the account IDs.
    pub(crate) fn in_trie(trie: &Trie) -> anyhow::Result<ContractAccountIterator> {
        ContractAccountIterator::new(trie)
    }

    fn from_contract_trie_node(
        trie_key: &[u8],
        value_hash: CryptoHash,
        trie: &Trie,
    ) -> Result<Self, ContractAccountError> {
        let account_id = parse_account_id_from_contract_code_key(trie_key)
            .map_err(|err| ContractAccountError::InvalidKey(err, trie_key.to_vec()))?;
        let source_wasm = trie
            .storage
            .retrieve_raw_bytes(&value_hash)
            .map_err(|err| ContractAccountError::NoCode(err, account_id.clone()))?;

        Ok(Self { account_id, source_wasm })
    }
}

pub(crate) struct ContractAccountIterator<'a> {
    /// Trie nodes that point to the contracts.
    contract_nodes: VecDeque<TrieTraversalItem>,
    trie: &'a Trie,
}

impl<'a> ContractAccountIterator<'a> {
    pub(crate) fn new(trie: &'a Trie) -> anyhow::Result<Self> {
        let mut trie_iter = trie.iter()?;
        // TODO(#8376): Consider changing the interface to TrieKey to make this easier.
        // `TrieKey::ContractCode` requires a valid `AccountId`, we use "xx"
        let key = TrieKey::ContractCode { account_id: "xx".parse()? }.to_vec();
        let (prefix, suffix) = key.split_at(key.len() - 2);
        assert_eq!(suffix, "xx".as_bytes());

        // `visit_nodes_interval` wants nibbles stored in `Vec<u8>` as input
        let nibbles_before: Vec<u8> = NibbleSlice::new(prefix).iter().collect();
        let nibbles_after = {
            let mut tmp = nibbles_before.clone();
            *tmp.last_mut().unwrap() += 1;
            tmp
        };

        // finally, use trie iterator to find all contract nodes
        let vec_of_nodes = trie_iter.visit_nodes_interval(&nibbles_before, &nibbles_after)?;
        let contract_nodes = VecDeque::from(vec_of_nodes);
        Ok(Self { contract_nodes, trie })
    }

    /// todo
    pub(crate) fn actions(self, store: &Store) -> BTreeMap<AccountId, BTreeSet<ActionType>> {
        // Find all accounts with contract and create an empty set of actions for each.
        let mut accounts: BTreeMap<_, _> = self
            .flat_map(|result| match result {
                Ok(contract) => Some((contract.account_id, BTreeSet::new())),
                Err(e) => {
                    eprintln!("skipping contract due to {e}");
                    None
                }
            })
            .collect();

        // TODO: iterate receipts
        // TODO: currently this is repeated per shard, which is bad
        for pair in store.iter(near_store::DBCol::Receipts) {
            if let Err(e) = try_find_actions(pair, &mut accounts, store) {
                eprintln!("skipping receipt due to {e}");
            }
        }
        accounts
    }
}

// todo: filter for receiver, -> outcome -> receipt.actions
fn try_find_actions(
    raw_kv_pair: std::io::Result<(Box<[u8]>, Box<[u8]>)>,
    accounts: &mut BTreeMap<AccountId, BTreeSet<ActionType>>,
    store: &Store,
) -> anyhow::Result<()> {
    // key: receipt (CryptoHash)
    let (raw_receipt_hash, raw_value) = raw_kv_pair?;
    let receipt = Receipt::deserialize(&mut raw_value.as_ref())?;

    // TODO: consider entry API
    if accounts.contains_key(&receipt.receiver_id) {
        // yes, this is a contract in our list
        // next, check the execution result(s)
        for pair in store.iter_prefix_ser::<ExecutionOutcomeWithProof>(
            DBCol::TransactionResultForBlock,
            &raw_receipt_hash,
        ) {
            let (_key, outcome) = pair?;
            for outgoing_receipt_id in &outcome.outcome.receipt_ids {
                let outgoing_receipt: Receipt = store
                    .get_ser(near_store::DBCol::Receipts, outgoing_receipt_id.as_bytes())?
                    .context("missing outgoing receipt")?;
                let entry = accounts.get_mut(&receipt.receiver_id).unwrap();
                match outgoing_receipt.receipt {
                    ReceiptEnum::Action(action_receipt) => {
                        for action in &action_receipt.actions {
                            let action_type = match action {
                                Action::CreateAccount(_) => ActionType::CreateAccount,
                                Action::DeployContract(_) => ActionType::DeployContract,
                                Action::FunctionCall(_) => ActionType::FunctionCall,
                                Action::Transfer(_) => ActionType::Transfer,
                                Action::Stake(_) => ActionType::Stake,
                                Action::AddKey(_) => ActionType::AddKey,
                                Action::DeleteKey(_) => ActionType::DeleteKey,
                                Action::DeleteAccount(_) => ActionType::DeleteAccount,
                            };
                            entry.insert(action_type);
                        }
                    }
                    ReceiptEnum::Data(_) => {
                        entry.insert(ActionType::DataReceipt);
                    }
                }
            }
        }
    }
    Ok(())
}

impl Iterator for ContractAccountIterator<'_> {
    type Item = Result<ContractAccount, ContractAccountError>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(item) = self.contract_nodes.pop_front() {
            // only look at nodes with a value, ignoring intermediate nodes
            // without values
            if let TrieTraversalItem { hash, key: Some(trie_key) } = item {
                let contract = ContractAccount::from_contract_trie_node(&trie_key, hash, self.trie);
                return Some(contract);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::ContractAccount;
    use near_primitives::trie_key::TrieKey;
    use near_store::test_utils::{create_tries, test_populate_trie};
    use near_store::{ShardUId, Trie};

    #[test]
    fn test_three_contracts() {
        let tries = create_tries();
        let initial = vec![
            contract_tuple("caroline.near", 3),
            contract_tuple("alice.near", 1),
            contract_tuple("alice.nearx", 2),
            // data right before contracts in trie order
            account_tuple("xeno.near", 1),
            // data right after contracts in trie order
            access_key_tuple("alan.near", 1),
        ];
        let root = test_populate_trie(&tries, &Trie::EMPTY_ROOT, ShardUId::single_shard(), initial);
        let trie = tries.get_trie_for_shard(ShardUId::single_shard(), root);

        let contract_accounts: Vec<_> =
            ContractAccount::in_trie(&trie).expect("failed creating iterator").collect();
        assert_eq!(3, contract_accounts.len(), "wrong number of contracts returned by iterator");

        // expect reordering toe lexicographic order
        let contract1 = contract_accounts[0].as_ref().expect("returned error instead of contract");
        let contract2 = contract_accounts[1].as_ref().expect("returned error instead of contract");
        let contract3 = contract_accounts[2].as_ref().expect("returned error instead of contract");
        assert_eq!(contract1.account_id.as_str(), "alice.near");
        assert_eq!(contract2.account_id.as_str(), "alice.nearx");
        assert_eq!(contract3.account_id.as_str(), "caroline.near");
        assert_eq!(&*contract1.source_wasm, &[1u8, 1, 1]);
        assert_eq!(&*contract2.source_wasm, &[2u8, 2, 2]);
        assert_eq!(&*contract3.source_wasm, &[3u8, 3, 3]);
    }

    /// Create a test contract key-value pair to insert in the test trie.
    fn contract_tuple(account: &str, num: u8) -> (Vec<u8>, Option<Vec<u8>>) {
        (
            TrieKey::ContractCode { account_id: account.parse().unwrap() }.to_vec(),
            Some(vec![num, num, num]),
        )
    }

    /// Create a test account key-value pair to insert in the test trie.
    fn account_tuple(account: &str, num: u8) -> (Vec<u8>, Option<Vec<u8>>) {
        (TrieKey::Account { account_id: account.parse().unwrap() }.to_vec(), Some(vec![num, num]))
    }

    /// Create a test access key key-value pair to insert in the test trie.
    fn access_key_tuple(account: &str, num: u8) -> (Vec<u8>, Option<Vec<u8>>) {
        (
            TrieKey::AccessKey {
                account_id: account.parse().unwrap(),
                public_key: near_crypto::PublicKey::empty(near_crypto::KeyType::ED25519),
            }
            .to_vec(),
            Some(vec![num, num, num, num]),
        )
    }
}
