use genesis_populate::state_dump::StateDump;
use near_primitives::receipt::Receipt;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::runtime::migration_data::{MigrationData, MigrationFlags};
use near_primitives::test_utils::MockEpochInfoProvider;
use near_primitives::transaction::{ExecutionStatus, SignedTransaction};
use near_primitives::types::{Gas, MerkleHash};
use near_primitives::version::PROTOCOL_VERSION;
use near_store::{ShardTries, ShardUId, Store, StoreCompiledContractCache, TrieUpdate};
use near_vm_logic::VMLimitConfig;
use node_runtime::{ApplyState, Runtime};
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

pub struct RuntimeTestbed {
    /// Directory where we temporarily keep the storage.
    _workdir: tempfile::TempDir,
    tries: ShardTries,
    root: MerkleHash,
    runtime: Runtime,
    prev_receipts: Vec<Receipt>,
    apply_state: ApplyState,
    epoch_info_provider: MockEpochInfoProvider,
}

impl RuntimeTestbed {
    /// Copies dump from another directory and loads the state from it.
    pub fn from_state_dump(dump_dir: &Path, in_memory_db: bool) -> Self {
        let workdir = tempfile::Builder::new().prefix("runtime_testbed").tempdir().unwrap();
        let StateDump { store, roots } =
            StateDump::from_dir(dump_dir, workdir.path(), in_memory_db);
        // Ensure decent RocksDB SST file layout.
        store.compact().expect("compaction failed");

        // Create ShardTries with relevant settings adjusted for estimator.
        let shard_uids = [ShardUId { shard_id: 0, version: 0 }];
        let mut trie_config = near_store::TrieConfig::default();
        trie_config.enable_receipt_prefetching = true;
        let tries = ShardTries::new(
            store.clone(),
            trie_config,
            &shard_uids,
            near_store::flat_state::FlatStateFactory::new(store.clone()),
        );

        assert!(roots.len() <= 1, "Parameter estimation works with one shard only.");
        assert!(!roots.is_empty(), "No state roots found.");
        let root = roots[0];

        let mut runtime_config =
            RuntimeConfigStore::new(None).get_config(PROTOCOL_VERSION).as_ref().clone();

        // Override vm limits config to simplify block processing.
        runtime_config.wasm_config.limit_config = VMLimitConfig {
            max_total_log_length: u64::MAX,
            max_number_registers: u64::MAX,
            max_gas_burnt: u64::MAX,
            max_register_size: u64::MAX,
            max_number_logs: u64::MAX,

            max_actions_per_receipt: u64::MAX,
            max_promises_per_function_call_action: u64::MAX,
            max_number_input_data_dependencies: u64::MAX,

            max_total_prepaid_gas: u64::MAX,

            ..VMLimitConfig::test()
        };
        runtime_config.account_creation_config.min_allowed_top_level_account_length = 0;

        let runtime = Runtime::new();
        let prev_receipts = vec![];

        let apply_state = ApplyState {
            // Put each runtime into a separate shard.
            block_height: 1,
            // Epoch length is long enough to avoid corner cases.
            prev_block_hash: Default::default(),
            block_hash: Default::default(),
            epoch_id: Default::default(),
            epoch_height: 0,
            gas_price: 0,
            block_timestamp: 0,
            gas_limit: None,
            random_seed: Default::default(),
            current_protocol_version: PROTOCOL_VERSION,
            config: Arc::new(runtime_config),
            cache: Some(Box::new(StoreCompiledContractCache::new(&tries.get_store()))),
            is_new_chunk: true,
            migration_data: Arc::new(MigrationData::default()),
            migration_flags: MigrationFlags::default(),
        };

        Self {
            _workdir: workdir,
            tries,
            root,
            runtime,
            prev_receipts,
            apply_state,
            epoch_info_provider: MockEpochInfoProvider::default(),
        }
    }

    pub fn process_block(
        &mut self,
        transactions: &[SignedTransaction],
        allow_failures: bool,
    ) -> Gas {
        let trie = self.trie();
        let apply_result = self
            .runtime
            .apply(
                trie,
                &None,
                &self.apply_state,
                &self.prev_receipts,
                transactions,
                &self.epoch_info_provider,
                Default::default(),
            )
            .unwrap();

        let mut store_update = self.tries.store_update();
        self.root = self.tries.apply_all(
            &apply_result.trie_changes,
            ShardUId::single_shard(),
            &mut store_update,
        );
        store_update.commit().unwrap();
        self.apply_state.block_height += 1;

        let mut total_burnt_gas = 0;
        if !allow_failures {
            for outcome in &apply_result.outcomes {
                total_burnt_gas += outcome.outcome.gas_burnt;
                match &outcome.outcome.status {
                    ExecutionStatus::Failure(e) => panic!("Execution failed {:#?}", e),
                    _ => (),
                }
            }
        }
        self.prev_receipts = apply_result.outgoing_receipts;
        total_burnt_gas
    }

    /// Returns the number of blocks required to reach quiescence
    pub fn process_blocks_until_no_receipts(&mut self, allow_failures: bool) -> usize {
        let mut n = 0;
        while !self.prev_receipts.is_empty() {
            self.process_block(&[], allow_failures);
            n += 1;
        }
        n
    }

    /// Process just the verification of a transaction, without action execution.
    ///
    /// Use this method for measuring the SEND cost of actions. This is the
    /// workload done on the sender's shard before an action receipt is created.
    /// Network costs for sending are not included.
    pub fn verify_transaction(
        &mut self,
        tx: &SignedTransaction,
    ) -> Result<node_runtime::VerificationResult, near_primitives::errors::RuntimeError> {
        let mut state_update = TrieUpdate::new(Rc::new(self.trie()));
        // gas price and block height can be anything, it doesn't affect performance
        let gas_price = 1;
        let block_height = None;
        // do a full verification
        let verify_signature = true;
        node_runtime::verify_and_charge_transaction(
            &self.apply_state.config,
            &mut state_update,
            gas_price,
            tx,
            verify_signature,
            block_height,
            PROTOCOL_VERSION,
        )
    }

    /// Instantiate a new trie for the estimator.
    fn trie(&mut self) -> near_store::Trie {
        self.tries.get_trie_for_shard(ShardUId::single_shard(), self.root.clone())
    }

    /// Flushes RocksDB memtable
    pub fn flush_db_write_buffer(&mut self) {
        self.tries.get_store().flush().unwrap();
    }

    pub fn store(&mut self) -> Store {
        self.tries.get_store()
    }
}
