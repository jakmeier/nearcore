use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::ONE_NEAR;
use crate::utils::transactions::{
    do_call_contract_with_context, do_create_sharded_subcontract, do_deploy_global_contract,
};
use near_async::time::Duration;
use near_chain_configs::MIN_GAS_PRICE;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_o11y::testonly::init_test_logger;
use near_parameters::RuntimeConfigStore;
use near_primitives::action::{GlobalContractDeployMode, GlobalContractIdentifier};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::AccountId;
use near_primitives::version::ProtocolFeature;
use near_primitives_core::subcontract::{ContractContext, SubcontractPermission};
use near_vm_runner::ContractCode;

struct TestLoopUser {
    pub env: TestLoopEnv,
    // pub runtime_config_store: RuntimeConfigStore,
    pub contract: ContractCode,
    pub account_shard_0: AccountId,
    pub account_shard_1: AccountId,
    pub account_3: AccountId,
    pub rpc: AccountId,
    // pub nonce: u64,
}

impl TestLoopUser {
    fn new() -> Self {
        init_test_logger();

        let [account_shard_0, account_shard_1, account_3, rpc] =
            ["account0", "account2", "account", "rpc"].map(|acc| acc.parse::<AccountId>().unwrap());

        let boundary_accounts = ["account1"].iter().map(|&a| a.parse().unwrap()).collect();
        let shard_layout = ShardLayout::multi_shard_custom(boundary_accounts, 1);
        let block_and_chunk_producers = ["cp0", "cp1"];
        let chunk_validators_only = ["cv0", "cv1"];
        let validators_spec =
            ValidatorsSpec::desired_roles(&block_and_chunk_producers, &chunk_validators_only);

        let initial_balance = ONE_NEAR * 100;
        let genesis = TestLoopBuilder::new_genesis_builder()
            .validators_spec(validators_spec)
            .shard_layout(shard_layout)
            .add_user_accounts_simple(
                &[account_shard_0.clone(), account_shard_1.clone(), account_3.clone()],
                initial_balance,
            )
            .gas_prices(MIN_GAS_PRICE, 10 * MIN_GAS_PRICE)
            // Test also that before rejects
            .protocol_version(ProtocolFeature::ShardedContracts.protocol_version())
            .build();
        let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);

        let clients = block_and_chunk_producers
            .iter()
            .chain(chunk_validators_only.iter())
            .map(|acc| acc.parse().unwrap())
            .chain(std::iter::once(rpc.clone()))
            .collect();
        let runtime_config_store = RuntimeConfigStore::new(None);
        let env = TestLoopBuilder::new()
            .genesis(genesis)
            .clients(clients)
            .epoch_config_store(epoch_config_store)
            .runtime_config_store(runtime_config_store.clone())
            .build()
            .warmup();
        // TODO(sharded_contract): this also needs to support calling the new host functions
        let contract = ContractCode::new(near_test_contracts::rs_contract().to_vec(), None);

        Self {
            env,
            // runtime_config_store,
            account_shard_0,
            account_shard_1,
            account_3,
            contract,
            rpc,
            // nonce: 1,
        }
    }
}

#[test]
fn create_subcontract_by_account_id() {
    init_test_logger();
    let mut user = TestLoopUser::new();

    do_deploy_global_contract(
        &mut user.env,
        &user.rpc,
        &user.account_3,
        user.contract.code().to_vec(),
        GlobalContractDeployMode::AccountId,
    );

    do_create_sharded_subcontract(
        &mut user.env,
        &user.rpc,
        &user.account_shard_1,
        &GlobalContractIdentifier::AccountId(user.account_3.clone()),
        &SubcontractPermission::FullAccess,
        ONE_NEAR,
    );

    user.env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

#[test]
fn call_subcontract_by_account_id() {
    init_test_logger();
    let mut user = TestLoopUser::new();

    do_deploy_global_contract(
        &mut user.env,
        &user.rpc,
        &user.account_3,
        user.contract.code().to_vec(),
        GlobalContractDeployMode::AccountId,
    );

    do_create_sharded_subcontract(
        &mut user.env,
        &user.rpc,
        &user.account_shard_1,
        &GlobalContractIdentifier::AccountId(user.account_3.clone()),
        &SubcontractPermission::FullAccess,
        ONE_NEAR,
    );

    let lazy_creation = false;
    // TODO: should fail with 0
    let initial_allowance = 0; //ONE_NEAR;
    do_call_contract_with_context(
        &mut user.env,
        &user.rpc,
        &user.account_shard_0,
        &user.account_shard_1,
        "run_test_with_storage_change".to_owned(),
        vec![],
        ContractContext::ShardedByAccountId { account_id: user.account_3.clone() },
        lazy_creation,
        initial_allowance,
    );

    // TODO(sharded_contract) test state changes are in subcontract but not in parent

    user.env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

#[test]
fn call_subcontract_by_account_id_create_lazily() {
    init_test_logger();
    let mut user = TestLoopUser::new();

    do_deploy_global_contract(
        &mut user.env,
        &user.rpc,
        &user.account_3,
        user.contract.code().to_vec(),
        GlobalContractDeployMode::AccountId,
    );

    let lazy_creation = true;
    // TODO: should fail with 0
    let initial_allowance = 0; //ONE_NEAR;
    do_call_contract_with_context(
        &mut user.env,
        &user.rpc,
        &user.account_shard_0,
        &user.account_shard_1,
        "run_test_with_storage_change".to_owned(),
        vec![],
        ContractContext::ShardedByAccountId { account_id: user.account_3.clone() },
        lazy_creation,
        initial_allowance,
    );

    // TODO(sharded_contract) test state changes are in subcontract but not in parent

    user.env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

// TODO(sharded_contract) test cases
// - forge invalid caller context in switch context
// - deploy global contract, set permission and call the method all in one receipt
// - same as above but also delete in various slots (in the end, before the call, etc)
// - weird combos of switch context, e.g. as last action, or multiple of them in a row
// - gas costs: lazy vs non-lazy
// - storage balance isolation
// - permissions denied in limited (function calls, transfers, stake, deploy, create subaccount, create new subcontract, change permissions of existing...)
// - implicit creation with missing global contract
// ...
