//! NearVM (wasmer-fork / singlepass) bench utilities.
//!
//! Parallel to `wasmtime_runner::bench` but for the in-house NearVM engine.
//! Exposed under the `bench_utils` feature only.

use super::memory::NearVmMemory;
use super::runner::{NearVM, build_imports, get_entrypoint_index};
use crate::logic::mocks::mock_external::MockedExternal;
use crate::logic::{ExecutionResultState, VMContext, VMLogic};
use crate::wasmtime_runner::bench::{ExecMetrics, InitCallOwned, StateSnapshot};
use near_parameters::RuntimeFeesConfig;
use near_parameters::vm::{Config, VMKind};
use near_primitives_core::gas::Gas;
use near_primitives_core::types::Balance;
use near_vm_engine::universal::{UniversalArtifact, UniversalExecutableRef};
use std::rc::Rc;
use std::sync::Arc;

type VMArtifact = Arc<UniversalArtifact>;

/// A NearVM engine ready to prepare, compile, and instantiate modules.
pub struct NearBenchEngine {
    vm: Arc<NearVM>,
    strategy_name: &'static str,
    metered: bool,
}

impl NearBenchEngine {
    /// Production config: gas charged per WASM instruction (`regular_op_cost > 0`).
    pub fn with_gas(near_config: Arc<Config>) -> Self {
        Self { vm: Arc::new(NearVM::new(near_config)), strategy_name: "near-vm", metered: true }
    }

    /// No-gas baseline: `regular_op_cost = 0`, so WASM instruction gas is zero.
    pub fn no_gas(near_config: Arc<Config>) -> Self {
        let mut cfg = (*near_config).clone();
        cfg.regular_op_cost = 0;
        Self {
            vm: Arc::new(NearVM::new(Arc::new(cfg))),
            strategy_name: "near-vm-ng",
            metered: false,
        }
    }

    pub fn strategy_name(&self) -> &'static str {
        self.strategy_name
    }

    pub fn is_metered(&self) -> bool {
        self.metered
    }

    /// Run the basic WASM preparation pass (validation, export renaming, block-limit checks).
    /// Note: for NearVM, gas instrumentation (finite-wasm analysis) happens inside `compile`.
    pub fn prepare_wasm(&self, raw_wasm: &[u8]) -> Result<Vec<u8>, String> {
        // Force vm_kind = NearVm so prepare_v3 doesn't prefix exports with '\0'.
        // The production config has vm_kind = Wasmtime since protocol 84, but
        // NearVM's get_entrypoint_index looks up the raw export name without any prefix.
        let mut cfg = (*self.vm.config).clone();
        cfg.vm_kind = VMKind::NearVm;
        crate::prepare::prepare_contract(raw_wasm, &Arc::new(cfg), VMKind::NearVm)
            .map_err(|e| format!("{e:?}"))
    }

    /// Compile prepared wasm: runs finite-wasm gas analysis and singlepass codegen, then serializes.
    pub fn compile(&self, prepared_wasm: &[u8]) -> Result<Vec<u8>, String> {
        let vm_ref: &NearVM = &self.vm;
        let executable =
            self.vm.engine.compile_universal(prepared_wasm, &vm_ref).map_err(|e| e.to_string())?;
        executable.serialize().map_err(|e| format!("serialize error: {e}"))
    }

    /// Deserialize a compiled artifact and build a `NearBenchModule`.
    pub fn link_module(&self, compiled: &[u8]) -> Result<NearBenchModule, String> {
        let executable = unsafe {
            UniversalExecutableRef::deserialize(compiled)
                .map_err(|_| "near-vm deserialize error".to_string())?
        };
        let artifact = self
            .vm
            .engine
            .load_universal_executable_ref(&executable)
            .map(Arc::new)
            .map_err(|e| format!("near-vm load error: {e}"))?;
        Ok(NearBenchModule {
            artifact,
            vm: Arc::clone(&self.vm),
            near_config: Arc::clone(&self.vm.config),
            strategy_name: self.strategy_name,
            metered: self.metered,
            current_account_id: "test.near".to_string(),
        })
    }
}

/// A linked NearVM module ready to instantiate and call repeatedly.
pub struct NearBenchModule {
    artifact: VMArtifact,
    vm: Arc<NearVM>,
    near_config: Arc<Config>,
    strategy_name: &'static str,
    metered: bool,
    pub current_account_id: String,
}

impl NearBenchModule {
    pub fn strategy_name(&self) -> &'static str {
        self.strategy_name
    }

    pub fn run_once(
        &self,
        method: &str,
        args: &[u8],
        fees: Arc<RuntimeFeesConfig>,
    ) -> Result<ExecMetrics, String> {
        let mut ext = MockedExternal::new();
        self.run_once_with_ext(method, args, fees, &mut ext)
    }

    pub fn run_once_with_state(
        &self,
        method: &str,
        args: &[u8],
        fees: Arc<RuntimeFeesConfig>,
        state: &StateSnapshot,
    ) -> Result<ExecMetrics, String> {
        let mut ext = MockedExternal::new();
        ext.fake_trie.clone_from(state);
        self.run_once_with_ext(method, args, fees, &mut ext)
    }

    pub fn run_init_sequence_owned(
        &self,
        calls: &[InitCallOwned],
        fees: Arc<RuntimeFeesConfig>,
    ) -> Result<StateSnapshot, String> {
        let mut ext = MockedExternal::new();
        for call in calls {
            let context = make_bench_context_full(
                call.args.clone(),
                &call.current_account_id,
                &call.predecessor_id,
                call.attached_deposit_yocto,
            );
            self.run_with_context(&call.method, fees.clone(), &mut ext, context)?;
        }
        Ok(ext.fake_trie.clone())
    }

    fn run_once_with_ext(
        &self,
        method: &str,
        args: &[u8],
        fees: Arc<RuntimeFeesConfig>,
        ext: &mut MockedExternal,
    ) -> Result<ExecMetrics, String> {
        let context = make_bench_context(args.to_vec(), &self.current_account_id);
        self.run_with_context(method, fees, ext, context)
    }

    fn run_with_context(
        &self,
        method: &str,
        fees: Arc<RuntimeFeesConfig>,
        ext: &mut MockedExternal,
        context: VMContext,
    ) -> Result<ExecMetrics, String> {
        let entrypoint =
            get_entrypoint_index(&*self.artifact, method).map_err(|e| format!("{e:?}"))?;

        let gas_counter = context.make_gas_counter(&self.near_config);
        let result_state =
            ExecutionResultState::new(&context, gas_counter, Arc::clone(&self.near_config));

        let mut memory = NearVmMemory::new(
            self.near_config.limit_config.initial_memory_pages,
            self.near_config.limit_config.max_memory_pages,
        )
        .map_err(|e| format!("memory error: {e}"))?;

        let vmmemory = memory.vm();
        let mut logic = VMLogic::new(ext, &context, fees, result_state, &mut memory);
        let import = build_imports(
            vmmemory,
            &mut logic,
            Arc::clone(&self.near_config),
            self.artifact.engine(),
        );

        match self.vm.run_method(&self.artifact, import, entrypoint) {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(format!("{e:?}")),
            Err(e) => return Err(format!("near-vm error: {e:?}")),
        }

        let gas_burnt =
            if self.metered { logic.result_state.gas_counter.burnt_gas().as_gas() } else { 0 };
        Ok(ExecMetrics { gas_burnt, fuel_used: None })
    }
}

fn make_bench_context(args: Vec<u8>, current_account_id: &str) -> VMContext {
    make_bench_context_full(args, current_account_id, "test.near", 0)
}

fn make_bench_context_full(
    args: Vec<u8>,
    current_account_id: &str,
    predecessor_account_id: &str,
    attached_deposit_yocto: u128,
) -> VMContext {
    VMContext {
        current_account_id: current_account_id.parse().unwrap(),
        signer_account_id: "test.near".parse().unwrap(),
        signer_account_pk: vec![],
        predecessor_account_id: predecessor_account_id.parse().unwrap(),
        refund_to_account_id: "test.near".parse().unwrap(),
        input: Rc::from(args),
        promise_results: vec![].into(),
        block_height: 10,
        block_timestamp: 42,
        epoch_height: 0,
        account_balance: Balance::from_yoctonear(10u128.pow(25)),
        account_locked_balance: Balance::ZERO,
        storage_usage: 10_000_000_000,
        account_contract: near_primitives_core::account::AccountContract::None,
        attached_deposit: Balance::from_yoctonear(attached_deposit_yocto),
        prepaid_gas: Gas::from_teragas(1_000_000),
        random_seed: vec![0, 1, 2],
        view_config: None,
        output_data_receivers: vec![],
    }
}
