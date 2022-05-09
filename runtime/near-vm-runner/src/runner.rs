use near_primitives::checked_feature;
use near_primitives::config::VMConfig;
use near_primitives::contract::ContractCode;
use near_primitives::hash::CryptoHash;
use near_primitives::profile::ProfileData;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::types::CompiledContractCache;
use near_primitives::version::ProtocolVersion;
use near_vm_errors::{FunctionCallError, MethodResolveError, VMError};
use near_vm_logic::gas_counter::GasCounter;
use near_vm_logic::types::PromiseResult;
use near_vm_logic::{ExtCosts, External, ReturnData, VMContext, VMLogic, VMOutcome};

use crate::vm_kind::VMKind;

/// Validate and run the specified contract.
///
/// This is the entry point for executing a NEAR protocol contract. Before the entry point (as
/// specified by the `method_name` argument) of the contract code is executed, the contract will be
/// validated (see [`prepare::prepare_contract`]), instrumented (e.g. for gas accounting), and
/// linked with the externs specified via the `ext` argument.
///
/// [`VMContext::input`] will be passed to the contract entrypoint as an argument.
///
/// The contract will be executed with the default VM implementation for the current protocol
/// version. In order to specify a different VM implementation call [`run_vm`] instead.
///
/// The gas cost for contract preparation will be subtracted by the VM implementation.
pub fn run(
    code: &ContractCode,
    method_name: &str,
    ext: &mut dyn External,
    context: VMContext,
    wasm_config: &VMConfig,
    fees_config: &RuntimeFeesConfig,
    promise_results: &[PromiseResult],
    current_protocol_version: ProtocolVersion,
    cache: Option<&dyn CompiledContractCache>,
) -> VMResult {
    let vm_kind = VMKind::for_protocol_version(current_protocol_version);

    run_with_vm_kind(
        code,
        method_name,
        ext,
        context,
        wasm_config,
        fees_config,
        promise_results,
        current_protocol_version,
        cache,
        vm_kind,
    )
}

/// Usually the VM is defined by the protocol version. For test and benchmarks,
/// however, it is useful to have an entry point that can specify protocol
/// version and VM kind independently.
pub(crate) fn run_with_vm_kind(
    code: &ContractCode,
    method_name: &str,
    ext: &mut dyn External,
    context: VMContext,
    wasm_config: &VMConfig,
    fees_config: &RuntimeFeesConfig,
    promise_results: &[PromiseResult],
    current_protocol_version: ProtocolVersion,
    cache: Option<&dyn CompiledContractCache>,
    vm_kind: VMKind,
) -> VMResult {
    if method_name.is_empty() {
        let error = VMError::FunctionCallError(FunctionCallError::MethodResolveError(
            MethodResolveError::MethodEmptyName,
        ));
        return VMResult::nop_outcome(error);
    }

    let mut gas_counter = context.new_gas_counter(wasm_config);
    if checked_feature!(
        "protocol_feature_fix_contract_loading_cost",
        FixContractLoadingCost,
        current_protocol_version
    ) {
        let code_bytes = code.code().len() as u64;

        if gas_counter.pay_per(ExtCosts::contract_loading_bytes, code_bytes).is_err()
            || gas_counter.pay_base(ExtCosts::contract_loading_base).is_err()
        {
            let error = VMError::FunctionCallError(FunctionCallError::HostError(
                near_vm_errors::HostError::GasExceeded,
            ));

            return VMResult::not_run_with_gas_counter(gas_counter, error);
        }
    }

    if let Some(runtime) = vm_kind.runtime(wasm_config.clone()) {
        runtime.run(
            code,
            method_name,
            ext,
            context,
            fees_config,
            gas_counter,
            promise_results,
            current_protocol_version,
            cache,
        )
    } else {
        panic!("the {:?} runtime has not been enabled at compile time", vm_kind);
    }
}

pub trait VM {
    /// Validate and run the specified contract.
    ///
    /// This is the entry point for executing a NEAR protocol contract. Before the entry point (as
    /// specified by the `method_name` argument) of the contract code is executed, the contract
    /// will be validated (see [`prepare::prepare_contract`]), instrumented (e.g. for gas
    /// accounting), and linked with the externs specified via the `ext` argument.
    ///
    /// [`VMContext::input`] will be passed to the contract entrypoint as an argument.
    ///
    /// XXX The gas cost for contract preparation will be subtracted by the VM implementation.
    fn run(
        &self,
        code: &ContractCode,
        method_name: &str,
        ext: &mut dyn External,
        context: VMContext,
        fees_config: &RuntimeFeesConfig,
        gas_counter: GasCounter,
        promise_results: &[PromiseResult],
        current_protocol_version: ProtocolVersion,
        cache: Option<&dyn CompiledContractCache>,
    ) -> VMResult;

    /// Precompile a WASM contract to a VM specific format and store the result into the `cache`.
    ///
    /// Further calls to [`Runtime::run`] or [`Runtime::precompile`] with the same `code`, `cache`
    /// and [`VMConfig`] may reuse the results of this precompilation step.
    fn precompile(
        &self,
        code: &[u8],
        code_hash: &CryptoHash,
        cache: &dyn CompiledContractCache,
    ) -> Option<VMError>;

    /// Verify the `code` contract can be compiled with this [`Runtime`].
    ///
    /// This is intended primarily for testing purposes.
    fn check_compile(&self, code: &Vec<u8>) -> bool;
}

impl VMKind {
    /// Make a [`Runtime`] for this [`VMKind`].
    ///
    /// This is not intended to be used by code other than standalone-vm-runner.
    pub fn runtime(&self, config: VMConfig) -> Option<Box<dyn VM>> {
        match self {
            #[cfg(all(feature = "wasmer0_vm", target_arch = "x86_64"))]
            Self::Wasmer0 => Some(Box::new(crate::wasmer_runner::Wasmer0VM::new(config))),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime => Some(Box::new(crate::wasmtime_runner::WasmtimeVM::new(config))),
            #[cfg(all(feature = "wasmer2_vm", target_arch = "x86_64"))]
            Self::Wasmer2 => Some(Box::new(crate::wasmer2_runner::Wasmer2VM::new(config))),
            #[allow(unreachable_patterns)] // reachable when some of the VMs are disabled.
            _ => None,
        }
    }
}

/// Type returned by any VM instance after loading a contract and executing a
/// function inside.
#[derive(Debug, PartialEq)]
pub enum VMResult {
    /// Execution started but hit an error.
    Aborted(VMOutcome, VMError),
    /// Execution finished without error.
    Ok(VMOutcome),
}

impl VMResult {
    /// Consumes the `VMLogic` object and computes the final outcome with the
    /// given error that stopped execution from finishing successfully.
    pub fn abort(logic: VMLogic, error: VMError) -> VMResult {
        let outcome = logic.compute_outcome_and_distribute_gas();
        VMResult::Aborted(outcome, error)
    }

    /// Consumes the `VMLogic` object and computes the final outcome for a
    /// successful execution.
    pub fn ok(logic: VMLogic) -> VMResult {
        let outcome = logic.compute_outcome_and_distribute_gas();
        VMResult::Ok(outcome)
    }

    /// Creates an outcome with a no-op outcome.
    pub fn nop_outcome(error: VMError) -> VMResult {
        let outcome = VMOutcome {
            // Note: Balance and storage fields are ignored on a failed outcome.
            balance: 0,
            storage_usage: 0,
            // Note: Fields below are added or merged when processing the
            // outcome. With 0 or the empty set, those are no-ops.
            return_data: ReturnData::None,
            burnt_gas: 0,
            used_gas: 0,
            logs: Vec::new(),
            profile: ProfileData::default(),
            action_receipts: Vec::new(),
        };
        VMResult::Aborted(outcome, error)
    }

    /// Creates an outcome with a no-op outcome other than spending some gas.
    pub fn not_run_with_gas_counter(gas_counter: GasCounter, error: VMError) -> VMResult {
        let burnt_gas = gas_counter.burnt_gas();
        let used_gas = gas_counter.used_gas();

        let mut profile = gas_counter.profile_data();
        profile.compute_wasm_instruction_cost(burnt_gas);

        let outcome = VMOutcome {
            // Note: Balance and storage fields are ignored on a failed outcome.
            balance: 0,
            storage_usage: 0,
            return_data: ReturnData::None,
            burnt_gas,
            used_gas,
            logs: Vec::new(),
            profile,
            action_receipts: Vec::new(),
        };
        VMResult::Aborted(outcome, error)
    }

    /// Borrow the internal outcome, if there is one.
    pub fn outcome(&self) -> &VMOutcome {
        match self {
            VMResult::Aborted(outcome, _err) => outcome,
            VMResult::Ok(outcome) => outcome,
        }
    }

    /// Borrow the internal error, if there is one.
    pub fn error(&self) -> Option<&VMError> {
        match self {
            VMResult::Aborted(_outcome, err) => Some(err),
            VMResult::Ok(_outcome) => None,
        }
    }

    /// Unpack the internal outcome and error. This method mostly exists for
    /// easy compatibility with code that was written before `VMResult` existed.
    pub fn outcome_error(self) -> (VMOutcome, Option<VMError>) {
        match self {
            VMResult::Aborted(outcome, err) => (outcome, Some(err)),
            VMResult::Ok(outcome) => (outcome, None),
        }
    }
}
