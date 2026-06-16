//! Gas metering strategy benchmark utilities.
//!
//! Exposed under the `bench_utils` feature. Not for production use.

use super::{
    Ctx, DECOMMIT_BATCH_SIZE, DEFAULT_MAX_ELEMENTS_PER_TABLE, DEFAULT_MAX_TABLES_PER_MODULE,
    ErrorContainer, MAX_CONCURRENCY, guest_memory_size, link,
};
use crate::logic::mocks::mock_external::MockedExternal;
use crate::logic::{ExecutionResultState, External, VMContext};
use crate::{EXPORT_PREFIX, MEMORY_EXPORT, REMAINING_GAS_EXPORT};
use core::mem::transmute;
use near_parameters::RuntimeFeesConfig;
use near_parameters::vm::{Config, LimitConfig, VMKind};
use near_primitives_core::gas::Gas;
use near_primitives_core::types::Balance;
use std::rc::Rc;
use std::sync::Arc;
use wasmtime::{
    Engine, Extern, Inlining, InstanceAllocationStrategy, InstancePre, Linker, Module,
    ModuleExport, OptLevel, PoolingAllocationConfig, RegallocAlgorithm, Store, Strategy,
    WasmBacktraceDetails,
};

/// The gas metering strategies to compare.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GasMeteringStrategy {
    /// Winch + finite-wasm — production on x86_64.
    WinchFiniteWasm,
    /// Cranelift + finite-wasm — same instrumentation, slower compiler.
    CraneliftFiniteWasm,
    /// Cranelift + wasmtime fuel — no finite-wasm instrumentation; wasmtime
    /// instruments the compiled code itself. Incompatible with Winch.
    CraneliftFuel,
    /// Winch, no gas or cycle accounting — baseline with zero metering overhead.
    WinchNoGas,
    /// Cranelift, no gas or cycle accounting — baseline with zero metering overhead.
    CraneliftNoGas,
    /// Winch + finite-wasm with the old inline gas instrumentation (~13
    /// instructions per block boundary, pre-`f6e40fe391`).
    WinchInlineGas,
    /// Cranelift + finite-wasm with the old inline gas instrumentation.
    CraneliftInlineGas,
    /// Cranelift + finite-wasm with Wasmtime's function inlining enabled.
    /// Inlining is disabled in production; this measures its potential benefit.
    CraneliftFiniteWasmInlining,
    /// Winch + host-function gas: every block boundary calls `internal.finite_wasm_gas`
    /// directly. No `remaining_gas` global or module-defined wrapper needed.
    WinchHostGas,
    /// Cranelift + host-function gas: same as `WinchHostGas` but compiled with Cranelift.
    CraneliftHostGas,
    /// Winch + inline subtract-first: one `global.get` per check instead of two,
    /// using a wasm local as scratch register. Saves one memory load vs `WinchInlineGas`.
    WinchInlineSubcheck,
    /// Cranelift + inline subtract-first: same as `WinchInlineSubcheck`.
    CraneliftInlineSubcheck,
    /// Winch + local gas counter: gas kept in a wasm local for the entire function;
    /// synced with the global only at host-call boundaries. Zero global accesses in
    /// the hot path.
    WinchLocalGas,
    /// Cranelift + local gas counter: same as `WinchLocalGas`.
    CraneliftLocalGas,
}

impl GasMeteringStrategy {
    pub fn name(self) -> &'static str {
        match self {
            Self::WinchFiniteWasm => "winch-fw",
            Self::CraneliftFiniteWasm => "cranelift-fw",
            Self::CraneliftFuel => "cranelift-fuel",
            Self::WinchNoGas => "winch-ng",
            Self::CraneliftNoGas => "cranelift-ng",
            Self::WinchInlineGas => "winch-fw-inline",
            Self::CraneliftInlineGas => "cranelift-fw-inline",
            Self::CraneliftFiniteWasmInlining => "cranelift-fw-wt-inlined",
            Self::WinchHostGas => "winch-fw-host",
            Self::CraneliftHostGas => "cranelift-fw-host",
            Self::WinchInlineSubcheck => "winch-fw-subcheck",
            Self::CraneliftInlineSubcheck => "cranelift-fw-subcheck",
            Self::WinchLocalGas => "winch-fw-local",
            Self::CraneliftLocalGas => "cranelift-fw-local",
        }
    }

    pub fn all() -> &'static [GasMeteringStrategy] {
        &[
            // Winch variants — current production first, then alternatives, then no-gas baseline.
            GasMeteringStrategy::WinchInlineGas,
            GasMeteringStrategy::WinchInlineSubcheck,
            GasMeteringStrategy::WinchLocalGas,
            GasMeteringStrategy::WinchFiniteWasm,
            GasMeteringStrategy::WinchHostGas,
            GasMeteringStrategy::WinchNoGas,
            // Cranelift variants — same grouping order.
            GasMeteringStrategy::CraneliftInlineGas,
            GasMeteringStrategy::CraneliftInlineSubcheck,
            GasMeteringStrategy::CraneliftLocalGas,
            GasMeteringStrategy::CraneliftFiniteWasm,
            GasMeteringStrategy::CraneliftHostGas,
            GasMeteringStrategy::CraneliftFiniteWasmInlining,
            GasMeteringStrategy::CraneliftNoGas,
            GasMeteringStrategy::CraneliftFuel,
        ]
    }

    fn use_fuel(self) -> bool {
        matches!(self, GasMeteringStrategy::CraneliftFuel)
    }

    fn skip_gas_instrumentation(self) -> bool {
        matches!(
            self,
            GasMeteringStrategy::CraneliftFuel
                | GasMeteringStrategy::WinchNoGas
                | GasMeteringStrategy::CraneliftNoGas
        )
    }

    fn uses_inline_gas(self) -> bool {
        matches!(
            self,
            GasMeteringStrategy::WinchInlineGas | GasMeteringStrategy::CraneliftInlineGas
        )
    }

    fn uses_inline_subcheck(self) -> bool {
        matches!(
            self,
            GasMeteringStrategy::WinchInlineSubcheck | GasMeteringStrategy::CraneliftInlineSubcheck
        )
    }

    fn uses_local_gas(self) -> bool {
        matches!(self, GasMeteringStrategy::WinchLocalGas | GasMeteringStrategy::CraneliftLocalGas)
    }

    fn wasmtime_strategy(self) -> Strategy {
        match self {
            GasMeteringStrategy::WinchFiniteWasm
            | GasMeteringStrategy::WinchNoGas
            | GasMeteringStrategy::WinchInlineGas
            | GasMeteringStrategy::WinchHostGas
            | GasMeteringStrategy::WinchInlineSubcheck
            | GasMeteringStrategy::WinchLocalGas => Strategy::Winch,
            GasMeteringStrategy::CraneliftFiniteWasm
            | GasMeteringStrategy::CraneliftFuel
            | GasMeteringStrategy::CraneliftNoGas
            | GasMeteringStrategy::CraneliftInlineGas
            | GasMeteringStrategy::CraneliftFiniteWasmInlining
            | GasMeteringStrategy::CraneliftHostGas
            | GasMeteringStrategy::CraneliftInlineSubcheck
            | GasMeteringStrategy::CraneliftLocalGas => Strategy::Cranelift,
        }
    }

    fn uses_wasmtime_inlining(self) -> bool {
        matches!(self, GasMeteringStrategy::CraneliftFiniteWasmInlining)
    }

    fn uses_host_gas(self) -> bool {
        matches!(self, GasMeteringStrategy::WinchHostGas | GasMeteringStrategy::CraneliftHostGas)
    }
}

/// Metrics collected during one execution of [`BenchModule::run_once`] and variants.
pub struct ExecMetrics {
    /// NEAR gas burned (raw units; divide by `1e12` for TGas).
    pub gas_burnt: u64,
    /// Wasmtime fuel consumed per call. Only `Some` for `CraneliftFuel`.
    pub fuel_used: Option<u64>,
}

/// A wasmtime engine configured for a specific gas metering strategy.
pub struct BenchEngine {
    engine: Engine,
    strategy: GasMeteringStrategy,
    near_config: Arc<Config>,
}

/// A compiled + linked module ready to instantiate repeatedly.
pub struct BenchModule {
    pre: InstancePre<Ctx>,
    remaining_gas: Option<ModuleExport>,
    memory: ModuleExport,
    near_config: Arc<Config>,
    use_fuel: bool,
    /// `current_account_id` used in the mock VMContext. Defaults to `"test.near"`.
    pub current_account_id: String,
}

impl BenchEngine {
    /// Build an engine configured for the given strategy, starting from the
    /// production NEAR config as a base for memory/table limits.
    pub fn new(strategy: GasMeteringStrategy, near_config: Arc<Config>) -> Self {
        let engine = build_engine(strategy, &near_config);
        Self { engine, strategy, near_config }
    }

    pub fn strategy(&self) -> GasMeteringStrategy {
        self.strategy
    }

    pub fn is_metered(&self) -> bool {
        !self.strategy.skip_gas_instrumentation()
    }

    /// Run the finite-wasm preparation pass (validation + gas instrumentation).
    /// For `CraneliftFuel`, gas instrumentation is skipped.
    /// For `*InlineGas` strategies, uses the old inline instrumentation approach.
    pub fn prepare_wasm(&self, raw_wasm: &[u8]) -> Result<Vec<u8>, String> {
        let mut cfg = (*self.near_config).clone();
        cfg.skip_gas_instrumentation = self.strategy.skip_gas_instrumentation();
        let cfg = Arc::new(cfg);
        if self.strategy.uses_inline_gas() {
            crate::prepare::prepare_contract_inline_gas(raw_wasm, &cfg, VMKind::Wasmtime)
        } else if self.strategy.uses_host_gas() {
            crate::prepare::prepare_contract_host_gas(raw_wasm, &cfg, VMKind::Wasmtime)
        } else if self.strategy.uses_inline_subcheck() {
            crate::prepare::prepare_contract_inline_subcheck(raw_wasm, &cfg, VMKind::Wasmtime)
        } else if self.strategy.uses_local_gas() {
            crate::prepare::prepare_contract_local_gas(raw_wasm, &cfg, VMKind::Wasmtime)
        } else {
            crate::prepare::prepare_contract(raw_wasm, &cfg, VMKind::Wasmtime)
        }
        .map_err(|e| format!("{e:?}"))
    }

    /// Compile prepared wasm to a serialized native artifact.
    pub fn compile(&self, prepared_wasm: &[u8]) -> Result<Vec<u8>, String> {
        self.engine.precompile_module(prepared_wasm).map_err(|e| e.to_string())
    }

    /// Deserialize a compiled artifact and build an `InstancePre` with host
    /// functions linked. Call this once per contract; use `BenchModule::run_once`
    /// for the per-iteration measurement.
    pub fn link_module(&self, compiled: &[u8]) -> Result<BenchModule, String> {
        // SAFETY: compiled bytes were produced by this same engine.
        let module =
            unsafe { Module::deserialize(&self.engine, compiled) }.map_err(|e| e.to_string())?;

        let memory = module
            .get_export_index(MEMORY_EXPORT)
            .ok_or_else(|| "memory export missing".to_string())?;
        let remaining_gas = module.get_export_index(REMAINING_GAS_EXPORT);

        let mut linker = Linker::new(&self.engine);
        link(&mut linker, &self.near_config);

        let pre = linker.instantiate_pre(&module).map_err(|e| e.to_string())?;

        Ok(BenchModule {
            pre,
            remaining_gas,
            memory,
            near_config: Arc::clone(&self.near_config),
            use_fuel: self.strategy.use_fuel(),
            current_account_id: "test.near".to_string(),
        })
    }
}

impl BenchModule {
    /// Execute one iteration: instantiate + call `method` with `args`.
    ///
    /// Uses a fresh `MockedExternal` (all storage reads return empty) and a
    /// generous gas budget so the contract runs to completion or traps due to
    /// logic, not gas exhaustion.
    ///
    /// Returns `Ok(ExecMetrics)` if the contract completed (or aborted cleanly);
    /// returns `Err` if there was a fatal VM-level error.
    pub fn run_once(
        &self,
        method: &str,
        args: &[u8],
        fees: Arc<RuntimeFeesConfig>,
    ) -> Result<ExecMetrics, String> {
        let mut ext = MockedExternal::new();
        self.run_once_with_ext(method, args, fees, &mut ext)
    }

    /// Run `method` on a fresh empty external and return all storage writes
    /// it produced. Useful for calling an init/constructor method to obtain
    /// the STATE key without fetching it from the chain.
    pub fn run_capturing_state(
        &self,
        method: &str,
        args: &[u8],
        fees: Arc<RuntimeFeesConfig>,
    ) -> Result<StateSnapshot, String> {
        let mut ext = MockedExternal::new();
        self.run_once_with_ext(method, args, fees, &mut ext)?;
        Ok(ext.fake_trie.clone())
    }

    /// Like `run_once` but pre-populates the mock external with `state` before
    /// instantiation. Use this when the contract requires on-chain storage to
    /// have been initialized.
    ///
    /// Each call clones `state` into a fresh `MockedExternal`, so writes during
    /// execution don't accumulate across iterations.
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

    /// Run a sequence of calls on a shared `MockedExternal`, each with its own
    /// context (predecessor, current account, attached deposit). Returns all
    /// storage written during the sequence. Use this to initialise a contract
    /// with multiple calls (e.g. `new` + `add_public_key` + `ft_on_transfer`).
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
            // metrics discarded; init calls are not timed
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
        // SAFETY: ext and context outlive the store and are not moved while the
        // store exists. The transmutes extend lifetimes only for the duration of
        // this stack frame.
        let ext_ref = unsafe { transmute::<&mut dyn External, &'static mut dyn External>(ext) };
        let ctx_ref = unsafe { transmute::<&VMContext, &'static VMContext>(&context) };

        let gas_counter = context.make_gas_counter(&self.near_config);
        let result_state =
            ExecutionResultState::new(&context, gas_counter, Arc::clone(&self.near_config));

        let ctx = Ctx::new(ext_ref, ctx_ref, fees, result_state, self.memory);
        let mut store = Store::new(self.pre.module().engine(), ctx);
        store.limiter(|ctx| &mut ctx.limits);

        if self.use_fuel {
            store.set_fuel(u64::MAX / 2).map_err(|e| e.to_string())?;
        }

        let instance = self.pre.instantiate(&mut store).map_err(|e| e.to_string())?;

        if let Some(gas_export) = &self.remaining_gas {
            let Some(Extern::Global(global)) = instance.get_module_export(&mut store, gas_export)
            else {
                return Err("gas global export missing on instance".to_string());
            };
            store.call_hook(move |mut store, hook| {
                use wasmtime::{CallHook, Val};
                match hook {
                    CallHook::CallingHost | CallHook::ReturningFromWasm => {
                        let Val::I64(remaining_gas) = global.get(&mut store) else {
                            panic!("gas global export is not i64");
                        };
                        let ctx = store.data_mut();
                        let burned = ctx
                            .result_state
                            .gas_counter
                            .remaining_gas()
                            .saturating_sub(Gas::from_gas(remaining_gas as _));
                        if burned.as_gas() > 0 {
                            ctx.result_state.gas_counter.burn_gas(burned)?;
                        }
                    }
                    CallHook::ReturningFromHost | CallHook::CallingWasm => {
                        let remaining = store.data().result_state.gas_counter.remaining_gas();
                        global
                            .set(&mut store, Val::I64(remaining.as_gas() as _))
                            .expect("failed to set gas global");
                    }
                }
                Ok(())
            });
        }

        let export_name = format!("{EXPORT_PREFIX}{method}");
        let Some(func) = instance.get_func(&mut store, &export_name) else {
            return Err(format!("method not found: {method}"));
        };

        let gas_before = store.data().result_state.gas_counter.burnt_gas().as_gas();

        if let Err(e) = func.call(&mut store, &[], &mut []) {
            if let Some(container) = e.downcast_ref::<ErrorContainer>() {
                if let Some(logic_err) = container.take() {
                    return Err(format!("{logic_err:?}"));
                }
            }
            return Err(format!("wasm trap: {e:#}"));
        }
        let gas_after = store.data().result_state.gas_counter.burnt_gas().as_gas();
        let gas_burnt = gas_after - gas_before;
        let fuel_used = if self.use_fuel {
            let remaining = store.get_fuel().unwrap_or(0);
            Some((u64::MAX / 2).saturating_sub(remaining))
        } else {
            None
        };
        Ok(ExecMetrics { gas_burnt, fuel_used })
    }
}

/// Decode base64-encoded method args as they appear in NEAR RPC responses.
/// Contracts receive the decoded bytes via `env::input()`.
pub fn decode_args_b64(b64: &[u8]) -> Result<Vec<u8>, String> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|e| format!("base64 decode error: {e}"))
}

/// A snapshot of contract storage as it exists on-chain. Keys and values are
/// raw trie key/value bytes (not base64-encoded).
pub type StateSnapshot = std::collections::HashMap<Vec<u8>, Vec<u8>>;

/// Fetch storage state for `account_id` from a NEAR RPC node.
///
/// `rpc_url` is typically `"https://rpc.mainnet.near.org"` for mainnet or
/// `"https://rpc.testnet.near.org"` for testnet.
///
/// `prefix` filters which keys are fetched — only keys that start with
/// `prefix` are returned. Use `b""` to fetch all keys, `b"STATE"` to fetch
/// only the default `#[near_bindgen]` contract state key.
///
/// State is fetched in pages of up to 50 000 entries.
pub fn fetch_contract_state(
    account_id: &str,
    rpc_url: &str,
    prefix: &[u8],
) -> Result<StateSnapshot, String> {
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD;

    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .map_err(|e| e.to_string())?;

    let mut state: StateSnapshot = StateSnapshot::new();
    let filter_b64 = b64.encode(prefix);

    let prefix_desc =
        if prefix.is_empty() { "all keys".to_string() } else { format!("prefix {:?}", prefix) };
    eprintln!("fetching state for {account_id} ({prefix_desc}) from {rpc_url} …");

    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": "bench",
        "method": "query",
        "params": {
            "request_type": "view_state",
            "finality": "final",
            "account_id": account_id,
            "prefix_base64": filter_b64,
            "limit": 50_000u32,
        }
    });

    let body_str = body.to_string();
    let resp_text = client
        .post(rpc_url)
        .header("Content-Type", "application/json")
        .body(body_str)
        .send()
        .map_err(|e| format!("RPC request failed: {e}"))?
        .text()
        .map_err(|e| format!("RPC response read error: {e}"))?;
    let resp: serde_json::Value =
        serde_json::from_str(&resp_text).map_err(|e| format!("RPC response parse error: {e}"))?;

    if let Some(err) = resp.get("error") {
        return Err(format!("RPC error: {err}"));
    }

    let values = resp["result"]["values"]
        .as_array()
        .ok_or("unexpected RPC response shape: missing result.values")?;

    let count = values.len();
    for entry in values {
        let key_b64 = entry["key"].as_str().ok_or("missing key")?;
        let val_b64 = entry["value"].as_str().ok_or("missing value")?;
        let key = b64.decode(key_b64).map_err(|e| format!("key decode: {e}"))?;
        let val = b64.decode(val_b64).map_err(|e| format!("val decode: {e}"))?;
        state.insert(key, val);
    }

    eprintln!("  fetched {count} entries (total {})", state.len());


    eprintln!("done — {} storage entries", state.len());
    Ok(state)
}

/// Like `fetch_contract_state` but reads from a local binary cache when
/// available, writing to it after a fresh RPC fetch.
///
/// `cache_dir` is created automatically if it doesn't exist. The cache
/// filename encodes both `account_id` and `prefix` so different prefixes
/// get separate files. Delete the file to force a re-fetch.
pub fn fetch_or_load_state(
    account_id: &str,
    rpc_url: &str,
    cache_dir: &std::path::Path,
    prefix: &[u8],
) -> Result<StateSnapshot, String> {
    let suffix =
        if prefix.is_empty() { String::new() } else { format!("_{}", hex::encode(prefix)) };
    let path = cache_dir.join(format!("{account_id}{suffix}.bin"));

    if path.exists() {
        eprintln!("loading state for {account_id} from {} …", path.display());
        let state = load_state_cache(&path)?;
        eprintln!("loaded {} entries", state.len());
        return Ok(state);
    }

    let state = fetch_contract_state(account_id, rpc_url, prefix)?;

    std::fs::create_dir_all(cache_dir).map_err(|e| format!("create cache dir: {e}"))?;
    save_state_cache(&path, &state)?;
    eprintln!("saved state cache to {}", path.display());

    Ok(state)
}

// Cache file format:
//   8 bytes  magic "NEARST01"
//   4 bytes  entry count (u32 LE)
//   per entry:
//     4 bytes  key length (u32 LE)
//     N bytes  key
//     4 bytes  value length (u32 LE)
//     M bytes  value
const CACHE_MAGIC: &[u8; 8] = b"NEARST01";

fn save_state_cache(path: &std::path::Path, state: &StateSnapshot) -> Result<(), String> {
    use std::io::Write;
    let mut w = std::io::BufWriter::new(std::fs::File::create(path).map_err(|e| e.to_string())?);
    w.write_all(CACHE_MAGIC).map_err(|e| e.to_string())?;
    let count = state.len() as u32;
    w.write_all(&count.to_le_bytes()).map_err(|e| e.to_string())?;
    for (k, v) in state {
        w.write_all(&(k.len() as u32).to_le_bytes()).map_err(|e| e.to_string())?;
        w.write_all(k).map_err(|e| e.to_string())?;
        w.write_all(&(v.len() as u32).to_le_bytes()).map_err(|e| e.to_string())?;
        w.write_all(v).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn load_state_cache(path: &std::path::Path) -> Result<StateSnapshot, String> {
    use std::io::Read;
    let mut r = std::io::BufReader::new(std::fs::File::open(path).map_err(|e| e.to_string())?);

    let mut magic = [0u8; 8];
    r.read_exact(&mut magic).map_err(|e| e.to_string())?;
    if &magic != CACHE_MAGIC {
        return Err(format!("bad cache magic in {}", path.display()));
    }

    let mut count_buf = [0u8; 4];
    r.read_exact(&mut count_buf).map_err(|e| e.to_string())?;
    let count = u32::from_le_bytes(count_buf) as usize;

    let mut state = StateSnapshot::with_capacity(count);
    let mut len_buf = [0u8; 4];
    for _ in 0..count {
        r.read_exact(&mut len_buf).map_err(|e| e.to_string())?;
        let klen = u32::from_le_bytes(len_buf) as usize;
        let mut key = vec![0u8; klen];
        r.read_exact(&mut key).map_err(|e| e.to_string())?;

        r.read_exact(&mut len_buf).map_err(|e| e.to_string())?;
        let vlen = u32::from_le_bytes(len_buf) as usize;
        let mut val = vec![0u8; vlen];
        r.read_exact(&mut val).map_err(|e| e.to_string())?;

        state.insert(key, val);
    }
    Ok(state)
}

fn build_engine(strategy: GasMeteringStrategy, near_config: &Config) -> Engine {
    let LimitConfig {
        max_memory_pages,
        max_tables_per_contract,
        max_elements_per_contract_table,
        ..
    } = near_config.limit_config;

    let max_memory_size = guest_memory_size(max_memory_pages).unwrap_or(usize::MAX);
    let max_tables_per_contract = max_tables_per_contract.unwrap_or(DEFAULT_MAX_TABLES_PER_MODULE);
    let max_elements_per_contract_table =
        max_elements_per_contract_table.unwrap_or(DEFAULT_MAX_ELEMENTS_PER_TABLE);
    let max_tables = MAX_CONCURRENCY.saturating_mul(max_tables_per_contract);

    let mut pooling_config = PoolingAllocationConfig::default();
    pooling_config
        .decommit_batch_size(DECOMMIT_BATCH_SIZE)
        .max_memory_size(max_memory_size)
        .table_elements(max_elements_per_contract_table)
        .total_component_instances(0)
        .total_core_instances(MAX_CONCURRENCY)
        .total_memories(MAX_CONCURRENCY)
        .total_tables(max_tables)
        .max_memories_per_module(1)
        .max_tables_per_module(max_tables_per_contract)
        .table_keep_resident(max_elements_per_contract_table);

    let features = crate::features::WasmFeatures::new(near_config);
    let mut engine_config = wasmtime::Config::from(features);

    engine_config
        .allocation_strategy(InstanceAllocationStrategy::Pooling(pooling_config))
        .native_unwind_info(false)
        .wasm_backtrace_max_frames(None)
        .wasm_backtrace_details(WasmBacktraceDetails::Disable)
        .generate_address_map(false)
        .memory_init_cow(true)
        .max_wasm_stack(1024 * 1024 * 1024)
        .strategy(strategy.wasmtime_strategy())
        .cranelift_opt_level(OptLevel::None)
        .cranelift_regalloc_algorithm(RegallocAlgorithm::SinglePass)
        .compiler_inlining(if strategy.uses_wasmtime_inlining() {
            Inlining::Yes
        } else {
            Inlining::No
        })
        .cranelift_nan_canonicalization(true)
        .signals_based_traps(true)
        .force_memory_init_memfd(true)
        .memory_guaranteed_dense_image_size(0)
        .guard_before_linear_memory(false)
        .memory_guard_size(0)
        .memory_may_move(false)
        .memory_reservation(max_memory_size.try_into().unwrap_or(u64::MAX))
        .memory_reservation_for_growth(0)
        .wasm_wide_arithmetic(true);

    if strategy.use_fuel() {
        engine_config.consume_fuel(true);
    }

    Engine::new(&engine_config).expect("failed to construct bench engine")
}

/// One call in a multi-step init sequence.
pub struct InitCallOwned {
    pub method: String,
    pub args: Vec<u8>,
    /// `predecessor_account_id` for this call. Defaults to `"test.near"`.
    pub predecessor_id: String,
    /// `current_account_id` for this call. Defaults to `"test.near"`.
    pub current_account_id: String,
    /// Attached deposit in yoctonear (required by e.g. `assert_one_yocto`).
    pub attached_deposit_yocto: u128,
}

impl InitCallOwned {
    pub fn new(method: impl Into<String>, args: Vec<u8>) -> Self {
        Self {
            method: method.into(),
            args,
            predecessor_id: "test.near".to_string(),
            current_account_id: "test.near".to_string(),
            attached_deposit_yocto: 0,
        }
    }

    pub fn with_predecessor(mut self, id: impl Into<String>) -> Self {
        self.predecessor_id = id.into();
        self
    }

    pub fn with_current_account(mut self, id: impl Into<String>) -> Self {
        self.current_account_id = id.into();
        self
    }

    pub fn with_deposit_yocto(mut self, yocto: u128) -> Self {
        self.attached_deposit_yocto = yocto;
        self
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
