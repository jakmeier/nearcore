//! Gas metering strategy benchmark.
//!
//! Edit `CASES` (static) or `build_dyn_cases()` (dynamic) below, then:
//!
//!   cargo build -p near-vm-runner --bin gas-metering-bench \
//!       --features bench_utils --profile=release
//!   ./target/release/gas-metering-bench [--json-output]
//!
//! Compares Winch+finite-wasm, Cranelift+finite-wasm, and Cranelift+fuel
//! for each listed contract × method combination.

use near_parameters::{ActionCosts, ExtCosts, RuntimeConfigStore};
use near_primitives_core::version::PROTOCOL_VERSION;
use near_vm_runner::bench_utils::{
    BenchEngine, BenchModule, ExecMetrics, GasMeteringStrategy, InitCallOwned, StateSnapshot,
};
#[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
use near_vm_runner::bench_utils::{NearBenchEngine, NearBenchModule};
use std::sync::Arc;
use std::time::Instant;

struct TestCase {
    /// File path to the wasm binary. Also used as the display name.
    path: String,
    /// If `Some`, use these bytes directly instead of reading `path` from disk.
    raw_wasm: Option<Vec<u8>>,
    method: String,
    /// Method args computed at runtime.
    args: Vec<u8>,
    /// `current_account_id` in the mock VMContext.
    current_account_id: String,
    /// Sequence of calls that build the contract state before benchmarking.
    init_calls: Vec<InitCallOwned>,
    warmup: u32,
    iters: u32,
}

/// Build a WASM module with a single exported `main` function that contains
/// `num_blocks` sequential `if` blocks. Every block condition is `i32.const 1`
/// (always true), so all branches execute on each call.
///
/// After gas instrumentation the function has one metering instruction per
/// block boundary, making this a direct stress test for gas-metering throughput.
fn make_many_blocks_wasm(num_blocks: u32) -> Vec<u8> {
    use wasm_encoder::{
        BlockType, CodeSection, ExportKind, ExportSection, Function, FunctionSection, Instruction,
        Module, TypeSection,
    };
    let mut module = Module::new();
    let mut types = TypeSection::new();
    types.ty().function([], []); // () -> ()
    module.section(&types);
    let mut functions = FunctionSection::new();
    functions.function(0);
    module.section(&functions);
    let mut exports = ExportSection::new();
    exports.export("main", ExportKind::Func, 0);
    module.section(&exports);
    let mut code = CodeSection::new();
    let mut f = Function::new([]);
    for _ in 0..num_blocks {
        f.instruction(&Instruction::I32Const(1));
        f.instruction(&Instruction::If(BlockType::Empty));
        f.instruction(&Instruction::Nop);
        f.instruction(&Instruction::End);
    }
    f.instruction(&Instruction::End);
    code.function(&f);
    module.section(&code);
    module.finish()
}

fn build_cases() -> Vec<TestCase> {
    let mut cases = Vec::new();

    // intents.near — 3 NEP-413 signed intents, full execution including
    // signature verification, balance checks, and token_diff + transfer.
    let (init_calls, args) = build_intents_benchmark();
    cases.push(TestCase {
        path: "benched_wasms/intents.near.wasm".to_string(),
        raw_wasm: None,
        method: "execute_intents".to_string(),
        args,
        current_account_id: "intents.near".to_string(),
        init_calls,
        warmup: 3,
        iters: 100,
    });

    // token.sweat — 100-entry record_batch (steps → token minting with oracle fee).
    // "test.near" is registered as the oracle so the default predecessor passes the auth check.
    let (init_calls, args) = build_sweat_benchmark();
    cases.push(TestCase {
        path: "benched_wasms/token.sweat.wasm".to_string(),
        raw_wasm: None,
        method: "record_batch".to_string(),
        args,
        current_account_id: "token.sweat".to_string(),
        init_calls,
        warmup: 3,
        iters: 100,
    });

    // Synthetic contract: many sequential if-blocks, each with condition
    // `i32.const 1` so all branches execute on every call. Stresses gas
    // metering throughput — every block boundary gets a metering instruction.
    // Production limit is max_blocks_per_function = 5000. Each if-block is one
    // block; the function body is another. 4000 if-blocks → 4001 total.
    const NUM_BLOCKS: u32 = 4_000;
    cases.push(TestCase {
        path: format!("many-blocks-{NUM_BLOCKS}.wasm"),
        raw_wasm: Some(make_many_blocks_wasm(NUM_BLOCKS)),
        method: "main".to_string(),
        args: vec![],
        current_account_id: "test.near".to_string(),
        init_calls: vec![],
        warmup: 5,
        iters: 200,
    });

    cases
}

/// Init sequence and args for a 100-entry `record_batch` call on token.sweat.
///
/// The oracle is registered as `"test.near"` because the benchmark's `run_once`
/// always uses `"test.near"` as `predecessor_account_id`. The contract's custom
/// `internal_deposit` uses `unwrap_or_default()` so no explicit `storage_deposit`
/// is needed before the batch.
fn build_sweat_benchmark() -> (Vec<InitCallOwned>, Vec<u8>) {
    const ACCOUNT: &str = "token.sweat";
    const ORACLE: &str = "test.near";
    const BATCH_SIZE: usize = 100;

    let init_calls = vec![
        InitCallOwned::new("new", br#"{"postfix":null}"#.to_vec())
            .with_predecessor(ACCOUNT)
            .with_current_account(ACCOUNT),
        InitCallOwned::new("add_oracle", format!(r#"{{"account_id":"{ORACLE}"}}"#).into_bytes())
            .with_predecessor(ACCOUNT)
            .with_current_account(ACCOUNT),
    ];

    let entries: Vec<String> =
        (0..BATCH_SIZE).map(|i| format!(r#"["user{i}.near",10000]"#)).collect();
    let args = format!(r#"{{"steps_batch":[{}]}}"#, entries.join(",")).into_bytes();

    (init_calls, args)
}

const INTENTS_ACCOUNT: &str = "intents.near";
/// Fake multi-token contract whose tokens we use inside the benchmark.
const TOKEN_CONTRACT: &str = "mtt.near";
const TOKEN_A: &str = "nep245:mtt.near:token_a";
const TOKEN_B: &str = "nep245:mtt.near:token_b";

/// Compute the bytes that are SHA-256-hashed to produce the NEP-413 signing
/// hash: `SHA256(BORSH((2^31 + 413, Nep413Payload)))`.
///
/// Borsh layout: u32-LE tag | u32-LE-prefixed message bytes | 32-byte nonce
///               | u32-LE-prefixed recipient bytes | 0x00 (None callback_url)
fn nep413_prehash(message: &str, nonce: &[u8; 32], recipient: &str) -> Vec<u8> {
    const TAG: u32 = (1u32 << 31) + 413; // NEP-461 offchain tag for NEP-413
    let mut buf = Vec::new();
    buf.extend_from_slice(&TAG.to_le_bytes());
    let msg = message.as_bytes();
    buf.extend_from_slice(&(msg.len() as u32).to_le_bytes());
    buf.extend_from_slice(msg);
    buf.extend_from_slice(nonce);
    let rec = recipient.as_bytes();
    buf.extend_from_slice(&(rec.len() as u32).to_le_bytes());
    buf.extend_from_slice(rec);
    buf.push(0u8); // Option<String>::None
    buf
}

fn nep413_hash(message: &str, nonce: &[u8; 32], recipient: &str) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(nep413_prehash(message, nonce, recipient)).into()
}

/// Build the init-call sequence and `execute_intents` args for a self-contained
/// 3-signer benchmark modelled on a real mainnet transaction:
///
///  Signer 1 (solver1.near): one token_diff  — sells token_a, buys token_b
///  Signer 2 (solver2.near): one token_diff  — sells token_b, buys token_a
///  Signer 3 (solver3.near): token_diff + transfer  — arb/relay role
///
/// Diffs balance to zero: each token's net flow across all signers is 0.
///
/// Init sequence:
///   1. `new(config)`             — predecessor = intents.near
///   2. `add_public_key(pk)`×3   — predecessor = the signer account, deposit = 1 yocto
///   3. `mt_on_transfer(…)`×3    — predecessor = token contract, funds each account
fn build_intents_benchmark() -> (Vec<InitCallOwned>, Vec<u8>) {
    use ed25519_dalek::{Signer, SigningKey};
    use near_crypto::{ED25519PublicKey, KeyType, PublicKey, Signature};

    let make_keypair = |seed_byte: u8| -> (SigningKey, PublicKey) {
        let seed = [seed_byte; 32];
        // SHA-256 the seed so a low-entropy constant like [1;32] still
        // produces a uniformly-distributed ed25519 private key scalar.
        let key_bytes: [u8; 32] = {
            use sha2::{Digest, Sha256};
            Sha256::digest(seed).into()
        };
        let sk = SigningKey::from_bytes(&key_bytes);
        let pk = PublicKey::ED25519(ED25519PublicKey(sk.verifying_key().to_bytes()));
        (sk, pk)
    };

    let sign = |sk: &SigningKey, hash: &[u8; 32]| -> Signature {
        let sig_bytes = sk.sign(hash).to_bytes();
        Signature::from_parts(KeyType::ED25519, &sig_bytes).expect("valid ed25519 sig")
    };

    let (sk1, pk1) = make_keypair(1);
    let (sk2, pk2) = make_keypair(2);
    let (sk3, pk3) = make_keypair(3);

    let config_args =
        br#"{"config":{"wnear_id":"wrap.near","fees":{"fee":0,"fee_collector":"intents.near"}}}"#
            .to_vec();

    // add_public_key JSON args for each signer.
    // Format: {"public_key":"ed25519:<base58>"}
    let add_key_args = |pk: &near_crypto::PublicKey| -> Vec<u8> {
        format!(r#"{{"public_key":"{}"}}"#, pk).into_bytes()
    };

    // mt_on_transfer args: deposit `amount` of `token_id` for `owner`.
    // predecessor must be the token contract (mtt.near).
    let deposit_args = |owner: &str, token_id: &str, amount: u64| -> Vec<u8> {
        format!(
            r#"{{"sender_id":"{owner}","previous_owner_ids":["{owner}"],"token_ids":["{token_id}"],"amounts":["{amount}"],"msg":""}}"#,
        )
        .into_bytes()
    };

    let init_calls = vec![
        InitCallOwned::new("new", config_args)
            .with_current_account(INTENTS_ACCOUNT)
            .with_predecessor(INTENTS_ACCOUNT),
        InitCallOwned::new("add_public_key", add_key_args(&pk1))
            .with_current_account(INTENTS_ACCOUNT)
            .with_predecessor("solver1.near")
            .with_deposit_yocto(1),
        InitCallOwned::new("add_public_key", add_key_args(&pk2))
            .with_current_account(INTENTS_ACCOUNT)
            .with_predecessor("solver2.near")
            .with_deposit_yocto(1),
        InitCallOwned::new("add_public_key", add_key_args(&pk3))
            .with_current_account(INTENTS_ACCOUNT)
            .with_predecessor("solver3.near")
            .with_deposit_yocto(1),
        InitCallOwned::new("mt_on_transfer", deposit_args("solver1.near", "token_a", 1000))
            .with_current_account(INTENTS_ACCOUNT)
            .with_predecessor(TOKEN_CONTRACT),
        InitCallOwned::new("mt_on_transfer", deposit_args("solver2.near", "token_b", 500))
            .with_current_account(INTENTS_ACCOUNT)
            .with_predecessor(TOKEN_CONTRACT),
        InitCallOwned::new("mt_on_transfer", deposit_args("solver3.near", "token_b", 500))
            .with_current_account(INTENTS_ACCOUNT)
            .with_predecessor(TOKEN_CONTRACT),
    ];

    let nonce1 = [1u8; 32];
    let nonce2 = [2u8; 32];
    let nonce3 = [3u8; 32];

    // Deadline far enough in the future that block_timestamp=42ns never hits it.
    const DEADLINE: &str = "2099-12-31T23:59:59.000Z";

    let msg1 = format!(
        r#"{{"signer_id":"solver1.near","deadline":"{DEADLINE}","intents":[{{"intent":"token_diff","diff":{{"{TOKEN_A}":"-1000","{TOKEN_B}":"1000"}}}}]}}"#,
    );
    let msg2 = format!(
        r#"{{"signer_id":"solver2.near","deadline":"{DEADLINE}","intents":[{{"intent":"token_diff","diff":{{"{TOKEN_B}":"-500","{TOKEN_A}":"500"}}}}]}}"#,
    );
    let msg3 = format!(
        r#"{{"signer_id":"solver3.near","deadline":"{DEADLINE}","intents":[{{"intent":"token_diff","diff":{{"{TOKEN_B}":"-500","{TOKEN_A}":"500"}}}},{{"intent":"transfer","tokens":{{"{TOKEN_A}":"1"}},"receiver_id":"receiver.near"}}]}}"#,
    );

    let hash1 = nep413_hash(&msg1, &nonce1, INTENTS_ACCOUNT);
    let hash2 = nep413_hash(&msg2, &nonce2, INTENTS_ACCOUNT);
    let hash3 = nep413_hash(&msg3, &nonce3, INTENTS_ACCOUNT);
    let sig1 = sign(&sk1, &hash1);
    let sig2 = sign(&sk2, &hash2);
    let sig3 = sign(&sk3, &hash3);

    // Nonce in JSON: base64 of the 32 raw bytes (as used on mainnet).
    use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
    let nonce1_b64 = B64.encode(nonce1);
    let nonce2_b64 = B64.encode(nonce2);
    let nonce3_b64 = B64.encode(nonce3);

    // `sig` and `pk` display as "ed25519:<base58>" via near_crypto's Display impl.
    let signed_entry = |msg: &str,
                        nonce_b64: &str,
                        sig: &dyn std::fmt::Display,
                        pk: &dyn std::fmt::Display|
     -> String {
        let msg_json = serde_json::to_string(msg).unwrap();
        format!(
            r#"{{"payload":{{"message":{msg_json},"nonce":"{nonce_b64}","recipient":"{INTENTS_ACCOUNT}"}},"standard":"nep413","signature":"{sig}","public_key":"{pk}"}}"#,
        )
    };

    let args = format!(
        r#"{{"signed":[{},{},{}]}}"#,
        signed_entry(&msg1, &nonce1_b64, &sig1, &pk1),
        signed_entry(&msg2, &nonce2_b64, &sig2, &pk2),
        signed_entry(&msg3, &nonce3_b64, &sig3, &pk3),
    )
    .into_bytes();

    (init_calls, args)
}

struct Row {
    contract: String,
    method: String,
    strategy: &'static str,
    /// finite-wasm instrumentation time in milliseconds.
    t_instrument_ms: f64,
    t_compile_ms: f64,
    t_exec_ns: f64,
    /// Protocol NEAR gas for the deploy action (instrument + compile), in TGas.
    deploy_tgas: f64,
    /// Total deploy time (instrument + compile) in ms per TGas of deploy cost.
    deploy_ms_per_tgas: f64,
    /// Average NEAR gas burned per exec call, in TGas. `None` when gas metering is off.
    exec_tgas: Option<f64>,
    /// Milliseconds per TGas of execution (latency per unit of gas). `None` when gas metering is off.
    ms_per_tgas: Option<f64>,
    /// Wasmtime fuel consumed per call (`None` for non-fuel strategies).
    fuel: Option<u64>,
    /// Milliseconds per million fuel units (`None` for non-fuel strategies).
    ms_per_mfuel: Option<f64>,
    note: String,
}

fn print_table(rows: &[Row]) {
    println!(
        "{:<28} {:<16} {:<23}  {:>9} {:>9} {:>8} {:>10}  {:>12} {:>9} {:>9}  {:>16} {:>10}  note",
        "contract",
        "method",
        "strategy",
        "instr_ms",
        "compile_ms",
        "dep_tgas",
        "dep_ms/tgas",
        "exec_ns",
        "exec_tgas",
        "ms/tgas",
        "fuel",
        "ms/Mfuel",
    );
    println!("{}", "-".repeat(176));
    for r in rows {
        let strategy_str = r.strategy.to_string();
        let exec_tgas_str = match r.exec_tgas {
            Some(v) => format!("{v:>9.3}"),
            None => format!("{:>9}", "NA"),
        };
        let ms_tgas_str = match r.ms_per_tgas {
            Some(v) => format!("{v:>9.3}"),
            None => format!("{:>9}", "NA"),
        };
        let fuel_str = match r.fuel {
            Some(f) => format!("{f:>16}"),
            None => format!("{:>16}", "-"),
        };
        let ms_mfuel_str = match r.ms_per_mfuel {
            Some(f) => format!("{f:>10.3}"),
            None => format!("{:>10}", "-"),
        };
        let note_str = r.note.clone();
        println!(
            "{:<28} {:<16} {:<23}  {:>9.2} {:>9.2} {:>8.1} {:>10.3}  {:>12.0} {} {}  {}{}  {}",
            r.contract,
            r.method,
            strategy_str,
            r.t_instrument_ms,
            r.t_compile_ms,
            r.deploy_tgas,
            r.deploy_ms_per_tgas,
            r.t_exec_ns,
            exec_tgas_str,
            ms_tgas_str,
            fuel_str,
            ms_mfuel_str,
            note_str,
        );
    }
}

fn csv_quote(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn print_csv(rows: &[Row]) {
    println!(
        "contract,method,strategy,instrument_ms,compile_ms,deploy_tgas,deploy_ms_per_tgas,exec_ns,exec_tgas,ms_per_tgas,fuel,ms_per_mfuel,note"
    );
    for r in rows {
        let exec_tgas = match r.exec_tgas {
            Some(v) => format!("{v:.3}"),
            None => String::new(),
        };
        let ms_per_tgas = match r.ms_per_tgas {
            Some(v) => format!("{v:.3}"),
            None => String::new(),
        };
        let fuel = match r.fuel {
            Some(f) => format!("{f}"),
            None => String::new(),
        };
        let ms_per_mfuel = match r.ms_per_mfuel {
            Some(v) => format!("{v:.3}"),
            None => String::new(),
        };
        println!(
            "{},{},{},{:.2},{:.2},{:.1},{:.3},{:.0},{},{},{},{},{}",
            csv_quote(&r.contract),
            csv_quote(&r.method),
            r.strategy,
            r.t_instrument_ms,
            r.t_compile_ms,
            r.deploy_tgas,
            r.deploy_ms_per_tgas,
            r.t_exec_ns,
            exec_tgas,
            ms_per_tgas,
            fuel,
            ms_per_mfuel,
            csv_quote(&r.note),
        );
    }
}

fn print_json(rows: &[Row]) {
    for r in rows {
        let fuel_json = match r.fuel {
            Some(f) => format!("{f}"),
            None => "null".to_string(),
        };
        let ms_mfuel_json = match r.ms_per_mfuel {
            Some(f) => format!("{f:.3}"),
            None => "null".to_string(),
        };
        let exec_tgas_json = match r.exec_tgas {
            Some(v) => format!("{v:.3}"),
            None => "null".to_string(),
        };
        let ms_tgas_json = match r.ms_per_tgas {
            Some(v) => format!("{v:.3}"),
            None => "null".to_string(),
        };
        println!(
            r#"{{"contract":{:?},"method":{:?},"strategy":{:?},"instrument_ms":{:.2},"compile_ms":{:.2},"deploy_tgas":{:.1},"deploy_ms_per_tgas":{:.3},"exec_ns":{:.0},"exec_tgas":{},"ms_per_tgas":{},"fuel":{},"ms_per_mfuel":{},"note":{:?}}}"#,
            r.contract,
            r.method,
            r.strategy,
            r.t_instrument_ms,
            r.t_compile_ms,
            r.deploy_tgas,
            r.deploy_ms_per_tgas,
            r.t_exec_ns,
            exec_tgas_json,
            ms_tgas_json,
            fuel_json,
            ms_mfuel_json,
            r.note,
        );
    }
}

enum AnyEngine {
    Wasmtime(BenchEngine),
    #[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
    NearVm(NearBenchEngine),
}

enum AnyModule {
    Wasmtime(BenchModule),
    #[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
    NearVm(NearBenchModule),
}

impl AnyEngine {
    fn strategy_name(&self) -> &'static str {
        match self {
            Self::Wasmtime(e) => e.strategy().name(),
            #[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
            Self::NearVm(e) => e.strategy_name(),
        }
    }

    fn prepare_wasm(&self, raw: &[u8]) -> Result<Vec<u8>, String> {
        match self {
            Self::Wasmtime(e) => e.prepare_wasm(raw),
            #[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
            Self::NearVm(e) => e.prepare_wasm(raw),
        }
    }

    fn compile(&self, prepared: &[u8]) -> Result<Vec<u8>, String> {
        match self {
            Self::Wasmtime(e) => e.compile(prepared),
            #[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
            Self::NearVm(e) => e.compile(prepared),
        }
    }

    fn link_module(&self, compiled: &[u8]) -> Result<AnyModule, String> {
        match self {
            Self::Wasmtime(e) => e.link_module(compiled).map(AnyModule::Wasmtime),
            #[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
            Self::NearVm(e) => e.link_module(compiled).map(AnyModule::NearVm),
        }
    }

    fn is_metered(&self) -> bool {
        match self {
            Self::Wasmtime(e) => e.is_metered(),
            #[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
            Self::NearVm(e) => e.is_metered(),
        }
    }
}

impl AnyModule {
    fn set_current_account_id(&mut self, id: String) {
        match self {
            Self::Wasmtime(m) => m.current_account_id = id,
            #[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
            Self::NearVm(m) => m.current_account_id = id,
        }
    }

    fn run_init_sequence_owned(
        &self,
        calls: &[InitCallOwned],
        fees: Arc<near_parameters::RuntimeFeesConfig>,
    ) -> Result<StateSnapshot, String> {
        match self {
            Self::Wasmtime(m) => m.run_init_sequence_owned(calls, fees),
            #[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
            Self::NearVm(m) => m.run_init_sequence_owned(calls, fees),
        }
    }

    fn run_once(
        &self,
        method: &str,
        args: &[u8],
        fees: Arc<near_parameters::RuntimeFeesConfig>,
    ) -> Result<ExecMetrics, String> {
        match self {
            Self::Wasmtime(m) => m.run_once(method, args, fees),
            #[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
            Self::NearVm(m) => m.run_once(method, args, fees),
        }
    }

    fn run_once_with_state(
        &self,
        method: &str,
        args: &[u8],
        fees: Arc<near_parameters::RuntimeFeesConfig>,
        state: &StateSnapshot,
    ) -> Result<ExecMetrics, String> {
        match self {
            Self::Wasmtime(m) => m.run_once_with_state(method, args, fees, state),
            #[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
            Self::NearVm(m) => m.run_once_with_state(method, args, fees, state),
        }
    }
}

fn run(
    method: &str,
    args: &[u8],
    fees: Arc<near_parameters::RuntimeFeesConfig>,
    module: &AnyModule,
    state: Option<&StateSnapshot>,
) -> Result<ExecMetrics, String> {
    match state {
        Some(s) => module.run_once_with_state(method, args, fees, s),
        None => module.run_once(method, args, fees),
    }
}

/// Run all gas-metering strategies for one contract × method combination.
///
/// `init_calls` builds the contract state before benchmarking. The resulting
/// state snapshot is cloned into each iteration so writes don't accumulate.
#[allow(clippy::too_many_arguments)]
fn bench_contract(
    path: &str,
    wasm_bytes: Option<Vec<u8>>,
    method: &str,
    args: &[u8],
    current_account_id: &str,
    init_calls: &[InitCallOwned],
    engines: &[AnyEngine],
    fees: &Arc<near_parameters::RuntimeFeesConfig>,
    warmup: u32,
    iters: u32,
    rows: &mut Vec<Row>,
) {
    let raw_wasm = if let Some(b) = wasm_bytes {
        b
    } else {
        match std::fs::read(path) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("SKIP  {path} — {e}");
                return;
            }
        }
    };
    let contract_name =
        std::path::Path::new(path).file_name().unwrap_or_default().to_string_lossy().into_owned();

    // Protocol NEAR gas charged for the deploy action (send + execute).
    let deploy_gas_base = fees.fee(ActionCosts::deploy_contract_base).execution.gas().as_gas();
    let deploy_gas_byte = fees.fee(ActionCosts::deploy_contract_byte).execution.gas().as_gas();
    let deploy_gas =
        deploy_gas_base.saturating_add(deploy_gas_byte.saturating_mul(raw_wasm.len() as u64));
    let deploy_tgas = deploy_gas as f64 / 1e12;

    let mut state: Option<StateSnapshot> = None;
    let mut state_initialized = init_calls.is_empty();

    let zeroed_row = |strategy: &'static str, note: String| Row {
        contract: contract_name.clone(),
        method: method.to_string(),
        strategy,
        t_instrument_ms: 0.0,
        t_compile_ms: 0.0,
        t_exec_ns: 0.0,
        deploy_tgas,
        deploy_ms_per_tgas: 0.0,
        exec_tgas: None,
        ms_per_tgas: None,
        fuel: None,
        ms_per_mfuel: None,
        note,
    };

    for engine in engines {
        let strategy_name = engine.strategy_name();

        let t0 = Instant::now();
        let prepared = match engine.prepare_wasm(&raw_wasm) {
            Ok(p) => p,
            Err(e) => {
                rows.push(zeroed_row(strategy_name, format!("prepare error: {e}")));
                continue;
            }
        };
        let t_instrument_ms = t0.elapsed().as_secs_f64() * 1e3;

        let t0 = Instant::now();
        let compiled = match engine.compile(&prepared) {
            Ok(c) => c,
            Err(e) => {
                rows.push(Row {
                    t_instrument_ms,
                    ..zeroed_row(strategy_name, format!("compile error: {e}"))
                });
                continue;
            }
        };
        let t_compile_ms = t0.elapsed().as_secs_f64() * 1e3;

        let mut bench_module = match engine.link_module(&compiled) {
            Ok(m) => m,
            Err(e) => {
                rows.push(Row {
                    t_instrument_ms,
                    t_compile_ms,
                    ..zeroed_row(strategy_name, format!("link error: {e}"))
                });
                continue;
            }
        };
        bench_module.set_current_account_id(current_account_id.to_string());

        if !state_initialized {
            match bench_module.run_init_sequence_owned(init_calls, Arc::clone(fees)) {
                Ok(init_state) => {
                    state.get_or_insert_with(StateSnapshot::new).extend(init_state);
                    state_initialized = true;
                }
                Err(e) => {
                    eprintln!(
                        "WARNING: init sequence failed for {contract_name}: {e}\n  \
                         Contract may not work correctly without proper state."
                    );
                    state_initialized = true;
                }
            }
        }

        if let Err(e) = run(method, args, Arc::clone(fees), &bench_module, state.as_ref()) {
            rows.push(Row {
                t_instrument_ms,
                t_compile_ms,
                ..zeroed_row(strategy_name, format!("FAILED: {e}"))
            });
            continue;
        }

        for _ in 0..warmup {
            if let Err(e) = run(method, args, Arc::clone(fees), &bench_module, state.as_ref()) {
                eprintln!("WARNING: warmup failed for {contract_name}/{method}: {e}");
                break;
            }
        }

        let t0 = Instant::now();
        let mut run_err: Option<String> = None;
        let mut completed = 0u32;
        let mut total_gas_burnt: u64 = 0;
        let mut total_fuel: u64 = 0;
        let mut has_fuel = false;
        for _ in 0..iters {
            match run(method, args, Arc::clone(fees), &bench_module, state.as_ref()) {
                Ok(metrics) => {
                    completed += 1;
                    total_gas_burnt = total_gas_burnt.saturating_add(metrics.gas_burnt);
                    if let Some(f) = metrics.fuel_used {
                        total_fuel = total_fuel.saturating_add(f);
                        has_fuel = true;
                    }
                }
                Err(e) => {
                    run_err = Some(e);
                    break;
                }
            }
        }
        let t_exec_ns =
            if completed > 0 { t0.elapsed().as_secs_f64() * 1e9 / completed as f64 } else { 0.0 };
        let exec_tgas = if engine.is_metered() && completed > 0 {
            Some((total_gas_burnt / completed as u64) as f64 / 1e12)
        } else {
            None
        };
        let ms_per_tgas = exec_tgas.filter(|&t| t > 0.0).map(|t| (t_exec_ns / 1e6) / t);
        let deploy_ms_per_tgas = (t_instrument_ms + t_compile_ms) / deploy_tgas;
        let (fuel, ms_per_mfuel) = if has_fuel && completed > 0 {
            let avg = total_fuel / completed as u64;
            // ms per million fuel units = (exec_ns / 1e6) / (avg / 1e6) = exec_ns / avg
            let ms_mf = if avg > 0 { t_exec_ns / avg as f64 } else { 0.0 };
            (Some(avg), Some(ms_mf))
        } else {
            (None, None)
        };

        rows.push(Row {
            contract: contract_name.clone(),
            method: method.to_string(),
            strategy: strategy_name,
            t_instrument_ms,
            t_compile_ms,
            t_exec_ns,
            deploy_tgas,
            deploy_ms_per_tgas,
            exec_tgas,
            ms_per_tgas,
            fuel,
            ms_per_mfuel,
            note: run_err
                .map(|e| format!("failed after {completed}/{iters} iters: {e}"))
                .unwrap_or_default(),
        });
    }
}

fn main() -> anyhow::Result<()> {
    let json_output = std::env::args().any(|a| a == "--json-output");
    let csv_output = std::env::args().any(|a| a == "--csv-output");

    let config_store = RuntimeConfigStore::new(None);
    let runtime_config = config_store.get_config(PROTOCOL_VERSION);
    let near_config = Arc::new(near_parameters::vm::Config::clone(&runtime_config.wasm_config));
    let fees = Arc::clone(&runtime_config.fees);
    let near_config = {
        let mut cfg = (*near_config).clone();
        // remove storage fees, since storage host functions don't do menaingful work in the benchmark setup
        for cost in [
            ExtCosts::storage_write_base,
            ExtCosts::storage_write_key_byte,
            ExtCosts::storage_write_value_byte,
            ExtCosts::storage_write_evicted_byte,
            ExtCosts::storage_read_base,
            ExtCosts::storage_read_key_byte,
            ExtCosts::storage_read_value_byte,
            ExtCosts::storage_remove_base,
            ExtCosts::storage_remove_key_byte,
            ExtCosts::storage_remove_ret_value_byte,
            ExtCosts::storage_has_key_base,
            ExtCosts::storage_has_key_byte,
        ] {
            cfg.ext_costs.costs[cost].gas = near_primitives_core::types::Gas::ZERO;
        }
        Arc::new(cfg)
    };

    let mut engines: Vec<AnyEngine> = GasMeteringStrategy::all()
        .iter()
        .map(|&s| AnyEngine::Wasmtime(BenchEngine::new(s, Arc::clone(&near_config))))
        .collect();
    #[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
    {
        engines.push(AnyEngine::NearVm(NearBenchEngine::with_gas(Arc::clone(&near_config))));
        engines.push(AnyEngine::NearVm(NearBenchEngine::no_gas(Arc::clone(&near_config))));
    }

    let mut rows: Vec<Row> = Vec::new();
    let cases = build_cases();
    if cases.is_empty() {
        eprintln!("No test cases defined. Edit build_cases() in gas_metering_bench.rs.");
        return Ok(());
    }
    for case in cases {
        bench_contract(
            &case.path,
            case.raw_wasm,
            &case.method,
            &case.args,
            &case.current_account_id,
            &case.init_calls,
            &engines,
            &fees,
            case.warmup,
            case.iters,
            &mut rows,
        );
    }

    if json_output {
        print_json(&rows);
    } else if csv_output {
        print_csv(&rows);
    } else {
        print_table(&rows);
    }

    Ok(())
}
