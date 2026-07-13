//! Compiler daemon subprocess entry point.
//!
//! Runs inside the child process spawned by the parent. Sets a memory limit,
//! then loops reading compilation requests and writing responses.

use super::MIN_WORKER_MEMORY_LIMIT_BYTES;
use super::protocol::{
    CompileRequest, CompileResponse, CompileStats, DaemonStartup, read_frame, write_frame,
};
use super::sandbox::{self, SandboxStatus};
use crate::wasmtime_runner::{create_compiler_engine, create_compiler_engine_with_strategy};
use std::collections::{HashMap, hash_map};
use std::time::Instant;
use wasmtime::Strategy;

/// A wasmtime [`Engine`] plus a count of compilations it has served.
///
/// The daemon caches one engine per `max_memory_pages` value so that repeated
/// requests with the same limit reuse the same engine (and its internal
/// configuration caches) instead of rebuilding it each time.
struct EngineEntry {
    engine: wasmtime::Engine,
    /// Total compilations performed with this engine, including the current
    /// request. Starts at 1 on creation.
    compiles_served: u32,
}

/// Entry point for the dedicated compiler daemon binary.
pub fn daemon_main() -> ! {
    let stdout = std::io::stdout();
    let mut writer = stdout.lock();

    let memory_limit = parse_memory_limit_arg().unwrap_or(MIN_WORKER_MEMORY_LIMIT_BYTES);
    let compiler_strategy = match parse_compiler_strategy_arg() {
        Ok(strategy) => strategy,
        Err(err) => {
            let _ = write_frame(&mut writer, &borsh::to_vec(&DaemonStartup::Err(err)).unwrap());
            std::process::exit(1);
        }
    };
    set_memory_limit(memory_limit);
    let sandbox_status = match sandbox::apply() {
        Ok(status) => status,
        Err(err) => {
            let startup = DaemonStartup::Err(err);
            let _ = write_frame(&mut writer, &borsh::to_vec(&startup).unwrap());
            std::process::exit(1);
        }
    };
    let startup = DaemonStartup::Ready;
    if write_frame(&mut writer, &borsh::to_vec(&startup).unwrap()).is_err() {
        std::process::exit(1);
    }

    let stdin = std::io::stdin();
    let mut reader = stdin.lock();
    let mut engines: HashMap<u32, EngineEntry> = HashMap::new();
    let mut compile_index_in_worker: u32 = 0;

    loop {
        let frame = match read_frame(&mut reader) {
            Ok(f) => f,
            Err(_) => std::process::exit(0),
        };
        let request: CompileRequest = match borsh::from_slice(&frame) {
            Ok(r) => r,
            Err(e) => {
                let resp = CompileResponse::Err(format!("failed to deserialize request: {e}"));
                let _ = write_frame(&mut writer, &borsh::to_vec(&resp).unwrap());
                continue;
            }
        };
        let response = handle_request(
            &mut engines,
            &mut compile_index_in_worker,
            request,
            &sandbox_status,
            compiler_strategy,
        );
        if write_frame(&mut writer, &borsh::to_vec(&response).unwrap()).is_err() {
            std::process::exit(0);
        }
    }
}

fn handle_request(
    engines: &mut HashMap<u32, EngineEntry>,
    compile_index_in_worker: &mut u32,
    request: CompileRequest,
    sandbox_status: &SandboxStatus,
    compiler_strategy: Option<Strategy>,
) -> CompileResponse {
    let index = *compile_index_in_worker;
    *compile_index_in_worker += 1;
    let pid = std::process::id();

    #[cfg(all(target_os = "linux", feature = "test_features"))]
    if request.prepared_code == super::protocol::TEST_LANDLOCK_PROBE_REQUEST {
        return match sandbox::run_probe(sandbox_status) {
            Ok(()) => CompileResponse::Ok {
                artifact: super::protocol::TEST_LANDLOCK_PROBE_RESPONSE.to_vec(),
                stats: CompileStats { pid, compile_index_in_worker: index, ..Default::default() },
            },
            Err(err) => CompileResponse::Err(err),
        };
    }
    let _ = sandbox_status;
    handle_compile(engines, pid, index, request, compiler_strategy)
}

fn handle_compile(
    engines: &mut HashMap<u32, EngineEntry>,
    pid: u32,
    compile_index_in_worker: u32,
    request: CompileRequest,
    compiler_strategy: Option<Strategy>,
) -> CompileResponse {
    #[cfg(feature = "test_features")]
    if request.prepared_code == super::protocol::TEST_ABORT_REQUEST {
        std::process::abort();
    }

    let start = Instant::now();
    let entry = match engines.entry(request.max_memory_pages) {
        hash_map::Entry::Occupied(e) => {
            let entry = e.into_mut();
            entry.compiles_served += 1;
            entry
        }
        hash_map::Entry::Vacant(e) => match compiler_strategy.map_or_else(
            || create_compiler_engine(request.max_memory_pages),
            |strategy| create_compiler_engine_with_strategy(request.max_memory_pages, strategy),
        ) {
            Ok(engine) => e.insert(EngineEntry { engine, compiles_served: 1 }),
            Err(e) => {
                return CompileResponse::Err(format!("failed to create engine: {e}"));
            }
        },
    };

    // `compiles_served` was incremented above (or set to 1 on creation), so the
    // 0-based index for this request is `compiles_served - 1`.
    let compile_index_in_engine = entry.compiles_served - 1;
    let engine_created = entry.compiles_served == 1;

    let result = entry.engine.precompile_module(&request.prepared_code);
    let compile_duration_us = start.elapsed().as_micros() as u64;

    match result {
        Ok(artifact) => CompileResponse::Ok {
            artifact,
            stats: CompileStats {
                pid,
                compile_index_in_worker,
                compile_index_in_engine,
                engine_created,
                compile_duration_us,
            },
        },
        Err(e) => CompileResponse::Err(e.to_string()),
    }
}

/// Read `--memory-limit-bytes <N>` from the command line, if present.
///
/// Allows experiments to override the default RLIMIT_AS without rebuilding.
fn parse_memory_limit_arg() -> Option<u64> {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--memory-limit-bytes" {
            if let Some(val) = args.next() {
                if let Ok(bytes) = val.parse() {
                    return Some(bytes);
                }
            }
        }
    }
    None
}

/// Parse the optional experiment-only compiler backend selection.
fn parse_compiler_strategy_arg() -> Result<Option<Strategy>, String> {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--compiler-backend" {
            let backend =
                args.next().ok_or_else(|| "missing value for --compiler-backend".to_owned())?;
            return match backend.as_str() {
                "winch" => Ok(Some(Strategy::Winch)),
                "cranelift" => Ok(Some(Strategy::Cranelift)),
                _ => Err(format!("invalid --compiler-backend: {backend}")),
            };
        }
    }
    Ok(None)
}

#[cfg(unix)]
fn set_memory_limit(limit: u64) {
    let ret = unsafe {
        let limit = libc::rlimit { rlim_cur: limit, rlim_max: limit };
        libc::setrlimit(libc::RLIMIT_AS, &limit)
    };
    if ret != 0 {
        eprintln!("warning: failed to set memory limit: {}", std::io::Error::last_os_error());
    }
}

#[cfg(not(unix))]
fn set_memory_limit(_limit: u64) {}
