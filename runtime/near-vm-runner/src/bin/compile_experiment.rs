//! CLI tool for running compiler-daemon memory experiments.
//!
//! Compiles every `.wasm` file in a directory through the out-of-process
//! compiler daemon and writes per-compilation diagnostics to a CSV file.
//!
//! ## Quick start
//!
//! ```sh
//! cargo run --bin compile-experiment -- \
//!     --wasm-dir path/to/wasm/files \
//!     --output   results.csv
//! ```
//!
//! The daemon binary is found automatically via `CARGO_BIN_EXE`. Override with
//! `--daemon-binary` if needed.
//!
//! ## Comparing fresh vs reused workers
//!
//! Run once with `--compiles-per-worker 1` (every compile in a fresh process)
//! and once with `--compiles-per-worker 0` (reuse one worker for all files).
//! Merge the CSVs and group by `compile_index_in_worker` to see the difference.

use near_vm_runner::compiler_daemon::experiment::{CompilerBackend, ExperimentConfig, run};
use std::path::PathBuf;
use std::process::ExitCode;

const USAGE: &str = "\
compile-experiment — measure compiler-daemon memory usage

USAGE:
    compile-experiment --wasm-dir <DIR> --output <CSV> [OPTIONS]

OPTIONS:
        --daemon-binary <PATH>      Path to the daemon binary
                                    [default: built near-vm-compiler-daemon]
        --wasm-dir <DIR>            Directory of .wasm files (non-recursive)
        --output <CSV>              Output CSV path (overwritten)
        --run-id <ID>               Label for this run [default: auto-timestamp]
        --compiles-per-worker <N>   Restart worker after N compiles.
                                    0 = reuse for all files [default: 0]
        --repeat <N>                Number of passes over the directory [default: 1]
        --memory-limit-bytes <B>    Worker RLIMIT_AS [default: 4 GiB]
        --rayon-num-threads <N>     Compiler Rayon worker threads [default: CPU parallelism]
        --compiler-backend <NAME>   Wasmtime backend: winch or cranelift [default: production]
    -h, --help                      Show this help
";

fn main() -> ExitCode {
    match parse_args() {
        Ok(config) => match run(&config) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("error: {e:#}");
                ExitCode::FAILURE
            }
        },
        Err(ParseError::Help) => {
            print!("{USAGE}");
            ExitCode::SUCCESS
        }
        Err(ParseError::Msg(msg)) => {
            eprintln!("{msg}");
            eprintln!();
            eprintln!("{USAGE}");
            ExitCode::FAILURE
        }
    }
}

enum ParseError {
    Help,
    Msg(String),
}

fn parse_args() -> Result<ExperimentConfig, ParseError> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    let mut daemon_binary: Option<PathBuf> = None;
    let mut wasm_dir: Option<PathBuf> = None;
    let mut output_csv: Option<PathBuf> = None;
    let mut run_id: Option<String> = None;
    let mut compiles_per_worker: usize = 0;
    let mut repeat: usize = 1;
    let mut memory_limit_bytes: Option<u64> = None;
    let mut rayon_num_threads: Option<usize> = None;
    let mut compiler_backend: Option<CompilerBackend> = None;

    let mut i = 0;
    while i < args.len() {
        let key = &args[i];
        let take_val = |i: &mut usize| -> Result<String, ParseError> {
            *i += 1;
            args.get(*i).cloned().ok_or_else(|| ParseError::Msg(format!("missing value for {key}")))
        };
        match key.as_str() {
            "-h" | "--help" => return Err(ParseError::Help),
            "--daemon-binary" => daemon_binary = Some(PathBuf::from(take_val(&mut i)?)),
            "--wasm-dir" => wasm_dir = Some(PathBuf::from(take_val(&mut i)?)),
            "--output" => output_csv = Some(PathBuf::from(take_val(&mut i)?)),
            "--run-id" => run_id = Some(take_val(&mut i)?),
            "--compiles-per-worker" => {
                compiles_per_worker = take_val(&mut i)?
                    .parse()
                    .map_err(|e| ParseError::Msg(format!("invalid --compiles-per-worker: {e}")))?;
            }
            "--repeat" => {
                repeat = take_val(&mut i)?
                    .parse()
                    .map_err(|e| ParseError::Msg(format!("invalid --repeat: {e}")))?;
            }
            "--memory-limit-bytes" => {
                memory_limit_bytes =
                    Some(take_val(&mut i)?.parse().map_err(|e| {
                        ParseError::Msg(format!("invalid --memory-limit-bytes: {e}"))
                    })?);
            }
            "--rayon-num-threads" => {
                let threads: usize = take_val(&mut i)?
                    .parse()
                    .map_err(|e| ParseError::Msg(format!("invalid --rayon-num-threads: {e}")))?;
                if threads == 0 {
                    return Err(ParseError::Msg("--rayon-num-threads must be positive".into()));
                }
                rayon_num_threads = Some(threads);
            }
            "--compiler-backend" => {
                let value = take_val(&mut i)?;
                compiler_backend = Some(CompilerBackend::parse(&value).ok_or_else(|| {
                    ParseError::Msg(format!(
                        "invalid --compiler-backend: {value}; expected winch or cranelift"
                    ))
                })?);
            }
            _ => return Err(ParseError::Msg(format!("unknown argument: {key}"))),
        }
        i += 1;
    }

    let wasm_dir = wasm_dir.ok_or_else(|| ParseError::Msg("--wasm-dir is required".into()))?;
    let output_csv = output_csv.ok_or_else(|| ParseError::Msg("--output is required".into()))?;

    // Find the daemon binary. Cargo's CARGO_BIN_EXE_* only works for test
    // targets, not bin-to-bin references, so we look relative to this
    // executable instead (both binaries land in the same target dir).
    let daemon_binary = daemon_binary
        .or_else(|| option_env!("CARGO_BIN_EXE_near-vm-compiler-daemon").map(PathBuf::from))
        .or_else(|| {
            std::env::current_exe()
                .ok()
                .and_then(|exe| exe.parent().map(|dir| dir.join("near-vm-compiler-daemon")))
                .filter(|p| p.exists())
        })
        .ok_or_else(|| {
            ParseError::Msg(
                "daemon binary not found; build it with `cargo build --bin near-vm-compiler-daemon` or pass --daemon-binary".into(),
            )
        })?;

    let run_id = run_id.unwrap_or_else(|| {
        format!(
            "run-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        )
    });

    Ok(ExperimentConfig {
        daemon_binary,
        wasm_dir,
        output_csv,
        run_id,
        compiles_per_worker,
        repeat,
        memory_limit_bytes,
        rayon_num_threads,
        compiler_backend,
    })
}
