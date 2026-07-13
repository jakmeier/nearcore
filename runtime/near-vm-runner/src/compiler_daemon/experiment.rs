//! Experiment harness for measuring compiler-daemon memory usage.
//!
//! Compiles every `.wasm` file in a directory through a daemon worker
//! subprocess and records per-compilation diagnostics — memory snapshots,
//! timing, worker reuse — to a CSV file for offline analysis.
//!
//! ## Design
//!
//! **Memory is measured from the parent side** by reading `/proc/<pid>/status`.
//! This is necessary because the worker applies a Landlock deny-all sandbox
//! that blocks the worker's own filesystem access — including `/proc/self`.
//! The parent is not sandboxed and can freely read the child's `/proc` entry.
//!
//! `VmPeak` and `VmHWM` are monotonic (never decrease), so a snapshot taken
//! immediately after a compile accurately reflects the peak reached *during*
//! that compile. `VmSize` and `VmRSS` show the current footprint at snapshot
//! time.
//!
//! The worker reports compile-internal timing and reuse indices via
//! [`CompileStats`], so `compile_duration_us` excludes IPC overhead.
//!
//! ## What to look for in the CSV
//!
//! - **Memory to allocate**: `max(vm_peak_after_kb)` across all rows.
//! - **Fresh vs reused**: compare rows with `compile_index_in_worker == 0`
//!   against `compile_index_in_worker > 0`.
//! - **Growth trend**: within a worker (same `worker_pid`), track
//!   `vm_peak_after_kb` as `compile_index_in_worker` increases.

use super::MIN_WORKER_MEMORY_LIMIT_BYTES;
use super::protocol::{
    CompileRequest, CompileResponse, CompileStats, DaemonStartup, read_frame, write_frame,
};
use crate::prepare::prepare_contract;
use anyhow::{Context, Result, bail};
use near_parameters::RuntimeConfigStore;
use near_parameters::vm::VMKind;
use near_primitives_core::version::PROTOCOL_VERSION;
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Wasmtime backend selected for an experiment worker.
#[derive(Clone, Copy)]
pub enum CompilerBackend {
    Winch,
    Cranelift,
}

impl CompilerBackend {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "winch" => Some(Self::Winch),
            "cranelift" => Some(Self::Cranelift),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Winch => "winch",
            Self::Cranelift => "cranelift",
        }
    }
}

/// Configuration for a single experiment invocation.
pub struct ExperimentConfig {
    /// Path to the `near-vm-compiler-daemon` binary.
    pub daemon_binary: PathBuf,
    /// Directory containing `.wasm` files to compile (non-recursive).
    pub wasm_dir: PathBuf,
    /// Output CSV file path (overwritten).
    pub output_csv: PathBuf,
    /// Identifier written to every CSV row. Use different values to separate
    /// runs when merging multiple CSV files for error-bar analysis.
    pub run_id: String,
    /// Restart the worker subprocess after this many compilations.
    /// `0` = never restart (reuse one worker for all files in a pass).
    /// `1` = fresh worker for every single compile.
    pub compiles_per_worker: usize,
    /// Number of full passes over the contract directory.
    pub repeat: usize,
    /// Worker `RLIMIT_AS` in bytes. `None` = use the daemon default
    /// (`MIN_WORKER_MEMORY_LIMIT_BYTES`).
    pub memory_limit_bytes: Option<u64>,
    /// Number of threads in Rayon's global compiler pool. `None` uses Rayon's
    /// default, which is the worker's available CPU parallelism.
    pub rayon_num_threads: Option<usize>,
    /// Wasmtime compiler backend. `None` uses the production default.
    pub compiler_backend: Option<CompilerBackend>,
}

/// Run the experiment: iterate over wasm files, compile each in a daemon
/// worker, and append one CSV row per compilation.
pub fn run(config: &ExperimentConfig) -> Result<()> {
    let wasm_config = load_wasm_config();
    let limit = &wasm_config.limit_config;
    let wasm_files = collect_wasm_files(&config.wasm_dir)?;
    if wasm_files.is_empty() {
        bail!("no .wasm files found in {}", config.wasm_dir.display());
    }
    eprintln!("found {} wasm files in {}", wasm_files.len(), config.wasm_dir.display());

    let mut csv = CsvWriter::create(&config.output_csv)?;
    let effective_memory_limit = config.memory_limit_bytes.unwrap_or(MIN_WORKER_MEMORY_LIMIT_BYTES);

    for repeat_idx in 0..config.repeat {
        eprintln!(
            "[{}] repeat {}/{}: {} files",
            config.run_id,
            repeat_idx + 1,
            config.repeat,
            wasm_files.len()
        );

        let mut worker: Option<Worker> = None;
        let mut compiles_in_worker: usize = 0;

        for (file_idx, wasm_path) in wasm_files.iter().enumerate() {
            // Ensure we have a live worker.
            if worker.is_none() {
                worker = Some(
                    Worker::spawn(
                        &config.daemon_binary,
                        config.memory_limit_bytes,
                        config.rayon_num_threads,
                        config.compiler_backend,
                    )
                    .context("failed to spawn daemon worker")?,
                );
                compiles_in_worker = 0;
            }
            let w = worker.as_mut().unwrap();
            let contract_name = file_name(wasm_path);

            // --- Read the raw wasm ---
            let wasm = match std::fs::read(wasm_path) {
                Ok(data) => data,
                Err(e) => {
                    csv.write(csv_row(
                        config,
                        unix_millis(),
                        repeat_idx,
                        file_idx,
                        &contract_name,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        w.pid,
                        compiles_in_worker as u32,
                        0,
                        false,
                        effective_memory_limit,
                        limit.max_memory_pages,
                        limit.max_tables_per_contract,
                        limit.max_elements_per_contract_table,
                        ProcMemory::default(),
                        ProcMemory::default(),
                        "read_error",
                        &e.to_string(),
                    ))
                    .context("failed to write CSV row")?;
                    compiles_in_worker += 1;
                    if should_recycle(config, compiles_in_worker) {
                        worker = None;
                    }
                    continue;
                }
            };

            let non_custom_wasm_size = non_custom_wasm_size(&wasm).unwrap_or(0);

            // --- Prepare (instrumentation/validation pass, in-process) ---
            let prepared = match prepare_contract(&wasm, &wasm_config, VMKind::Wasmtime) {
                Ok(p) => p,
                Err(e) => {
                    // Preparation failure is not a worker issue — don't count
                    // it toward the recycle budget.
                    csv.write(csv_row(
                        config,
                        unix_millis(),
                        repeat_idx,
                        file_idx,
                        &contract_name,
                        wasm.len(),
                        non_custom_wasm_size,
                        0,
                        0,
                        0,
                        0,
                        w.pid,
                        compiles_in_worker as u32,
                        0,
                        false,
                        effective_memory_limit,
                        limit.max_memory_pages,
                        limit.max_tables_per_contract,
                        limit.max_elements_per_contract_table,
                        ProcMemory::default(),
                        ProcMemory::default(),
                        "prepare_error",
                        &e.to_string(),
                    ))
                    .context("failed to write CSV row")?;
                    continue;
                }
            };

            let request = CompileRequest {
                prepared_code: prepared.clone(),
                max_memory_pages: limit.max_memory_pages,
                max_tables_per_contract: limit.max_tables_per_contract,
                max_elements_per_contract_table: limit
                    .max_elements_per_contract_table
                    .map(|v| v as u64),
            };

            // --- Measure + compile + measure ---
            let mem_before = read_proc_memory(w.pid);
            let wall_start = Instant::now();
            let result = w.compile(&request);
            let wall_duration_us = wall_start.elapsed().as_micros() as u64;
            let mem_after = read_proc_memory(w.pid);
            let ts = unix_millis();

            compiles_in_worker += 1;

            match result {
                Ok(Ok((artifact, stats))) => {
                    csv.write(csv_row(
                        config,
                        ts,
                        repeat_idx,
                        file_idx,
                        &contract_name,
                        wasm.len(),
                        non_custom_wasm_size,
                        prepared.len(),
                        artifact.len() as u64,
                        wall_duration_us,
                        stats.compile_duration_us,
                        w.pid,
                        stats.compile_index_in_worker,
                        stats.compile_index_in_engine,
                        stats.engine_created,
                        effective_memory_limit,
                        limit.max_memory_pages,
                        limit.max_tables_per_contract,
                        limit.max_elements_per_contract_table,
                        mem_before,
                        mem_after,
                        "ok",
                        "",
                    ))
                    .context("failed to write CSV row")?;
                    if should_recycle(config, compiles_in_worker) {
                        worker = None;
                    }
                }
                Ok(Err(msg)) => {
                    // Compilation error: the worker is healthy, keep it.
                    csv.write(csv_row(
                        config,
                        ts,
                        repeat_idx,
                        file_idx,
                        &contract_name,
                        wasm.len(),
                        non_custom_wasm_size,
                        prepared.len(),
                        0,
                        wall_duration_us,
                        0,
                        w.pid,
                        compiles_in_worker as u32 - 1,
                        0,
                        false,
                        effective_memory_limit,
                        limit.max_memory_pages,
                        limit.max_tables_per_contract,
                        limit.max_elements_per_contract_table,
                        mem_before,
                        mem_after,
                        "compile_error",
                        &msg,
                    ))
                    .context("failed to write CSV row")?;
                    if should_recycle(config, compiles_in_worker) {
                        worker = None;
                    }
                }
                Err(ipc_err) => {
                    eprintln!("  warning: worker IPC failure on {contract_name}: {ipc_err}");
                    csv.write(csv_row(
                        config,
                        ts,
                        repeat_idx,
                        file_idx,
                        &contract_name,
                        wasm.len(),
                        non_custom_wasm_size,
                        prepared.len(),
                        0,
                        wall_duration_us,
                        0,
                        w.pid,
                        compiles_in_worker as u32 - 1,
                        0,
                        false,
                        effective_memory_limit,
                        limit.max_memory_pages,
                        limit.max_tables_per_contract,
                        limit.max_elements_per_contract_table,
                        mem_before,
                        mem_after,
                        "ipc_error",
                        &ipc_err,
                    ))
                    .context("failed to write CSV row")?;
                    // Worker is dead — force recycle.
                    worker = None;
                }
            }
        }
    }

    eprintln!("wrote results to {}", config.output_csv.display());
    Ok(())
}

// ---------------------------------------------------------------------------
// Worker — minimal daemon subprocess wrapper
// ---------------------------------------------------------------------------

/// A single daemon worker subprocess, managed directly (not through the pool).
struct Worker {
    child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
    pid: u32,
}

impl Worker {
    fn spawn(
        binary: &Path,
        memory_limit_bytes: Option<u64>,
        rayon_num_threads: Option<usize>,
        compiler_backend: Option<CompilerBackend>,
    ) -> Result<Worker> {
        let mut cmd = Command::new(binary);
        // Match production: the child configures itself entirely via IPC and
        // must not inherit allocator/proxy/logging env from the experiment.
        cmd.env_clear().current_dir("/").stdin(Stdio::piped()).stdout(Stdio::piped());
        if let Some(limit) = memory_limit_bytes {
            cmd.arg("--memory-limit-bytes").arg(limit.to_string());
        }
        if let Some(threads) = rayon_num_threads {
            cmd.env("RAYON_NUM_THREADS", threads.to_string());
        }
        if let Some(backend) = compiler_backend {
            cmd.arg("--compiler-backend").arg(backend.as_str());
        }
        let mut child = cmd
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("failed to spawn daemon binary {}", binary.display()))?;
        let pid = child.id();
        let stdin = child.stdin.take().unwrap();
        let mut stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();

        // Wait for the startup handshake before declaring the worker ready.
        let startup_bytes = read_frame(&mut stdout).context("failed to read startup frame")?;
        let startup: DaemonStartup =
            borsh::from_slice(&startup_bytes).context("failed to parse startup frame")?;
        match startup {
            DaemonStartup::Ready => {}
            DaemonStartup::Err(err) => {
                let _ = child.kill();
                let _ = child.wait();
                bail!("daemon startup failed: {err}");
            }
        }

        // Drain stderr so the worker can't block on a full pipe. Relay to the
        // parent's stderr for debugging.
        std::thread::Builder::new()
            .name("experiment-worker-stderr".to_owned())
            .spawn(move || relay_stderr(stderr))
            .context("failed to spawn stderr relay thread")?;

        Ok(Worker { child, stdin, stdout, pid })
    }

    /// Send a compile request and read the response.
    ///
    /// Returns:
    /// - `Ok(Ok((artifact, stats)))` — compilation succeeded
    /// - `Ok(Err(msg))` — daemon reported a compilation error (worker healthy)
    /// - `Err(msg)` — IPC failure, worker likely crashed
    fn compile(
        &mut self,
        request: &CompileRequest,
    ) -> std::result::Result<std::result::Result<(Vec<u8>, CompileStats), String>, String> {
        let request_bytes =
            borsh::to_vec(request).map_err(|e| format!("failed to serialize request: {e}"))?;
        write_frame(&mut self.stdin, &request_bytes)
            .map_err(|e| format!("failed to send to daemon: {e}"))?;
        let response_bytes =
            read_frame(&mut self.stdout).map_err(|e| format!("failed to read from daemon: {e}"))?;
        let response: CompileResponse = borsh::from_slice(&response_bytes)
            .map_err(|e| format!("failed to deserialize response: {e}"))?;
        match response {
            CompileResponse::Ok { artifact, stats } => Ok(Ok((artifact, stats))),
            CompileResponse::Err(msg) => Ok(Err(msg)),
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn relay_stderr(mut stderr: std::process::ChildStderr) {
    let mut buf = [0u8; 4096];
    let mut out = std::io::stderr();
    loop {
        match stderr.read(&mut buf) {
            Ok(0) | Err(_) => return,
            Ok(n) => {
                let _ = out.write_all(&buf[..n]);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// /proc/<pid>/status memory reader (parent-side)
// ---------------------------------------------------------------------------

/// Snapshot of the worker's memory metrics from `/proc/<pid>/status`.
#[derive(Default, Clone, Copy)]
struct ProcMemory {
    vm_size_kb: u64,
    vm_peak_kb: u64,
    vm_rss_kb: u64,
    vm_hwm_kb: u64,
}

#[cfg(target_os = "linux")]
fn read_proc_memory(pid: u32) -> ProcMemory {
    let path = format!("/proc/{pid}/status");
    let Ok(status) = std::fs::read_to_string(&path) else {
        return ProcMemory::default();
    };
    let mut mem = ProcMemory::default();
    for line in status.lines() {
        let Some((key, val)) = line.split_once(':') else { continue };
        let kb = val.trim().split_whitespace().next().and_then(|s| s.parse().ok()).unwrap_or(0);
        match key.trim() {
            "VmSize" => mem.vm_size_kb = kb,
            "VmPeak" => mem.vm_peak_kb = kb,
            "VmRSS" => mem.vm_rss_kb = kb,
            "VmHWM" => mem.vm_hwm_kb = kb,
            _ => {}
        }
    }
    mem
}

#[cfg(not(target_os = "linux"))]
fn read_proc_memory(_pid: u32) -> ProcMemory {
    ProcMemory::default()
}

// ---------------------------------------------------------------------------
// CSV writer
// ---------------------------------------------------------------------------

/// Column order — must match [`csv_row`] below.
const CSV_HEADER: &str = "\
run_id,\
timestamp_unix_ms,\
repeat_idx,\
file_idx,\
contract_name,\
wasm_size_bytes,\
non_custom_wasm_size_bytes,\
prepared_size_bytes,\
artifact_size_bytes,\
wall_duration_us,\
compile_duration_us,\
worker_pid,\
compile_index_in_worker,\
compile_index_in_engine,\
engine_created,\
memory_limit_bytes,\
rayon_num_threads,\
compiler_backend,\
max_memory_pages,\
max_tables_per_contract,\
max_elements_per_contract_table,\
vm_size_before_kb,\
vm_peak_before_kb,\
vm_rss_before_kb,\
vm_hwm_before_kb,\
vm_size_after_kb,\
vm_peak_after_kb,\
vm_rss_after_kb,\
vm_hwm_after_kb,\
status,\
error_message";

struct CsvWriter {
    file: BufWriter<std::fs::File>,
}

impl CsvWriter {
    fn create(path: &Path) -> Result<Self> {
        let mut file = std::fs::File::create(path)
            .with_context(|| format!("failed to create CSV at {}", path.display()))?;
        writeln!(file, "{CSV_HEADER}")?;
        Ok(CsvWriter { file: BufWriter::new(file) })
    }

    fn write(&mut self, fields: Vec<String>) -> Result<()> {
        for (i, field) in fields.iter().enumerate() {
            if i > 0 {
                write!(self.file, ",")?;
            }
            write!(self.file, "{}", csv_escape(field))?;
        }
        writeln!(self.file)?;
        Ok(())
    }
}

/// RFC 4180 quoting: if the field contains comma, quote, or newline, wrap in
/// double quotes and double any embedded quotes.
fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_owned()
    }
}

/// Build a single CSV row from raw fields. The order must match
/// [`CSV_HEADER`].
#[allow(clippy::too_many_arguments)]
fn csv_row(
    config: &ExperimentConfig,
    timestamp_unix_ms: u64,
    repeat_idx: usize,
    file_idx: usize,
    contract_name: &str,
    wasm_size_bytes: usize,
    non_custom_wasm_size_bytes: usize,
    prepared_size_bytes: usize,
    artifact_size_bytes: u64,
    wall_duration_us: u64,
    compile_duration_us: u64,
    worker_pid: u32,
    compile_index_in_worker: u32,
    compile_index_in_engine: u32,
    engine_created: bool,
    memory_limit_bytes: u64,
    max_memory_pages: u32,
    max_tables_per_contract: Option<u32>,
    max_elements_per_contract_table: Option<usize>,
    mem_before: ProcMemory,
    mem_after: ProcMemory,
    status: &str,
    error_message: &str,
) -> Vec<String> {
    vec![
        config.run_id.clone(),
        timestamp_unix_ms.to_string(),
        repeat_idx.to_string(),
        file_idx.to_string(),
        contract_name.to_owned(),
        wasm_size_bytes.to_string(),
        non_custom_wasm_size_bytes.to_string(),
        prepared_size_bytes.to_string(),
        artifact_size_bytes.to_string(),
        wall_duration_us.to_string(),
        compile_duration_us.to_string(),
        worker_pid.to_string(),
        compile_index_in_worker.to_string(),
        compile_index_in_engine.to_string(),
        engine_created.to_string(),
        memory_limit_bytes.to_string(),
        opt_to_string(config.rayon_num_threads),
        config.compiler_backend.map(CompilerBackend::as_str).unwrap_or_default().to_owned(),
        max_memory_pages.to_string(),
        opt_to_string(max_tables_per_contract),
        opt_to_string(max_elements_per_contract_table),
        mem_before.vm_size_kb.to_string(),
        mem_before.vm_peak_kb.to_string(),
        mem_before.vm_rss_kb.to_string(),
        mem_before.vm_hwm_kb.to_string(),
        mem_after.vm_size_kb.to_string(),
        mem_after.vm_peak_kb.to_string(),
        mem_after.vm_rss_kb.to_string(),
        mem_after.vm_hwm_kb.to_string(),
        status.to_owned(),
        error_message.to_owned(),
    ]
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn load_wasm_config() -> near_parameters::vm::Config {
    let store = RuntimeConfigStore::new(None);
    let config = store.get_config(PROTOCOL_VERSION);
    let mut wasm_config = near_parameters::vm::Config::clone(&config.wasm_config);
    wasm_config.vm_kind = VMKind::Wasmtime;
    wasm_config
}

/// Return the input size after excluding every custom section, including its
/// section ID and length prefix. `None` means that the Wasm section framing is
/// malformed.
fn non_custom_wasm_size(wasm: &[u8]) -> Option<usize> {
    if wasm.len() < 8 || &wasm[..4] != b"\0asm" {
        return None;
    }

    let mut offset = 8;
    let mut custom_size = 0_usize;
    while offset < wasm.len() {
        let section_start = offset;
        let id = *wasm.get(offset)?;
        offset += 1;
        let (section_len, length_len) = decode_var_u32(wasm.get(offset..)?)?;
        offset = offset.checked_add(length_len)?;
        let section_end = offset.checked_add(section_len as usize)?;
        wasm.get(offset..section_end)?;
        if id == 0 {
            custom_size = custom_size.checked_add(section_end.checked_sub(section_start)?)?;
        }
        offset = section_end;
    }
    wasm.len().checked_sub(custom_size)
}

fn decode_var_u32(bytes: &[u8]) -> Option<(u32, usize)> {
    let mut value = 0_u32;
    for (idx, byte) in bytes.iter().copied().enumerate().take(5) {
        if idx == 4 && byte > 0x0f {
            return None;
        }
        value |= u32::from(byte & 0x7f) << (idx * 7);
        if byte & 0x80 == 0 {
            return Some((value, idx + 1));
        }
    }
    None
}

fn collect_wasm_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .with_context(|| format!("failed to read directory {}", dir.display()))?
        .filter_map(std::result::Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().map_or(false, |ext| ext == "wasm"))
        .collect();
    files.sort();
    Ok(files)
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.display().to_string())
}

fn should_recycle(config: &ExperimentConfig, compiles_in_worker: usize) -> bool {
    config.compiles_per_worker > 0 && compiles_in_worker >= config.compiles_per_worker
}

fn unix_millis() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

fn opt_to_string<T: ToString>(v: Option<T>) -> String {
    v.map(|x| x.to_string()).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::non_custom_wasm_size;

    #[test]
    fn non_custom_wasm_size_excludes_section_framing() {
        let wasm = [
            0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, // header
            0x00, 0x04, 0x03, b'f', b'o', b'o', // custom section
            0x01, 0x01, 0x00, // empty type section
        ];

        assert_eq!(non_custom_wasm_size(&wasm), Some(11));
    }
}
