//! Out-of-process WASM compiler daemon.
//!
//! Isolates Cranelift/Winch compilation into subprocesses to protect the main
//! neard process from compiler crashes and to enforce memory limits.
//!
//! A pool of worker subprocesses serves compilations in parallel; checkout is
//! ordered by [`crate::CompilePriority`] so background work cannot starve
//! latency-critical compilations. See `parent` for the pool, `child` for the
//! worker loop, and `protocol` for the IPC framing.

mod child;
mod parent;
pub mod protocol;
mod sandbox;

pub use child::daemon_main;
pub use parent::{
    compile_in_subprocess, is_daemon_configured, set_daemon_binary, set_daemon_pool_size,
};

#[cfg(feature = "test_features")]
pub use parent::spawned_worker_high_water;

/// Minimum per-worker virtual memory budget.
///
/// Applied as `RLIMIT_AS` in the daemon child (VmSize). Lives here so the
/// parent and child share a single source of truth and the value can never
/// drift between them.
///
/// If compilation fails due to this limit, it will be retried with a higher
/// limit, up to `MAX_WORKER_MEMORY_LIMIT_BYTES`. (TODO)
///
/// This is a virtual address space limit, not physical memory: the OS
/// OOM-killer reacts to physical usage (VmRSS), which may be significantly less
/// than VmSize. Setting this too high reduces the number of spawned workers;
/// setting it too low causes compilation failures.
const MIN_WORKER_MEMORY_LIMIT_BYTES: u64 = 4 * bytesize::GIB;

/// Hard cap on worker subprocesses regardless of the configured/derived size.
///
/// Each worker has a rayon pool utilizing multiple threads, so a handful of
/// overlapping large compilations already saturate the CPU. More workers only
/// add memory pressure and oversubscription.
const MAX_POOL_SIZE: usize = 8;

/// Total virtual memory budget set aside for compiler-daemon workers, in bytes.
///
/// Not a limit in itself: the default pool size caps the worker count so that
/// `workers × MIN_WORKER_MEMORY_LIMIT_BYTES` stays within this budget. Each
/// worker's actual cap is its per-worker `RLIMIT_AS` (VmSize), enforced via
/// `setrlimit` in the child. The OS OOM-killer, however, reacts to physical
/// memory usage (VmRSS), which may be significantly less than VmSize, so you
/// may want to oversubscribe this above the actually available physical memory.
const DEFAULT_TOTAL_MEMORY_BUDGET_BYTES: u64 = 4 * bytesize::GIB;

/// Per-request retry budget on IPC failure (worker crash).
const MAX_SPAWN_ATTEMPTS: u32 = 2;
