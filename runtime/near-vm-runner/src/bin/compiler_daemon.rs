//! Out-of-process WASM compiler daemon worker.

fn main() -> ! {
    near_vm_runner::compiler_daemon::daemon_main()
}
