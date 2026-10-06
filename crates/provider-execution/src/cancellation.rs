//! Process-scoped termination requests for one-shot provider invocations.
//!
//! `SIGTERM` and `SIGINT` are recorded instead of terminating the provider, so
//! launch custody can terminate the native process group, emit its terminal
//! event, and publish durable state. Signal dispositions belong to the whole
//! process: this suits a provider process that owns one launch. A resident
//! runtime serving several sessions needs session-scoped cancellation instead.
//! The latch has no reset. Installation does not check `sigaction` errors or
//! restore previous dispositions. The caller must observe recorded signals and
//! perform cleanup; installing these handlers does not discharge custody.

use std::sync::atomic::{AtomicI32, Ordering};

static TERMINATION_SIGNAL: AtomicI32 = AtomicI32::new(0);

extern "C" fn record_termination_signal(signal: i32) {
    TERMINATION_SIGNAL.store(signal, Ordering::Relaxed);
}

/// Installs the recording handlers once per process.
pub fn install_termination_handlers() {
    static INSTALLED: std::sync::Once = std::sync::Once::new();
    INSTALLED.call_once(|| unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = record_termination_signal as *const () as usize;
        libc::sigemptyset(&mut action.sa_mask);
        libc::sigaction(libc::SIGTERM, &action, std::ptr::null_mut());
        libc::sigaction(libc::SIGINT, &action, std::ptr::null_mut());
    });
}

/// Whether this process has received a recorded termination signal.
pub fn termination_requested() -> bool {
    termination_signal().is_some()
}

/// The most recently recorded termination signal, if any.
pub fn termination_signal() -> Option<i32> {
    match TERMINATION_SIGNAL.load(Ordering::Relaxed) {
        0 => None,
        signal => Some(signal),
    }
}
