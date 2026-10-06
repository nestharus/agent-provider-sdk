//! Provider-neutral execution machinery for external `oulipoly.provider/v1`
//! adapters.
//!
//! The modules were seeded from the Codex adapter's launch path and generalized
//! by parameterizing provider identity. They cover one native invocation per
//! provider process: process-group custody behind an effect gate, bounded
//! launch-output delivery, request custody with durable launch state and an
//! exactly replayable event journal, provider/v1 launch-event framing, and
//! durable filesystem and encoding helpers. [`lifecycle`] composes them into
//! the shared one-shot launch lifecycle that adapters plug native behavior
//! into.
//!
//! Native argv, authentication, account and config roots, model aliases, tool
//! restrictions, session formats, and native event translation stay in
//! provider adapters. Logical session identity, admission, ancestry,
//! scheduling, and host-state authority stay in Agent Runner. This crate is not
//! a resident runtime: termination handling is process-scoped and the custody
//! types assume the caller owns one launch at a time.
//!
//! Callers of the individual modules, rather than [`lifecycle::run_launch`],
//! enforce lifecycle ordering, validate events/state, hold request custody,
//! and unwind child custody on errors. See [`custody`] and [`framing`]
//! for error/seal obligations. Delivery bounds apply to FIFO/socket progress,
//! and incarnation checks do not make signalling atomic; see [`delivery`] and
//! [`process`].

#[cfg(unix)]
pub mod cancellation;
pub mod custody;
#[cfg(unix)]
pub mod delivery;
pub mod durable_fs;
pub mod encoding;
pub mod framing;
#[cfg(unix)]
pub mod lifecycle;
pub mod process;
