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
//! scheduling, and host-state authority stay in Agent Runner. Termination
//! handling is process-scoped and the custody types assume the caller owns one
//! launch at a time; [`lifecycle::run_launch_until`] adds a caller-scoped stop,
//! and [`resident`] serves resident ACP v2 sessions whose turns each run one
//! launch through it. [`tool_bridge`] is the mediated `bash` tool a provider
//! registers natively under `oulipoly.tool_mediation/v1`; under
//! `oulipoly.exploration/v1` it also serves [`explore_tool`], the
//! non-command child exploration tool.
//!
//! Callers of the individual modules, rather than [`lifecycle::run_launch`],
//! enforce lifecycle ordering, hold request custody, and unwind child custody
//! on errors. Neither path validates events or state against the contract
//! schema; see [`lifecycle::LaunchAdapter`] for adapter obligations and
//! [`custody`] and [`framing`] for error/seal obligations. Delivery bounds apply to FIFO/socket progress,
//! and incarnation checks do not make signalling atomic; see [`delivery`] and
//! [`process`].

#[cfg(unix)]
pub mod cancellation;
pub mod custody;
#[cfg(unix)]
pub mod delivery;
pub mod durable_fs;
pub mod encoding;
#[cfg(unix)]
pub mod explore_tool;
pub mod framing;
#[cfg(unix)]
pub mod lifecycle;
pub mod process;
#[cfg(unix)]
pub mod resident;
#[cfg(unix)]
pub mod tool_bridge;
