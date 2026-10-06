//! Shared one-shot launch lifecycle.
//!
//! [`run_launch`] owns the ordering that the other modules leave to callers:
//! request custody and digest checks, exact completed replay, discharge of an
//! interrupted actor followed by required reconciliation, admission checks
//! against termination requests and the host deadline, prepared state before
//! spawn, a running state carrying the process-group actor before the effect
//! gate opens, journaled event framing, concurrent output draining, heartbeats,
//! termination on cancellation or deadline, process-group cleanup after the
//! native leader exits, a bounded drain, input delivery, the final `exit`
//! event, and the sealed completion receipt.
//!
//! A provider adapter plugs native behavior in through [`LaunchAdapter`]: its
//! request digest, native argv/environment/session preparation, translation of
//! native output into launch events, and the terminal status and signal.
//! Native program names, models, events and terminal classes never appear
//! here, and logical session identity, admission and scheduling remain with
//! the host.
//!
//! Scope and caller assumptions:
//!
//! - One launch per provider process. Cancellation is the process-scoped
//!   `SIGTERM`/`SIGINT` latch in [`crate::cancellation`]; a resident runtime
//!   serving several sessions needs session-scoped cancellation instead.
//! - The state root is a trusted, existing, provider-private directory. The
//!   request key is derived from the provider instance and request ID; the
//!   digest is whatever the adapter supplies. No executable or source identity
//!   is added by this module, so a compatible rebuild that keeps the adapter's
//!   digest inputs and the seven-field [`LaunchState`] record replays completed
//!   requests and reconciles interrupted ones.
//! - Host output should be a [`crate::delivery::BoundedOutput`]; an arbitrary
//!   blocking writer carries no delivery bound.
//! - Any error leaves incomplete durable evidence. The native process group is
//!   terminated when the lifecycle unwinds, and a retry of the same request
//!   discharges any recorded actor and reports
//!   [`LifecycleError::ReconciliationRequired`] instead of starting another
//!   native turn.
//! - Drain bounds measure silence, not total time: a native group that keeps
//!   writing after its leader exited is drained until it stops.

use crate::cancellation;
use crate::custody::{self, CustodyError, LaunchState, RequestCustody};
use crate::encoding::now_unix_ms;
use crate::framing::{FramingError, LaunchEventWriter};
use crate::process::{self, GatedCommand, ProcessGroupActor};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fmt;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::process::{Child, ExitStatus, Stdio};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Identity and limits of one launch request.
#[derive(Clone, Copy, Debug)]
pub struct LaunchSpec<'a> {
    /// Contract discriminator stamped on every launch event.
    pub contract: &'a str,
    pub request_id: &'a str,
    pub provider_instance_id: Option<&'a str>,
    /// Host deadline; reaching it before admission refuses the launch and
    /// reaching it later terminates the native process group.
    pub deadline_unix_ms: Option<u64>,
    /// Existing provider-private directory holding launch custody.
    pub state_root: &'a std::path::Path,
    pub timing: LifecycleTiming,
}

/// Polling, heartbeat and drain intervals.
#[derive(Clone, Copy, Debug)]
pub struct LifecycleTiming {
    /// Longest wait for native output before rechecking termination requests.
    pub poll_interval: Duration,
    /// Interval between `heartbeat` events; `None` disables them.
    pub heartbeat_interval: Option<Duration>,
    /// How long native output may stay closed without the leader exiting, and
    /// how long native output may stay silent after the leader exited, before
    /// the launch fails. Also bounds the wait for the input writer.
    pub drain_grace: Duration,
}

impl Default for LifecycleTiming {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(100),
            heartbeat_interval: Some(Duration::from_secs(1)),
            drain_grace: Duration::from_secs(2),
        }
    }
}

/// Native output channel.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Channel {
    Stdout,
    Stderr,
}

impl Channel {
    /// The launch event kind for data on this channel.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        }
    }
}

/// How native output is divided before it reaches [`LaunchAdapter::output`].
#[derive(Clone, Copy, Debug)]
pub enum OutputFraming {
    /// Newline-terminated records (the final record may lack its newline). A
    /// record longer than `max_bytes` fails the launch.
    Lines { max_bytes: u64 },
    /// Raw reads of at most `max_bytes` bytes, preserving byte boundaries only
    /// as the operating system delivers them.
    Chunks { max_bytes: usize },
}

/// A native command prepared behind the provider's effect gate.
pub struct NativeCommand {
    /// Native program, arguments, environment and working directory. The
    /// lifecycle configures stdio and process-group custody.
    pub command: GatedCommand,
    /// Bytes written to native stdin by a separate writer; `None` connects
    /// stdin to `/dev/null`.
    pub stdin: Option<Vec<u8>>,
    pub framing: OutputFraming,
}

/// Outcome of adapter preparation.
pub enum Preparation {
    /// Run the native command.
    Native(NativeCommand),
    /// The adapter settled the launch without a native process, for example a
    /// provider whose contract reports an unspawnable command as an `exit`
    /// event. `events` precede the exit event. The outcome is journaled and
    /// replayed exactly like a native one.
    Settled {
        events: Vec<Value>,
        terminal: Terminal,
    },
}

/// Why the lifecycle terminated the native process group.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StopCause {
    /// A recorded termination signal.
    Cancelled { signal: i32 },
    /// The host deadline elapsed.
    Deadline,
}

/// Native termination observed by the lifecycle.
#[derive(Clone, Copy, Debug)]
pub struct NativeOutcome {
    /// Wait status of the native leader.
    pub status: ExitStatus,
    /// Set when the lifecycle terminated the group for cancellation or the
    /// deadline rather than observing its own exit.
    pub stopped: Option<StopCause>,
}

/// The final `exit` event and the provider process exit code recorded with it.
#[derive(Clone, Debug)]
pub struct Terminal {
    /// Contract process status object.
    pub status: Value,
    /// Contract terminal-signal object.
    pub terminal_signal: Value,
    /// Optional `session` object.
    pub session: Option<Value>,
    /// Exit code of this provider invocation, also returned on replay.
    pub exit_code: i32,
}

/// Native plug points for [`run_launch`].
///
/// Methods run in this order: [`request_digest`](Self::request_digest) once
/// custody is held; [`prepare`](Self::prepare) after the first admission
/// check; [`discard`](Self::discard) only if a later admission check refuses
/// the launch before the gate opens; [`started`](Self::started) after the gate
/// opens; [`output`](Self::output) for each native record in arrival order; and
/// [`finish`](Self::finish) after the native group has ended, output has
/// drained and input delivery has been checked. Any error stops the launch
/// and leaves incomplete custody.
pub trait LaunchAdapter {
    type Failure: From<LifecycleError>;

    /// Digest of every input that determines the launch's effects. A retry
    /// with an equal digest replays; a different digest is a conflict. Do not
    /// include executable or source identity.
    fn request_digest(&mut self) -> Result<String, Self::Failure>;

    /// Validates native preconditions and builds the gated command. Sidecar
    /// files belong under `custody.sibling(..)`; locks that must outlive the
    /// launch belong in the adapter.
    fn prepare(&mut self, custody: &RequestCustody) -> Result<Preparation, Self::Failure>;

    /// Removes sidecars created by [`prepare`](Self::prepare) after admission
    /// was refused before the native program could run.
    fn discard(&mut self, _custody: &RequestCustody) -> Result<(), Self::Failure> {
        Ok(())
    }

    /// Emits events that follow gate release and precede native output.
    fn started<W: Write>(&mut self, _events: &mut EventSink<'_, W>) -> Result<(), Self::Failure> {
        Ok(())
    }

    /// Translates one native output record.
    fn output<W: Write>(
        &mut self,
        channel: Channel,
        bytes: Vec<u8>,
        events: &mut EventSink<'_, W>,
    ) -> Result<(), Self::Failure>;

    /// Emits final markers and returns the terminal outcome.
    fn finish<W: Write>(
        &mut self,
        outcome: NativeOutcome,
        events: &mut EventSink<'_, W>,
    ) -> Result<Terminal, Self::Failure>;
}

/// Lifecycle failure. Adapters map each variant to their contract failure.
#[derive(Debug)]
pub enum LifecycleError {
    /// Another invocation holds this request's custody.
    Busy,
    /// The request ID was already used with a different digest.
    RequestChanged,
    /// An earlier invocation ended before terminal custody; any recorded
    /// actor has been discharged.
    ReconciliationRequired,
    /// A termination signal arrived before the native program was admitted.
    Cancelled,
    /// The host deadline elapsed before the native program was admitted.
    DeadlineElapsed,
    Custody(CustodyError),
    Framing(FramingError),
    Io(io::Error),
    /// A native output record exceeded its framing limit or could not be read.
    NativeStreamInvalid,
    /// Native output closed and the leader did not exit within the drain grace.
    NativeStreamsClosed,
    /// Native output stayed open and silent after the group was terminated.
    NativeDrainIncomplete,
    /// The input writer was still blocked after native termination.
    InputStalled,
    /// The input writer thread panicked.
    InputWriterFailed,
    /// The native program exited successfully without accepting all input.
    InputIncomplete,
    /// The native leader could not be reaped.
    WaitFailed,
    /// Output accounting overflowed.
    AccountingOverflow,
}

impl fmt::Display for LifecycleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Busy => formatter.write_str("this request is already executing"),
            Self::RequestChanged => {
                formatter.write_str("request ID was already used with different inputs")
            }
            Self::ReconciliationRequired => {
                formatter.write_str("prior invocation ended before terminal custody; reconcile it")
            }
            Self::Cancelled => formatter.write_str("launch was cancelled before native admission"),
            Self::DeadlineElapsed => {
                formatter.write_str("host launch deadline elapsed before native admission")
            }
            Self::Custody(error) => error.fmt(formatter),
            Self::Framing(error) => error.fmt(formatter),
            Self::Io(error) => error.fmt(formatter),
            Self::NativeStreamInvalid => {
                formatter.write_str("native output exceeded its record limit or could not be read")
            }
            Self::NativeStreamsClosed => formatter.write_str(
                "native output closed without the native program exiting; process group terminated",
            ),
            Self::NativeDrainIncomplete => formatter
                .write_str("native output pipes remained open after process-group termination"),
            Self::InputStalled => {
                formatter.write_str("native input pipe remained open after native termination")
            }
            Self::InputWriterFailed => formatter.write_str("native input writer failed"),
            Self::InputIncomplete => {
                formatter.write_str("could not deliver the complete input to the native program")
            }
            Self::WaitFailed => formatter.write_str("could not reap the native process"),
            Self::AccountingOverflow => formatter.write_str("launch output accounting overflowed"),
        }
    }
}

impl std::error::Error for LifecycleError {}

impl From<io::Error> for LifecycleError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<CustodyError> for LifecycleError {
    fn from(error: CustodyError) -> Self {
        match error {
            CustodyError::Busy => Self::Busy,
            error => Self::Custody(error),
        }
    }
}

impl From<FramingError> for LifecycleError {
    fn from(error: FramingError) -> Self {
        Self::Framing(error)
    }
}

/// Refuses admission after a recorded termination signal or at the deadline.
pub fn check_admission(deadline_unix_ms: Option<u64>) -> Result<(), LifecycleError> {
    match stop_requested(deadline_unix_ms) {
        Some(StopCause::Cancelled { .. }) => Err(LifecycleError::Cancelled),
        Some(StopCause::Deadline) => Err(LifecycleError::DeadlineElapsed),
        None => Ok(()),
    }
}

fn stop_requested(deadline_unix_ms: Option<u64>) -> Option<StopCause> {
    if let Some(signal) = cancellation::termination_signal() {
        return Some(StopCause::Cancelled { signal });
    }
    deadline_unix_ms
        .is_some_and(|deadline| now_unix_ms() >= deadline)
        .then_some(StopCause::Deadline)
}

/// Byte count and SHA-256 of one channel's data events.
#[derive(Clone, Default)]
pub struct ChannelAccounting {
    bytes: u64,
    sha256: Sha256,
}

impl ChannelAccounting {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn sha256_hex(&self) -> String {
        format!("{:x}", self.sha256.clone().finalize())
    }

    fn accept(&mut self, bytes: &[u8]) -> Result<(), LifecycleError> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len() as u64)
            .ok_or(LifecycleError::AccountingOverflow)?;
        self.sha256.update(bytes);
        Ok(())
    }

    fn to_json(&self) -> Value {
        json!({"bytes":self.bytes,"sha256":self.sha256_hex()})
    }
}

/// Accounting of the data events delivered so far.
#[derive(Clone, Default)]
pub struct DataAccounting {
    pub stdout: ChannelAccounting,
    pub stderr: ChannelAccounting,
    pub data_event_count: u64,
}

impl DataAccounting {
    /// `{"stdout":{bytes,sha256},"stderr":{bytes,sha256},"data_event_count"}`.
    pub fn to_json(&self) -> Value {
        json!({"stdout":self.stdout.to_json(),"stderr":self.stderr.to_json(),
            "data_event_count":self.data_event_count})
    }
}

/// Journaled launch-event output available to adapters. The `exit` event is
/// reserved to the lifecycle.
pub struct EventSink<'a, W: Write> {
    events: LaunchEventWriter<'a, W>,
    accounting: DataAccounting,
}

impl<W: Write> EventSink<'_, W> {
    /// Frames and delivers an object event other than `exit`.
    pub fn event(&mut self, event: Value) -> Result<(), LifecycleError> {
        if !event.is_object() || event.get("kind") == Some(&json!("exit")) {
            return Err(LifecycleError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "adapter events must be objects and must not be exit events",
            )));
        }
        Ok(self.events.event(event)?)
    }

    pub fn marker(&mut self, name: &str, value: Value) -> Result<(), LifecycleError> {
        Ok(self.events.marker(name, value)?)
    }

    /// Emits a data event and adds it to the output accounting.
    pub fn data(&mut self, channel: Channel, bytes: &[u8]) -> Result<(), LifecycleError> {
        self.events.data(channel.as_str(), bytes)?;
        self.accounting.data_event_count = self
            .accounting
            .data_event_count
            .checked_add(1)
            .ok_or(LifecycleError::AccountingOverflow)?;
        match channel {
            Channel::Stdout => self.accounting.stdout.accept(bytes),
            Channel::Stderr => self.accounting.stderr.accept(bytes),
        }
    }

    pub fn accounting(&self) -> &DataAccounting {
        &self.accounting
    }

    fn exit(&mut self, terminal: &Terminal) -> Result<(), LifecycleError> {
        let mut event = json!({"kind":"exit","status":terminal.status,
            "terminal_signal":terminal.terminal_signal});
        if let Some(session) = &terminal.session {
            event["session"] = session.clone();
        }
        Ok(self.events.event(event)?)
    }
}

/// Terminates the native group if the lifecycle unwinds while it is live.
struct ChildGuard {
    child: Child,
    live: bool,
}

impl ChildGuard {
    fn terminate(&mut self) -> Option<ExitStatus> {
        self.live = false;
        process::terminate_process_group_child(&mut self.child)
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.live {
            process::terminate_process_group_child(&mut self.child);
        }
    }
}

type Record = (Channel, Result<Vec<u8>, ()>);

fn spawn_reader(
    channel: Channel,
    reader: Box<dyn Read + Send>,
    framing: OutputFraming,
    send: mpsc::SyncSender<Record>,
) {
    std::thread::spawn(move || match framing {
        OutputFraming::Lines { max_bytes } => {
            let mut reader = BufReader::new(reader);
            loop {
                let mut bytes = Vec::new();
                match reader
                    .by_ref()
                    .take(max_bytes.saturating_add(1))
                    .read_until(b'\n', &mut bytes)
                {
                    Ok(0) => break,
                    Ok(_) if bytes.len() as u64 <= max_bytes => {
                        if send.send((channel, Ok(bytes))).is_err() {
                            return;
                        }
                    }
                    _ => {
                        let _ = send.send((channel, Err(())));
                        break;
                    }
                }
            }
        }
        OutputFraming::Chunks { max_bytes } => {
            let mut reader = reader;
            let mut buffer = vec![0_u8; max_bytes.max(1)];
            loop {
                match reader.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(count) => {
                        if send.send((channel, Ok(buffer[..count].to_vec()))).is_err() {
                            return;
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(_) => {
                        let _ = send.send((channel, Err(())));
                        break;
                    }
                }
            }
        }
    });
}

/// Runs one launch through custody, admission, native execution and
/// completion. Returns the provider exit code recorded in the terminal
/// outcome, or the recorded code when an identical completed request replays.
pub fn run_launch<A, W>(
    spec: &LaunchSpec<'_>,
    adapter: &mut A,
    writer: &mut W,
) -> Result<i32, A::Failure>
where
    A: LaunchAdapter,
    W: Write,
{
    cancellation::install_termination_handlers();
    let key = custody::request_key(spec.provider_instance_id, spec.request_id);
    let launch_custody =
        RequestCustody::acquire(spec.state_root, &key).map_err(LifecycleError::from)?;
    let digest = adapter.request_digest()?;
    if let Some(state) = launch_custody.load_state().map_err(LifecycleError::from)? {
        if state.digest != digest {
            return Err(LifecycleError::RequestChanged.into());
        }
        if state.is_complete() {
            launch_custody
                .replay(&state, writer)
                .map_err(LifecycleError::from)?;
            return Ok(state.exit_code.unwrap_or(1));
        }
        if let (Some(process_group_id), Some(incarnation)) = (state.actor_id, state.incarnation) {
            process::terminate_process_group_actor(&ProcessGroupActor {
                process_group_id,
                incarnation,
            })
            .map_err(LifecycleError::from)?;
        }
        return Err(LifecycleError::ReconciliationRequired.into());
    }
    check_admission(spec.deadline_unix_ms)?;
    let mut state = LaunchState::prepared(digest);
    let native = match adapter.prepare(&launch_custody)? {
        Preparation::Native(native) => native,
        Preparation::Settled { events, terminal } => {
            return settle(spec, &launch_custody, state, events, terminal, writer)
                .map_err(Into::into);
        }
    };
    let NativeCommand {
        mut command,
        stdin,
        framing,
    } = native;
    command
        .command_mut()
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Err(error) = check_admission(spec.deadline_unix_ms) {
        adapter.discard(&launch_custody)?;
        return Err(error.into());
    }
    launch_custody
        .write_state(&state)
        .map_err(LifecycleError::from)?;
    if let Err(error) = check_admission(spec.deadline_unix_ms) {
        std::fs::remove_file(launch_custody.state_path()).map_err(LifecycleError::from)?;
        adapter.discard(&launch_custody)?;
        return Err(error.into());
    }
    let (child, release) = command.spawn().map_err(LifecycleError::from)?;
    let mut child = ChildGuard { child, live: true };
    let actor = process::actor_for_child(&child.child).map_err(LifecycleError::from)?;
    state.actor_id = Some(actor.process_group_id);
    state.incarnation = Some(actor.incarnation);
    state.phase = custody::PHASE_RUNNING.into();
    launch_custody
        .write_state(&state)
        .map_err(LifecycleError::from)?;
    let journal = launch_custody
        .create_journal()
        .map_err(LifecycleError::from)?;
    let mut sink = EventSink {
        events: LaunchEventWriter::new(writer, journal, spec.contract, spec.request_id),
        accounting: DataAccounting::default(),
    };
    let (send, receive) = mpsc::sync_channel(32);
    let stdout = child.child.stdout.take().expect("piped native stdout");
    let stderr = child.child.stderr.take().expect("piped native stderr");
    spawn_reader(Channel::Stdout, Box::new(stdout), framing, send.clone());
    spawn_reader(Channel::Stderr, Box::new(stderr), framing, send);
    if let Err(error) = check_admission(spec.deadline_unix_ms) {
        drop(release);
        child.terminate();
        std::fs::remove_file(launch_custody.journal_path()).map_err(LifecycleError::from)?;
        std::fs::remove_file(launch_custody.state_path()).map_err(LifecycleError::from)?;
        adapter.discard(&launch_custody)?;
        return Err(error.into());
    }
    release.release().map_err(LifecycleError::from)?;
    let input: Option<JoinHandle<io::Result<()>>> = stdin.map(|bytes| {
        let mut pipe = child.child.stdin.take().expect("piped native stdin");
        std::thread::spawn(move || pipe.write_all(&bytes))
    });
    adapter.started(&mut sink)?;

    let timing = spec.timing;
    let mut native_status = None;
    let mut stopped = None;
    let mut last_heartbeat = Instant::now();
    let mut last_native_event = Instant::now();
    let mut exited_at = None;
    let mut streams_closed_at = None;
    loop {
        if stopped.is_none() {
            if let Some(cause) = stop_requested(spec.deadline_unix_ms) {
                stopped = Some(cause);
                native_status = child.terminate();
                exited_at = Some(Instant::now());
            }
        }
        match receive.recv_timeout(timing.poll_interval) {
            Ok((channel, record)) => {
                last_native_event = Instant::now();
                let bytes = record.map_err(|()| LifecycleError::NativeStreamInvalid)?;
                adapter.output(channel, bytes, &mut sink)?;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                streams_closed_at.get_or_insert_with(Instant::now);
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
        if native_status.is_none() {
            native_status = child.child.try_wait().map_err(LifecycleError::from)?;
            if native_status.is_some() {
                // Once the leader exits, stop descendants but continue
                // draining buffered output; a busy drain has no lifetime cap.
                if child.live {
                    child.terminate();
                }
                exited_at = Some(Instant::now());
            }
        }
        if streams_closed_at.is_some() && native_status.is_some() {
            break;
        }
        if streams_closed_at.is_some_and(|time: Instant| time.elapsed() >= timing.drain_grace) {
            return Err(LifecycleError::NativeStreamsClosed.into());
        }
        if exited_at.is_some_and(|time: Instant| time.elapsed() > timing.drain_grace)
            && last_native_event.elapsed() > timing.drain_grace
        {
            return Err(LifecycleError::NativeDrainIncomplete.into());
        }
        if let Some(interval) = timing.heartbeat_interval {
            if last_heartbeat.elapsed() >= interval {
                sink.event(json!({"kind":"heartbeat"}))?;
                last_heartbeat = Instant::now();
            }
        }
    }
    let status = match native_status {
        Some(status) => status,
        None => child.terminate().ok_or(LifecycleError::WaitFailed)?,
    };
    // Finish process-group custody even if a native child inherited stream fds.
    if child.live {
        child.terminate();
    }
    if let Some(input) = input {
        let input_done = Instant::now();
        while !input.is_finished() && input_done.elapsed() < timing.drain_grace {
            std::thread::sleep(Duration::from_millis(10));
        }
        if !input.is_finished() {
            return Err(LifecycleError::InputStalled.into());
        }
        let delivered = input
            .join()
            .map_err(|_| LifecycleError::InputWriterFailed)?;
        if delivered.is_err() && status.success() && stopped.is_none() {
            return Err(LifecycleError::InputIncomplete.into());
        }
    }
    let terminal = adapter.finish(NativeOutcome { status, stopped }, &mut sink)?;
    complete(&launch_custody, state, sink, &terminal).map_err(Into::into)
}

fn settle<W: Write>(
    spec: &LaunchSpec<'_>,
    launch_custody: &RequestCustody,
    state: LaunchState,
    events: Vec<Value>,
    terminal: Terminal,
    writer: &mut W,
) -> Result<i32, LifecycleError> {
    // A crash after this point leaves prepared state without an actor, which
    // requires reconciliation like any other incomplete launch.
    launch_custody.write_state(&state)?;
    let journal = launch_custody.create_journal()?;
    let mut sink = EventSink {
        events: LaunchEventWriter::new(writer, journal, spec.contract, spec.request_id),
        accounting: DataAccounting::default(),
    };
    for event in events {
        sink.event(event)?;
    }
    complete(launch_custody, state, sink, &terminal)
}

fn complete<W: Write>(
    launch_custody: &RequestCustody,
    mut state: LaunchState,
    mut sink: EventSink<'_, W>,
    terminal: &Terminal,
) -> Result<i32, LifecycleError> {
    sink.exit(terminal)?;
    let receipt = sink.events.seal()?;
    state.journal_sha256 = Some(receipt.sha256);
    state.journal_len = Some(receipt.len);
    state.phase = custody::PHASE_COMPLETE.into();
    state.exit_code = Some(terminal.exit_code);
    state.actor_id = None;
    state.incarnation = None;
    launch_custody.write_state(&state)?;
    Ok(terminal.exit_code)
}
