//! Durable native-process admission and process-group custody.
//!
//! A native command starts behind an effect gate: the provider re-executes its
//! own binary with a gate argument and an inherited descriptor, and the gate
//! does not `exec` the native program until the caller has durably published
//! the process-group actor and released the gate. Publication before release
//! is the caller's obligation, not enforced by the gate. The native command
//! leads its own process group; Linux parent-death `SIGKILL` applies to the
//! leader, not automatically to every descendant. Durable recovery checks a
//! boot-scoped start-time incarnation and skips a changed live leader. The
//! observation and signal are separate syscalls, so this is not atomic
//! protection against process-group recycling.
//!
//! The provider binary chooses its gate argument and descriptor variable and
//! dispatches the gate before any other argument handling, through
//! [`run_effect_gate`].
//!
//! The gate descriptor also carries the native start outcome back to the
//! caller. The gate marks it close-on-exec before `exec`, so a successful
//! `exec` closes it without data; a failed `exec` writes the native `errno`
//! to it before the gate exits 126. [`ExecStart`] reports which one the
//! caller observed. This is the actual exec boundary, not a prediction from
//! `PATH` or permission bits, and it does not interpret native exit statuses:
//! a native program that runs and exits 126 reports no start failure. A gate
//! from a build without this report closes the descriptor before `exec`, and a
//! caller that drops the observer makes the report undeliverable; both reduce
//! to the earlier behavior of a gate exit status with a stderr diagnostic.
//! The caller retains a cleanup guard, terminates/reaps the child on errors,
//! and owns durable recovery/reconciliation if its in-process owner is lost.

use std::ffi::OsStr;
#[cfg(target_os = "linux")]
use std::fs;
use std::io;
#[cfg(unix)]
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus};
#[cfg(unix)]
use std::time::Duration;
#[cfg(unix)]
use std::{os::fd::AsRawFd, os::unix::net::UnixStream};

#[cfg(unix)]
const TERMINATION_GRACE: Duration = Duration::from_millis(100);

/// How a provider binary re-enters itself as the native effect gate.
#[derive(Clone, Copy, Debug)]
pub struct EffectGate<'a> {
    /// Provider executable that dispatches `argument` to [`run_effect_gate`].
    pub executable: &'a Path,
    /// First argument that selects the gate in the provider binary.
    pub argument: &'a str,
    /// Environment variable carrying the inherited gate descriptor number.
    pub descriptor_env: &'a str,
}

/// Durable identity of a native process group: its leader PID and the leader's
/// boot-scoped start incarnation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessGroupActor {
    pub process_group_id: u32,
    pub incarnation: String,
}

/// Write side of the effect gate; the native command runs only after
/// [`ExecGate::release`].
pub struct ExecGate {
    #[cfg(unix)]
    writer: UnixStream,
}

/// Caller side of the gate after release, which receives the native start
/// outcome from [`run_effect_gate`].
pub struct ExecObserver {
    #[cfg(unix)]
    reader: UnixStream,
    #[cfg(unix)]
    report: Vec<u8>,
}

/// Native start outcome observed at the gate's `exec`.
#[derive(Debug)]
pub enum ExecStart {
    /// The gate descriptor closed without a report: `exec` replaced the gate,
    /// or the gate ended before attempting it (for example, it was killed).
    NoFailureReported,
    /// `exec` failed with this error, so the native program never ran.
    Failed(io::Error),
}

const EXEC_REPORT_LEN: usize = std::mem::size_of::<i32>();

/// A native command wrapped by the provider's effect gate and configured to lead
/// its own process group.
#[cfg(unix)]
pub struct GatedCommand {
    command: Command,
    writer: UnixStream,
    inherited_gate: UnixStream,
    descriptor_env: String,
}

#[cfg(not(unix))]
pub struct GatedCommand;

#[cfg(unix)]
impl GatedCommand {
    pub fn new<I, S>(gate: &EffectGate<'_>, program: impl AsRef<OsStr>, args: I) -> io::Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        use std::os::unix::process::CommandExt;

        let (writer, inherited_gate) = UnixStream::pair()?;
        let inherited_gate_fd = inherited_gate.as_raw_fd();
        let retained_gate = inherited_gate.try_clone()?;
        let mut command = Command::new(gate.executable);
        command.arg(gate.argument).arg(program).args(args);
        unsafe {
            command.pre_exec(move || {
                let _keep_gate_open = &retained_gate;
                let flags = libc::fcntl(inherited_gate_fd, libc::F_GETFD);
                if flags == -1
                    || libc::fcntl(inherited_gate_fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC)
                        == -1
                {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        Ok(Self {
            command,
            writer,
            inherited_gate,
            descriptor_env: gate.descriptor_env.to_string(),
        })
    }

    pub fn command_mut(&mut self) -> &mut Command {
        &mut self.command
    }

    pub fn spawn(mut self) -> io::Result<(Child, ExecGate)> {
        configure_process_group(&mut self.command);
        self.command.env(
            &self.descriptor_env,
            self.inherited_gate.as_raw_fd().to_string(),
        );
        let child = self.command.spawn()?;
        drop(self.inherited_gate);
        Ok((
            child,
            ExecGate {
                writer: self.writer,
            },
        ))
    }
}

#[cfg(not(unix))]
impl GatedCommand {
    pub fn new<I, S>(
        _gate: &EffectGate<'_>,
        _program: impl AsRef<OsStr>,
        _args: I,
    ) -> io::Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "native commands require Unix process-group custody",
        ))
    }

    pub fn command_mut(&mut self) -> &mut Command {
        unreachable!("unsupported native gated command cannot be configured")
    }

    pub fn spawn(self) -> io::Result<(Child, ExecGate)> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "native commands require Unix process-group custody",
        ))
    }
}

impl ExecGate {
    /// Lets the gate `exec` the native program. Keep the returned observer to
    /// learn whether `exec` failed; dropping it discards that report.
    #[cfg(unix)]
    pub fn release(mut self) -> io::Result<ExecObserver> {
        self.writer.write_all(&[1])?;
        self.writer.flush()?;
        Ok(ExecObserver {
            reader: self.writer,
            report: Vec::with_capacity(EXEC_REPORT_LEN),
        })
    }

    #[cfg(not(unix))]
    pub fn release(self) -> io::Result<ExecObserver> {
        Ok(ExecObserver {})
    }
}

impl ExecObserver {
    /// Waits up to `timeout` for the native start outcome; `None` means the
    /// gate has neither executed nor reported a failure yet.
    #[cfg(unix)]
    pub fn wait(&mut self, timeout: Duration) -> io::Result<Option<ExecStart>> {
        self.reader
            .set_read_timeout(Some(timeout.max(Duration::from_millis(1))))?;
        let mut buffer = [0_u8; EXEC_REPORT_LEN];
        loop {
            let wanted = EXEC_REPORT_LEN - self.report.len();
            match self.reader.read(&mut buffer[..wanted]) {
                Ok(0) if self.report.is_empty() => return Ok(Some(ExecStart::NoFailureReported)),
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "native effect gate sent a truncated start report",
                    ))
                }
                Ok(count) => {
                    self.report.extend_from_slice(&buffer[..count]);
                    if self.report.len() == EXEC_REPORT_LEN {
                        let errno = i32::from_ne_bytes(
                            self.report[..].try_into().expect("complete start report"),
                        );
                        return Ok(Some(ExecStart::Failed(io::Error::from_raw_os_error(errno))));
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                    ) =>
                {
                    return Ok(None)
                }
                Err(error) => return Err(error),
            }
        }
    }

    #[cfg(not(unix))]
    pub fn wait(&mut self, _timeout: std::time::Duration) -> io::Result<Option<ExecStart>> {
        Ok(Some(ExecStart::NoFailureReported))
    }
}

/// Runs the effect gate inside the provider binary. `args` is the provider's
/// complete argv: `[provider, gate argument, native program, native args...]`.
/// Returns 126 when the gate cannot release or `exec` fails; otherwise the
/// process image is replaced by the native program. A failed `exec` is
/// reported through the gate descriptor (see [`ExecObserver`]) and also
/// written to stderr as a diagnostic.
#[cfg(unix)]
pub fn run_effect_gate(args: &[String], descriptor_env: &str) -> i32 {
    use std::fs::File;
    use std::os::fd::FromRawFd;
    use std::os::unix::process::CommandExt;

    let Some((program, program_args)) = args.get(2..).and_then(|args| args.split_first()) else {
        eprintln!("native effect gate is missing its command");
        return 126;
    };
    let gate_fd = match std::env::var(descriptor_env)
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        .filter(|value| *value >= 3)
    {
        Some(gate_fd) => gate_fd,
        None => {
            eprintln!("native effect gate has no valid inherited gate descriptor");
            return 126;
        }
    };
    let mut gate = unsafe { File::from_raw_fd(gate_fd) };
    let mut release = [0_u8; 1];
    if let Err(error) = gate.read_exact(&mut release) {
        eprintln!("native effect gate closed before actor publication: {error}");
        return 126;
    }
    if release != [1] {
        eprintln!("native effect gate received an invalid release token");
        return 126;
    }
    // Keep the descriptor across the attempt: close-on-exec closes it when
    // `exec` succeeds, and it carries the error back when `exec` fails.
    let flags = unsafe { libc::fcntl(gate_fd, libc::F_GETFD) };
    if flags == -1 || unsafe { libc::fcntl(gate_fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } == -1
    {
        eprintln!(
            "native effect gate could not protect its descriptor: {}",
            io::Error::last_os_error()
        );
        return 126;
    }
    std::env::remove_var(descriptor_env);
    let error = Command::new(program).args(program_args).exec();
    let report = error.raw_os_error().unwrap_or(libc::EIO).to_ne_bytes();
    // `exec` restored the default `SIGPIPE` disposition before failing, and a
    // caller that dropped its observer has closed the other end: report
    // without raising `SIGPIPE` so the gate still exits 126 below.
    unsafe {
        libc::send(
            gate_fd,
            report.as_ptr().cast(),
            report.len(),
            libc::MSG_NOSIGNAL,
        );
    }
    drop(gate);
    eprintln!("native effect gate could not execute native command: {error}");
    126
}

#[cfg(not(unix))]
pub fn run_effect_gate(_args: &[String], _descriptor_env: &str) -> i32 {
    eprintln!("native effect gate requires Unix process-group custody");
    126
}

/// Captures the durable actor identity of a spawned process-group leader.
pub fn actor_for_child(child: &Child) -> io::Result<ProcessGroupActor> {
    let process_group_id = child.id();
    Ok(ProcessGroupActor {
        process_group_id,
        incarnation: process_group_incarnation(process_group_id)?,
    })
}

/// Reports whether the recorded actor has ended or its number was recycled.
pub fn actor_is_terminal_or_recycled(actor: &ProcessGroupActor) -> io::Result<bool> {
    if !process_group_is_live(actor.process_group_id) {
        return Ok(true);
    }
    match process_group_incarnation(actor.process_group_id) {
        Ok(incarnation) => Ok(incarnation != actor.incarnation),
        Err(_) if !process_group_is_live(actor.process_group_id) => Ok(true),
        Err(error) => Err(error),
    }
}

/// Discharge custody for a durably recorded process-group incarnation after
/// its in-process owner has been lost. A changed leader incarnation proves the
/// numeric process-group identity was recycled and must not be signalled. A
/// missing leader while the group remains live is the original orphaned group:
/// a new group with that number cannot exist without a leader whose PID equals
/// the PGID.
///
/// Only PGIDs `2..=i32::MAX` are accepted. Zero and one would produce reserved
/// kill selectors `0` (caller's group) and `-1` (all permitted processes);
/// both and out-of-range PGIDs return `InvalidInput` before any group probe or
/// signal. This guard applies to recovery, not the other public process helpers.
/// Valid actors must come from the caller's own durable custody. The
/// incarnation check and subsequent signal are not atomic; recovery does not
/// eliminate the inherited check-then-signal race.
#[cfg(unix)]
pub fn terminate_process_group_actor(actor: &ProcessGroupActor) -> io::Result<()> {
    let pgid = recovery_process_group_id(actor.process_group_id)?;
    if !process_group_actor_requires_signal(actor)? {
        return Ok(());
    }
    send_process_group_signal_checked(-pgid, SIGTERM)?;
    std::thread::sleep(TERMINATION_GRACE);
    if !process_group_actor_requires_signal(actor)? {
        return Ok(());
    }
    send_process_group_signal_checked(-pgid, SIGKILL)?;
    for _ in 0..10 {
        if !process_group_actor_requires_signal(actor)? {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    if process_group_actor_requires_signal(actor)? {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "native process group remains live after termination",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn recovery_process_group_id(process_group_id: u32) -> io::Result<i32> {
    let pgid = i32::try_from(process_group_id).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "native process-group identity exceeds the platform PID range",
        )
    })?;
    if pgid <= 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "native process-group identity must exceed one to avoid reserved kill selectors",
        ));
    }
    Ok(pgid)
}

#[cfg(unix)]
fn process_group_actor_requires_signal(actor: &ProcessGroupActor) -> io::Result<bool> {
    if !process_group_is_live(actor.process_group_id) {
        return Ok(false);
    }
    match process_group_incarnation(actor.process_group_id) {
        Ok(incarnation) => Ok(incarnation == actor.incarnation),
        Err(_) if !process_group_is_live(actor.process_group_id) => Ok(false),
        Err(error) if process_group_leader_is_missing(&error) => Ok(true),
        Err(error) => Err(error),
    }
}

#[cfg(not(unix))]
pub fn terminate_process_group_actor(_actor: &ProcessGroupActor) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "durable process-group recovery requires Unix process custody",
    ))
}

#[cfg(target_os = "linux")]
fn process_group_leader_is_missing(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::NotFound
}

#[cfg(all(unix, not(target_os = "linux")))]
fn process_group_leader_is_missing(error: &io::Error) -> bool {
    error.raw_os_error() == Some(libc::ESRCH)
}

/// Terminates a live child's whole process group (`SIGTERM`, grace, `SIGKILL`)
/// and reaps the leader.
#[cfg(unix)]
pub fn terminate_process_group_child(child: &mut Child) -> Option<ExitStatus> {
    let pgid = -(child.id() as i32);
    send_process_group_signal(pgid, SIGTERM);
    std::thread::sleep(TERMINATION_GRACE);
    send_process_group_signal(pgid, SIGKILL);
    child.wait().ok()
}

#[cfg(not(unix))]
pub fn terminate_process_group_child(child: &mut Child) -> Option<ExitStatus> {
    let _ = child.kill();
    child.wait().ok()
}

/// Locates the provider binary named `binary_name` from the current process:
/// the current executable itself, or a sibling (one level above Cargo's `deps`
/// directory for test binaries).
pub fn locate_provider_executable(binary_name: &str) -> io::Result<PathBuf> {
    let current = std::env::current_exe()?;
    if current.file_name().and_then(|name| name.to_str()) == Some(binary_name) {
        return Ok(current);
    }
    let Some(parent) = current.parent() else {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "provider executable has no containing directory",
        ));
    };
    let candidate = if parent.file_name().and_then(|name| name.to_str()) == Some("deps") {
        parent.parent().unwrap_or(parent).join(binary_name)
    } else {
        parent.join(binary_name)
    };
    candidate.is_file().then_some(candidate).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "could not locate the provider native-command gate executable",
        )
    })
}

/// Makes the command lead a new process group and, on Linux, receive `SIGKILL`
/// when its parent dies. Spawning fails if the parent already exited.
#[cfg(target_os = "linux")]
pub fn configure_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;
    let parent_pid = unsafe { getpid() };
    unsafe {
        command.pre_exec(move || set_current_process_group_with_parent_death(parent_pid));
    }
}

#[cfg(target_os = "linux")]
fn set_current_process_group_with_parent_death(parent_pid: i32) -> io::Result<()> {
    set_current_process_group()?;
    if unsafe { prctl(PR_SET_PDEATHSIG, SIGKILL, 0, 0, 0) } == -1 {
        return Err(io::Error::last_os_error());
    }
    if unsafe { getppid() } != parent_pid {
        return Err(io::Error::other(
            "provider parent exited before child custody was established",
        ));
    }
    Ok(())
}

#[cfg(all(unix, not(target_os = "linux")))]
pub fn configure_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;
    unsafe {
        command.pre_exec(set_current_process_group);
    }
}

#[cfg(unix)]
fn set_current_process_group() -> io::Result<()> {
    if unsafe { setpgid(0, 0) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Boot-scoped start-time identity of a process.
#[cfg(target_os = "linux")]
pub fn process_group_incarnation(process_id: u32) -> io::Result<String> {
    require_matching_proc_view()?;
    let stat = fs::read_to_string(format!("/proc/{process_id}/stat"))?;
    let command_end = stat.rfind(')').ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "process stat has no command terminator",
        )
    })?;
    let start_ticks = stat[command_end + 1..]
        .split_whitespace()
        .nth(19)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "process stat has no start-time field",
            )
        })?
        .parse::<u64>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let boot_id = fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
    let boot_id = boot_id.trim();
    if boot_id.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "kernel boot identity is empty",
        ));
    }
    Ok(format!("linux:{boot_id}:{start_ticks}"))
}

/// `/proc/self/status` lists NSpid from the proc mount's PID namespace down
/// to ours. A matching view has exactly our one PID. Checking in this process
/// avoids attributing a helper's NSpid to us. Do this before numeric lookups;
/// refusal is not ENOENT (which recovery interprets as a missing leader).
#[cfg(target_os = "linux")]
fn require_matching_proc_view() -> io::Result<()> {
    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "proc view does not identify the caller's PID namespace",
        )
    };
    let own_pid = std::process::id();
    let self_pid = fs::read_link("/proc/self").map_err(|_| invalid())?;
    let status = fs::read_to_string("/proc/self/status").map_err(|_| invalid())?;
    let mut pids = status
        .lines()
        .find_map(|line| line.strip_prefix("NSpid:"))
        .ok_or_else(invalid)?
        .split_whitespace();
    if self_pid.to_str().and_then(|pid| pid.parse::<u32>().ok()) != Some(own_pid)
        || pids.next().and_then(|pid| pid.parse::<u32>().ok()) != Some(own_pid)
        || pids.next().is_some()
    {
        return Err(invalid());
    }
    Ok(())
}

#[cfg(target_os = "macos")]
pub fn process_group_incarnation(process_id: u32) -> io::Result<String> {
    let process_id = i32::try_from(process_id).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "process identity exceeds the platform pid range",
        )
    })?;
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
    let info_size = i32::try_from(std::mem::size_of::<libc::proc_bsdinfo>()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "process identity structure exceeds the platform query range",
        )
    })?;
    let read_size = unsafe {
        libc::proc_pidinfo(
            process_id,
            libc::PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            info_size,
        )
    };
    if read_size <= 0 {
        return Err(io::Error::last_os_error());
    }
    if read_size != info_size {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "process identity query returned a partial record",
        ));
    }
    let info = unsafe { info.assume_init() };
    if info.pbi_pid != process_id as u32 || info.pbi_pgid != process_id as u32 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "native actor is not the leader of its registered process group",
        ));
    }
    Ok(format!(
        "macos:{}:{}",
        info.pbi_start_tvsec, info.pbi_start_tvusec
    ))
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
pub fn process_group_incarnation(_process_id: u32) -> io::Result<String> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "durable native actor incarnation is unsupported on this Unix platform",
    ))
}

#[cfg(not(unix))]
pub fn process_group_incarnation(_process_id: u32) -> io::Result<String> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "durable native actor incarnation requires Unix process custody",
    ))
}

#[cfg(unix)]
pub fn process_group_is_live(process_group_id: u32) -> bool {
    let Ok(process_group_id) = i32::try_from(process_group_id) else {
        return true;
    };
    if unsafe { kill(-process_group_id, 0) } == 0 {
        return true;
    }
    io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

#[cfg(not(unix))]
pub fn process_group_is_live(_process_group_id: u32) -> bool {
    true
}

#[cfg(unix)]
fn send_process_group_signal(pgid: i32, signal: i32) {
    unsafe {
        let _ = kill(pgid, signal);
    }
}

#[cfg(unix)]
fn send_process_group_signal_checked(pgid: i32, signal: i32) -> io::Result<()> {
    if unsafe { kill(pgid, signal) } == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        return Ok(());
    }
    Err(error)
}

#[cfg(unix)]
const SIGTERM: i32 = 15;
#[cfg(unix)]
const SIGKILL: i32 = 9;
#[cfg(target_os = "linux")]
const PR_SET_PDEATHSIG: i32 = 1;

#[cfg(unix)]
unsafe extern "C" {
    fn setpgid(pid: i32, pgid: i32) -> i32;
    fn kill(pid: i32, sig: i32) -> i32;
}

#[cfg(target_os = "linux")]
unsafe extern "C" {
    fn getpid() -> i32;
    fn getppid() -> i32;
    fn prctl(option: i32, arg2: i32, arg3: usize, arg4: usize, arg5: usize) -> i32;
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn recovery_process_group_id_rejects_reserved_selectors_and_overflow() {
        // Pure validation only: never pass PGID one to a probe or recovery call.
        for id in [0, 1, i32::MAX as u32 + 1, u32::MAX] {
            let error = recovery_process_group_id(id).expect_err("invalid recovery identity");
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "PGID {id}");
        }
        for id in [2, i32::MAX as u32] {
            let pgid = recovery_process_group_id(id).expect("ordinary group selector");
            assert_eq!(pgid as u32, id);
            assert!(-pgid < -1);
        }
    }

    #[test]
    fn durable_actor_recovery_terminates_a_group_after_its_leader_exits() {
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("sleep 30 </dev/null >/dev/null 2>&1 & exit 0");
        configure_process_group(&mut command);
        let mut child = command.spawn().expect("spawn orphanable process group");
        let actor = actor_for_child(&child).expect("capture durable actor identity");
        child.wait().expect("process-group leader exits");
        assert!(
            process_group_is_live(actor.process_group_id),
            "background descendant retains the original process group"
        );

        terminate_process_group_actor(&actor).expect("terminate orphaned process group");
        assert!(!process_group_is_live(actor.process_group_id));
    }
}
