//! Resident ACP v2 session endpoint over the shared launch lifecycle.
//!
//! [`serve`] is the agent side of the Agent Client Protocol v2 draft subset
//! that Agent Runner's root supervisor consumes (schema tag
//! [`ACP_SCHEMA_TAG`]), spoken as newline-delimited JSON-RPC on one
//! connection. It keeps provider-native sessions resident across turns: one
//! process serves `session/new`, `session/resume`, `session/prompt`,
//! `session/cancel`, `session/close` and `session/list` until its connection
//! closes. Every native turn is one `oulipoly.provider/v1` launch run through
//! [`crate::lifecycle::run_launch_until`] by the adapter's [`ResidentTurns`]
//! implementation, so request custody, the seven-field launch state, the
//! effect gate, process-group custody, draining, exact replay and
//! reconciliation of an interrupted turn are the shared one-shot lifecycle's,
//! not a second loop. The adapter owns native argv, authentication, models,
//! tools and the translation of native output into launch events.
//!
//! # Wire meaning
//!
//! * `initialize` answers protocol version 2 (the only one served),
//!   `capabilities.session = {}` and, in `_meta`, the message-key dedup
//!   contract ([`DEDUP_CONTRACT_META`] version 1) and
//!   [`RESIDENT_SESSION_META`] naming [`RESIDENT_SESSION_PROTOCOL`].
//! # Directory publication precondition
//!
//! All durable-session/input/insertion/ACK claims below require a successfully
//! published incoming state-root lineage. `serve` publishes links it creates;
//! it does not certify pre-existing or host-created links. Current per-call use
//! uses fresh roots and discards failed publication; diagnostic keep is inspection.
//! A same-path initialize/new/resume success or ACK after an earlier publication
//! failure is not a receipt for that failure, even after a later successful sync.
//! The wire may still succeed there: this precondition is not runtime-enforced.
//! Deliberate recovery must name the roots and lineage it relies on and establish
//! its required guarantee at the consumer boundary. A consumer unable to do so
//! uses a fresh root and carries prior uncertainty as do-not-replay. No general
//! recovery mechanism or hardware-crash durability qualification is supplied.
//!
//! * `session/new` creates a provider-private durable session record under
//!   the state root and answers its id. The native session id is learned from
//!   the first turn's `oulipoly.provider_session` marker, recorded, and resumed
//!   by later turns. An adapter-chosen create id is only a candidate until
//!   observed in that marker. Interrupted work without a known native identity
//!   blocks new input and reports uncertainty; actor discharge is not proof
//!   that no native work happened.
//! * `session/prompt` accepts text content only. The input is durably
//!   recorded with a fresh ascending `messageId` before its native turn
//!   starts. Turns of one session run one at a time in arrival order. The
//!   prompt is answered only when the native turn reports that it consumed
//!   the input (`oulipoly.submitted_user_turn` marker): that is the insertion
//!   acknowledgement, recorded durably before `user_message`, then the ACK and
//!   `state_update: running`. It is not turn completion.
//!   A completed non-consuming turn or a justified pre-start refusal records
//!   non-insertion. Unavailable settlement evidence retains uncertainty.
//! * Each native `stdout` data event of a consumed turn becomes one
//!   `agent_message` tagged with [`PARENT_MESSAGE_META`] = that input. The end
//!   of a consumed turn is one `state_update: idle` tagged with
//!   [`TURN_INPUT_META`] = that input, a stop reason (`end_turn`, `cancelled`,
//!   or `_oulipoly_*` for a native failure, interrupted custody or an
//!   incomplete lifecycle) and [`NATIVE_TURN_META`]: the launch request id,
//!   exit status and terminal signal, the launch-output accounting when
//!   reported, and the custody state.
//! * A prompt whose [`MESSAGE_KEY_META`] key this session already recorded
//!   inserts nothing: it answers the original `messageId` with
//!   [`DUPLICATE_META`] `true` once insertion is known, and an input whose
//!   turn already ended gets its recorded tagged idle again. Key identity does
//!   not attest the resubmission's bytes (outputs are not
//!   re-sent). This memory lasts as long as the session's durable record.
//! * `session/cancel` stops the running turn's native process group through
//!   its lifecycle stop flag and refuses inputs queued before it; a consumed
//!   turn still ends with its tagged idle (`cancelled`). `session/close`
//!   cancels, settles the running turn, releases ownership and then answers.
//! * `session/resume` opens a recorded session in this process (its working
//!   directory must match) and first settles inputs an earlier process left
//!   unfinished without native readmission: a complete equal-request journal
//!   replays without native effects, while an interrupted one has its
//!   recorded process group discharged before adapter validation and is reported as
//!   `_oulipoly_reconciliation_required`, never run again. An input whose
//!   consumption is unknown is refused rather than rerun.
//!
//! Connection end (input EOF) or a recorded `SIGTERM`/`SIGINT` stops every
//! running turn, refuses queued inputs and joins workers. Unresolved session
//! launch custody returns an I/O error, including after a refused close. Input
//! uncertainty survives physical actor settlement and successful close/EOF.
//! Each session
//! is held by one process at a time through an exclusive lock. Logical
//! session ancestry, admission, scheduling and delivery policy remain the
//! host's; this endpoint keeps only the provider-native session record its
//! own resume and dedup promises need.

use crate::cancellation;
use crate::custody;
use crate::durable_fs::{create_private_directories, sync_directory};
use crate::encoding::{decode_base64, now_unix_ms, sha256_hex};
use agent_provider_contract::{acp, resident_session};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

/// The only ACP protocol version served.
pub const ACP_PROTOCOL_VERSION: u64 = resident_session::ACP_PROTOCOL_VERSION;
/// Release tag of the ACP v2 draft schema whose subset is served.
pub const ACP_SCHEMA_TAG: &str = acp::pin::SCHEMA_TAG;
/// Resident session contract named in `initialize` `_meta`.
pub const RESIDENT_SESSION_PROTOCOL: &str = resident_session::PROTOCOL;
/// `_meta` key carrying the sender's message key on a prompt and its echo.
pub const MESSAGE_KEY_META: &str = acp::MESSAGE_KEY_META;
/// `_meta` key advertising the message-key dedup contract in `initialize`.
pub const DEDUP_CONTRACT_META: &str = acp::DEDUP_CONTRACT_META;
/// `_meta` key on a prompt response returning an earlier insertion.
pub const DUPLICATE_META: &str = acp::DUPLICATE_META;
/// `_meta` key on an `agent_message`: the input it answers.
pub const PARENT_MESSAGE_META: &str = acp::PARENT_MESSAGE_META;
/// `_meta` key on an idle `state_update`: the input whose turn ended.
pub const TURN_INPUT_META: &str = acp::TURN_INPUT_META;
/// `_meta` key on `initialize`: the resident session contract served.
pub const RESIDENT_SESSION_META: &str = acp::RESIDENT_SESSION_META;
/// `_meta` key on an idle `state_update`: the native turn's outcome.
pub const NATIVE_TURN_META: &str = acp::NATIVE_TURN_META;
/// Launch marker naming the native session a turn ran in.
pub const PROVIDER_SESSION_MARKER: &str = "oulipoly.provider_session";
/// Launch marker reporting that the native turn consumed its input.
pub const SUBMITTED_USER_TURN_MARKER: &str = "oulipoly.submitted_user_turn";
/// Launch marker carrying the launch-output accounting.
pub const LAUNCH_OUTPUT_COMPLETE_MARKER: &str = "oulipoly.launch_output_complete/v1";

use acp::code::{
    INPUT_NOT_INSERTED, INPUT_UNCERTAIN, INVALID_PARAMS, METHOD_NOT_FOUND, NOT_INITIALIZED,
    PARSE_ERROR, SESSION_UNAVAILABLE,
};
const MAX_SESSION_RECORD_BYTES: usize = 64 * 1024;
const MAX_INPUT_RECORD_BYTES: usize = 16 * 1024 * 1024;
// Leave room for dispatch, insertion and terminal metadata in the recovery record.
const MAX_ACCEPTED_INPUT_BYTES: usize = 8 * 1024 * 1024;
const MAX_WIRE_LINE_BYTES: usize = 32 * 1024 * 1024;
const MAX_JOURNAL_RECORD_BYTES: usize = 16 * 1024 * 1024;

/// One native turn the adapter runs as one provider/v1 launch.
#[derive(Clone, Debug)]
pub struct TurnRequest {
    /// Resident session id answered to the client.
    pub session_id: String,
    /// The input's `messageId`.
    pub message_id: String,
    /// Launch request id; equal requests replay or reconcile. Run the launch
    /// with this request id, no provider instance id and [`Self::state_root`]
    /// so the endpoint can tell whether an earlier process recorded it.
    pub request_id: String,
    /// Existing provider-private launch state root for this session's turns.
    pub state_root: PathBuf,
    /// Absolute working directory of the session.
    pub cwd: PathBuf,
    /// Prompt text of the input.
    pub prompt: String,
    /// Known native session to continue; `None` starts one.
    pub native_session_id: Option<String>,
    /// Adapter-chosen id for a native session this turn creates.
    pub create_native_session_id: Option<String>,
}

/// How a turn's lifecycle failed before it delivered its `exit` event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TurnFailureKind {
    /// The stop flag or a termination signal refused admission: nothing ran.
    Cancelled,
    /// An earlier launch of this request was interrupted; its recorded actor
    /// was discharged and nothing ran now.
    ReconciliationRequired,
    /// Any other failure; custody may be incomplete.
    Failed,
}

/// Adapter report of a lifecycle failure.
#[derive(Clone, Debug)]
pub struct TurnFailure {
    pub kind: TurnFailureKind,
    pub code: String,
    pub message: String,
}

/// Native plug points of the resident endpoint.
pub trait ResidentTurns: Send + Sync + 'static {
    /// `name` and `version` reported in `initialize`.
    fn implementation(&self) -> (String, String);

    /// A candidate id for creating the session's first native turn. It becomes
    /// known only through the `oulipoly.provider_session` marker; choosing it
    /// does not authorize a recovery probe or prove native creation. `None`
    /// lets the native program choose and report its id.
    fn create_native_session_id(&self) -> Option<String> {
        None
    }

    /// Runs `turn` as one launch through [`crate::lifecycle::run_launch_until`] with
    /// `stop`, writing its NDJSON launch events to `events`. The launch must
    /// emit `oulipoly.submitted_user_turn` once the native program consumed
    /// the prompt, `oulipoly.provider_session` naming its native session, and
    /// one `stdout` data event per agent message; other data is accounted
    /// but not forwarded. Return the lifecycle's exit code, or classify its
    /// failure.
    fn run_turn(
        &self,
        turn: &TurnRequest,
        stop: &AtomicBool,
        events: &mut dyn Write,
    ) -> Result<i32, TurnFailure>;
}

/// Why [`serve`] returned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServeEnd {
    /// The client closed the connection.
    ConnectionClosed,
    /// A termination signal was recorded.
    Terminated(i32),
}

/// Serves one ACP v2 connection until it closes or the process is asked to
/// terminate. `state_root` must be a trusted provider-private directory; it
/// is created if missing. Durable claims require the module's successfully
/// published incoming lineage; same-path success does not certify prior failed
/// publication. This is a guarantee precondition, not an enforced startup check.
/// Returns after joining every session worker. Unresolved
/// session custody, including an earlier refused close, returns an I/O error;
/// success does not clear earlier input/insertion uncertainty.
pub fn serve<T, R, W>(turns: Arc<T>, state_root: &Path, input: R, output: W) -> io::Result<ServeEnd>
where
    T: ResidentTurns,
    R: BufRead + Send + 'static,
    W: Write + Send + 'static,
{
    cancellation::install_termination_handlers();
    let sessions_root = state_root.join("sessions");
    create_private_directories(&sessions_root)?;
    let wire = Wire::new(output);
    let (lines, receive) = mpsc::sync_channel::<io::Result<Option<String>>>(1);
    std::thread::spawn(move || read_lines(input, lines));
    let mut endpoint = Endpoint {
        turns,
        sessions_root,
        wire: wire.clone(),
        initialized: false,
        sessions: HashMap::new(),
        unsettled_closed: std::collections::HashSet::new(),
    };
    let end = loop {
        if let Some(signal) = cancellation::termination_signal() {
            break ServeEnd::Terminated(signal);
        }
        match receive.recv_timeout(Duration::from_millis(100)) {
            Ok(Ok(Some(line))) => endpoint.dispatch(&line),
            Ok(Err(error)) => {
                wire.error(&Value::Null, INVALID_PARAMS, &error.to_string(), None);
                break ServeEnd::ConnectionClosed;
            }
            Ok(Ok(None)) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                break ServeEnd::ConnectionClosed
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
    };
    endpoint.shutdown()?;
    Ok(end)
}

// A bounded record reader shared by ACP ingress and interrupted evidence.
// A partial record is returned at EOF so callers can distinguish it from absence.
fn read_record<R: BufRead>(input: &mut R, maximum: usize) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    loop {
        let available = input.fill_buf()?;
        if available.is_empty() {
            return Ok(bytes);
        }
        let count = available
            .iter()
            .position(|b| *b == b'\n')
            .map_or(available.len(), |i| i + 1);
        if count > maximum.saturating_sub(bytes.len()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "record exceeds declared byte bound",
            ));
        }
        bytes.extend_from_slice(&available[..count]);
        input.consume(count);
        if bytes.last() == Some(&b'\n') {
            return Ok(bytes);
        }
    }
}

fn read_lines<R: BufRead>(mut input: R, lines: mpsc::SyncSender<io::Result<Option<String>>>) {
    loop {
        let line = read_record(&mut input, MAX_WIRE_LINE_BYTES).and_then(|bytes| {
            if bytes.is_empty() {
                Ok(None)
            } else {
                String::from_utf8(bytes)
                    .map(Some)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
            }
        });
        let end = !matches!(line, Ok(Some(_)));
        if lines.send(line).is_err() || end {
            return;
        }
    }
}

// ---- wire -------------------------------------------------------------------

/// Serialized writer of one JSON message per line; a failed write closes it.
#[derive(Clone)]
struct Wire {
    output: Arc<Mutex<Option<Box<dyn Write + Send>>>>,
}

impl Wire {
    fn new(output: impl Write + Send + 'static) -> Self {
        Self {
            output: Arc::new(Mutex::new(Some(Box::new(output)))),
        }
    }

    fn send(&self, message: &Value) {
        let mut line = message.to_string();
        line.push('\n');
        let mut output = self.output.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(writer) = output.as_mut() {
            if writer
                .write_all(line.as_bytes())
                .and_then(|()| writer.flush())
                .is_err()
            {
                *output = None;
            }
        }
    }

    fn result(&self, id: &Value, result: Value) {
        self.send(&json!({"jsonrpc":"2.0","id":id,"result":result}));
    }

    fn error(&self, id: &Value, code: i64, message: &str, data: Option<Value>) {
        let mut error = json!({"code":code,"message":message});
        if let Some(data) = data {
            error["data"] = data;
        }
        self.send(&json!({"jsonrpc":"2.0","id":id,"error":error}));
    }

    fn update(&self, session_id: &str, update: Value) {
        self.send(&json!({"jsonrpc":"2.0","method":"session/update",
            "params":{"sessionId":session_id,"update":update}}));
    }
}

// ---- durable records --------------------------------------------------------

fn publish_json(directory: &Path, name: &str, value: &Value) -> io::Result<()> {
    let mut file = tempfile::NamedTempFile::new_in(directory)?;
    let maximum = if name == "session.json" {
        MAX_SESSION_RECORD_BYTES
    } else {
        MAX_INPUT_RECORD_BYTES
    };
    serde_json::to_writer(
        BoundedWriter {
            inner: &mut file,
            remaining: maximum,
        },
        value,
    )?;
    file.as_file().sync_all()?;
    file.persist(directory.join(name)).map_err(|e| e.error)?;
    sync_directory(directory)
}

struct BoundedWriter<W> {
    inner: W,
    remaining: usize,
}
impl<W: Write> Write for BoundedWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.remaining {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "JSON record exceeds declared byte bound",
            ));
        }
        let count = self.inner.write(bytes)?;
        self.remaining -= count;
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn read_json(path: &Path, maximum_bytes: usize) -> io::Result<Value> {
    let bytes = crate::durable_fs::read_file_bounded(path, maximum_bytes)?;
    serde_json::from_slice(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn random_hex(bytes: usize) -> io::Result<String> {
    let mut buffer = vec![0u8; bytes];
    File::open("/dev/urandom")?.read_exact(&mut buffer)?;
    Ok(buffer.iter().map(|byte| format!("{byte:02x}")).collect())
}

/// A random UUID (version 4) from the Linux random source.
pub fn random_uuid() -> io::Result<String> {
    let hex = random_hex(16)?;
    let variant = ['8', '9', 'a', 'b'][hex[16..17]
        .parse::<char>()
        .ok()
        .and_then(|c| c.to_digit(16))
        .unwrap_or(0) as usize
        & 3];
    let hex = format!("{}4{}{variant}{}", &hex[..12], &hex[13..16], &hex[17..]);
    Ok(format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    ))
}

fn valid_session_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 64 && id.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-')
}

/// Input phases recorded before the effects they describe are reported.
mod phase {
    /// Recorded before the native turn may start; consumption unknown.
    pub const ACCEPTED: &str = "accepted";
    /// The native turn reported consumption; acknowledged.
    pub const INSERTED: &str = "inserted";
    /// The turn ended (or was refused) without consuming the input.
    pub const NOT_INSERTED: &str = "not_inserted";
    /// The consumed input's turn ended; `stop_reason` and `native_turn` set.
    pub const ENDED: &str = "ended";
    /// Settlement could not establish whether the input was consumed.
    pub const UNCERTAIN: &str = "uncertain";
}

#[derive(Clone, Debug)]
struct InputRecord {
    value: Value,
}

impl InputRecord {
    fn message_id(&self) -> &str {
        self.value["message_id"].as_str().unwrap_or_default()
    }
    fn key(&self) -> Option<&str> {
        self.value["message_key"].as_str()
    }
    fn phase(&self) -> &str {
        self.value["phase"].as_str().unwrap_or_default()
    }
    fn stop_reason(&self) -> Option<&str> {
        self.value["stop_reason"].as_str()
    }
}

// ---- endpoint ---------------------------------------------------------------

struct Endpoint<T: ResidentTurns> {
    turns: Arc<T>,
    sessions_root: PathBuf,
    wire: Wire,
    initialized: bool,
    sessions: HashMap<String, OpenSession>,
    /// A refused close releases ownership but must not turn later EOF clean.
    unsettled_closed: std::collections::HashSet<String>,
}

struct OpenSession {
    shared: Arc<SessionShared>,
    jobs: Option<mpsc::Sender<Job>>,
    worker: Option<JoinHandle<bool>>,
}

/// State shared by the connection thread and one session's turn worker.
struct SessionShared {
    id: String,
    dir: PathBuf,
    cwd: PathBuf,
    record: Mutex<Value>,
    inputs: Mutex<SessionInputs>,
    /// Stop flag of the running turn.
    stop: AtomicBool,
    /// Incremented by every cancel; inputs queued under an older epoch are
    /// refused.
    cancel_epoch: AtomicU64,
    closed: AtomicBool,
    lock: Mutex<Option<File>>,
}

#[derive(Default)]
struct SessionInputs {
    unreadable: Vec<String>,
    /// Reconstructable inputs whose settlement evidence is unavailable.
    recovery_errors: Vec<String>,
    /// Session launch records not established as physically settled.
    unsettled_launches: Vec<String>,
    by_id: HashMap<String, InputRecord>,
    by_key: HashMap<String, String>,
    /// Prompt requests waiting for the insertion acknowledgement.
    waiters: HashMap<String, Vec<Value>>,
    /// Inputs whose turn runs or waits in this process.
    live: std::collections::HashSet<String>,
    last_message: u64,
}

enum Job {
    Turn { message_id: String, epoch: u64 },
    Close,
}

impl<T: ResidentTurns> Endpoint<T> {
    fn dispatch(&mut self, line: &str) {
        let message: Value = match serde_json::from_str(line.trim()) {
            Ok(message) => message,
            Err(_) => {
                self.wire
                    .error(&Value::Null, PARSE_ERROR, "invalid JSON message", None);
                return;
            }
        };
        let Some(method) = message.get("method").and_then(Value::as_str) else {
            return; // A response: this endpoint sends no requests.
        };
        let params = message.get("params").cloned().unwrap_or(Value::Null);
        let Some(id) = message.get("id").cloned() else {
            if method == "session/cancel" {
                if let Some(session) = params
                    .get("sessionId")
                    .and_then(Value::as_str)
                    .and_then(|id| self.sessions.get(id))
                {
                    cancel(&session.shared);
                }
            }
            return;
        };
        if method != "initialize" && !self.initialized {
            self.wire
                .error(&id, NOT_INITIALIZED, "initialize first", None);
            return;
        }
        let outcome = match method {
            "initialize" => Ok(Some(self.initialize())),
            "session/new" => self.new_session(&params).map(Some),
            "session/resume" => self.resume_session(&params).map(Some),
            "session/prompt" => self.prompt(&id, &params),
            "session/close" => self.close_session(&id, &params),
            "session/list" => Ok(Some(self.list_sessions(&params))),
            _ => Err((METHOD_NOT_FOUND, "method not found".to_owned())),
        };
        match outcome {
            Ok(Some(result)) => self.wire.result(&id, result),
            Ok(None) => {}
            Err((code, reason)) => self.wire.error(&id, code, &reason, None),
        }
    }

    fn initialize(&mut self) -> Value {
        self.initialized = true;
        let (name, version) = self.turns.implementation();
        let mut meta = Map::new();
        meta.insert(DEDUP_CONTRACT_META.into(), json!({"version":1}));
        meta.insert(
            RESIDENT_SESSION_META.into(),
            json!({"protocol":RESIDENT_SESSION_PROTOCOL,"acp_schema":ACP_SCHEMA_TAG}),
        );
        json!({"protocolVersion":ACP_PROTOCOL_VERSION,
            "info":{"name":name,"version":version},
            "capabilities":{"session":{}},
            "_meta":meta})
    }

    fn cwd_param(params: &Value) -> Result<PathBuf, (i64, String)> {
        let cwd = params
            .get("cwd")
            .and_then(Value::as_str)
            .filter(|cwd| cwd.starts_with('/'))
            .ok_or((INVALID_PARAMS, "cwd must be an absolute path".to_owned()))?;
        if !Path::new(cwd).is_dir() {
            return Err((INVALID_PARAMS, "cwd must be an existing directory".into()));
        }
        Ok(PathBuf::from(cwd))
    }

    fn new_session(&mut self, params: &Value) -> Result<Value, (i64, String)> {
        let cwd = Self::cwd_param(params)?;
        let id = random_uuid().map_err(internal)?;
        let dir = self.sessions_root.join(&id);
        create_private_directories(&dir.join("inputs")).map_err(internal)?;
        create_private_directories(&dir.join("turns")).map_err(internal)?;
        let record = json!({"protocol":RESIDENT_SESSION_PROTOCOL,"session_id":id,
            "cwd":cwd,"native_session_id":null,
            "create_native_session_id":self.turns.create_native_session_id(),
            "created_unix_ms":now_unix_ms()});
        publish_json(&dir, "session.json", &record).map_err(internal)?;
        let lock = custody::try_lock_exclusive(&dir.join("session.lock"))
            .map_err(|e| (SESSION_UNAVAILABLE, e.to_string()))?;
        self.open(id.clone(), dir, cwd, record, SessionInputs::default(), lock);
        Ok(json!({"sessionId":id}))
    }

    fn resume_session(&mut self, params: &Value) -> Result<Value, (i64, String)> {
        let id = params
            .get("sessionId")
            .and_then(Value::as_str)
            .filter(|id| valid_session_id(id))
            .ok_or((
                INVALID_PARAMS,
                "sessionId is not a session of this provider".into(),
            ))?
            .to_owned();
        let cwd = Self::cwd_param(params)?;
        if self.sessions.contains_key(&id) {
            return Err((SESSION_UNAVAILABLE, "session is already open".into()));
        }
        let dir = self.sessions_root.join(&id);
        let record = read_json(&dir.join("session.json"), MAX_SESSION_RECORD_BYTES)
            .map_err(|_| (SESSION_UNAVAILABLE, "unknown session".to_owned()))?;
        if record["cwd"].as_str() != cwd.to_str() {
            return Err((
                INVALID_PARAMS,
                "cwd differs from the session's working directory".into(),
            ));
        }
        let lock = custody::try_lock_exclusive(&dir.join("session.lock")).map_err(|_| {
            (
                SESSION_UNAVAILABLE,
                "session is held by another resident process".to_owned(),
            )
        })?;
        let mut inputs = SessionInputs::default();
        for entry in fs::read_dir(dir.join("inputs")).map_err(internal)? {
            let path = entry.map_err(internal)?.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let value = match read_json(&path, MAX_INPUT_RECORD_BYTES).and_then(|value| {
                let message_id = value["message_id"].as_str().unwrap_or_default();
                let expected_request = format!("resident-{id}-{message_id}");
                if message_id.len() != 20
                    || !message_id.starts_with("msg_")
                    || !message_id[4..].bytes().all(|b| b.is_ascii_hexdigit())
                    || path.file_stem().and_then(|n| n.to_str()) != Some(message_id)
                    || value["request_id"].as_str() != Some(expected_request.as_str())
                    || !value["prompt"].is_string()
                    || !matches!(
                        value["phase"].as_str(),
                        Some(
                            phase::ACCEPTED
                                | phase::INSERTED
                                | phase::ENDED
                                | phase::NOT_INSERTED
                                | phase::UNCERTAIN
                        )
                    )
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid input identity or phase",
                    ));
                }
                Ok(value)
            }) {
                Ok(value) => value,
                Err(error) => {
                    inputs
                        .unreadable
                        .push(format!("{}: {error}", path.display()));
                    continue;
                }
            };
            let input = InputRecord { value };
            inputs.last_message = inputs.last_message.max(message_number(input.message_id()));
            if let Some(key) = input.key() {
                inputs
                    .by_key
                    .insert(key.to_owned(), input.message_id().to_owned());
            }
            inputs.by_id.insert(input.message_id().to_owned(), input);
        }
        let shared = self.open(id, dir, cwd, record, inputs, lock);
        settle_unfinished(&*self.turns, &shared);
        if has_unsettled_custody(&shared) {
            return Err((SESSION_UNAVAILABLE, "native custody is not settled".into()));
        }
        if let Some(error) = recovery_uncertainty(&shared) {
            return Err((SESSION_UNAVAILABLE, error));
        }
        Ok(json!({}))
    }

    fn open(
        &mut self,
        id: String,
        dir: PathBuf,
        cwd: PathBuf,
        record: Value,
        inputs: SessionInputs,
        lock: File,
    ) -> Arc<SessionShared> {
        let shared = Arc::new(SessionShared {
            id: id.clone(),
            dir,
            cwd,
            record: Mutex::new(record),
            inputs: Mutex::new(inputs),
            stop: AtomicBool::new(false),
            cancel_epoch: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            lock: Mutex::new(Some(lock)),
        });
        let (jobs, receive) = mpsc::channel();
        let worker = {
            let shared = shared.clone();
            let turns = self.turns.clone();
            let wire = self.wire.clone();
            std::thread::spawn(move || work(&*turns, &shared, &wire, receive))
        };
        self.sessions.insert(
            id,
            OpenSession {
                shared: shared.clone(),
                jobs: Some(jobs),
                worker: Some(worker),
            },
        );
        shared
    }

    fn prompt(&mut self, id: &Value, params: &Value) -> Result<Option<Value>, (i64, String)> {
        let session_id = params
            .get("sessionId")
            .and_then(Value::as_str)
            .unwrap_or("");
        let session = self
            .sessions
            .get(session_id)
            .ok_or((INVALID_PARAMS, "session is not open here".to_owned()))?;
        let shared = session.shared.clone();
        if shared.closed.load(Ordering::SeqCst) {
            return Err((SESSION_UNAVAILABLE, "session is closed".into()));
        }
        let blocks = params
            .get("prompt")
            .and_then(Value::as_array)
            .ok_or((INVALID_PARAMS, "prompt must be an array".to_owned()))?;
        let mut texts = Vec::with_capacity(blocks.len());
        for block in blocks {
            match (block.get("type").and_then(Value::as_str), block.get("text")) {
                (Some("text"), Some(Value::String(text))) => texts.push(text.as_str()),
                _ => return Err((INVALID_PARAMS, "only text content is accepted".into())),
            }
        }
        let prompt = texts.join("\n");
        let key = params
            .pointer(&format!("/_meta/{}", MESSAGE_KEY_META.replace('/', "~1")))
            .and_then(Value::as_str)
            .filter(|key| !key.is_empty())
            .map(str::to_owned);
        let mut inputs = shared.inputs.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(existing) = key.as_ref().and_then(|key| inputs.by_key.get(key)).cloned() {
            return duplicate(
                &shared,
                &mut inputs,
                &self.wire,
                id,
                &existing,
                key.as_deref(),
            );
        }
        if !inputs.unreadable.is_empty() {
            return Err((
                SESSION_UNAVAILABLE,
                "input evidence unreadable; new input is blocked".into(),
            ));
        }
        if let Some(reason) = shared.record.lock().unwrap_or_else(|e| e.into_inner())
            ["native_session_uncertain"]
            .as_str()
        {
            return Err((
                SESSION_UNAVAILABLE,
                format!("native session identity is uncertain; new input blocked: {reason}"),
            ));
        }
        let number = next_message_number(inputs.last_message);
        inputs.last_message = number;
        let message_id = format!("msg_{number:016x}");
        let native = shared
            .record
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let value = json!({"message_id":message_id,"message_key":key,
            "request_id":format!("resident-{}-{}", shared.id, message_id),
            "prompt":prompt,"prompt_sha256":sha256_hex(prompt.as_bytes()),
            "native_session_id":native["native_session_id"],
            "create_native_session_id": if native["native_session_id"].is_null() {
                native["create_native_session_id"].clone() } else { Value::Null },
            "dispatched":false,
            "phase":phase::ACCEPTED,"accepted_unix_ms":now_unix_ms()});
        serde_json::to_writer(
            BoundedWriter {
                inner: io::sink(),
                remaining: MAX_ACCEPTED_INPUT_BYTES,
            },
            &value,
        )
        .map_err(|_| {
            (
                INVALID_PARAMS,
                "input exceeds the 8 MiB serialized admission bound".to_owned(),
            )
        })?;
        // Durable before any native effect of this input.
        publish_json(
            &shared.dir.join("inputs"),
            &format!("{message_id}.json"),
            &value,
        )
        .map_err(internal)?;
        if let Some(key) = &key {
            inputs.by_key.insert(key.clone(), message_id.clone());
        }
        inputs
            .by_id
            .insert(message_id.clone(), InputRecord { value });
        inputs.waiters.insert(message_id.clone(), vec![id.clone()]);
        inputs.live.insert(message_id.clone());
        drop(inputs);
        let epoch = shared.cancel_epoch.load(Ordering::SeqCst);
        if let Some(jobs) = &session.jobs {
            let _ = jobs.send(Job::Turn { message_id, epoch });
        }
        Ok(None)
    }

    fn close_session(
        &mut self,
        _id: &Value,
        params: &Value,
    ) -> Result<Option<Value>, (i64, String)> {
        let session_id = params
            .get("sessionId")
            .and_then(Value::as_str)
            .unwrap_or("");
        let mut session = self
            .sessions
            .remove(session_id)
            .ok_or((INVALID_PARAMS, "session is not open here".to_owned()))?;
        session.shared.closed.store(true, Ordering::SeqCst);
        cancel(&session.shared);
        if let Some(jobs) = session.jobs.take() {
            let _ = jobs.send(Job::Close);
        }
        // The worker answers after settlement and releases the lock. Join and
        // drop all remaining live session resources before reading another RPC.
        let settled = session
            .worker
            .take()
            .is_some_and(|worker| worker.join().unwrap_or(false));
        drop(session);
        if settled {
            self.unsettled_closed.remove(session_id);
            Ok(Some(json!({})))
        } else {
            self.unsettled_closed.insert(session_id.to_owned());
            Err((SESSION_UNAVAILABLE, "native custody is not settled".into()))
        }
    }

    fn list_sessions(&self, params: &Value) -> Value {
        let filter = params.get("cwd").and_then(Value::as_str);
        let mut sessions = Vec::new();
        if let Ok(entries) = fs::read_dir(&self.sessions_root) {
            for entry in entries.flatten() {
                let Ok(record) =
                    read_json(&entry.path().join("session.json"), MAX_SESSION_RECORD_BYTES)
                else {
                    continue;
                };
                let cwd = record["cwd"].as_str().unwrap_or_default();
                if filter.is_none_or(|filter| filter == cwd) {
                    sessions.push(json!({"sessionId":record["session_id"],"cwd":cwd}));
                }
            }
        }
        json!({"sessions":sessions})
    }

    fn shutdown(&mut self) -> io::Result<()> {
        for session in self.sessions.values_mut() {
            session.shared.closed.store(true, Ordering::SeqCst);
            cancel(&session.shared);
            session.jobs.take();
        }
        for (id, session) in &mut self.sessions {
            let settled = session
                .worker
                .take()
                .is_some_and(|worker| worker.join().unwrap_or(false));
            if settled {
                self.unsettled_closed.remove(id);
            } else {
                self.unsettled_closed.insert(id.clone());
            }
        }
        if self.unsettled_closed.is_empty() {
            Ok(())
        } else {
            Err(io::Error::other("native session custody is not settled"))
        }
    }
}

fn internal(error: impl std::fmt::Display) -> (i64, String) {
    (-32603, error.to_string())
}

fn message_number(message_id: &str) -> u64 {
    message_id
        .strip_prefix("msg_")
        .and_then(|hex| u64::from_str_radix(hex, 16).ok())
        .unwrap_or(0)
}

/// Fixed-width ascending ids: `max(now_ms * 4096, previous + 1)`.
fn next_message_number(previous: u64) -> u64 {
    (now_unix_ms().saturating_mul(4096)).max(previous.saturating_add(1))
}

fn cancel(shared: &SessionShared) {
    shared.cancel_epoch.fetch_add(1, Ordering::SeqCst);
    shared.stop.store(true, Ordering::SeqCst);
}

fn duplicate(
    shared: &SessionShared,
    inputs: &mut SessionInputs,
    wire: &Wire,
    id: &Value,
    message_id: &str,
    key: Option<&str>,
) -> Result<Option<Value>, (i64, String)> {
    let mut input = inputs
        .by_id
        .get(message_id)
        .cloned()
        .ok_or_else(|| internal("lost input"))?;
    let non_start = !inputs.live.contains(message_id) && record_not_started(shared, &mut input);
    if non_start {
        inputs.by_id.insert(message_id.to_owned(), input.clone());
        inputs
            .recovery_errors
            .retain(|error| !error.starts_with(&format!("{message_id}:")));
    }
    match input.phase() {
        phase::INSERTED | phase::ENDED => {
            wire.result(id, ack(message_id, key, true));
            if input.phase() == phase::ENDED && !inputs.live.contains(message_id) {
                wire.update(&shared.id, idle_update(&input));
            }
            Ok(None)
        }
        phase::ACCEPTED if inputs.live.contains(message_id) => {
            inputs
                .waiters
                .entry(message_id.to_owned())
                .or_default()
                .push(id.clone());
            Ok(None)
        }
        phase::NOT_INSERTED
            if input.value["consumption_seen"] != json!(true)
                && (!undispatched(&input) || non_start)
                && !inputs.recovery_errors.iter().any(|error| error.starts_with(message_id)) => Err((
            INPUT_NOT_INSERTED,
            format!("input {message_id} with this key was not inserted; send a new key"),
        )),
        _ => Err((
            INPUT_UNCERTAIN,
            format!(
                "input {message_id} with this key may have been consumed by an interrupted turn; it is not run again"
            ),
        )),
    }
}

fn ack(message_id: &str, key: Option<&str>, duplicate: bool) -> Value {
    let mut meta = Map::new();
    if let Some(key) = key {
        meta.insert(MESSAGE_KEY_META.into(), json!(key));
        meta.insert(DUPLICATE_META.into(), json!(duplicate));
    }
    json!({"messageId":message_id,"_meta":meta})
}

fn idle_update(input: &InputRecord) -> Value {
    json!({"sessionUpdate":"state_update","state":"idle",
        "stopReason":input.stop_reason().unwrap_or("_oulipoly_unknown"),
        "_meta":{TURN_INPUT_META:input.message_id(),NATIVE_TURN_META:input.value["native_turn"]}})
}

fn store(shared: &SessionShared, input: &mut InputRecord) -> io::Result<()> {
    publish_json(
        &shared.dir.join("inputs"),
        &format!("{}.json", input.message_id()),
        &input.value,
    )
}

// ---- turns ------------------------------------------------------------------

fn work<T: ResidentTurns>(
    turns: &T,
    shared: &SessionShared,
    wire: &Wire,
    jobs: mpsc::Receiver<Job>,
) -> bool {
    while let Ok(job) = jobs.recv() {
        match job {
            Job::Turn { message_id, epoch } => {
                if epoch != shared.cancel_epoch.load(Ordering::SeqCst)
                    || shared.closed.load(Ordering::SeqCst)
                {
                    refuse(
                        shared,
                        wire,
                        &message_id,
                        "cancelled before the input was inserted",
                    );
                    continue;
                }
                shared.stop.store(false, Ordering::SeqCst);
                if epoch != shared.cancel_epoch.load(Ordering::SeqCst) {
                    refuse(
                        shared,
                        wire,
                        &message_id,
                        "cancelled before the input was inserted",
                    );
                    continue;
                }
                if has_unsettled_custody(shared) {
                    settle_unfinished(turns, shared);
                    if has_unsettled_custody(shared) {
                        refuse(
                            shared,
                            wire,
                            &message_id,
                            "previous native custody remains unsettled",
                        );
                        continue;
                    }
                }
                if let Some(error) = recovery_uncertainty(shared) {
                    refuse(shared, wire, &message_id, &error);
                    continue;
                }
                run(turns, shared, Some(wire), &message_id);
                if has_unsettled_custody(shared) {
                    settle_unfinished(turns, shared);
                    let inputs = shared.inputs.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(input) = inputs.by_id.get(&message_id) {
                        if input.phase() == phase::ENDED {
                            wire.update(&shared.id, idle_update(input));
                        }
                    }
                }
            }
            Job::Close => {
                while let Ok(Job::Turn { message_id, .. }) = jobs.try_recv() {
                    refuse(
                        shared,
                        wire,
                        &message_id,
                        "session closed before the input was inserted",
                    );
                }
                settle_unfinished(turns, shared);
                if has_unsettled_custody(shared) {
                    return false;
                }
                shared.lock.lock().unwrap_or_else(|e| e.into_inner()).take();
                return true;
            }
        }
    }
    settle_unfinished(turns, shared);
    // Connection end: refuse what is still queued.
    while let Ok(Job::Turn { message_id, .. }) = jobs.try_recv() {
        refuse(
            shared,
            wire,
            &message_id,
            "connection ended before the input was inserted",
        );
    }
    !has_unsettled_custody(shared)
}

fn refuse(shared: &SessionShared, wire: &Wire, message_id: &str, reason: &str) {
    let mut inputs = shared.inputs.lock().unwrap_or_else(|e| e.into_inner());
    inputs.live.remove(message_id);
    let waiters = inputs.waiters.remove(message_id).unwrap_or_default();
    let mut code = INPUT_UNCERTAIN;
    if let Some(input) = inputs.by_id.get_mut(message_id) {
        if undispatched(input) && no_launch_evidence(shared, input) {
            code = INPUT_NOT_INSERTED;
            input.value["phase"] = json!(phase::NOT_INSERTED);
        } else if !matches!(input.phase(), phase::INSERTED | phase::ENDED) {
            input.value["phase"] = json!(phase::UNCERTAIN);
        }
        input.value["refusal"] = json!(reason);
        let mut input = input.clone();
        drop(inputs);
        let _ = store(shared, &mut input);
        shared
            .inputs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .by_id
            .insert(message_id.to_owned(), input);
    }
    for waiter in waiters {
        wire.error(&waiter, code, reason, None);
    }
}

/// Settles unfinished inputs without a client: replay or reconciliation only.
/// Dispatch-bound native selection is reused; current template changes cannot
/// prevent physical actor discharge or authorize a new native attempt.
fn has_unsettled_custody(shared: &SessionShared) -> bool {
    let inputs = shared.inputs.lock().unwrap_or_else(|e| e.into_inner());
    !inputs.unsettled_launches.is_empty()
        || !inputs.recovery_errors.is_empty()
        || inputs
            .by_id
            .values()
            .any(|input| input.value["native_turn"]["custody"] == json!("incomplete"))
}

fn recovery_uncertainty(shared: &SessionShared) -> Option<String> {
    let inputs = shared.inputs.lock().unwrap_or_else(|e| e.into_inner());
    if !inputs.unreadable.is_empty() {
        return Some(format!(
            "input evidence unreadable; new input blocked and continuity uncertain; input records retained: {}",
            inputs.unreadable.join("; ")
        ));
    }
    if !inputs.recovery_errors.is_empty() {
        return Some(format!(
            "input settlement evidence unavailable; new input blocked: {}",
            inputs.recovery_errors.join("; ")
        ));
    }
    let record = shared.record.lock().unwrap_or_else(|e| e.into_inner());
    record["native_session_uncertain"]
        .as_str()
        .map(|reason| format!("native session identity is uncertain; new input blocked: {reason}"))
}

// Streaming evidence recovery never allocates the whole journal and never
// redelivers output. Any missing, malformed, cut or over-bound record is evidence
// uncertainty; valid prefix markers still preserve known insertion.
fn recover_markers(sink: &mut TurnSink<'_>, journal: &Path) -> io::Result<()> {
    let mut reader = BufReader::new(File::open(journal)?);
    loop {
        let bytes = read_record(&mut reader, MAX_JOURNAL_RECORD_BYTES)?;
        if bytes.is_empty() {
            return Ok(());
        }
        if bytes.last() != Some(&b'\n') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "interrupted journal has a cut record",
            ));
        }
        let event: Value = serde_json::from_slice(&bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        if !event["kind"].is_string()
            || (event["kind"] == "marker" && !event["name"].is_string())
            || (event["kind"] == "marker"
                && event["name"] == PROVIDER_SESSION_MARKER
                && event["value"]["provider_session_id"]
                    .as_str()
                    .is_none_or(str::is_empty))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid interrupted journal event",
            ));
        }
        sink.event(event);
    }
}

fn settle_unfinished<T: ResidentTurns>(turns: &T, shared: &SessionShared) {
    let unfinished: Vec<String> = {
        let mut inputs = shared.inputs.lock().unwrap_or_else(|e| e.into_inner());
        inputs.recovery_errors.clear();
        let mut ids: Vec<String> = inputs
            .by_id
            .values()
            .filter(|input| {
                !inputs.live.contains(input.message_id())
                    && (matches!(
                        input.phase(),
                        phase::ACCEPTED | phase::INSERTED | phase::UNCERTAIN
                    ) || input.value["native_turn"]["custody"] == json!("incomplete")
                        || (input.phase() == phase::NOT_INSERTED
                            && (input.value["consumption_seen"] == json!(true)
                                || launch_evidence_may_exist(shared, input))))
            })
            .map(|input| input.message_id().to_owned())
            .collect();
        ids.sort();
        ids
    };
    for message_id in unfinished {
        if !not_started(shared, &message_id) {
            run(turns, shared, None, &message_id);
        }
    }
    let unsettled = settle_recorded_launches(&shared.dir.join("turns"));
    shared
        .inputs
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .unsettled_launches = unsettled;
}

// Session custody is independent of reconstructable input/insertion evidence.
// Scan only this owned session's durable launch records after its live work has
// settled, and continue past unreadable records to discharge readable actors.
fn settle_recorded_launches(turns: &Path) -> Vec<String> {
    let mut unsettled = Vec::new();
    let entries = match fs::read_dir(turns) {
        Ok(entries) => entries,
        Err(error) => return vec![format!("{}: {error}", turns.display())],
    };
    for entry in entries {
        let path = match entry {
            Ok(entry) => entry.path(),
            Err(error) => {
                unsettled.push(error.to_string());
                continue;
            }
        };
        match path.extension().and_then(|e| e.to_str()) {
            Some("json") => {}
            // An orphaned journal is evidence of a launch, not evidence of no actor.
            Some("jsonl") if !path.with_extension("json").is_file() => {}
            _ => continue,
        }
        let key = path.file_stem().and_then(|n| n.to_str()).unwrap_or("");
        if key.len() != 64 || !key.bytes().all(|b| b.is_ascii_hexdigit()) {
            unsettled.push(format!("{}: invalid custody key", path.display()));
            continue;
        }
        if let Err(error) = crate::lifecycle::reconcile_recorded_launch(turns, key) {
            unsettled.push(format!("{}: {error}", path.display()));
        }
    }
    unsettled
}

fn undispatched(input: &InputRecord) -> bool {
    // Dispatch is published before entering the adapter, and recovery never
    // resets it. Refusal can change phase without dispatching: uncertainty
    // about temporarily unavailable custody does not destroy this premise.
    matches!(
        input.phase(),
        phase::ACCEPTED | phase::UNCERTAIN | phase::NOT_INSERTED
    ) && input.value["dispatched"] == json!(false)
        && (input.value["consumption_seen"].is_null()
            || input.value["consumption_seen"] == json!(false))
        && input.value["native_turn"].is_null()
        && input.value["inserted_unix_ms"].is_null()
}

// Absence is useful only alongside positive pre-dispatch evidence. Hold request
// custody and distinguish NotFound from unreadable/non-file/other evidence.
fn no_launch_evidence(shared: &SessionShared, input: &InputRecord) -> bool {
    let root = shared.dir.join("turns");
    let key = custody::request_key(None, input.value["request_id"].as_str().unwrap_or_default());
    let Ok(_custody) = custody::RequestCustody::acquire(&root, &key) else {
        return false;
    };
    ["json", "jsonl"].iter().all(|extension| {
        matches!(fs::symlink_metadata(root.join(format!("{key}.{extension}"))),
            Err(error) if error.kind() == io::ErrorKind::NotFound)
    })
}

fn launch_evidence_may_exist(shared: &SessionShared, input: &InputRecord) -> bool {
    !no_launch_evidence(shared, input)
}

fn not_started(shared: &SessionShared, message_id: &str) -> bool {
    let mut inputs = shared.inputs.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(input) = inputs.by_id.get_mut(message_id) {
        return record_not_started(shared, input);
    }
    false
}

fn record_not_started(shared: &SessionShared, input: &mut InputRecord) -> bool {
    if !undispatched(input) || !no_launch_evidence(shared, input) {
        return false;
    }
    let mut updated = input.clone();
    updated.value["phase"] = json!(phase::NOT_INSERTED);
    updated.value["refusal"] = json!("native dispatch was never recorded");
    // Do not publish a process-local refinement when its durable write failed.
    if store(shared, &mut updated).is_err() {
        return false;
    }
    *input = updated;
    true
}

fn run<T: ResidentTurns>(turns: &T, shared: &SessionShared, wire: Option<&Wire>, message_id: &str) {
    let Some(mut input) = shared
        .inputs
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .by_id
        .get(message_id)
        .cloned()
    else {
        return;
    };
    let fresh = undispatched(&input);
    if wire.is_some() && input.value["dispatched"] == json!(false) {
        let record = shared.record.lock().unwrap_or_else(|e| e.into_inner());
        input.value["native_session_id"] = record["native_session_id"].clone();
        input.value["create_native_session_id"] = if record["native_session_id"].is_null() {
            record["create_native_session_id"].clone()
        } else {
            Value::Null
        };
        input.value["dispatched"] = json!(true);
        drop(record);
        if let Err(error) = store(shared, &mut input) {
            if let Some(wire) = wire {
                refuse(
                    shared,
                    wire,
                    message_id,
                    &format!("cannot record dispatch: {error}"),
                );
            }
            return;
        }
        shared
            .inputs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .by_id
            .insert(message_id.to_owned(), input.clone());
    }
    let request = TurnRequest {
        session_id: shared.id.clone(),
        message_id: message_id.to_owned(),
        request_id: input.value["request_id"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        state_root: shared.dir.join("turns"),
        cwd: shared.cwd.clone(),
        prompt: input.value["prompt"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        native_session_id: input.value["native_session_id"].as_str().map(str::to_owned),
        create_native_session_id: input.value["create_native_session_id"]
            .as_str()
            .map(str::to_owned),
    };
    let previously_inserted = matches!(input.phase(), phase::INSERTED | phase::ENDED);
    let mut sink = TurnSink {
        shared,
        wire,
        message_id,
        key: input.key().map(str::to_owned),
        buffer: Vec::new(),
        consumed: previously_inserted,
        consumption_seen: previously_inserted || input.value["consumption_seen"] == json!(true),
        record_error: None,
        held: Vec::new(),
        exit: None,
        output: None,
        markers_only: false,
    };
    // Recovery settles the recorded actor before calling any adapter admission
    // logic. Adapters may validate current policy before entering the lifecycle.
    let mut result = if wire.is_none() {
        match crate::lifecycle::reconcile_recorded_launch(
            &request.state_root,
            &custody::request_key(None, &request.request_id),
        ) {
            Ok(true) => Err(TurnFailure {
                kind: TurnFailureKind::ReconciliationRequired,
                code: "reconciliation_required".into(),
                message: "the recorded incomplete native actor was discharged; input is not rerun"
                    .into(),
            }),
            Ok(false) => turns.run_turn(&request, &shared.stop, &mut sink),
            Err(error) => {
                shared
                    .inputs
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .recovery_errors
                    .push(format!("{message_id}: {error}"));
                return;
            }
        }
    } else {
        turns.run_turn(&request, &shared.stop, &mut sink)
    };
    sink.drain_line();
    let mut evidence_error = None;
    let mut no_native_start = false;
    let reconciled =
        matches!(&result, Err(failure) if failure.kind == TurnFailureKind::ReconciliationRequired);
    let recovered_complete = wire.is_none() && result.is_ok() && sink.exit.is_some();
    let mut uncertainty = None;
    if reconciled {
        // The lifecycle publishes running actor custody before releasing the
        // effect gate. A validated prepared record therefore proves native
        // admission never occurred; a running/reconciled actor or ACK does not.
        let key = custody::request_key(None, &request.request_id);
        let prepared = read_json(
            &request.state_root.join(format!("{key}.json")),
            custody::LAUNCH_STATE_MAX_BYTES,
        )
        .is_ok_and(|state| {
            state["phase"] == json!(custody::PHASE_PREPARED)
                && state["actor_id"].is_null()
                && state["incarnation"].is_null()
        });
        // Recover any journal markers even for prepared custody: adapter
        // settlement can report insertion without a native process. A missing
        // journal is expected only when prepared custody proves no start.
        sink.markers_only = true;
        let journal = request.state_root.join(format!("{key}.jsonl"));
        let recovered = recover_markers(&mut sink, &journal);
        no_native_start = prepared
            && !sink.consumption_seen
            && (recovered.is_ok()
                || recovered
                    .as_ref()
                    .is_err_and(|error| error.kind() == io::ErrorKind::NotFound));
        uncertainty = if let Err(error) = recovered {
            if no_native_start && error.kind() == io::ErrorKind::NotFound {
                None
            } else {
                let reason = format!(
                    "journal evidence unreadable: {}: {error}",
                    journal.display()
                );
                evidence_error = Some(reason.clone());
                Some(reason)
            }
        } else {
            None
        };
    }
    // Complete launch custody proves completion/replay, not the identity of
    // native work. Recovery may replay its receipt but must not start a later
    // turn with an unobserved identity. No binary or permanent journal gate.
    if (reconciled || recovered_complete)
        && uncertainty.is_none()
        && !no_native_start
        && shared.record.lock().unwrap_or_else(|e| e.into_inner())["native_session_id"]
            .as_str()
            .is_none_or(str::is_empty)
    {
        uncertainty = Some(
            "interrupted native work may have occurred without an observed session identity".into(),
        );
    }
    if let Some(reason) = uncertainty {
        let mut record = shared.record.lock().unwrap_or_else(|e| e.into_inner());
        let mut updated = record.clone();
        if evidence_error.is_some() {
            updated["native_session_id"] = Value::Null;
            updated["create_native_session_id"] = Value::Null;
        }
        // A create candidate remains evidence, never an observed identity.
        updated["native_session_uncertain"] = json!(reason);
        *record = updated;
        if let Err(error) = publish_json(&shared.dir, "session.json", &record) {
            sink.record_error = Some(format!(
                "cannot publish native session uncertainty: {error}"
            ));
        }
    }
    if let Some(error) = &sink.record_error {
        result = Err(TurnFailure {
            kind: TurnFailureKind::Failed,
            code: "native_session_record_failed".into(),
            message: error.clone(),
        });
    }
    let consumed = sink.consumed;
    let consumption_seen = sink.consumption_seen;
    let exit = sink.exit.take();
    let output = sink.output.take();
    // Refresh: the sink may have recorded insertion.
    if let Some(current) = shared
        .inputs
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .by_id
        .get(message_id)
    {
        input = current.clone();
    }
    // A fresh local attempt with neither consumption nor durable launch/journal
    // evidence can be refused before start. Lost recovery evidence cannot.
    let not_started = fresh && !consumption_seen && no_launch_evidence(shared, &input);
    let launch_complete = read_json(
        &request.state_root.join(format!(
            "{}.json",
            custody::request_key(None, &request.request_id)
        )),
        custody::LAUNCH_STATE_MAX_BYTES,
    )
    .is_ok_and(|state| {
        state["phase"] == json!(custody::PHASE_COMPLETE)
            && state["actor_id"].is_null()
            && state["incarnation"].is_null()
    });
    let mut native_turn = json!({"request_id":request.request_id});
    let stop_reason = match (&result, &exit) {
        (Ok(_), Some(exit)) => {
            native_turn["custody"] = json!("complete");
            native_turn["status"] = exit["status"].clone();
            native_turn["terminal_signal"] = exit["terminal_signal"].clone();
            if let Some(output) = output {
                native_turn["launch_output"] = output;
            }
            match exit["status"]["kind"].as_str() {
                Some("cancelled") => "cancelled",
                Some("exited") if exit["status"]["code"] == json!(0) => "end_turn",
                _ => "_oulipoly_native_failed",
            }
        }
        (Ok(_), None) => {
            native_turn["custody"] = json!("complete_without_exit");
            "_oulipoly_turn_failed"
        }
        (Err(failure), _) => {
            native_turn["failure"] = json!({"code":failure.code,"message":failure.message});
            match failure.kind {
                TurnFailureKind::Cancelled if not_started => {
                    native_turn["custody"] = json!("not_admitted");
                    "cancelled"
                }
                TurnFailureKind::ReconciliationRequired => {
                    native_turn["custody"] = json!("reconciled");
                    "_oulipoly_reconciliation_required"
                }
                TurnFailureKind::Failed if not_started => {
                    native_turn["custody"] = json!("not_admitted");
                    "_oulipoly_turn_failed"
                }
                TurnFailureKind::Failed | TurnFailureKind::Cancelled if launch_complete => {
                    native_turn["custody"] = json!("complete_without_exit");
                    "_oulipoly_turn_failed"
                }
                TurnFailureKind::Failed | TurnFailureKind::Cancelled => {
                    native_turn["custody"] = json!("incomplete");
                    "_oulipoly_turn_failed"
                }
            }
        }
    };
    if let Some(error) = evidence_error {
        if sink.record_error.is_some() {
            let failure_message = native_turn["failure"]["message"]
                .as_str()
                .unwrap_or_default();
            native_turn["failure"]["message"] = json!(format!(
                "{failure_message}; journal evidence unreadable: {error}"
            ));
        } else {
            native_turn["failure"] = json!({"code":"journal_evidence_unreadable","message":error});
        }
    }
    let settled = native_turn["custody"] != json!("incomplete");
    let not_consumed_but_known = !consumption_seen
        && match &result {
            Ok(_) => exit.is_some(),
            Err(_) => not_started || no_native_start,
        };
    let phase = if consumed && !settled {
        phase::INSERTED
    } else if consumed {
        phase::ENDED
    } else if not_consumed_but_known {
        phase::NOT_INSERTED
    } else {
        phase::UNCERTAIN
    };
    input.value["phase"] = json!(phase);
    input.value["stop_reason"] = json!(stop_reason);
    input.value["native_turn"] = native_turn.clone();
    input.value["consumption_seen"] = json!(consumption_seen);
    if settled {
        input.value["ended_unix_ms"] = json!(now_unix_ms());
    } else {
        input.value.as_object_mut().unwrap().remove("ended_unix_ms");
    }
    let stored = store(shared, &mut input);
    let waiters = {
        let mut inputs = shared.inputs.lock().unwrap_or_else(|e| e.into_inner());
        inputs.live.remove(message_id);
        inputs.by_id.insert(message_id.to_owned(), input.clone());
        inputs.waiters.remove(message_id).unwrap_or_default()
    };
    let Some(wire) = wire else {
        return;
    };
    if consumed {
        if settled {
            wire.update(&shared.id, idle_update(&input));
        }
        if let Err(error) = stored {
            wire.update(
                &shared.id,
                json!({"sessionUpdate":"session_info_update",
                    "_meta":{NATIVE_TURN_META:{"record_error":error.to_string(),
                    "message_id":message_id}}}),
            );
        }
    } else {
        let (code, reason) = if phase == phase::NOT_INSERTED && not_started {
            (
                INPUT_NOT_INSERTED,
                "native turn was refused before it started",
            )
        } else if phase == phase::NOT_INSERTED {
            (
                INPUT_NOT_INSERTED,
                "native turn ended without consuming the input",
            )
        } else {
            (
                INPUT_UNCERTAIN,
                "native turn failed before its consumption was known",
            )
        };
        let mut data = json!({"nativeTurn":native_turn});
        if let Err(error) = stored {
            // A native launch report does not certify input publication.
            // Keep a failed final record write visible on rejected attempts too.
            data["recordError"] = json!({"message_id":message_id,"record_error":error.to_string()});
        }
        for waiter in waiters {
            wire.error(&waiter, code, reason, Some(data.clone()));
        }
    }
}

/// Translates one turn's launch events into ACP updates as they arrive.
struct TurnSink<'a> {
    shared: &'a SessionShared,
    wire: Option<&'a Wire>,
    message_id: &'a str,
    key: Option<String>,
    buffer: Vec<u8>,
    consumed: bool,
    consumption_seen: bool,
    record_error: Option<String>,
    /// Agent messages that arrived before consumption.
    held: Vec<String>,
    exit: Option<Value>,
    output: Option<Value>,
    /// Apply native-session and consumption markers only.
    markers_only: bool,
}

impl TurnSink<'_> {
    fn drain_line(&mut self) {
        while let Some(end) = self.buffer.iter().position(|b| *b == b'\n') {
            let line: Vec<u8> = self.buffer.drain(..=end).collect();
            if let Ok(event) = serde_json::from_slice::<Value>(&line) {
                self.event(event);
            }
        }
    }

    fn event(&mut self, event: Value) {
        if self.markers_only && event["kind"] != json!("marker") {
            return;
        }
        match event["kind"].as_str() {
            Some("marker") => match event["name"].as_str() {
                Some(PROVIDER_SESSION_MARKER) => {
                    if let Some(native) = event["value"]["provider_session_id"].as_str() {
                        self.native_session(native);
                    }
                }
                Some(SUBMITTED_USER_TURN_MARKER) if self.markers_only => self.record_consumed(),
                Some(SUBMITTED_USER_TURN_MARKER) => self.consume(),
                Some(LAUNCH_OUTPUT_COMPLETE_MARKER) => self.output = Some(event["value"].clone()),
                _ => {}
            },
            Some("stdout") => {
                let Some(bytes) = event["data_base64"]
                    .as_str()
                    .and_then(|d| decode_base64(d).ok())
                else {
                    return;
                };
                let mut text = String::from_utf8_lossy(&bytes).into_owned();
                if text.ends_with('\n') {
                    text.pop();
                }
                if self.consumed {
                    self.agent_message(&text);
                } else {
                    self.held.push(text);
                }
            }
            Some("exit") => self.exit = Some(event),
            _ => {}
        }
    }

    fn native_session(&mut self, native: &str) {
        let mut record = self.shared.record.lock().unwrap_or_else(|e| e.into_inner());
        if record["native_session_id"].as_str() == Some(native) {
            return;
        }
        let mut updated = record.clone();
        updated["native_session_id"] = json!(native);
        updated["create_native_session_id"] = Value::Null;
        match publish_json(&self.shared.dir, "session.json", &updated) {
            Ok(()) => *record = updated,
            Err(error) => {
                self.record_error = Some(format!("cannot publish native session: {error}"))
            }
        }
    }

    /// Consumption learned from evidence of an interrupted turn: recorded,
    /// and acknowledged only to a later duplicate.
    fn record_consumed(&mut self) {
        self.consumption_seen = true;
        if self.consumed {
            return;
        }
        let mut inputs = self.shared.inputs.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(input) = inputs.by_id.get_mut(self.message_id) {
            input.value["phase"] = json!(phase::INSERTED);
            let mut copy = input.clone();
            if store(self.shared, &mut copy).is_ok() {
                self.consumed = true;
            }
        }
    }

    fn consume(&mut self) {
        self.consumption_seen = true;
        if self.consumed || self.record_error.is_some() {
            return;
        }
        let waiters = {
            let mut inputs = self.shared.inputs.lock().unwrap_or_else(|e| e.into_inner());
            let Some(input) = inputs.by_id.get_mut(self.message_id) else {
                return;
            };
            input.value["phase"] = json!(phase::INSERTED);
            input.value["inserted_unix_ms"] = json!(now_unix_ms());
            let mut copy = input.clone();
            // Durable before the acknowledgement is sent.
            if store(self.shared, &mut copy).is_err() {
                input.value["phase"] = json!(phase::ACCEPTED);
                return;
            }
            inputs.waiters.remove(self.message_id).unwrap_or_default()
        };
        self.consumed = true;
        if let Some(wire) = self.wire {
            let prompt = self
                .shared
                .inputs
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .by_id
                .get(self.message_id)
                .and_then(|input| input.value["prompt"].as_str().map(str::to_owned))
                .unwrap_or_default();
            wire.update(
                &self.shared.id,
                json!({"sessionUpdate":"user_message","messageId":self.message_id,
                    "content":[{"type":"text","text":prompt}]}),
            );
            for (index, waiter) in waiters.iter().enumerate() {
                wire.result(waiter, ack(self.message_id, self.key.as_deref(), index > 0));
            }
            wire.update(
                &self.shared.id,
                json!({"sessionUpdate":"state_update","state":"running"}),
            );
        }
        for text in std::mem::take(&mut self.held) {
            self.agent_message(&text);
        }
    }

    fn agent_message(&mut self, text: &str) {
        let Some(wire) = self.wire else {
            return;
        };
        static AGENT_MESSAGES: AtomicU64 = AtomicU64::new(0);
        let number = next_message_number(AGENT_MESSAGES.load(Ordering::SeqCst));
        AGENT_MESSAGES.fetch_max(number, Ordering::SeqCst);
        wire.update(
            &self.shared.id,
            json!({"sessionUpdate":"agent_message","messageId":format!("amsg_{number:016x}"),
                "content":[{"type":"text","text":text}],
                "_meta":{PARENT_MESSAGE_META:self.message_id}}),
        );
    }
}

impl Write for TurnSink<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(bytes);
        self.drain_line();
        if let Some(error) = &self.record_error {
            return Err(io::Error::other(error.clone()));
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_ids_ascend_with_fixed_width() {
        let first = next_message_number(0);
        let second = next_message_number(first);
        assert!(second > first);
        let (a, b) = (format!("msg_{first:016x}"), format!("msg_{second:016x}"));
        assert!(b > a);
        assert_eq!(message_number(&b), second);
    }

    #[test]
    fn uuids_are_version_four_and_valid_session_ids() {
        let id = random_uuid().unwrap();
        assert_eq!(id.len(), 36);
        assert_eq!(&id[14..15], "4");
        assert!(valid_session_id(&id));
        assert!(!valid_session_id("../etc"));
        assert!(!valid_session_id(""));
    }
}
