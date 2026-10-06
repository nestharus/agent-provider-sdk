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
//! * `session/new` creates a provider-private durable session record under
//!   the state root and answers its id. The native session id is learned from
//!   the first turn (`oulipoly.provider_session` marker) or chosen by the
//!   adapter ([`ResidentTurns::create_native_session_id`]), recorded, and
//!   resumed by every later turn.
//! * `session/prompt` accepts text content only. The input is durably
//!   recorded with a fresh ascending `messageId` before its native turn
//!   starts. Turns of one session run one at a time in arrival order. The
//!   prompt is answered only when the native turn reports that it consumed
//!   the input (`oulipoly.submitted_user_turn` marker): that is the insertion
//!   acknowledgement, recorded durably before `user_message`, then the ACK and
//!   `state_update: running`. It is not turn completion.
//!   A turn that ends without that evidence answers a JSON-RPC error and
//!   records the input as not inserted.
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
//! running turn, settles it, refuses queued inputs and returns. Each session
//! is held by one process at a time through an exclusive lock. Logical
//! session ancestry, admission, scheduling and delivery policy remain the
//! host's; this endpoint keeps only the provider-native session record its
//! own resume and dedup promises need.

use crate::cancellation;
use crate::custody;
use crate::durable_fs::{create_private_directories, sync_directory};
use crate::encoding::{decode_base64, now_unix_ms, sha256_hex};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufRead, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

/// The only ACP protocol version served.
pub const ACP_PROTOCOL_VERSION: u64 = 2;
/// Release tag of the ACP v2 draft schema whose subset is served.
pub const ACP_SCHEMA_TAG: &str = "schema-v2.0.0-alpha.7";
/// Resident session contract named in `initialize` `_meta`.
pub const RESIDENT_SESSION_PROTOCOL: &str = "oulipoly.resident_session/v1";
/// `_meta` key carrying the sender's message key on a prompt and its echo.
pub const MESSAGE_KEY_META: &str = "oulipoly.ai/messageKey";
/// `_meta` key advertising the message-key dedup contract in `initialize`.
pub const DEDUP_CONTRACT_META: &str = "oulipoly.ai/messageKeyDedup";
/// `_meta` key on a prompt response returning an earlier insertion.
pub const DUPLICATE_META: &str = "oulipoly.ai/duplicate";
/// `_meta` key on an `agent_message`: the input it answers.
pub const PARENT_MESSAGE_META: &str = "oulipoly.ai/parentMessageId";
/// `_meta` key on an idle `state_update`: the input whose turn ended.
pub const TURN_INPUT_META: &str = "oulipoly.ai/lastUserMessageId";
/// `_meta` key on `initialize`: the resident session contract served.
pub const RESIDENT_SESSION_META: &str = "oulipoly.ai/residentSession";
/// `_meta` key on an idle `state_update`: the native turn's outcome.
pub const NATIVE_TURN_META: &str = "oulipoly.ai/nativeTurn";
/// Launch marker naming the native session a turn ran in.
pub const PROVIDER_SESSION_MARKER: &str = "oulipoly.provider_session";
/// Launch marker reporting that the native turn consumed its input.
pub const SUBMITTED_USER_TURN_MARKER: &str = "oulipoly.submitted_user_turn";
/// Launch marker carrying the launch-output accounting.
pub const LAUNCH_OUTPUT_COMPLETE_MARKER: &str = "oulipoly.launch_output_complete/v1";

const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;
const PARSE_ERROR: i64 = -32700;
const NOT_INITIALIZED: i64 = -32002;
const INPUT_NOT_INSERTED: i64 = -32010;
const INPUT_UNCERTAIN: i64 = -32011;
const SESSION_UNAVAILABLE: i64 = -32012;
const MAX_SESSION_RECORD_BYTES: usize = 64 * 1024;
const MAX_INPUT_RECORD_BYTES: usize = 16 * 1024 * 1024;

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

    /// A native session id the adapter assigns when a session's first turn
    /// creates its native session. `None` lets the native program choose and
    /// report it with the `oulipoly.provider_session` marker.
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
/// is created if missing. Returns after every session's running turn has
/// settled.
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
    let (lines, receive) = mpsc::channel::<Option<String>>();
    std::thread::spawn(move || read_lines(input, lines));
    let mut endpoint = Endpoint {
        turns,
        sessions_root,
        wire: wire.clone(),
        initialized: false,
        sessions: HashMap::new(),
    };
    let end = loop {
        if let Some(signal) = cancellation::termination_signal() {
            break ServeEnd::Terminated(signal);
        }
        match receive.recv_timeout(Duration::from_millis(100)) {
            Ok(Some(line)) => endpoint.dispatch(&line),
            Ok(None) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                break ServeEnd::ConnectionClosed
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
    };
    endpoint.shutdown();
    Ok(end)
}

fn read_lines<R: BufRead>(mut input: R, lines: mpsc::Sender<Option<String>>) {
    loop {
        let mut line = String::new();
        match input.read_line(&mut line) {
            Ok(0) | Err(_) => {
                let _ = lines.send(None);
                return;
            }
            Ok(_) => {
                if !line.trim().is_empty() && lines.send(Some(line)).is_err() {
                    return;
                }
            }
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
    serde_json::to_writer(&mut file, value)?;
    file.as_file().sync_all()?;
    file.persist(directory.join(name)).map_err(|e| e.error)?;
    sync_directory(directory)
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
            let value = read_json(&path, MAX_INPUT_RECORD_BYTES).map_err(internal)?;
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
            Ok(Some(json!({})))
        } else {
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

    fn shutdown(&mut self) {
        for session in self.sessions.values_mut() {
            session.shared.closed.store(true, Ordering::SeqCst);
            cancel(&session.shared);
            session.jobs.take();
        }
        for session in self.sessions.values_mut() {
            if let Some(worker) = session.worker.take() {
                let _ = worker.join();
            }
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
    let input = inputs
        .by_id
        .get(message_id)
        .cloned()
        .ok_or_else(|| internal("lost input"))?;
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
        phase::NOT_INSERTED => Err((
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
    if let Some(input) = inputs.by_id.get_mut(message_id) {
        input.value["phase"] = json!(phase::NOT_INSERTED);
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
        wire.error(&waiter, INPUT_NOT_INSERTED, reason, None);
    }
}

/// Settles unfinished inputs without a client: replay or reconciliation only.
/// Dispatch-bound native selection is reused; current template changes cannot
/// prevent physical actor discharge or authorize a new native attempt.
fn has_unsettled_custody(shared: &SessionShared) -> bool {
    shared
        .inputs
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .by_id
        .values()
        .any(|input| input.value["native_turn"]["custody"] == json!("incomplete"))
}

fn settle_unfinished<T: ResidentTurns>(turns: &T, shared: &SessionShared) {
    let unfinished: Vec<String> = {
        let inputs = shared.inputs.lock().unwrap_or_else(|e| e.into_inner());
        let mut ids: Vec<String> = inputs
            .by_id
            .values()
            .filter(|input| {
                !inputs.live.contains(input.message_id())
                    && (matches!(
                        input.phase(),
                        phase::ACCEPTED | phase::INSERTED | phase::UNCERTAIN
                    ) || input.value["native_turn"]["custody"] == json!("incomplete"))
            })
            .map(|input| input.message_id().to_owned())
            .collect();
        ids.sort();
        ids
    };
    for message_id in unfinished {
        let request_id = shared
            .inputs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .by_id
            .get(&message_id)
            .and_then(|input| input.value["request_id"].as_str().map(str::to_owned))
            .unwrap_or_default();
        let key = custody::request_key(None, &request_id);
        if shared
            .dir
            .join("turns")
            .join(format!("{key}.json"))
            .is_file()
        {
            run(turns, shared, None, &message_id);
        } else {
            // No launch state was ever recorded: nothing native ran.
            not_started(shared, &message_id);
        }
    }
}

fn not_started(shared: &SessionShared, message_id: &str) {
    let mut inputs = shared.inputs.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(input) = inputs.by_id.get_mut(message_id) {
        input.value["phase"] = json!(phase::NOT_INSERTED);
        input.value["refusal"] = json!("earlier process ended before the native turn was recorded");
        let mut copy = input.clone();
        let _ = store(shared, &mut copy);
    }
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
    if input.value["dispatched"] == json!(false) {
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
            } else {
                not_started(shared, message_id);
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
    let previously_inserted = input.phase() == phase::INSERTED;
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
        match crate::lifecycle::reconcile_interrupted_launch(
            &request.state_root,
            None,
            &request.request_id,
        ) {
            Ok(true) => Err(TurnFailure {
                kind: TurnFailureKind::ReconciliationRequired,
                code: "reconciliation_required".into(),
                message: "the recorded incomplete native actor was discharged; input is not rerun"
                    .into(),
            }),
            Ok(false) => turns.run_turn(&request, &shared.stop, &mut sink),
            Err(error) => Err(TurnFailure {
                kind: TurnFailureKind::Failed,
                code: "custody_recovery_failed".into(),
                message: error.to_string(),
            }),
        }
    } else {
        turns.run_turn(&request, &shared.stop, &mut sink)
    };
    sink.drain_line();
    if matches!(&result, Err(failure) if failure.kind == TurnFailureKind::ReconciliationRequired) {
        // The interrupted launch's journal precedes its delivery, so its
        // markers are the best evidence of native session identity and
        // consumption. Its output is not delivered again.
        sink.markers_only = true;
        let journal = request.state_root.join(format!(
            "{}.jsonl",
            custody::request_key(None, &request.request_id)
        ));
        if let Ok(bytes) = crate::durable_fs::read_file_bounded(&journal, MAX_INPUT_RECORD_BYTES) {
            let _ = sink.write(&bytes);
            sink.buffer.push(b'\n');
            sink.drain_line();
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
    // Without a recorded launch state nothing native ran for this request.
    let launch_recorded = request
        .state_root
        .join(format!(
            "{}.json",
            custody::request_key(None, &request.request_id)
        ))
        .is_file();
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
                TurnFailureKind::Cancelled => {
                    native_turn["custody"] = json!("not_admitted");
                    "cancelled"
                }
                TurnFailureKind::ReconciliationRequired => {
                    native_turn["custody"] = json!("reconciled");
                    "_oulipoly_reconciliation_required"
                }
                TurnFailureKind::Failed if !launch_recorded => {
                    native_turn["custody"] = json!("not_admitted");
                    "_oulipoly_turn_failed"
                }
                TurnFailureKind::Failed if launch_complete => {
                    native_turn["custody"] = json!("complete_without_exit");
                    "_oulipoly_turn_failed"
                }
                TurnFailureKind::Failed => {
                    native_turn["custody"] = json!("incomplete");
                    "_oulipoly_turn_failed"
                }
            }
        }
    };
    let settled = native_turn["custody"] != json!("incomplete");
    let not_consumed_but_known = !consumption_seen
        && match &result {
            Ok(_) => exit.is_some(),
            Err(failure) => failure.kind == TurnFailureKind::Cancelled || !launch_recorded,
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
        let (code, reason) = if phase == phase::NOT_INSERTED && !launch_recorded {
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
        for waiter in waiters {
            wire.error(
                &waiter,
                code,
                reason,
                Some(json!({"nativeTurn":native_turn})),
            );
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
