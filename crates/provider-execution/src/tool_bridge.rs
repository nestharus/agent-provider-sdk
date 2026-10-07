//! The mediated `bash` tool: a stdio MCP server that a provider registers as
//! its native agent's only command tool under `oulipoly.tool_mediation/v1`.
//!
//! A provider adapter starts it as its own executable's [`SUBCOMMAND`] (for
//! example from a native CLI's MCP configuration) with the launch's
//! [`ToolMediation`] in [`tool_mediation::ENV`] and the host's Bash ingress
//! in the variable that policy names, both inherited from or set by the
//! native agent. [`main`] serves MCP (JSON-RPC 2.0, newline-delimited) on
//! stdin/stdout with one tool, [`TOOL`]:
//!
//! * `{"command": C, "workdir"?: ABS, "background"?: bool}` runs `bash -lc C`
//!   only through the host's requester
//!   (`requester run --delivery sync|async -- bash -lc C`, in `workdir` or
//!   this process's directory), so through the host's Bash ingress and
//!   nowhere else. Under an allow list, a command not named exactly is
//!   refused here and no requester starts. Without the ingress variable no
//!   requester starts either: it could otherwise reach no owner or another
//!   route.
//! * `{"output_identity": ID, "output_offset"?, "output_length"?}` and
//!   `{"output_identity": ID, "accept_output": true}` read or accept the
//!   host-retained output of an earlier run (`requester native-output` /
//!   `native-accept`); they run no command and are not policy decisions.
//!
//! The answer renders only what the requester's one JSON result object
//! established (`agent-bash-root-v1` surfaces): the owner's refusal, a
//! positive no-start, a waited end with its output facts, or an unknown
//! outcome that must not be replayed. A requester failure or malformed
//! result is "unresolved; the command may have run".
//!
//! The server and requesters stay in the native agent's process group, so a
//! cancelled turn's group termination ends them. An MCP
//! `notifications/cancelled` kills that call's requester and sends no
//! answer. Ending the requester does not end owner-side work: the host
//! decides what an accepted run does when its requester goes away. This is
//! construction under deterministic tests, not proof that a given native CLI
//! offers no other tool.

use crate::encoding::decode_base64;
use agent_provider_contract::tool_mediation::{self, Decision, ToolMediation};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::io::{self, BufRead, Read, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// The provider subcommand that serves this bridge.
pub const SUBCOMMAND: &str = "tool.bridge";
/// The one tool's MCP name.
pub const TOOL: &str = "bash";
const ROOT_SURFACE: &str = "agent-bash-root-v1";
const OUTPUT_SURFACE: &str = "agent-bash-root-v1-output";
/// Largest requester result read; a larger one is unresolved.
const RESULT_LIMIT: usize = 64 * 1024 * 1024;
/// Output bytes shown inline; the rest stays with the owner's retention.
const SHOWN_BYTES: usize = 16 * 1024;
const MAX_LINE: u64 = 16 * 1024 * 1024;

/// One tool call's outcome: its text and whether it is an error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    pub text: String,
    pub error: bool,
}

fn answer(text: impl Into<String>, error: bool) -> Answer {
    Answer {
        text: text.into(),
        error,
    }
}

/// The tool's MCP definition for `policy`.
pub fn tool_definition(policy: &ToolMediation) -> Value {
    let scope = match &policy.bash {
        tool_mediation::BashPolicy::Allow { allow } => format!(
            "Run one of these exact shell commands (bash -lc); any other command is refused and nothing runs: {}.",
            serde_json::to_string(allow).expect("strings serialize")
        ),
        tool_mediation::BashPolicy::Authority { .. } => "Run a shell command (bash -lc).".to_owned(),
    };
    json!({
        "name": TOOL,
        "description": format!("{scope} Every command runs only through this root's own attributed Bash ingress, synchronously by default; with background: true it returns once started and its end arrives later in this conversation as a separate message. Read retained output with output_identity/output_offset/output_length; accept_output records explicit exact local acceptance of that identity."),
        "inputSchema": {
            "type": "object",
            "properties": {
                "command": {"type": "string", "description": "The whole shell command."},
                "workdir": {"type": "string", "description": "Absolute working directory; defaults to the session's."},
                "background": {"type": "boolean", "description": "Run as background work; its end arrives later as a message."},
                "output_identity": {"type": "string", "description": "Retained output identity (rv1o:...) or work reference (rv1w:...) from an earlier result."},
                "output_offset": {"type": "integer", "minimum": 0},
                "output_length": {"type": "integer", "minimum": 1},
                "accept_output": {"type": "boolean"}
            },
            "additionalProperties": false
        },
        "annotations": {"readOnlyHint": false, "destructiveHint": true, "idempotentHint": false, "openWorldHint": true}
    })
}

/// What one call asks the requester for.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Call {
    Run {
        command: String,
        workdir: Option<String>,
        background: bool,
    },
    Output {
        identity: String,
        offset: u64,
        length: u64,
    },
    Accept {
        identity: String,
    },
}

fn parse_call(arguments: &Value) -> Result<Call, String> {
    let empty = Map::new();
    let args = match arguments {
        Value::Null => &empty,
        Value::Object(map) => map,
        _ => return Err("bash arguments must be an object".into()),
    };
    const KNOWN: &[&str] = &[
        "command",
        "workdir",
        "background",
        "output_identity",
        "output_offset",
        "output_length",
        "accept_output",
    ];
    if let Some(unknown) = args.keys().find(|key| !KNOWN.contains(&key.as_str())) {
        return Err(format!("unknown bash argument {unknown:?}"));
    }
    let text = |key: &str| -> Result<Option<String>, String> {
        match args.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(value)) => Ok(Some(value.clone())),
            Some(_) => Err(format!("{key} must be a string")),
        }
    };
    let flag = |key: &str| -> Result<Option<bool>, String> {
        match args.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::Bool(value)) => Ok(Some(*value)),
            Some(_) => Err(format!("{key} must be a boolean")),
        }
    };
    let number = |key: &str| -> Result<Option<u64>, String> {
        match args.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(value) => value
                .as_u64()
                .map(Some)
                .ok_or_else(|| format!("{key} must be a non-negative integer")),
        }
    };
    if let Some(identity) = text("output_identity")? {
        if args.contains_key("command")
            || args.contains_key("workdir")
            || args.contains_key("background")
        {
            return Err(
                "output_identity cannot be combined with command, workdir or background".into(),
            );
        }
        if flag("accept_output")? == Some(true) {
            if args.contains_key("output_offset") || args.contains_key("output_length") {
                return Err("accept_output takes no range".into());
            }
            return Ok(Call::Accept { identity });
        }
        let length = number("output_length")?.unwrap_or(1024);
        if length == 0 {
            return Err("output_length must be at least 1".into());
        }
        return Ok(Call::Output {
            identity,
            offset: number("output_offset")?.unwrap_or(0),
            length,
        });
    }
    if args.contains_key("output_offset")
        || args.contains_key("output_length")
        || args.contains_key("accept_output")
    {
        return Err("output ranges and acceptance need output_identity".into());
    }
    let command = text("command")?.ok_or("command is required")?;
    let workdir = text("workdir")?;
    if workdir.as_deref().is_some_and(|dir| !dir.starts_with('/')) {
        return Err("workdir must be absolute".into());
    }
    Ok(Call::Run {
        command,
        workdir,
        background: flag("background")?.unwrap_or(false),
    })
}

/// Running requesters by MCP request id, so a cancellation can end one.
#[derive(Default)]
struct Requesters {
    running: HashMap<String, Child>,
    cancelled: HashSet<String>,
    closed: bool,
}

type Running = Arc<Mutex<Requesters>>;

impl Requesters {
    fn stopped(&self, key: &str) -> bool {
        self.closed || self.cancelled.contains(key)
    }

    fn close(&mut self) {
        self.closed = true;
        for child in self.running.values_mut() {
            let _ = child.kill();
        }
    }
}

/// Serves one MCP connection. `lookup` reads this process's environment.
pub fn serve<R, W>(
    policy: ToolMediation,
    lookup: impl Fn(&str) -> Option<String> + Send + Sync + 'static,
    input: R,
    output: W,
) -> io::Result<()>
where
    R: BufRead,
    W: Write + Send + 'static,
{
    let output = Arc::new(Mutex::new(output));
    let send = {
        let output = Arc::clone(&output);
        move |value: Value| {
            let mut output = output.lock().expect("bridge output");
            let _ = writeln!(output, "{value}").and_then(|()| output.flush());
        }
    };
    let policy = Arc::new(policy);
    let lookup = Arc::new(lookup);
    let running: Running = Arc::default();
    let mut calls = Vec::new();
    let mut input = input;
    let read_result = loop {
        let mut line = Vec::new();
        let read = match (&mut input).take(MAX_LINE).read_until(b'\n', &mut line) {
            Ok(read) => read,
            Err(error) => break Err(error),
        };
        if read == 0 {
            break Ok(());
        }
        let Ok(message) = serde_json::from_slice::<Value>(&line) else {
            send(
                json!({"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Invalid JSON"}}),
            );
            continue;
        };
        let method = message["method"].as_str().unwrap_or_default();
        let id = message.get("id").cloned();
        if method == "notifications/cancelled" {
            let key = message["params"]["requestId"].to_string();
            let mut custody = running.lock().expect("requesters");
            custody.cancelled.insert(key.clone());
            if let Some(child) = custody.running.get_mut(&key) {
                let _ = child.kill();
            }
            continue;
        }
        let Some(id) = id else { continue };
        let reply = |result: Value| json!({"jsonrpc":"2.0","id":id,"result":result});
        match method {
            "initialize" => {
                let version = message["params"]["protocolVersion"]
                    .as_str()
                    .unwrap_or("2025-06-18");
                send(reply(json!({"protocolVersion":version,
                    "capabilities":{"tools":{"listChanged":false}},
                    "serverInfo":{"name":"oulipoly-tool-bridge","version":env!("CARGO_PKG_VERSION")}})));
            }
            "ping" => send(reply(json!({}))),
            "tools/list" => send(reply(json!({"tools":[tool_definition(&policy)]}))),
            "tools/call" => {
                if message["params"]["name"] != json!(TOOL) {
                    send(
                        json!({"jsonrpc":"2.0","id":id,"error":{"code":-32602,"message":"Unknown tool"}}),
                    );
                    continue;
                }
                let key = id.to_string();
                let arguments = message["params"]["arguments"].clone();
                let (policy, lookup, running, send) = (
                    Arc::clone(&policy),
                    Arc::clone(&lookup),
                    Arc::clone(&running),
                    send.clone(),
                );
                calls.push(thread::spawn(move || {
                    let answer = call(&policy, &*lookup, &arguments, &key, &running)?;
                    if running.lock().expect("requesters").stopped(&key) {
                        return Ok(());
                    }
                    send(json!({"jsonrpc":"2.0","id":id,"result":{
                        "content":[{"type":"text","text":answer.text}],"isError":answer.error}}));
                    Ok::<(), io::Error>(())
                }));
            }
            "resources/list" => send(reply(json!({"resources":[]}))),
            "resources/templates/list" => send(reply(json!({"resourceTemplates":[]}))),
            "prompts/list" => send(reply(json!({"prompts":[]}))),
            _ => send(
                json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"Method not found"}}),
            ),
        }
    };
    // Close the spawn gate before collecting workers, including on read failure.
    running.lock().expect("requesters").close();
    let mut result = read_result;
    for call in calls {
        let joined = call
            .join()
            .unwrap_or_else(|_| Err(io::Error::other("bridge worker panicked")));
        if result.is_ok() {
            result = joined;
        }
    }
    result
}

/// One tool call: decision, ingress, requester, rendering.
fn call(
    policy: &ToolMediation,
    lookup: &dyn Fn(&str) -> Option<String>,
    arguments: &Value,
    key: &str,
    running: &Running,
) -> io::Result<Answer> {
    let call = match parse_call(arguments) {
        Ok(call) => call,
        Err(reason) => {
            return Ok(answer(
                format!("Invalid bash call ({reason}). Nothing was run."),
                true,
            ))
        }
    };
    if let Call::Run { command, .. } = &call {
        if let Decision::Refuse(text) = policy.decide(command) {
            return Ok(answer(text, true));
        }
    }
    let ingress = match policy.ingress(lookup) {
        Ok(ingress) => ingress,
        Err(error) => {
            return Ok(answer(
                format!("Root Bash ingress unavailable: {error}. No requester was started."),
                true,
            ))
        }
    };
    let mut command = Command::new(&policy.requester);
    let (delivery, surface) = match &call {
        Call::Run {
            command: text,
            workdir,
            background,
        } => {
            let delivery = if *background { "async" } else { "sync" };
            command.args(["run", "--delivery", delivery, "--", "bash", "-lc", text]);
            if let Some(dir) = workdir {
                command.current_dir(dir);
            }
            (Some(delivery), ROOT_SURFACE)
        }
        Call::Output {
            identity,
            offset,
            length,
        } => {
            command.args(["native-output", identity, "--offset"]);
            command
                .arg(offset.to_string())
                .arg("--length")
                .arg(length.to_string());
            (None, OUTPUT_SURFACE)
        }
        Call::Accept { identity } => {
            command.args(["native-accept", identity]);
            (None, OUTPUT_SURFACE)
        }
    };
    command
        .env(&policy.ingress_env, ingress)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // Cancellation/EOF and spawn+registration are one transition. A worker
    // cannot publish a new child after shutdown's scan has finished.
    let mut custody = running.lock().expect("requesters");
    if custody.stopped(key) {
        return Ok(answer(
            "Requester cancelled before start. Nothing was run.",
            true,
        ));
    }
    if custody.running.contains_key(key) {
        return Ok(answer(
            "Duplicate active request id; no new requester started.",
            true,
        ));
    }
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            return Ok(answer(
                format!(
                    "Bash requester not started ({error}{}); nothing was run.",
                    match &call {
                        Call::Run {
                            workdir: Some(dir), ..
                        } if !Path::new(dir).is_dir() =>
                            format!("; workdir {dir:?} is not a directory"),
                        _ => String::new(),
                    }
                ),
                true,
            ))
        }
    };
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");
    custody.running.insert(key.to_owned(), child);
    drop(custody);
    let collected = collect(key, running, stdout, stderr);
    // Keep the requester available to cancellation until its exit is collected.
    let mut custody = running.lock().expect("requesters");
    let mut child = custody.running.remove(key).expect("requester registered");
    drop(custody);
    if collected.is_err() {
        let _ = child.kill();
        let started = Instant::now();
        while child.try_wait()?.is_none() {
            if started.elapsed() >= Duration::from_secs(2) {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "requester exit unconfirmed",
                ));
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
    // Collection (or bounded stop above) has already observed the exit, so
    // wait returns its cached status and cannot block on a still-live child.
    child.wait()?;
    let (code, bytes, stderr) = collected?;
    Ok(match delivery {
        Some(delivery) => render_run(code, &bytes, &stderr, delivery),
        None => render_output(code, &bytes, surface),
    })
}

/// Nonblocking pipe collection keeps cancellation independent of pipe EOF.
/// We collect the direct child even if a descendant holds a pipe open. Stops
/// and post-exit pipe drain have a two-second bound; an uncollected exit is an
/// explicit serve error rather than successful physical settlement.
#[cfg(unix)]
fn collect(
    key: &str,
    running: &Running,
    mut stdout: std::process::ChildStdout,
    mut stderr: std::process::ChildStderr,
) -> io::Result<(Option<i32>, Vec<u8>, String)> {
    use std::os::fd::AsRawFd;
    let setup = |fd| {
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if flags == -1 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    };
    let mut fault = setup(stdout.as_raw_fd())
        .and_then(|()| setup(stderr.as_raw_fd()))
        .err();
    let mut bytes = Vec::new();
    let mut errors = Vec::new();
    let mut out_eof = false;
    let mut err_eof = false;
    let mut status = None;
    let mut bound = None;
    loop {
        if fault.is_none() {
            if let Err(error) = drain(&mut stdout, &mut bytes, RESULT_LIMIT + 1, &mut out_eof)
                .and_then(|()| drain(&mut stderr, &mut errors, 64 * 1024, &mut err_eof))
            {
                fault = Some(error);
            }
        }
        let stopped;
        {
            let mut custody = running.lock().expect("requesters");
            stopped = custody.stopped(key);
            let child = custody.running.get_mut(key).expect("requester registered");
            if stopped || fault.is_some() || bytes.len() > RESULT_LIMIT {
                let _ = child.kill();
                bound.get_or_insert_with(Instant::now);
            }
            if status.is_none() {
                status = child.try_wait()?;
            }
        }
        if status.is_some() {
            if stopped || fault.is_some() || (out_eof && err_eof) {
                break;
            }
            bound.get_or_insert_with(Instant::now);
        }
        if bound.is_some_and(|start| start.elapsed() >= Duration::from_secs(2)) {
            if status.is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "requester exit unconfirmed after stop",
                ));
            }
            fault = Some(io::Error::new(
                io::ErrorKind::TimedOut,
                "requester pipes did not close",
            ));
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    let code = if fault.is_none() && bytes.len() <= RESULT_LIMIT {
        status.and_then(|status| status.code())
    } else {
        None
    };
    Ok((code, bytes, String::from_utf8_lossy(&errors).into_owned()))
}

#[cfg(unix)]
fn drain(
    reader: &mut impl Read,
    bytes: &mut Vec<u8>,
    limit: usize,
    eof: &mut bool,
) -> io::Result<()> {
    let mut buffer = [0; 8192];
    // Bound one pass so a continuously writing requester cannot starve stop.
    for _ in 0..32 {
        match reader.read(&mut buffer) {
            Ok(0) => {
                *eof = true;
                break;
            }
            Ok(count) => {
                bytes.extend_from_slice(&buffer[..count.min(limit.saturating_sub(bytes.len()))])
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn collect(
    _key: &str,
    _running: &Running,
    _stdout: std::process::ChildStdout,
    _stderr: std::process::ChildStderr,
) -> io::Result<(Option<i32>, Vec<u8>, String)> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "requester custody requires Unix",
    ))
}

fn unresolved(reason: &str, stderr: &str) -> Answer {
    answer(
        format!(
            "Root v1 result unresolved ({reason}); the command may have run; do not replay.{}",
            if stderr.is_empty() {
                String::new()
            } else {
                format!(
                    "\nrequester stderr: {}",
                    crate::encoding::bounded_text_bytes(stderr, 4096)
                )
            }
        ),
        true,
    )
}

fn stages(value: &Value) -> String {
    let stages: Vec<String> = value["stages"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|stage| {
            let name = stage["event"].as_str().unwrap_or("?");
            match stage["reason"].as_str() {
                Some(reason) => format!("{name}({reason})"),
                None => name.to_owned(),
            }
        })
        .collect();
    if stages.is_empty() {
        "stages: none".into()
    } else {
        format!("stages: {}", stages.join(" -> "))
    }
}

/// Only a positive diagnostic on the ordered accepted/started relation proves
/// the requested image failed to exec. The numeric wait is not such evidence.
fn exec_error(value: &Value) -> Result<Option<&str>, ()> {
    let stages = value["stages"].as_array().ok_or(())?;
    let mut started = stages
        .iter()
        .enumerate()
        .filter(|(_, s)| s["event"] == "started");
    let Some((index, stage)) = started.next() else {
        return Ok(None);
    };
    if started.next().is_some() {
        return Err(());
    }
    let error = match stage.get("exec_error") {
        None | Some(Value::Null) => return Ok(None),
        Some(Value::String(error)) if !error.is_empty() => error.as_str(),
        _ => return Err(()),
    };
    let accepted = &stages[0];
    if index != 1
        || accepted["event"] != "accepted"
        || accepted["durable"] != true
        || !accepted["root_id"].as_str().is_some_and(|s| !s.is_empty())
        || !accepted["work"].as_i64().is_some_and(|w| w > 0)
    {
        return Err(());
    }
    Ok(Some(error))
}

fn failed_exec(error: &str) -> String {
    format!(
        "Root v1 accepted work and created its child, but exec of the requested program failed: {}. The requested program did not run. Setup effects remain possible; retry safety is not established. Do not replay.",
        crate::encoding::bounded_text_bytes(error, 4096)
    )
}

fn completion_owed(value: &Value) -> bool {
    let accepted = &value["stages"][0];
    let detached = &value["stages"][2];
    value["stages"].as_array().is_some_and(|s| s.len() == 3)
        && accepted["delivery"] == "async"
        && detached["event"] == "detached"
        && detached["work"] == accepted["work"]
        && detached["completion"] == "owed-to-requesting-harness"
        && value["completion"]["delivery"] == "owed-to-requesting-harness"
        && value["completion"]["root_id"] == accepted["root_id"]
        && value["completion"]["work"] == accepted["work"]
        && value["output"]["reference"]
            == format!(
                "rv1w:{}:{}",
                accepted["root_id"].as_str().unwrap_or_default(),
                accepted["work"]
            )
}

fn shown_count(bytes: &[u8]) -> usize {
    if std::str::from_utf8(bytes).is_err() || bytes.contains(&0) {
        return bytes.len().min(SHOWN_BYTES / 2);
    }
    let mut cut = bytes.len().min(SHOWN_BYTES);
    while cut < bytes.len() && cut > 0 && (bytes[cut] & 0xc0) == 0x80 {
        cut -= 1;
    }
    cut
}

fn shown(bytes: &[u8]) -> String {
    let cut = shown_count(bytes);
    match std::str::from_utf8(bytes) {
        Ok(_) if !bytes.contains(&0) => {
            let more = bytes.len() - cut;
            format!(
                "--- output (stderr joined; {cut} of {} bytes shown, utf8) ---\n{}{}",
                bytes.len(),
                String::from_utf8_lossy(&bytes[..cut]),
                if more > 0 {
                    format!("\n({more} more bytes not shown)")
                } else {
                    String::new()
                }
            )
        }
        _ => {
            let hex: String = bytes[..cut].iter().map(|b| format!("{b:02x}")).collect();
            format!(
                "--- output (stderr joined; {cut} of {} bytes shown, hex) ---\n{hex}",
                bytes.len()
            )
        }
    }
}

fn retention(output: &Value) -> String {
    let record = &output["retained"];
    if let Some(identity) = record["identity"]
        .as_str()
        .filter(|_| valid_retained(record))
    {
        return format!(
            "\nOwner retention: {}, {} bytes; identity={identity}. Read more with {{\"output_identity\": \"{identity}\", \"output_offset\": 0, \"output_length\": 1024}}.",
            record["state"].as_str().unwrap_or("unknown"),
            record["bytes"]
        );
    }
    match output["reference"].as_str() {
        Some(reference) => format!("\nOwner output reference={reference}; seal not observed."),
        None => String::new(),
    }
}

/// Renders a `run` result object; `code` is the requester's exit code when
/// it exited and its result was read whole.
pub fn render_run(code: Option<i32>, stdout: &[u8], stderr: &str, delivery: &str) -> Answer {
    if code != Some(0) {
        return unresolved(
            &format!("requester did not complete its result: {code:?}"),
            stderr,
        );
    }
    let Ok(value) = serde_json::from_slice::<Value>(stdout) else {
        return unresolved("result is not one JSON object", stderr);
    };
    if value["result_surface"] != json!(ROOT_SURFACE)
        || value["version"] != json!(1)
        || value["delivery_mode"] != json!(delivery)
        || !value["stages"].is_array()
    {
        return unresolved("result surface invalid", stderr);
    }
    let exec_error = match exec_error(&value) {
        Ok(error) => error,
        Err(()) => return unresolved("invalid exec diagnostic or stage relation", stderr),
    };
    if exec_error.is_some()
        && (value["effects_possible"] != true
            || value["retry_safe"] != false
            || !matches!(
                value["outcome"].as_str(),
                Some("unknown" | "running" | "ended" | "ended-output-unproven")
            ))
    {
        return unresolved("exec failure contradicts custody facts", stderr);
    }
    let stages = stages(&value);
    let output = &value["output"];

    let refusal = &value["refusal"];
    match (delivery, value["outcome"].as_str().unwrap_or_default()) {
        (_, "refused") if value["effects_possible"] == json!(false)
            && refusal["by"].as_str().is_some_and(|s| !s.is_empty())
            && refusal["reason"].as_str().is_some_and(|s| !s.is_empty()) => answer(
            format!(
                "Root v1 refused by {} ({}){}. Nothing was run.\n{stages}",
                refusal["by"].as_str().unwrap_or("owner"),
                refusal["reason"].as_str().unwrap_or("unknown"),
                refusal["detail"]
                    .as_str()
                    .map(|detail| format!(": {detail}"))
                    .unwrap_or_default()
            ),
            true,
        ),
        (_, "not-started") if value["effects_possible"] == json!(false) => answer(
            format!("Root v1 accepted the command, then reported a positive no-start; nothing was run.\n{stages}"),
            true,
        ),
        ("async", "unknown" | "running") if exec_error.is_some() => answer(
            format!("{} {}\n{stages}{}",
                failed_exec(exec_error.expect("positive diagnostic")),
                if completion_owed(&value) {
                    "Work end, wait and output are owed as a later input to this conversation; do not poll."
                } else {
                    "Work end, wait, output and completion delivery remain unconfirmed."
                }, retention(output)),
            true,
        ),
        (_, "unknown") => answer(
            format!(
                "{}Work outcome unknown ({}). Do not replay.\n{stages}{}",
                exec_error.map(|error| format!("{} ", failed_exec(error)))
                    .unwrap_or_else(|| "Root v1: the command may have run. ".into()),
                value["meaning"].as_str().unwrap_or("unknown"), retention(output)
            ),
            true,
        ),
        ("async", "running") if value["effects_possible"] == json!(true)
            && output["reference"].as_str().is_some_and(|s| s.starts_with("rv1w:") && s.len() > 5) => answer(
            format!(
                "Root v1 background work accepted and started (reference={}); it is still running. Its end will arrive later in this conversation as a separate message; do not poll for it.\n{stages}",
                output["reference"].as_str().unwrap_or("unknown")
            ),
            false,
        ),
        ("sync", outcome @ ("ended" | "ended-output-unproven")) => {
            let wait = &value["wait"];
            let Some(exit) = waited_exit(wait) else {
                return unresolved("missing or inconsistent waited exit", stderr);
            };
            let Some(bytes) = output["base64"].as_str().and_then(|s| decode_base64(s).ok()) else {
                return unresolved("invalid output bytes", stderr);
            };
            let Some(total) = output["bytes"].as_u64() else {
                return unresolved("missing output byte count", stderr);
            };
            let delivery = output["delivery"].as_str();
            if total < bytes.len() as u64
                || (outcome == "ended" && !matches!(delivery, Some("complete" | "partial")))
                || (delivery == Some("complete") && total != bytes.len() as u64)
                || (outcome == "ended-output-unproven" && delivery != Some("unproven"))
            {
                return unresolved("output facts inconsistent", stderr);
            }
            let delivered = if outcome == "ended" {
                format!("output {}", output["delivery"].as_str().unwrap_or("unknown"))
            } else {
                "output delivery unproven: the output below may be incomplete; do not replay".into()
            };
            answer(
                format!(
                    "{}work ended: {exit} ({}, observer {}); {delivered}.\n{stages}{}\n{}",
                    exec_error.map(|error| format!("{}\n", failed_exec(error)))
                        .unwrap_or_else(|| "Root v1 ".into()),
                    wait["status"].as_str().unwrap_or("unknown"),
                    wait["observer"].as_str().unwrap_or("unknown"),
                    retention(output),
                    shown(&bytes)
                ),
                exec_error.is_some() || outcome == "ended-output-unproven",
            )
        }
        _ => unresolved("result outcome inconsistent", stderr),
    }
}

fn waited_exit(wait: &Value) -> Option<String> {
    let status = wait["status"].as_str()?;
    if wait["observer"].as_str()?.is_empty() {
        return None;
    }
    match (
        wait["exit"]["code"].as_i64(),
        wait["exit"]["signal"].as_i64(),
    ) {
        (Some(code), None)
            if i32::try_from(code).is_ok()
                && wait["exit"]["signal"].is_null()
                && status == format!("code:{code}") =>
        {
            Some(format!("exited with code {code}"))
        }
        (None, Some(signal))
            if signal > 0
                && i32::try_from(signal).is_ok()
                && wait["exit"]["code"].is_null()
                && status == format!("signal:{signal}") =>
        {
            Some(format!("signaled with signal {signal}"))
        }
        _ => None,
    }
}

/// The displayed retention identity must agree with its reported facts.
fn valid_retained(record: &Value) -> bool {
    let (Some(identity), Some(root), Some(work), Some(bytes), Some(hash)) = (
        record["identity"].as_str(),
        record["root_id"].as_str(),
        record["work"].as_u64(),
        record["bytes"].as_u64(),
        record["sha256"].as_str(),
    ) else {
        return false;
    };
    !root.is_empty()
        && root.len() <= 64
        && root
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        && work > 0
        && work <= i64::MAX as u64
        && bytes <= RESULT_LIMIT as u64
        && hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        && identity == format!("rv1o:{root}:{work}:{bytes}:{hash}")
        && matches!(record["state"].as_str(), Some("complete" | "partial"))
        && record["losses"].is_array()
}

/// Renders a `native-output` / `native-accept` result object.
pub fn render_output(code: Option<i32>, stdout: &[u8], surface: &str) -> Answer {
    let unknown = answer(
        "Retained output reply unresolved; local acceptance unconfirmed. No command was run or replayed.",
        true,
    );
    if code != Some(0) {
        return unknown;
    }
    let Ok(value) = serde_json::from_slice::<Value>(stdout) else {
        return unknown;
    };
    if value["result_surface"] != json!(surface) || value["version"] != json!(1) {
        return unknown;
    }
    let reply = &value["reply"];
    match value["outcome"].as_str().unwrap_or_default() {
        "accepted" if valid_retained(&reply["retained"])
            && reply["event"] == json!("output-accepted")
            && reply["durable"] == json!(true) && reply["repeat"].is_boolean()
            && ["root_id", "work", "bytes", "sha256"].iter()
                .all(|key| reply["receipt"][*key] == reply["retained"][*key]) => answer(
            format!(
                "Root v1 exact local acceptance: {}; durable={}; repeat={}. Not an input ACK, processing, remote settlement or drain.",
                reply["retained"]["identity"], reply["durable"], reply["repeat"]
            ),
            false,
        ),
        "read" if valid_retained(&reply["retained"]) && reply["event"] == json!("output-range") => {
            let Some(bytes) = reply["b64"].as_str().and_then(|s| decode_base64(s).ok()) else { return unknown };
            let (Some(offset), Some(next), Some(length), Some(eof)) = (
                reply["offset"].as_u64(), reply["next_offset"].as_u64(),
                reply["length"].as_u64(), reply["eof"].as_bool(),
            ) else { return unknown };
            let total = reply["retained"]["bytes"].as_u64().expect("validated retained bytes");
            if offset.checked_add(length) != Some(next) || length != bytes.len() as u64
                || next > total || eof != (next == total)
            { return unknown }
            let visible_next = offset + shown_count(&bytes) as u64;
            let visible_eof = eof && visible_next == next;
            answer(
                format!(
                    "Root v1 retained range: identity={}; offset={offset}; next_offset={visible_next}; eof={visible_eof}. Continuation follows displayed bytes; requester range ended at {next}. No local acceptance was recorded by this read.\n{}",
                    reply["retained"]["identity"], shown(&bytes)
                ),
                false,
            )
        }
        outcome @ ("refused" | "unknown") => answer(
            format!(
                "Root v1 output {outcome}: {}. Local acceptance unconfirmed; no command was run or replayed.",
                value["reason"].as_str().unwrap_or("unknown")
            ),
            true,
        ),
        _ => unknown,
    }
}

/// The bridge's process entry: the policy from this process's
/// [`tool_mediation::ENV`], served on stdin/stdout. Exit 2 when no valid
/// policy is present: the native agent then has no working command tool,
/// never an unrestricted one.
pub fn main() -> i32 {
    let policy = match std::env::var(tool_mediation::ENV) {
        Ok(text) => match ToolMediation::decode(&text) {
            Ok(policy) => policy,
            Err(error) => {
                eprintln!("tool bridge refused: {error}");
                return 2;
            }
        },
        Err(_) => {
            eprintln!("tool bridge refused: {} is not set", tool_mediation::ENV);
            return 2;
        }
    };
    let stdin = io::stdin();
    match serve(
        policy,
        |name| std::env::var(name).ok(),
        stdin.lock(),
        io::stdout(),
    ) {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("tool bridge failed: {error}");
            1
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn stop_before_spawn_blocks_late_registration_for_cancel_and_eof() {
        for eof in [false, true] {
            let policy = ToolMediation::decode(
                &json!({"protocol":tool_mediation::PROTOCOL,
                "bash":{"authority":"trusted-task"}, "requester":"/must-not-start",
                "ingress_env":"INGRESS"})
                .to_string(),
            )
            .unwrap();
            let running: Running = Arc::default();
            let worker_state = Arc::clone(&running);
            let (entered, ready) = std::sync::mpsc::channel();
            let (release, released) = std::sync::mpsc::channel();
            let worker = thread::spawn(move || {
                call(
                    &policy,
                    &|_| {
                        entered.send(()).unwrap();
                        released.recv().unwrap();
                        Some("ingress".into())
                    },
                    &json!({"command":"hang"}),
                    "7",
                    &worker_state,
                )
            });
            ready.recv_timeout(Duration::from_secs(2)).unwrap();
            {
                let mut state = running.lock().unwrap();
                if eof {
                    state.close();
                } else {
                    state.cancelled.insert("7".into());
                }
            }
            release.send(()).unwrap();
            let result = worker.join().unwrap().unwrap();
            assert!(result.text.contains("cancelled before start"), "{result:?}");
            assert!(running.lock().unwrap().running.is_empty());
        }
    }
}
