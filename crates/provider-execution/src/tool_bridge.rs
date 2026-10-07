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
use std::collections::HashMap;
use std::io::{self, BufRead, Read, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;

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
type Running = Arc<Mutex<HashMap<String, Child>>>;

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
    let cancelled: Arc<Mutex<Vec<String>>> = Arc::default();
    let mut calls = Vec::new();
    let mut input = input;
    loop {
        let mut line = Vec::new();
        let read = (&mut input).take(MAX_LINE).read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
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
            cancelled.lock().expect("cancelled").push(key.clone());
            if let Some(child) = running.lock().expect("running").get_mut(&key) {
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
                let (policy, lookup, running, cancelled, send) = (
                    Arc::clone(&policy),
                    Arc::clone(&lookup),
                    Arc::clone(&running),
                    Arc::clone(&cancelled),
                    send.clone(),
                );
                calls.push(thread::spawn(move || {
                    let answer = call(&policy, &*lookup, &arguments, &key, &running);
                    if cancelled.lock().expect("cancelled").contains(&key) {
                        return;
                    }
                    send(json!({"jsonrpc":"2.0","id":id,"result":{
                        "content":[{"type":"text","text":answer.text}],"isError":answer.error}}));
                }));
            }
            "resources/list" => send(reply(json!({"resources":[]}))),
            "resources/templates/list" => send(reply(json!({"resourceTemplates":[]}))),
            "prompts/list" => send(reply(json!({"prompts":[]}))),
            _ => send(
                json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"Method not found"}}),
            ),
        }
    }
    // Connection end: no caller remains for any running requester.
    for child in running.lock().expect("running").values_mut() {
        let _ = child.kill();
    }
    for call in calls {
        let _ = call.join();
    }
    Ok(())
}

/// One tool call: decision, ingress, requester, rendering.
fn call(
    policy: &ToolMediation,
    lookup: &dyn Fn(&str) -> Option<String>,
    arguments: &Value,
    key: &str,
    running: &Running,
) -> Answer {
    let call = match parse_call(arguments) {
        Ok(call) => call,
        Err(reason) => {
            return answer(
                format!("Invalid bash call ({reason}). Nothing was run."),
                true,
            )
        }
    };
    if let Call::Run { command, .. } = &call {
        if let Decision::Refuse(text) = policy.decide(command) {
            return answer(text, true);
        }
    }
    let ingress = match policy.ingress(lookup) {
        Ok(ingress) => ingress,
        Err(error) => {
            return answer(
                format!("Root Bash ingress unavailable: {error}. No requester was started."),
                true,
            )
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
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            return answer(
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
            )
        }
    };
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");
    running
        .lock()
        .expect("running")
        .insert(key.to_owned(), child);
    let err = thread::spawn(move || {
        let mut bytes = Vec::new();
        let _ = stderr.take(64 * 1024).read_to_end(&mut bytes);
        String::from_utf8_lossy(&bytes).into_owned()
    });
    let mut bytes = Vec::new();
    let read = stdout.take(RESULT_LIMIT as u64 + 1).read_to_end(&mut bytes);
    let mut child = running
        .lock()
        .expect("running")
        .remove(key)
        .expect("requester registered");
    if bytes.len() > RESULT_LIMIT {
        let _ = child.kill();
    }
    let status = child.wait();
    let stderr = err.join().unwrap_or_default();
    let code = match (&read, &status) {
        (Ok(_), Ok(status)) if bytes.len() <= RESULT_LIMIT => status.code(),
        _ => None,
    };
    match delivery {
        Some(delivery) => render_run(code, &bytes, &stderr, delivery),
        None => render_output(code, &bytes, surface),
    }
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

fn shown(bytes: &[u8]) -> String {
    let mut cut = bytes.len().min(SHOWN_BYTES);
    match std::str::from_utf8(bytes) {
        Ok(_) if !bytes.contains(&0) => {
            while cut < bytes.len() && cut > 0 && (bytes[cut] & 0xc0) == 0x80 {
                cut -= 1;
            }
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
            let cut = cut.min(SHOWN_BYTES / 2);
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
    if let Some(identity) = record["identity"].as_str() {
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
    let stages = stages(&value);
    let output = &value["output"];
    let bytes = output["base64"]
        .as_str()
        .map(decode_base64)
        .transpose()
        .unwrap_or(None)
        .unwrap_or_default();
    let refusal = &value["refusal"];
    match (delivery, value["outcome"].as_str().unwrap_or_default()) {
        (_, "refused") => answer(
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
        (_, "not-started") => answer(
            format!("Root v1 accepted the command, then reported a positive no-start; nothing was run.\n{stages}"),
            true,
        ),
        (_, "unknown") => answer(
            format!(
                "Root v1 outcome unknown ({}): the command may have run. Do not replay.\n{stages}{}",
                value["meaning"].as_str().unwrap_or("unknown"),
                retention(output)
            ),
            true,
        ),
        ("async", "running") if value["effects_possible"] == json!(true) => answer(
            format!(
                "Root v1 background work accepted and started (reference={}); it is still running. Its end will arrive later in this conversation as a separate message; do not poll for it.\n{stages}",
                output["reference"].as_str().unwrap_or("unknown")
            ),
            false,
        ),
        ("sync", outcome @ ("ended" | "ended-output-unproven")) if value["wait"].is_object() => {
            let wait = &value["wait"];
            let exit = match wait["exit"]["code"].as_i64() {
                Some(code) => format!("exited with code {code}"),
                None => format!("signaled with signal {}", wait["exit"]["signal"]),
            };
            let delivered = if outcome == "ended" {
                format!("output {}", output["delivery"].as_str().unwrap_or("unknown"))
            } else {
                "output delivery unproven: the output below may be incomplete; do not replay".into()
            };
            answer(
                format!(
                    "Root v1 work ended: {exit} ({}, observer {}); {delivered}.\n{stages}{}\n{}",
                    wait["status"].as_str().unwrap_or("unknown"),
                    wait["observer"].as_str().unwrap_or("unknown"),
                    retention(output),
                    shown(&bytes)
                ),
                false,
            )
        }
        _ => unresolved("result outcome inconsistent", stderr),
    }
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
        "accepted" => answer(
            format!(
                "Root v1 exact local acceptance: {}; durable={}; repeat={}. Not an input ACK, processing, remote settlement or drain.",
                reply["retained"]["identity"], reply["durable"], reply["repeat"]
            ),
            false,
        ),
        "read" => {
            let bytes = reply["b64"]
                .as_str()
                .map(decode_base64)
                .transpose()
                .unwrap_or(None)
                .unwrap_or_default();
            answer(
                format!(
                    "Root v1 retained range: identity={}; offset={}; next_offset={}; eof={}. No local acceptance was recorded by this read.\n{}",
                    reply["retained"]["identity"], reply["offset"], reply["next_offset"], reply["eof"],
                    shown(&bytes)
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
