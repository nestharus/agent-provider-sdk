//! Resident ACP v2 endpoint through a real provider process.
//!
//! The test binary is its own resident provider. Invoked with `--resident`, it
//! serves one ACP v2 connection on stdin/stdout with a fixture adapter whose
//! turns run through `run_launch_until` over a `/bin/sh` fake native program.
//! The fake reads its prompt on stdin and follows it: `reply <text>`,
//! `whoami`, `hang <dir>` (consumes, starts a descendant in its group, then
//! waits), `noconsume` and `fail`. Every native start appends to
//! `runs.log`, so a test can prove that no input ran twice. Tests speak ACP
//! v2 to the provider as a separate process, so process-group custody,
//! provider loss and connection end are real.
#![cfg(target_os = "linux")]

use agent_provider_contract::resident_session as extension;
use agent_provider_execution::custody::RequestCustody;
use agent_provider_execution::lifecycle::{
    run_launch_until, Channel, EventSink, LaunchAdapter, LaunchSpec, LifecycleError,
    LifecycleTiming, NativeCommand, NativeOutcome, OutputFraming, Preparation, StopCause, Terminal,
};
use agent_provider_execution::process::{run_effect_gate, EffectGate, GatedCommand};
use agent_provider_execution::resident::{
    self, ResidentTurns, TurnFailure, TurnFailureKind, TurnRequest,
};
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::AtomicBool;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

const GATE_ARG: &str = "__resident_fixture_gate";
const GATE_ENV: &str = "RESIDENT_FIXTURE_EFFECT_GATE_FD";
const TIMEOUT: Duration = Duration::from_secs(20);

const NATIVE: &str = r#"
echo "$$" >> "$RUNS"
prompt=$(cat)
set -- $prompt
case "$1" in
  reply) shift; echo CONSUMED; [ -z "$NATIVE_SESSION" ] && echo "SESSION native-$$"; echo "TEXT $*"; exit 0 ;;
  barrier) echo $$ > "$2/ready"
        while [ ! -f "$2/release" ]; do sleep 0.02; done
        [ -z "$NATIVE_SESSION" ] && echo "SESSION native-$$"
        echo CONSUMED; echo "TEXT barrier done"; exit 0 ;;
  whoami) echo CONSUMED; echo "TEXT native=$NATIVE_SESSION"; exit 0 ;;
  hang) echo CONSUMED; [ -z "$NATIVE_SESSION" ] && echo "SESSION native-$$"
        sleep 300 & echo $! > "$2/descendant.pid"; echo $$ > "$2/leader.pid"
        echo "TEXT waiting"; wait ;;
  noconsume) echo "warming up" >&2; exit 3 ;;
  fail) echo CONSUMED; echo "TEXT partial"; exit 1 ;;
  *) echo CONSUMED; echo "TEXT unknown"; exit 0 ;;
esac
"#;

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    match args.get(1).map(String::as_str) {
        Some(GATE_ARG) => std::process::exit(run_effect_gate(&args, GATE_ENV)),
        Some("--resident") => std::process::exit(serve(&args[2])),
        _ => {}
    }
    let tests: [(&str, fn()); 28] = [
        (
            "initialize_serves_only_v2_with_dedup_and_resident_contract",
            initialize_serves_only_v2_with_dedup_and_resident_contract,
        ),
        (
            "requests_before_initialize_and_unknown_methods_are_refused",
            requests_before_initialize_and_unknown_methods_are_refused,
        ),
        (
            "consumption_acknowledges_and_turn_end_is_attributed",
            consumption_acknowledges_and_turn_end_is_attributed,
        ),
        (
            "later_turns_continue_the_recorded_native_session",
            later_turns_continue_the_recorded_native_session,
        ),
        (
            "duplicate_key_inserts_nothing_and_repeats_its_turn_end",
            duplicate_key_inserts_nothing_and_repeats_its_turn_end,
        ),
        (
            "turn_without_consumption_is_not_acknowledged",
            turn_without_consumption_is_not_acknowledged,
        ),
        (
            "native_failure_is_a_tagged_failed_turn_end",
            native_failure_is_a_tagged_failed_turn_end,
        ),
        (
            "cancel_stops_the_turn_settles_descendants_and_refuses_queued_input",
            cancel_stops_the_turn_settles_descendants_and_refuses_queued_input,
        ),
        (
            "connection_end_settles_the_running_turn",
            connection_end_settles_the_running_turn,
        ),
        (
            "lost_provider_is_reconciled_on_resume_and_never_rerun",
            lost_provider_is_reconciled_on_resume_and_never_rerun,
        ),
        (
            "close_settles_and_refuses_later_input",
            close_settles_and_refuses_later_input,
        ),
        ("cancel_is_session_scoped", cancel_is_session_scoped),
        (
            "recovery_precedes_current_adapter_admission",
            recovery_precedes_current_adapter_admission,
        ),
        (
            "queued_turn_selects_settled_native_session",
            queued_turn_selects_settled_native_session,
        ),
        (
            "consumption_store_failure_is_unknown",
            consumption_store_failure_is_unknown,
        ),
        (
            "close_releases_session_ownership",
            close_releases_session_ownership,
        ),
        (
            "native_session_publish_failure_keeps_custody_unsettled",
            native_session_publish_failure_keeps_custody_unsettled,
        ),
        (
            "refused_preparation_is_not_an_insertion",
            refused_preparation_is_not_an_insertion,
        ),
        (
            "insertion_missing_launch_preserves_ack_and_restored_reconciliation",
            insertion_missing_launch_preserves_ack_and_restored_reconciliation,
        ),
        (
            "custody_corrupt_own_input_settles_without_reconstruction",
            custody_corrupt_own_input_settles_without_reconstruction,
        ),
        (
            "custody_unreadable_launch_refuses_close_and_eof",
            custody_unreadable_launch_refuses_close_and_eof,
        ),
        (
            "bounds_admission_refuses_before_effects",
            bounds_admission_refuses_before_effects,
        ),
        (
            "bounds_unreadable_input_does_not_block_other_actor",
            bounds_unreadable_input_does_not_block_other_actor,
        ),
        (
            "bounds_large_journal_recovers_consumption",
            bounds_large_journal_recovers_consumption,
        ),
        (
            "bounds_large_journal_recovers_native_session",
            bounds_large_journal_recovers_native_session,
        ),
        (
            "bounds_corrupt_journal_preserves_known_insertion_and_reports_uncertainty",
            bounds_corrupt_journal_preserves_known_insertion_and_reports_uncertainty,
        ),
        (
            "bounds_wire_limit_settles_running_actor",
            bounds_wire_limit_settles_running_actor,
        ),
        (
            "bounds_missing_marker_differs_from_unreadable_journal",
            bounds_missing_marker_differs_from_unreadable_journal,
        ),
    ];
    let filter = args.get(1).map(String::as_str).unwrap_or("");
    let mut selected = 0;
    let mut failed = 0;
    for (name, test) in tests {
        if !name.contains(filter) {
            continue;
        }
        selected += 1;
        match std::panic::catch_unwind(test) {
            Ok(()) => println!("test {name} ... ok"),
            Err(_) => {
                println!("test {name} ... FAILED");
                failed += 1;
            }
        }
    }
    println!(
        "\ntest result: {}. {} passed; {failed} failed",
        if failed == 0 { "ok" } else { "FAILED" },
        selected - failed
    );
    std::process::exit(i32::from(failed != 0));
}

// ---- fixture resident provider ---------------------------------------------

struct FixtureTurns {
    runs: PathBuf,
}

impl ResidentTurns for FixtureTurns {
    fn implementation(&self) -> (String, String) {
        ("fixture-resident".into(), "0".into())
    }

    fn run_turn(
        &self,
        turn: &TurnRequest,
        stop: &AtomicBool,
        events: &mut dyn Write,
    ) -> Result<i32, TurnFailure> {
        if self
            .runs
            .parent()
            .unwrap()
            .join("refuse-current-policy")
            .exists()
        {
            return Err(TurnFailure {
                kind: TurnFailureKind::Failed,
                code: "current_policy_refused".into(),
                message: "changed policy refused before lifecycle".into(),
            });
        }
        let spec = LaunchSpec {
            contract: "oulipoly.provider/v1",
            request_id: &turn.request_id,
            provider_instance_id: None,
            deadline_unix_ms: None,
            state_root: &turn.state_root,
            timing: LifecycleTiming {
                poll_interval: Duration::from_millis(20),
                heartbeat_interval: None,
                drain_grace: Duration::from_millis(500),
            },
        };
        let mut adapter = FixtureTurn {
            turn: turn.clone(),
            runs: self.runs.clone(),
        };
        let mut events = events;
        run_launch_until(&spec, stop, &mut adapter, &mut events).map_err(|failure| failure.0)
    }
}

struct FixtureTurn {
    turn: TurnRequest,
    runs: PathBuf,
}

struct Failure(TurnFailure);

impl From<LifecycleError> for Failure {
    fn from(error: LifecycleError) -> Self {
        let kind = match error {
            LifecycleError::Cancelled => TurnFailureKind::Cancelled,
            LifecycleError::ReconciliationRequired => TurnFailureKind::ReconciliationRequired,
            _ => TurnFailureKind::Failed,
        };
        Self(TurnFailure {
            kind,
            code: format!("{error:?}"),
            message: error.to_string(),
        })
    }
}

impl LaunchAdapter for FixtureTurn {
    type Failure = Failure;

    fn request_digest(&mut self) -> Result<String, Failure> {
        let turn = &self.turn;
        Ok(format!(
            "{:?}",
            (&turn.prompt, &turn.native_session_id, &turn.cwd)
        ))
    }

    fn prepare(&mut self, _custody: &RequestCustody) -> Result<Preparation, Failure> {
        if self.turn.prompt.starts_with("refuse") {
            return Err(Failure(TurnFailure {
                kind: TurnFailureKind::Failed,
                code: "fixture_refused".into(),
                message: "refused before native start".into(),
            }));
        }
        let executable = std::env::current_exe().unwrap();
        let mut command = GatedCommand::new(
            &EffectGate {
                executable: &executable,
                argument: GATE_ARG,
                descriptor_env: GATE_ENV,
            },
            "/bin/sh",
            ["-c", NATIVE],
        )
        .unwrap();
        command
            .command_mut()
            .current_dir(&self.turn.cwd)
            .env("RUNS", &self.runs)
            .env(
                "NATIVE_SESSION",
                self.turn.native_session_id.as_deref().unwrap_or(""),
            );
        Ok(Preparation::Native(NativeCommand {
            command,
            stdin: Some(self.turn.prompt.clone().into_bytes()),
            framing: OutputFraming::Lines { max_bytes: 4096 },
        }))
    }

    fn output<W: Write>(
        &mut self,
        channel: Channel,
        bytes: Vec<u8>,
        events: &mut EventSink<'_, W>,
    ) -> Result<(), Failure> {
        let line = String::from_utf8_lossy(&bytes).trim_end().to_owned();
        match (channel, line.split_once(' ')) {
            (Channel::Stdout, _) if line == "CONSUMED" => events.marker(
                resident::SUBMITTED_USER_TURN_MARKER,
                json!({"source":"fixture"}),
            )?,
            (Channel::Stdout, Some(("SESSION", id))) => events.marker(
                resident::PROVIDER_SESSION_MARKER,
                json!({"provider_session_id":id}),
            )?,
            (Channel::Stdout, Some(("TEXT", text))) => {
                events.data(Channel::Stdout, format!("{text}\n").as_bytes())?
            }
            _ => events.data(Channel::Stderr, &bytes)?,
        }
        Ok(())
    }

    fn finish<W: Write>(
        &mut self,
        outcome: NativeOutcome,
        events: &mut EventSink<'_, W>,
    ) -> Result<Terminal, Failure> {
        let mut summary = events.accounting().to_json();
        summary["protocol"] = json!("oulipoly.launch_output/v1");
        events.marker(resident::LAUNCH_OUTPUT_COMPLETE_MARKER, summary)?;
        let status = match outcome.stopped {
            Some(StopCause::Requested | StopCause::Cancelled { .. }) => json!({"kind":"cancelled"}),
            Some(StopCause::Deadline) => json!({"kind":"timeout"}),
            None => json!({"kind":"exited","code":outcome.status.code().unwrap_or(1)}),
        };
        Ok(Terminal {
            status,
            terminal_signal: json!({"kind":"fixture"}),
            session: None,
            exit_code: 0,
        })
    }
}

fn serve(state_root: &str) -> i32 {
    let state_root = PathBuf::from(state_root);
    let turns = Arc::new(FixtureTurns {
        runs: state_root.join("runs.log"),
    });
    let stdin = BufReader::new(std::io::stdin());
    match resident::serve(turns, &state_root, stdin, std::io::stdout()) {
        Ok(_) => 0,
        Err(error) => {
            eprintln!("serve failed: {error}");
            1
        }
    }
}

// ---- ACP test client --------------------------------------------------------

struct Fixture {
    dir: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::Builder::new()
            .prefix("u108-resident-bounds-fixture-")
            .tempdir_in(std::env::temp_dir())
            .unwrap();
        std::fs::create_dir(dir.path().join("work")).unwrap();
        Self { dir }
    }
    fn state(&self) -> PathBuf {
        self.dir.path().join("state")
    }
    fn cwd(&self) -> String {
        self.dir.path().join("work").display().to_string()
    }
    fn runs(&self) -> usize {
        std::fs::read_to_string(self.state().join("runs.log"))
            .map(|log| log.lines().count())
            .unwrap_or(0)
    }
    fn start(&self) -> Client {
        Client::start(&self.state())
    }
}

// Optional construction-witness export, kept separate from test assertions.
impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(root) = std::env::var_os("U108_EVIDENCE_DIR") {
            fn copy(from: &Path, to: &Path) {
                std::fs::create_dir_all(to).unwrap();
                for entry in std::fs::read_dir(from).unwrap() {
                    let entry = entry.unwrap();
                    let target = to.join(entry.file_name());
                    if entry.file_type().unwrap().is_dir() {
                        copy(&entry.path(), &target);
                    } else {
                        std::fs::copy(entry.path(), target).unwrap();
                    }
                }
            }
            let to = PathBuf::from(root).join(self.dir.path().file_name().unwrap());
            copy(self.dir.path(), &to);
            println!("fixture evidence: {}", to.display());
        }
    }
}

struct Client {
    child: Child,
    stdin: Option<ChildStdin>,
    messages: mpsc::Receiver<Value>,
    seen: Vec<Value>,
    next_id: u64,
}

impl Client {
    fn start(state: &Path) -> Self {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--resident")
            .arg(state)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let observations = std::env::var_os("U108_EVIDENCE_DIR").map(|_| {
            std::fs::File::create(
                state
                    .parent()
                    .unwrap()
                    .join(format!("connection-{}.jsonl", child.id())),
            )
            .unwrap()
        });
        let (send, messages) = mpsc::channel();
        std::thread::spawn(move || {
            let mut observations = observations;
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { return };
                if let Some(file) = observations.as_mut() {
                    writeln!(file, "{line}").unwrap();
                }
                let value: Value = serde_json::from_str(&line).expect("one JSON message per line");
                if send.send(value).is_err() {
                    return;
                }
            }
        });
        let stdin = child.stdin.take();
        Self {
            child,
            stdin,
            messages,
            seen: Vec::new(),
            next_id: 0,
        }
    }

    fn send(&mut self, message: Value) {
        let stdin = self.stdin.as_mut().unwrap();
        writeln!(stdin, "{message}").unwrap();
        stdin.flush().unwrap();
    }

    fn request(&mut self, method: &str, params: Value) -> u64 {
        self.next_id += 1;
        let id = self.next_id;
        self.send(json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}));
        id
    }

    /// Next message (from `seen` first) matching `pred`; earlier others stay seen.
    fn wait(&mut self, what: &str, pred: impl Fn(&Value) -> bool) -> Value {
        if let Some(index) = self.seen.iter().position(&pred) {
            return self.seen.remove(index);
        }
        let start = Instant::now();
        loop {
            let left = TIMEOUT
                .checked_sub(start.elapsed())
                .unwrap_or_else(|| panic!("timed out waiting for {what}; seen {:?}", self.seen));
            let message = self
                .messages
                .recv_timeout(left)
                .unwrap_or_else(|_| panic!("no {what}; seen {:?}", self.seen));
            if pred(&message) {
                return message;
            }
            self.seen.push(message);
        }
    }

    fn response(&mut self, id: u64) -> Value {
        self.wait(&format!("response {id}"), |m| {
            m.get("method").is_none() && m["id"] == json!(id)
        })
    }

    fn call(&mut self, method: &str, params: Value) -> Value {
        let id = self.request(method, params);
        self.response(id)
    }

    fn update(&mut self, what: &str, pred: impl Fn(&Value) -> bool) -> Value {
        self.wait(what, |m| {
            m["method"] == json!("session/update") && pred(&m["params"]["update"])
        })["params"]
            .clone()
    }

    /// The tagged idle for `message_id`; its stop reason and native-turn
    /// `_meta` must satisfy the resident-session/v1 extension schema.
    fn idle_for(&mut self, message_id: &str) -> Value {
        let id = message_id.to_owned();
        let idle = self.update(&format!("idle for {message_id}"), move |u| {
            u["state"] == json!("idle") && u["_meta"]["oulipoly.ai/lastUserMessageId"] == json!(id)
        })["update"]
            .clone();
        extension::validate("TurnStopReason", &idle["stopReason"]).unwrap();
        extension::validate("NativeTurnMeta", &idle["_meta"]["oulipoly.ai/nativeTurn"]).unwrap();
        idle
    }

    fn open(&mut self, cwd: &str) -> String {
        let init = self.call(
            "initialize",
            json!({"protocolVersion":2,"info":{"name":"test","version":"0"},"capabilities":{}}),
        );
        assert_eq!(init["result"]["protocolVersion"], json!(2));
        let opened = self.call("session/new", json!({"cwd":cwd}));
        opened["result"]["sessionId"].as_str().unwrap().to_owned()
    }

    fn prompt(&mut self, session: &str, text: &str, key: Option<&str>) -> u64 {
        let mut params = json!({"sessionId":session,"prompt":[{"type":"text","text":text}]});
        if let Some(key) = key {
            params["_meta"] = json!({"oulipoly.ai/messageKey":key});
        }
        self.request("session/prompt", params)
    }

    fn assert_silent(&mut self, for_ms: u64, pred: impl Fn(&Value) -> bool) {
        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(for_ms) {
            if let Ok(message) = self.messages.recv_timeout(Duration::from_millis(20)) {
                assert!(!pred(&message), "unexpected {message}");
                self.seen.push(message);
            }
        }
    }

    fn end(mut self) -> std::process::ExitStatus {
        self.stdin.take();
        let start = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(start.elapsed() < TIMEOUT, "provider did not end after EOF");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn wait_for(path: &Path) -> i32 {
    let start = Instant::now();
    loop {
        if let Ok(text) = std::fs::read_to_string(path) {
            if let Ok(pid) = text.trim().parse() {
                return pid;
            }
        }
        assert!(
            start.elapsed() < TIMEOUT,
            "{} never appeared",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn alive(pid: i32) -> bool {
    match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => !stat
            .rsplit_once(") ")
            .is_some_and(|(_, rest)| rest.starts_with('Z')),
        Err(_) => false,
    }
}

fn assert_dies(pid: i32) {
    let start = Instant::now();
    while alive(pid) {
        assert!(
            start.elapsed() < TIMEOUT,
            "process {pid} survived settlement"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn message_id(response: &Value) -> String {
    response["result"]["messageId"]
        .as_str()
        .unwrap_or_else(|| panic!("no messageId in {response}"))
        .to_owned()
}

// ---- tests ------------------------------------------------------------------

fn initialize_serves_only_v2_with_dedup_and_resident_contract() {
    let fixture = Fixture::new();
    let mut client = fixture.start();
    // A client offering v1 still gets the only version served; it decides.
    let init = client.call(
        "initialize",
        json!({"protocolVersion":1,"info":{"name":"test","version":"0"}}),
    );
    let result = &init["result"];
    assert_eq!(result["protocolVersion"], json!(2));
    assert_eq!(result["capabilities"]["session"], json!({}));
    assert_eq!(
        result["_meta"]["oulipoly.ai/messageKeyDedup"],
        json!({"version":1})
    );
    assert_eq!(
        result["_meta"]["oulipoly.ai/residentSession"]["protocol"],
        json!("oulipoly.resident_session/v1")
    );
    assert_eq!(result["info"]["name"], json!("fixture-resident"));
    extension::validate(
        "ResidentSessionMeta",
        &result["_meta"]["oulipoly.ai/residentSession"],
    )
    .unwrap();
    assert_eq!(resident::RESIDENT_SESSION_PROTOCOL, extension::PROTOCOL);
    assert_eq!(resident::ACP_SCHEMA_TAG, extension::ACP_SCHEMA_TAG);
    assert_eq!(
        resident::ACP_PROTOCOL_VERSION,
        extension::ACP_PROTOCOL_VERSION
    );
    assert!(client.end().success());
}

fn requests_before_initialize_and_unknown_methods_are_refused() {
    let fixture = Fixture::new();
    let mut client = fixture.start();
    let early = client.call("session/new", json!({"cwd":fixture.cwd()}));
    assert_eq!(early["error"]["code"], json!(-32002));
    client.call(
        "initialize",
        json!({"protocolVersion":2,"info":{"name":"t","version":"0"}}),
    );
    let unknown = client.call("session/fork", json!({}));
    assert_eq!(unknown["error"]["code"], json!(-32601));
    let relative = client.call("session/new", json!({"cwd":"work"}));
    assert_eq!(relative["error"]["code"], json!(-32602));
    let foreign = client.call(
        "session/prompt",
        json!({"sessionId":"not-open","prompt":[{"type":"text","text":"reply x"}]}),
    );
    assert_eq!(foreign["error"]["code"], json!(-32602));
    assert_eq!(fixture.runs(), 0, "refusals start nothing");
}

fn consumption_acknowledges_and_turn_end_is_attributed() {
    let fixture = Fixture::new();
    let mut client = fixture.start();
    let session = client.open(&fixture.cwd());
    let request = client.prompt(&session, "reply hello there", Some("k1"));
    let ack = client.response(request);
    let id = message_id(&ack);
    assert!(id.starts_with("msg_"));
    assert_eq!(
        ack["result"]["_meta"]["oulipoly.ai/messageKey"],
        json!("k1")
    );
    assert_eq!(
        ack["result"]["_meta"]["oulipoly.ai/duplicate"],
        json!(false)
    );
    let user = client.update("user_message", |u| {
        u["sessionUpdate"] == json!("user_message")
    });
    assert_eq!(user["sessionId"], json!(session));
    assert_eq!(user["update"]["messageId"], json!(id));
    let text = client.update("agent_message", |u| {
        u["sessionUpdate"] == json!("agent_message")
    });
    assert_eq!(text["sessionId"], json!(session));
    assert_eq!(text["update"]["content"][0]["text"], json!("hello there"));
    assert_eq!(
        text["update"]["_meta"]["oulipoly.ai/parentMessageId"],
        json!(id)
    );
    let idle = client.idle_for(&id);
    assert_eq!(idle["stopReason"], json!("end_turn"));
    let native = &idle["_meta"]["oulipoly.ai/nativeTurn"];
    assert_eq!(native["custody"], json!("complete"));
    assert_eq!(native["status"], json!({"kind":"exited","code":0}));
    assert_eq!(native["launch_output"]["stdout"]["bytes"], json!(12));
    assert_eq!(native["launch_output"]["data_event_count"], json!(1));
    assert_eq!(fixture.runs(), 1);
    assert!(client.end().success());
}

fn later_turns_continue_the_recorded_native_session() {
    let fixture = Fixture::new();
    let mut client = fixture.start();
    let session = client.open(&fixture.cwd());
    let first = client.prompt(&session, "reply one", None);
    let first = message_id(&client.response(first));
    client.idle_for(&first);
    let record: Value = serde_json::from_slice(
        &std::fs::read(
            fixture
                .state()
                .join("sessions")
                .join(&session)
                .join("session.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let native = record["native_session_id"].as_str().unwrap().to_owned();
    assert!(native.starts_with("native-"));
    let second = client.prompt(&session, "whoami", None);
    let second = message_id(&client.response(second));
    assert!(second > first, "message ids ascend");
    let text = client.update("whoami text", |u| {
        u["sessionUpdate"] == json!("agent_message")
            && u["content"][0]["text"]
                .as_str()
                .unwrap_or("")
                .starts_with("native=")
    });
    assert_eq!(
        text["update"]["content"][0]["text"],
        json!(format!("native={native}"))
    );
    assert_eq!(
        text["update"]["_meta"]["oulipoly.ai/parentMessageId"],
        json!(second)
    );
    client.idle_for(&second);
}

fn duplicate_key_inserts_nothing_and_repeats_its_turn_end() {
    let fixture = Fixture::new();
    let mut client = fixture.start();
    let session = client.open(&fixture.cwd());
    let first = client.prompt(&session, "reply once", Some("dup"));
    let id = message_id(&client.response(first));
    client.idle_for(&id);
    let again = client.prompt(&session, "reply once", Some("dup"));
    let again = client.response(again);
    assert_eq!(message_id(&again), id);
    assert_eq!(
        again["result"]["_meta"]["oulipoly.ai/duplicate"],
        json!(true)
    );
    let idle = client.idle_for(&id);
    assert_eq!(idle["stopReason"], json!("end_turn"));
    assert_eq!(fixture.runs(), 1, "a duplicate key runs nothing");
}

fn turn_without_consumption_is_not_acknowledged() {
    let fixture = Fixture::new();
    let mut client = fixture.start();
    let session = client.open(&fixture.cwd());
    let request = client.prompt(&session, "noconsume", Some("nc"));
    let refused = client.response(request);
    assert_eq!(refused["error"]["code"], json!(-32010), "{refused}");
    assert_eq!(
        refused["error"]["data"]["nativeTurn"]["status"],
        json!({"kind":"exited","code":3})
    );
    client.assert_silent(300, |m| m["params"]["update"]["state"] == json!("idle"));
    // The same key is still not an insertion.
    let again = client.prompt(&session, "noconsume", Some("nc"));
    assert_eq!(client.response(again)["error"]["code"], json!(-32010));
    assert_eq!(fixture.runs(), 1);
}

fn native_failure_is_a_tagged_failed_turn_end() {
    let fixture = Fixture::new();
    let mut client = fixture.start();
    let session = client.open(&fixture.cwd());
    let request = client.prompt(&session, "fail", None);
    let id = message_id(&client.response(request));
    let idle = client.idle_for(&id);
    assert_eq!(idle["stopReason"], json!("_oulipoly_native_failed"));
    assert_eq!(
        idle["_meta"]["oulipoly.ai/nativeTurn"]["status"],
        json!({"kind":"exited","code":1})
    );
}

fn cancel_stops_the_turn_settles_descendants_and_refuses_queued_input() {
    let fixture = Fixture::new();
    let mut client = fixture.start();
    let session = client.open(&fixture.cwd());
    let marks = fixture.dir.path().join("hang");
    std::fs::create_dir(&marks).unwrap();
    let running = client.prompt(&session, &format!("hang {}", marks.display()), Some("h"));
    let id = message_id(&client.response(running));
    let descendant = wait_for(&marks.join("descendant.pid"));
    let leader = wait_for(&marks.join("leader.pid"));
    let queued = client.prompt(&session, "reply later", Some("q"));
    client.assert_silent(200, |m| m["id"] == json!(queued));
    client.send(json!({"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":session}}));
    let idle = client.idle_for(&id);
    assert_eq!(idle["stopReason"], json!("cancelled"));
    assert_eq!(
        idle["_meta"]["oulipoly.ai/nativeTurn"]["custody"],
        json!("complete")
    );
    assert_dies(leader);
    assert_dies(descendant);
    let refused = client.response(queued);
    assert_eq!(refused["error"]["code"], json!(-32010));
    assert_eq!(fixture.runs(), 1, "queued input never started");
    // A prompt after the cancel runs normally.
    let after = client.prompt(&session, "reply after", None);
    let after = message_id(&client.response(after));
    assert_eq!(client.idle_for(&after)["stopReason"], json!("end_turn"));
}

fn connection_end_settles_the_running_turn() {
    let fixture = Fixture::new();
    let mut client = fixture.start();
    let session = client.open(&fixture.cwd());
    let marks = fixture.dir.path().join("hang");
    std::fs::create_dir(&marks).unwrap();
    let running = client.prompt(&session, &format!("hang {}", marks.display()), None);
    client.response(running);
    let descendant = wait_for(&marks.join("descendant.pid"));
    assert!(client.end().success());
    assert_dies(descendant);
}

fn lost_provider_is_reconciled_on_resume_and_never_rerun() {
    let fixture = Fixture::new();
    let mut first = fixture.start();
    let session = first.open(&fixture.cwd());
    let marks = fixture.dir.path().join("hang");
    std::fs::create_dir(&marks).unwrap();
    let running = first.prompt(&session, &format!("hang {}", marks.display()), Some("lost"));
    let id = message_id(&first.response(running));
    let descendant = wait_for(&marks.join("descendant.pid"));
    // Provider loss: no settlement by the lost process.
    first.child.kill().unwrap();
    first.child.wait().unwrap();
    assert!(
        alive(descendant),
        "the descendant outlives the lost provider"
    );

    let mut second = fixture.start();
    second.call(
        "initialize",
        json!({"protocolVersion":2,"info":{"name":"t","version":"0"}}),
    );
    let wrong = second.call("session/resume", json!({"sessionId":session,"cwd":"/tmp"}));
    assert_eq!(wrong["error"]["code"], json!(-32602), "cwd must match");
    let resumed = second.call(
        "session/resume",
        json!({"sessionId":session,"cwd":fixture.cwd()}),
    );
    assert_eq!(resumed["result"], json!({}), "{resumed}");
    assert_dies(descendant);
    // A third process cannot hold the same session.
    let mut third = fixture.start();
    third.call(
        "initialize",
        json!({"protocolVersion":2,"info":{"name":"t","version":"0"}}),
    );
    let held = third.call(
        "session/resume",
        json!({"sessionId":session,"cwd":fixture.cwd()}),
    );
    assert_eq!(held["error"]["code"], json!(-32012));
    // The owed input, resent with its key, is acknowledged as the earlier
    // insertion and ended visibly; nothing runs again.
    let again = second.prompt(&session, &format!("hang {}", marks.display()), Some("lost"));
    let again = second.response(again);
    assert_eq!(message_id(&again), id);
    assert_eq!(
        again["result"]["_meta"]["oulipoly.ai/duplicate"],
        json!(true)
    );
    let idle = second.idle_for(&id);
    assert_eq!(
        idle["stopReason"],
        json!("_oulipoly_reconciliation_required")
    );
    assert_eq!(
        idle["_meta"]["oulipoly.ai/nativeTurn"]["custody"],
        json!("reconciled")
    );
    assert_eq!(fixture.runs(), 1, "an interrupted input never runs twice");
    // New input continues the same native session in the new process.
    let next = second.prompt(&session, "whoami", None);
    let next = message_id(&second.response(next));
    assert!(next > id);
    let text = second.update("whoami", |u| u["sessionUpdate"] == json!("agent_message"));
    let said = text["update"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(said.starts_with("native=native-"), "{said}");
    second.idle_for(&next);
}

fn close_settles_and_refuses_later_input() {
    let fixture = Fixture::new();
    let mut client = fixture.start();
    let session = client.open(&fixture.cwd());
    let marks = fixture.dir.path().join("hang");
    std::fs::create_dir(&marks).unwrap();
    let running = client.prompt(&session, &format!("hang {}", marks.display()), None);
    let id = message_id(&client.response(running));
    let descendant = wait_for(&marks.join("descendant.pid"));
    let closed = client.call("session/close", json!({"sessionId":session}));
    assert_eq!(closed["result"], json!({}));
    assert_dies(descendant);
    assert_eq!(client.idle_for(&id)["stopReason"], json!("cancelled"));
    let late = client.prompt(&session, "reply late", None);
    assert_eq!(client.response(late)["error"]["code"], json!(-32602));
    let listed = client.call("session/list", json!({}));
    assert_eq!(listed["result"]["sessions"][0]["sessionId"], json!(session));
}

fn cancel_is_session_scoped() {
    let fixture = Fixture::new();
    let mut client = fixture.start();
    let first = client.open(&fixture.cwd());
    let second = client.call("session/new", json!({"cwd":fixture.cwd()}))["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_ne!(first, second);
    let (a, b) = (fixture.dir.path().join("a"), fixture.dir.path().join("b"));
    std::fs::create_dir(&a).unwrap();
    std::fs::create_dir(&b).unwrap();
    let ra = client.prompt(&first, &format!("hang {}", a.display()), None);
    let ia = message_id(&client.response(ra));
    let rb = client.prompt(&second, &format!("hang {}", b.display()), None);
    let ib = message_id(&client.response(rb));
    let (da, db) = (
        wait_for(&a.join("descendant.pid")),
        wait_for(&b.join("descendant.pid")),
    );
    client.send(json!({"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":first}}));
    let idle = client.idle_for(&ia);
    assert_eq!(idle["stopReason"], json!("cancelled"));
    assert_dies(da);
    client.assert_silent(500, |m| {
        m["params"]["update"]["_meta"]["oulipoly.ai/lastUserMessageId"] == json!(ib)
    });
    assert!(alive(db), "the other session's turn keeps running");
    // Every update names its own session.
    for message in &client.seen {
        if message["method"] == json!("session/update") {
            let session = message["params"]["sessionId"].as_str().unwrap();
            assert!(session == first || session == second);
        }
    }
    client.send(json!({"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":second}}));
    let idle = client.idle_for(&ib);
    assert_eq!(idle["stopReason"], json!("cancelled"));
    assert_dies(db);
}

fn refused_preparation_is_not_an_insertion() {
    let fixture = Fixture::new();
    let mut client = fixture.start();
    let session = client.open(&fixture.cwd());
    let request = client.prompt(&session, "refuse", Some("r"));
    let refused = client.response(request);
    assert_eq!(refused["error"]["code"], json!(-32010), "{refused}");
    let native = &refused["error"]["data"]["nativeTurn"];
    assert_eq!(native["custody"], json!("not_admitted"));
    assert_eq!(native["failure"]["code"], json!("fixture_refused"));
    extension::validate("NativeTurnMeta", native).unwrap();
    assert_eq!(fixture.runs(), 0);
    let duplicate = client.prompt(&session, "ignored", Some("r"));
    assert_eq!(client.response(duplicate)["error"]["code"], json!(-32010));
    let next = client.prompt(&session, "reply fine", None);
    let next = message_id(&client.response(next));
    assert_eq!(client.idle_for(&next)["stopReason"], json!("end_turn"));
}

fn barrier(fixture: &Fixture, client: &mut Client, session: &str) -> (u64, PathBuf) {
    let marks = fixture.dir.path().join("barrier");
    std::fs::create_dir(&marks).unwrap();
    let pending = client.prompt(
        session,
        &format!("barrier {}", marks.display()),
        Some("barrier"),
    );
    wait_for(&marks.join("ready"));
    (pending, marks)
}

fn queued_turn_selects_settled_native_session() {
    let fixture = Fixture::new();
    let mut client = fixture.start();
    let session = client.open(&fixture.cwd());
    let (first, marks) = barrier(&fixture, &mut client, &session);
    let second = client.prompt(&session, "whoami", None);
    client.call("session/list", json!({})); // input-dispatch barrier
    std::fs::write(marks.join("release"), "release").unwrap();
    let first = message_id(&client.response(first));
    client.idle_for(&first);
    let second = message_id(&client.response(second));
    let text = client.update("queued native session", |u| {
        u["sessionUpdate"] == json!("agent_message")
            && u["_meta"]["oulipoly.ai/parentMessageId"] == json!(second)
    });
    let record: Value = serde_json::from_slice(
        &std::fs::read(
            fixture
                .state()
                .join("sessions")
                .join(&session)
                .join("session.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        text["update"]["content"][0]["text"],
        json!(format!(
            "native={}",
            record["native_session_id"].as_str().unwrap()
        ))
    );
    client.idle_for(&second);
    assert_eq!(fixture.runs(), 2);
}

fn consumption_store_failure_is_unknown() {
    let fixture = Fixture::new();
    let mut client = fixture.start();
    let session = client.open(&fixture.cwd());
    let (pending, marks) = barrier(&fixture, &mut client, &session);
    let dir = fixture.state().join("sessions").join(&session);
    std::fs::rename(dir.join("inputs"), dir.join("inputs-held")).unwrap();
    std::fs::write(dir.join("inputs"), "ENOTDIR").unwrap();
    std::fs::write(marks.join("release"), "release").unwrap();
    let response = client.response(pending);
    assert_eq!(response["error"]["code"], json!(-32011), "{response}");
    assert_eq!(
        response["error"]["data"]["nativeTurn"]["status"],
        json!({"kind":"exited","code":0})
    );
    let duplicate = client.prompt(&session, "different bytes", Some("barrier"));
    assert_eq!(client.response(duplicate)["error"]["code"], json!(-32011));
    client.assert_silent(200, |m| {
        m["params"]["update"]["sessionUpdate"] == json!("user_message")
            || m["params"]["update"]["sessionUpdate"] == json!("agent_message")
    });
    assert_eq!(fixture.runs(), 1);
    // Restore storage: a later reopen can recover the complete journal, with no new native call.
    std::fs::remove_file(dir.join("inputs")).unwrap();
    std::fs::rename(dir.join("inputs-held"), dir.join("inputs")).unwrap();
    assert!(client.end().success());
    let mut reopened = fixture.start();
    reopened.call("initialize", json!({"protocolVersion":2}));
    assert_eq!(
        reopened.call(
            "session/resume",
            json!({"sessionId":session,"cwd":fixture.cwd()})
        )["result"],
        json!({})
    );
    let duplicate = reopened.prompt(&session, "different bytes", Some("barrier"));
    let duplicate = reopened.response(duplicate);
    assert_eq!(
        duplicate["result"]["_meta"]["oulipoly.ai/duplicate"],
        json!(true)
    );
    assert_eq!(fixture.runs(), 1);
}

fn close_releases_session_ownership() {
    let fixture = Fixture::new();
    let mut first = fixture.start();
    let session = first.open(&fixture.cwd());
    assert_eq!(
        first.call("session/close", json!({"sessionId":session}))["result"],
        json!({})
    );
    assert_eq!(
        first.call(
            "session/resume",
            json!({"sessionId":session,"cwd":fixture.cwd()})
        )["result"],
        json!({})
    );
    assert_eq!(
        first.call("session/close", json!({"sessionId":session}))["result"],
        json!({})
    );
    let mut second = fixture.start();
    second.call("initialize", json!({"protocolVersion":2}));
    assert_eq!(
        second.call(
            "session/resume",
            json!({"sessionId":session,"cwd":fixture.cwd()})
        )["result"],
        json!({}),
        "first connection remains open"
    );
}

fn native_session_publish_failure_keeps_custody_unsettled() {
    let fixture = Fixture::new();
    let mut client = fixture.start();
    let session = client.open(&fixture.cwd());
    let (pending, marks) = barrier(&fixture, &mut client, &session);
    let dir = fixture.state().join("sessions").join(&session);
    std::fs::rename(dir.join("session.json"), dir.join("session-held.json")).unwrap();
    std::fs::create_dir(dir.join("session.json")).unwrap();
    std::fs::write(marks.join("release"), "release").unwrap();
    assert_eq!(client.response(pending)["error"]["code"], json!(-32011));
    let close = client.call("session/close", json!({"sessionId":session}));
    assert_eq!(close["error"]["code"], json!(-32012), "{close}");
    let input: Value = serde_json::from_slice(
        &std::fs::read(
            std::fs::read_dir(dir.join("inputs"))
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .path(),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(input["native_turn"]["custody"], json!("incomplete"));
    assert_ne!(input["phase"], json!("ended"));
    assert!(input.get("ended_unix_ms").is_none());
    std::fs::remove_dir(dir.join("session.json")).unwrap();
    std::fs::rename(dir.join("session-held.json"), dir.join("session.json")).unwrap();
    assert!(
        !client.end().success(),
        "a refused close still owes custody evidence"
    );
    let mut reopened = fixture.start();
    reopened.call("initialize", json!({"protocolVersion":2}));
    assert_eq!(
        reopened.call(
            "session/resume",
            json!({"sessionId":session,"cwd":fixture.cwd()})
        )["result"],
        json!({})
    );
    assert_eq!(
        reopened.call("session/close", json!({"sessionId":session}))["result"],
        json!({})
    );
    assert_eq!(fixture.runs(), 1);
}

fn recovery_precedes_current_adapter_admission() {
    let fixture = Fixture::new();
    let mut first = fixture.start();
    let session = first.open(&fixture.cwd());
    let marks = fixture.dir.path().join("lost");
    std::fs::create_dir(&marks).unwrap();
    let req = first.prompt(&session, &format!("hang {}", marks.display()), Some("lost"));
    let id = message_id(&first.response(req));
    let descendant = wait_for(&marks.join("descendant.pid"));
    first.child.kill().unwrap();
    first.child.wait().unwrap();
    assert!(alive(descendant));
    std::fs::write(fixture.state().join("refuse-current-policy"), "changed").unwrap();
    let mut next = fixture.start();
    next.call("initialize", json!({"protocolVersion":2}));
    assert_eq!(
        next.call(
            "session/resume",
            json!({"sessionId":session,"cwd":fixture.cwd()})
        )["result"],
        json!({})
    );
    assert_dies(descendant);
    let duplicate = next.prompt(&session, "changed bytes", Some("lost"));
    assert_eq!(message_id(&next.response(duplicate)), id);
    assert_eq!(
        next.idle_for(&id)["_meta"]["oulipoly.ai/nativeTurn"]["custody"],
        json!("reconciled")
    );
    assert_eq!(fixture.runs(), 1);
}

// Each actor is task-owned; this guard also closes it if a red assertion fails.
struct LostActor {
    leader: i32,
    descendant: i32,
}
impl Drop for LostActor {
    fn drop(&mut self) {
        if alive(self.descendant) {
            unsafe {
                libc::kill(self.descendant, libc::SIGKILL);
                if alive(self.leader) {
                    libc::kill(self.leader, libc::SIGKILL);
                }
            }
        }
    }
}

fn bounds_lost(fixture: &Fixture) -> (String, String, LostActor) {
    let mut first = fixture.start();
    let session = first.open(&fixture.cwd());
    let marks = fixture.dir.path().join("bounds-hang");
    std::fs::create_dir(&marks).unwrap();
    let request = first.prompt(
        &session,
        &format!("hang {}", marks.display()),
        Some("bounds-lost"),
    );
    let id = message_id(&first.response(request));
    first.update("waiting after session marker", |u| {
        u["sessionUpdate"] == json!("agent_message")
    });
    let actor = LostActor {
        leader: wait_for(&marks.join("leader.pid")),
        descendant: wait_for(&marks.join("descendant.pid")),
    };
    first.child.kill().unwrap();
    first.child.wait().unwrap();
    assert!(
        alive(actor.descendant),
        "real actor survives actual provider kill"
    );
    (session, id, actor)
}

fn bounds_resume(fixture: &Fixture, session: &str) -> (Client, Value) {
    let mut client = fixture.start();
    client.call(
        "initialize",
        json!({"protocolVersion":2,"info":{"name":"bounds","version":"0"}}),
    );
    let response = client.call(
        "session/resume",
        json!({"sessionId":session,"cwd":fixture.cwd()}),
    );
    (client, response)
}

fn bounds_dir(fixture: &Fixture, session: &str) -> PathBuf {
    fixture.state().join("sessions").join(session)
}
fn bounds_input(fixture: &Fixture, session: &str, id: &str) -> PathBuf {
    bounds_dir(fixture, session)
        .join("inputs")
        .join(format!("{id}.json"))
}
fn bounds_journal(fixture: &Fixture, session: &str, id: &str) -> PathBuf {
    let request = format!("resident-{session}-{id}");
    bounds_dir(fixture, session).join("turns").join(format!(
        "{}.jsonl",
        agent_provider_execution::custody::request_key(None, &request)
    ))
}
fn bounds_read(path: &Path) -> Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}
fn bounds_write(path: &Path, value: &Value) {
    std::fs::write(path, value.to_string()).unwrap();
}

fn bounds_admission_refuses_before_effects() {
    let fixture = Fixture::new();
    let mut client = fixture.start();
    let session = client.open(&fixture.cwd());
    // Fits the wire envelope but leaves no room in the durable input envelope.
    let request = client.prompt(&session, &"x".repeat(16 * 1024 * 1024), Some("too-large"));
    let response = client.response(request);
    assert_eq!(response["error"]["code"], json!(-32602), "{response}");
    assert_eq!(
        std::fs::read_dir(bounds_dir(&fixture, &session).join("inputs"))
            .unwrap()
            .count(),
        0
    );
    assert_eq!(fixture.runs(), 0, "refusal precedes native effects");
    let ordinary = client.prompt(&session, "reply after refusal", Some("too-large"));
    let id = message_id(&client.response(ordinary));
    client.idle_for(&id);
    assert_eq!(fixture.runs(), 1, "refusal did not reserve the key");
    assert!(client.end().success());
    let (mut resumed, result) = bounds_resume(&fixture, &session);
    assert_eq!(result["result"], json!({}));
    let duplicate = resumed.prompt(&session, "reply after refusal", Some("too-large"));
    assert_eq!(message_id(&resumed.response(duplicate)), id);
    resumed.idle_for(&id);
    assert_eq!(fixture.runs(), 1);
}

fn bounds_unreadable_input_does_not_block_other_actor() {
    let fixture = Fixture::new();
    let (session, id, actor) = bounds_lost(&fixture);
    // Inject a separate corrupt historical record; do not confuse injection with interruption.
    let corrupt = bounds_input(&fixture, &session, "msg_0000000000000002");
    std::fs::write(&corrupt, b"{not json").unwrap();
    let (mut second, response) = bounds_resume(&fixture, &session);
    assert_dies(actor.descendant);
    assert_eq!(response["error"]["code"], json!(-32012));
    assert!(response["error"]["message"]
        .as_str()
        .unwrap()
        .contains("input evidence unreadable"));
    let duplicate = second.prompt(&session, "ignored duplicate", Some("bounds-lost"));
    assert_eq!(message_id(&second.response(duplicate)), id);
    second.idle_for(&id);
    let new = second.prompt(&session, "whoami", None);
    assert_eq!(second.response(new)["error"]["code"], json!(-32012));
    assert_eq!(fixture.runs(), 1);
    assert_eq!(std::fs::read(corrupt).unwrap(), b"{not json");
}

fn bounds_large_journal(identity: bool) {
    let fixture = Fixture::new();
    let (session, id, actor) = bounds_lost(&fixture);
    let path = bounds_input(&fixture, &session, &id);
    let mut input = bounds_read(&path);
    // Inject an earlier durable input snapshot to make the journal the sole evidence.
    input["phase"] = json!("accepted");
    input["consumption_seen"] = json!(false);
    bounds_write(&path, &input);
    let session_path = bounds_dir(&fixture, &session).join("session.json");
    let mut record = bounds_read(&session_path);
    let native = record["native_session_id"].clone();
    assert!(native.as_str().unwrap().starts_with("native-"));
    if identity {
        record["native_session_id"] = json!("stale-id");
        bounds_write(&session_path, &record);
    }
    let journal = bounds_journal(&fixture, &session, &id);
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&journal)
        .unwrap();
    let filler = json!({"kind":"stderr","data_base64":"x".repeat(4096)}).to_string() + "\n";
    for _ in 0..4200 {
        file.write_all(filler.as_bytes()).unwrap();
    }
    file.sync_all().unwrap();
    drop(file);
    let before = std::fs::read(&journal).unwrap();
    assert!(before.len() > 16 * 1024 * 1024);
    println!(
        "large interrupted journal bytes={} identity={identity}",
        before.len()
    );
    let (mut second, response) = bounds_resume(&fixture, &session);
    assert_eq!(response["result"], json!({}), "{response}");
    assert_dies(actor.descendant);
    let duplicate = second.prompt(&session, "ignored", Some("bounds-lost"));
    assert_eq!(message_id(&second.response(duplicate)), id);
    let idle = second.idle_for(&id);
    assert_eq!(
        idle["_meta"]["oulipoly.ai/nativeTurn"]["custody"],
        json!("reconciled")
    );
    assert_eq!(
        std::fs::read(&journal).unwrap(),
        before,
        "recovery preserves all custody bytes"
    );
    assert_eq!(fixture.runs(), 1, "recovery never reruns");
    if identity {
        assert_eq!(bounds_read(&session_path)["native_session_id"], native);
        let request = second.prompt(&session, "whoami", None);
        let next = message_id(&second.response(request));
        let text = second.update("native identity", |u| {
            u["sessionUpdate"] == json!("agent_message")
        });
        assert_eq!(
            text["update"]["content"][0]["text"],
            json!(format!("native={}", native.as_str().unwrap()))
        );
        second.idle_for(&next);
        assert_eq!(fixture.runs(), 2);
    }
}
fn bounds_large_journal_recovers_consumption() {
    bounds_large_journal(false);
}
fn bounds_large_journal_recovers_native_session() {
    bounds_large_journal(true);
}

fn bounds_corrupt_journal_preserves_known_insertion_and_reports_uncertainty() {
    let fixture = Fixture::new();
    let (session, id, actor) = bounds_lost(&fixture);
    let journal = bounds_journal(&fixture, &session, &id);
    // A deliberately injected torn record, following valid early markers.
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&journal)
        .unwrap();
    file.write_all(b"{torn").unwrap();
    drop(file);
    let before = std::fs::read(&journal).unwrap();
    let (mut second, response) = bounds_resume(&fixture, &session);
    assert_dies(actor.descendant);
    assert_eq!(response["error"]["code"], json!(-32012));
    assert!(response["error"]["message"]
        .as_str()
        .unwrap()
        .contains("journal evidence unreadable"));
    let duplicate = second.prompt(&session, "ignored", Some("bounds-lost"));
    assert_eq!(message_id(&second.response(duplicate)), id);
    let idle = second.idle_for(&id);
    assert_eq!(
        idle["_meta"]["oulipoly.ai/nativeTurn"]["custody"],
        json!("reconciled")
    );
    assert_eq!(
        idle["_meta"]["oulipoly.ai/nativeTurn"]["failure"]["code"],
        json!("journal_evidence_unreadable")
    );
    let record = bounds_read(&bounds_dir(&fixture, &session).join("session.json"));
    assert!(record["native_session_id"].is_null());
    assert!(record["native_session_uncertain"].is_string());
    let new = second.prompt(&session, "whoami", None);
    assert_eq!(second.response(new)["error"]["code"], json!(-32012));
    assert_eq!(fixture.runs(), 1);
    assert_eq!(std::fs::read(&journal).unwrap(), before);
    assert!(second.end().success());
    let (mut third, response) = bounds_resume(&fixture, &session);
    assert_eq!(response["error"]["code"], json!(-32012));
    let new = third.prompt(&session, "whoami", None);
    assert_eq!(third.response(new)["error"]["code"], json!(-32012));
    assert_eq!(fixture.runs(), 1);
}

fn bounds_wire_limit_settles_running_actor() {
    let fixture = Fixture::new();
    let mut client = fixture.start();
    let session = client.open(&fixture.cwd());
    let marks = fixture.dir.path().join("wire-hang");
    std::fs::create_dir(&marks).unwrap();
    let request = client.prompt(&session, &format!("hang {}", marks.display()), None);
    let id = message_id(&client.response(request));
    let actor = LostActor {
        leader: wait_for(&marks.join("leader.pid")),
        descendant: wait_for(&marks.join("descendant.pid")),
    };
    let bytes = vec![b'x'; 1024 * 1024];
    for _ in 0..33 {
        if client.stdin.as_mut().unwrap().write_all(&bytes).is_err() {
            break;
        }
    }
    let error = client.wait("wire bound error", |m| m["error"]["code"] == json!(-32602));
    assert!(
        error["id"].is_null(),
        "over-bound line has no trustworthy request ID"
    );
    client.idle_for(&id);
    assert_dies(actor.descendant);
    assert_eq!(fixture.runs(), 1);
    assert!(client.end().success());
}

fn bounds_missing_marker_differs_from_unreadable_journal() {
    for unreadable in [false, true] {
        let fixture = Fixture::new();
        let (session, id, actor) = bounds_lost(&fixture);
        let input_path = bounds_input(&fixture, &session, &id);
        let mut input = bounds_read(&input_path);
        input["phase"] = json!("accepted");
        input["consumption_seen"] = json!(false);
        bounds_write(&input_path, &input);
        let journal = bounds_journal(&fixture, &session, &id);
        let original = std::fs::read_to_string(&journal).unwrap();
        // Deliberate evidence cut: native insertion really occurred, but this
        // issued disk condition cannot prove it. Never infer non-insertion.
        let retained: String = original
            .lines()
            .filter(|line| {
                let event: Value = serde_json::from_str(line).unwrap();
                event["name"] != json!("oulipoly.submitted_user_turn")
            })
            .map(|line| format!("{line}\n"))
            .collect();
        let issued = if unreadable {
            retained.clone() + "{cut"
        } else {
            retained
        };
        std::fs::write(&journal, &issued).unwrap();
        let (mut second, response) = bounds_resume(&fixture, &session);
        assert_dies(actor.descendant);
        if unreadable {
            assert_eq!(response["error"]["code"], json!(-32012));
            assert!(response["error"]["message"]
                .as_str()
                .unwrap()
                .contains("journal evidence unreadable"));
        } else {
            assert_eq!(response["result"], json!({}));
        }
        let duplicate = second.prompt(&session, "ignored", Some("bounds-lost"));
        assert_eq!(second.response(duplicate)["error"]["code"], json!(-32011));
        second.assert_silent(100, |m| {
            m["params"]["update"]["state"] == json!("idle")
                || m["params"]["update"]["sessionUpdate"] == json!("user_message")
        });
        assert_eq!(bounds_read(&input_path)["phase"], json!("uncertain"));
        assert_eq!(
            bounds_read(&input_path)["native_turn"]["custody"],
            json!("reconciled")
        );
        assert_eq!(std::fs::read_to_string(&journal).unwrap(), issued);
        assert_eq!(fixture.runs(), 1, "unproved insertion is never rerun");
        assert!(second.end().success());
    }
}

// These controls reuse the actual interrupted actor fixture; disk corruption
// is issued after the provider's real SIGKILL wait. A subreaper retains exact
// descendant waits as well as physical non-running observations.
struct CustodyActors {
    actor: std::mem::ManuallyDrop<LostActor>,
    identities: [(i32, String); 2],
    reaper: Option<std::thread::JoinHandle<()>>,
}
fn custody_actor_identity(pid: i32) -> String {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    let ticks = stat
        .rsplit_once(") ")
        .unwrap()
        .1
        .split_whitespace()
        .nth(19)
        .unwrap();
    format!(
        "{}:{ticks}",
        std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
            .unwrap()
            .trim()
    )
}
impl CustodyActors {
    fn capture(fixture: &Fixture) -> (String, String, Self) {
        assert_eq!(unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1) }, 0);
        let (session, id, actors) = bounds_lost(fixture);
        let identities =
            [actors.leader, actors.descendant].map(|pid| (pid, custody_actor_identity(pid)));
        let pids = [actors.leader, actors.descendant];
        let reaper = std::thread::spawn(move || {
            for pid in pids {
                let mut status = 0;
                loop {
                    let result = unsafe { libc::waitpid(pid, &mut status, 0) };
                    if result == -1
                        && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted
                    {
                        continue;
                    }
                    assert_eq!(result, pid);
                    break;
                }
                println!("owned actor wait pid={pid} status={status}");
            }
        });
        (
            session,
            id,
            Self {
                actor: std::mem::ManuallyDrop::new(actors),
                identities,
                reaper: Some(reaper),
            },
        )
    }
}
impl Drop for CustodyActors {
    fn drop(&mut self) {
        for (pid, identity) in &self.identities {
            if alive(*pid) {
                assert_eq!(&custody_actor_identity(*pid), identity);
                unsafe {
                    libc::kill(*pid, libc::SIGKILL);
                }
            }
        }
        self.reaper.take().unwrap().join().unwrap();
        assert_eq!(unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 0) }, 0);
    }
}

fn custody_corrupt_own_input_settles_without_reconstruction() {
    let fixture = Fixture::new();
    let (session, id, actor) = CustodyActors::capture(&fixture);
    let input = bounds_input(&fixture, &session, &id);
    let journal = bounds_journal(&fixture, &session, &id);
    let launch = journal.with_extension("json");
    let launch_bytes = std::fs::read(&launch).unwrap();
    let journal_bytes = std::fs::read(&journal).unwrap();
    std::fs::write(&input, b"{corrupt own input").unwrap();
    let (mut client, response) = bounds_resume(&fixture, &session);
    assert_eq!(response["error"]["code"], json!(-32012));
    assert_dies(actor.actor.descendant);
    let duplicate = client.prompt(&session, "ignored", Some("bounds-lost"));
    assert_eq!(client.response(duplicate)["error"]["code"], json!(-32012));
    assert_eq!(
        client.call("session/close", json!({"sessionId":session}))["result"],
        json!({})
    );
    assert!(client.end().success());
    assert_eq!(std::fs::read(&input).unwrap(), b"{corrupt own input");
    assert_eq!(std::fs::read(&launch).unwrap(), launch_bytes);
    assert_eq!(std::fs::read(&journal).unwrap(), journal_bytes);
    assert_eq!(fixture.runs(), 1);
    let (next, response) = bounds_resume(&fixture, &session);
    assert_eq!(response["error"]["code"], json!(-32012));
    assert!(next.end().success());
}

fn custody_unreadable_launch_refuses_close_and_eof() {
    let fixture = Fixture::new();
    let (session, id, actor) = CustodyActors::capture(&fixture);
    let input = bounds_input(&fixture, &session, &id);
    let journal = bounds_journal(&fixture, &session, &id);
    let launch = journal.with_extension("json");
    std::fs::write(&input, b"{corrupt own input").unwrap();
    std::fs::write(&launch, b"{unreadable launch").unwrap();
    let (mut client, response) = bounds_resume(&fixture, &session);
    assert_eq!(response["error"]["code"], json!(-32012));
    assert!(alive(actor.actor.descendant));
    assert_eq!(
        client.call("session/close", json!({"sessionId":session}))["error"]["code"],
        json!(-32012)
    );
    assert!(
        !client.end().success(),
        "refused close must survive as EOF failure"
    );
    let (next, response) = bounds_resume(&fixture, &session);
    assert_eq!(response["error"]["code"], json!(-32012));
    assert!(
        !next.end().success(),
        "open-session EOF must expose unreadable custody"
    );
    assert!(alive(actor.actor.descendant));
    assert_eq!(std::fs::read(&input).unwrap(), b"{corrupt own input");
    assert_eq!(std::fs::read(&launch).unwrap(), b"{unreadable launch");
    assert_eq!(fixture.runs(), 1);
}

fn insertion_missing_launch_preserves_ack_and_restored_reconciliation() {
    let fixture = Fixture::new();
    let (session, id, actor) = CustodyActors::capture(&fixture);
    let input = bounds_input(&fixture, &session, &id);
    let journal = bounds_journal(&fixture, &session, &id);
    let launch = journal.with_extension("json");
    let input_bytes = std::fs::read(&input).unwrap();
    let launch_bytes = std::fs::read(&launch).unwrap();
    let journal_bytes = std::fs::read(&journal).unwrap();
    std::fs::remove_file(&launch).unwrap();
    let (mut client, response) = bounds_resume(&fixture, &session);
    assert_eq!(response["error"]["code"], json!(-32012));
    assert!(alive(actor.actor.descendant));
    let duplicate = client.prompt(&session, "ignored", Some("bounds-lost"));
    assert_eq!(message_id(&client.response(duplicate)), id);
    assert_eq!(std::fs::read(&input).unwrap(), input_bytes);
    assert_eq!(
        client.call("session/close", json!({"sessionId":session}))["error"]["code"],
        json!(-32012)
    );
    assert!(!client.end().success());
    std::fs::write(&launch, &launch_bytes).unwrap();
    let (mut client, response) = bounds_resume(&fixture, &session);
    assert_eq!(response["result"], json!({}));
    assert_dies(actor.actor.descendant);
    let duplicate = client.prompt(&session, "ignored", Some("bounds-lost"));
    assert_eq!(message_id(&client.response(duplicate)), id);
    assert_eq!(
        client.idle_for(&id)["_meta"]["oulipoly.ai/nativeTurn"]["custody"],
        json!("reconciled")
    );
    assert_eq!(
        client.call("session/close", json!({"sessionId":session}))["result"],
        json!({})
    );
    assert!(client.end().success());
    assert_eq!(std::fs::read(&launch).unwrap(), launch_bytes);
    assert_eq!(std::fs::read(&journal).unwrap(), journal_bytes);
    assert_eq!(fixture.runs(), 1);
}
