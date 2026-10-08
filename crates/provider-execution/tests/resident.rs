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

use agent_provider_contract::acp::resident::{
    self as host, Binding, PreparedEndpoint, SessionStart, StartRefusal,
};
use agent_provider_contract::acp::{
    AcpClient, AtMostOnceBasis, ClientInfo, DeliveryOutcome, Incoming, MessageKey, NativeCustody,
    OutboundMessage, PeerClosed, SessionEvent, Transport,
};
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
  endbarrier) echo CONSUMED; echo $$ > "$2/ready"
        while [ ! -f "$2/release" ]; do sleep 0.02; done
        echo "TEXT after ack"; exit 0 ;;
  whoami) echo CONSUMED; echo "TEXT native=$NATIVE_SESSION"; exit 0 ;;
  hang) echo CONSUMED; [ -z "$NATIVE_SESSION" ] && echo "SESSION native-$$"
        sleep 300 & echo $! > "$2/descendant.pid"; echo $$ > "$2/leader.pid"
        echo "TEXT waiting"; wait ;;
  hanglate) echo CONSUMED; while [ ! -f "$2/go" ]; do sleep 0.02; done
        [ -z "$NATIVE_SESSION" ] && echo "SESSION ${CREATE_SESSION:-native-$$}"
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
    let tests: [(&str, fn()); 45] = [
        (
            "completed_journal_without_identity_blocks_recovery",
            completed_journal_without_identity_blocks_recovery,
        ),
        (
            "complete_replay_policy_refusal_without_identity_blocks_recovery",
            complete_replay_policy_refusal_without_identity_blocks_recovery,
        ),
        (
            "complete_replay_missing_journal_without_identity_blocks_recovery",
            complete_replay_missing_journal_without_identity_blocks_recovery,
        ),
        (
            "complete_replay_error_with_known_identity_continues",
            complete_replay_error_with_known_identity_continues,
        ),
        (
            "unobserved_native_identity_blocks_recovery",
            unobserved_native_identity_blocks_recovery,
        ),
        (
            "chosen_native_id_is_not_observed_identity",
            chosen_native_id_is_not_observed_identity,
        ),
        (
            "journal_only_native_identity_recovers_exactly",
            journal_only_native_identity_recovers_exactly,
        ),
        (
            "prepared_spawn_failure_allows_fresh_start",
            prepared_spawn_failure_allows_fresh_start,
        ),
        (
            "refused_preparation_allows_fresh_start_after_reopen",
            refused_preparation_allows_fresh_start_after_reopen,
        ),
        (
            "queued_refusal_recovers_non_start_after_unlock",
            queued_refusal_recovers_non_start_after_unlock,
        ),
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
        (
            "sdk_host_rejected_turn_preserves_native_report",
            sdk_host_rejected_turn_preserves_native_report,
        ),
        (
            "sdk_host_late_record_failure_is_not_durable_completion",
            sdk_host_late_record_failure_is_not_durable_completion,
        ),
        (
            "sdk_host_rejected_record_failure_preserves_publication_uncertainty",
            sdk_host_rejected_record_failure_preserves_publication_uncertainty,
        ),
        (
            "sdk_host_starts_from_prepared_argv_and_attributes_turns",
            sdk_host_starts_from_prepared_argv_and_attributes_turns,
        ),
        (
            "sdk_host_native_failure_is_reported_not_success",
            sdk_host_native_failure_is_reported_not_success,
        ),
        (
            "sdk_host_resume_after_loss_reports_reconciliation_and_never_reruns",
            sdk_host_resume_after_loss_reports_reconciliation_and_never_reruns,
        ),
        (
            "sdk_host_refuses_resume_the_endpoint_did_not_declare",
            sdk_host_refuses_resume_the_endpoint_did_not_declare,
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
    fn create_native_session_id(&self) -> Option<String> {
        std::fs::read_to_string(self.runs.parent().unwrap().join("create-id")).ok()
    }

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
        command.command_mut().env(
            "CREATE_SESSION",
            self.turn.create_native_session_id.as_deref().unwrap_or(""),
        );
        if self.turn.prompt == "spawnfail" {
            // The effect-gate process cannot spawn with this nonexistent cwd.
            command
                .command_mut()
                .current_dir(self.runs.parent().unwrap().join("missing-cwd"));
        }
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

fn queued_refusal_recovers_non_start_after_unlock() {
    let fixture = Fixture::new();
    let mut client = fixture.start();
    let session = client.open(&fixture.cwd());
    // This control concerns the queued input's non-start. Establish native
    // identity independently before cancelling the separate running turn;
    // cancellation before SESSION would leave session continuity uncertain.
    let seed = client.prompt(&session, "reply establish identity", None);
    let seed = message_id(&client.response(seed));
    client.idle_for(&seed);
    let (running, _marks) = barrier(&fixture, &mut client, &session);
    let queued = client.prompt(&session, "reply must not run", Some("queued"));
    let dir = bounds_dir(&fixture, &session);
    let deadline = Instant::now() + TIMEOUT;
    let input_path = loop {
        let found = std::fs::read_dir(dir.join("inputs"))
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| bounds_read(path)["message_key"] == json!("queued"));
        if let Some(path) = found {
            break path;
        }
        assert!(Instant::now() < deadline, "queued input was not published");
        std::thread::sleep(Duration::from_millis(20));
    };
    let input = bounds_read(&input_path);
    assert_eq!(input["dispatched"], json!(false));
    let key =
        agent_provider_execution::custody::request_key(None, input["request_id"].as_str().unwrap());
    let lock = RequestCustody::acquire(&dir.join("turns"), &key).unwrap();
    client.send(json!({"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":session}}));
    client.response(running);
    assert_eq!(client.response(queued)["error"]["code"], json!(-32011));
    assert_eq!(bounds_read(&input_path)["phase"], json!("uncertain"));
    assert_eq!(
        client.call("session/close", json!({"sessionId":session}))["error"]["code"],
        json!(-32012)
    );
    assert!(!client.end().success());
    drop(lock);
    let (mut client, resumed) = bounds_resume(&fixture, &session);
    assert_eq!(resumed["result"], json!({}));
    let duplicate = client.prompt(&session, "ignored", Some("queued"));
    assert_eq!(client.response(duplicate)["error"]["code"], json!(-32010));
    assert_eq!(bounds_read(&input_path)["phase"], json!("not_inserted"));
    assert!(!dir.join("turns").join(format!("{key}.json")).exists());
    assert!(!dir.join("turns").join(format!("{key}.jsonl")).exists());
    assert_eq!(
        fixture.runs(),
        2,
        "only seed and running turn were dispatched"
    );
    assert_eq!(
        client.call("session/close", json!({"sessionId":session}))["result"],
        json!({})
    );
    assert!(client.end().success());
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
    // Same-session continuation requires that the provider observed the native
    // session before its loss. The fake emits CONSUMED before SESSION, so the
    // ACK and marker files do not prove that; the later output line does.
    first.update("waiting after session marker", |u| {
        u["sessionUpdate"] == json!("agent_message")
    });
    let actor = LostActor {
        leader: wait_for(&marks.join("leader.pid")),
        descendant: wait_for(&marks.join("descendant.pid")),
    };
    // Provider loss: no settlement by the lost process.
    first.child.kill().unwrap();
    first.child.wait().unwrap();
    assert!(
        alive(actor.descendant),
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
    assert_dies(actor.descendant);
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
    assert_eq!(said, format!("native=native-{}", actor.leader));
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

// ---- SDK host client against this endpoint ---------------------------------

/// A host-supplied transport over the endpoint process's stdio whose reads
/// time out, so a missing message fails the test instead of hanging it.
struct EndpointTransport {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: mpsc::Receiver<String>,
}

impl EndpointTransport {
    /// Starts the endpoint from the prepared argv, as a host would.
    fn spawn(argv: &[String]) -> Self {
        let mut child = Command::new(&argv[0])
            .args(&argv[1..])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { return };
                if send.send(line).is_err() {
                    return;
                }
            }
        });
        let stdin = child.stdin.take();
        Self {
            child,
            stdin,
            lines,
        }
    }

    fn end(mut self) -> std::process::ExitStatus {
        self.stdin.take();
        self.child.wait().unwrap()
    }
}

impl Transport for EndpointTransport {
    fn send(&mut self, message: &Value) -> Result<(), PeerClosed> {
        let stdin = self.stdin.as_mut().ok_or(PeerClosed)?;
        writeln!(stdin, "{message}")
            .and_then(|()| stdin.flush())
            .map_err(|_| PeerClosed)
    }

    fn recv(&mut self) -> Incoming {
        match self.lines.recv_timeout(TIMEOUT) {
            Ok(line) => match serde_json::from_str(&line) {
                Ok(value) => Incoming::Message(value),
                Err(_) => Incoming::Malformed(line),
            },
            Err(mpsc::RecvTimeoutError::Disconnected) => Incoming::Closed,
            Err(mpsc::RecvTimeoutError::Timeout) => panic!("endpoint silent for {TIMEOUT:?}"),
        }
    }
}

/// The stand-in provider's `resident.prepare` answer: this test binary is the
/// registered executable, and these are the arguments it declares.
fn prepared(fixture: &Fixture, operations: &[&str]) -> PreparedEndpoint {
    let mut result = extension::ResidentPrepareResult::v1(
        vec!["--resident".into(), fixture.state().display().to_string()],
        "0".repeat(64),
    );
    result.operations = operations.iter().map(|op| (*op).to_owned()).collect();
    PreparedEndpoint::agree(&result).unwrap()
}

fn sdk_client(endpoint: &PreparedEndpoint) -> AcpClient<EndpointTransport> {
    let executable = std::env::current_exe().unwrap();
    let argv = endpoint.argv(&executable.display().to_string());
    AcpClient::new(
        EndpointTransport::spawn(&argv),
        ClientInfo {
            name: "sdk-host-test".into(),
            version: "0".into(),
        },
    )
}

fn agent_text(client: &AcpClient<EndpointTransport>, parent: &str) -> String {
    client
        .events()
        .iter()
        .filter_map(|event| match event {
            SessionEvent::AgentMessage {
                text,
                parent_message_id: Some(id),
                ..
            } if id == parent => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

fn sdk_host_starts_from_prepared_argv_and_attributes_turns() {
    let fixture = Fixture::new();
    let endpoint = prepared(&fixture, extension::OPERATIONS);
    let mut client = sdk_client(&endpoint);
    let mut session = host::start_session(
        &mut client,
        &endpoint,
        SessionStart::New { cwd: fixture.cwd() },
    )
    .unwrap();
    // The native session is attributed to the endpoint; nothing is bound.
    assert_eq!(session.native.agent_name, "fixture-resident");
    assert_eq!(session.native.protocol, extension::PROTOCOL);
    assert_eq!(session.native.config_sha256, "0".repeat(64));
    assert!(!session.resumed);
    assert_eq!(session.binding(), &Binding::Unbound);

    let mut first = OutboundMessage::fresh("reply hello").unwrap();
    let delivery = host::send_turn(&mut client, &session, &mut first);
    let DeliveryOutcome::Accepted(acceptance) = &delivery.outcome else {
        panic!("not accepted: {:?}", delivery.outcome);
    };
    assert_eq!(acceptance.basis, Some(AtMostOnceBasis::SingleAttempt));
    let turn = delivery.turn.unwrap();
    assert_eq!(turn.session_id, session.native.session_id);
    let end = host::await_turn_end(&mut client, &turn).unwrap();
    assert!(end.is_own());
    assert_eq!(end.stop_reason.as_deref(), Some("end_turn"));
    let native = end.native_turn.unwrap();
    assert_eq!(native.custody, NativeCustody::Complete);
    assert!(!native.request_id.is_empty());
    assert_eq!(agent_text(&client, &turn.message_id), "hello");

    // The next turn continues the native session the first one created.
    let mut second = OutboundMessage::fresh("whoami").unwrap();
    let next = host::send_turn(&mut client, &session, &mut second)
        .turn
        .unwrap();
    assert!(next.message_id > turn.message_id);
    let end = host::await_turn_end(&mut client, &next).unwrap();
    assert!(end.is_own());
    assert!(agent_text(&client, &next.message_id).starts_with("native=native-"));
    assert_ne!(
        end.native_turn.unwrap().request_id,
        native.request_id,
        "each turn is its own launch"
    );
    // A later turn's end also covers the earlier input.
    let covered = host::await_turn_end(&mut client, &turn).unwrap();
    assert!(
        covered.is_own(),
        "the earlier input's own idle is found first"
    );

    // Only the host binds a canonical reference.
    session.bind("host-chain-7/segment-1");
    assert_eq!(
        session.binding(),
        &Binding::Bound("host-chain-7/segment-1".into())
    );
    assert_eq!(fixture.runs(), 2);
    assert!(client.into_transport().end().success());
}

fn sdk_host_native_failure_is_reported_not_success() {
    let fixture = Fixture::new();
    let endpoint = prepared(&fixture, extension::OPERATIONS);
    let mut client = sdk_client(&endpoint);
    let session = host::start_session(
        &mut client,
        &endpoint,
        SessionStart::New { cwd: fixture.cwd() },
    )
    .unwrap();
    let mut message = OutboundMessage::fresh("fail").unwrap();
    let turn = host::send_turn(&mut client, &session, &mut message)
        .turn
        .unwrap();
    let end = host::await_turn_end(&mut client, &turn).unwrap();
    assert_eq!(end.stop_reason.as_deref(), Some("_oulipoly_native_failed"));
    let native = end.native_turn.unwrap();
    assert_eq!(native.report["status"], json!({"kind":"exited","code":1}));
    assert!(client.into_transport().end().success());
}

fn sdk_host_resume_after_loss_reports_reconciliation_and_never_reruns() {
    let fixture = Fixture::new();
    let endpoint = prepared(&fixture, extension::OPERATIONS);
    let marks = fixture.dir.path().join("hang");
    std::fs::create_dir(&marks).unwrap();
    let mut client = sdk_client(&endpoint);
    let session = host::start_session(
        &mut client,
        &endpoint,
        SessionStart::New { cwd: fixture.cwd() },
    )
    .unwrap();
    // The host records the fresh key in its own store before sending.
    let mut recorded = None;
    let mut message = OutboundMessage::fresh_recorded(format!("hang {}", marks.display()), |key| {
        recorded = Some(key.as_str().to_owned());
        Ok::<(), std::io::Error>(())
    })
    .unwrap();
    let turn = host::send_turn(&mut client, &session, &mut message)
        .turn
        .unwrap();
    let descendant = wait_for(&marks.join("descendant.pid"));
    // Provider loss: no settlement by the lost process.
    let mut lost = client.into_transport();
    lost.child.kill().unwrap();
    lost.child.wait().unwrap();

    let mut client = sdk_client(&endpoint);
    let resumed = host::start_session(
        &mut client,
        &endpoint,
        SessionStart::Resume {
            session_id: session.native.session_id.clone(),
            cwd: fixture.cwd(),
        },
    )
    .unwrap();
    assert!(resumed.resumed);
    assert_eq!(resumed.binding(), &Binding::Unbound, "never inherited");
    assert_dies(descendant);
    // The key rebuilt from the host's store has unknown history: the
    // earlier insertion is recovered, never labelled at-most-once.
    let key = MessageKey::new(recorded.unwrap()).unwrap();
    let mut again = OutboundMessage::new(key, format!("hang {}", marks.display()));
    let delivery = host::send_turn(&mut client, &resumed, &mut again);
    let DeliveryOutcome::DuplicateUnknown(acceptance) = &delivery.outcome else {
        panic!("not a recovered duplicate: {:?}", delivery.outcome);
    };
    assert!(acceptance.recovered);
    assert_eq!(acceptance.message_id, turn.message_id);
    let end = host::await_turn_end(&mut client, &delivery.turn.unwrap()).unwrap();
    assert_eq!(
        end.stop_reason.as_deref(),
        Some("_oulipoly_reconciliation_required")
    );
    assert_eq!(end.native_turn.unwrap().custody, NativeCustody::Reconciled);
    assert_eq!(fixture.runs(), 1, "the interrupted input never ran again");
    assert!(client.into_transport().end().success());
}

fn sdk_host_refuses_resume_the_endpoint_did_not_declare() {
    let fixture = Fixture::new();
    let endpoint = prepared(
        &fixture,
        &[
            "initialize",
            "session/new",
            "session/prompt",
            "session/close",
        ],
    );
    let mut client = sdk_client(&endpoint);
    let refused = host::start_session(
        &mut client,
        &endpoint,
        SessionStart::Resume {
            session_id: "any".into(),
            cwd: fixture.cwd(),
        },
    );
    assert_eq!(
        refused,
        Err(StartRefusal::OperationNotServed("session/resume"))
    );
    assert!(client.peer().is_none(), "refused before initialize");
    assert!(client.into_transport().end().success());
    assert_eq!(fixture.runs(), 0);
}

fn sdk_host_rejected_turn_preserves_native_report() {
    let fixture = Fixture::new();
    let endpoint = prepared(&fixture, extension::OPERATIONS);
    let mut client = sdk_client(&endpoint);
    let session = host::start_session(
        &mut client,
        &endpoint,
        SessionStart::New { cwd: fixture.cwd() },
    )
    .unwrap();
    let mut message = OutboundMessage::fresh("noconsume").unwrap();
    let delivery = host::send_turn(&mut client, &session, &mut message);
    assert!(delivery.turn.is_none());
    assert!(matches!(
        delivery.outcome,
        DeliveryOutcome::Rejected { code: -32010, .. }
    ));
    let native = delivery
        .outcome
        .native_turn()
        .expect("rejection's native report must reach host");
    assert_eq!(native.custody, NativeCustody::Complete);
    assert_eq!(native.report["status"], json!({"kind":"exited","code":3}));
    assert!(!native.request_id.is_empty());
    assert!(message.is_owed());
    assert_eq!(message.unacknowledged_attempts(), 1);
    assert_eq!(fixture.runs(), 1);
    assert!(client.into_transport().end().success());
}

fn obstruct_input_store(fixture: &Fixture, session: &str) -> PathBuf {
    let dir = fixture.state().join("sessions").join(session);
    std::fs::rename(dir.join("inputs"), dir.join("inputs-held")).unwrap();
    std::fs::write(dir.join("inputs"), "ENOTDIR").unwrap();
    dir
}

fn restore_input_store(dir: &Path) {
    std::fs::remove_file(dir.join("inputs")).unwrap();
    std::fs::rename(dir.join("inputs-held"), dir.join("inputs")).unwrap();
}

fn sdk_host_late_record_failure_is_not_durable_completion() {
    let fixture = Fixture::new();
    let endpoint = prepared(&fixture, extension::OPERATIONS);
    let mut client = sdk_client(&endpoint);
    let session = host::start_session(
        &mut client,
        &endpoint,
        SessionStart::New { cwd: fixture.cwd() },
    )
    .unwrap();
    let marks = fixture.dir.path().join("endbarrier");
    std::fs::create_dir(&marks).unwrap();
    let mut message = OutboundMessage::fresh(format!("endbarrier {}", marks.display())).unwrap();
    let delivery = host::send_turn(&mut client, &session, &mut message);
    assert!(matches!(delivery.outcome, DeliveryOutcome::Accepted(_)));
    let turn = delivery.turn.unwrap();
    wait_for(&marks.join("ready"));
    // The insertion ACK is already durable. Fail only the final input record.
    let dir = obstruct_input_store(&fixture, &session.native.session_id);
    std::fs::write(marks.join("release"), "release").unwrap();
    let end = host::await_turn_end(&mut client, &turn).unwrap();
    assert!(end.is_own());
    assert_eq!(end.native_turn.unwrap().custody, NativeCustody::Complete);
    let event = client
        .receive_event()
        .unwrap()
        .expect("late record diagnostic");
    let SessionEvent::Other {
        session_id,
        kind,
        update,
    } = event
    else {
        panic!("{event:?}")
    };
    assert_eq!(session_id, session.native.session_id);
    assert_eq!(kind, "session_info_update");
    let report = &update["_meta"]["oulipoly.ai/nativeTurn"];
    assert_eq!(report["message_id"], turn.message_id);
    assert!(!report["record_error"].as_str().unwrap().is_empty());
    assert!(agent_provider_contract::acp::NativeTurn::from_report(report).is_none());
    restore_input_store(&dir);
    let input: Value = serde_json::from_slice(
        &std::fs::read(dir.join("inputs").join(format!("{}.json", turn.message_id))).unwrap(),
    )
    .unwrap();
    assert_eq!(
        input["phase"], "inserted",
        "old durable input does not claim ended"
    );
    assert_eq!(fixture.runs(), 1);
    assert!(client.into_transport().end().success());
}

fn sdk_host_rejected_record_failure_preserves_publication_uncertainty() {
    let fixture = Fixture::new();
    let endpoint = prepared(&fixture, extension::OPERATIONS);
    let mut client = sdk_client(&endpoint);
    let session = host::start_session(
        &mut client,
        &endpoint,
        SessionStart::New { cwd: fixture.cwd() },
    )
    .unwrap();
    let marks = fixture.dir.path().join("barrier");
    std::fs::create_dir(&marks).unwrap();
    let question = format!("barrier {}", marks.display());
    let session_id = session.native.session_id.clone();
    let attempt = std::thread::spawn(move || {
        let mut message = OutboundMessage::fresh(question).unwrap();
        let delivery = host::send_turn(&mut client, &session, &mut message);
        (client, delivery, message)
    });
    wait_for(&marks.join("ready"));
    let dir = obstruct_input_store(&fixture, &session_id);
    std::fs::write(marks.join("release"), "release").unwrap();
    let (client, delivery, message) = attempt.join().unwrap();
    assert!(delivery.turn.is_none());
    let DeliveryOutcome::Rejected {
        code,
        data: Some(data),
        ..
    } = &delivery.outcome
    else {
        panic!("{:?}", delivery.outcome)
    };
    assert_eq!(*code, -32011);
    assert_eq!(
        delivery.outcome.native_turn().unwrap().custody,
        NativeCustody::Complete
    );
    assert_eq!(
        data["nativeTurn"]["status"],
        json!({"kind":"exited","code":0})
    );
    assert!(!data["recordError"]["message_id"]
        .as_str()
        .unwrap()
        .is_empty());
    assert!(!data["recordError"]["record_error"]
        .as_str()
        .unwrap()
        .is_empty());
    assert!(message.is_owed());
    assert_eq!(message.unacknowledged_attempts(), 1);
    assert_eq!(fixture.runs(), 1);
    restore_input_store(&dir);
    assert!(client.into_transport().end().success());
}

// D3: interruption may hide native identity even though insertion and actor
// custody are known. The supplied U219 specimen gates SESSION until the
// endpoint is SIGSTOPed, so no timing race or invented recovered id is needed.
fn unobserved_native_identity_blocks_recovery() {
    unobserved_native_identity(None);
}
fn chosen_native_id_is_not_observed_identity() {
    unobserved_native_identity(Some("12345678-1234-1234-1234-123456789abc"));
}
fn unobserved_native_identity(chosen: Option<&str>) {
    let fixture = Fixture::new();
    if let Some(chosen) = chosen {
        std::fs::create_dir_all(fixture.state()).unwrap();
        std::fs::write(fixture.state().join("create-id"), chosen).unwrap();
    }
    let mut first = fixture.start();
    let session = first.open(&fixture.cwd());
    let marks = fixture.dir.path().join("late-session");
    std::fs::create_dir(&marks).unwrap();
    let prompt = format!("hanglate {}", marks.display());
    let running = first.prompt(&session, &prompt, Some("lost"));
    let id = message_id(&first.response(running));
    assert_eq!(
        unsafe { libc::kill(first.child.id() as i32, libc::SIGSTOP) },
        0
    );
    std::fs::write(marks.join("go"), "").unwrap();
    let actor = LostActor {
        leader: wait_for(&marks.join("leader.pid")),
        descendant: wait_for(&marks.join("descendant.pid")),
    };
    first.child.kill().unwrap();
    first.child.wait().unwrap();
    assert!(alive(actor.descendant));
    let dir = bounds_dir(&fixture, &session);
    let original_session = bounds_read(&dir.join("session.json"));
    assert!(original_session["native_session_id"].is_null());
    assert_eq!(original_session["create_native_session_id"], json!(chosen));
    let journal = bounds_journal(&fixture, &session, &id);
    let journal_bytes = std::fs::read(&journal).unwrap();
    let events = std::str::from_utf8(&journal_bytes)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert!(events
        .iter()
        .any(|event| event["name"] == json!(resident::SUBMITTED_USER_TURN_MARKER)));
    assert!(!events
        .iter()
        .any(|event| event["name"] == json!(resident::PROVIDER_SESSION_MARKER)));
    let launch_bytes = std::fs::read(journal.with_extension("json")).unwrap();
    let input_path = bounds_input(&fixture, &session, &id);
    let original_input = bounds_read(&input_path);

    let (mut second, resumed) = bounds_resume(&fixture, &session);
    assert_identity_uncertain(&resumed);
    assert_dies(actor.leader);
    assert_dies(actor.descendant);
    let duplicate = second.prompt(&session, "different bytes", Some("lost"));
    let duplicate = second.response(duplicate);
    assert_eq!(message_id(&duplicate), id);
    assert_eq!(
        duplicate["result"]["_meta"][resident::DUPLICATE_META],
        json!(true)
    );
    let idle = second.idle_for(&id);
    assert_eq!(
        idle["stopReason"],
        json!("_oulipoly_reconciliation_required")
    );
    assert_eq!(
        idle["_meta"][resident::NATIVE_TURN_META]["custody"],
        json!("reconciled")
    );
    let next = second.prompt(&session, "whoami", Some("next"));
    assert_identity_uncertain(&second.response(next));
    let record = bounds_read(&dir.join("session.json"));
    assert!(record["native_session_id"].is_null());
    assert_eq!(record["create_native_session_id"], json!(chosen));
    assert!(record["native_session_uncertain"].is_string());
    let recovered = bounds_read(&input_path);
    for key in [
        "prompt",
        "prompt_sha256",
        "request_id",
        "message_id",
        "native_session_id",
        "create_native_session_id",
    ] {
        assert_eq!(recovered[key], original_input[key], "retained {key}");
    }
    assert_eq!(std::fs::read(&journal).unwrap(), journal_bytes);
    assert_eq!(
        std::fs::read(journal.with_extension("json")).unwrap(),
        launch_bytes
    );
    assert_eq!(
        fixture.runs(),
        1,
        "duplicate and new input start no native work"
    );
    assert_eq!(
        second.call("session/close", json!({"sessionId":session}))["result"],
        json!({})
    );
    assert!(second.end().success());

    let (mut reopened, response) = bounds_resume(&fixture, &session);
    assert_identity_uncertain(&response);
    let next = reopened.prompt(&session, "reply must not create or resume", None);
    assert_identity_uncertain(&reopened.response(next));
    let duplicate = reopened.prompt(&session, "ignored", Some("lost"));
    assert_eq!(message_id(&reopened.response(duplicate)), id);
    reopened.idle_for(&id);
    assert_eq!(fixture.runs(), 1);
    assert!(reopened.end().success());
    println!("unobserved identity candidate={chosen:?}: uncertainty visible, actors settled, runs=1, duplicate retained after reopen");
}
fn assert_identity_uncertain(response: &Value) {
    assert_eq!(response["error"]["code"], json!(-32012), "{response}");
    assert!(
        response["error"]["message"]
            .as_str()
            .unwrap()
            .contains("native session identity is uncertain"),
        "{response}"
    );
    assert!(
        !response["error"]["message"]
            .as_str()
            .unwrap()
            .contains("journal evidence unreadable"),
        "readable journal remains readable"
    );
}

fn journal_only_native_identity_recovers_exactly() {
    let fixture = Fixture::new();
    let (session, id, actor) = bounds_lost(&fixture);
    let path = bounds_dir(&fixture, &session).join("session.json");
    let mut record = bounds_read(&path);
    let native = record["native_session_id"].clone();
    assert_eq!(native, json!(format!("native-{}", actor.leader)));
    // Issued stale session snapshot: the untouched journal is sole identity evidence.
    record["native_session_id"] = Value::Null;
    bounds_write(&path, &record);
    let (mut second, resumed) = bounds_resume(&fixture, &session);
    assert_eq!(resumed["result"], json!({}), "{resumed}");
    assert_dies(actor.descendant);
    assert_eq!(bounds_read(&path)["native_session_id"], native);
    assert!(bounds_read(&path)["native_session_uncertain"].is_null());
    let duplicate = second.prompt(&session, "ignored", Some("bounds-lost"));
    assert_eq!(message_id(&second.response(duplicate)), id);
    second.idle_for(&id);
    assert_eq!(fixture.runs(), 1);
    let next = second.prompt(&session, "whoami", None);
    let next = message_id(&second.response(next));
    let text = second.update("exact journal identity", |u| {
        u["sessionUpdate"] == json!("agent_message")
    });
    assert_eq!(
        text["update"]["content"][0]["text"],
        json!(format!("native={}", native.as_str().unwrap()))
    );
    second.idle_for(&next);
    assert_eq!(fixture.runs(), 2);
    assert!(second.end().success());
}

fn prepared_spawn_failure_allows_fresh_start() {
    let fixture = Fixture::new();
    let mut first = fixture.start();
    let session = first.open(&fixture.cwd());
    let attempt = first.prompt(&session, "spawnfail", Some("no-start"));
    let refused = first.response(attempt);
    assert_eq!(refused["error"]["code"], json!(-32011));
    assert_eq!(
        first.call("session/close", json!({"sessionId":session}))["result"],
        json!({})
    );
    assert!(first.end().success());
    assert_eq!(fixture.runs(), 0, "native never started");
    let (mut reopened, resumed) = bounds_resume(&fixture, &session);
    assert_eq!(resumed["result"], json!({}), "{resumed}");
    let duplicate = reopened.prompt(&session, "ignored", Some("no-start"));
    assert_eq!(reopened.response(duplicate)["error"]["code"], json!(-32010));
    let next = reopened.prompt(&session, "reply fresh", None);
    let next = message_id(&reopened.response(next));
    reopened.idle_for(&next);
    assert_eq!(fixture.runs(), 1);
    assert!(reopened.end().success());
}

fn refused_preparation_allows_fresh_start_after_reopen() {
    let fixture = Fixture::new();
    let mut first = fixture.start();
    let session = first.open(&fixture.cwd());
    let attempt = first.prompt(&session, "refuse", Some("no-start"));
    assert_eq!(first.response(attempt)["error"]["code"], json!(-32010));
    assert!(first.end().success());
    assert_eq!(fixture.runs(), 0);
    let (mut reopened, resumed) = bounds_resume(&fixture, &session);
    assert_eq!(resumed["result"], json!({}));
    let next = reopened.prompt(&session, "reply fresh", None);
    let next = message_id(&reopened.response(next));
    reopened.idle_for(&next);
    assert_eq!(fixture.runs(), 1);
    assert!(reopened.end().success());
}

fn completed_journal_without_identity_blocks_recovery() {
    let fixture = Fixture::new();
    let mut first = fixture.start();
    let session = first.open(&fixture.cwd());
    // This fake ran and consumed the input, but supplied no SESSION marker.
    let request = first.prompt(&session, "fail", Some("complete"));
    let id = message_id(&first.response(request));
    assert_eq!(
        first.idle_for(&id)["stopReason"],
        json!("_oulipoly_native_failed")
    );
    let input_path = bounds_input(&fixture, &session, &id);
    let mut input = bounds_read(&input_path);
    let journal = bounds_journal(&fixture, &session, &id);
    let journal_bytes = std::fs::read(&journal).unwrap();
    let launch_bytes = std::fs::read(journal.with_extension("json")).unwrap();
    assert_eq!(
        bounds_read(&journal.with_extension("json"))["phase"],
        json!("complete")
    );
    first.child.kill().unwrap();
    first.child.wait().unwrap();
    // Issued durable snapshot lag: launch completion survived, input end did
    // not. This selects exact completed replay, not actor reconciliation.
    input["phase"] = json!("inserted");
    input["native_turn"] = Value::Null;
    input.as_object_mut().unwrap().remove("ended_unix_ms");
    bounds_write(&input_path, &input);
    let (mut second, resumed) = bounds_resume(&fixture, &session);
    assert_identity_uncertain(&resumed);
    let duplicate = second.prompt(&session, "ignored", Some("complete"));
    assert_eq!(message_id(&second.response(duplicate)), id);
    let idle = second.idle_for(&id);
    assert_eq!(idle["stopReason"], json!("_oulipoly_native_failed"));
    assert_eq!(
        idle["_meta"][resident::NATIVE_TURN_META]["custody"],
        json!("complete")
    );
    let next = second.prompt(&session, "whoami", None);
    assert_identity_uncertain(&second.response(next));
    assert_eq!(std::fs::read(&journal).unwrap(), journal_bytes);
    assert_eq!(
        std::fs::read(journal.with_extension("json")).unwrap(),
        launch_bytes
    );
    assert_eq!(
        fixture.runs(),
        1,
        "completed replay is receipt-only; later input blocked"
    );
    assert!(second.end().success());
}

// D3 / O3: valid complete custody whose recovery replay itself fails before it
// can supply identity or an exit. The completed fake native turn consumed its
// input and supplied no SESSION; an issued lagging input snapshot selects exact
// completed replay, then a current-policy refusal or a lost journal makes that
// replay error. Neither is a native failure or evidence of no native effect.
fn complete_replay_policy_refusal_without_identity_blocks_recovery() {
    let fixture = Fixture::new();
    let mut first = fixture.start();
    let session = first.open(&fixture.cwd());
    let id = complete_without_identity(&fixture, &mut first, &session);
    let journal = bounds_journal(&fixture, &session, &id);
    let journal_bytes = std::fs::read(&journal).unwrap();
    let launch_bytes = std::fs::read(journal.with_extension("json")).unwrap();
    let policy = fixture.state().join("refuse-current-policy");
    std::fs::write(&policy, "changed").unwrap();
    let (mut second, resumed) = bounds_resume(&fixture, &session);
    assert_identity_uncertain(&resumed);
    let idle = duplicate_idle(&mut second, &session, &id);
    assert_eq!(
        idle["_meta"][resident::NATIVE_TURN_META]["failure"]["code"],
        json!("current_policy_refused")
    );
    // Without the refusal a missing block would start a fresh native turn.
    std::fs::remove_file(&policy).unwrap();
    let next = second.prompt(&session, "whoami", None);
    assert_identity_uncertain(&second.response(next));
    assert!(
        bounds_read(&bounds_dir(&fixture, &session).join("session.json"))
            ["native_session_uncertain"]
            .is_string()
    );
    assert_eq!(std::fs::read(&journal).unwrap(), journal_bytes);
    assert_eq!(
        std::fs::read(journal.with_extension("json")).unwrap(),
        launch_bytes
    );
    assert_eq!(fixture.runs(), 1, "replay error starts no native work");
    assert!(second.end().success());
    let (mut reopened, response) = bounds_resume(&fixture, &session);
    assert_identity_uncertain(&response);
    let next = reopened.prompt(&session, "whoami", None);
    assert_identity_uncertain(&reopened.response(next));
    assert_eq!(fixture.runs(), 1);
    assert!(reopened.end().success());
}

fn complete_replay_missing_journal_without_identity_blocks_recovery() {
    let fixture = Fixture::new();
    let mut first = fixture.start();
    let session = first.open(&fixture.cwd());
    let id = complete_without_identity(&fixture, &mut first, &session);
    let journal = bounds_journal(&fixture, &session, &id);
    let launch_bytes = std::fs::read(journal.with_extension("json")).unwrap();
    std::fs::remove_file(&journal).unwrap();
    let (mut second, resumed) = bounds_resume(&fixture, &session);
    assert_identity_uncertain(&resumed);
    let idle = duplicate_idle(&mut second, &session, &id);
    let failure = &idle["_meta"][resident::NATIVE_TURN_META]["failure"];
    assert!(failure["code"].is_string(), "{idle}");
    assert_ne!(failure["code"], json!("journal_evidence_unreadable"));
    let next = second.prompt(&session, "whoami", None);
    assert_identity_uncertain(&second.response(next));
    assert_eq!(
        std::fs::read(journal.with_extension("json")).unwrap(),
        launch_bytes
    );
    assert!(!journal.exists(), "recovery does not recreate the journal");
    assert_eq!(fixture.runs(), 1, "replay error starts no native work");
    assert!(second.end().success());
    let (reopened, response) = bounds_resume(&fixture, &session);
    assert_identity_uncertain(&response);
    assert_eq!(fixture.runs(), 1);
    assert!(reopened.end().success());
}

// The same replay error is not an identity loss when identity was observed.
fn complete_replay_error_with_known_identity_continues() {
    let fixture = Fixture::new();
    let mut first = fixture.start();
    let session = first.open(&fixture.cwd());
    let seed = first.prompt(&session, "reply seed", Some("seed"));
    let seed = message_id(&first.response(seed));
    assert_eq!(first.idle_for(&seed)["stopReason"], json!("end_turn"));
    let dir = bounds_dir(&fixture, &session);
    let native = bounds_read(&dir.join("session.json"))["native_session_id"].clone();
    assert!(native.is_string());
    let id = complete_without_identity(&fixture, &mut first, &session);
    std::fs::write(fixture.state().join("refuse-current-policy"), "changed").unwrap();
    let (mut second, resumed) = bounds_resume(&fixture, &session);
    assert_eq!(resumed["result"], json!({}), "{resumed}");
    let idle = duplicate_idle(&mut second, &session, &id);
    assert_eq!(
        idle["_meta"][resident::NATIVE_TURN_META]["failure"]["code"],
        json!("current_policy_refused")
    );
    let record = bounds_read(&dir.join("session.json"));
    assert!(record["native_session_uncertain"].is_null());
    assert_eq!(record["native_session_id"], native);
    std::fs::remove_file(fixture.state().join("refuse-current-policy")).unwrap();
    let next = second.prompt(&session, "whoami", None);
    let next = message_id(&second.response(next));
    let text = second.update("known identity continues", |u| {
        u["sessionUpdate"] == json!("agent_message")
    });
    assert_eq!(
        text["update"]["content"][0]["text"],
        json!(format!("native={}", native.as_str().unwrap()))
    );
    second.idle_for(&next);
    assert_eq!(fixture.runs(), 3, "seed, completed fail and later turn");
    assert!(second.end().success());
}

// Runs a completed native failure that supplied no SESSION, ends the provider
// and issues the lagging input snapshot (launch complete, input end lost).
fn complete_without_identity(fixture: &Fixture, first: &mut Client, session: &str) -> String {
    let request = first.prompt(session, "fail", Some("complete"));
    let id = message_id(&first.response(request));
    assert_eq!(
        first.idle_for(&id)["stopReason"],
        json!("_oulipoly_native_failed")
    );
    let journal = bounds_journal(fixture, session, &id);
    assert_eq!(
        bounds_read(&journal.with_extension("json"))["phase"],
        json!("complete")
    );
    first.child.kill().unwrap();
    first.child.wait().unwrap();
    let input_path = bounds_input(fixture, session, &id);
    let mut input = bounds_read(&input_path);
    input["phase"] = json!("inserted");
    input["native_turn"] = Value::Null;
    input.as_object_mut().unwrap().remove("ended_unix_ms");
    bounds_write(&input_path, &input);
    id
}

// The original duplicate still receives its receipt and a truthful replay
// error: a settled launch without exit, not a native failure or corruption.
fn duplicate_idle(client: &mut Client, session: &str, id: &str) -> Value {
    let duplicate = client.prompt(session, "ignored", Some("complete"));
    assert_eq!(message_id(&client.response(duplicate)), id);
    let idle = client.idle_for(id);
    assert_eq!(idle["stopReason"], json!("_oulipoly_turn_failed"));
    let native_turn = &idle["_meta"][resident::NATIVE_TURN_META];
    assert_eq!(native_turn["custody"], json!("complete_without_exit"));
    assert_ne!(
        native_turn["failure"]["code"],
        json!("journal_evidence_unreadable")
    );
    idle
}
