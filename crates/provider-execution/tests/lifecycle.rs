//! Shared launch lifecycle through a real provider process.
//!
//! The test binary is its own provider. Invoked with `--provider`, it runs one
//! launch through `run_launch` with a minimal adapter over a `/bin/sh` fake
//! native program, writes launch events to bounded stdout, and reports any
//! lifecycle failure on stderr. Tests drive it as a separate process so
//! termination signals, provider loss and process-group custody are real.
#![cfg(target_os = "linux")]

use agent_provider_execution::custody::{LaunchState, RequestCustody};
use agent_provider_execution::delivery::BoundedOutput;
use agent_provider_execution::lifecycle::{
    run_launch, Channel, EventSink, LaunchAdapter, LaunchSpec, LifecycleError, LifecycleTiming,
    NativeCommand, NativeOutcome, OutputFraming, Preparation, StopCause, Terminal,
};
use agent_provider_execution::process::{run_effect_gate, EffectGate, GatedCommand};
use serde_json::{json, Value};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

const GATE_ARG: &str = "__lifecycle_fixture_gate";
const GATE_ENV: &str = "LIFECYCLE_FIXTURE_EFFECT_GATE_FD";
const CONTRACT: &str = "fixture.contract/v1";

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    match args.get(1).map(String::as_str) {
        Some(GATE_ARG) => std::process::exit(run_effect_gate(&args, GATE_ENV)),
        Some("--provider") => std::process::exit(provider(&args[2])),
        _ => {}
    }
    let tests: [(&str, fn()); 10] = [
        (
            "completed_launch_replays_exactly_without_native_effects",
            completed_launch_replays_exactly_without_native_effects,
        ),
        (
            "changed_inputs_for_a_used_request_conflict_before_native_effects",
            changed_inputs_for_a_used_request_conflict_before_native_effects,
        ),
        (
            "elapsed_deadline_refuses_admission_and_discards_sidecars",
            elapsed_deadline_refuses_admission_and_discards_sidecars,
        ),
        (
            "deadline_terminates_the_native_group_and_completes",
            deadline_terminates_the_native_group_and_completes,
        ),
        (
            "termination_signal_cancels_the_native_group_and_completes",
            termination_signal_cancels_the_native_group_and_completes,
        ),
        (
            "lost_provider_leaves_an_actor_that_retry_discharges_before_reconciliation",
            lost_provider_leaves_an_actor_that_retry_discharges_before_reconciliation,
        ),
        (
            "leader_exit_terminates_descendants_holding_output_and_drains",
            leader_exit_terminates_descendants_holding_output_and_drains,
        ),
        (
            "closed_output_without_leader_exit_fails_within_the_drain_grace",
            closed_output_without_leader_exit_fails_within_the_drain_grace,
        ),
        (
            "oversized_native_record_fails_without_completion",
            oversized_native_record_fails_without_completion,
        ),
        (
            "settled_launch_is_journaled_and_replayed",
            settled_launch_is_journaled_and_replayed,
        ),
    ];
    let mut failed = 0;
    for (name, test) in tests {
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
        tests.len() - failed
    );
    std::process::exit(i32::from(failed != 0));
}

// ---- fixture provider -------------------------------------------------------

struct FixtureAdapter {
    config: Value,
    sidecar: Option<PathBuf>,
}

#[derive(Debug)]
struct FixtureFailure(String);

impl From<LifecycleError> for FixtureFailure {
    fn from(error: LifecycleError) -> Self {
        let code = match error {
            LifecycleError::Busy => "busy",
            LifecycleError::RequestChanged => "request_changed",
            LifecycleError::ReconciliationRequired => "reconciliation_required",
            LifecycleError::Cancelled => "cancelled",
            LifecycleError::DeadlineElapsed => "deadline",
            LifecycleError::NativeStreamInvalid => "stream_invalid",
            LifecycleError::NativeStreamsClosed => "streams_closed",
            LifecycleError::NativeDrainIncomplete => "drain_incomplete",
            LifecycleError::InputStalled
            | LifecycleError::InputWriterFailed
            | LifecycleError::InputIncomplete => "input",
            other => return Self(format!("other:{other}")),
        };
        Self(code.into())
    }
}

impl LaunchAdapter for FixtureAdapter {
    type Failure = FixtureFailure;

    fn request_digest(&mut self) -> Result<String, FixtureFailure> {
        Ok(self.config["digest"].as_str().unwrap().into())
    }

    fn prepare(&mut self, custody: &RequestCustody) -> Result<Preparation, FixtureFailure> {
        let sidecar = custody.sibling("sidecar");
        std::fs::write(&sidecar, b"prepared").unwrap();
        self.sidecar = Some(sidecar);
        if self.config["settled"].as_bool() == Some(true) {
            return Ok(Preparation::Settled {
                events: vec![json!({"kind":"marker","name":"settled","value":true})],
                terminal: Terminal {
                    status: json!({"kind":"spawn_error","reason":"fixture"}),
                    terminal_signal: json!({"kind":"spawn_error"}),
                    session: None,
                    exit_code: 5,
                },
            });
        }
        let executable = std::env::current_exe().unwrap();
        let mut command = GatedCommand::new(
            &EffectGate {
                executable: &executable,
                argument: GATE_ARG,
                descriptor_env: GATE_ENV,
            },
            "/bin/sh",
            ["-c", self.config["script"].as_str().unwrap()],
        )
        .unwrap();
        command
            .command_mut()
            .current_dir(self.config["dir"].as_str().unwrap());
        Ok(Preparation::Native(NativeCommand {
            command,
            stdin: Some(b"native input\n".to_vec()),
            framing: OutputFraming::Lines {
                max_bytes: self.config["max_line"].as_u64().unwrap_or(1024),
            },
        }))
    }

    fn discard(&mut self, _custody: &RequestCustody) -> Result<(), FixtureFailure> {
        std::fs::remove_file(self.sidecar.as_ref().unwrap()).unwrap();
        Ok(())
    }

    fn started<W: Write>(&mut self, events: &mut EventSink<'_, W>) -> Result<(), FixtureFailure> {
        Ok(events.marker("started", json!(true))?)
    }

    fn output<W: Write>(
        &mut self,
        channel: Channel,
        bytes: Vec<u8>,
        events: &mut EventSink<'_, W>,
    ) -> Result<(), FixtureFailure> {
        Ok(events.data(channel, &bytes)?)
    }

    fn finish<W: Write>(
        &mut self,
        outcome: NativeOutcome,
        events: &mut EventSink<'_, W>,
    ) -> Result<Terminal, FixtureFailure> {
        events.marker("accounting", events.accounting().to_json())?;
        let (status, code) = match outcome.stopped {
            Some(StopCause::Deadline) => (json!({"kind":"deadline"}), 9),
            Some(StopCause::Cancelled { signal }) => {
                (json!({"kind":"cancelled","signal":signal}), 8)
            }
            None => {
                let code = outcome.status.code().unwrap_or(1);
                (json!({"kind":"exited","code":code}), code)
            }
        };
        Ok(Terminal {
            status,
            terminal_signal: json!({"kind":"fixture"}),
            session: Some(json!({"id":"fixture-session"})),
            exit_code: code,
        })
    }
}

fn provider(config: &str) -> i32 {
    let config: Value = serde_json::from_str(config).unwrap();
    let state_root = PathBuf::from(config["state_root"].as_str().unwrap());
    let deadline = config["deadline_unix_ms"].as_u64();
    let timing = LifecycleTiming {
        poll_interval: Duration::from_millis(20),
        heartbeat_interval: None,
        drain_grace: Duration::from_millis(config["drain_grace_ms"].as_u64().unwrap_or(500)),
    };
    let request_id = config["request_id"].as_str().unwrap().to_string();
    let spec = LaunchSpec {
        contract: CONTRACT,
        request_id: &request_id,
        provider_instance_id: Some("fixture-instance"),
        deadline_unix_ms: deadline,
        state_root: &state_root,
        timing,
    };
    let mut output = BoundedOutput::stdout().unwrap();
    let mut adapter = FixtureAdapter {
        config,
        sidecar: None,
    };
    match run_launch(&spec, &mut adapter, &mut output) {
        Ok(code) => code,
        Err(FixtureFailure(code)) => {
            eprintln!("lifecycle-error:{code}");
            70
        }
    }
}

// ---- harness ------------------------------------------------------------------

struct Fixture {
    root: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("state")).unwrap();
        Self { root }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.root.path().join(name)
    }

    fn config(&self, script: &str) -> Value {
        json!({
            "state_root": self.path("state"),
            "dir": self.root.path(),
            "request_id": "request-1",
            "digest": "digest-1",
            "script": script,
        })
    }

    fn spawn(&self, config: &Value) -> Child {
        Command::new(std::env::current_exe().unwrap())
            .arg("--provider")
            .arg(config.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    }

    fn run(&self, config: &Value) -> Output {
        self.spawn(config).wait_with_output().unwrap()
    }

    fn calls(&self) -> usize {
        std::fs::read_to_string(self.path("calls"))
            .map(|text| text.lines().count())
            .unwrap_or(0)
    }

    fn state(&self) -> Option<LaunchState> {
        let entries = std::fs::read_dir(self.path("state")).unwrap();
        for entry in entries {
            let path = entry.unwrap().path();
            if path.extension().and_then(|e| e.to_str()) == Some("json") {
                return Some(serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap());
            }
        }
        None
    }

    fn state_files(&self) -> Vec<String> {
        let mut names = std::fs::read_dir(self.path("state"))
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                path.extension().unwrap().to_str().unwrap().to_string()
            })
            .collect::<Vec<_>>();
        names.sort();
        names
    }
}

fn events(output: &Output) -> Vec<Value> {
    String::from_utf8(output.stdout.clone())
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn lifecycle_error(output: &Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    stderr
        .lines()
        .find_map(|line| line.strip_prefix("lifecycle-error:"))
        .unwrap_or_else(|| panic!("no lifecycle error in {stderr:?}"))
        .to_string()
}

fn wait_for(path: &Path) {
    let started = Instant::now();
    while !path.exists() {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "{path:?} never appeared"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn pid_from(path: &Path) -> i32 {
    wait_for(path);
    let started = Instant::now();
    loop {
        if let Ok(pid) = std::fs::read_to_string(path).unwrap().trim().parse() {
            return pid;
        }
        assert!(started.elapsed() < Duration::from_secs(10));
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn process_alive(pid: i32) -> bool {
    // A reaped or zombie-free process returns ESRCH. Zombies of unrelated
    // parents are reported by /proc state.
    if unsafe { libc::kill(pid, 0) } != 0 {
        return false;
    }
    std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .map(|stat| {
            !stat
                .rsplit(')')
                .next()
                .unwrap_or("")
                .trim_start()
                .starts_with('Z')
        })
        .unwrap_or(false)
}

fn assert_dies(pid: i32) {
    let started = Instant::now();
    while process_alive(pid) {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "process {pid} survived"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

const RECORD_CALL: &str = "echo call >> calls; ";

// ---- tests --------------------------------------------------------------------

fn completed_launch_replays_exactly_without_native_effects() {
    let fixture = Fixture::new();
    let config = fixture.config(&format!(
        "{RECORD_CALL}read line; echo \"got $line\"; echo diagnostic >&2; exit 3"
    ));
    let first = fixture.run(&config);
    assert_eq!(first.status.code(), Some(3), "{first:?}");
    let delivered = events(&first);
    assert_eq!(delivered.first().unwrap()["name"], "started");
    let exit = delivered.last().unwrap();
    assert_eq!(exit["kind"], "exit");
    assert_eq!(exit["status"], json!({"kind":"exited","code":3}));
    assert_eq!(exit["session"]["id"], "fixture-session");
    for (index, event) in delivered.iter().enumerate() {
        assert_eq!(event["contract"], CONTRACT);
        assert_eq!(event["request_id"], "request-1");
        assert_eq!(event["seq"], index as u64 + 1);
    }
    let stdout = delivered
        .iter()
        .find(|event| event["kind"] == "stdout")
        .unwrap();
    assert_eq!(stdout["data_base64"], "Z290IG5hdGl2ZSBpbnB1dAo="); // "got native input\n"
    let accounting = &delivered[delivered.len() - 2]["value"];
    assert_eq!(accounting["stdout"]["bytes"], 17);
    assert_eq!(accounting["stderr"]["bytes"], 11);
    assert_eq!(accounting["data_event_count"], 2);
    let state = fixture.state().unwrap();
    assert!(state.is_complete());
    assert_eq!(state.exit_code, Some(3));
    assert_eq!((state.actor_id, state.incarnation.as_deref()), (None, None));

    let replay = fixture.run(&config);
    assert_eq!(replay.status.code(), Some(3));
    assert_eq!(replay.stdout, first.stdout, "replay is byte-identical");
    assert_eq!(fixture.calls(), 1, "replay ran no native effect");
}

fn changed_inputs_for_a_used_request_conflict_before_native_effects() {
    let fixture = Fixture::new();
    let config = fixture.config(&format!("{RECORD_CALL}exit 0"));
    assert_eq!(fixture.run(&config).status.code(), Some(0));
    let mut changed = config.clone();
    changed["digest"] = json!("digest-2");
    let output = fixture.run(&changed);
    assert_eq!(lifecycle_error(&output), "request_changed");
    assert!(output.stdout.is_empty());
    assert_eq!(fixture.calls(), 1);
}

fn elapsed_deadline_refuses_admission_and_discards_sidecars() {
    let fixture = Fixture::new();
    let mut config = fixture.config(&format!("{RECORD_CALL}exit 0"));
    config["deadline_unix_ms"] = json!(1);
    let output = fixture.run(&config);
    assert_eq!(lifecycle_error(&output), "deadline");
    assert!(output.stdout.is_empty());
    assert_eq!(fixture.calls(), 0, "native program never ran");
    assert_eq!(
        fixture.state_files(),
        ["lock"],
        "no state or sidecar remains"
    );
    // Nothing durable was published, so the same request can run later.
    config["deadline_unix_ms"] = Value::Null;
    assert_eq!(fixture.run(&config).status.code(), Some(0));
    assert_eq!(fixture.calls(), 1);
}

fn deadline_terminates_the_native_group_and_completes() {
    let fixture = Fixture::new();
    let mut config = fixture.config("sleep 60 & echo $! > descendant; echo ready; exec sleep 60");
    let deadline = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 700;
    config["deadline_unix_ms"] = json!(deadline);
    let started = Instant::now();
    let output = fixture.run(&config);
    assert!(started.elapsed() < Duration::from_secs(10));
    assert_eq!(output.status.code(), Some(9), "{output:?}");
    let delivered = events(&output);
    assert_eq!(delivered.last().unwrap()["status"]["kind"], "deadline");
    assert_dies(pid_from(&fixture.path("descendant")));
    assert!(fixture.state().unwrap().is_complete());
}

fn termination_signal_cancels_the_native_group_and_completes() {
    let fixture = Fixture::new();
    let config = fixture.config("sleep 60 & echo $! > descendant; touch ready; exec sleep 60");
    let child = fixture.spawn(&config);
    wait_for(&fixture.path("ready"));
    assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGTERM) }, 0);
    let output = child.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(8), "{output:?}");
    let exit = events(&output).pop().unwrap();
    assert_eq!(
        exit["status"],
        json!({"kind":"cancelled","signal":libc::SIGTERM})
    );
    assert_dies(pid_from(&fixture.path("descendant")));
    let state = fixture.state().unwrap();
    assert!(state.is_complete());
    assert_eq!(state.exit_code, Some(8));
}

fn lost_provider_leaves_an_actor_that_retry_discharges_before_reconciliation() {
    let fixture = Fixture::new();
    let config = fixture.config(&format!(
        "{RECORD_CALL}sleep 60 & echo $! > descendant; touch ready; exec sleep 60"
    ));
    let mut child = fixture.spawn(&config);
    wait_for(&fixture.path("ready"));
    let descendant = pid_from(&fixture.path("descendant"));
    child.kill().unwrap();
    child.wait().unwrap();
    // The leader receives parent-death SIGKILL; its descendant keeps the group.
    assert!(
        process_alive(descendant),
        "orphaned descendant keeps the group"
    );
    let state = fixture.state().unwrap();
    assert_eq!(state.phase, "running");
    assert!(state.actor_id.unwrap() > 1);

    let retry = fixture.run(&config);
    assert_eq!(lifecycle_error(&retry), "reconciliation_required");
    assert!(retry.stdout.is_empty());
    assert_dies(descendant);
    assert_eq!(fixture.calls(), 1, "retry started no second native turn");
    assert_eq!(
        fixture.state().unwrap().phase,
        "running",
        "evidence is retained"
    );
    let again = fixture.run(&config);
    assert_eq!(lifecycle_error(&again), "reconciliation_required");
}

fn leader_exit_terminates_descendants_holding_output_and_drains() {
    let fixture = Fixture::new();
    let config = fixture.config("sleep 60 & echo $! > descendant; echo leader-done; exit 0");
    let started = Instant::now();
    let output = fixture.run(&config);
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "descendant-held pipe stranded completion"
    );
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let delivered = events(&output);
    assert!(delivered
        .iter()
        .any(|event| event["data_base64"] == "bGVhZGVyLWRvbmUK"));
    assert_eq!(
        delivered.last().unwrap()["status"],
        json!({"kind":"exited","code":0})
    );
    assert_dies(pid_from(&fixture.path("descendant")));
}

fn closed_output_without_leader_exit_fails_within_the_drain_grace() {
    let fixture = Fixture::new();
    let mut config = fixture.config("exec >&- 2>&-; echo $$ > leader; exec sleep 60");
    config["drain_grace_ms"] = json!(300);
    let started = Instant::now();
    let output = fixture.run(&config);
    assert!(started.elapsed() < Duration::from_secs(10));
    assert_eq!(lifecycle_error(&output), "streams_closed");
    assert_dies(pid_from(&fixture.path("leader")));
    let state = fixture.state().unwrap();
    assert_eq!(
        state.phase, "running",
        "no completion receipt after failure"
    );
    assert_eq!(state.journal_sha256, None);
}

fn oversized_native_record_fails_without_completion() {
    let fixture = Fixture::new();
    let mut config = fixture.config("printf '%0100d\\n' 0; exec sleep 60");
    config["max_line"] = json!(16);
    let output = fixture.run(&config);
    assert_eq!(lifecycle_error(&output), "stream_invalid");
    assert!(!fixture.state().unwrap().is_complete());
}

fn settled_launch_is_journaled_and_replayed() {
    let fixture = Fixture::new();
    let mut config = fixture.config("exit 0");
    config["settled"] = json!(true);
    let first = fixture.run(&config);
    assert_eq!(first.status.code(), Some(5));
    let delivered = events(&first);
    assert_eq!(delivered.len(), 2);
    assert_eq!(delivered[0]["name"], "settled");
    assert_eq!(delivered[1]["status"]["kind"], "spawn_error");
    assert_eq!(delivered[1]["seq"], 2);
    let replay = fixture.run(&config);
    assert_eq!(replay.status.code(), Some(5));
    assert_eq!(replay.stdout, first.stdout);
}
