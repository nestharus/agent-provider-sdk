//! The test binary is its own provider: it dispatches the gate argument to
//! `run_effect_gate` before running tests, as a provider binary's `main` must.
//! The gate argument and descriptor variable deliberately differ from any
//! adapter's names.
#![cfg(target_os = "linux")]

use agent_provider_execution::process::{
    actor_for_child, locate_provider_executable, process_group_is_live, run_effect_gate,
    terminate_process_group_actor, terminate_process_group_child, EffectGate, GatedCommand,
};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

const GATE_ARG: &str = "__sdk_fixture_gate";
const GATE_ENV: &str = "SDK_FIXTURE_EFFECT_GATE_FD";

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if args.get(1).map(String::as_str) == Some(GATE_ARG) {
        std::process::exit(run_effect_gate(&args, GATE_ENV));
    }
    let tests: [(&str, fn()); 5] = [
        (
            "native_effect_waits_for_release",
            native_effect_waits_for_release,
        ),
        (
            "unreleased_gate_never_executes_the_native_program",
            unreleased_gate_never_executes_the_native_program,
        ),
        (
            "native_program_leads_its_own_group_without_the_gate_descriptor",
            native_program_leads_its_own_group_without_the_gate_descriptor,
        ),
        (
            "group_termination_reaches_native_descendants",
            group_termination_reaches_native_descendants,
        ),
        (
            "provider_executable_location_is_parameterized",
            provider_executable_location_is_parameterized,
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

fn executable() -> PathBuf {
    std::env::current_exe().unwrap()
}

fn gated_shell(script: &str, marker: &Path) -> GatedCommand {
    let executable = executable();
    let gate = EffectGate {
        executable: &executable,
        argument: GATE_ARG,
        descriptor_env: GATE_ENV,
    };
    let mut command = GatedCommand::new(&gate, "sh", ["-c", script]).unwrap();
    command
        .command_mut()
        .env("MARKER", marker)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command
}

fn wait_for(path: &Path) {
    let started = Instant::now();
    while !path.exists() {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "marker never appeared"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn native_effect_waits_for_release() {
    let root = tempfile::tempdir().unwrap();
    let marker = root.path().join("ran");
    let (mut child, gate) = gated_shell("printf ran > \"$MARKER\"", &marker)
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(200));
    assert!(!marker.exists(), "native effect preceded gate release");
    gate.release().unwrap();
    assert!(child.wait().unwrap().success());
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "ran");
}

fn unreleased_gate_never_executes_the_native_program() {
    let root = tempfile::tempdir().unwrap();
    let marker = root.path().join("ran");
    let (mut child, gate) = gated_shell("printf ran > \"$MARKER\"", &marker)
        .spawn()
        .unwrap();
    drop(gate);
    assert_eq!(child.wait().unwrap().code(), Some(126));
    assert!(!marker.exists());
}

fn native_program_leads_its_own_group_without_the_gate_descriptor() {
    let root = tempfile::tempdir().unwrap();
    let marker = root.path().join("identity");
    let script = format!(
        "printf '%s %s' \"$(cut -d' ' -f5 /proc/$$/stat)\" \"${{{GATE_ENV}-unset}}\" > \"$MARKER.tmp\" && mv \"$MARKER.tmp\" \"$MARKER\""
    );
    let (mut child, gate) = gated_shell(&script, &marker).spawn().unwrap();
    let actor = actor_for_child(&child).unwrap();
    gate.release().unwrap();
    assert!(child.wait().unwrap().success());
    let observed = std::fs::read_to_string(&marker).unwrap();
    assert_eq!(observed, format!("{} unset", child.id()));
    assert_eq!(actor.process_group_id, child.id());
    assert!(actor.incarnation.starts_with("linux:"));
}

fn group_termination_reaches_native_descendants() {
    let root = tempfile::tempdir().unwrap();
    let marker = root.path().join("started");
    let (mut child, gate) = gated_shell(
        "sleep 30 </dev/null >/dev/null 2>&1 & : > \"$MARKER\"; sleep 30",
        &marker,
    )
    .spawn()
    .unwrap();
    let actor = actor_for_child(&child).unwrap();
    gate.release().unwrap();
    wait_for(&marker);
    let status = terminate_process_group_child(&mut child).expect("reaped leader");
    assert!(!status.success());
    let started = Instant::now();
    while process_group_is_live(actor.process_group_id) {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "descendant survived"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    // Recovery of an already-terminated actor is a no-op.
    terminate_process_group_actor(&actor).unwrap();
}

fn provider_executable_location_is_parameterized() {
    let executable = executable();
    let name = executable.file_name().unwrap().to_str().unwrap();
    assert_eq!(locate_provider_executable(name).unwrap(), executable);
    assert_eq!(
        locate_provider_executable("agent-provider-execution-absent")
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::NotFound
    );
}
