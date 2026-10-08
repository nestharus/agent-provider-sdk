//! Real matching and mismatching Linux proc mounts, without an owner/runtime.
#![cfg(target_os = "linux")]
use agent_provider_execution::process::{
    actor_for_child, run_effect_gate, terminate_process_group_child, EffectGate, GatedCommand,
};
use std::process::{Command, Stdio};
const GATE_ARG: &str = "__proc_view_gate";
const GATE_ENV: &str = "PROC_VIEW_GATE_FD";

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some(GATE_ARG) {
        std::process::exit(run_effect_gate(&args, GATE_ENV));
    }
    if let Some(mode) = args.get(1) {
        assert!(matches!(mode.as_str(), "ordinary" | "mismatch" | "private"));
        println!("proc-view control {mode}: product assertions starting");
        let pid = std::process::id();
        let proc_self = std::fs::read_link("/proc/self").unwrap();
        let status = std::fs::read_to_string("/proc/self/status").unwrap();
        println!(
            "pid={pid}; proc_self={}; {}",
            proc_self.display(),
            status.lines().find(|s| s.starts_with("NSpid:")).unwrap()
        );
        let root = tempfile::tempdir().unwrap();
        let marker = root.path().join("native-ran");
        let exe = std::env::current_exe().unwrap();
        let gate = EffectGate {
            executable: &exe,
            argument: GATE_ARG,
            descriptor_env: GATE_ENV,
        };
        let mut command =
            GatedCommand::new(&gate, "/bin/sh", ["-c", "printf ran > \"$MARKER\""]).unwrap();
        command
            .command_mut()
            .env("MARKER", &marker)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let (mut child, release) = command.spawn().unwrap();
        let captured = actor_for_child(&child);
        if mode == "mismatch" {
            let error = captured.expect_err("host proc in child PID namespace must refuse capture");
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
            println!("capture refused for child {}: {error}", child.id());
            drop(release);
            terminate_process_group_child(&mut child).unwrap();
            assert!(
                !marker.exists(),
                "native effect escaped rejected actor capture"
            );
            println!("capture refusal and gated no-effect assertions passed");
        } else {
            let actor = captured.expect("matching proc view captures actor");
            assert_eq!(actor.process_group_id, child.id());
            println!("capture accepted: {actor:?}");
            release.release().unwrap();
            assert!(child.wait().unwrap().success());
            assert_eq!(std::fs::read_to_string(&marker).unwrap(), "ran");
            println!("actor capture, gate release and native marker assertions passed");
        }
        println!("proc-view control {mode}: product assertions passed");
        return;
    }
    let mut failed = 0;
    for (name, flags) in [
        ("ordinary", vec![]),
        ("mismatch", vec!["-Urpf", "--kill-child"]),
        ("private", vec!["-Urpf", "--mount-proc", "--kill-child"]),
    ] {
        let exe = std::env::current_exe().unwrap();
        let mut command = if flags.is_empty() {
            Command::new(&exe)
        } else {
            let mut c = Command::new("unshare");
            c.args(flags).arg(&exe);
            c
        };
        let output = match command.arg(name).output() {
            Ok(output) => output,
            Err(error) => {
                eprintln!(
                    "proc-view control {name}: subprocess setup unavailable: {error}; \
                     product assertions NOT EXECUTED"
                );
                failed += 1;
                continue;
            }
        };
        let stdout = String::from_utf8_lossy(&output.stdout);
        print!(
            "{name}: {}\n{}{}",
            output.status,
            stdout,
            String::from_utf8_lossy(&output.stderr)
        );
        let started = format!("proc-view control {name}: product assertions starting");
        let passed = format!("proc-view control {name}: product assertions passed");
        if !stdout.lines().any(|line| line == started) {
            eprintln!(
                "proc-view control {name}: subprocess/namespace setup unavailable; \
                 product assertions NOT EXECUTED"
            );
            failed += 1;
        } else if !output.status.success() || !stdout.lines().any(|line| line == passed) {
            eprintln!(
                "proc-view control {name}: product/control failure after entry; \
                 assertion completion NOT ESTABLISHED"
            );
            failed += 1;
        }
    }
    assert_eq!(
        failed, 0,
        "{failed} proc-view controls failed; see each control's output"
    );
    println!("test matching_and_mismatching_proc_views ... ok");
}
