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
        } else {
            let actor = captured.expect("matching proc view captures actor");
            assert_eq!(actor.process_group_id, child.id());
            println!("capture accepted: {actor:?}");
            release.release().unwrap();
            assert!(child.wait().unwrap().success());
            assert_eq!(std::fs::read_to_string(&marker).unwrap(), "ran");
        }
        return;
    }
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
        let output = command.arg(name).output().unwrap();
        print!(
            "{name}: {}\n{}{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.status.success(), "proc view {name} control failed");
    }
    println!("test matching_and_mismatching_proc_views ... ok");
}
