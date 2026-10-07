//! The mediated `bash` tool over MCP with a fake requester that records what
//! reached it and answers like the agent-bash root v1 requester.
#![cfg(target_os = "linux")]

use agent_provider_contract::tool_mediation::ToolMediation;
use agent_provider_execution::tool_bridge::{self, render_output, render_run};
use serde_json::{json, Value};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::mpsc;
use std::time::Duration;

/// Records argv, cwd and the ingress variable, then answers. `hang` writes
/// its pid and sleeps; `refuse` answers an owner refusal.
const REQUESTER: &str = r#"#!/usr/bin/env python3
import base64, json, os, sys, time
log = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), 'requests.jsonl')
with open(log, 'a') as f:
    f.write(json.dumps({'argv': sys.argv[1:], 'cwd': os.getcwd(), 'ingress': os.environ.get('OULIPOLY_ROOT_BASH_V1')}) + '\n')
command = sys.argv[-1]
if command.startswith('hang'):
    open(command.split()[1], 'w').write(str(os.getpid()))
    time.sleep(300)
if command == 'refuse':
    print(json.dumps({'result_surface': 'agent-bash-root-v1', 'version': 1, 'delivery_mode': 'sync', 'outcome': 'refused', 'effects_possible': False, 'refusal': {'by': 'owner', 'reason': 'peer-unattributed'}, 'stages': [{'event': 'refused'}], 'faults': [], 'wait': None, 'output': {'base64': '', 'bytes': 0, 'delivery': 'none'}}))
    sys.exit(0)
out = ('ran %s\n' % command).encode()
print(json.dumps({'result_surface': 'agent-bash-root-v1', 'version': 1, 'delivery_mode': 'sync', 'outcome': 'ended', 'effects_possible': True, 'stages': [{'event': 'accepted', 'work': 7}, {'event': 'started'}, {'event': 'output-closed'}, {'event': 'end'}], 'faults': [], 'wait': {'status': 'code:0', 'observer': 'work-pid1-wait', 'exit': {'code': 0}}, 'output': {'base64': base64.b64encode(out).decode(), 'bytes': len(out), 'delivery': 'complete', 'retained': {'state': 'complete', 'identity': 'rv1o:r:7:%d:%s' % (len(out), '0' * 64), 'bytes': len(out)}}}))
"#;

/// Collects whole reply lines (a reply may arrive in several writes).
struct Lines(mpsc::Sender<Value>, Vec<u8>);

impl Write for Lines {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.1.extend_from_slice(bytes);
        while let Some(end) = self.1.iter().position(|b| *b == b'\n') {
            let line: Vec<u8> = self.1.drain(..=end).collect();
            let _ = self.0.send(serde_json::from_slice(&line).unwrap());
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct Bridge {
    input: Option<std::io::PipeWriter>,
    replies: mpsc::Receiver<Value>,
    served: Option<std::thread::JoinHandle<()>>,
}

impl Bridge {
    fn start(policy: ToolMediation, ingress: Option<&str>) -> Self {
        let (reader, writer) = std::io::pipe().unwrap();
        let (send, replies) = mpsc::channel();
        let ingress = ingress.map(str::to_owned);
        let served = std::thread::spawn(move || {
            tool_bridge::serve(
                policy,
                move |name| {
                    (name == "OULIPOLY_ROOT_BASH_V1")
                        .then(|| ingress.clone())
                        .flatten()
                },
                std::io::BufReader::new(reader),
                Lines(send, Vec::new()),
            )
            .unwrap();
        });
        Self {
            input: Some(writer),
            replies,
            served: Some(served),
        }
    }

    fn send(&mut self, message: Value) {
        let input = self.input.as_mut().unwrap();
        writeln!(input, "{message}").unwrap();
    }

    fn reply(&self) -> Value {
        self.replies.recv_timeout(Duration::from_secs(20)).unwrap()
    }

    fn call(&mut self, id: u64, arguments: Value) -> Value {
        self.send(json!({"jsonrpc":"2.0","id":id,"method":"tools/call",
            "params":{"name":"bash","arguments":arguments}}));
        let reply = self.reply();
        assert_eq!(reply["id"], json!(id), "{reply}");
        reply["result"].clone()
    }

    /// Ends the connection; returns every reply not yet read.
    fn close(mut self) -> Vec<Value> {
        self.input.take();
        self.served.take().unwrap().join().unwrap();
        self.replies.try_iter().collect()
    }
}

fn policy(dir: &Path, bash: Value) -> ToolMediation {
    let requester = dir.join("requester");
    std::fs::write(&requester, REQUESTER).unwrap();
    std::fs::set_permissions(&requester, std::fs::Permissions::from_mode(0o755)).unwrap();
    ToolMediation::decode(
        &json!({"protocol":"oulipoly.tool_mediation/v1","bash":bash,
            "requester":requester,"ingress_env":"OULIPOLY_ROOT_BASH_V1"})
        .to_string(),
    )
    .unwrap()
}

fn requests(dir: &Path) -> Vec<Value> {
    std::fs::read_to_string(dir.join("requests.jsonl"))
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn text(result: &Value) -> &str {
    result["content"][0]["text"].as_str().unwrap()
}

#[test]
fn allowed_commands_reach_only_the_requester_and_others_never_start_one() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    std::fs::create_dir(&work).unwrap();
    let mut bridge = Bridge::start(
        policy(dir.path(), json!({"allow":["echo hi"]})),
        Some("/ipc/bash.sock"),
    );
    bridge.send(json!({"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}));
    assert_eq!(
        bridge.reply()["result"]["protocolVersion"],
        json!("2025-06-18")
    );
    bridge.send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}));
    bridge.send(json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}));
    let tools = bridge.reply()["result"]["tools"].clone();
    assert_eq!(tools.as_array().unwrap().len(), 1);
    assert_eq!(tools[0]["name"], json!("bash"));
    assert!(tools[0]["description"]
        .as_str()
        .unwrap()
        .contains("[\"echo hi\"]"));

    let denied = bridge.call(2, json!({"command":"echo hi; rm -rf /"}));
    assert_eq!(denied["isError"], json!(true));
    assert!(text(&denied).contains("Nothing was run"), "{denied}");
    assert!(
        requests(dir.path()).is_empty(),
        "a refused command starts no requester"
    );

    let ran = bridge.call(3, json!({"command":"echo hi","workdir":work}));
    assert_eq!(ran["isError"], json!(false), "{ran}");
    assert!(
        text(&ran).contains("Root v1 work ended: exited with code 0"),
        "{ran}"
    );
    assert!(text(&ran).contains("ran echo hi"), "{ran}");
    assert!(text(&ran).contains("identity=rv1o:r:7:"), "{ran}");
    let seen = requests(dir.path());
    assert_eq!(seen.len(), 1);
    assert_eq!(
        seen[0]["argv"],
        json!(["run", "--delivery", "sync", "--", "bash", "-lc", "echo hi"])
    );
    assert_eq!(seen[0]["cwd"], json!(work));
    assert_eq!(seen[0]["ingress"], json!("/ipc/bash.sock"));

    // Retained-output reads are not commands and pass any allow list.
    let read = bridge.call(4, json!({"output_identity":"rv1w:r:7","output_length":10}));
    assert_eq!(
        read["isError"],
        json!(true),
        "the fake has no output surface"
    );
    assert_eq!(
        requests(dir.path())[1]["argv"],
        json!([
            "native-output",
            "rv1w:r:7",
            "--offset",
            "0",
            "--length",
            "10"
        ])
    );
    let invalid = bridge.call(5, json!({"command":"echo hi","shell":"zsh"}));
    assert!(
        text(&invalid).contains("unknown bash argument"),
        "{invalid}"
    );
    bridge.send(json!({"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"Bash","arguments":{}}}));
    assert_eq!(bridge.reply()["error"]["code"], json!(-32602));
    assert_eq!(requests(dir.path()).len(), 2);
    bridge.close();
}

#[test]
fn without_the_ingress_no_requester_starts_and_owner_refusals_are_shown() {
    let dir = tempfile::tempdir().unwrap();
    let trusted = json!({"authority":"trusted-task"});
    let mut bridge = Bridge::start(policy(dir.path(), trusted.clone()), None);
    let result = bridge.call(1, json!({"command":"anything"}));
    assert_eq!(result["isError"], json!(true));
    assert!(text(&result).contains("ingress unavailable"), "{result}");
    assert!(text(&result).contains("OULIPOLY_ROOT_BASH_V1"), "{result}");
    bridge.close();
    assert!(requests(dir.path()).is_empty());

    let mut bridge = Bridge::start(policy(dir.path(), trusted), Some("/ipc/bash.sock"));
    let refused = bridge.call(1, json!({"command":"refuse"}));
    assert!(
        text(&refused).contains("refused by owner (peer-unattributed)"),
        "{refused}"
    );
    assert_eq!(refused["isError"], json!(true));
    bridge.close();
}

#[test]
fn a_cancelled_call_ends_its_requester_and_gets_no_answer() {
    let dir = tempfile::tempdir().unwrap();
    let pid_file = dir.path().join("requester.pid");
    let mut bridge = Bridge::start(
        policy(dir.path(), json!({"authority":"trusted-task"})),
        Some("/ipc/bash.sock"),
    );
    bridge.send(json!({"jsonrpc":"2.0","id":9,"method":"tools/call",
        "params":{"name":"bash","arguments":{"command":format!("hang {}", pid_file.display())}}}));
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !pid_file.exists() || std::fs::read_to_string(&pid_file).unwrap().is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "requester never started"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let pid: i32 = std::fs::read_to_string(&pid_file).unwrap().parse().unwrap();
    bridge
        .send(json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":9}}));
    bridge.send(json!({"jsonrpc":"2.0","id":10,"method":"ping"}));
    assert_eq!(bridge.reply()["id"], json!(10));
    while Path::new(&format!("/proc/{pid}")).exists()
        && !std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .unwrap_or_default()
            .contains(") Z ")
    {
        assert!(
            std::time::Instant::now() < deadline,
            "requester survived cancel"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let late = bridge.close();
    assert!(late.is_empty(), "no answer for a cancelled call: {late:?}");
}

#[test]
fn unproven_results_are_never_rendered_as_success() {
    for (code, stdout) in [
        (Some(1), b"{}".as_slice()),
        (None, b"".as_slice()),
        (Some(0), b"not json".as_slice()),
        (Some(0), br#"{"result_surface":"other","version":1,"delivery_mode":"sync","stages":[]}"#.as_slice()),
        (Some(0), br#"{"result_surface":"agent-bash-root-v1","version":1,"delivery_mode":"sync","outcome":"ended","stages":[],"wait":null}"#.as_slice()),
        (Some(0), br#"{"result_surface":"agent-bash-root-v1","version":1,"delivery_mode":"async","outcome":"ended","stages":[]}"#.as_slice()),
    ] {
        let answer = render_run(code, stdout, "", "sync");
        assert!(answer.error, "{answer:?}");
        assert!(answer.text.contains("may have run; do not replay"), "{answer:?}");
    }
    let unknown = render_run(
        Some(0),
        br#"{"result_surface":"agent-bash-root-v1","version":1,"delivery_mode":"sync","outcome":"unknown","meaning":"accepted-end-unknown","stages":[],"output":{}}"#,
        "",
        "sync",
    );
    assert!(
        unknown.error && unknown.text.contains("Do not replay"),
        "{unknown:?}"
    );
    let running = render_run(
        Some(0),
        br#"{"result_surface":"agent-bash-root-v1","version":1,"delivery_mode":"async","outcome":"running","effects_possible":true,"stages":[],"wait":null,"output":{"reference":"rv1w:r:3"}}"#,
        "",
        "async",
    );
    assert!(
        !running.error && running.text.contains("rv1w:r:3"),
        "{running:?}"
    );
    assert!(render_output(Some(0), b"{}", "agent-bash-root-v1-output").error);
}
