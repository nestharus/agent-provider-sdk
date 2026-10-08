//! The `explore` tool beside the mediated `bash` tool, over MCP, with a fake
//! child requester that records what reached it and answers like the
//! root-child v1 requester surface. Results here are synthetic owner replies;
//! they exercise the bridge's admission of the offer and its rendering.
#![cfg(target_os = "linux")]

use agent_provider_contract::exploration::Exploration;
use agent_provider_contract::tool_mediation::ToolMediation;
use agent_provider_execution::explore_tool::render_child;
use agent_provider_execution::tool_bridge;
use serde_json::{json, Value};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::mpsc;
use std::time::Duration;

/// Records argv and the ingress, then answers by the question: `hang PATH`
/// writes its pid and sleeps; `reply CODE` prints the JSON file
/// `reply.json` (if any) beside it, relays `stages.jsonl` on stderr and
/// exits CODE.
const REQUESTER: &str = r#"#!/usr/bin/env python3
import json, os, sys, time
here = os.path.dirname(os.path.abspath(sys.argv[0]))
with open(os.path.join(here, 'requests.jsonl'), 'a') as f:
    f.write(json.dumps({'argv': sys.argv[1:], 'ingress': os.environ.get('OULIPOLY_ROOT_BASH_V1')}) + '\n')
question = sys.argv[-1]
if question.startswith('hang'):
    open(question.split()[1], 'w').write(str(os.getpid()))
    time.sleep(300)
code = int(question.split()[1])
stages = os.path.join(here, 'stages.jsonl')
if os.path.exists(stages):
    for line in open(stages):
        sys.stderr.write('fake-root-child: ' + line)
reply = os.path.join(here, 'reply.json')
if os.path.exists(reply):
    print(open(reply).read().strip())
sys.exit(code)
"#;

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
    fn start(dir: &Path, offer: Option<Exploration>, ingress: Option<&str>) -> Self {
        let mediation = ToolMediation::decode(
            &json!({"protocol":"oulipoly.tool_mediation/v1","bash":{"allow":["echo hi"]},
                "requester":dir.join("no-bash-requester"),"ingress_env":"OULIPOLY_ROOT_BASH_V1"})
            .to_string(),
        )
        .unwrap();
        let (reader, writer) = std::io::pipe().unwrap();
        let (send, replies) = mpsc::channel();
        let ingress = ingress.map(str::to_owned);
        let served = std::thread::spawn(move || {
            tool_bridge::serve_offered(
                mediation,
                offer,
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
        writeln!(self.input.as_mut().unwrap(), "{message}").unwrap();
    }

    fn reply(&self) -> Value {
        self.replies.recv_timeout(Duration::from_secs(20)).unwrap()
    }

    fn call(&mut self, id: u64, name: &str, arguments: Value) -> Value {
        self.send(json!({"jsonrpc":"2.0","id":id,"method":"tools/call",
            "params":{"name":name,"arguments":arguments}}));
        let reply = self.reply();
        assert_eq!(reply["id"], json!(id), "{reply}");
        reply
    }

    fn close(mut self) -> Vec<Value> {
        self.input.take();
        self.served.take().unwrap().join().unwrap();
        self.replies.try_iter().collect()
    }
}

fn offer(dir: &Path, routes: &[&str]) -> Exploration {
    let requester = dir.join("requester");
    std::fs::write(&requester, REQUESTER).unwrap();
    std::fs::set_permissions(&requester, std::fs::Permissions::from_mode(0o755)).unwrap();
    Exploration::decode(
        &json!({"protocol":"oulipoly.exploration/v1","routes":routes,
            "requester":requester,"ingress_env":"OULIPOLY_ROOT_BASH_V1",
            "limits":{"max_starts":4}})
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

fn text(reply: &Value) -> &str {
    reply["result"]["content"][0]["text"].as_str().unwrap()
}

fn accepted() -> Value {
    json!({"event":"accepted","durable":true,"child":"child-3","route":"luna",
        "starts":{"used":3,"max":4},"concurrent":{"live":1,"max":2},
        "slot":{"index":2,"of":3}})
}

/// The owner's `result` for an answered child, as the delivered owner sends.
fn answered() -> Value {
    json!({"event":"result","child":"child-3","route":"luna","outcome":"answered",
        "answer":"src/lib.rs wires it","turn_end":{"stop_reason":"end_turn"},
        "stopped":null,"launch":null,
        "end":{"event":"end","status":"signal:9","observer":"work-pid1-wait","namespace":{"drained":true}},
        "lifecycle":{"end":"observed","bash_runs_open":0,"bash_run_end_unknown":false,
            "budget":"still charged (release pending)"},
        "a_later_field":{"additive":true}})
}

#[test]
fn without_an_offer_the_bridge_is_bash_alone() {
    let dir = tempfile::tempdir().unwrap();
    let mut bridge = Bridge::start(dir.path(), None, Some("/ipc/bash.sock"));
    bridge.send(json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}));
    let tools = bridge.reply()["result"]["tools"].clone();
    assert_eq!(tools.as_array().unwrap().len(), 1, "{tools}");
    assert_eq!(tools[0]["name"], "bash");
    let reply = bridge.call(2, "explore", json!({"question":"where?"}));
    assert_eq!(reply["error"]["message"], "Unknown tool", "{reply}");
    assert!(bridge.close().is_empty());
}

#[test]
fn an_offer_adds_a_non_command_tool_and_asks_only_offered_routes() {
    let dir = tempfile::tempdir().unwrap();
    let mut bridge = Bridge::start(
        dir.path(),
        Some(offer(dir.path(), &["luna", "other"])),
        Some("/ipc/bash.sock"),
    );
    bridge.send(json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}));
    let tools = bridge.reply()["result"]["tools"].clone();
    let names: Vec<&str> = tools
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["bash", "explore"]);
    assert_eq!(
        tools[1]["inputSchema"]["properties"]["route"]["enum"],
        json!(["luna", "other"])
    );
    let description = tools[1]["description"].as_str().unwrap();
    assert!(
        description.contains("runs no command itself"),
        "{description}"
    );
    assert!(
        description.contains("not a technical barrier"),
        "{description}"
    );
    // Nothing asked: unoffered, missing or ambiguous route, bad arguments.
    for (id, arguments) in [
        (2, json!({"question":"q","route":"opus"})),
        (3, json!({"question":"q"})),
        (4, json!({"question":"  ","route":"luna"})),
        (5, json!({"question":"q","route":"luna","model":"x"})),
        (6, json!({"route":"luna"})),
    ] {
        let reply = bridge.call(id, "explore", arguments);
        assert_eq!(reply["result"]["isError"], json!(true), "{reply}");
        assert!(text(&reply).contains("Nothing was asked"), "{reply}");
    }
    assert!(requests(dir.path()).is_empty());
    // An offered route: exactly `requester ROUTE QUESTION` with the ingress.
    std::fs::write(dir.path().join("reply.json"), answered().to_string()).unwrap();
    std::fs::write(dir.path().join("stages.jsonl"), format!("{}\n", accepted())).unwrap();
    let reply = bridge.call(7, "explore", json!({"question":"reply 0","route":"luna"}));
    assert_eq!(reply["result"]["isError"], json!(false), "{reply}");
    assert_eq!(
        requests(dir.path()),
        [json!({"argv":["luna","reply 0"],"ingress":"/ipc/bash.sock"})]
    );
    let shown = text(&reply);
    for part in [
        "Explorer child-3 (route luna): answered.",
        "Turn end (final result): end_turn.",
        "src/lib.rs wires it",
        "not verified",
        "End: signal:9, observed by work-pid1-wait; namespace drained.",
        "end observed; its Bash runs still open at this result: 0",
        "does not prove the child or its Bash ended",
        "accepted(child-3, starts 3/4, live 1/2, slot 2 of 3)",
    ] {
        assert!(shown.contains(part), "{part}: {shown}");
    }
    assert!(bridge.close().is_empty());
}

#[test]
fn without_the_ingress_no_requester_starts() {
    let dir = tempfile::tempdir().unwrap();
    let mut bridge = Bridge::start(dir.path(), Some(offer(dir.path(), &["luna"])), None);
    let reply = bridge.call(1, "explore", json!({"question":"reply 0"}));
    assert!(text(&reply).contains("No requester was started"), "{reply}");
    assert!(requests(dir.path()).is_empty());
    bridge.close();
}

#[test]
fn cancellation_and_connection_end_stop_the_requester_without_an_answer() {
    for eof in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let pidfile = dir.path().join("pid");
        let mut bridge = Bridge::start(
            dir.path(),
            Some(offer(dir.path(), &["luna"])),
            Some("/ipc/bash.sock"),
        );
        bridge.send(json!({"jsonrpc":"2.0","id":9,"method":"tools/call",
            "params":{"name":"explore","arguments":{"question":format!("hang {}", pidfile.display())}}}));
        let started = std::time::Instant::now();
        let pid = loop {
            if let Ok(pid) = std::fs::read_to_string(&pidfile) {
                if !pid.is_empty() {
                    break pid.parse::<i32>().unwrap();
                }
            }
            assert!(started.elapsed() < Duration::from_secs(20));
            std::thread::sleep(Duration::from_millis(10));
        };
        if eof {
            assert!(bridge.close().is_empty());
        } else {
            bridge.send(json!({"jsonrpc":"2.0","method":"notifications/cancelled",
                "params":{"requestId":9}}));
            // The bridge still serves; the cancelled call gets no answer.
            let reply = bridge.call(10, "explore", json!({"question":"q","route":"opus"}));
            assert!(text(&reply).contains("not offered"), "{reply}");
            assert!(bridge.close().is_empty());
        }
        // The requester was killed and collected (reaped): its pid no longer
        // names it.
        let cmdline = std::fs::read(format!("/proc/{pid}/cmdline")).unwrap_or_default();
        assert!(
            !String::from_utf8_lossy(&cmdline).contains("requester"),
            "requester {pid} still present"
        );
    }
}

fn render(
    code: Option<i32>,
    stdout: &Value,
    stages: &[Value],
) -> agent_provider_execution::tool_bridge::Answer {
    let stdout = if stdout.is_null() {
        String::new()
    } else {
        stdout.to_string()
    };
    let stderr: String = stages
        .iter()
        .map(|stage| format!("requester: {stage}\n"))
        .collect();
    render_child("luna", code, stdout.as_bytes(), &stderr)
}

#[test]
fn refusals_say_nothing_was_admitted_and_keep_their_kinds_apart() {
    for (reason, meaning) in [
        ("route-slots-used: 2 of 2", "a failed launch uses one too"),
        ("budget-starts: 4 of 4", "total child starts are used"),
        (
            "budget-concurrent: 2 of 2 (1 unknown-end charged)",
            "end is unknown stays counted",
        ),
        ("route-not-allowed", "does not allow that route"),
        (
            "depth: a child cannot have children",
            "a child cannot ask for children",
        ),
        (
            "peer-unattributed: outside-every-harness-namespace",
            "not come from an agent of this root",
        ),
        ("some-future-reason", ""),
    ] {
        let answer = render(Some(65), &json!({"event":"refused","reason":reason}), &[]);
        assert!(answer.error);
        assert!(answer.text.contains(reason), "{}", answer.text);
        assert!(answer.text.contains(meaning), "{}", answer.text);
        assert!(
            answer.text.contains("No child was admitted or started"),
            "{}",
            answer.text
        );
    }
}

#[test]
fn launch_end_and_stop_facts_are_rendered_as_the_owner_reported_them() {
    // Work created, exec failed: not a no-start, despite no turn end.
    let mut failed = answered();
    failed["outcome"] = json!("ended-without-turn-end");
    failed["answer"] = Value::Null;
    failed["turn_end"] = Value::Null;
    failed["launch"] = json!({"event":"launch-failed","reason":"No such file or directory (os error 2)","not_started":false});
    failed["end"]["status"] = json!("code:127");
    let answer = render(Some(0), &failed, &[]);
    assert!(answer.error);
    for part in [
        "ended-without-turn-end",
        "a child work process was created, but its launch failed",
        "not a positive no-start: setup effects are possible",
        "End: code:127",
        "No answer was received",
    ] {
        assert!(answer.text.contains(part), "{part}: {}", answer.text);
    }
    // Positive no-start before any process.
    let mut setup = failed.clone();
    setup["outcome"] = json!("launch-failed");
    setup["launch"] = json!({"event":"launch-failed","reason":"setup-failed: x","not_started":true,"setup_effects":"possible"});
    setup["end"] = Value::Null;
    setup["lifecycle"]["end"] = json!("no-process");
    let answer = render(Some(0), &setup, &[]);
    assert!(
        answer
            .text
            .contains("no child process was started (setup-failed: x); setup effects: possible"),
        "{}",
        answer.text
    );
    assert!(
        answer.text.contains("End: none reported"),
        "{}",
        answer.text
    );
    // Unknown launch and end stay unknown and charged.
    let mut unknown = failed.clone();
    unknown["outcome"] = json!("launch-unknown");
    unknown["launch"] =
        json!({"event":"launch-unknown","reason":"after-clone","not_started":false});
    unknown["end"] = json!({"event":"end-unknown","reason":"waiter-lost"});
    unknown["lifecycle"]["end"] = json!("unknown");
    unknown["lifecycle"]["bash_run_end_unknown"] = json!(true);
    unknown["lifecycle"]["budget"] = json!("still charged (an end is unknown: it may be running)");
    let answer = render(Some(0), &unknown, &[]);
    for part in [
        "Launch outcome unknown (after-clone): the child may have started",
        "End unknown (waiter-lost): the child may still be running",
        "end unknown (it may still be running)",
        "(one ended unknown)",
        "it may be running",
    ] {
        assert!(answer.text.contains(part), "{part}: {}", answer.text);
    }
    // Stopped, with the reason; lifecycle missing is said to be unknown.
    let mut stopped = failed.clone();
    stopped["outcome"] = json!("stopped");
    stopped["launch"] = Value::Null;
    stopped["stopped"] = json!("requester-gone");
    stopped["end"]["status"] = json!("signal:9");
    stopped.as_object_mut().unwrap().remove("lifecycle");
    let answer = render(Some(0), &stopped, &[]);
    assert!(
        answer
            .text
            .contains("stopped; stopped by the root (requester-gone)"),
        "{}",
        answer.text
    );
    assert!(
        answer.text.contains("not reported by the root"),
        "{}",
        answer.text
    );
}

#[test]
fn missing_process_creation_fact_stays_unknown_after_admission() {
    // Source-shaped Inbox failure: no launch boolean and no worker/end.
    let mut failed = answered();
    failed["outcome"] = json!("launch-failed");
    failed["answer"] = Value::Null;
    failed["turn_end"] = Value::Null;
    failed["launch"] =
        json!({"event":"launch-failed","reason":"inbox: Too many open files (os error 24)"});
    failed["end"] = Value::Null;
    failed["lifecycle"]["end"] = json!("no-process");
    for null_fact in [false, true] {
        if null_fact {
            failed["launch"]["not_started"] = Value::Null;
        }
        let answer = render(Some(0), &failed, &[accepted()]);
        assert!(answer.error);
        for part in [
            "inbox: Too many open files",
            "process creation was not reported",
            "launch status is unknown",
            "Setup effects are possible",
            "No answer was received",
            "End: none reported",
            "end no process",
            "budget: still charged (release pending)",
            "This admission used one",
            "nothing is retried",
        ] {
            assert!(answer.text.contains(part), "{part}: {}", answer.text);
        }
        assert!(!answer.text.contains("a child work process was created"));
        assert!(!answer.text.contains("no child process was started"));
    }
}

#[test]
fn final_turn_end_reason_survives_without_its_relayed_stage() {
    let mut partial = answered();
    partial["outcome"] = json!("no-answer");
    partial["turn_end"]["stop_reason"] = json!("max_tokens");
    for with_text in [true, false] {
        if !with_text {
            partial["answer"] = Value::Null;
        }
        // Empty stderr and an accepted-only prefix both lack turn-end.
        for stages in [vec![], vec![accepted()]] {
            let answer = render(Some(0), &partial, &stages);
            assert!(answer.error);
            for part in [
                "no-answer",
                "Turn end (final result): max_tokens.",
                "End: signal:9",
                "budget: still charged (release pending)",
                "nothing is retried",
            ] {
                assert!(answer.text.contains(part), "{part}: {}", answer.text);
            }
            if with_text {
                assert!(answer.text.contains("not verified"));
                assert!(answer.text.contains("src/lib.rs wires it"));
            } else {
                assert!(answer.text.contains("No answer was received"));
            }
        }
    }
}

#[test]
fn missing_mismatched_or_unknown_results_are_unresolved_and_never_replayed() {
    let unresolved = |answer: &agent_provider_execution::tool_bridge::Answer| {
        assert!(answer.error, "{}", answer.text);
        assert!(
            answer.text.starts_with("Explorer outcome unresolved"),
            "{}",
            answer.text
        );
        assert!(
            answer.text.contains("Do not ask again automatically"),
            "{}",
            answer.text
        );
    };
    // Lost after a relayed admission: admission confirmed, result unknown.
    let answer = render(
        Some(75),
        &json!({"event":"lost","meaning":"no result received; the child may have run"}),
        &[accepted(), json!({"event":"started","work":4})],
    );
    unresolved(&answer);
    assert!(
        answer.text.contains("durably admitted child-3"),
        "{}",
        answer.text
    );
    assert!(answer.text.contains("started(work=4)"), "{}", answer.text);
    // Lost with no relayed admission: may have been admitted.
    let answer = render(Some(75), &json!({"event":"lost"}), &[]);
    unresolved(&answer);
    assert!(
        answer.text.contains("may have been admitted and run"),
        "{}",
        answer.text
    );
    // Another route, an unknown outcome, contradictory facts, a refusal
    // under exit 0, a result under 75, a signal or a malformed object.
    let mut other_route = answered();
    other_route["route"] = json!("other");
    let mut future = answered();
    future["outcome"] = json!("answered-partially");
    let mut no_text = answered();
    no_text["answer"] = Value::Null;
    let mut cut_short = answered();
    cut_short["turn_end"]["stop_reason"] = json!("max_tokens");
    let mut bad_end = answered();
    bad_end["end"] = json!({"event":"ended"});
    let mut bad_life = answered();
    bad_life["lifecycle"]["end"] = json!("maybe");
    for (code, stdout) in [
        (Some(0), other_route),
        (Some(0), future),
        (Some(0), no_text),
        (Some(0), cut_short),
        (Some(0), bad_end),
        (Some(0), bad_life),
        (Some(0), json!({"event":"refused","reason":"depth"})),
        (Some(75), answered()),
        (Some(65), json!({"event":"refused"})),
        (None, answered()),
        (Some(1), Value::Null),
        (Some(0), Value::Null),
        (Some(69), json!({"event":"lost"})),
    ] {
        unresolved(&render(code, &stdout, &[]));
    }
    let answer = render_child("luna", Some(0), b"{not json", "");
    unresolved(&answer);
    // A relayed admission makes 69 unresolved too: something was sent.
    unresolved(&render(Some(69), &Value::Null, &[accepted()]));
}

#[test]
fn only_a_positive_requester_no_send_says_nothing_was_asked() {
    for code in [69, 64] {
        let answer = render_child(
            "luna",
            Some(code),
            b"",
            "requester: owner unreachable: ENOENT\n",
        );
        assert!(answer.error);
        assert!(
            answer.text.contains("Explorer request not sent"),
            "{}",
            answer.text
        );
        assert!(
            answer.text.contains("owner unreachable: ENOENT"),
            "{}",
            answer.text
        );
    }
}

#[test]
fn long_answers_are_cut_on_a_character_boundary_and_say_so() {
    let mut long = answered();
    long["answer"] = json!("é".repeat(200 * 1024));
    let answer = render(Some(0), &long, &[]);
    assert!(!answer.error);
    assert!(
        answer
            .text
            .contains("[answer truncated by this tool: 262144 of 409600 bytes shown]"),
        "{}",
        &answer.text[answer.text.len() - 600..]
    );
}

/// A cancelled child after a held rejection: no turn end, the owner's stop.
fn ended_after_rejection() -> Value {
    json!({"event":"result","child":"child-3","route":"luna","outcome":"stopped",
        "answer":null,"turn_end":null,"stopped":"cancelled","launch":null,
        "end":{"event":"end","status":"signal:15","observer":"work-pid1-wait","namespace":{"drained":true}},
        "lifecycle":null})
}

/// The `rejected` stage as the delivered owner projects it to the requester:
/// no input index, attempt scope or arbitrary endpoint `message`.
fn rejected() -> Value {
    json!({"event":"rejected","detail":"insertion unresolved; cancel or peer exit",
        "code":-32010,"insertion":"unresolved","retry":"not-authorized",
        "endpoint_declaration":"not-inserted",
        "declaration_attribution":"endpoint-rpc-code",
        "physical_non_insertion":"not-established",
        "hold":"unresolved-input","exit":"cancel-or-peer-exit",
        "native_report":{"state":"absent"},"endpoint_record_error":false,
        "endpoint_durability":"not-established",
        "canonical_publication":"not-established"})
}

#[test]
fn a_relayed_rejection_keeps_its_scoped_facts_whether_a_result_or_nothing_follows() {
    let stages = [accepted(), rejected()];
    let after_cancel = render(Some(0), &ended_after_rejection(), &stages);
    let lost = render(
        Some(75),
        &json!({"event":"lost","meaning":"no result received; the child may have run"}),
        &stages,
    );
    for answer in [&after_cancel, &lost] {
        assert!(answer.error);
        for part in [
            "rejected(code -32010 (INPUT_NOT_INSERTED))",
            "input index and attempt scope not reported",
            "insertion: unresolved (whether the input reached the session is not established)",
            "endpoint declaration: not-inserted (the endpoint's own claim), attributed to endpoint-rpc-code (its error code); physical non-insertion: not-established",
            "retry: not-authorized; hold: unresolved-input (further input and close stay held); exit: cancel-or-peer-exit",
            "endpoint native-turn report: absent (no report came with the rejection)",
            "endpoint record error: none reported with this rejection",
            "endpoint durability: not-established; canonical publication: not-established",
            "grants no retry or release",
            "do not resend it automatically",
        ] {
            assert!(answer.text.contains(part), "{part}: {}", answer.text);
        }
    }
    // The stage is not a result: each answer keeps its own ending facts.
    assert!(after_cancel
        .text
        .contains("stopped by the root (cancelled)"));
    assert!(after_cancel.text.contains("End: signal:15"));
    assert!(lost.text.starts_with("Explorer outcome unresolved"));
    assert!(lost.text.contains("durably admitted child-3"));
}

#[test]
fn a_rejection_is_neither_stronger_nor_weaker_than_the_owner_reported() {
    let render_one = |stage: Value| render(Some(0), &ended_after_rejection(), &[stage]).text;
    // The endpoint declared nothing: the not-inserted claim is not implied.
    let mut uncertain = rejected();
    uncertain["code"] = json!(-32011);
    uncertain["endpoint_declaration"] = json!("no-non-insertion-declaration");
    let text = render_one(uncertain);
    assert!(text.contains("-32011 (INPUT_UNCERTAIN)"), "{text}");
    assert!(
        text.contains("the endpoint did not declare non-insertion"),
        "{text}"
    );
    assert!(
        !text.contains("endpoint declaration: not-inserted"),
        "{text}"
    );
    // Missing fields stay unknown, not defaulted to a hold, retry or scope.
    let text = render_one(json!({"event":"rejected"}));
    for part in [
        "endpoint code: not reported",
        "insertion: not reported",
        "endpoint declaration: not reported, attributed to not reported; physical non-insertion: not reported",
        "retry: not reported; hold: not reported; exit: not reported",
        "endpoint native-turn report: not reported",
        "endpoint record error: not reported",
        "endpoint durability: not reported; canonical publication: not reported",
        "grants no retry or release",
    ] {
        assert!(text.contains(part), "{part}: {text}");
    }
    assert!(!text.contains("not-inserted"), "{text}");
    assert!(!text.contains("not-established"), "{text}");
    // Index, attempt scope, unresolved count, the endpoint's native report
    // and its record-error flag appear in their own scopes when carried.
    let mut scoped = rejected();
    scoped["index"] = json!(2);
    scoped["scope"] = json!("rpc-attempt");
    scoped["unresolved_attempts"] = json!(3);
    scoped["endpoint_record_error"] = json!(true);
    scoped["native_report"] = json!({"state":"valid","custody":"not-admitted","status_code":3,
        "source":"endpoint-report","physical_custody":"not-certified-by-report"});
    let text = render_one(scoped);
    for part in [
        "input 2, this RPC attempt",
        "Unresolved attempts of this input: 3.",
        "endpoint native-turn report: valid, custody not-admitted, status code 3; an endpoint report, physical custody not-certified-by-report (the report does not certify it)",
        "endpoint record error: reported with this rejection (details withheld)",
        "physical non-insertion: not-established",
    ] {
        assert!(text.contains(part), "{part}: {text}");
    }
}

#[test]
fn rejection_free_text_and_unrecognized_values_are_not_echoed() {
    let mut stage = rejected();
    stage["detail"] = json!("PRIVATE-PAYLOAD token=abc");
    stage["message"] = json!("PRIVATE-PAYLOAD");
    stage["insertion"] = json!("inserted-PRIVATE-PAYLOAD");
    stage["retry"] = json!("safe-to-retry");
    stage["hold"] = json!({"PRIVATE-PAYLOAD": true});
    stage["native_report"] = json!({"state":"valid-PRIVATE-PAYLOAD","custody":"PRIVATE-PAYLOAD",
        "physical_custody":"PRIVATE-PAYLOAD"});
    let text = render(Some(0), &ended_after_rejection(), &[stage]).text;
    assert!(!text.contains("PRIVATE-PAYLOAD"), "{text}");
    assert!(!text.contains("safe-to-retry"), "{text}");
    assert!(
        text.contains("insertion: unrecognized value (not shown)"),
        "{text}"
    );
    assert!(
        text.contains("retry: unrecognized value (not shown)"),
        "{text}"
    );
    assert!(text.contains("grants no retry or release"), "{text}");
    // Start-failure labels outside plain label characters are dropped too.
    let text = render(
        Some(0),
        &ended_after_rejection(),
        &[json!({"event":"session-failed","detail":"PRIVATE-PAYLOAD with spaces"})],
    )
    .text;
    assert!(!text.contains("PRIVATE-PAYLOAD"), "{text}");
    assert!(text.contains("no label carried"), "{text}");
}

#[test]
fn start_death_and_endpoint_refusal_stay_different_facts() {
    let death = render(
        Some(75),
        &json!({"event":"lost"}),
        &[
            accepted(),
            json!({"event":"session-peer-gone","detail":null}),
        ],
    )
    .text;
    assert!(
        death.contains("transport death, not an endpoint refusal"),
        "{death}"
    );
    assert!(death.contains("session-peer-gone"), "{death}");
    assert!(!death.contains("error code"), "{death}");
    let refused = render(
        Some(0),
        &ended_after_rejection(),
        &[json!({"event":"session-failed","detail":"session-rejected--32012"})],
    )
    .text;
    assert!(
        refused.contains("session-failed(session-rejected--32012)"),
        "{refused}"
    );
    assert!(
        refused.contains(
            "error code -32012 (SESSION_UNAVAILABLE); this is its refusal, not transport death"
        ),
        "{refused}"
    );
    assert!(!refused.contains("connection ended"), "{refused}");
    // Other labels are shown as labels, without an invented meaning.
    let other = render(
        Some(0),
        &ended_after_rejection(),
        &[
            json!({"event":"session-failed","detail":"session-protocol-violation"}),
            json!({"event":"resident-start-refused","detail":"resident-start-refused"}),
        ],
    )
    .text;
    assert!(
        other.contains("label session-protocol-violation; no further meaning is defined here"),
        "{other}"
    );
    assert!(
        other.contains("refused before a session was established; the stage carries no cause"),
        "{other}"
    );
    for text in [&death, &refused, &other] {
        assert!(text.contains("it grants no retry"), "{text}");
    }
}

#[test]
fn an_ack_stage_is_consumption_only_and_never_an_end() {
    let text = render(
        Some(75),
        &json!({"event":"lost"}),
        &[accepted(), json!({"event":"ack","message_id":"PRIVATE-ID","meaning":"consumption, not processing"})],
    )
    .text;
    assert!(
        text.contains("ack(consumption, not processing or end)"),
        "{text}"
    );
    assert!(!text.contains("end("), "{text}");
    assert!(!text.contains("PRIVATE-ID"), "{text}");
}

#[test]
fn many_relayed_stages_are_counted_and_the_last_ones_stay_visible() {
    let mut stages = vec![accepted()];
    stages.extend((0..30).map(|_| rejected()));
    stages.extend((0..100).map(|_| json!({"event":"ack"})));
    stages.push(json!({"event":"end","status":"signal:15","namespace":{"drained":true}}));
    let text = render(Some(0), &ended_after_rejection(), &stages).text;
    assert_eq!(
        text.matches("Input rejection relayed by the requester")
            .count(),
        8,
        "{text}"
    );
    assert!(
        text.contains(
            "22 further rejection or start-failure stages were relayed and are not shown."
        ),
        "{text}"
    );
    assert!(text.contains("stages not shown]"), "{text}");
    assert!(
        text.contains("-> end(signal:15, namespace drained)"),
        "{text}"
    );
    assert!(text.starts_with("Explorer child-3"), "{text}");
    assert!(text.len() < 32 * 1024, "{}", text.len());
}
