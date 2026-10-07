//! The logical `explore` tool of `oulipoly.exploration/v1`, served by
//! [`crate::tool_bridge`] beside the mediated `bash` tool when the launch
//! carries an [`Exploration`] offer.
//!
//! It runs no command. `{"question": Q, "route"?: R}` names one offered route
//! (or the only one) and runs only `requester ROUTE Q` (the root-child v1
//! requester surface) with the owner ingress the offer names, so the host's
//! owner, not this tool, admits or refuses the child. An unoffered or
//! missing route and a missing ingress start no requester.
//!
//! The answer renders only what the requester's one JSON object and exit
//! established: the owner's refusal (nothing admitted), the owner's final
//! `result` with its outcome, answer, launch, stop and end facts and the
//! lifecycle qualifiers, a positive "not sent", or an unresolved outcome
//! after which a child may have been admitted and run and must not be asked
//! again automatically. `answered` is the child's answer text, not its
//! correctness, its end or the parent's consumption. Owner stage lines the
//! requester relays on stderr are shown as the lifecycle and only confirm an
//! admission when no result arrived; they never replace the final result.
//! Unknown additional fields are ignored; an unknown outcome is unresolved.

use agent_provider_contract::exploration::{Exploration, Limits};
use serde_json::{json, Value};

/// The tool's MCP name.
pub const TOOL: &str = "explore";
/// Answer bytes shown inline.
const ANSWER_LIMIT: usize = 256 * 1024;
/// Owner outcomes root-child v1 defines.
const OUTCOMES: &[&str] = &[
    "answered",
    "no-answer",
    "launch-failed",
    "launch-unknown",
    "stopped",
    "ended-without-turn-end",
];

use crate::tool_bridge::Answer;

fn answer(text: impl Into<String>, error: bool) -> Answer {
    Answer {
        text: text.into(),
        error,
    }
}

fn limits(limits: &Option<Limits>) -> String {
    let Some(limits) = limits else {
        return String::new();
    };
    let mut parts = Vec::new();
    if let Some(starts) = limits.max_starts {
        parts.push(format!("{starts} child starts in total"));
    }
    if let Some(live) = limits.max_concurrent {
        parts.push(format!("{live} at once"));
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!(
            " Offered limits for this root: {} (the root may refuse sooner).",
            parts.join(", ")
        )
    }
}

/// The tool's MCP definition for `offer`.
pub fn tool_definition(offer: &Exploration) -> Value {
    let default = match offer.routes.as_slice() {
        [only] => format!(" (default {only})"),
        _ => String::new(),
    };
    json!({
        "name": TOOL,
        "description": format!(
            "Ask a registered child explorer, a separate session inside this same root, one orientation question: where things are and how they are wired together. It runs no command itself; this root admits or refuses each child and returns its result here, blocking until then. Routes: {}{default}.{} A refusal or failure is returned visibly and nothing is retried; each call is a new request. The answer is the child's last reply, orientation rather than validation, and may be wrong: verify what matters yourself. Read-only is the child's instruction, not a technical barrier. A child cannot ask for children.",
            offer.routes.join(", "),
            limits(&offer.limits)
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "question": {"type": "string", "minLength": 1, "description": "The whole question for the child."},
                "route": {"type": "string", "enum": offer.routes, "description": "An offered route label."}
            },
            "required": ["question"],
            "additionalProperties": false
        },
        "annotations": {"readOnlyHint": false, "idempotentHint": false, "openWorldHint": true}
    })
}

/// What one call asks: `(route, question)`, or why nothing may be asked.
pub(crate) fn parse_call(
    offer: &Exploration,
    arguments: &Value,
) -> Result<(String, String), String> {
    let empty = serde_json::Map::new();
    let args = match arguments {
        Value::Null => &empty,
        Value::Object(map) => map,
        _ => return Err("explore arguments must be an object".into()),
    };
    if let Some(unknown) = args
        .keys()
        .find(|key| !["question", "route"].contains(&key.as_str()))
    {
        return Err(format!("unknown explore argument {unknown:?}"));
    }
    let question = match args.get("question") {
        Some(Value::String(question)) if !question.trim().is_empty() => question.clone(),
        Some(Value::String(_)) | None | Some(Value::Null) => {
            return Err("question is required".into())
        }
        Some(_) => return Err("question must be a string".into()),
    };
    if question.contains('\0') {
        return Err("question contains a NUL byte".into());
    }
    let route = match args.get("route") {
        None | Some(Value::Null) => None,
        Some(Value::String(route)) => Some(route.as_str()),
        Some(_) => return Err("route must be a string".into()),
    };
    Ok((offer.route(route)?, question))
}

/// Owner stage lines relayed on stderr as `LABEL: JSON`; others ignored.
fn relayed(stderr: &str) -> Vec<Value> {
    stderr
        .lines()
        .filter_map(|line| line.split_once(": "))
        .filter_map(|(_, json)| serde_json::from_str::<Value>(json).ok())
        .filter(|value| value["event"].is_string())
        .collect()
}

fn stage_text(stage: &Value) -> Option<String> {
    let text = |key: &str| {
        stage[key]
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| stage[key].to_string())
    };
    Some(match stage["event"].as_str()? {
        "accepted" => format!(
            "accepted({}, starts {}/{}, live {}/{}{})",
            text("child"),
            stage["starts"]["used"],
            stage["starts"]["max"],
            stage["concurrent"]["live"],
            stage["concurrent"]["max"],
            match stage["slot"]["index"].as_u64() {
                Some(index) => format!(", slot {index} of {}", stage["slot"]["of"]),
                None => String::new(),
            }
        ),
        "started" => format!("started(work={})", stage["work"]),
        "launch-failed" | "launch-unknown" => format!(
            "{}({}, not_started={})",
            text("event"),
            text("reason"),
            stage["not_started"]
        ),
        "turn-end" => format!("turn-end({})", text("stop_reason")),
        "stopping" => format!("stopping({})", text("reason")),
        "end" => format!(
            "end({}, namespace {})",
            text("status"),
            if stage["namespace"]["drained"] == json!(true) {
                "drained"
            } else {
                "drain not reported"
            }
        ),
        "end-unknown" => format!("end-unknown({})", text("reason")),
        "agent-message" | "notice" | "ack" | "result" | "refused" => return None,
        other => other.to_owned(),
    })
}

fn lifecycle(stages: &[Value]) -> String {
    let shown: Vec<String> = stages.iter().filter_map(stage_text).collect();
    if shown.is_empty() {
        String::new()
    } else {
        format!(
            "\nLifecycle relayed by the requester: {}",
            shown.join(" -> ")
        )
    }
}

fn stderr_note(stderr: &str) -> String {
    let plain: Vec<&str> = stderr
        .lines()
        .filter(|line| {
            line.split_once(": ")
                .is_none_or(|(_, json)| serde_json::from_str::<Value>(json).is_err())
        })
        .collect();
    if plain.is_empty() {
        String::new()
    } else {
        format!(
            "\nrequester stderr: {}",
            crate::encoding::bounded_text_bytes(&plain.join("\n"), 4096)
        )
    }
}

fn unresolved(reason: &str, stages: &[Value], stderr: &str) -> Answer {
    let admitted = stages
        .iter()
        .find(|stage| stage["event"] == "accepted")
        .map(|stage| {
            format!(
                "The root durably admitted {} for this request, but its result was not established: it may have run, and the root stops a child whose requester goes away.",
                stage["child"].as_str().unwrap_or("a child")
            )
        })
        .unwrap_or_else(|| {
            "The request may have reached the root: a child may have been admitted and run.".into()
        });
    answer(
        format!(
            "Explorer outcome unresolved ({reason}). {admitted} Do not ask again automatically; a new call is a new admission.{}{}",
            lifecycle(stages),
            stderr_note(stderr)
        ),
        true,
    )
}

fn refusal_meaning(reason: &str) -> &'static str {
    let prefix = reason.split(':').next().unwrap_or(reason);
    match prefix {
        "route-not-allowed" => "this root does not allow that route for this agent",
        "route-slots-used" => "every prepared slot of this route is used (a failed launch uses one too); another route may still have room",
        "budget-starts" => "this root's total child starts are used",
        "budget-concurrent" => "this root's limit of children at once is reached (a child whose end is unknown stays counted)",
        "depth" => "a child cannot ask for children",
        "children-not-enabled" => "this root has no child routes",
        "parent-not-current" => "the asking agent's work was no longer current at admission",
        "closing" | "owner-stopping" | "ingress-closed" => "this root is closing or stopping",
        "peer-unattributed" => "the request did not come from an agent of this root",
        _ => "",
    }
}

fn refused(value: &Value, stages: &[Value]) -> Answer {
    let reason = value["reason"].as_str().unwrap_or("unknown");
    let meaning = refusal_meaning(reason);
    answer(
        format!(
            "Explorer request refused by this root ({reason}){}. No child was admitted or started and no start or slot was used. Not retried.{}",
            if meaning.is_empty() {
                String::new()
            } else {
                format!(": {meaning}")
            },
            lifecycle(stages)
        ),
        true,
    )
}

fn launch_text(launch: &Value) -> Result<String, &'static str> {
    if launch.is_null() {
        return Ok(String::new());
    }
    let reason = launch["reason"].as_str().unwrap_or("unknown");
    match (launch["event"].as_str(), launch["not_started"].as_bool()) {
        (Some("launch-failed"), Some(true)) => Ok(format!(
            "\nLaunch: no child process was started ({reason}); setup effects: {}.",
            launch["setup_effects"].as_str().unwrap_or("not reported")
        )),
        (Some("launch-failed"), Some(false)) => Ok(format!(
            "\nLaunch: a child work process was created, but its launch failed ({reason}). This is not a positive no-start: setup effects are possible."
        )),
        (Some("launch-failed"), None) => Ok(format!(
            "\nLaunch failed ({reason}); process creation was not reported, so launch status is unknown to this tool. Setup effects are possible."
        )),
        (Some("launch-unknown"), _) => Ok(format!(
            "\nLaunch outcome unknown ({reason}): the child may have started; it stays counted against this root's children at once."
        )),
        _ => Err("launch facts inconsistent"),
    }
}

fn end_text(end: &Value) -> Result<String, &'static str> {
    match end["event"].as_str() {
        None if end.is_null() => Ok("\nEnd: none reported.".into()),
        Some("end") => {
            let status = end["status"].as_str().ok_or("end facts inconsistent")?;
            Ok(format!(
                "\nEnd: {status}, observed by {}; namespace {}.",
                end["observer"].as_str().unwrap_or("unknown"),
                if end["namespace"]["drained"] == json!(true) {
                    "drained"
                } else {
                    "drain not reported"
                }
            ))
        }
        Some("end-unknown") => Ok(format!(
            "\nEnd unknown ({}): the child may still be running.",
            end["reason"].as_str().unwrap_or("unknown")
        )),
        _ => Err("end facts inconsistent"),
    }
}

fn status_text(life: &Value) -> Result<String, &'static str> {
    if life.is_null() {
        return Ok(
            "\nLifecycle status: not reported by the root (end and drain unknown to this tool)."
                .into(),
        );
    }
    let (Some(end), Some(open), Some(unknown)) = (
        life["end"].as_str(),
        life["bash_runs_open"].as_u64(),
        life["bash_run_end_unknown"].as_bool(),
    ) else {
        return Err("lifecycle facts inconsistent");
    };
    let end = match end {
        "observed" => "observed",
        "unknown" => "unknown (it may still be running)",
        "no-process" => "no process",
        _ => return Err("lifecycle facts inconsistent"),
    };
    Ok(format!(
        "\nLifecycle status: end {end}; its Bash runs still open at this result: {open}{}; budget: {}.",
        if unknown { " (one ended unknown)" } else { "" },
        life["budget"].as_str().unwrap_or("not reported")
    ))
}

fn shown_answer(text: &str) -> String {
    if text.len() <= ANSWER_LIMIT {
        return text.to_owned();
    }
    let mut cut = ANSWER_LIMIT;
    while !text.is_char_boundary(cut) {
        cut -= 1;
    }
    format!(
        "{}\n[answer truncated by this tool: {} of {} bytes shown]",
        &text[..cut],
        cut,
        text.len()
    )
}

fn result(route: &str, value: &Value, stages: &[Value], stderr: &str) -> Answer {
    let outcome = value["outcome"].as_str().unwrap_or_default();
    let Some(child) = value["child"].as_str().filter(|child| !child.is_empty()) else {
        return unresolved("result names no child", stages, stderr);
    };
    if value["route"] != json!(route) {
        return unresolved("result is for another route", stages, stderr);
    }
    if !OUTCOMES.contains(&outcome) {
        return unresolved(
            &format!(
                "unrecognized outcome {}",
                serde_json::to_string(&value["outcome"]).expect("value serializes")
            ),
            stages,
            stderr,
        );
    }
    let answered = value["answer"].as_str();
    let launch = &value["launch"];
    let consistent = match outcome {
        "answered" => answered.is_some() && value["turn_end"]["stop_reason"] == "end_turn",
        "launch-failed" => launch["event"] == "launch-failed",
        "launch-unknown" => launch["event"] == "launch-unknown",
        "stopped" => value["stopped"].is_string(),
        _ => true,
    };
    if !consistent {
        return unresolved("result facts inconsistent with its outcome", stages, stderr);
    }
    let facts = launch_text(launch)
        .and_then(|launch| Ok(launch + &end_text(&value["end"])?))
        .and_then(|facts| Ok(facts + &status_text(&value["lifecycle"])?));
    let facts = match facts {
        Ok(facts) => facts,
        Err(reason) => return unresolved(reason, stages, stderr),
    };
    let head = format!(
        "Explorer {child} (route {route}): {outcome}{}.",
        value["stopped"]
            .as_str()
            .map(|reason| format!("; stopped by the root ({reason})"))
            .unwrap_or_default()
    );
    let turn_end = value["turn_end"]["stop_reason"]
        .as_str()
        .map(|reason| format!("\nTurn end (final result): {reason}."))
        .unwrap_or_default();
    let body = match answered {
        Some(text) => format!(
            "\nAnswer (the child's last reply before its turn ended; not verified):\n{}",
            shown_answer(text)
        ),
        None => "\nNo answer was received from the child.".into(),
    };
    answer(
        format!(
            "{head}{turn_end}{body}{facts}\nThe outcome is about the answer's content only: it does not prove the child or its Bash ended or drained, or that anything was done with it. This admission used one of this root's child starts (and a prepared route's slot) whatever its outcome; nothing is retried.{}",
            lifecycle(stages)
        ),
        outcome != "answered",
    )
}

/// Renders one requester invocation: its exit `code` when it exited and its
/// stdout was read whole, its stdout and its (bounded) stderr.
pub fn render_child(route: &str, code: Option<i32>, stdout: &[u8], stderr: &str) -> Answer {
    let stages = relayed(stderr);
    let text = String::from_utf8_lossy(stdout);
    let text = text.trim();
    let value = (!text.is_empty())
        .then(|| serde_json::from_str::<Value>(text).ok())
        .flatten();
    match (code, value) {
        (Some(0), Some(value)) if value["event"] == "result" => {
            result(route, &value, &stages, stderr)
        }
        (Some(65), Some(value))
            if value["event"] == "refused"
                && value["reason"].as_str().is_some_and(|r| !r.is_empty()) =>
        {
            refused(&value, &stages)
        }
        (Some(75), Some(value)) if value["event"] == "lost" => {
            unresolved("the requester received no result", &stages, stderr)
        }
        (Some(69 | 64), None) if text.is_empty() && stages.is_empty() => answer(
            format!(
                "Explorer request not sent ({}): nothing was asked and no child was admitted.{}",
                if code == Some(69) {
                    "the requester found no owner ingress or could not reach the owner"
                } else {
                    "the requester refused its arguments"
                },
                stderr_note(stderr)
            ),
            true,
        ),
        (Some(code @ (0 | 65 | 75)), _) => unresolved(
            &format!("requester exit {code} without its matching result object"),
            &stages,
            stderr,
        ),
        (code, _) => unresolved(
            &format!("requester did not complete its result: {code:?}"),
            &stages,
            stderr,
        ),
    }
}
