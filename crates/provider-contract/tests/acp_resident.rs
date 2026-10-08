//! Resident start/turn agreement, refusal, attribution and turn-end
//! semantics against scripted peers. No process, model or provider runs.

use agent_provider_contract::acp::resident::{
    self as host, Binding, PreparedEndpoint, SessionStart, StartRefusal, TurnRef,
};
use agent_provider_contract::acp::{
    AcpClient, ClientInfo, IdleWaitFailure, LineTransport, NativeCustody, NegotiationFailure,
};
use agent_provider_contract::generated::PolicyEvaluateResult;
use agent_provider_contract::resident_session::{
    self, ResidentPrepareResult, TemplateRefusal, OPERATIONS,
};
use serde_json::{json, Value};
use std::io::Cursor;

type Scripted = AcpClient<LineTransport<Cursor<Vec<u8>>, Vec<u8>>>;

fn scripted(replies: &[Value]) -> Scripted {
    let input: String = replies.iter().map(|reply| format!("{reply}\n")).collect();
    AcpClient::new(
        LineTransport::new(Cursor::new(input.into_bytes()), Vec::new()),
        ClientInfo {
            name: "resident-host-test".into(),
            version: "0".into(),
        },
    )
}

/// Every request the client wrote.
fn sent(client: Scripted) -> Vec<Value> {
    let (_, written) = client.into_transport().into_parts();
    String::from_utf8(written)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn prepare_result() -> ResidentPrepareResult {
    ResidentPrepareResult::v1(
        vec![
            "resident.serve".into(),
            "--config".into(),
            "/p/c.json".into(),
        ],
        "a".repeat(64),
    )
}

fn endpoint() -> PreparedEndpoint {
    PreparedEndpoint::agree(&prepare_result()).unwrap()
}

fn init(meta: Value) -> Value {
    json!({"jsonrpc":"2.0","id":1,"result":{
        "protocolVersion":2,"info":{"name":"native-adapter","version":"9.9.9"},
        "capabilities":{"session":{}},"_meta":meta
    }})
}

fn resident_meta() -> Value {
    json!({
        "oulipoly.ai/messageKeyDedup":{"version":1},
        "oulipoly.ai/residentSession":{
            "protocol":"oulipoly.resident_session/v1","acp_schema":"schema-v2.0.0-alpha.7"}
    })
}

fn idle(tag: Option<&str>, native: Option<Value>) -> Value {
    let mut meta = serde_json::Map::new();
    if let Some(tag) = tag {
        meta.insert("oulipoly.ai/lastUserMessageId".into(), json!(tag));
    }
    if let Some(native) = native {
        meta.insert("oulipoly.ai/nativeTurn".into(), native);
    }
    json!({"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s",
        "update":{"sessionUpdate":"state_update","state":"idle","stopReason":"end_turn",
            "_meta":meta}}})
}

#[test]
fn agreement_is_declared_schema_and_operations_not_identity() {
    let endpoint = endpoint();
    assert_eq!(
        endpoint.argv("/opt/providers/registered"),
        [
            "/opt/providers/registered",
            "resident.serve",
            "--config",
            "/p/c.json"
        ]
    );
    assert!(endpoint.serves("session/resume"));
    // Another digest or arguments are attribution, still agreed.
    let mut other = prepare_result();
    other.config_sha256 = "b".repeat(64);
    other.invocation.args = vec!["serve".into()];
    assert!(PreparedEndpoint::agree(&other).is_ok());
}

#[test]
fn unshared_acp_schema_or_version_is_no_common_acp() {
    let mut newer = prepare_result();
    newer.acp.schema = "schema-v2.0.0-alpha.9".into();
    assert_eq!(
        PreparedEndpoint::agree(&newer),
        Err(StartRefusal::NoCommonAcp {
            protocol_version: 2,
            schema: "schema-v2.0.0-alpha.9".into()
        })
    );
    let mut v1 = prepare_result();
    v1.acp.protocol_version = 1;
    assert!(matches!(
        PreparedEndpoint::agree(&v1),
        Err(StartRefusal::NoCommonAcp {
            protocol_version: 1,
            ..
        })
    ));
}

#[test]
fn missing_required_operation_or_invalid_result_is_refused() {
    let mut no_prompt = prepare_result();
    no_prompt.operations.retain(|op| op != "session/prompt");
    assert_eq!(
        PreparedEndpoint::agree(&no_prompt),
        Err(StartRefusal::OperationNotServed("session/prompt"))
    );
    let mut socket = prepare_result();
    socket.invocation.endpoint = "unix".into();
    assert!(matches!(
        PreparedEndpoint::agree(&socket),
        Err(StartRefusal::InvalidPrepared(_))
    ));
    let mut no_dedup = prepare_result();
    no_dedup.acp.dedup_contract = 2;
    assert!(matches!(
        PreparedEndpoint::agree(&no_dedup),
        Err(StartRefusal::InvalidPrepared(_))
    ));
}

#[test]
fn start_attributes_the_native_session_and_leaves_it_unbound() {
    let mut client = scripted(&[
        init(resident_meta()),
        json!({"jsonrpc":"2.0","id":2,"result":{"sessionId":"native-res-1"}}),
    ]);
    let session = host::start_session(
        &mut client,
        &endpoint(),
        SessionStart::New {
            cwd: "/work".into(),
        },
    )
    .unwrap();
    assert_eq!(session.native.session_id, "native-res-1");
    assert_eq!(session.native.agent_name, "native-adapter");
    assert_eq!(session.native.agent_version, "9.9.9");
    assert_eq!(session.native.config_sha256, "a".repeat(64));
    assert_eq!(session.binding(), &Binding::Unbound);
    let requests = sent(client);
    assert_eq!(requests[1]["method"], "session/new");
    assert_eq!(requests[1]["params"], json!({"cwd":"/work"}));
}

fn contradicted(meta: Value) -> (Result<host::StartedSession, StartRefusal>, Vec<Value>) {
    let mut client = scripted(&[
        init(meta),
        json!({"jsonrpc":"2.0","id":2,"result":{"sessionId":"never"}}),
    ]);
    let result = host::start_session(
        &mut client,
        &endpoint(),
        SessionStart::New {
            cwd: "/work".into(),
        },
    );
    (result, sent(client))
}

#[test]
fn initialize_contradicting_prepare_is_refused_before_any_session_request() {
    let absent = json!({"oulipoly.ai/messageKeyDedup":{"version":1}});
    let other_schema = json!({
        "oulipoly.ai/messageKeyDedup":{"version":1},
        "oulipoly.ai/residentSession":{"protocol":"oulipoly.resident_session/v1",
            "acp_schema":"schema-v2.0.0-alpha.8"}
    });
    let no_dedup = json!({"oulipoly.ai/residentSession":{
        "protocol":"oulipoly.resident_session/v1","acp_schema":"schema-v2.0.0-alpha.7"}});
    for meta in [absent, other_schema, no_dedup] {
        let (result, requests) = contradicted(meta.clone());
        assert!(
            matches!(result, Err(StartRefusal::DeclarationContradicted(_))),
            "{meta}: {result:?}"
        );
        assert_eq!(requests.len(), 1, "only initialize was sent for {meta}");
    }
}

#[test]
fn negotiation_failure_is_a_start_refusal() {
    let mut client = scripted(&[json!({"jsonrpc":"2.0","id":1,"result":{
        "protocolVersion":1,"agentCapabilities":{},"authMethods":[]}})]);
    assert_eq!(
        host::start_session(
            &mut client,
            &endpoint(),
            SessionStart::New {
                cwd: "/work".into()
            }
        ),
        Err(StartRefusal::Negotiation(
            NegotiationFailure::UnsupportedVersion { agent_version: 1 }
        ))
    );
}

#[test]
fn undeclared_resume_is_refused_before_anything_is_sent() {
    let mut result = prepare_result();
    result.operations.retain(|op| op != "session/resume");
    let endpoint = PreparedEndpoint::agree(&result).unwrap();
    let mut client = scripted(&[init(resident_meta())]);
    assert_eq!(
        host::start_session(
            &mut client,
            &endpoint,
            SessionStart::Resume {
                session_id: "native-res-1".into(),
                cwd: "/work".into()
            }
        ),
        Err(StartRefusal::OperationNotServed("session/resume"))
    );
    assert!(sent(client).is_empty());
}

#[test]
fn binding_is_only_what_the_host_supplies() {
    let mut client = scripted(&[
        init(resident_meta()),
        json!({"jsonrpc":"2.0","id":2,"result":{}}),
    ]);
    let mut session = host::start_session(
        &mut client,
        &endpoint(),
        SessionStart::Resume {
            session_id: "native-res-1".into(),
            cwd: "/work".into(),
        },
    )
    .unwrap();
    assert!(session.resumed);
    assert_eq!(session.binding(), &Binding::Unbound);
    session.bind("chain:42");
    assert_eq!(session.binding(), &Binding::Bound("chain:42".into()));
}

fn turn(message_id: &str) -> TurnRef {
    TurnRef {
        session_id: "s".into(),
        message_id: message_id.into(),
    }
}

#[test]
fn turn_end_needs_a_covering_tag_and_untagged_idle_is_readiness_only() {
    let native = json!({"request_id":"turn-3","custody":"complete",
        "status":{"kind":"exited","code":0}});
    let mut client = scripted(&[
        idle(None, None),
        idle(Some("msg_0000000000000001"), None),
        idle(Some("msg_0000000000000003"), Some(native)),
    ]);
    let end = host::await_turn_end(&mut client, &turn("msg_0000000000000002")).unwrap();
    assert_eq!(end.covered_by, "msg_0000000000000003");
    assert!(!end.is_own(), "covered by a later input's turn end");
    let report = end.native_turn.unwrap();
    assert_eq!(report.request_id, "turn-3");
    assert_eq!(report.custody, NativeCustody::Complete);
    // Earlier inputs are found in already-observed events.
    let earlier = host::await_turn_end(&mut client, &turn("msg_0000000000000001")).unwrap();
    assert!(earlier.is_own());
    // An input no idle covers waits until the peer is gone.
    assert_eq!(
        host::await_turn_end(&mut client, &turn("msg_0000000000000004")),
        Err(IdleWaitFailure::PeerGone)
    );
}

#[test]
fn invalid_native_turn_report_claims_nothing() {
    let mut client = scripted(&[
        idle(
            Some("msg_0000000000000001"),
            Some(json!({"record_error":"disk full"})),
        ),
        idle(
            Some("msg_0000000000000002"),
            Some(json!({"request_id":"r","custody":"settled"})),
        ),
        idle(
            Some("msg_0000000000000003"),
            Some(json!({"request_id":"r","custody":"reconciled",
                "failure":{"code":"launch_reconciliation_required","message":"m"}})),
        ),
        idle(
            Some("msg_0000000000000004"),
            Some(json!({"request_id":"","custody":"complete"})),
        ),
        idle(
            Some("msg_0000000000000005"),
            Some(json!({"request_id":"r","custody":"complete","verified":true})),
        ),
    ]);
    let first = host::await_turn_end(&mut client, &turn("msg_0000000000000001")).unwrap();
    assert_eq!(first.native_turn, None);
    let second = host::await_turn_end(&mut client, &turn("msg_0000000000000002")).unwrap();
    assert_eq!(second.native_turn, None);
    let third = host::await_turn_end(&mut client, &turn("msg_0000000000000003")).unwrap();
    let report = third.native_turn.unwrap();
    assert_eq!(report.custody, NativeCustody::Reconciled);
    assert_eq!(
        report.failure_code.as_deref(),
        Some("launch_reconciliation_required")
    );
    // Shapes outside NativeTurnMeta claim nothing, even with a known custody.
    for tag in ["msg_0000000000000004", "msg_0000000000000005"] {
        let end = host::await_turn_end(&mut client, &turn(tag)).unwrap();
        assert_eq!(end.native_turn, None, "{tag}");
    }
}

#[test]
fn shared_vocabulary_matches_the_resident_extension() {
    use agent_provider_contract::acp;
    assert_eq!(resident_session::ACP_SCHEMA_TAG, acp::pin::SCHEMA_TAG);
    assert_eq!(
        resident_session::ACP_PROTOCOL_VERSION,
        u64::from(acp::PROTOCOL_VERSION)
    );
    for operation in OPERATIONS {
        assert!(
            resident_session::validate("ResidentPrepareResult", &{
                let mut value = serde_json::to_value(prepare_result()).unwrap();
                value["operations"] = json!([operation]);
                value
            })
            .is_ok(),
            "{operation} is a schema operation"
        );
    }
}

fn settings() -> Value {
    json!({"settings_id":"account-a","mode":"agent",
        "model":{"name":"role:refine","provider_args":[],"inputs":{"named":{}}},
        "launch":{"env":{}}})
}

fn policy(edit: impl FnOnce(&mut Value)) -> PolicyEvaluateResult {
    let mut value = json!({"accepted":true,"argv":["native","--flag"],
        "env":{"KEEP":"1"},"stdin":null,"prompt":null,"diagnostics":[],"markers":[]});
    edit(&mut value);
    serde_json::from_value(value).unwrap()
}

#[test]
fn template_carries_opaque_settings_and_evaluated_launch() {
    let params = resident_session::template_from_policy(&settings(), &policy(|_| {})).unwrap();
    assert_eq!(params.protocol, resident_session::PROTOCOL);
    assert_eq!(params.launch.settings_id, "account-a");
    assert_eq!(
        params.launch.model,
        json!({"name":"role:refine","provider_args":[],"inputs":{"named":{}}})
    );
    assert_eq!(params.launch.argv, ["native", "--flag"]);
    assert_eq!(params.launch.env.unwrap()["KEEP"], "1");
}

#[test]
fn template_refuses_what_it_cannot_honour() {
    assert_eq!(
        resident_session::template_from_policy(
            &settings(),
            &policy(|p| {
                p["accepted"] = json!(false);
                p["diagnostics"] = json!([{"severity":"error","message":"tool limit unsupported"}]);
            })
        ),
        Err(TemplateRefusal::NotAccepted(vec![
            "tool limit unsupported".into()
        ]))
    );
    assert_eq!(
        resident_session::template_from_policy(
            &settings(),
            &policy(|p| p["stdin"] = json!("preamble"))
        ),
        Err(TemplateRefusal::Unhonourable("stdin"))
    );
    assert_eq!(
        resident_session::template_from_policy(
            &settings(),
            &policy(|p| p["prompt"] = json!("rewritten"))
        ),
        Err(TemplateRefusal::Unhonourable("prompt"))
    );
    assert_eq!(
        resident_session::template_from_policy(&settings(), &policy(|p| p["argv"] = Value::Null)),
        Err(TemplateRefusal::MissingArgv)
    );
    assert!(matches!(
        resident_session::template_from_policy(&json!({"mode":"agent"}), &policy(|_| {})),
        Err(TemplateRefusal::Invalid(_))
    ));
}
