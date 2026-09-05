use agent_provider_contract::generated::HostContext;
use agent_provider_contract::terminal_unavailable::{
    decode_signal, host_selected, AdmissionError, ProviderUnavailableKind, HOST_SELECTION_ENV,
};
use agent_provider_contract::SchemaRegistry;
use serde_json::{json, Value};

fn host(selection: Option<&str>) -> HostContext {
    let mut value = json!({"app": "extension-test", "env": {}});
    if let Some(selection) = selection {
        value["env"][HOST_SELECTION_ENV] = json!(selection);
    }
    serde_json::from_value(value).unwrap()
}

fn signal() -> Value {
    json!({
        "kind": "provider_unavailable",
        "evidence": "native model service temporarily unavailable",
        "observed_at_unix_ms": 1788622298202_u64
    })
}

#[test]
fn explicit_current_request_selection_is_required() {
    for selection in [
        None,
        Some(""),
        Some("0"),
        Some("true"),
        Some(" 1"),
        Some("1 "),
    ] {
        let host = host(selection);
        assert!(!host_selected(&host));
        assert_eq!(
            decode_signal(&host, &signal()),
            Err(AdmissionError::NotSelected)
        );
    }
    let selected = host(Some("1"));
    assert!(host_selected(&selected));
    let decoded = decode_signal(&selected, &signal()).unwrap();
    assert_eq!(decoded.kind, ProviderUnavailableKind::ProviderUnavailable);
    assert_eq!(serde_json::to_value(decoded).unwrap(), signal());
}

#[test]
fn invalid_payloads_and_other_failure_categories_are_not_admitted() {
    let selected = host(Some("1"));
    for kind in ["quota_exhausted_inband", "rate_limited", "unknown"] {
        let mut value = signal();
        value["kind"] = json!(kind);
        assert_eq!(
            decode_signal(&selected, &value),
            Err(AdmissionError::InvalidSignal)
        );
    }
    for value in [
        json!({"kind": "provider_unavailable"}),
        json!({"kind": "provider_unavailable", "observed_at_unix_ms": -1}),
        json!({"kind": "provider_unavailable", "observed_at_unix_ms": 1.5}),
        json!({"kind": "provider_unavailable", "observed_at_unix_ms": 1, "retry": true}),
        json!({"kind": "provider_unavailable", "observed_at_unix_ms": 1, "evidence": null}),
    ] {
        assert_eq!(
            decode_signal(&selected, &value),
            Err(AdmissionError::InvalidSignal)
        );
    }
}

#[test]
fn evidence_bound_counts_characters_and_errors_do_not_echo_payload() {
    let selected = host(Some("1"));
    let mut value = signal();
    value["evidence"] = json!("é".repeat(1024));
    assert!(decode_signal(&selected, &value).is_ok());
    value["evidence"] = json!("é".repeat(1025));
    assert_eq!(
        decode_signal(&selected, &value),
        Err(AdmissionError::InvalidSignal)
    );
    value["unexpected"] = json!("fixture-secret-must-not-be-rendered");
    assert_eq!(
        decode_signal(&selected, &value).unwrap_err().to_string(),
        "invalid terminal-unavailable/v1 signal"
    );
}

#[test]
fn pinned_base_remains_strict_and_legacy_fallback_remains_valid() {
    let registry = SchemaRegistry::new();
    let mut response = json!({
        "contract": "oulipoly.provider/v1", "request_id": "terminal-fixture", "ok": true,
        "result": {"terminal_signal": signal()}
    });
    let mut exit = json!({
        "contract": "oulipoly.provider/v1", "request_id": "launch-fixture",
        "seq": 1, "time_unix_ms": 1788622298202_u64, "kind": "exit",
        "status": {"kind": "exited", "code": 1}, "terminal_signal": signal()
    });
    assert!(registry
        .validate_response("terminal.classify", &response)
        .is_err());
    assert!(registry.validate_launch_event("exit", &exit).is_err());
    response["result"]["terminal_signal"]["kind"] = json!("nonzero_exit");
    exit["terminal_signal"]["kind"] = json!("nonzero_exit");
    registry
        .validate_response("terminal.classify", &response)
        .unwrap();
    registry.validate_launch_event("exit", &exit).unwrap();
}
