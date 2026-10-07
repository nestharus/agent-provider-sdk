//! session_control/v1: structural schema versus semantic admission; the
//! claim ladder (intent, receipt, admission, acknowledgment/refusal,
//! outcome); repetition and idempotency; settlement facts kept apart;
//! redaction and bounds; selection and its diagnostics.
//!
//! These are deterministic contract checks over claims. They do not show that
//! any producer tells the truth, that a requester is authorized, that a hold
//! is enforced or durable, or that any actor is in custody.

pub mod support {
    pub mod contract_matrix;
}

use agent_provider_contract::session_control::{
    classify_repetition, read_settlement, select, validate, Authority, ControlScope,
    ControlUnavailable, LogicalRef, Observation, Offer, Operation, Record, Repetition, Request,
    RequestTrace, Selected, SettlementReading, Step, UnavailableReason, MAX_RECORD_BYTES,
    MAX_TRACE_RECEIPTS, PROTOCOL,
};
use agent_provider_contract::SchemaRegistry;
use serde_json::{json, Value};
use support::contract_matrix::{
    fixtures as contract_fixtures, launch_event_fixture, LAUNCH_EVENT_ROWS, NON_LAUNCH_ROWS,
};

fn fixtures() -> Value {
    serde_json::from_str(include_str!("fixtures/session_control/v1.json")).unwrap()
}

fn cases(value: &Value) -> &Vec<Value> {
    value.as_array().unwrap()
}

fn request(value: &Value) -> Request {
    match Record::decode(value).unwrap() {
        Record::Request(request) => request,
        other => panic!("not a request: {other:?}"),
    }
}

#[test]
fn classified_records_distinguish_raw_schema_from_semantic_admission() {
    let all = fixtures();
    assert_eq!(all["protocol"], PROTOCOL);
    for case in cases(&all["valid"]["Record"]) {
        let value = &case["value"];
        let record = Record::decode(value).unwrap_or_else(|e| panic!("{}: {e}", case["name"]));
        assert_eq!(
            &serde_json::to_value(&record).unwrap(),
            value,
            "{}",
            case["name"]
        );
        assert_eq!(Record::decode_line(&record.encode_line()).unwrap(), record);
    }
    for (layer, schema_accepts) in [("invalid_structural", false), ("invalid_semantic", true)] {
        for case in cases(&all[layer]["Record"]) {
            let value = &case["value"];
            let structural = validate("Record", value, UnavailableReason::InvalidRecord);
            assert_eq!(
                structural.is_ok(),
                schema_accepts,
                "{layer}: {}",
                case["name"]
            );
            let error = Record::decode(value).expect_err(case["name"].as_str().unwrap());
            assert_eq!(
                error.reason,
                UnavailableReason::InvalidRecord,
                "{}",
                case["name"]
            );
        }
    }
}

#[test]
fn typed_records_cannot_bypass_semantic_admission() {
    let all = fixtures();
    let stale_ack = cases(&all["invalid_semantic"]["Record"])
        .iter()
        .find(|case| case["name"] == "acknowledgment by a stale or successor authority")
        .unwrap();
    // Raw Serde accepts the representation; admission refuses the claim.
    let raw: Record = serde_json::from_value(stale_ack["value"].clone()).unwrap();
    assert!(Record::decode_line(&raw.encode_line()).is_err());
    let hold = request(&cases(&all["valid"]["Record"])[0]["value"]);
    let mut misaddressed = hold.clone();
    misaddressed.scope = ControlScope {
        root: "root-8".into(),
        child: None,
    };
    assert_eq!(
        RequestTrace::new(misaddressed).unwrap_err().reason,
        UnavailableReason::InvalidRecord
    );
    let mut trace = RequestTrace::new(hold).unwrap();
    assert!(trace.accept(&raw).is_err());
    assert!(trace.admission().is_none(), "refused record left no state");
}

#[test]
fn bounds_and_redaction_are_exact() {
    let all = fixtures();
    let base = cases(&all["valid"]["Record"])[0]["value"].clone();
    let mut longest = base.clone();
    longest["reason"]["text"] = json!("é".repeat(512));
    longest["request_key"] = json!("k".repeat(128));
    let line = Record::decode(&longest).unwrap().encode_line();
    assert!(line.len() <= MAX_RECORD_BYTES);
    let mut over = longest.clone();
    over["reason"]["text"] = json!("é".repeat(513));
    assert!(Record::decode(&over).is_err());

    // A maximal observation fits the line bound.
    let host = "h".repeat(256);
    let authority = json!({"root": host, "owner": host, "generation": host, "incarnation": host});
    let evidence: Vec<Value> = (0..16)
        .map(|_| {
            json!({"actor": "os_process", "exactness": "exact",
                        "ref": {"state": "present", "ref": host}})
        })
        .collect();
    let observation = json!({"kind": "observation", "protocol": PROTOCOL,
        "reporter": authority, "subject": {"root": host, "child": host, "work": host, "input": host},
        "fact": {"type": "physical_custody", "state": "exited_wait_pending"},
        "evidence": evidence, "observed_at_unix_ms": 9_007_199_254_740_991u64});
    let line = Record::decode(&observation).unwrap().encode_line();
    assert!(line.len() <= MAX_RECORD_BYTES, "{}", line.len());

    // The line bound applies before parsing.
    let padded = format!("{line}{}", " ".repeat(MAX_RECORD_BYTES - line.len()));
    assert!(Record::decode_line(&padded).is_ok());
    let error = Record::decode_line(&format!("{padded} ")).unwrap_err();
    assert!(error.detail.unwrap().contains("exceeds"));

    // Redacted and missing are distinct admitted states, and neither
    // carries the withheld value.
    let redacted = cases(&all["valid"]["Record"])
        .iter()
        .find(|case| case["name"] == "request with redacted reason")
        .unwrap();
    let missing = cases(&all["valid"]["Record"])
        .iter()
        .find(|case| case["name"] == "request with missing reason")
        .unwrap();
    assert_ne!(
        request(&redacted["value"]).reason,
        request(&missing["value"]).reason
    );
}

#[test]
fn sdk_generated_diagnostics_do_not_repeat_submitted_values() {
    let all = fixtures();
    let secret = cases(&all["invalid_structural"]["Record"])
        .iter()
        .find(|case| case["value"].get("secret").is_some())
        .unwrap();
    let error = Record::decode(&secret["value"]).unwrap_err();
    let shown = format!("{error} {}", serde_json::to_string(&error).unwrap());
    assert!(!shown.contains("SECRETVALUE"), "{shown}");
    validate(
        "ControlUnavailable",
        &serde_json::to_value(&error).unwrap(),
        error.reason,
    )
    .unwrap();
    let long = ControlUnavailable::new(UnavailableReason::InvalidRecord, "é".repeat(600));
    assert_eq!(long.detail.as_ref().unwrap().chars().count(), 512);
    validate(
        "ControlUnavailable",
        &serde_json::to_value(&long).unwrap(),
        long.reason,
    )
    .unwrap();
}

#[test]
fn selection_vectors_and_capability_diagnostics() {
    for case in cases(&fixtures()["selection"]) {
        let local: Offer = serde_json::from_value(case["local"].clone()).unwrap();
        let result = select(&local, &case["remote"]);
        match case["expect"].get("selected") {
            Some(selected) => assert_eq!(
                &serde_json::to_value(result.unwrap()).unwrap(),
                selected,
                "{}",
                case["name"]
            ),
            None => {
                let error = result.expect_err(case["name"].as_str().unwrap());
                assert_eq!(
                    serde_json::to_value(error.reason).unwrap(),
                    case["expect"]["unavailable"],
                    "{}",
                    case["name"]
                );
            }
        }
    }
    let oversized = json!({ PROTOCOL: {"operations": ["input_hold"], "facts": []},
                            "x.pad/v1": "p".repeat(16_400) });
    let local = Offer {
        operations: vec![Operation::InputHold],
        facts: vec![],
    };
    assert_eq!(
        select(&local, &oversized).unwrap_err().reason,
        UnavailableReason::InvalidAdvertisement
    );
}

#[test]
fn agreement_joins_records_with_the_selection() {
    for case in cases(&fixtures()["agreement"]) {
        let selected: Selected = serde_json::from_value(case["selected"].clone()).unwrap();
        let result = match Record::decode(&case["record"]).unwrap() {
            Record::Request(request) => request.agree(&selected),
            Record::Observation(observation) => observation.agree(&selected),
            other => panic!("unexpected {other:?}"),
        };
        let got = match result {
            Ok(()) => json!("agrees"),
            Err(error) => serde_json::to_value(error.reason).unwrap(),
        };
        assert_eq!(got, case["expect"], "{}", case["name"]);
    }
}

#[test]
fn repetition_vectors_separate_retry_from_key_conflict() {
    for case in cases(&fixtures()["repetition"]) {
        let first = request(&case["first"]);
        let again = request(&case["again"]);
        let got = classify_repetition(&first, &again);
        assert_eq!(
            serde_json::to_value(got).unwrap(),
            case["expect"],
            "{}",
            case["name"]
        );
        // Classification is symmetric.
        assert_eq!(classify_repetition(&again, &first), got, "{}", case["name"]);
    }
    let all = fixtures();
    let hold = request(&cases(&all["valid"]["Record"])[0]["value"]);
    assert_eq!(classify_repetition(&hold, &hold), Repetition::SameRequest);
}

#[test]
fn trace_vectors_keep_the_claim_ladder_distinct() {
    for case in cases(&fixtures()["traces"]) {
        let name = case["name"].as_str().unwrap();
        let mut trace = RequestTrace::new(request(&case["request"])).unwrap();
        let records = cases(&case["records"]);
        let expect = cases(&case["expect"]);
        assert_eq!(records.len(), expect.len(), "{name}");
        for (index, (value, expected)) in records.iter().zip(expect).enumerate() {
            let record = Record::decode(value)
                .unwrap_or_else(|e| panic!("{name} record {index} must be admissible: {e}"));
            let before = trace.clone();
            match trace.accept(&record) {
                Ok(step) => {
                    assert!(
                        expected.get("violation").is_none(),
                        "{name} {index}: {step:?}"
                    );
                    assert_eq!(
                        &serde_json::to_value(&step).unwrap(),
                        expected,
                        "{name} {index}"
                    );
                    if step == Step::Duplicate {
                        assert_eq!(trace, before, "{name} {index}: duplicate mutated state");
                    }
                }
                Err(error) => {
                    assert_eq!(
                        expected,
                        &json!({"violation": true}),
                        "{name} {index}: {error}"
                    );
                    assert_eq!(error.reason, UnavailableReason::ProtocolViolation, "{name}");
                    assert_eq!(
                        trace, before,
                        "{name} {index}: refused record mutated state"
                    );
                }
            }
        }
    }
}

#[test]
fn acknowledged_hold_survives_later_uncertainty() {
    let all = fixtures();
    let case = cases(&all["traces"])
        .iter()
        .find(|case| {
            case["name"]
                .as_str()
                .unwrap()
                .starts_with("acknowledged hold whose")
        })
        .unwrap();
    let mut trace = RequestTrace::new(request(&case["request"])).unwrap();
    for value in cases(&case["records"]).iter().take(3) {
        trace.accept(&Record::decode(value).unwrap()).unwrap();
    }
    assert!(
        trace.acknowledgment().is_some(),
        "uncertainty did not erase the ACK"
    );
    assert!(trace.outcome().unwrap().uncertainty.is_some());
}

#[test]
fn receipts_are_bounded_per_trace() {
    let all = fixtures();
    let hold = request(&cases(&all["valid"]["Record"])[0]["value"]);
    let mut trace = RequestTrace::new(hold.clone()).unwrap();
    for at in 0..=MAX_TRACE_RECEIPTS as u64 {
        let receipt = json!({"kind": "receipt", "protocol": PROTOCOL,
            "request_key": hold.request_key, "requester": hold.requester,
            "addressed": hold.addressed, "durable": false, "observed_at_unix_ms": at});
        let result = trace.accept(&Record::decode(&receipt).unwrap());
        assert_eq!(
            result.is_ok(),
            at < MAX_TRACE_RECEIPTS as u64,
            "receipt {at}"
        );
    }
}

#[test]
fn settlement_vectors_keep_logical_and_physical_facts_apart() {
    for case in cases(&fixtures()["settlement"]) {
        let subject: LogicalRef = serde_json::from_value(case["subject"].clone()).unwrap();
        let observations: Vec<Observation> = cases(&case["observations"])
            .iter()
            .map(|value| match Record::decode(value).unwrap() {
                Record::Observation(observation) => observation,
                other => panic!("not an observation: {other:?}"),
            })
            .collect();
        let reading: SettlementReading = read_settlement(&subject, &observations);
        assert_eq!(
            serde_json::to_value(reading).unwrap(),
            case["expect"],
            "{}",
            case["name"]
        );
        // Physical custody never changes the logical reading.
        let logical_only: Vec<Observation> = observations
            .iter()
            .filter(|o| {
                !matches!(
                    o.fact,
                    agent_provider_contract::session_control::Fact::PhysicalCustody { .. }
                )
            })
            .cloned()
            .collect();
        assert_eq!(
            read_settlement(&subject, &logical_only).logical,
            reading.logical,
            "{}",
            case["name"]
        );
    }
}

#[test]
fn provider_session_is_evidence_never_logical_authority() {
    let authority = Authority {
        root: "root-7".into(),
        owner: "owner-a".into(),
        generation: "g12".into(),
        incarnation: "inc-3f9".into(),
    };
    let value = serde_json::to_value(&authority).unwrap();
    validate("Authority", &value, UnavailableReason::InvalidRecord).unwrap();
    for field in ["provider_session", "pid", "stream_id"] {
        let mut extended = value.clone();
        extended[field] = json!("x");
        assert!(validate("Authority", &extended, UnavailableReason::InvalidRecord).is_err());
        let mut subject = json!({"root": "root-7"});
        subject[field] = json!("x");
        assert!(validate("LogicalRef", &subject, UnavailableReason::InvalidRecord).is_err());
    }
}

#[test]
fn control_records_and_diagnostics_are_not_provider_outcomes() {
    let registry = SchemaRegistry::new();
    let all = fixtures();
    let records: Vec<&Value> = ["valid", "invalid_semantic"]
        .iter()
        .flat_map(|layer| {
            cases(&all[*layer]["Record"])
                .iter()
                .map(|case| &case["value"])
        })
        .collect();
    let diagnostic = serde_json::to_value(ControlUnavailable::new(
        UnavailableReason::NoCommonVersion,
        "",
    ))
    .unwrap();
    let contract = contract_fixtures();
    for row in LAUNCH_EVENT_ROWS {
        for value in records.iter().copied().chain([&diagnostic]) {
            assert!(
                registry.validate_launch_event(row.kind, value).is_err(),
                "{}",
                row.kind
            );
        }
        // Provider launch events, including the terminal exit, are not control records.
        let event = launch_event_fixture(&contract, row.kind);
        assert!(Record::decode(event).is_err(), "{}", row.kind);
    }
    for row in NON_LAUNCH_ROWS {
        for value in records.iter().copied().chain([&diagnostic]) {
            assert!(registry.validate_response(row.subcommand, value).is_err());
            assert!(registry
                .validate_error_response(row.subcommand, value)
                .is_err());
        }
    }
}
