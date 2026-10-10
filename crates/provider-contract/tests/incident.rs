//! incident/v1: structural schema versus semantic admission; the minimum
//! observed evidence a claimed cause needs (SIGKILL is not OOM); withheld
//! evidence versus negative observation; verification proof versus the
//! authorization record resting on it; authorization coverage of
//! session-control requests (release only, never hold, recover or cancel);
//! scope containment known only where the contract can know it; selection,
//! agreement, report/authorization repetition and diagnostics; separation
//! from session-control records and provider outcomes.
//!
//! These are deterministic contract checks over claims. They do not show that
//! any reporter tells the truth, that a cause is real, that an issuer holds a
//! current fence, that a coordinator classifies or recovers correctly, or that
//! any host enforces a hold or release.

pub mod support {
    pub mod contract_matrix;
}

use agent_provider_contract::incident::{
    classify_authorization_repetition, classify_report_repetition, contains, select, validate,
    Cause, Containment, Evidence, IncidentRef, IncidentUnavailable, Offer, ProofReading,
    RecordKind, RecoveryAuthorization, Report, Selected, Selector, MAX_RECORD_BYTES, PROTOCOL,
    SUPPORTED_VERSIONS,
};
use agent_provider_contract::incident::{Record, Scope};
use agent_provider_contract::session_control::{self, Repetition, UnavailableReason};
use agent_provider_contract::SchemaRegistry;
use serde_json::{json, Value};
use support::contract_matrix::{
    fixtures as contract_fixtures, launch_event_fixture, LAUNCH_EVENT_ROWS, NON_LAUNCH_ROWS,
};

fn fixtures() -> Value {
    serde_json::from_str(include_str!("fixtures/incident/v1.json")).unwrap()
}

fn cases(value: &Value) -> &Vec<Value> {
    value.as_array().unwrap()
}

fn named<'a>(list: &'a Value, name: &str) -> &'a Value {
    cases(list)
        .iter()
        .find(|case| case["name"] == name)
        .unwrap_or_else(|| panic!("no case {name}"))
}

fn report(value: &Value) -> Report {
    match Record::decode(value).unwrap() {
        Record::Report(report) => report,
        other => panic!("not a report: {other:?}"),
    }
}

fn authorization(value: &Value) -> RecoveryAuthorization {
    match Record::decode(value).unwrap() {
        Record::RecoveryAuthorization(authorization) => authorization,
        other => panic!("not an authorization: {other:?}"),
    }
}

fn oom() -> Value {
    named(
        &fixtures()["valid"]["Record"],
        "oom_kill report with correlated exact signal, leaf membership and local oom_kill delta",
    )["value"]
        .clone()
}

fn snake(value: impl serde::Serialize) -> Value {
    serde_json::to_value(value).unwrap()
}

#[test]
fn classified_records_distinguish_raw_schema_from_semantic_admission() {
    let all = fixtures();
    assert_eq!(all["protocol"], PROTOCOL);
    assert_eq!(SUPPORTED_VERSIONS, &[1]);
    for case in cases(&all["valid"]["Record"]) {
        let value = &case["value"];
        let record = Record::decode(value).unwrap_or_else(|e| panic!("{}: {e}", case["name"]));
        assert_eq!(&snake(&record), value, "{}", case["name"]);
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
fn claimed_cause_needs_its_own_minimum_observed_evidence() {
    // The correlated triplet supports oom_kill; removing any one element
    // leaves SIGKILL-without-attribution, which only unknown or signal admit.
    let base = oom();
    let evidence = base["evidence"].as_array().unwrap().clone();
    assert_eq!(evidence.len(), 3);
    let admits = |items: &[Value], cause: &str| {
        let mut value = base.clone();
        value["evidence"] = json!(items);
        value["cause"] = json!(cause);
        Record::decode(&value).is_ok()
    };
    assert!(admits(&evidence, "oom_kill"));
    for removed in 0..evidence.len() {
        let mut fewer = evidence.clone();
        fewer.remove(removed);
        assert!(!admits(&fewer, "oom_kill"), "without item {removed}");
        assert!(admits(&fewer, "unknown"), "without item {removed}");
    }
    assert!(admits(&evidence[..1], "signal"));
    assert!(
        !admits(&evidence[1..], "signal"),
        "membership and counter are no signal"
    );
    // The same evidence claims neither an exit nor a provider condition.
    for cause in ["exit", "provider_condition", "component_failure"] {
        assert!(!admits(&evidence, cause), "{cause}");
    }
    // Every other signal is a signal, never an OOM kill.
    let mut sigterm = evidence.clone();
    sigterm[0]["signal"] = json!(15);
    assert!(admits(&sigterm, "signal"));
    assert!(!admits(&sigterm, "oom_kill"));
    // Withholding the counter is not observing it.
    let mut withheld = evidence.clone();
    withheld[2] = json!({"type": "withheld", "evidence": "memory_event",
                         "disclosure": {"state": "missing", "reason": "access_denied"}});
    assert!(!admits(&withheld, "oom_kill"));
    assert!(admits(&withheld, "unknown"));
    assert_eq!(
        serde_json::from_value::<Evidence>(withheld[2].clone())
            .unwrap()
            .evidence_type(),
        agent_provider_contract::incident::EvidenceType::MemoryEvent
    );
    assert_eq!(report(&base).cause, Cause::OomKill);
}

#[test]
fn typed_records_cannot_bypass_semantic_admission() {
    let all = fixtures();
    let sigkill = named(
        &all["invalid_semantic"]["Record"],
        "SIGKILL alone cannot claim oom_kill",
    );
    // Raw Serde accepts the representation; admission refuses the claim.
    let raw: Record = serde_json::from_value(sigkill["value"].clone()).unwrap();
    assert!(Record::decode_line(&raw.encode_line()).is_err());
    let Record::Report(raw_report) = raw else {
        panic!("report")
    };
    assert!(raw_report.admit().is_err());

    let failed = named(
        &all["invalid_semantic"]["Record"],
        "authorization on a failed reproduction",
    );
    let raw: RecoveryAuthorization =
        match serde_json::from_value::<Record>(failed["value"].clone()).unwrap() {
            Record::RecoveryAuthorization(authorization) => authorization,
            other => panic!("{other:?}"),
        };
    assert!(raw.admit().is_err());
    let release = &cases(&all["coverage"])[0]["request"];
    let release = match session_control::Record::decode(release).unwrap() {
        session_control::Record::Request(request) => request,
        other => panic!("{other:?}"),
    };
    // An inadmissible authorization covers nothing, not even a release.
    assert_eq!(
        raw.covers(&release).unwrap_err().reason,
        UnavailableReason::InvalidRecord
    );
}

#[test]
fn bounds_and_redaction_are_exact() {
    let base = oom();
    let line = |value: &Value| serde_json::to_string(value).unwrap();

    let mut longest = base.clone();
    longest["detail"] = json!({"state": "present", "text": "é".repeat(512)});
    longest["report_key"] = json!("k".repeat(128));
    assert!(Record::decode(&longest).is_ok());
    let mut over = longest.clone();
    over["detail"]["text"] = json!("é".repeat(513));
    assert!(Record::decode(&over).is_err());
    let mut key = longest.clone();
    key["report_key"] = json!("k".repeat(129));
    assert!(Record::decode(&key).is_err());

    // A maximal conflict (two reports, sixteen widest evidence items each,
    // widest host values and four-byte detail) fits the line bound.
    let host = "h".repeat(256);
    let actor = json!({"actor": "os_process", "exactness": "exact",
                       "ref": {"state": "present", "ref": host}});
    let widest = json!({"type": "cgroup_membership", "actor": actor,
                        "cgroup": {"state": "present", "ref": host}});
    let mut maximal = base.clone();
    maximal["report_key"] = json!("k".repeat(128));
    maximal["reporter"] = json!(host);
    maximal["subject"] = json!({"root": host, "child": host, "work": host, "input": host});
    maximal["scope"] = json!({"level": "logical", "subject": maximal["subject"]});
    maximal["cause"] = json!("unknown");
    maximal["evidence"] = json!(vec![widest.clone(); 16]);
    maximal["detail"] = json!({"state": "present", "text": "𝄞".repeat(512)});
    assert!(Record::decode(&maximal).is_ok());
    let mut original = maximal.clone();
    original.as_object_mut().unwrap().remove("kind");
    let mut submitted = original.clone();
    submitted["severity"] = json!("critical");
    let conflict = json!({"kind": "report_conflict", "protocol": PROTOCOL,
        "submitted": submitted, "original": original, "collector": host,
        "observed_at_unix_ms": 9007199254740991_u64});
    let conflict_line = line(&conflict);
    assert!(
        conflict_line.len() <= MAX_RECORD_BYTES,
        "{}",
        conflict_line.len()
    );
    assert!(Record::decode_line(&conflict_line).is_ok());
    let mut seventeen = maximal.clone();
    seventeen["evidence"] = json!(vec![widest; 17]);
    assert!(Record::decode(&seventeen).is_err());

    // Over-limit lines are refused before parsing.
    let padded = format!("{}{}", line(&base), " ".repeat(MAX_RECORD_BYTES));
    let error = Record::decode_line(&padded).unwrap_err();
    assert_eq!(error.reason, UnavailableReason::InvalidRecord);
    assert!(error.detail.unwrap().contains("exceeds"));

    // Redacted and missing are distinct states, and a redaction carries no
    // remnant of what it withholds.
    for detail in [
        json!({"state": "redacted"}),
        json!({"state": "missing", "reason": "access_denied"}),
    ] {
        let mut value = base.clone();
        value["detail"] = detail.clone();
        assert_eq!(snake(Record::decode(&value).unwrap())["detail"], detail);
    }
    let mut remnant = base.clone();
    remnant["detail"] = json!({"state": "redacted", "text": "withheld"});
    assert!(Record::decode(&remnant).is_err());
}

#[test]
fn sdk_generated_diagnostics_do_not_repeat_submitted_values() {
    let all = fixtures();
    let secret = named(
        &all["invalid_structural"]["Record"],
        "unknown property and its value are never echoed",
    );
    let error = Record::decode(&secret["value"]).unwrap_err();
    let shown = format!("{error} {}", serde_json::to_string(&error).unwrap());
    assert!(!shown.contains("SECRET"), "{shown}");
    validate("IncidentUnavailable", &snake(&error), error.reason).unwrap();
    let long = IncidentUnavailable::new(UnavailableReason::InvalidRecord, "é".repeat(600));
    assert_eq!(long.detail.as_ref().unwrap().chars().count(), 512);
    validate("IncidentUnavailable", &snake(&long), long.reason).unwrap();
}

#[test]
fn proof_readings_separate_failure_from_incompleteness() {
    for case in cases(&fixtures()["proof"]) {
        let verification = match Record::decode(&case["verification"]).unwrap() {
            Record::Verification(verification) => verification,
            other => panic!("{other:?}"),
        };
        assert_eq!(
            snake(verification.proof()),
            case["expect"],
            "{}",
            case["name"]
        );
    }
}

#[test]
fn authorization_rests_on_complete_proof_of_its_own_incident_epoch() {
    let all = fixtures();
    for case in cases(&all["valid"]["Record"]) {
        if let Record::RecoveryAuthorization(authorization) =
            Record::decode(&case["value"]).unwrap()
        {
            assert_eq!(authorization.proof.proof(), ProofReading::Complete);
            assert_eq!(authorization.proof.incident, authorization.incident);
            assert_eq!(
                contains(&authorization.proof.scope, &authorization.scope),
                Containment::Contained
            );
        }
    }
    // A verification may report failure; only an authorization record needs
    // the complete proof. The same failing checks are a valid verification.
    let failing = named(
        &all["valid"]["Record"],
        "verification reporting a failed reproduction",
    );
    let Record::Verification(verification) = Record::decode(&failing["value"]).unwrap() else {
        panic!("verification")
    };
    assert!(matches!(verification.proof(), ProofReading::Failed { .. }));
    let mut built = authorization(
        &named(
            &all["valid"]["Record"],
            "recovery authorization resting on complete proof",
        )["value"],
    );
    built.proof = verification;
    assert!(built.admit().is_err());
}

#[test]
fn containment_is_known_only_where_the_contract_can_know_it() {
    for case in cases(&fixtures()["containment"]) {
        let outer: Scope = serde_json::from_value(case["outer"].clone()).unwrap();
        let inner: Scope = serde_json::from_value(case["inner"].clone()).unwrap();
        assert_eq!(
            snake(contains(&outer, &inner)),
            case["expect"],
            "{}",
            case["name"]
        );
    }
}

#[test]
fn selectors_read_claimed_severity_and_scope() {
    for case in cases(&fixtures()["selector"]) {
        let selector: Selector = serde_json::from_value(case["selector"].clone()).unwrap();
        selector.admit().unwrap();
        let got = selector.matches(&report(&case["report"]));
        assert_eq!(snake(got), case["expect"], "{}", case["name"]);
    }
    let empty = Selector {
        at_least: agent_provider_contract::incident::Severity::Notice,
        scopes: vec![],
    };
    assert_eq!(
        empty.admit().unwrap_err().reason,
        UnavailableReason::InvalidRecord
    );
}

#[test]
fn authorization_covers_only_releases_within_its_scope() {
    for case in cases(&fixtures()["coverage"]) {
        let authorization = authorization(&case["authorization"]);
        let request = match session_control::Record::decode(&case["request"]).unwrap() {
            session_control::Record::Request(request) => request,
            other => panic!("{other:?}"),
        };
        let got = authorization
            .covers(&request)
            .unwrap_or_else(|e| panic!("{}: {e}", case["name"]));
        assert_eq!(snake(got), case["expect"], "{}", case["name"]);
    }
}

#[test]
fn greater_epoch_supersedes_and_coverage_does_not_read_currency() {
    let all = fixtures();
    let case = &cases(&all["coverage"])[0];
    let authorization = authorization(&case["authorization"]);
    let current = IncidentRef {
        incident: authorization.incident.incident.clone(),
        epoch: authorization.incident.epoch + 1,
    };
    assert!(authorization.superseded_by(&current));
    assert!(!authorization.superseded_by(&authorization.incident));
    let other = IncidentRef {
        incident: "inc-other".into(),
        epoch: 99,
    };
    assert!(!authorization.superseded_by(&other));
    // A superseded record still reads covered: currency and fencing are
    // the host's check, not something an existing record grants or loses.
    let request = match session_control::Record::decode(&case["request"]).unwrap() {
        session_control::Record::Request(request) => request,
        other => panic!("{other:?}"),
    };
    assert_eq!(snake(authorization.covers(&request).unwrap()), "covered");
}

#[test]
fn selection_vectors_negotiate_versions_and_capabilities() {
    for case in cases(&fixtures()["selection"]) {
        let local: Offer = serde_json::from_value(case["local"].clone()).unwrap();
        let result = select(&local, &case["remote"]);
        match case["expect"].get("selected") {
            Some(selected) => {
                let got = result.unwrap_or_else(|e| panic!("{}: {e}", case["name"]));
                assert_eq!(&snake(&got), selected, "{}", case["name"]);
            }
            None => {
                let error = result.expect_err(case["name"].as_str().unwrap());
                assert_eq!(
                    snake(error.reason),
                    case["expect"]["unavailable"],
                    "{}",
                    case["name"]
                );
            }
        }
    }
    let local = Offer {
        records: vec![RecordKind::Report],
        evidence: vec![],
    };
    let oversized = json!({ PROTOCOL: {"records": ["report"], "evidence": []},
                            "x.pad/v1": "p".repeat(16_400) });
    assert_eq!(
        select(&local, &oversized).unwrap_err().reason,
        UnavailableReason::InvalidAdvertisement
    );
    let bad_local = Offer {
        records: vec![],
        evidence: vec![],
    };
    assert_eq!(
        select(
            &bad_local,
            &json!({PROTOCOL: {"records": ["report"], "evidence": []}})
        )
        .unwrap_err()
        .reason,
        UnavailableReason::InvalidAdvertisement
    );
}

#[test]
fn agreement_joins_records_and_evidence_with_the_selection() {
    for case in cases(&fixtures()["agreement"]) {
        let selected: Selected = serde_json::from_value(case["selected"].clone()).unwrap();
        let got = match Record::decode(&case["record"]).unwrap().agree(&selected) {
            Ok(()) => json!("agrees"),
            Err(error) => snake(error.reason),
        };
        assert_eq!(got, case["expect"], "{}", case["name"]);
    }
    let mut foreign: Selected =
        serde_json::from_value(cases(&fixtures()["agreement"])[0]["selected"].clone()).unwrap();
    foreign.protocol = session_control::PROTOCOL.into();
    assert_eq!(
        Record::decode(&oom())
            .unwrap()
            .agree(&foreign)
            .unwrap_err()
            .reason,
        UnavailableReason::ProtocolViolation
    );
}

#[test]
fn repetition_vectors_separate_retry_from_key_conflict() {
    for case in cases(&fixtures()["repetition"]) {
        let got = match case["of"].as_str().unwrap() {
            "report" => {
                classify_report_repetition(&report(&case["first"]), &report(&case["again"]))
            }
            "authorization" => classify_authorization_repetition(
                &authorization(&case["first"]),
                &authorization(&case["again"]),
            ),
            other => panic!("{other}"),
        };
        assert_eq!(snake(got), case["expect"], "{}", case["name"]);
    }
}

#[test]
fn receipt_and_conflict_answer_the_exact_submission() {
    let all = fixtures();
    let valid = &all["valid"]["Record"];
    let original = report(&oom());
    let Record::ReportReceipt(receipt) =
        Record::decode(&named(valid, "collector receipt")["value"]).unwrap()
    else {
        panic!("receipt")
    };
    assert!(receipt.answers(&original));
    let mut elsewhere = original.clone();
    elsewhere.reporter = "host:root-8/bash".into();
    assert!(!receipt.answers(&elsewhere));

    let Record::ReportConflict(conflict) = Record::decode(
        &named(valid, "report key conflict carrying both complete reports")["value"],
    )
    .unwrap() else {
        panic!("conflict")
    };
    assert_eq!(*conflict.original, original);
    assert_eq!(
        classify_report_repetition(&conflict.original, &conflict.submitted),
        Repetition::KeyConflict
    );
    conflict.answer_to(&conflict.submitted).unwrap();
    assert_eq!(
        conflict.answer_to(&original).unwrap_err().reason,
        UnavailableReason::ProtocolViolation
    );
}

#[test]
fn incident_records_are_not_control_records_or_provider_outcomes() {
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
    let diagnostic = snake(IncidentUnavailable::new(
        UnavailableReason::NoCommonVersion,
        "",
    ));
    for value in records.iter().copied().chain([&diagnostic]) {
        // No incident record or diagnostic is a session-control record or
        // control diagnostic: neither holds, releases nor acknowledges.
        assert!(session_control::Record::decode(value).is_err());
        assert!(session_control::validate(
            "ControlUnavailable",
            value,
            UnavailableReason::InvalidRecord
        )
        .is_err());
    }
    let contract = contract_fixtures();
    for row in LAUNCH_EVENT_ROWS {
        for value in records.iter().copied().chain([&diagnostic]) {
            assert!(
                registry.validate_launch_event(row.kind, value).is_err(),
                "{}",
                row.kind
            );
        }
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

#[test]
fn schema_imports_session_control_definitions_instead_of_copying_them() {
    let schema: Value =
        serde_json::from_str(agent_provider_contract::incident::SCHEMA_JSON).unwrap();
    let control: Value = serde_json::from_str(session_control::SCHEMA_JSON).unwrap();
    assert_eq!(
        schema["$schema"],
        "https://json-schema.org/draft/2020-12/schema"
    );
    let meta = jsonschema::meta::validator_for(&schema).unwrap();
    assert!(meta.is_valid(&schema));
    let own = schema["$defs"].as_object().unwrap();
    let imported = control["$defs"].as_object().unwrap();
    let mut refs = Vec::new();
    collect_refs(&schema, &mut refs);
    let mut used_imports = 0;
    for reference in refs {
        if let Some(name) = reference.strip_prefix("../session-control/v3.schema.json#/$defs/") {
            assert!(imported.contains_key(name), "{reference}");
            used_imports += 1;
        } else {
            let name = reference
                .strip_prefix("#/$defs/")
                .unwrap_or_else(|| panic!("{reference}"));
            assert!(own.contains_key(name), "{reference}");
        }
    }
    assert!(used_imports > 0);
    for shared in [
        "Authority",
        "LogicalRef",
        "DisclosedText",
        "DisclosedRef",
        "ActorEvidence",
        "HostRef",
        "RequestKey",
        "MissingReason",
    ] {
        assert!(!own.contains_key(shared), "{shared} is copied");
    }
}

fn collect_refs(value: &Value, refs: &mut Vec<String>) {
    match value {
        Value::Object(object) => {
            if let Some(Value::String(reference)) = object.get("$ref") {
                refs.push(reference.clone());
            }
            object.values().for_each(|child| collect_refs(child, refs));
        }
        Value::Array(values) => values.iter().for_each(|child| collect_refs(child, refs)),
        _ => {}
    }
}
