//! session_control/v2: structural schema versus semantic admission; one claim
//! ladder for hold/release, recover, cancel and close; refinement of the same
//! intent versus contradiction; prior acknowledgment versus a successor's
//! current knowledge; warranted versus arbitrary settlement composition;
//! descriptive discovery versus owner authority; hold/release pairing in
//! selection; redaction, bounds and diagnostics.
//!
//! These are deterministic contract checks over claims. They do not show that
//! any producer tells the truth, that a requester is authorized, that a
//! control is enforced or durable, that a lineage is warranted, or that any
//! actor is in custody.

pub mod support {
    pub mod contract_matrix;
}

use agent_provider_contract::session_control::{
    classify_repetition, read_settlement, select, validate, Authority, Basis, ControlScope,
    ControlState, ControlUnavailable, Fact, Lineage, LogicalReading, LogicalRef, Observation,
    Offer, Operation, Record, Relation, Repetition, Request, RequestTrace, RootEntry, Selected,
    SettlementReading, Step, UnavailableReason, MAX_RECORD_BYTES, MAX_TRACE_OUTCOMES,
    MAX_TRACE_RECEIPTS, PROTOCOL,
};
use agent_provider_contract::SchemaRegistry;
use serde_json::{json, Value};
use support::contract_matrix::{
    fixtures as contract_fixtures, launch_event_fixture, LAUNCH_EVENT_ROWS, NON_LAUNCH_ROWS,
};

fn fixtures() -> Value {
    serde_json::from_str(include_str!("fixtures/session_control/v2.json")).unwrap()
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

fn request(value: &Value) -> Request {
    match Record::decode(value).unwrap() {
        Record::Request(request) => request,
        other => panic!("not a request: {other:?}"),
    }
}

fn observations(value: &Value) -> Vec<Observation> {
    cases(value)
        .iter()
        .map(|value| match Record::decode(value).unwrap() {
            Record::Observation(observation) => observation,
            other => panic!("not an observation: {other:?}"),
        })
        .collect()
}

fn trace_of(case: &Value) -> RequestTrace {
    let mut trace = RequestTrace::new(request(&case["request"])).unwrap();
    for value in cases(&case["records"]) {
        trace.accept(&Record::decode(value).unwrap()).unwrap();
    }
    trace
}

fn hold() -> Request {
    request(&cases(&fixtures()["valid"]["Record"])[0]["value"])
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
    let successor_ack = named(
        &all["invalid_semantic"]["Record"],
        "hold acknowledgment by a successor: no past authority",
    );
    // Raw Serde accepts the representation; admission refuses the claim.
    let raw: Record = serde_json::from_value(successor_ack["value"].clone()).unwrap();
    assert!(Record::decode_line(&raw.encode_line()).is_err());
    let mut misaddressed = hold();
    misaddressed.scope = ControlScope {
        root: "root-8".into(),
        child: None,
    };
    assert_eq!(
        RequestTrace::new(misaddressed).unwrap_err().reason,
        UnavailableReason::InvalidRecord
    );
    let mut trace = RequestTrace::new(hold()).unwrap();
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

    // Maximal observation and maximal control state fit the line bound.
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
    let since =
        |key: String| json!({"request_key": key, "requester": host, "addressed": authority});
    let pending: Vec<Value> = (0..8)
        .map(|i| {
            json!({"request": since(format!("{i}{}", "k".repeat(127))),
                   "operation": "input_release", "status": "admitted"})
        })
        .collect();
    let state = json!({"kind": "control_state", "protocol": PROTOCOL, "reporter": authority,
        "scope": {"root": host, "child": host},
        "input": {"state": "input_held", "since": since("a".repeat(128))},
        "lifecycle": {"state": "cancelling", "since": since("b".repeat(128))},
        "pending": pending, "observed_at_unix_ms": 9_007_199_254_740_991u64});
    let line = Record::decode(&state).unwrap().encode_line();
    assert!(line.len() <= MAX_RECORD_BYTES, "{}", line.len());

    // The line bound applies before parsing.
    let padded = format!("{line}{}", " ".repeat(MAX_RECORD_BYTES - line.len()));
    assert!(Record::decode_line(&padded).is_ok());
    let error = Record::decode_line(&format!("{padded} ")).unwrap_err();
    assert!(error.detail.unwrap().contains("exceeds"));

    // Redacted and missing are distinct admitted states, and neither
    // carries the withheld value.
    let redacted = named(&all["valid"]["Record"], "request with redacted reason");
    let missing = named(&all["valid"]["Record"], "request with missing reason");
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
fn selection_vectors_negotiate_versions_and_pair_hold_with_release() {
    for case in cases(&fixtures()["selection"]) {
        let local: Offer = serde_json::from_value(case["local"].clone()).unwrap();
        let result = select(&local, &case["remote"]);
        match case["expect"].get("selected") {
            Some(selected) => {
                let got = result.unwrap_or_else(|e| panic!("{}: {e}", case["name"]));
                assert_eq!(
                    &serde_json::to_value(&got).unwrap(),
                    selected,
                    "{}",
                    case["name"]
                );
                // No selection ever carries a hold without its release.
                assert!(
                    !got.operations.contains(&Operation::InputHold)
                        || got.operations.contains(&Operation::InputRelease),
                    "{}",
                    case["name"]
                );
            }
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
    let oversized = json!({ PROTOCOL: {"operations": ["cancel"], "reports": [], "facts": []},
                            "x.pad/v1": "p".repeat(16_400) });
    let local = Offer {
        operations: vec![Operation::Cancel],
        reports: vec![],
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
            Record::RootEntry(entry) => entry.agree(&selected),
            Record::ControlState(state) => state.agree(&selected),
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
    assert_eq!(
        classify_repetition(&hold(), &hold()),
        Repetition::SameRequest
    );
}

#[test]
fn trace_vectors_keep_one_claim_ladder_and_separate_refinement_from_contradiction() {
    for case in cases(&fixtures()["traces"]) {
        let name = case["name"].as_str().unwrap();
        let mut trace = RequestTrace::new(request(&case["request"])).unwrap();
        let records = cases(&case["records"]);
        let expect = cases(&case["expect"]);
        assert_eq!(records.len(), expect.len(), "{name}");
        for (index, (value, expected)) in records.iter().zip(expect).enumerate() {
            if expected == &json!({"invalid_record": true}) {
                assert!(Record::decode(value).is_err(), "{name} {index}");
                continue;
            }
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
fn prior_acknowledgment_survives_successor_knowledge() {
    let all = fixtures();
    let case = named(
        &all["traces"],
        "prior ACK survives owner death; the successor reports knowledge and later confirms it",
    );
    let mut trace = RequestTrace::new(request(&case["request"])).unwrap();
    let records = cases(&case["records"]);
    for value in records.iter().take(3) {
        trace.accept(&Record::decode(value).unwrap()).unwrap();
    }
    let ack = trace
        .acknowledgment()
        .expect("uncertainty did not erase the ACK")
        .clone();
    assert_eq!(ack.responder, request(&case["request"]).addressed);
    assert!(trace.outcome().unwrap().uncertainty.is_some());
    trace.accept(&Record::decode(&records[3]).unwrap()).unwrap();
    assert_eq!(trace.outcomes().len(), 2, "earlier uncertainty is retained");
    assert_eq!(
        trace.acknowledgment().unwrap().responder,
        ack.responder,
        "the successor's report did not take over the acknowledgment"
    );
}

#[test]
fn outcome_knowledge_is_bounded_per_trace() {
    let request = hold();
    let mut trace = RequestTrace::new(request.clone()).unwrap();
    for at in 0..=MAX_TRACE_OUTCOMES as u64 {
        let outcome = json!({"kind": "outcome", "protocol": PROTOCOL,
            "request_key": request.request_key, "requester": request.requester,
            "addressed": request.addressed, "result": "unknown",
            "uncertainty": "evidence_unavailable", "observed_at_unix_ms": at});
        let result = trace.accept(&Record::decode(&outcome).unwrap());
        assert_eq!(
            result.is_ok(),
            at < MAX_TRACE_OUTCOMES as u64,
            "outcome {at}"
        );
    }
}

#[test]
fn receipts_are_bounded_per_trace() {
    let request = hold();
    let mut trace = RequestTrace::new(request.clone()).unwrap();
    for at in 0..=MAX_TRACE_RECEIPTS as u64 {
        let receipt = json!({"kind": "receipt", "protocol": PROTOCOL,
            "request_key": request.request_key, "requester": request.requester,
            "addressed": request.addressed, "durable": false, "observed_at_unix_ms": at});
        let result = trace.accept(&Record::decode(&receipt).unwrap());
        assert_eq!(
            result.is_ok(),
            at < MAX_TRACE_RECEIPTS as u64,
            "receipt {at}"
        );
    }
}

#[test]
fn current_state_relates_to_prior_acknowledgment_without_erasing_it() {
    for case in cases(&fixtures()["relations"]) {
        let trace = trace_of(case);
        let state = match Record::decode(&case["state"]).unwrap() {
            Record::ControlState(state) => state,
            other => panic!("not a control state: {other:?}"),
        };
        let relation: Relation = trace.relate(&state);
        assert_eq!(
            serde_json::to_value(relation).unwrap(),
            case["expect"],
            "{}",
            case["name"]
        );
        // Relating reads; it never changes the trace's claims.
        assert_eq!(trace, trace_of(case), "{}", case["name"]);
    }
}

#[test]
fn settlement_reads_one_warranted_evolving_account() {
    for case in cases(&fixtures()["settlement"]) {
        let name = case["name"].as_str().unwrap();
        let subject: LogicalRef = serde_json::from_value(case["subject"].clone()).unwrap();
        let lineage: Option<Lineage> = case
            .get("lineage")
            .map(|value| serde_json::from_value(value.clone()).unwrap());
        let observations = observations(&case["observations"]);
        let reading: SettlementReading = read_settlement(&subject, lineage.as_ref(), &observations);
        assert_eq!(
            serde_json::to_value(reading).unwrap(),
            case["expect"],
            "{name}"
        );
        // Report order does not change the reading.
        let mut reversed = observations.clone();
        reversed.reverse();
        assert_eq!(
            read_settlement(&subject, lineage.as_ref(), &reversed),
            reading,
            "{name}"
        );
        // Physical custody never changes the logical reading.
        let logical_only: Vec<Observation> = observations
            .iter()
            .filter(|o| !matches!(o.fact, Fact::PhysicalCustody { .. }))
            .cloned()
            .collect();
        assert_eq!(
            read_settlement(&subject, lineage.as_ref(), &logical_only).logical,
            reading.logical,
            "{name}"
        );
        // Only a lineage for the subject's root warrants a reading.
        assert_eq!(
            reading.basis == Basis::Warranted,
            lineage.as_ref().is_some_and(|l| l.root == subject.root),
            "{name}"
        );
    }
}

#[test]
fn actual_u112_ack_without_end_stays_owed_after_physical_wait() {
    let all = fixtures();
    let case = named(
        &all["settlement"],
        "actual U112 records: ACK, no tagged end, async debt owed, harness exit7 waited stays owed",
    );
    assert!(case["provenance"]
        .as_str()
        .unwrap()
        .contains("completed historical data"));
    let subject: LogicalRef = serde_json::from_value(case["subject"].clone()).unwrap();
    let lineage: Lineage = serde_json::from_value(case["lineage"].clone()).unwrap();
    let observations = observations(&case["observations"]);
    let reading = read_settlement(&subject, Some(&lineage), &observations);
    assert_eq!(reading.logical, LogicalReading::Owed);
    // The physical exit and wait remain a separate, reported fact.
    let physical = serde_json::to_value(reading.physical_custody).unwrap();
    assert_eq!(
        physical,
        json!({"reading": "reported", "state": "exited_waited"})
    );
    // Without the exit the logical reading is the same; the exit adds nothing.
    let before_exit: Vec<Observation> = observations
        .iter()
        .filter(|o| !matches!(o.fact, Fact::PhysicalCustody { .. }))
        .cloned()
        .collect();
    assert_eq!(
        read_settlement(&subject, Some(&lineage), &before_exit).logical,
        LogicalReading::Owed
    );
}

#[test]
fn discovery_is_descriptive_addressing_not_owner_authority() {
    let all = fixtures();
    let entry = match Record::decode(
        &named(
            &all["valid"]["Record"],
            "discovery entry for the requester's root",
        )["value"],
    )
    .unwrap()
    {
        Record::RootEntry(entry) => entry,
        other => panic!("not a root entry: {other:?}"),
    };
    // A discovery entry answers no request, even when its values coincide
    // with the request's key, requester and addressed authority.
    let request = hold();
    let colliding = RootEntry {
        describer: request.request_key.clone(),
        requester: request.requester.clone(),
        authority: request.addressed.clone(),
        ..entry.clone()
    };
    for candidate in [&entry, &colliding] {
        let mut trace = RequestTrace::new(request.clone()).unwrap();
        let before = trace.clone();
        assert_eq!(
            trace
                .accept(&Record::RootEntry(candidate.clone()))
                .unwrap_err()
                .reason,
            UnavailableReason::ProtocolViolation
        );
        assert_eq!(trace, before);
    }
    // Addressing a request from the entry is only an address: the owner may
    // answer that the described authority is stale.
    let addressed = Request {
        request_key: "k-from-discovery".into(),
        addressed: entry.authority.clone(),
        ..hold()
    };
    let successor = Authority {
        owner: "owner-b".into(),
        generation: "g13".into(),
        ..entry.authority.clone()
    };
    let mut trace = RequestTrace::new(addressed.clone()).unwrap();
    let stale = json!({"kind": "refusal", "protocol": PROTOCOL,
        "request_key": addressed.request_key, "requester": addressed.requester,
        "addressed": addressed.addressed, "operation": "input_hold", "stage": "admission",
        "reason": "stale_authority", "responder": successor, "observed_at_unix_ms": 1});
    assert!(matches!(
        trace.accept(&Record::decode(&stale).unwrap()).unwrap(),
        Step::Refused { .. }
    ));
    // The describer is a locator, never an authority, and cannot be one.
    let mut as_authority = serde_json::to_value(Record::RootEntry(entry.clone())).unwrap();
    as_authority["describer"] = serde_json::to_value(&entry.authority).unwrap();
    assert!(Record::decode(&as_authority).is_err());
    // Scheduling, admission and ownership claims have no place in an entry.
    for field in [
        "capacity",
        "reservation",
        "admitted",
        "owner_of_record",
        "schedule",
    ] {
        let mut extended = serde_json::to_value(Record::RootEntry(entry.clone())).unwrap();
        extended[field] = json!(1);
        assert!(Record::decode(&extended).is_err(), "{field}");
    }
    // A root entry is not a current-state report: inspection comes only from
    // a root authority's control state.
    let _: fn(&RootEntry, &Selected) -> Result<(), ControlUnavailable> = RootEntry::agree;
    let _: fn(&ControlState, &Selected) -> Result<(), ControlUnavailable> = ControlState::agree;
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
