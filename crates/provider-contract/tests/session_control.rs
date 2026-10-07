//! session_control/v3: structural schema versus semantic admission; one claim
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
    serde_json::from_str(include_str!("fixtures/session_control/v3.json")).unwrap()
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
    // Raw exit/wait is retained; this historical mapping supplies no actor
    // reference for its physical fact, so it cannot yield a correlated summary.
    let physical = serde_json::to_value(reading.physical_custody).unwrap();
    assert_eq!(physical, json!({"reading": "conflicting"}));
    assert!(observations.iter().any(|o| matches!(
        o.fact,
        Fact::PhysicalCustody {
            state: agent_provider_contract::session_control::PhysicalCustodyState::ExitedWaited,
            ..
        }
    )));
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

// ROOT D1-B/D2-i: authored claim encounters, not executed owner death/actors.
fn control_claim(request: &Request, kind: &str) -> Value {
    json!({"kind": kind, "protocol": PROTOCOL, "request_key": request.request_key,
        "requester": request.requester, "addressed": request.addressed})
}

fn inherited_claims(request: &Request) -> (Record, Record, Record) {
    let mut admission = control_claim(request, "admission");
    admission["operation"] = json!(request.operation);
    admission["responder"] = json!(request.addressed);
    admission["observed_at_unix_ms"] = json!(1);
    let mut successor = request.addressed.clone();
    successor.owner = "successor".into();
    successor.generation = "later-generation-claim".into();
    let mut fulfillment = control_claim(request, "fulfillment");
    fulfillment["operation"] = json!(request.operation);
    fulfillment["reporter"] = json!(successor);
    fulfillment["from"] = json!("unknown");
    fulfillment["to"] = json!(request.operation.target());
    fulfillment["observed_at_unix_ms"] = json!(2);
    let mut ack = control_claim(request, "acknowledgment");
    ack["operation"] = json!(request.operation);
    ack["responder"] = json!(request.addressed);
    ack["from"] = json!("unknown");
    ack["to"] = json!(request.operation.target());
    ack["observed_at_unix_ms"] = json!(1);
    (
        Record::decode(&admission).unwrap(),
        Record::decode(&fulfillment).unwrap(),
        Record::decode(&ack).unwrap(),
    )
}

fn knowledge(request: &Request, result: &str, at: u64) -> Record {
    let mut value = control_claim(request, "outcome");
    value["result"] = json!(result);
    value["observed_at_unix_ms"] = json!(at);
    if result == "unknown" {
        value["uncertainty"] = json!("evidence_unavailable");
    }
    Record::decode(&value).unwrap()
}

fn rejected_unchanged(trace: &mut RequestTrace, record: &Record) {
    let before = trace.clone();
    assert!(trace.accept(record).is_err());
    assert_eq!(*trace, before);
}

#[test]
fn correction_final_knowledge_survives_full_unknown_history() {
    for final_result in ["acknowledged", "refused", "fulfilled"] {
        let req = hold();
        let mut trace = RequestTrace::new(req.clone()).unwrap();
        let (admit, fulfilled, ack) = inherited_claims(&req);
        for at in 0..MAX_TRACE_OUTCOMES as u64 {
            let unknown = knowledge(&req, "unknown", at);
            trace.accept(&unknown).unwrap();
            assert_eq!(trace.accept(&unknown).unwrap(), Step::Duplicate);
        }
        rejected_unchanged(&mut trace, &knowledge(&req, "unknown", 99));
        match final_result {
            "acknowledged" => {
                trace.accept(&admit).unwrap();
                trace.accept(&ack).unwrap();
            }
            "fulfilled" => {
                trace.accept(&admit).unwrap();
                trace.accept(&fulfilled).unwrap();
            }
            _ => {
                let mut refusal = control_claim(&req, "refusal");
                refusal["operation"] = json!(req.operation);
                refusal["stage"] = json!("admission");
                refusal["reason"] = json!("not_permitted");
                refusal["observed_at_unix_ms"] = json!(100);
                trace.accept(&Record::decode(&refusal).unwrap()).unwrap();
            }
        }
        let final_record = knowledge(&req, final_result, 101);
        assert_eq!(
            trace.accept(&final_record).unwrap(),
            Step::Concluded {
                result: serde_json::from_value(json!(final_result)).unwrap(),
                refines: true
            }
        );
        assert_eq!(trace.outcomes().len(), MAX_TRACE_OUTCOMES + 1);
        assert_eq!(trace.accept(&final_record).unwrap(), Step::Duplicate);
        assert_eq!(
            trace.accept(&knowledge(&req, "unknown", 0)).unwrap(),
            Step::Duplicate
        );
        rejected_unchanged(&mut trace, &knowledge(&req, "unknown", 102));
        rejected_unchanged(&mut trace, &knowledge(&req, final_result, 103));
        rejected_unchanged(
            &mut trace,
            &knowledge(
                &req,
                if final_result == "refused" {
                    "acknowledged"
                } else {
                    "refused"
                },
                104,
            ),
        );
    }
}

#[test]
fn correction_successor_fulfills_original_admitted_intent_without_predecessor_ack() {
    for operation in [
        Operation::InputHold,
        Operation::InputRelease,
        Operation::Close,
        Operation::Cancel,
    ] {
        let req = Request {
            operation,
            ..hold()
        };
        let (admit, fulfilled, ack) = inherited_claims(&req);
        let mut trace = RequestTrace::new(req.clone()).unwrap();
        rejected_unchanged(&mut trace, &fulfilled); // retention alone is not admission
        rejected_unchanged(&mut trace, &knowledge(&req, "fulfilled", 0));
        let mut receipt = control_claim(&req, "receipt");
        receipt["durable"] = json!(true);
        receipt["observed_at_unix_ms"] = json!(0);
        trace.accept(&Record::decode(&receipt).unwrap()).unwrap();
        trace.accept(&admit).unwrap();
        trace.accept(&knowledge(&req, "unknown", 1)).unwrap();
        rejected_unchanged(&mut trace, &knowledge(&req, "acknowledged", 2));
        trace.accept(&fulfilled).unwrap();
        assert!(
            trace.acknowledgment().is_none(),
            "no predecessor ACK was manufactured"
        );
        assert_eq!(
            trace.admission(),
            match &admit {
                Record::Admission(a) => Some(a),
                _ => unreachable!(),
            }
        );
        assert_eq!(trace.request().reference(), req.reference());
        assert_eq!(trace.accept(&fulfilled).unwrap(), Step::Duplicate);
        let mut restamped = serde_json::to_value(&fulfilled).unwrap();
        restamped["observed_at_unix_ms"] = json!(99);
        rejected_unchanged(&mut trace, &Record::decode(&restamped).unwrap());
        let mut refusal = control_claim(&req, "refusal");
        refusal["operation"] = json!(operation);
        refusal["stage"] = json!("transition");
        refusal["reason"] = json!("transition_failed");
        refusal["responder"] = json!(req.addressed);
        refusal["observed_at_unix_ms"] = json!(3);
        rejected_unchanged(&mut trace, &Record::decode(&refusal).unwrap());
        let mut state_value = json!({"kind":"control_state", "protocol":PROTOCOL,
            "reporter":trace.fulfillment().unwrap().reporter, "scope":req.scope,
            "input":{"state":"unknown"}, "lifecycle":{"state":"unknown"}, "observed_at_unix_ms":3});
        let domain = if matches!(operation, Operation::Close | Operation::Cancel) {
            "lifecycle"
        } else {
            "input"
        };
        state_value[domain] = json!({"state":operation.target(),"since":req.reference()});
        let state = |v: &Value| match Record::decode(v).unwrap() {
            Record::ControlState(s) => s,
            _ => unreachable!(),
        };
        assert_eq!(trace.relate(&state(&state_value)), Relation::Current);
        state_value[domain]["state"] = json!("unknown");
        assert_eq!(trace.relate(&state(&state_value)), Relation::PriorRetained);
        state_value[domain]["state"] = json!(if domain == "lifecycle" {
            "open"
        } else if operation == Operation::InputHold {
            "input_open"
        } else {
            "input_held"
        });
        assert_eq!(trace.relate(&state(&state_value)), Relation::Contradicts);
        state_value[domain]["state"] = json!(operation.target());
        state_value["pending"] =
            json!([{"request":req.reference(),"operation":operation,"status":"admitted"}]);
        assert_eq!(trace.relate(&state(&state_value)), Relation::Contradicts);
        let mut no_ack_final = trace.clone();
        no_ack_final
            .accept(&knowledge(&req, "fulfilled", 4))
            .unwrap();
        assert!(no_ack_final.acknowledgment().is_none());
        assert_eq!(no_ack_final.request().reference(), req.reference());
        assert_eq!(no_ack_final.admission(), trace.admission());
        // Faithful prior positive evidence can be retained alongside own fulfillment.
        trace.accept(&ack).unwrap();
        assert_eq!(trace.acknowledgment().unwrap().responder, req.addressed);
        trace.accept(&knowledge(&req, "fulfilled", 4)).unwrap();
        assert_eq!(trace.accept(&admit).unwrap(), Step::Duplicate);
        assert_eq!(trace.accept(&ack).unwrap(), Step::Duplicate);
        assert!(trace.fulfillment().is_some());
        let mut readdressed = req.clone();
        readdressed.addressed = trace.fulfillment().unwrap().reporter.clone();
        assert_eq!(
            classify_repetition(&req, &readdressed),
            Repetition::KeyConflict
        );
    }
}

#[test]
fn correction_cancel_precedence_is_shared_by_ack_fulfillment_and_relation() {
    for kind in ["acknowledgment", "fulfillment"] {
        for (operation, from, accepted) in [
            (Operation::Close, "cancelling", false),
            (Operation::Close, "closing", true),
            (Operation::Close, "open", true),
            (Operation::Close, "unknown", true),
            (Operation::Cancel, "closing", true),
            (Operation::Cancel, "cancelling", true),
        ] {
            let req = Request {
                operation,
                ..hold()
            };
            let (admit, fulfilled, ack) = inherited_claims(&req);
            let base = if kind == "fulfillment" {
                fulfilled
            } else {
                ack
            };
            let mut v = serde_json::to_value(base).unwrap();
            v["from"] = json!(from);
            validate("Record", &v, UnavailableReason::InvalidRecord).unwrap();
            assert_eq!(Record::decode(&v).is_ok(), accepted);
            let raw: Record = serde_json::from_value(v).unwrap();
            let mut trace = RequestTrace::new(req).unwrap();
            trace.accept(&admit).unwrap();
            if accepted {
                trace.accept(&raw).unwrap();
            } else {
                rejected_unchanged(&mut trace, &raw);
            }
        }
    }
    let req = Request {
        operation: Operation::Cancel,
        ..hold()
    };
    let (admit, _, ack) = inherited_claims(&req);
    let mut trace = RequestTrace::new(req.clone()).unwrap();
    trace.accept(&admit).unwrap();
    trace.accept(&ack).unwrap();
    let mut other = req.reference();
    other.request_key = "other-close-claim".into();
    let v = json!({"kind":"control_state","protocol":PROTOCOL,"reporter":req.addressed,"scope":req.scope,
        "input":{"state":"unknown"},"lifecycle":{"state":"closing","since":other},"observed_at_unix_ms":3});
    let Record::ControlState(state) = Record::decode(&v).unwrap() else {
        unreachable!()
    };
    assert_eq!(trace.relate(&state), Relation::Contradicts);
}

#[test]
fn correction_physical_wait_refines_one_actor_and_never_discharges_multiple_actors() {
    use agent_provider_contract::session_control::{FactReading, PhysicalCustodyState};
    let subject: LogicalRef =
        serde_json::from_value(json!({"root":"root-7","input":"input-1"})).unwrap();
    let reporter = hold().addressed;
    let physical = |actor: &str, state: &str, at: u64| {
        let v = json!({"kind":"observation","protocol":PROTOCOL,"reporter":reporter,"subject":subject,
            "fact":{"type":"physical_custody","state":state},"evidence":[{"actor":"os_process","exactness":"exact",
            "ref":{"state":"present","ref":actor}}],"observed_at_unix_ms":at});
        match Record::decode(&v).unwrap() {
            Record::Observation(o) => o,
            _ => unreachable!(),
        }
    };
    let a_waited = physical("actor-A/incarnation-1", "exited_waited", 1);
    let b_live = physical("actor-B/incarnation-2", "live", 2);
    let lineage = Lineage {
        root: subject.root.clone(),
        authorities: vec![reporter.clone()],
    };
    for pair in [
        vec![a_waited.clone(), b_live.clone()],
        vec![b_live.clone(), a_waited.clone()],
    ] {
        let r = read_settlement(&subject, Some(&lineage), &pair);
        assert_eq!(r.physical_custody, FactReading::Conflicting);
        assert_eq!(r.logical, LogicalReading::Unknown);
    }
    let same = vec![
        physical("actor-A/incarnation-1", "unsettled", 3),
        a_waited.clone(),
    ];
    assert_eq!(
        read_settlement(&subject, Some(&lineage), &same).physical_custody,
        FactReading::Reported(PhysicalCustodyState::ExitedWaited)
    );
    let mut other_reporter = a_waited.clone();
    other_reporter.reporter.owner = "successor".into();
    let joined = Lineage {
        root: subject.root.clone(),
        authorities: vec![reporter.clone(), other_reporter.reporter.clone()],
    };
    assert_eq!(
        read_settlement(&subject, Some(&joined), &[a_waited.clone(), other_reporter])
            .physical_custody,
        FactReading::Reported(PhysicalCustodyState::ExitedWaited)
    ); // reporter is not actor
    for exactness in ["legacy", "incomplete"] {
        let mut v = serde_json::to_value(Record::Observation(a_waited.clone())).unwrap();
        v["evidence"][0]["exactness"] = json!(exactness);
        let Record::Observation(o) = Record::decode(&v).unwrap() else {
            unreachable!()
        };
        assert_eq!(
            read_settlement(&subject, Some(&lineage), &[o]).physical_custody,
            FactReading::Conflicting
        );
    }
    let mut anonymous = a_waited.clone();
    anonymous.evidence.clear();
    assert_eq!(
        read_settlement(&subject, Some(&lineage), &[anonymous]).physical_custody,
        FactReading::Conflicting
    );
    let mut multiple = a_waited.clone();
    multiple.evidence.extend(b_live.evidence.clone());
    assert_eq!(
        read_settlement(&subject, Some(&lineage), &[multiple]).physical_custody,
        FactReading::Conflicting
    );
    let mut cross_root = b_live.clone();
    cross_root.reporter.root = "root-8".into();
    let r = read_settlement(&subject, Some(&lineage), &[a_waited, cross_root]);
    assert_eq!(
        r.physical_custody,
        FactReading::Reported(PhysicalCustodyState::ExitedWaited)
    );
    assert_eq!(r.excluded, 1);
    // Fully settled logical claims still do not repair another live actor's custody.
    let mut logical = Vec::new();
    for (kind, state) in [
        ("insertion", "acknowledged"),
        ("tagged_end", "observed"),
        ("logical_settlement", "settled"),
    ] {
        let mut v = serde_json::to_value(Record::Observation(b_live.clone())).unwrap();
        v["fact"] = json!({"type":kind,"state":state});
        let Record::Observation(o) = Record::decode(&v).unwrap() else {
            unreachable!()
        };
        logical.push(o);
    }
    logical.extend([
        physical("actor-A/incarnation-1", "exited_waited", 1),
        b_live,
    ]);
    let r = read_settlement(&subject, Some(&lineage), &logical);
    assert_eq!(r.logical, LogicalReading::Settled);
    assert_eq!(r.physical_custody, FactReading::Conflicting);
}

#[test]
fn correction_fulfillment_retains_known_prior_positive_and_negative_claims() {
    let req = hold();
    let (admit, fulfilled, ack) = inherited_claims(&req);
    let mut trace = RequestTrace::new(req.clone()).unwrap();
    trace.accept(&admit).unwrap();
    trace.accept(&ack).unwrap();
    trace.accept(&knowledge(&req, "unknown", 1)).unwrap();
    let prior = trace.acknowledgment().unwrap().clone();
    trace.accept(&fulfilled).unwrap();
    assert_eq!(trace.acknowledgment(), Some(&prior));
    assert_eq!(trace.accept(&ack).unwrap(), Step::Duplicate);
    let mut other = serde_json::to_value(&fulfilled).unwrap();
    other["request_key"] = json!("another-intent");
    rejected_unchanged(&mut trace, &Record::decode(&other).unwrap());
    other = serde_json::to_value(&fulfilled).unwrap();
    other["operation"] = json!("input_release");
    other["to"] = json!("input_open");
    rejected_unchanged(&mut trace, &Record::decode(&other).unwrap());
    let mut refused = RequestTrace::new(req.clone()).unwrap();
    refused.accept(&admit).unwrap();
    let mut negative = control_claim(&req, "refusal");
    negative["operation"] = json!(req.operation);
    negative["stage"] = json!("transition");
    negative["reason"] = json!("transition_failed");
    negative["responder"] = json!(req.addressed);
    negative["observed_at_unix_ms"] = json!(3);
    refused.accept(&Record::decode(&negative).unwrap()).unwrap();
    rejected_unchanged(&mut refused, &fulfilled);
    rejected_unchanged(&mut refused, &knowledge(&req, "fulfilled", 4));
    assert!(refused.refusal().is_some());
    assert!(refused.fulfillment().is_none());
}

fn successor_negative(request: &Request) -> Record {
    let (_, fulfillment, _) = inherited_claims(request);
    let mut value = serde_json::to_value(fulfillment).unwrap();
    value["kind"] = json!("non_fulfillment");
    value.as_object_mut().unwrap().remove("from");
    value.as_object_mut().unwrap().remove("to");
    value["reason"] = json!("already_terminal");
    Record::decode(&value).unwrap()
}

#[test]
fn resolution_inherited_close_after_cancel_has_attributed_terminal_knowledge() {
    let mut original = hold();
    original.operation = Operation::Close;
    let (admission, _, _) = inherited_claims(&original);
    let negative = successor_negative(&original);
    let mut trace = RequestTrace::new(original.clone()).unwrap();
    trace.accept(&admission).unwrap();
    for at in 0..MAX_TRACE_OUTCOMES {
        trace
            .accept(&knowledge(&original, "unknown", at as u64))
            .unwrap();
    }
    rejected_unchanged(&mut trace, &knowledge(&original, "unknown", 100));
    assert!(matches!(
        trace.accept(&negative).unwrap(),
        Step::Unfulfilled { .. }
    ));
    trace
        .accept(&knowledge(&original, "unfulfilled", 101))
        .unwrap();
    assert_eq!(trace.request(), &original);
    assert_eq!(
        trace.admission(),
        match &admission {
            Record::Admission(a) => Some(a),
            _ => unreachable!(),
        }
    );
    assert!(trace.acknowledgment().is_none());
    assert!(trace.fulfillment().is_none());
    assert!(trace.refusal().is_none());
    assert!(trace.non_fulfillment().is_some());
    assert_eq!(trace.outcomes().len(), MAX_TRACE_OUTCOMES + 1);
    let final_trace = trace.clone();
    assert_eq!(trace.accept(&negative).unwrap(), Step::Duplicate);
    assert_eq!(trace, final_trace);

    // The constructed actual C-O2n case: D sees another cancel, not a close.
    let mut state = json!({"kind":"control_state", "protocol":PROTOCOL,
        "reporter":serde_json::to_value(&negative).unwrap()["reporter"],
        "scope":original.scope, "input":{"state":"unknown"},
        "lifecycle":{"state":"cancelling", "since":original.reference()},
        "observed_at_unix_ms":102});
    state["lifecycle"]["since"]["request_key"] = json!("another-admitted-cancel");
    let report = match Record::decode(&state).unwrap() {
        Record::ControlState(s) => s,
        _ => unreachable!(),
    };
    assert_eq!(trace.relate(&report), Relation::NoAcknowledgment);
    let mut incoherent = report.clone();
    incoherent
        .pending
        .push(agent_provider_contract::session_control::Pending {
            request: original.reference(),
            operation: Operation::Close,
            status: agent_provider_contract::session_control::PendingStatus::Admitted,
        });
    assert_eq!(trace.relate(&incoherent), Relation::Contradicts);
    assert_eq!(trace, final_trace);
}

#[test]
fn resolution_negative_needs_original_admission_and_exact_successor_scope() {
    let original = hold();
    let (admission, _, _) = inherited_claims(&original);
    let negative = successor_negative(&original);
    let mut trace = RequestTrace::new(original.clone()).unwrap();
    rejected_unchanged(&mut trace, &negative);
    rejected_unchanged(&mut trace, &knowledge(&original, "unfulfilled", 3));
    trace.accept(&admission).unwrap();
    for field in ["root", "incarnation"] {
        let mut value = serde_json::to_value(&negative).unwrap();
        value["reporter"][field] = json!("other");
        assert!(Record::decode(&value).is_err());
        let raw = serde_json::from_value(value).unwrap();
        rejected_unchanged(&mut trace, &raw);
    }
    let mut value = serde_json::to_value(&negative).unwrap();
    value["reporter"] = json!(original.addressed);
    assert!(Record::decode(&value).is_err());
    value.as_object_mut().unwrap().remove("reporter");
    assert!(Record::decode(&value).is_err());
    let mut value = serde_json::to_value(&negative).unwrap();
    value["operation"] = json!("input_release");
    rejected_unchanged(&mut trace, &Record::decode(&value).unwrap());
    value["operation"] = json!("recover");
    assert!(Record::decode(&value).is_err());
    for reason in ["already_terminal", "root_absent", "transition_failed"] {
        let mut value = serde_json::to_value(&negative).unwrap();
        value["reason"] = json!(reason);
        let mut independent = trace.clone();
        independent
            .accept(&Record::decode(&value).unwrap())
            .unwrap();
    }
}

#[test]
fn resolution_anonymous_terminal_refusal_cannot_resolve_admitted_intent() {
    let original = hold();
    let (admission, _, _) = inherited_claims(&original);
    let mut trace = RequestTrace::new(original.clone()).unwrap();
    trace.accept(&admission).unwrap();
    let mut value = control_claim(&original, "refusal");
    value["operation"] = json!(original.operation);
    value["stage"] = json!("transition");
    value["reason"] = json!("transition_failed");
    value["observed_at_unix_ms"] = json!(2);
    assert!(validate("Record", &value, UnavailableReason::InvalidRecord).is_ok());
    assert!(Record::decode(&value).is_err());
    let raw: Record = serde_json::from_value(value.clone()).unwrap();
    rejected_unchanged(&mut trace, &raw);
    value["responder"] = json!(original.addressed);
    trace.accept(&Record::decode(&value).unwrap()).unwrap();
    trace.accept(&knowledge(&original, "refused", 3)).unwrap();
}

#[test]
fn resolution_known_positive_survives_negative_and_late_positive_is_contradiction() {
    let original = hold();
    let (admission, fulfillment, ack) = inherited_claims(&original);
    let negative = successor_negative(&original);
    for positive in [&ack, &fulfillment] {
        let mut trace = RequestTrace::new(original.clone()).unwrap();
        trace.accept(&admission).unwrap();
        trace.accept(positive).unwrap();
        rejected_unchanged(&mut trace, &negative);
        assert_eq!(trace.accept(positive).unwrap(), Step::Duplicate);
    }
    let mut trace = RequestTrace::new(original.clone()).unwrap();
    trace.accept(&admission).unwrap();
    trace.accept(&negative).unwrap();
    for finalized in [false, true] {
        if finalized {
            trace
                .accept(&knowledge(&original, "unfulfilled", 3))
                .unwrap();
        }
        for positive in [
            &ack,
            &fulfillment,
            &knowledge(&original, "acknowledged", 4),
            &knowledge(&original, "fulfilled", 5),
        ] {
            let before = trace.clone();
            let error = trace.accept(positive).unwrap_err();
            assert_eq!(error.reason, UnavailableReason::ProtocolViolation);
            assert!(error.detail.unwrap().contains("contradict"));
            assert_eq!(trace, before);
        }
    }
}

fn r3_conflict_fixture() -> Value {
    serde_json::from_str(include_str!(
        "fixtures/session_control/successor-resolution.json"
    ))
    .unwrap()
}

#[test]
fn resolution_actual_r3_conflict_joins_current_submission_without_erasing_final_original() {
    let f = r3_conflict_fixture();
    let original: Request = serde_json::from_value(f["original"].clone()).unwrap();
    let submitted: Request = serde_json::from_value(f["submitted"].clone()).unwrap();
    let conflict = Record::decode(&f["conflict"]).unwrap();
    let answer = match &conflict {
        Record::Conflict(c) => c,
        _ => unreachable!(),
    };
    answer.answer_to(&submitted).unwrap();
    assert!(answer.answer_to(&original).is_err());
    assert!(conflict.correlation().is_none());
    assert_eq!(
        classify_repetition(&original, &submitted),
        Repetition::KeyConflict
    );
    let mut trace = RequestTrace::new(original.clone()).unwrap();
    for claim in cases(&f["original_claims"]) {
        trace.accept(&Record::decode(claim).unwrap()).unwrap();
    }
    assert_eq!(
        trace.outcome().unwrap().result,
        agent_provider_contract::session_control::OutcomeResult::Acknowledged
    );
    let before = trace.clone();
    for _ in 0..2 {
        assert_eq!(
            trace.accept(&conflict).unwrap(),
            Step::SubmissionConflict {
                submitted: Box::new(submitted.clone())
            }
        );
        assert_eq!(trace, before);
    }
    // Faithful original replay still means the original hold, not release.
    for claim in cases(&f["original_claims"]) {
        assert_eq!(
            trace.accept(&Record::decode(claim).unwrap()).unwrap(),
            Step::Duplicate
        );
        assert_eq!(trace, before);
    }
    rejected_unchanged(&mut trace, &Record::Request(submitted.clone()));
    for change in ["reason", "scope", "addressed"] {
        let mut changed = original.clone();
        match change {
            "reason" => {
                changed.reason =
                    Some(agent_provider_contract::session_control::DisclosedText::Redacted)
            }
            "scope" => changed.scope.child = Some("child".into()),
            _ => changed.addressed.generation = "new-generation".into(),
        }
        let mut current = answer.clone();
        current.submitted = changed.clone();
        current.answer_to(&changed).unwrap();
        trace.accept(&Record::Conflict(current)).unwrap();
        assert_eq!(trace, before);
    }
    let mut mismatch = answer.clone();
    mismatch.original.reason =
        Some(agent_provider_contract::session_control::DisclosedText::Redacted);
    rejected_unchanged(&mut trace, &Record::Conflict(mismatch));
    let mut raw = f["conflict"].clone();
    raw["submitted"] = f["original"].clone();
    assert!(Record::decode(&raw).is_err());
    let mut raw = f["conflict"].clone();
    raw["responder"]["root"] = json!("another-root");
    assert!(Record::decode(&raw).is_err());
}

#[test]
fn resolution_v3_selection_refuses_v2_only_and_preserves_control_only_degradation() {
    let local = Offer {
        operations: vec![Operation::Cancel],
        reports: vec![],
        facts: vec![],
    };
    assert_eq!(
        agent_provider_contract::session_control::SUPPORTED_VERSIONS,
        &[3]
    );
    let old = json!({"oulipoly.session_control/v2": local});
    assert_eq!(
        select(&local, &old).unwrap_err().reason,
        UnavailableReason::NoCommonVersion
    );
    let both = json!({"oulipoly.session_control/v2":local, PROTOCOL:local});
    let selected = select(&local, &both).unwrap();
    assert_eq!(selected.protocol, PROTOCOL);
    let mut cancel = hold();
    cancel.operation = Operation::Cancel;
    cancel.agree(&selected).unwrap();
    assert_eq!(
        hold().agree(&selected).unwrap_err().reason,
        UnavailableReason::NoCommonCapability
    );
}
