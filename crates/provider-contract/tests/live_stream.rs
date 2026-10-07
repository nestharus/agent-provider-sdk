//! live_stream/v1: schema, DTO and golden fixture agreement; frame and field
//! bounds; exact gap, cursor, incarnation and finalization semantics; peer
//! version selection; and separation from provider launch outcomes.
//!
//! These are deterministic contract checks. They do not show that any
//! publisher or broker tells the truth about gaps, that capture never
//! backpressures a launch, or that a visibility claim is enforced.

pub mod support {
    pub mod contract_matrix;
}

use agent_provider_contract::live_stream::{
    self, advertisement, plan_replay, select, validate, Accepted, Cursor, DataChannel, DataFrame,
    Descriptor, Follower, LiveUnavailable, Offer, Record, RetainedWindow, UnavailableReason,
    MAX_DATA_BYTES, MAX_RECORD_BYTES, PROTOCOL,
};
use agent_provider_contract::SchemaRegistry;
use base64::Engine;
use serde_json::{json, Value};
use support::contract_matrix::{
    fixtures as contract_fixtures, launch_event_fixture, LAUNCH_EVENT_ROWS, NON_LAUNCH_ROWS,
};

fn fixtures() -> Value {
    serde_json::from_str(include_str!("fixtures/live_stream/v1.json")).unwrap()
}

fn reason(value: &Value) -> UnavailableReason {
    serde_json::from_value(value.clone()).unwrap()
}

fn decode(definition: &str, value: &Value) -> Result<Value, LiveUnavailable> {
    let round = match definition {
        "Record" => serde_json::to_value(Record::decode(value)?),
        "Descriptor" => serde_json::to_value(Descriptor::decode(value)?),
        "Cursor" => serde_json::to_value(Cursor::decode(value)?),
        _ => {
            validate(definition, value, UnavailableReason::InvalidRecord)?;
            return Ok(value.clone());
        }
    };
    Ok(round.unwrap())
}

#[test]
fn golden_payloads_agree_with_schema_and_dto() {
    let cases = fixtures();
    assert_eq!(cases["protocol"], PROTOCOL);
    for (definition, values) in cases["valid"].as_object().unwrap() {
        for value in values.as_array().unwrap() {
            let round =
                decode(definition, value).unwrap_or_else(|e| panic!("{definition} {value}: {e}"));
            assert_eq!(&round, value, "{definition} DTO round trip");
        }
    }
    for value in cases["valid"]["Record"].as_array().unwrap() {
        let record = Record::decode(value).unwrap();
        assert_eq!(Record::decode_line(&record.encode_line()).unwrap(), record);
    }
    for (definition, values) in cases["invalid"].as_object().unwrap() {
        for value in values.as_array().unwrap() {
            let error = decode(definition, value).expect_err(&format!("{definition} {value}"));
            assert_eq!(error.reason, UnavailableReason::InvalidRecord, "{value}");
        }
    }
}

#[test]
fn frame_and_field_bounds_are_exact() {
    let stream = "0123456789abcdef0123456789abcdef";
    let frame = |bytes: usize| {
        Record::Data(DataFrame {
            stream_id: stream.into(),
            incarnation: "a".repeat(32),
            seq: 9_007_199_254_740_991,
            observed_at_unix_ms: 9_007_199_254_740_991,
            channel: DataChannel::Stdout,
            data_base64: base64::engine::general_purpose::STANDARD.encode(vec![0xff; bytes]),
        })
    };
    let largest = frame(MAX_DATA_BYTES as usize).encode_line();
    assert!(largest.len() <= MAX_RECORD_BYTES, "{}", largest.len());
    let Record::Data(admitted) = Record::decode_line(&largest).unwrap() else {
        panic!("data frame");
    };
    assert_eq!(admitted.bytes().len(), MAX_DATA_BYTES as usize);
    let over = Record::decode_line(&frame(MAX_DATA_BYTES as usize + 1).encode_line());
    assert_eq!(over.unwrap_err().reason, UnavailableReason::InvalidRecord);
    assert!(Record::decode_line(&frame(0).encode_line()).is_err());

    // The line bound applies before parsing.
    let padded = format!("{largest}{}", " ".repeat(MAX_RECORD_BYTES - largest.len()));
    assert!(Record::decode_line(&padded).is_ok());
    let error = Record::decode_line(&format!("{padded} ")).unwrap_err();
    assert!(error.detail.unwrap().contains("exceeds"));
    let mut sequence = largest.replace("9007199254740991", "9007199254740992");
    assert!(Record::decode_line(&sequence).is_err());
    sequence = json!({"kind": "ended", "stream_id": stream, "incarnation": "a".repeat(32),
        "seq": 1, "observed_at_unix_ms": 0})
    .to_string();
    assert!(Record::decode_line(&sequence).is_ok());
}

#[test]
fn diagnostics_do_not_repeat_submitted_values() {
    let invalid = &fixtures()["invalid"]["Record"];
    let with_secret = invalid
        .as_array()
        .unwrap()
        .iter()
        .find(|value| value.get("secret").is_some())
        .unwrap();
    let error = Record::decode(with_secret).unwrap_err();
    let shown = format!("{error} {}", serde_json::to_string(&error).unwrap());
    assert!(!shown.contains("SECRETVALUE"), "{shown}");
    validate(
        "LiveUnavailable",
        &serde_json::to_value(&error).unwrap(),
        error.reason,
    )
    .unwrap();
    let long = LiveUnavailable::new(UnavailableReason::InvalidRecord, "é".repeat(600));
    validate(
        "LiveUnavailable",
        &serde_json::to_value(&long).unwrap(),
        long.reason,
    )
    .unwrap();
}

#[test]
fn descriptor_claims_are_shape_checked_only() {
    let descriptor = Descriptor::decode(&fixtures()["valid"]["Descriptor"][0]).unwrap();
    // Correlations stay opaque values; the claim names who may observe,
    // which only a broker or host can enforce.
    assert_eq!(
        descriptor.correlation.as_ref().unwrap().epoch.as_deref(),
        Some("e5")
    );
    assert_eq!(descriptor.visibility.scope.as_deref(), Some("root:42"));
    let start = descriptor.start();
    assert_eq!(start.after_seq, 0);
    assert_eq!(Cursor::decode(&start.encode()).unwrap(), start);
}

#[test]
fn selection_vectors() {
    for case in fixtures()["selection"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let local: Offer = serde_json::from_value(case["local"].clone()).unwrap();
        let outcome = select(&local, &case["remote"]);
        match (&case["expect"].get("selected"), outcome) {
            (Some(expected), Ok(selected)) => {
                assert_eq!(
                    serde_json::to_value(&selected).unwrap(),
                    **expected,
                    "{name}"
                )
            }
            (None, Err(error)) => {
                assert_eq!(
                    error.reason,
                    reason(&case["expect"]["unavailable"]),
                    "{name}"
                );
                validate(
                    "LiveUnavailable",
                    &serde_json::to_value(&error).unwrap(),
                    error.reason,
                )
                .unwrap();
            }
            (_, outcome) => panic!("{name}: {outcome:?}"),
        }
    }
    // Each side advertising its own offer is enough to select.
    let offer: Offer = serde_json::from_value(fixtures()["valid"]["Offer"][0].clone()).unwrap();
    assert_eq!(
        select(&offer, &advertisement(&offer)).unwrap().channels,
        offer.channels
    );
    assert_eq!(live_stream::SUPPORTED_VERSIONS, &[1]);
}

fn run_follow(case: &Value) -> (Follower, Vec<Result<Accepted, LiveUnavailable>>) {
    let cursor = Cursor::decode(&case["cursor"]).unwrap();
    let channels =
        serde_json::from_value::<Vec<live_stream::Channel>>(case["channels"].clone()).unwrap();
    let mut follower = Follower::new(
        cursor,
        &channels,
        case["max_data_bytes"].as_u64().unwrap() as u32,
    );
    let mut outcomes = Vec::new();
    for value in case["records"].as_array().unwrap() {
        let record = Record::decode(value).unwrap();
        let outcome = follower.accept(&record);
        let stop = outcome.is_err();
        outcomes.push(outcome);
        if stop {
            break;
        }
    }
    (follower, outcomes)
}

#[test]
fn follow_vectors() {
    for case in fixtures()["follow"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let before = Cursor::decode(&case["cursor"]).unwrap();
        let (follower, outcomes) = run_follow(case);
        let expected = case["expect"].as_array().unwrap();
        assert_eq!(outcomes.len(), expected.len(), "{name}: {outcomes:?}");
        for (outcome, expected) in outcomes.iter().zip(expected) {
            match outcome {
                Ok(accepted) => {
                    assert_eq!(serde_json::to_value(accepted).unwrap(), *expected, "{name}")
                }
                Err(error) => assert_eq!(error.reason, reason(&expected["unavailable"]), "{name}"),
            }
        }
        match case.get("error_at") {
            Some(index) => {
                assert_eq!(
                    index.as_u64().unwrap() as usize,
                    outcomes.len() - 1,
                    "{name}"
                );
                assert!(outcomes.last().unwrap().is_err(), "{name}");
                // A record refused first leaves the caller's cursor unchanged.
                if outcomes.len() == 1 {
                    assert_eq!(follower.cursor(), &before, "{name}");
                }
            }
            None => {
                assert!(outcomes.iter().all(Result::is_ok), "{name}");
                assert_eq!(follower.cursor().encode(), case["final_cursor"], "{name}");
            }
        }
    }
}

#[test]
fn replay_vectors_agree_with_the_follower() {
    for case in fixtures()["replay"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let window: RetainedWindow = serde_json::from_value(case["window"].clone()).unwrap();
        let cursor = Cursor::decode(&case["cursor"]).unwrap();
        let outcome = plan_replay(&window, &cursor);
        let Some(expected) = case["expect"].get("plan") else {
            assert_eq!(
                outcome.unwrap_err().reason,
                reason(&case["expect"]["unavailable"]),
                "{name}"
            );
            continue;
        };
        let plan = outcome.unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(serde_json::to_value(&plan).unwrap(), *expected, "{name}");
        // Every planned record is admissible, and a subscriber following
        // the cursor through the plan ends exactly before deliver_from of the
        // window's current incarnation.
        let mut follower = Follower::new(cursor.clone(), &[live_stream::Channel::Stdout], 1);
        for record in &plan.prefix {
            assert_eq!(&Record::decode_line(&record.encode_line()).unwrap(), record);
            follower
                .accept(record)
                .unwrap_or_else(|e| panic!("{name}: {e}"));
        }
        assert_eq!(follower.cursor().incarnation, window.incarnation, "{name}");
        assert_eq!(follower.cursor().after_seq + 1, plan.deliver_from, "{name}");
    }
}

#[test]
fn live_records_and_diagnostics_are_not_provider_launch_outcomes() {
    let live = fixtures();
    let registry = SchemaRegistry::new();
    let mut live_values: Vec<&Value> = live["valid"]["Record"].as_array().unwrap().iter().collect();
    live_values.extend(live["valid"]["LiveUnavailable"].as_array().unwrap());
    for value in &live_values {
        for row in LAUNCH_EVENT_ROWS {
            assert!(
                registry.validate_launch_event(row.kind, value).is_err(),
                "{} admits {value}",
                row.kind
            );
        }
        for row in NON_LAUNCH_ROWS {
            assert!(
                registry
                    .validate_error_response(row.subcommand, value)
                    .is_err(),
                "{} error admits {value}",
                row.subcommand
            );
        }
    }
    // Launch events, including the terminal exit, are not live records.
    let contract = contract_fixtures();
    for row in LAUNCH_EVENT_ROWS {
        let event = launch_event_fixture(&contract, row.kind);
        assert!(
            Record::decode(event).is_err(),
            "{} admitted as live",
            row.kind
        );
    }
    // Broker absence has exactly one representation here.
    let absent = serde_json::to_value(LiveUnavailable::broker_absent()).unwrap();
    assert_eq!(absent, live["valid"]["LiveUnavailable"][0]);
}
