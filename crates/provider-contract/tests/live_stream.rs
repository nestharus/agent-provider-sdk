//! live_stream/v3: structural schema versus semantic admission; frame and field
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
    serde_json::from_str(include_str!("fixtures/live_stream/v3.json")).unwrap()
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
fn classified_payloads_distinguish_raw_schema_from_semantic_admission() {
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
    for (layer, schema_accepts) in [("invalid_structural", false), ("invalid_semantic", true)] {
        for (definition, values) in cases[layer].as_object().unwrap() {
            for value in values.as_array().unwrap() {
                let structural = validate(definition, value, UnavailableReason::InvalidRecord);
                assert_eq!(
                    structural.is_ok(),
                    schema_accepts,
                    "{layer} {definition}: {value}"
                );
                let error = decode(definition, value).expect_err(&format!("{definition} {value}"));
                assert_eq!(error.reason, UnavailableReason::InvalidRecord, "{value}");
            }
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
    assert_eq!(admitted.bytes().unwrap().len(), MAX_DATA_BYTES as usize);
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
fn sdk_generated_diagnostics_do_not_repeat_submitted_values() {
    let invalid = &fixtures()["invalid_structural"]["Record"];
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
    assert_eq!(live_stream::SUPPORTED_VERSIONS, &[3]);
}

fn run_follow(case: &Value) -> (Follower, Vec<Result<Accepted, LiveUnavailable>>) {
    let cursor = Cursor::decode(&case["cursor"]).unwrap();
    let channels =
        serde_json::from_value::<Vec<live_stream::Channel>>(case["channels"].clone()).unwrap();
    let mut follower = Follower::new(
        cursor,
        &channels,
        case["max_data_bytes"].as_u64().unwrap() as u32,
    )
    .unwrap();
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
        let mut follower =
            Follower::new(cursor.clone(), &[live_stream::Channel::Stdout], 1).unwrap();
        for record in &plan.prefix {
            assert_eq!(&Record::decode_line(&record.encode_line()).unwrap(), record);
            follower
                .accept(record)
                .unwrap_or_else(|e| panic!("{name}: {e}"));
        }
        assert_eq!(follower.cursor().incarnation, window.incarnation, "{name}");
        assert_eq!(follower.cursor().after_seq + 1, plan.deliver_from, "{name}");
        validate(
            "ReplayPlan",
            &serde_json::to_value(&plan).unwrap(),
            UnavailableReason::InvalidRecord,
        )
        .unwrap();
        if let Some(records) = case.get("deliver_records") {
            for value in records.as_array().unwrap() {
                follower.accept(&Record::decode(value).unwrap()).unwrap();
            }
        }
        if let Some(expected) = case["expect"].get("terminal_cursor") {
            assert_eq!(follower.cursor().encode(), *expected, "{name}");
            assert!(follower.ended(), "{name}");
            let restored = Follower::new(
                Cursor::decode(expected).unwrap(),
                &[live_stream::Channel::Stdout],
                1,
            )
            .unwrap();
            assert!(restored.ended(), "{name}");
        }
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

fn terminal_case() -> (Descriptor, Record) {
    let descriptor = Descriptor::decode(&fixtures()["valid"]["Descriptor"][0]).unwrap();
    let record = Record::decode(&json!({
        "kind": "finalized", "stream_id": descriptor.stream_id,
        "incarnation": descriptor.incarnation, "seq": 1, "observed_at_unix_ms": 0,
        "durable_reference": "host:canonical/turn/12"
    }))
    .unwrap();
    (descriptor, record)
}

#[test]
fn reconstructed_terminal_cursor_refuses_forward_gap_and_restart_without_mutation() {
    let (descriptor, terminal) = terminal_case();
    for terminal in [
        terminal,
        Record::Ended(live_stream::EndedFrame {
            stream_id: descriptor.stream_id.clone(),
            incarnation: descriptor.incarnation.clone(),
            seq: 1,
            observed_at_unix_ms: 0,
        }),
    ] {
        let mut original = Follower::new(descriptor.start(), &descriptor.channels, 4).unwrap();
        original.accept(&terminal).unwrap();
        let cursor = Cursor::decode(&original.cursor().encode()).unwrap();
        let mut restored = Follower::new(cursor.clone(), &descriptor.channels, 4).unwrap();
        assert!(restored.ended());
        assert_eq!(restored.accept(&terminal).unwrap(), Accepted::Duplicate);
        for value in [
            json!({"kind":"data", "stream_id":descriptor.stream_id, "incarnation":descriptor.incarnation,
                "seq":2, "observed_at_unix_ms":0, "channel":"stdout", "data_base64":"YQ=="}),
            json!({"kind":"gap", "stream_id":descriptor.stream_id, "incarnation":descriptor.incarnation,
                "first":2, "last":3, "reason":"evicted"}),
            json!({"kind":"discontinuity", "stream_id":descriptor.stream_id,
                "previous_incarnation":descriptor.incarnation, "after_seq":1, "incarnation":"b".repeat(32)}),
        ] {
            let error = restored
                .accept(&Record::decode(&value).unwrap())
                .unwrap_err();
            assert_eq!(error.reason, UnavailableReason::ProtocolViolation);
            assert_eq!(restored.cursor(), &cursor);
        }
        // A different terminal at the same position is not a positional duplicate.
        let conflicting = if matches!(terminal, Record::Finalized(_)) {
            json!({"kind":"ended", "stream_id":descriptor.stream_id, "incarnation":descriptor.incarnation,
                "seq":1,"observed_at_unix_ms":0})
        } else {
            json!({"kind":"finalized", "stream_id":descriptor.stream_id, "incarnation":descriptor.incarnation,
                "seq":1,"observed_at_unix_ms":0,"durable_reference":"host:other"})
        };
        assert!(restored
            .accept(&Record::decode(&conflicting).unwrap())
            .is_err());
        assert_eq!(restored.cursor(), &cursor);
    }
}

#[test]
fn selected_descriptor_and_follow_setup_share_capability_agreement() {
    let mut descriptor = Descriptor::decode(&fixtures()["valid"]["Descriptor"][0]).unwrap();
    let offer = Offer {
        channels: vec![live_stream::Channel::Stdout, live_stream::Channel::Control],
        audiences: vec![live_stream::Audience::Owner],
        max_data_bytes: 4,
    };
    let selected = select(&offer, &advertisement(&offer)).unwrap();
    descriptor.channels = offer.channels.clone();
    descriptor.max_data_bytes = 4;
    descriptor.visibility = live_stream::VisibilityClaim {
        audience: live_stream::Audience::Owner,
        scope: None,
        channels: vec![live_stream::Channel::Stdout],
    };
    descriptor.agree(&selected).unwrap();
    Follower::from_descriptor(&selected, &descriptor, descriptor.start()).unwrap();
    let agreed = descriptor.clone();
    for variant in 0..4 {
        descriptor = agreed.clone();
        match variant {
            0 => descriptor.channels.push(live_stream::Channel::Pty),
            1 => descriptor.max_data_bytes = 5,
            2 => descriptor.visibility.audience = live_stream::Audience::SameUser,
            _ => descriptor.visibility.channels = vec![live_stream::Channel::Stderr],
        }
        // Only the last is intrinsically invalid. Others pass standalone admission.
        assert_eq!(
            Descriptor::decode(&serde_json::to_value(&descriptor).unwrap()).is_ok(),
            variant != 3
        );
        assert!(descriptor.agree(&selected).is_err());
        assert!(Follower::from_descriptor(&selected, &descriptor, descriptor.start()).is_err());
    }
    let mut invalid_selected = selected.clone();
    invalid_selected.channels.push(live_stream::Channel::Stdout);
    assert!(agreed.agree(&invalid_selected).is_err());
    invalid_selected = selected;
    invalid_selected.protocol = "oulipoly.live_stream/v4".into();
    assert!(agreed.agree(&invalid_selected).is_err());
    // A supported scoped claim is agreement about shape, never an access grant.
    let scoped = Offer {
        audiences: vec![live_stream::Audience::Scoped],
        ..offer
    };
    let scoped_selected = select(&scoped, &advertisement(&scoped)).unwrap();
    let mut scoped_descriptor = agreed;
    scoped_descriptor.visibility.audience = live_stream::Audience::Scoped;
    scoped_descriptor.visibility.scope = Some("host:any-opaque-scope".into());
    scoped_descriptor.agree(&scoped_selected).unwrap();
}

#[test]
fn public_typed_inputs_fail_as_diagnostics_and_leave_follower_state_unchanged() {
    let (descriptor, _) = terminal_case();
    let mut follower = Follower::new(descriptor.start(), &descriptor.channels, 4).unwrap();
    let before = follower.cursor().clone();
    for value in [
        json!({"kind":"data","stream_id":descriptor.stream_id,"incarnation":descriptor.incarnation,
            "seq":1,"observed_at_unix_ms":0,"channel":"stdout","data_base64":"*"}),
        json!({"kind":"gap","stream_id":descriptor.stream_id,"incarnation":descriptor.incarnation,
            "first":4,"last":2,"reason":"evicted"}),
        json!({"kind":"control","stream_id":descriptor.stream_id,"incarnation":descriptor.incarnation,
            "seq":1,"observed_at_unix_ms":0,"fact":{"type":"exit_observed","signal":0}}),
    ] {
        let raw: Record = serde_json::from_value(value).unwrap();
        let error = follower.accept(&raw).unwrap_err();
        assert_eq!(error.reason, UnavailableReason::InvalidRecord);
        validate(
            "LiveUnavailable",
            &serde_json::to_value(&error).unwrap(),
            error.reason,
        )
        .unwrap();
        assert_eq!(follower.cursor(), &before);
    }
    let raw = DataFrame {
        stream_id: descriptor.stream_id.clone(),
        incarnation: descriptor.incarnation.clone(),
        seq: 1,
        observed_at_unix_ms: 0,
        channel: DataChannel::Stdout,
        data_base64: "*".into(),
    };
    assert!(raw.bytes().is_err());
    let mut bad_cursor = before.clone();
    bad_cursor.after_seq = u64::MAX;
    assert!(Follower::new(bad_cursor, &descriptor.channels, 4).is_err());
    assert!(Follower::new(before.clone(), &[], 4).is_err());
    assert!(Follower::new(before, &descriptor.channels, 0).is_err());
}

#[test]
fn typed_replay_window_checks_bounds_terminal_consistency_and_maximum_sentinel() {
    let (descriptor, record) = terminal_case();
    let Record::Finalized(mut terminal) = record else {
        panic!("finalized")
    };
    terminal.seq = 9_007_199_254_740_991;
    let mut window = RetainedWindow {
        stream_id: descriptor.stream_id.clone(),
        incarnation: descriptor.incarnation.clone(),
        first_retained: terminal.seq,
        last_published: terminal.seq,
        terminal: Some(live_stream::Terminal::Finalized(terminal.clone())),
        previous: None,
    };
    let mut cursor = descriptor.start();
    cursor.after_seq = terminal.seq;
    let plan = plan_replay(&window, &cursor).unwrap();
    assert_eq!(plan.deliver_from, 9_007_199_254_740_992);
    validate(
        "ReplayPlan",
        &serde_json::to_value(&plan).unwrap(),
        UnavailableReason::InvalidRecord,
    )
    .unwrap();
    let mut follower = Follower::new(cursor.clone(), &descriptor.channels, 4).unwrap();
    follower.accept(&plan.prefix[0]).unwrap();
    assert!(follower.ended());
    assert_eq!(follower.cursor().terminal, window.terminal);
    cursor = follower.cursor().clone();
    let valid = window.clone();
    for variant in 0..7 {
        window = valid.clone();
        match variant {
            0 => window.last_published = u64::MAX,
            1 => window.first_retained = u64::MAX,
            2 => window.first_retained += 1,
            3 => window.terminal = None,
            4 => window.incarnation = "b".repeat(32),
            5 => {
                window.previous = Some(live_stream::PreviousIncarnation {
                    incarnation: window.incarnation.clone(),
                    last_seq: None,
                })
            }
            _ => {
                window.terminal = Some(live_stream::Terminal::Finalized(
                    live_stream::FinalizedFrame {
                        durable_reference: "SYNTHETIC SECRET".into(),
                        ..terminal.clone()
                    },
                ))
            }
        }
        let error = plan_replay(&window, &cursor).unwrap_err();
        validate(
            "LiveUnavailable",
            &serde_json::to_value(&error).unwrap(),
            error.reason,
        )
        .unwrap();
        assert!(!format!("{error}").contains("SYNTHETIC"));
    }
}

#[test]
fn positional_suppression_includes_gapped_positions_but_learns_terminal_at_boundary() {
    let (descriptor, terminal) = terminal_case();
    let mut follower =
        Follower::new(descriptor.start(), &[live_stream::Channel::Stdout], 1).unwrap();
    let gap = Record::decode(&json!({"kind":"gap", "stream_id":descriptor.stream_id,
        "incarnation":descriptor.incarnation,"first":1,"last":3,"reason":"capture_overflow"}))
    .unwrap();
    follower.accept(&gap).unwrap();
    let old = Record::decode(&json!({"kind":"data", "stream_id":descriptor.stream_id,
        "incarnation":descriptor.incarnation,"seq":2,"observed_at_unix_ms":0,
        "channel":"pty","data_base64":"aGVsbG8="}))
    .unwrap();
    assert_eq!(follower.accept(&old).unwrap(), Accepted::Duplicate);
    assert!(!follower.ended());
    let mut final_value = serde_json::to_value(terminal).unwrap();
    final_value["seq"] = json!(3);
    follower
        .accept(&Record::decode(&final_value).unwrap())
        .unwrap();
    assert!(follower.ended());
    assert_eq!(follower.cursor().after_seq, 3);
    assert!(follower.cursor().terminal.is_some());
}

#[test]
fn raw_schema_acceptance_is_insufficient_for_decoded_byte_admission() {
    let (descriptor, _) = terminal_case();
    let value = json!({"kind":"data", "stream_id":descriptor.stream_id,
        "incarnation":descriptor.incarnation,"seq":1,"observed_at_unix_ms":0,
        "channel":"stdout", "data_base64":base64::engine::general_purpose::STANDARD.encode(vec![0;65_538])});
    validate("Record", &value, UnavailableReason::InvalidRecord).unwrap();
    assert_eq!(
        Record::decode(&value).unwrap_err().reason,
        UnavailableReason::InvalidRecord
    );
    // Constructor detail is caller responsibility, with a character bound only.
    let caller = LiveUnavailable::new(UnavailableReason::InvalidRecord, "SYNTHETIC_CALLER_DETAIL");
    assert!(caller.detail.unwrap().contains("SYNTHETIC_CALLER_DETAIL"));
}

#[test]
fn no_follow_context_claims_both_combined_and_separated_origin() {
    use live_stream::Channel::{Combined, Control, Stderr, Stdout};
    let descriptor = Descriptor::decode(&fixtures()["valid"]["Descriptor"][4]).unwrap();
    assert_eq!(descriptor.channels, vec![Combined, Control]);
    for channels in [
        &[Combined, Stdout][..],
        &[Stderr, Combined, Control],
        &[Stdout, Stderr, Combined],
    ] {
        let error = Follower::new(descriptor.start(), channels, 4).unwrap_err();
        assert_eq!(
            error.reason,
            UnavailableReason::InvalidRecord,
            "{channels:?}"
        );
    }
    Follower::new(descriptor.start(), &[Combined, Control], 4).unwrap();
    Follower::new(descriptor.start(), &[Stdout, Stderr, Control], 4).unwrap();
    // An offer states capability, not one incarnation's origin: it may name both.
    let offer: Offer = serde_json::from_value(fixtures()["valid"]["Offer"][2].clone()).unwrap();
    assert!(offer.channels.contains(&Combined) && offer.channels.contains(&Stdout));
    assert_eq!(
        select(&offer, &advertisement(&offer))
            .unwrap()
            .channels
            .len(),
        5
    );
}
