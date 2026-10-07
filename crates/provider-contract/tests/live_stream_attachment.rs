//! live_stream/v1 attachment: message admission, role pairing, host decisions
//! for registration and attachment, publisher ingest, and the subscriber's
//! replay continuation, end to end in memory.
//!
//! These are deterministic contract checks. No socket, broker, publisher or
//! host runs; they do not show that a host decides scope correctly, that an
//! incarnation is genuine, that finalization follows durable publication, or
//! that publication never blocks.

use agent_provider_contract::live_stream::attachment::{
    attach, follow_attached, pair, Attach, Finalization, Hello, HostDecision, Ingest, Message,
    Registration, Role, MAX_DIRECTORY_ENTRIES, MAX_MESSAGE_BYTES,
};
use agent_provider_contract::live_stream::{
    validate, Accepted, Audience, Channel, ControlFact, ControlFrame, Correlation, Cursor,
    DataChannel, DataFrame, Descriptor, EndedFrame, FinalizedFrame, Gap, GapReason, Lost, Offer,
    Record, Selected, UnavailableReason, VisibilityClaim, MAX_DATA_BYTES, PROTOCOL,
};
use base64::Engine;
use serde_json::Value;

const STREAM: &str = "0123456789abcdef0123456789abcdef";
const FIRST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SECOND: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn offer(audience: Audience) -> Offer {
    Offer {
        channels: vec![Channel::Stdout, Channel::Stderr, Channel::Control],
        audiences: vec![audience],
        max_data_bytes: 4096,
    }
}

fn descriptor(incarnation: &str, audience: Audience) -> Descriptor {
    Descriptor {
        protocol: PROTOCOL.into(),
        stream_id: STREAM.into(),
        incarnation: incarnation.into(),
        channels: vec![Channel::Stdout, Channel::Control],
        max_data_bytes: 4096,
        visibility: VisibilityClaim {
            audience,
            scope: (audience == Audience::Scoped).then(|| "root:42".into()),
            channels: vec![Channel::Stdout],
        },
        correlation: Some(Correlation {
            root: Some("root:42".into()),
            ..Correlation::default()
        }),
    }
}

/// What a broker and a peer select after exchanging `hello` lines.
fn handshake(peer: Role, audience: Audience) -> Selected {
    let mine = Hello::new(Role::Broker, &offer(audience));
    let theirs = Hello::new(peer, &offer(audience));
    let line = Message::Hello(theirs).encode_line();
    let Message::Hello(received) = Message::decode_line(&line).unwrap() else {
        panic!("hello");
    };
    let broker_side = received.select(Role::Broker, &offer(audience)).unwrap();
    let peer_side = mine.select(peer, &offer(audience)).unwrap();
    assert_eq!(broker_side, peer_side);
    broker_side
}

fn granted(scope: &str) -> HostDecision {
    HostDecision::Granted {
        stream_id: STREAM.into(),
        scope: scope.into(),
    }
}

/// A message as the far end receives it.
fn wire(message: Message, sender: Role) -> Message {
    message.check_sender(sender).unwrap();
    let received = Message::decode_line(&message.encode_line()).unwrap();
    assert_eq!(received, message);
    received
}

fn data(incarnation: &str, seq: u64, text: &str) -> Record {
    Record::Data(DataFrame {
        stream_id: STREAM.into(),
        incarnation: incarnation.into(),
        seq,
        observed_at_unix_ms: seq,
        channel: DataChannel::Stdout,
        data_base64: base64::engine::general_purpose::STANDARD.encode(text),
    })
}

fn exit_observed(incarnation: &str, seq: u64) -> Record {
    Record::Control(ControlFrame {
        stream_id: STREAM.into(),
        incarnation: incarnation.into(),
        seq,
        observed_at_unix_ms: seq,
        fact: ControlFact::ExitObserved {
            code: Some(0),
            signal: None,
        },
    })
}

fn finalized(incarnation: &str, seq: u64) -> Record {
    Record::Finalized(FinalizedFrame {
        stream_id: STREAM.into(),
        incarnation: incarnation.into(),
        seq,
        observed_at_unix_ms: seq,
        durable_reference: "host:canonical/turn/7".into(),
    })
}

fn gap(first: u64, last: u64, reason: GapReason) -> Record {
    Record::Gap(Gap {
        stream_id: STREAM.into(),
        incarnation: FIRST.into(),
        first,
        last,
        reason,
    })
}

fn register(
    selected: &Selected,
    descriptor: Descriptor,
    finalization: Finalization,
    decision: &HostDecision,
) -> Result<Ingest, agent_provider_contract::live_stream::LiveUnavailable> {
    let sent = wire(
        Message::Register(Registration {
            descriptor,
            finalization,
        }),
        Role::Publisher,
    );
    let Message::Register(registration) = sent else {
        panic!("register");
    };
    let (ingest, registered) = Ingest::register(selected, registration, decision)?;
    wire(Message::Registered(registered), Role::Broker);
    Ok(ingest)
}

#[test]
fn classified_messages_distinguish_raw_schema_from_semantic_admission() {
    let cases: Value =
        serde_json::from_str(include_str!("fixtures/live_stream/attachment-v1.json")).unwrap();
    assert_eq!(cases["protocol"], PROTOCOL);
    for value in cases["valid"]["Message"].as_array().unwrap() {
        let message = Message::decode(value).unwrap_or_else(|e| panic!("{value}: {e}"));
        assert_eq!(&serde_json::to_value(&message).unwrap(), value);
        assert_eq!(
            Message::decode_line(&message.encode_line()).unwrap(),
            message
        );
    }
    for (layer, schema_accepts) in [("invalid_structural", false), ("invalid_semantic", true)] {
        for value in cases[layer]["Message"].as_array().unwrap() {
            let structural = validate("Message", value, UnavailableReason::InvalidRecord);
            assert_eq!(structural.is_ok(), schema_accepts, "{layer}: {value}");
            let error = Message::decode(value).expect_err(&format!("{layer}: {value}"));
            assert_eq!(error.reason, UnavailableReason::InvalidRecord, "{value}");
            let shown = format!("{error} {}", serde_json::to_string(&error).unwrap());
            assert!(!shown.contains("SECRETVALUE"), "{shown}");
        }
    }
}

#[test]
fn message_lines_are_bounded_before_parsing_and_fit_their_largest_payloads() {
    let largest = Message::Record {
        record: Record::Data(DataFrame {
            stream_id: STREAM.into(),
            incarnation: FIRST.into(),
            seq: 9_007_199_254_740_991,
            observed_at_unix_ms: 9_007_199_254_740_991,
            channel: DataChannel::Stdout,
            data_base64: base64::engine::general_purpose::STANDARD.encode(vec![
                0xff;
                MAX_DATA_BYTES
                    as usize
            ]),
        }),
    }
    .encode_line();
    assert!(Message::decode_line(&largest).is_ok());
    let mut widest = descriptor(FIRST, Audience::Scoped);
    widest.channels = vec![
        Channel::Stdout,
        Channel::Stderr,
        Channel::Pty,
        Channel::Control,
    ];
    widest.visibility.scope = Some("s".repeat(256));
    widest.correlation = Some(Correlation {
        root: Some("r".repeat(256)),
        session: Some("s".repeat(256)),
        work: Some("w".repeat(256)),
        generation: Some("g".repeat(256)),
        epoch: Some("e".repeat(256)),
    });
    let directory = |count| {
        Message::Directory(
            agent_provider_contract::live_stream::attachment::Directory {
                streams: vec![widest.clone(); count],
            },
        )
        .encode_line()
    };
    let full = directory(MAX_DIRECTORY_ENTRIES);
    assert!(full.len() <= MAX_MESSAGE_BYTES, "{}", full.len());
    assert!(Message::decode_line(&full).is_ok());
    assert!(Message::decode_line(&directory(MAX_DIRECTORY_ENTRIES + 1)).is_err());
    let padded = format!("{largest}{}", " ".repeat(MAX_MESSAGE_BYTES - largest.len()));
    assert!(Message::decode_line(&padded).is_ok());
    let over = Message::decode_line(&format!("{padded} ")).unwrap_err();
    assert!(over.detail.unwrap().contains("exceeds"));
}

#[test]
fn connections_join_a_broker_and_each_role_sends_only_its_messages() {
    for (local, remote, ok) in [
        (Role::Broker, Role::Publisher, true),
        (Role::Subscriber, Role::Broker, true),
        (Role::Publisher, Role::Subscriber, false),
        (Role::Subscriber, Role::Subscriber, false),
        (Role::Broker, Role::Broker, false),
    ] {
        assert_eq!(pair(local, remote).is_ok(), ok, "{local:?} {remote:?}");
        let hello = Hello::new(remote, &offer(Audience::Owner));
        assert_eq!(
            hello.select(local, &offer(Audience::Owner)).is_ok(),
            ok,
            "{local:?} {remote:?}"
        );
    }
    let cursor = descriptor(FIRST, Audience::Owner).start();
    let register = Message::Register(Registration {
        descriptor: descriptor(FIRST, Audience::Owner),
        finalization: Finalization::Never,
    });
    let attach_message = Message::Attach(Attach { cursor });
    let record = Message::Record {
        record: data(FIRST, 1, "x"),
    };
    for (message, sender, ok) in [
        (&register, Role::Publisher, true),
        (&register, Role::Subscriber, false),
        (&register, Role::Broker, false),
        (&attach_message, Role::Subscriber, true),
        (&attach_message, Role::Publisher, false),
        (&Message::List {}, Role::Publisher, false),
        (&record, Role::Publisher, true),
        (&record, Role::Broker, true),
        (&record, Role::Subscriber, false),
    ] {
        assert_eq!(message.check_sender(sender).is_ok(), ok, "{sender:?}");
    }
}

#[test]
fn registration_and_attachment_need_a_host_decision_covering_stream_and_scope() {
    let selected = handshake(Role::Publisher, Audience::Scoped);
    let claim = descriptor(FIRST, Audience::Scoped);
    // A descriptor that agrees with the selection is still not admitted alone.
    claim.agree(&selected).unwrap();
    let other_stream = HostDecision::Granted {
        stream_id: "f".repeat(32),
        scope: "root:42".into(),
    };
    for decision in [
        HostDecision::Refused,
        other_stream,
        granted("root:43"),
        granted("not a host ref"),
    ] {
        let error = register(&selected, claim.clone(), Finalization::Never, &decision).unwrap_err();
        assert_eq!(
            error.reason,
            UnavailableReason::NotAuthorized,
            "{decision:?}"
        );
    }
    let ingest = register(
        &selected,
        claim.clone(),
        Finalization::Never,
        &granted("root:42"),
    )
    .unwrap();
    // Owner and same_user claims carry no scope; the host's reference is opaque.
    let owner = handshake(Role::Publisher, Audience::Owner);
    register(
        &owner,
        descriptor(FIRST, Audience::Owner),
        Finalization::Never,
        &granted("uid-bridge:none"),
    )
    .unwrap();

    let subscriber = handshake(Role::Subscriber, Audience::Scoped);
    let window = ingest.window(1, None);
    let request = Attach {
        cursor: claim.start(),
    };
    for decision in [HostDecision::Refused, granted("root:43")] {
        let error = attach(&subscriber, &claim, &window, &request, &decision).unwrap_err();
        assert_eq!(error.reason, UnavailableReason::NotAuthorized);
    }
    attach(&subscriber, &claim, &window, &request, &granted("root:42")).unwrap();
    // A subscriber whose selection lacks the claimed audience cannot attach.
    let owner_subscriber = handshake(Role::Subscriber, Audience::Owner);
    let error = attach(
        &owner_subscriber,
        &claim,
        &window,
        &request,
        &granted("root:42"),
    )
    .unwrap_err();
    assert_eq!(error.reason, UnavailableReason::ProtocolViolation);
}

#[test]
fn publisher_records_flow_through_ingest_and_replay_to_a_subscriber() {
    let selected = handshake(Role::Publisher, Audience::Scoped);
    let claim = descriptor(FIRST, Audience::Scoped);
    let decision = granted("root:42");
    let mut ingest = register(
        &selected,
        claim.clone(),
        Finalization::CustodyOwner,
        &decision,
    )
    .unwrap();
    let published = [
        data(FIRST, 1, "one"),
        data(FIRST, 2, "two"),
        // The publisher dropped 3..=4 rather than block, and says so exactly.
        gap(3, 4, GapReason::CaptureOverflow),
        exit_observed(FIRST, 5),
        data(FIRST, 6, "late"),
        finalized(FIRST, 7),
    ];
    let mut retained = Vec::new();
    for record in &published {
        let Message::Record { record } = wire(
            Message::Record {
                record: record.clone(),
            },
            Role::Publisher,
        ) else {
            panic!("record");
        };
        ingest.accept(&record).unwrap();
        retained.push(record);
        // An observed exit is a fact, not an end.
        if retained.len() == 4 {
            assert!(ingest.terminal().is_none());
        }
    }
    assert_eq!(ingest.last_published(), 7);
    assert!(ingest.terminal().is_some());
    assert_eq!(ingest.retire().last_seq, Some(7));

    // The broker evicted everything before seq 5; a fresh subscriber attaches.
    let subscriber = handshake(Role::Subscriber, Audience::Scoped);
    let window = ingest.window(5, None);
    let request = Attach {
        cursor: claim.start(),
    };
    let sent = wire(Message::Attach(request.clone()), Role::Subscriber);
    let Message::Attach(received) = sent else {
        panic!("attach");
    };
    let attached = attach(
        &subscriber,
        ingest.descriptor(),
        &window,
        &received,
        &decision,
    )
    .unwrap();
    let Message::Attached(attached) = wire(Message::Attached(attached), Role::Broker) else {
        panic!("attached");
    };
    let (mut follower, prefix) = follow_attached(&subscriber, &request, &attached).unwrap();
    assert_eq!(prefix, vec![Accepted::Gap { first: 1, last: 4 }]);
    assert_eq!(attached.plan.deliver_from, 5);
    for record in &retained[3..] {
        follower.accept(record).unwrap();
    }
    assert!(follower.ended());
    let persisted = Cursor::decode(&follower.cursor().encode()).unwrap();

    // Re-attaching at the final cursor restores terminal knowledge, nothing more.
    let again = Attach { cursor: persisted };
    let attached = attach(&subscriber, ingest.descriptor(), &window, &again, &decision).unwrap();
    let (follower, prefix) = follow_attached(&subscriber, &again, &attached).unwrap();
    assert_eq!(prefix, vec![Accepted::Duplicate]);
    assert!(follower.ended());
}

#[test]
fn ingest_accepts_only_what_this_publisher_originates_without_mutation_on_refusal() {
    let selected = handshake(Role::Publisher, Audience::Owner);
    let decision = granted("host:owner");
    let mut never = register(
        &selected,
        descriptor(FIRST, Audience::Owner),
        Finalization::Never,
        &decision,
    )
    .unwrap();
    never.accept(&data(FIRST, 1, "x")).unwrap();
    let before = never.clone();
    for record in [
        finalized(FIRST, 2),
        gap(2, 3, GapReason::Evicted),
        Record::Discontinuity(agent_provider_contract::live_stream::Discontinuity {
            stream_id: STREAM.into(),
            previous_incarnation: FIRST.into(),
            after_seq: 1,
            previous_last_seq: None,
            incarnation: SECOND.into(),
        }),
        data(SECOND, 2, "another incarnation"),
        data(FIRST, 3, "skips a sequence"),
    ] {
        let error = never.accept(&record).unwrap_err();
        assert_eq!(
            error.reason,
            UnavailableReason::ProtocolViolation,
            "{record:?}"
        );
        assert_eq!(never, before);
    }
    let ended = Record::Ended(EndedFrame {
        stream_id: STREAM.into(),
        incarnation: FIRST.into(),
        seq: 2,
        observed_at_unix_ms: 2,
    });
    assert_eq!(never.accept(&ended).unwrap(), Accepted::Ended { seq: 2 });
    assert!(never.accept(&data(FIRST, 3, "after end")).is_err());
}

#[test]
fn a_publisher_that_goes_away_unfinished_leaves_its_tail_unknown() {
    let selected = handshake(Role::Publisher, Audience::Owner);
    let decision = granted("host:owner");
    let mut first = register(
        &selected,
        descriptor(FIRST, Audience::Owner),
        Finalization::CustodyOwner,
        &decision,
    )
    .unwrap();
    first.accept(&data(FIRST, 1, "a")).unwrap();
    first.accept(&exit_observed(FIRST, 2)).unwrap();
    // Connection closed after an observed exit: not finalized, end unknown.
    assert!(first.terminal().is_none());
    let previous = first.retire();
    assert_eq!(previous.last_seq, None);

    let mut second = register(
        &selected,
        descriptor(SECOND, Audience::Owner),
        Finalization::CustodyOwner,
        &decision,
    )
    .unwrap();
    second.accept(&data(SECOND, 1, "b")).unwrap();
    let subscriber = handshake(Role::Subscriber, Audience::Owner);
    let mut old = descriptor(FIRST, Audience::Owner).start();
    old.after_seq = 1;
    let request = Attach { cursor: old };
    let window = second.window(1, Some(previous));
    let attached = attach(
        &subscriber,
        second.descriptor(),
        &window,
        &request,
        &decision,
    )
    .unwrap();
    let (follower, prefix) = follow_attached(&subscriber, &request, &attached).unwrap();
    assert_eq!(
        prefix,
        vec![Accepted::Restarted {
            incarnation: SECOND.into(),
            lost: Lost::Unknown,
        }]
    );
    assert_eq!(follower.cursor().incarnation, SECOND);
}

#[test]
fn attachment_refuses_foreign_and_ahead_cursors_but_cannot_detect_a_plausible_fabrication() {
    let selected = handshake(Role::Publisher, Audience::Owner);
    let decision = granted("host:owner");
    let claim = descriptor(FIRST, Audience::Owner);
    let mut ingest = register(&selected, claim.clone(), Finalization::Never, &decision).unwrap();
    for seq in 1..=3 {
        ingest.accept(&data(FIRST, seq, "x")).unwrap();
    }
    let subscriber = handshake(Role::Subscriber, Audience::Owner);
    let window = ingest.window(1, None);
    let mut foreign = claim.start();
    foreign.stream_id = "f".repeat(32);
    let mut ahead = claim.start();
    ahead.after_seq = 4;
    for (cursor, reason) in [
        (foreign, UnavailableReason::UnknownStream),
        (ahead, UnavailableReason::ProtocolViolation),
    ] {
        let error =
            attach(&subscriber, &claim, &window, &Attach { cursor }, &decision).unwrap_err();
        assert_eq!(error.reason, reason);
    }
    // A made-up position inside the published range is indistinguishable from a
    // genuine one: cursor genuineness stays a host obligation.
    let mut fabricated = claim.start();
    fabricated.after_seq = 2;
    let request = Attach { cursor: fabricated };
    let attached = attach(&subscriber, &claim, &window, &request, &decision).unwrap();
    assert_eq!(attached.plan.deliver_from, 3);
}

#[test]
fn subscriber_refuses_an_attached_answer_that_does_not_continue_its_cursor() {
    let selected = handshake(Role::Publisher, Audience::Owner);
    let decision = granted("host:owner");
    let claim = descriptor(FIRST, Audience::Owner);
    let mut ingest = register(&selected, claim.clone(), Finalization::Never, &decision).unwrap();
    for seq in 1..=3 {
        ingest.accept(&data(FIRST, seq, "x")).unwrap();
    }
    let subscriber = handshake(Role::Subscriber, Audience::Owner);
    let request = Attach {
        cursor: claim.start(),
    };
    let honest = attach(
        &subscriber,
        &claim,
        &ingest.window(3, None),
        &request,
        &decision,
    )
    .unwrap();
    follow_attached(&subscriber, &request, &honest).unwrap();
    let mut skipped = honest.clone();
    skipped.plan.prefix.clear();
    let mut wrong_stream = honest.clone();
    wrong_stream.descriptor.stream_id = "f".repeat(32);
    let mut widened = honest;
    widened.descriptor.max_data_bytes = MAX_DATA_BYTES;
    for attached in [skipped, wrong_stream, widened] {
        let error = follow_attached(&subscriber, &request, &attached).unwrap_err();
        assert_eq!(error.reason, UnavailableReason::ProtocolViolation);
    }
}
