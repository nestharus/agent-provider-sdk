//! Who says what on a live-stream connection, and which checks each side owns.
//!
//! Every connection joins a broker to one publisher or to one subscriber.
//! Both ends open with [`Hello`]; the rest is [`Message`] lines. A publisher
//! registers one incarnation and sends its records. A subscriber lists
//! streams, attaches from a caller-owned [`Cursor`] and follows records from
//! the [`ReplayPlan`] onward. This module reuses the record, selection, follow
//! and replay semantics of the parent module; it adds no second meaning for
//! any of them.
//!
//! The SDK opens no socket, finds no endpoint and runs no broker. The host
//! supplies the endpoint and an ordered, reliable byte stream, and it owns the
//! obligations listed in the contract README: establishing scope, keeping
//! incarnations genuine, finalizing only from the custody owner's terminal
//! retained result, and dropping rather than blocking publication. Admission
//! to publish or attach needs an explicit [`HostDecision`]. The SDK never
//! constructs a grant, and nothing on the wire (an advertisement, a
//! descriptor, a correlation, a control fact or a peer's UID) becomes one.

use super::{
    invalid_record, plan_replay, select, validate, violation, Accepted, Audience, Cursor,
    Descriptor, Follower, GapReason, LiveUnavailable, Offer, PreviousIncarnation, Record,
    ReplayPlan, RetainedWindow, Selected, Terminal, UnavailableReason, MAX_ADVERTISEMENT_BYTES,
    MAX_RECORD_BYTES,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Upper bound of one serialized message line, checked before parsing. It
/// admits the largest record line with its envelope.
pub const MAX_MESSAGE_BYTES: usize = MAX_RECORD_BYTES + 1_024;
/// Upper bound of descriptors in one directory message.
pub const MAX_DIRECTORY_ENTRIES: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Publisher,
    Broker,
    Subscriber,
}

/// Whether a publisher may send `finalized` frames. A declaration the host
/// wires when it starts the publisher, not a proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Finalization {
    /// Sends `finalized` only from the custody owner's terminal retained
    /// result, after the host's normal durable publication.
    CustodyOwner,
    /// Can only end. A tap with no access to the custody owner's result.
    Never,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hello {
    pub role: Role,
    pub advertisement: Value,
}

impl Hello {
    pub fn new(role: Role, offer: &Offer) -> Self {
        Self {
            role,
            advertisement: super::advertisement(offer),
        }
    }

    /// Checks that the two roles meet through a broker, then selects what
    /// both can use. Agreement establishes capability, not permission.
    pub fn select(&self, local: Role, offer: &Offer) -> Result<Selected, LiveUnavailable> {
        pair(local, self.role)?;
        select(offer, &self.advertisement)
    }
}

/// A connection joins a broker to a publisher or a subscriber. There is no
/// direct publisher-to-subscriber connection.
pub fn pair(local: Role, remote: Role) -> Result<(), LiveUnavailable> {
    match (local, remote) {
        (Role::Broker, Role::Publisher | Role::Subscriber)
        | (Role::Publisher | Role::Subscriber, Role::Broker) => Ok(()),
        _ => Err(violation("connection does not join a broker to a peer")),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Registration {
    pub descriptor: Descriptor,
    pub finalization: Finalization,
}

/// The broker attached this incarnation. It grants no observer, takes no
/// durable custody and finalizes nothing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Registered {
    pub stream_id: String,
    pub incarnation: String,
}

/// Descriptors the host chose to list. Listing is not permission to attach.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Directory {
    pub streams: Vec<Descriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Attach {
    pub cursor: Cursor,
}

/// The current descriptor and the replay plan for the attach cursor.
/// Retained records follow from `plan.deliver_from`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Attached {
    pub descriptor: Descriptor,
    pub plan: ReplayPlan,
}

/// One line of a live-stream connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Message {
    Hello(Hello),
    Register(Registration),
    Registered(Registered),
    List {},
    Directory(Directory),
    Attach(Attach),
    Attached(Attached),
    Record { record: Record },
    Unavailable { diagnostic: LiveUnavailable },
}

impl Message {
    /// Admits one message line: bounded before parsing, schema-strict, then
    /// the semantic admission of every record, descriptor and cursor it
    /// carries.
    pub fn decode_line(line: &str) -> Result<Self, LiveUnavailable> {
        if line.len() > MAX_MESSAGE_BYTES {
            return Err(invalid_record(format!(
                "message exceeds {MAX_MESSAGE_BYTES} bytes"
            )));
        }
        let value: Value =
            serde_json::from_str(line).map_err(|_| invalid_record("message is not JSON"))?;
        Self::decode(&value)
    }

    pub fn decode(value: &Value) -> Result<Self, LiveUnavailable> {
        let message: Self = super::admit("Message", value, UnavailableReason::InvalidRecord)?;
        let descriptor = |d: &Descriptor| {
            Descriptor::decode(&serde_json::to_value(d).expect("descriptor serializes"))
        };
        match &message {
            Message::Hello(hello) => {
                let size = serde_json::to_string(&hello.advertisement)
                    .map(|text| text.len())
                    .unwrap_or(usize::MAX);
                if size > MAX_ADVERTISEMENT_BYTES {
                    return Err(LiveUnavailable::new(
                        UnavailableReason::InvalidAdvertisement,
                        format!("advertisement exceeds {MAX_ADVERTISEMENT_BYTES} bytes"),
                    ));
                }
            }
            Message::Register(registration) => {
                descriptor(&registration.descriptor)?;
            }
            Message::Directory(directory) => {
                for entry in &directory.streams {
                    descriptor(entry)?;
                }
            }
            Message::Attach(attach) => {
                Cursor::decode(&attach.cursor.encode())?;
            }
            Message::Attached(attached) => {
                descriptor(&attached.descriptor)?;
                for record in &attached.plan.prefix {
                    Record::decode(&serde_json::to_value(record).expect("record serializes"))?;
                }
            }
            Message::Record { record } => {
                Record::decode(&serde_json::to_value(record).expect("record serializes"))?;
            }
            Message::Registered(_) | Message::List {} | Message::Unavailable { .. } => {}
        }
        Ok(message)
    }

    pub fn encode_line(&self) -> String {
        serde_json::to_string(self).expect("message serializes")
    }

    /// Refuses a message the sending role never sends.
    pub fn check_sender(&self, sender: Role) -> Result<(), LiveUnavailable> {
        let allowed = match self {
            Message::Hello(_) | Message::Unavailable { .. } => true,
            Message::Register(_) => sender == Role::Publisher,
            Message::List {} | Message::Attach(_) => sender == Role::Subscriber,
            Message::Registered(_) | Message::Directory(_) | Message::Attached(_) => {
                sender == Role::Broker
            }
            Message::Record { .. } => sender != Role::Subscriber,
        };
        if allowed {
            Ok(())
        } else {
            Err(violation("message not sent by this role"))
        }
    }
}

/// The host's decision about one peer and one stream. The broker obtains it
/// from the host before admitting a registration or an attachment. Only the
/// host's own authority produces `Granted`: peer credentials, a matching UID,
/// correlations, advertisements, descriptors and control facts are inputs the
/// host may weigh, never a decision. The SDK never constructs `Granted`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostDecision {
    /// `scope` is an opaque host reference to what was established. For a
    /// `scoped` claim it must equal the claimed scope.
    Granted {
        stream_id: String,
        scope: String,
    },
    Refused,
}

fn not_authorized(detail: &str) -> LiveUnavailable {
    LiveUnavailable::new(UnavailableReason::NotAuthorized, detail)
}

/// Checks that a decision covers this descriptor. It does not check that the
/// host decided well.
fn covers(decision: &HostDecision, descriptor: &Descriptor) -> Result<(), LiveUnavailable> {
    let HostDecision::Granted { stream_id, scope } = decision else {
        return Err(not_authorized("host refused"));
    };
    validate(
        "HostRef",
        &serde_json::json!(scope),
        UnavailableReason::NotAuthorized,
    )?;
    if stream_id != &descriptor.stream_id {
        return Err(not_authorized("host decision is for another stream"));
    }
    if descriptor.visibility.audience == Audience::Scoped
        && descriptor.visibility.scope.as_ref() != Some(scope)
    {
        return Err(not_authorized("host decision is for another scope"));
    }
    Ok(())
}

/// Broker-side check of one registered publisher incarnation.
///
/// It binds the connection to the registered stream and incarnation and
/// accepts only what a publisher originates: data, control, `ended`,
/// `finalized` when declared [`Finalization::CustodyOwner`], and
/// `capture_overflow` gaps for frames it dropped. Eviction gaps and
/// discontinuities are the broker's own delivery records. An observed exit or
/// a closed connection is not an end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ingest {
    registration: Registration,
    follower: Follower,
}

impl Ingest {
    pub fn register(
        selected: &Selected,
        registration: Registration,
        decision: &HostDecision,
    ) -> Result<(Self, Registered), LiveUnavailable> {
        let descriptor = &registration.descriptor;
        covers(decision, descriptor)?;
        let follower = Follower::from_descriptor(selected, descriptor, descriptor.start())?;
        let registered = Registered {
            stream_id: descriptor.stream_id.clone(),
            incarnation: descriptor.incarnation.clone(),
        };
        Ok((
            Self {
                registration,
                follower,
            },
            registered,
        ))
    }

    pub fn descriptor(&self) -> &Descriptor {
        &self.registration.descriptor
    }

    /// Admits one publisher record. Refusals leave the state unchanged.
    pub fn accept(&mut self, record: &Record) -> Result<Accepted, LiveUnavailable> {
        match record {
            Record::Discontinuity(_)
            | Record::Gap(super::Gap {
                reason: GapReason::Evicted,
                ..
            }) => return Err(violation("broker delivery record from a publisher")),
            Record::Finalized(_) if self.registration.finalization == Finalization::Never => {
                return Err(violation("publisher declared it never finalizes"));
            }
            _ => {}
        }
        // Reader catch-up may learn terminal metadata at its current position.
        // A publisher must originate a new position, never replace or redeliver one.
        let first = match record {
            Record::Gap(gap) => gap.first,
            _ => record.sequenced().expect("publisher frame").1,
        };
        if self.last_published().checked_add(1) != Some(first) {
            return Err(violation(
                "publisher record does not start at the next position",
            ));
        }
        self.follower.accept(record)
    }

    /// The last sequence position received, including dropped positions a
    /// `capture_overflow` gap covered.
    pub fn last_published(&self) -> u64 {
        self.follower.cursor().after_seq
    }

    pub fn terminal(&self) -> Option<&Terminal> {
        self.follower.cursor().terminal.as_ref()
    }

    /// The window [`plan_replay`] needs, given what the broker still retains.
    pub fn window(
        &self,
        first_retained: u64,
        previous: Option<PreviousIncarnation>,
    ) -> RetainedWindow {
        let descriptor = self.descriptor();
        RetainedWindow {
            stream_id: descriptor.stream_id.clone(),
            incarnation: descriptor.incarnation.clone(),
            first_retained,
            last_published: self.last_published(),
            terminal: self.terminal().cloned(),
            previous,
        }
    }

    /// What a later incarnation's window may say about this one. The last
    /// sequence is known only once a terminal frame arrived: if the publisher
    /// went away first, frames it published but the broker never received
    /// stay unknown rather than reported as nothing lost.
    pub fn retire(&self) -> PreviousIncarnation {
        PreviousIncarnation {
            incarnation: self.descriptor().incarnation.clone(),
            last_seq: self.terminal().map(|_| self.last_published()),
        }
    }
}

/// Broker-side attachment: the host decision must cover the descriptor, the
/// descriptor must agree with the subscriber selection, and the window must be
/// the descriptor's. The plan is [`plan_replay`]'s.
pub fn attach(
    selected: &Selected,
    descriptor: &Descriptor,
    window: &RetainedWindow,
    request: &Attach,
    decision: &HostDecision,
) -> Result<Attached, LiveUnavailable> {
    covers(decision, descriptor)?;
    if window.stream_id != descriptor.stream_id || window.incarnation != descriptor.incarnation {
        return Err(violation("retained window is not the descriptor's"));
    }
    let plan = plan_replay(window, &request.cursor)?;
    Follower::from_descriptor(selected, descriptor, request.cursor.clone())?;
    Ok(Attached {
        descriptor: descriptor.clone(),
        plan,
    })
}

/// Subscriber-side setup from an `attached` answer to `request`: joins the
/// descriptor with the selection, applies the plan prefix, and checks that
/// retained delivery starts exactly at the follower's next position. Feed the
/// following `record` messages to the returned follower.
pub fn follow_attached(
    selected: &Selected,
    request: &Attach,
    attached: &Attached,
) -> Result<(Follower, Vec<Accepted>), LiveUnavailable> {
    validate(
        "ReplayPlan",
        &serde_json::to_value(&attached.plan).expect("plan serializes"),
        UnavailableReason::ProtocolViolation,
    )?;
    let mut follower =
        Follower::from_descriptor(selected, &attached.descriptor, request.cursor.clone())?;
    // Reconstruct only the window facts expressed by the answer, then require
    // the canonical replay shape. This checks consistency, not retention truth.
    let terminal_seq = attached
        .plan
        .terminal
        .as_ref()
        .map(|t| t.record().sequenced().expect("terminal frame").1);
    let previous = match attached.plan.prefix.first() {
        Some(Record::Discontinuity(d)) => Some(PreviousIncarnation {
            incarnation: d.previous_incarnation.clone(),
            last_seq: d.previous_last_seq,
        }),
        _ => None,
    };
    let window = RetainedWindow {
        stream_id: attached.descriptor.stream_id.clone(),
        incarnation: attached.descriptor.incarnation.clone(),
        first_retained: terminal_seq.map_or(attached.plan.deliver_from, |seq| {
            attached.plan.deliver_from.min(seq)
        }),
        last_published: terminal_seq.unwrap_or(attached.plan.deliver_from - 1),
        terminal: attached.plan.terminal.clone(),
        previous,
    };
    if plan_replay(&window, &request.cursor)? != attached.plan {
        return Err(violation("replay plan contradicts its terminal or prefix"));
    }
    let mut accepted = Vec::with_capacity(attached.plan.prefix.len());
    for record in &attached.plan.prefix {
        accepted.push(follower.accept(record)?);
    }
    let cursor = follower.cursor();
    if cursor.incarnation != attached.descriptor.incarnation
        || cursor.after_seq.checked_add(1) != Some(attached.plan.deliver_from)
    {
        return Err(violation("replay plan does not continue the cursor"));
    }
    Ok((follower, accepted))
}
