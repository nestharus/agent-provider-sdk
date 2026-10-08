//! Optional `oulipoly.live_stream/v3` live-output observation contract.
//!
//! Records a publisher (capture at a host seam), a broker and subscribers
//! exchange about live output: one stream's identity, its publisher
//! incarnation and monotonic sequence, its channels (including output whose
//! stdout/stderr origin was combined before capture), exact gaps, restarts,
//! the opaque durable reference it finalizes to, caller-owned cursors and
//! advertised visibility claims. Peers select the version by advertisement
//! ([`select`]), not by provider describe or any CLI, binary, package or
//! source identity.
//!
//! This plane is optional. It is not launch output, request custody,
//! completed replay, a resident session record, a session transcript page or a
//! retained-output offset, and it stores nothing: completed turns stay
//! canonical in the host's normal durable session storage, named here only by
//! an opaque [`FinalizedFrame::durable_reference`]. Fallible contract operations
//! return [`LiveUnavailable`] observability diagnostics for invalid public typed
//! inputs as well as decoded wire inputs. No conversion turns it into a
//! provider error, launch event or completion outcome; it disables live
//! viewing only.
//!
//! The contract owns stream, incarnation, sequence, gap, discontinuity and
//! cursor semantics. Root, session, work, generation and epoch values are
//! opaque host-owned [`Correlation`]s that it carries without interpreting or
//! registering. A [`VisibilityClaim`] is checked for shape only: whether an
//! observer is allowed is the broker's and host's to enforce. Capture, rings,
//! brokers, retention and runtime backpressure behaviour are not here.
//! [`attachment`] defines what publisher, broker and subscriber say to each
//! other and which checks each side owns; it opens no connection.
//!
//! The structural schema plus the normative semantic rules in the adjacent
//! contract README define language-independent conformance. Raw Serde supplies
//! representation only; joined agreement and stateful admission are explicit.

use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::OnceLock;

pub mod attachment;

pub const PROTOCOL: &str = "oulipoly.live_stream/v3";
pub const SCHEMA_JSON: &str = include_str!("../contract/extensions/live-stream/v3.schema.json");
/// Live-stream versions this SDK release selects. The retained v1 and v2
/// schemas are baseline material, not runtime fallbacks.
pub const SUPPORTED_VERSIONS: &[u32] = &[3];
/// Upper bound of captured bytes in one data frame; a selection may lower it.
pub const MAX_DATA_BYTES: u32 = 65_536;
/// Upper bound of one serialized record line, checked before parsing.
pub const MAX_RECORD_BYTES: usize = 90_112;
/// Upper bound of one serialized peer advertisement.
pub const MAX_ADVERTISEMENT_BYTES: usize = 16_384;
const MAX_DETAIL_CHARS: usize = 512;

const DEFINITIONS: &[&str] = &[
    "Record",
    "Descriptor",
    "Cursor",
    "Offer",
    "Advertisement",
    "LiveUnavailable",
    "VisibilityClaim",
    "Correlation",
    "HostRef",
    "DurableReference",
    "Channels",
    "MaxDataBytes",
    "Terminal",
    "RetainedWindow",
    "ReplayPlan",
    "Message",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Channel {
    Stdout,
    Stderr,
    Combined,
    Pty,
    Control,
}

/// A channel that carries captured bytes. A `pty` channel is a kind a
/// publisher may declare; nothing here requires any publisher to produce it.
///
/// `Stdout` and `Stderr` carry bytes captured from a descriptor that carried
/// only that origin. `Combined` carries one byte stream into which the
/// producer's stdout and stderr were joined before capture, for example both
/// written to one pipe: each byte's origin is unknown, and nothing here
/// splits, infers or relabels it. A descriptor never declares `combined`
/// together with `stdout` or `stderr`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataChannel {
    Stdout,
    Stderr,
    Combined,
    Pty,
}

impl From<DataChannel> for Channel {
    fn from(channel: DataChannel) -> Self {
        match channel {
            DataChannel::Stdout => Channel::Stdout,
            DataChannel::Stderr => Channel::Stderr,
            DataChannel::Combined => Channel::Combined,
            DataChannel::Pty => Channel::Pty,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Audience {
    Owner,
    SameUser,
    Scoped,
}

/// An advertised claim of intended audience and exposed channels, never a
/// guarantee that anyone enforces it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VisibilityClaim {
    pub audience: Audience,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    pub channels: Vec<Channel>,
}

/// Opaque host-owned correlations, compared only for equality.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Correlation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch: Option<String>,
}

/// What a publisher declares for one incarnation of a stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Descriptor {
    pub protocol: String,
    pub stream_id: String,
    pub incarnation: String,
    pub channels: Vec<Channel>,
    pub max_data_bytes: u32,
    pub visibility: VisibilityClaim,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation: Option<Correlation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataFrame {
    pub stream_id: String,
    pub incarnation: String,
    pub seq: u64,
    pub observed_at_unix_ms: u64,
    pub channel: DataChannel,
    pub data_base64: String,
}

/// A typed observed fact. It never commands, authorizes or acknowledges.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ControlFact {
    Heartbeat,
    ChannelClosed {
        channel: DataChannel,
    },
    /// Reports a process exit the publisher observed, not completion.
    /// No delivered fact means only no exit fact delivered on this live plane:
    /// control may be unselected or a gap may hide it. It does not establish
    /// whether the publisher observed an exit. Unknown command wait emits no
    /// such fact; wait knowledge remains in the matching durable record.
    ExitObserved {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        code: Option<i32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signal: Option<u8>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlFrame {
    pub stream_id: String,
    pub incarnation: String,
    pub seq: u64,
    pub observed_at_unix_ms: u64,
    pub fact: ControlFact,
}

/// Last frame of an incarnation, after the host's normal durable publication.
/// It names that durable record and claims nothing more: not a known or
/// successful command exit, not complete or readable retained bytes, and not
/// delivery or acknowledgement of any report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizedFrame {
    pub stream_id: String,
    pub incarnation: String,
    pub seq: u64,
    pub observed_at_unix_ms: u64,
    /// Opaque host-owned reference to the canonical durable record.
    pub durable_reference: String,
}

/// Last frame of an incarnation that claims no durable reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EndedFrame {
    pub stream_id: String,
    pub incarnation: String,
    pub seq: u64,
    pub observed_at_unix_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GapReason {
    Evicted,
    CaptureOverflow,
}

/// Frames `first..=last` of this incarnation will never be delivered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Gap {
    pub stream_id: String,
    pub incarnation: String,
    pub first: u64,
    pub last: u64,
    pub reason: GapReason,
}

/// The cursor's incarnation is not current; delivery restarts at the current
/// incarnation's beginning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Discontinuity {
    pub stream_id: String,
    pub previous_incarnation: String,
    pub after_seq: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_last_seq: Option<u64>,
    pub incarnation: String,
}

/// One line of the live stream: a sequenced publisher frame, or a gap or
/// discontinuity delivery record (which have no sequence of their own).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Record {
    Data(DataFrame),
    Control(ControlFrame),
    Finalized(FinalizedFrame),
    Ended(EndedFrame),
    Gap(Gap),
    Discontinuity(Discontinuity),
}

/// Retained terminal knowledge, represented by the complete last frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Terminal {
    Finalized(FinalizedFrame),
    Ended(EndedFrame),
}

impl Terminal {
    pub fn record(&self) -> Record {
        match self {
            Self::Finalized(frame) => Record::Finalized(frame.clone()),
            Self::Ended(frame) => Record::Ended(frame.clone()),
        }
    }
}

/// Caller-owned replay position and terminal knowledge. Persist the whole cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cursor {
    pub protocol: String,
    pub stream_id: String,
    pub incarnation: String,
    pub after_seq: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal: Option<Terminal>,
}

/// One peer's `oulipoly.live_stream/v3` advertisement entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Offer {
    pub channels: Vec<Channel>,
    pub audiences: Vec<Audience>,
    pub max_data_bytes: u32,
}

/// What two peers can both use.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Selected {
    pub protocol: String,
    pub channels: Vec<Channel>,
    pub audiences: Vec<Audience>,
    pub max_data_bytes: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticKind {
    LiveUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnavailableReason {
    BrokerAbsent,
    NoCommonVersion,
    InvalidAdvertisement,
    NoCommonCapability,
    InvalidRecord,
    ProtocolViolation,
    UnknownStream,
    /// The host refused, or its decision does not cover this stream or scope.
    NotAuthorized,
}

/// The observability diagnostic: live viewing is unavailable. Its detail
/// generated by the SDK never repeats submitted payload values. Caller-supplied
/// detail is bounded, not sanitized, and remains the caller's responsibility.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(deny_unknown_fields)]
pub struct LiveUnavailable {
    pub diagnostic: DiagnosticKind,
    pub reason: UnavailableReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl fmt::Display for LiveUnavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reason = serde_json::to_value(self.reason).expect("reason serializes");
        write!(
            f,
            "live viewing unavailable: {}",
            reason.as_str().unwrap_or("?")
        )?;
        if let Some(detail) = &self.detail {
            write!(f, " ({detail})")?;
        }
        Ok(())
    }
}

impl LiveUnavailable {
    /// Bounds caller-supplied detail; does not redact or sanitize it.
    pub fn new(reason: UnavailableReason, detail: impl Into<String>) -> Self {
        let mut detail: String = detail.into();
        if let Some((index, _)) = detail.char_indices().nth(MAX_DETAIL_CHARS) {
            detail.truncate(index);
        }
        Self {
            diagnostic: DiagnosticKind::LiveUnavailable,
            reason,
            detail: (!detail.is_empty()).then_some(detail),
        }
    }

    /// Broker absence is a diagnostic, not a launch or completion outcome.
    pub fn broker_absent() -> Self {
        Self::new(UnavailableReason::BrokerAbsent, "")
    }
}

fn violation(detail: impl Into<String>) -> LiveUnavailable {
    LiveUnavailable::new(UnavailableReason::ProtocolViolation, detail)
}

fn invalid_record(detail: impl Into<String>) -> LiveUnavailable {
    LiveUnavailable::new(UnavailableReason::InvalidRecord, detail)
}

fn validator(definition: &str) -> Option<&'static jsonschema::Validator> {
    static VALIDATORS: OnceLock<BTreeMap<&'static str, jsonschema::Validator>> = OnceLock::new();
    VALIDATORS
        .get_or_init(|| {
            let schema: Value = serde_json::from_str(SCHEMA_JSON).expect("embedded schema JSON");
            DEFINITIONS
                .iter()
                .map(|name| {
                    let wrapper = serde_json::json!({
                        "$schema": "https://json-schema.org/draft/2020-12/schema",
                        "$defs": schema["$defs"],
                        "$ref": format!("#/$defs/{name}"),
                    });
                    (
                        *name,
                        jsonschema::validator_for(&wrapper).expect("embedded live-stream schema"),
                    )
                })
                .collect()
        })
        .get(definition)
}

/// Validates `value` against one schema definition. The `Err` detail names
/// schema keywords only, never submitted values or caller-controlled keys.
/// This is structural validation; decode/join/follow/replay add semantic rules.
pub fn validate(
    definition: &str,
    value: &Value,
    reason: UnavailableReason,
) -> Result<(), LiveUnavailable> {
    let Some(validator) = validator(definition) else {
        return Err(LiveUnavailable::new(reason, "unknown definition"));
    };
    let mut keywords: Vec<String> = validator
        .iter_errors(value)
        .map(|error| error.kind().keyword().to_owned())
        .collect();
    if keywords.is_empty() {
        return Ok(());
    }
    keywords.sort();
    keywords.dedup();
    Err(LiveUnavailable::new(
        reason,
        format!("{definition}: {}", keywords.join(", ")),
    ))
}

fn admit<T: serde::de::DeserializeOwned>(
    definition: &str,
    value: &Value,
    reason: UnavailableReason,
) -> Result<T, LiveUnavailable> {
    validate(definition, value, reason)?;
    serde_json::from_value(value.clone())
        .map_err(|_| LiveUnavailable::new(reason, format!("{definition}: representation")))
}

/// Whether a channel set claims both combined and separated stdout/stderr
/// origin for one incarnation, which no follow context may declare.
fn mixes_combined_origin(channels: &[Channel]) -> bool {
    channels.contains(&Channel::Combined)
        && (channels.contains(&Channel::Stdout) || channels.contains(&Channel::Stderr))
}

impl DataFrame {
    /// The captured bytes.
    pub fn bytes(&self) -> Result<Vec<u8>, LiveUnavailable> {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(&self.data_base64)
            .map_err(|_| invalid_record("data_base64 is not canonical base64"))?;
        if bytes.is_empty() || bytes.len() > MAX_DATA_BYTES as usize {
            return Err(invalid_record(
                "data byte length is outside the contract bound",
            ));
        }
        Ok(bytes)
    }
}

impl Record {
    /// Admits one record line: bounded before parsing, schema-strict, then
    /// cross-field checks (gap order, distinct restart incarnations, data
    /// size).
    pub fn decode_line(line: &str) -> Result<Self, LiveUnavailable> {
        if line.len() > MAX_RECORD_BYTES {
            return Err(invalid_record(format!(
                "record exceeds {MAX_RECORD_BYTES} bytes"
            )));
        }
        let value: Value =
            serde_json::from_str(line).map_err(|_| invalid_record("record is not JSON"))?;
        Self::decode(&value)
    }

    pub fn decode(value: &Value) -> Result<Self, LiveUnavailable> {
        let record: Self = admit("Record", value, UnavailableReason::InvalidRecord)?;
        match &record {
            Record::Data(frame) => {
                frame.bytes()?;
            }
            Record::Gap(gap) if gap.first > gap.last => {
                return Err(invalid_record("gap first is after last"));
            }
            Record::Discontinuity(record) => {
                if record.incarnation == record.previous_incarnation {
                    return Err(invalid_record(
                        "discontinuity names the same incarnation twice",
                    ));
                }
                if record
                    .previous_last_seq
                    .is_some_and(|last| last < record.after_seq)
                {
                    return Err(invalid_record(
                        "previous_last_seq is before the cursor's after_seq",
                    ));
                }
            }
            _ => {}
        }
        Ok(record)
    }

    pub fn encode_line(&self) -> String {
        serde_json::to_string(self).expect("record serializes")
    }

    pub fn stream_id(&self) -> &str {
        match self {
            Record::Data(r) => &r.stream_id,
            Record::Control(r) => &r.stream_id,
            Record::Finalized(r) => &r.stream_id,
            Record::Ended(r) => &r.stream_id,
            Record::Gap(r) => &r.stream_id,
            Record::Discontinuity(r) => &r.stream_id,
        }
    }

    /// `(incarnation, seq)` of a sequenced publisher frame.
    fn sequenced(&self) -> Option<(&str, u64)> {
        match self {
            Record::Data(r) => Some((&r.incarnation, r.seq)),
            Record::Control(r) => Some((&r.incarnation, r.seq)),
            Record::Finalized(r) => Some((&r.incarnation, r.seq)),
            Record::Ended(r) => Some((&r.incarnation, r.seq)),
            Record::Gap(_) | Record::Discontinuity(_) => None,
        }
    }
}

impl Descriptor {
    /// Schema-strict (including that `combined` is never declared with
    /// `stdout` or `stderr`), and the visibility claim may expose only
    /// declared channels.
    pub fn decode(value: &Value) -> Result<Self, LiveUnavailable> {
        let descriptor: Self = admit("Descriptor", value, UnavailableReason::InvalidRecord)?;
        if !descriptor
            .visibility
            .channels
            .iter()
            .all(|channel| descriptor.channels.contains(channel))
        {
            return Err(invalid_record(
                "visibility claim exposes an undeclared channel",
            ));
        }
        Ok(descriptor)
    }

    /// The cursor of a subscriber that holds nothing of this incarnation.
    pub fn start(&self) -> Cursor {
        Cursor {
            protocol: PROTOCOL.to_owned(),
            stream_id: self.stream_id.clone(),
            incarnation: self.incarnation.clone(),
            after_seq: 0,
            terminal: None,
        }
    }

    /// Joins structural/semantic descriptor admission with selected capability
    /// agreement. Audience support is an advertised shape, not authorization.
    pub fn agree(&self, selected: &Selected) -> Result<(), LiveUnavailable> {
        Self::decode(&serde_json::to_value(self).expect("descriptor serializes"))?;
        if selected.protocol != PROTOCOL {
            return Err(violation("selected protocol is not supported"));
        }
        validate(
            "Offer",
            &serde_json::json!({
                "channels": selected.channels,
                "audiences": selected.audiences,
                "max_data_bytes": selected.max_data_bytes,
            }),
            UnavailableReason::ProtocolViolation,
        )?;
        if !self
            .channels
            .iter()
            .all(|channel| selected.channels.contains(channel))
            || self.max_data_bytes > selected.max_data_bytes
            || !selected.audiences.contains(&self.visibility.audience)
        {
            return Err(violation("descriptor exceeds selected capabilities"));
        }
        Ok(())
    }
}

impl Cursor {
    pub fn decode(value: &Value) -> Result<Self, LiveUnavailable> {
        let cursor: Self = admit("Cursor", value, UnavailableReason::InvalidRecord)?;
        if let Some(terminal) = &cursor.terminal {
            let record = terminal.record();
            if record.stream_id() != cursor.stream_id
                || record.sequenced() != Some((cursor.incarnation.as_str(), cursor.after_seq))
            {
                return Err(invalid_record(
                    "terminal does not match cursor identity and position",
                ));
            }
        }
        Ok(cursor)
    }

    pub fn encode(&self) -> Value {
        serde_json::to_value(self).expect("cursor serializes")
    }
}

/// A peer's advertisement carrying only this offer.
pub fn advertisement(offer: &Offer) -> Value {
    serde_json::json!({ PROTOCOL: offer })
}

/// Selects what `local` and a peer's advertisement can both use. Entries for
/// unknown, older or newer protocols are ignored; the
/// `oulipoly.live_stream/v3` entry must be strict. Every refusal is a [`LiveUnavailable`].
pub fn select(local: &Offer, remote: &Value) -> Result<Selected, LiveUnavailable> {
    let local_value = serde_json::to_value(local).expect("offer serializes");
    validate(
        "Offer",
        &local_value,
        UnavailableReason::InvalidAdvertisement,
    )
    .map_err(|_| LiveUnavailable::new(UnavailableReason::InvalidAdvertisement, "local offer"))?;
    let size = serde_json::to_string(remote)
        .map(|text| text.len())
        .unwrap_or(usize::MAX);
    if size > MAX_ADVERTISEMENT_BYTES {
        return Err(LiveUnavailable::new(
            UnavailableReason::InvalidAdvertisement,
            format!("advertisement exceeds {MAX_ADVERTISEMENT_BYTES} bytes"),
        ));
    }
    validate(
        "Advertisement",
        remote,
        UnavailableReason::InvalidAdvertisement,
    )?;
    let Some(entry) = remote.get(PROTOCOL) else {
        let others = remote.as_object().map_or(0, |map| map.len());
        return Err(LiveUnavailable::new(
            UnavailableReason::NoCommonVersion,
            format!("peer advertised {others} other protocol entries"),
        ));
    };
    let peer: Offer = admit("Offer", entry, UnavailableReason::InvalidAdvertisement)?;
    let mut channels: Vec<Channel> = local
        .channels
        .iter()
        .copied()
        .filter(|channel| peer.channels.contains(channel))
        .collect();
    channels.sort_unstable();
    let mut audiences: Vec<Audience> = local
        .audiences
        .iter()
        .copied()
        .filter(|audience| peer.audiences.contains(audience))
        .collect();
    audiences.sort_unstable();
    if channels.is_empty() || audiences.is_empty() {
        return Err(LiveUnavailable::new(
            UnavailableReason::NoCommonCapability,
            if channels.is_empty() {
                "no common channel"
            } else {
                "no common audience"
            },
        ));
    }
    Ok(Selected {
        protocol: PROTOCOL.to_owned(),
        channels,
        audiences,
        max_data_bytes: local.max_data_bytes.min(peer.max_data_bytes),
    })
}

/// How a lost range of a previous incarnation is known.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "lost", rename_all = "snake_case")]
pub enum Lost {
    Nothing,
    Exact { first: u64, last: u64 },
    Unknown,
}

/// What one delivered record meant to a [`Follower`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "accepted", rename_all = "snake_case")]
pub enum Accepted {
    Frame {
        seq: u64,
    },
    /// Positional suppression at or behind the cursor, including gapped
    /// positions. Does not establish identical content or truthful gaps.
    /// Terminal knowledge at the current position is learned separately.
    Duplicate,
    Gap {
        first: u64,
        last: u64,
    },
    Restarted {
        incarnation: String,
        #[serde(flatten)]
        lost: Lost,
    },
    Finalized {
        seq: u64,
        durable_reference: String,
    },
    Ended {
        seq: u64,
    },
}

/// Subscriber-side check that delivery from a cursor is exact: every
/// sequence is delivered, covered by a gap, or redelivered; an incarnation
/// changes only through a matching discontinuity; nothing follows the end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Follower {
    cursor: Cursor,
    channels: Vec<Channel>,
    max_data_bytes: u32,
}

impl Follower {
    /// Low-level follow setup. Validates even caller-constructed typed state.
    /// For negotiated use, prefer `from_descriptor` to join agreement.
    pub fn new(
        cursor: Cursor,
        channels: &[Channel],
        max_data_bytes: u32,
    ) -> Result<Self, LiveUnavailable> {
        Cursor::decode(&cursor.encode())?;
        validate(
            "Channels",
            &serde_json::json!(channels),
            UnavailableReason::InvalidRecord,
        )?;
        validate(
            "MaxDataBytes",
            &serde_json::json!(max_data_bytes),
            UnavailableReason::InvalidRecord,
        )?;
        if mixes_combined_origin(channels) {
            return Err(invalid_record("combined is declared with stdout or stderr"));
        }
        Ok(Self {
            cursor,
            channels: channels.to_vec(),
            max_data_bytes,
        })
    }

    /// Common negotiated setup. Does not authenticate a peer or authorize access.
    pub fn from_descriptor(
        selected: &Selected,
        descriptor: &Descriptor,
        cursor: Cursor,
    ) -> Result<Self, LiveUnavailable> {
        descriptor.agree(selected)?;
        if cursor.stream_id != descriptor.stream_id {
            return Err(violation("cursor for another descriptor stream"));
        }
        if cursor.terminal.is_some() && cursor.incarnation != descriptor.incarnation {
            return Err(violation(
                "terminal cursor cannot follow another incarnation",
            ));
        }
        Self::new(cursor, &descriptor.channels, descriptor.max_data_bytes)
    }

    /// The caller-owned cursor after everything accepted so far.
    pub fn cursor(&self) -> &Cursor {
        &self.cursor
    }

    pub fn ended(&self) -> bool {
        self.cursor.terminal.is_some()
    }

    pub fn accept(&mut self, record: &Record) -> Result<Accepted, LiveUnavailable> {
        // Public structs/raw Serde may bypass decode. Admit before dedupe or mutation.
        Record::decode_line(&record.encode_line())?;
        if record.stream_id() != self.cursor.stream_id {
            return Err(violation("record for another stream"));
        }
        if let Some((incarnation, seq)) = record.sequenced() {
            if incarnation != self.cursor.incarnation {
                return Err(violation(
                    "frame of another incarnation without a discontinuity",
                ));
            }
            if seq == self.cursor.after_seq {
                let terminal = match record {
                    Record::Finalized(frame) => Some(Terminal::Finalized(frame.clone())),
                    Record::Ended(frame) => Some(Terminal::Ended(frame.clone())),
                    _ => None,
                };
                if let Some(terminal) = terminal {
                    if let Some(known) = &self.cursor.terminal {
                        if known != &terminal {
                            return Err(violation("conflicting terminal knowledge"));
                        }
                    } else {
                        self.cursor.terminal = Some(terminal);
                        return Ok(match record {
                            Record::Finalized(frame) => Accepted::Finalized {
                                seq,
                                durable_reference: frame.durable_reference.clone(),
                            },
                            _ => Accepted::Ended { seq },
                        });
                    }
                }
            }
            if seq <= self.cursor.after_seq {
                return Ok(Accepted::Duplicate);
            }
            if self.ended() {
                return Err(violation("frame after the incarnation ended"));
            }
            if seq != self.cursor.after_seq.saturating_add(1) {
                return Err(violation("sequence skips after cursor without a gap"));
            }
        }
        let accepted = match record {
            Record::Data(frame) => {
                if !self.channels.contains(&frame.channel.into()) {
                    return Err(violation("data on an undeclared channel"));
                }
                if frame.bytes()?.len() > self.max_data_bytes as usize {
                    return Err(violation("data exceeds the selected max_data_bytes"));
                }
                Accepted::Frame { seq: frame.seq }
            }
            Record::Control(frame) => {
                if !self.channels.contains(&Channel::Control) {
                    return Err(violation(
                        "control fact on a stream without a control channel",
                    ));
                }
                if let ControlFact::ChannelClosed { channel } = frame.fact {
                    if !self.channels.contains(&channel.into()) {
                        return Err(violation("closed an undeclared channel"));
                    }
                }
                Accepted::Frame { seq: frame.seq }
            }
            Record::Finalized(frame) => {
                self.cursor.terminal = Some(Terminal::Finalized(frame.clone()));
                Accepted::Finalized {
                    seq: frame.seq,
                    durable_reference: frame.durable_reference.clone(),
                }
            }
            Record::Ended(frame) => {
                self.cursor.terminal = Some(Terminal::Ended(frame.clone()));
                Accepted::Ended { seq: frame.seq }
            }
            Record::Gap(gap) => {
                if gap.incarnation != self.cursor.incarnation {
                    return Err(violation("gap of another incarnation"));
                }
                if gap.last <= self.cursor.after_seq {
                    return Ok(Accepted::Duplicate);
                }
                if self.ended() {
                    return Err(violation("gap after the incarnation ended"));
                }
                if gap.first != self.cursor.after_seq.saturating_add(1) {
                    return Err(violation("gap does not start immediately after cursor"));
                }
                self.cursor.after_seq = gap.last;
                return Ok(Accepted::Gap {
                    first: gap.first,
                    last: gap.last,
                });
            }
            Record::Discontinuity(record) => {
                if record.previous_incarnation != self.cursor.incarnation
                    || record.after_seq != self.cursor.after_seq
                {
                    return Err(violation("discontinuity does not answer this cursor"));
                }
                if self.ended() {
                    return Err(violation("discontinuity after the incarnation ended"));
                }
                let lost = match record.previous_last_seq {
                    None => Lost::Unknown,
                    Some(last) if last == record.after_seq => Lost::Nothing,
                    Some(last) => Lost::Exact {
                        first: record.after_seq.saturating_add(1),
                        last,
                    },
                };
                self.cursor.incarnation = record.incarnation.clone();
                self.cursor.after_seq = 0;
                return Ok(Accepted::Restarted {
                    incarnation: record.incarnation.clone(),
                    lost,
                });
            }
        };
        self.cursor.after_seq += 1;
        Ok(accepted)
    }
}

/// A broker's retained window of one stream, as the replay oracle sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedWindow {
    pub stream_id: String,
    pub incarnation: String,
    /// Oldest retained sequence; `last_published + 1` when nothing is retained.
    pub first_retained: u64,
    /// 0 when nothing has been published.
    pub last_published: u64,
    /// Retained last frame, required once this incarnation ends. Never evicted
    /// while the broker knows the stream, even at a caught-up cursor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal: Option<Terminal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous: Option<PreviousIncarnation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreviousIncarnation {
    pub incarnation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seq: Option<u64>,
}

/// Records to deliver before retained frames, and the first retained
/// sequence to deliver.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayPlan {
    pub prefix: Vec<Record>,
    pub deliver_from: u64,
    /// The retained terminal metadata, also supplied at an already-final cursor.
    /// Lagging followers learn it when the terminal frame is actually delivered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal: Option<Terminal>,
}

/// Reference semantics for resuming `cursor` against a retained window: an
/// exact eviction gap, an explicit discontinuity for another incarnation, or
/// a diagnostic. It never yields content the window does not hold.
pub fn plan_replay(
    window: &RetainedWindow,
    cursor: &Cursor,
) -> Result<ReplayPlan, LiveUnavailable> {
    validate(
        "RetainedWindow",
        &serde_json::to_value(window).expect("window serializes"),
        UnavailableReason::ProtocolViolation,
    )?;
    Cursor::decode(&cursor.encode())?;
    if window.first_retained > window.last_published + 1 {
        return Err(violation("retained window is inconsistent"));
    }
    if cursor.stream_id != window.stream_id {
        return Err(LiveUnavailable::new(UnavailableReason::UnknownStream, ""));
    }
    if let Some(previous) = &window.previous {
        if previous.incarnation == window.incarnation {
            return Err(violation("previous incarnation is current"));
        }
    }
    if let Some(terminal) = &window.terminal {
        let record = terminal.record();
        if record.stream_id() != window.stream_id
            || record.sequenced() != Some((window.incarnation.as_str(), window.last_published))
            || window.first_retained > window.last_published
        {
            return Err(violation("terminal is inconsistent with retained window"));
        }
    }
    if let Some(terminal) = &cursor.terminal {
        if cursor.incarnation != window.incarnation || window.terminal.as_ref() != Some(terminal) {
            return Err(violation("window contradicts terminal cursor"));
        }
    }
    let mut prefix = Vec::new();
    let after_seq = if cursor.incarnation == window.incarnation {
        cursor.after_seq
    } else {
        let previous_last_seq = window
            .previous
            .as_ref()
            .filter(|previous| previous.incarnation == cursor.incarnation)
            .and_then(|previous| previous.last_seq);
        if previous_last_seq.is_some_and(|last| cursor.after_seq > last) {
            return Err(violation("cursor is ahead of its incarnation"));
        }
        prefix.push(Record::Discontinuity(Discontinuity {
            stream_id: window.stream_id.clone(),
            previous_incarnation: cursor.incarnation.clone(),
            after_seq: cursor.after_seq,
            previous_last_seq,
            incarnation: window.incarnation.clone(),
        }));
        0
    };
    if after_seq > window.last_published {
        return Err(violation("cursor is ahead of the stream"));
    }
    if after_seq == window.last_published {
        if let Some(terminal) = &window.terminal {
            prefix.push(terminal.record());
        }
    }
    if after_seq.saturating_add(1) < window.first_retained {
        prefix.push(Record::Gap(Gap {
            stream_id: window.stream_id.clone(),
            incarnation: window.incarnation.clone(),
            first: after_seq.saturating_add(1),
            last: window.first_retained - 1,
            reason: GapReason::Evicted,
        }));
        return Ok(ReplayPlan {
            prefix,
            deliver_from: window.first_retained,
            terminal: window.terminal.clone(),
        });
    }
    Ok(ReplayPlan {
        prefix,
        deliver_from: after_seq.saturating_add(1),
        terminal: window.terminal.clone(),
    })
}
