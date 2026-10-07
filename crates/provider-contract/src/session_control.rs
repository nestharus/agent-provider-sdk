//! `oulipoly.session_control/v1`: one shared session/root control vocabulary.
//!
//! Records a requester and the existing root owner exchange about a control
//! request, and settlement observations about logical work. The same claims
//! serve session control and infrastructure control; there is no second
//! meaning of hold, acknowledgment or idempotency elsewhere. Peers select the
//! version by advertisement ([`select`]), not by provider describe or any CLI,
//! binary, package or source identity. Providers do not speak it: it is not a
//! provider/v1 subcommand or a second host/provider protocol.
//!
//! The claim ladder keeps distinct records for requester intent
//! ([`Request`]), transport receipt ([`Receipt`]), admission ([`Admission`]),
//! semantic transition acknowledgment ([`Acknowledgment`]) or refusal
//! ([`Refusal`]), and the request's final [`Outcome`]. An [`Observation`]
//! reports one settlement fact — insertion acknowledgment, tagged turn end,
//! logical settlement/debt or physical custody/waits — and is never a control.
//!
//! v1 defines one operation pair: `input_hold` holds admission of new input
//! at a logical scope and `input_release` clears it. Running work and tools
//! may continue; neither claims drain, close, cancel or execution pause.
//!
//! Every value here is a claim. Validation checks shape, cross-field meaning
//! and claim order ([`RequestTrace`]); it does not establish that a producer
//! told the truth, that a requester was authorized, that a hold was enforced
//! or durable, or that any actor is in custody. Root, owner, generation,
//! incarnation, requester and logical ids are opaque host values compared
//! only for equality: the SDK is not their authority and keeps no registry.
//! It executes nothing and stores nothing. Refusals of the contract itself are
//! [`ControlUnavailable`] diagnostics with no conversion to provider errors,
//! launch events, launch unavailability or completion outcomes.
//!
//! The structural schema plus the normative semantic rules in the adjacent
//! contract README define language-independent conformance. Raw Serde
//! supplies representation only.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::OnceLock;

pub const PROTOCOL: &str = "oulipoly.session_control/v1";
pub const SCHEMA_JSON: &str = include_str!("../contract/extensions/session-control/v1.schema.json");
/// Session-control versions this SDK release defines.
pub const SUPPORTED_VERSIONS: &[u32] = &[1];
/// Upper bound of one serialized record line, checked before parsing.
pub const MAX_RECORD_BYTES: usize = 16_384;
/// Upper bound of one serialized peer advertisement.
pub const MAX_ADVERTISEMENT_BYTES: usize = 16_384;
/// Distinct receipts one [`RequestTrace`] retains for duplicate detection.
pub const MAX_TRACE_RECEIPTS: usize = 8;
const MAX_DETAIL_CHARS: usize = 512;

const DEFINITIONS: &[&str] = &[
    "Record",
    "Request",
    "Observation",
    "Authority",
    "LogicalRef",
    "Offer",
    "Advertisement",
    "ControlUnavailable",
];

/// The existing root authority a request addresses or a response comes from.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Authority {
    pub root: String,
    pub owner: String,
    pub generation: String,
    pub incarnation: String,
}

/// A root, optionally narrowed to one logical child.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlScope {
    pub root: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child: Option<String>,
}

/// A logical root/child/work/input link. Actor identities are separate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalRef {
    pub root: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    /// Hold admission of new input at the scope. Running work may continue.
    InputHold,
    /// Clear an input hold through the same root authority.
    InputRelease,
}

impl Operation {
    /// The hold state an acknowledgment of this operation moves to.
    pub fn target(self) -> HoldState {
        match self {
            Self::InputHold => HoldState::InputHeld,
            Self::InputRelease => HoldState::InputOpen,
        }
    }
}

/// Admission/input hold state only; `input_held` is not paused execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HoldState {
    InputHeld,
    InputOpen,
    Unknown,
}

/// Why a value is absent. None of these is a negative observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissingReason {
    NotCaptured,
    AccessDenied,
    Unsupported,
    NotApplicable,
}

/// Bounded text, or an explicit redaction or missing marker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum DisclosedText {
    Present { text: String },
    Redacted,
    Missing { reason: MissingReason },
}

/// An opaque host reference, or an explicit redaction or missing marker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum DisclosedRef {
    Present {
        #[serde(rename = "ref")]
        reference: String,
    },
    Redacted,
    Missing {
        reason: MissingReason,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Actor {
    OsProcess,
    BashHandle,
    ProviderSession,
    LiveStream,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Exactness {
    Exact,
    Legacy,
    Incomplete,
}

/// Actor identity evidence attached to a logical link, never a logical key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActorEvidence {
    pub actor: Actor,
    pub exactness: Exactness,
    #[serde(rename = "ref")]
    pub reference: DisclosedRef,
}

/// Requester intent. Not receipt, admission, acknowledgment or outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub protocol: String,
    pub request_key: String,
    pub requester: String,
    pub addressed: Authority,
    pub operation: Operation,
    pub scope: ControlScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<DisclosedText>,
}

/// Transport receipt. `durable` is the receiver's retention claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub protocol: String,
    pub request_key: String,
    pub requester: String,
    pub addressed: Authority,
    pub durable: bool,
    pub observed_at_unix_ms: u64,
}

/// The addressed authority admitted the request for its transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Admission {
    pub protocol: String,
    pub request_key: String,
    pub requester: String,
    pub addressed: Authority,
    pub responder: Authority,
    pub observed_at_unix_ms: u64,
}

/// The input hold state moved from `from` to `to` at the responder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Acknowledgment {
    pub protocol: String,
    pub request_key: String,
    pub requester: String,
    pub addressed: Authority,
    pub responder: Authority,
    pub operation: Operation,
    pub from: HoldState,
    pub to: HoldState,
    pub observed_at_unix_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefusalStage {
    Admission,
    Transition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefusalReason {
    StaleAuthority,
    KeyConflict,
    UnsupportedOperation,
    NotPermitted,
    UnknownScope,
    AlreadyTerminal,
    TransitionFailed,
}

impl RefusalReason {
    pub fn stage(self) -> RefusalStage {
        match self {
            Self::AlreadyTerminal | Self::TransitionFailed => RefusalStage::Transition,
            _ => RefusalStage::Admission,
        }
    }
}

/// An explicit refusal: an outcome, not an absent acknowledgment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Refusal {
    pub protocol: String,
    pub request_key: String,
    pub requester: String,
    pub addressed: Authority,
    pub stage: RefusalStage,
    pub reason: RefusalReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub responder: Option<Authority>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<DisclosedText>,
    pub observed_at_unix_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeResult {
    Acknowledged,
    Refused,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Uncertainty {
    AuthorityChanged,
    TransportLost,
    EvidenceUnavailable,
}

/// What is finally known about one request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Outcome {
    pub protocol: String,
    pub request_key: String,
    pub requester: String,
    pub addressed: Authority,
    pub result: OutcomeResult,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uncertainty: Option<Uncertainty>,
    pub observed_at_unix_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactType {
    Insertion,
    TaggedEnd,
    LogicalSettlement,
    PhysicalCustody,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InsertionState {
    Acknowledged,
    NotInserted,
    Uncertain,
    Missing,
    Redacted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaggedEndState {
    Observed,
    Absent,
    Missing,
    Redacted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogicalSettlementState {
    Settled,
    Owed,
    Missing,
    Redacted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhysicalCustodyState {
    Live,
    ExitedWaitPending,
    ExitedWaited,
    Unsettled,
    Missing,
    Redacted,
}

/// One settlement fact. The four types are distinct claims.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Fact {
    Insertion {
        state: InsertionState,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        missing_reason: Option<MissingReason>,
    },
    TaggedEnd {
        state: TaggedEndState,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        missing_reason: Option<MissingReason>,
    },
    LogicalSettlement {
        state: LogicalSettlementState,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        missing_reason: Option<MissingReason>,
    },
    PhysicalCustody {
        state: PhysicalCustodyState,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        missing_reason: Option<MissingReason>,
    },
}

impl Fact {
    pub fn fact_type(&self) -> FactType {
        match self {
            Self::Insertion { .. } => FactType::Insertion,
            Self::TaggedEnd { .. } => FactType::TaggedEnd,
            Self::LogicalSettlement { .. } => FactType::LogicalSettlement,
            Self::PhysicalCustody { .. } => FactType::PhysicalCustody,
        }
    }
}

/// A reported settlement fact about a logical link. Never a control.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observation {
    pub protocol: String,
    pub reporter: Authority,
    pub subject: LogicalRef,
    pub fact: Fact,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<ActorEvidence>,
    pub observed_at_unix_ms: u64,
}

/// One control record line.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Record {
    Request(Request),
    Receipt(Receipt),
    Admission(Admission),
    Acknowledgment(Acknowledgment),
    Refusal(Refusal),
    Outcome(Outcome),
    Observation(Observation),
}

/// One peer's `oulipoly.session_control/v1` advertisement entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Offer {
    pub operations: Vec<Operation>,
    pub facts: Vec<FactType>,
}

/// What two peers can both use.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Selected {
    pub protocol: String,
    pub operations: Vec<Operation>,
    pub facts: Vec<FactType>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticKind {
    ControlUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnavailableReason {
    NoCommonVersion,
    InvalidAdvertisement,
    NoCommonCapability,
    InvalidRecord,
    ProtocolViolation,
}

/// The control capability diagnostic. It is not provider launch or completion
/// unavailability. SDK-generated detail never repeats submitted values;
/// caller-supplied detail is bounded, not sanitized.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(deny_unknown_fields)]
pub struct ControlUnavailable {
    pub diagnostic: DiagnosticKind,
    pub reason: UnavailableReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl fmt::Display for ControlUnavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reason = serde_json::to_value(self.reason).expect("reason serializes");
        write!(
            f,
            "session control unavailable: {}",
            reason.as_str().unwrap_or("?")
        )?;
        if let Some(detail) = &self.detail {
            write!(f, " ({detail})")?;
        }
        Ok(())
    }
}

impl ControlUnavailable {
    /// Bounds caller-supplied detail; does not redact or sanitize it.
    pub fn new(reason: UnavailableReason, detail: impl Into<String>) -> Self {
        let mut detail: String = detail.into();
        if let Some((index, _)) = detail.char_indices().nth(MAX_DETAIL_CHARS) {
            detail.truncate(index);
        }
        Self {
            diagnostic: DiagnosticKind::ControlUnavailable,
            reason,
            detail: (!detail.is_empty()).then_some(detail),
        }
    }
}

fn violation(detail: &str) -> ControlUnavailable {
    ControlUnavailable::new(UnavailableReason::ProtocolViolation, detail)
}

fn invalid_record(detail: &str) -> ControlUnavailable {
    ControlUnavailable::new(UnavailableReason::InvalidRecord, detail)
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
                        jsonschema::validator_for(&wrapper)
                            .expect("embedded session-control schema"),
                    )
                })
                .collect()
        })
        .get(definition)
}

/// Validates `value` against one schema definition. The `Err` detail names
/// schema keywords only, never submitted values or caller-controlled keys.
/// This is structural validation; decoding adds the semantic rules.
pub fn validate(
    definition: &str,
    value: &Value,
    reason: UnavailableReason,
) -> Result<(), ControlUnavailable> {
    let Some(validator) = validator(definition) else {
        return Err(ControlUnavailable::new(reason, "unknown definition"));
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
    Err(ControlUnavailable::new(
        reason,
        format!("{definition}: {}", keywords.join(", ")),
    ))
}

fn admit<T: serde::de::DeserializeOwned>(
    definition: &str,
    value: &Value,
    reason: UnavailableReason,
) -> Result<T, ControlUnavailable> {
    validate(definition, value, reason)?;
    serde_json::from_value(value.clone())
        .map_err(|_| ControlUnavailable::new(reason, format!("{definition}: representation")))
}

impl Record {
    /// Admits one record line: bounded before parsing, schema-strict, then
    /// the normative cross-field rules.
    pub fn decode_line(line: &str) -> Result<Self, ControlUnavailable> {
        if line.len() > MAX_RECORD_BYTES {
            return Err(ControlUnavailable::new(
                UnavailableReason::InvalidRecord,
                format!("record exceeds {MAX_RECORD_BYTES} bytes"),
            ));
        }
        let value: Value =
            serde_json::from_str(line).map_err(|_| invalid_record("record is not JSON"))?;
        Self::decode(&value)
    }

    pub fn decode(value: &Value) -> Result<Self, ControlUnavailable> {
        let record: Self = admit("Record", value, UnavailableReason::InvalidRecord)?;
        record.check()?;
        Ok(record)
    }

    pub fn encode_line(&self) -> String {
        serde_json::to_string(self).expect("record serializes")
    }

    /// Normative cross-field rules beyond the structural schema.
    fn check(&self) -> Result<(), ControlUnavailable> {
        match self {
            Record::Request(request) => {
                if request.scope.root != request.addressed.root {
                    return Err(invalid_record("scope root differs from addressed root"));
                }
            }
            Record::Admission(admission) => {
                if admission.responder != admission.addressed {
                    return Err(invalid_record(
                        "admission responder is not the addressed authority",
                    ));
                }
            }
            Record::Acknowledgment(ack) => {
                if ack.responder != ack.addressed {
                    return Err(invalid_record(
                        "acknowledgment responder is not the addressed authority",
                    ));
                }
                if ack.to != ack.operation.target() {
                    return Err(invalid_record(
                        "acknowledged state does not match the operation",
                    ));
                }
            }
            Record::Refusal(refusal) => {
                if refusal.stage != refusal.reason.stage() {
                    return Err(invalid_record("refusal reason belongs to another stage"));
                }
                let stale = refusal.reason == RefusalReason::StaleAuthority;
                if let Some(responder) = &refusal.responder {
                    if (responder == &refusal.addressed) == stale {
                        return Err(invalid_record(if stale {
                            "stale_authority refusal from the addressed authority itself"
                        } else {
                            "refusal responder is not the addressed authority"
                        }));
                    }
                    if stale && responder.root != refusal.addressed.root {
                        return Err(invalid_record("stale_authority responder is another root"));
                    }
                }
            }
            Record::Observation(observation) => {
                for evidence in &observation.evidence {
                    if evidence.exactness == Exactness::Exact
                        && !matches!(evidence.reference, DisclosedRef::Present { .. })
                    {
                        return Err(invalid_record("exact actor evidence without a reference"));
                    }
                }
            }
            Record::Receipt(_) | Record::Outcome(_) => {}
        }
        Ok(())
    }

    /// `(request_key, requester, addressed)` of a control record; `None` for
    /// an observation.
    pub fn correlation(&self) -> Option<(&str, &str, &Authority)> {
        let (key, requester, addressed) = match self {
            Record::Request(r) => (&r.request_key, &r.requester, &r.addressed),
            Record::Receipt(r) => (&r.request_key, &r.requester, &r.addressed),
            Record::Admission(r) => (&r.request_key, &r.requester, &r.addressed),
            Record::Acknowledgment(r) => (&r.request_key, &r.requester, &r.addressed),
            Record::Refusal(r) => (&r.request_key, &r.requester, &r.addressed),
            Record::Outcome(r) => (&r.request_key, &r.requester, &r.addressed),
            Record::Observation(_) => return None,
        };
        Some((key, requester, addressed))
    }
}

impl Request {
    /// Admits a typed request through its wire form.
    pub fn admit(&self) -> Result<(), ControlUnavailable> {
        Record::decode(&serde_json::to_value(Record::Request(self.clone())).expect("serializes"))
            .map(|_| ())
    }

    /// The request is admissible and its operation was selected.
    pub fn agree(&self, selected: &Selected) -> Result<(), ControlUnavailable> {
        self.admit()?;
        check_selected(selected)?;
        if !selected.operations.contains(&self.operation) {
            return Err(ControlUnavailable::new(
                UnavailableReason::NoCommonCapability,
                "operation was not selected",
            ));
        }
        Ok(())
    }
}

impl Observation {
    /// The observation is admissible and its fact type was selected.
    pub fn agree(&self, selected: &Selected) -> Result<(), ControlUnavailable> {
        Record::decode(
            &serde_json::to_value(Record::Observation(self.clone())).expect("serializes"),
        )?;
        check_selected(selected)?;
        if !selected.facts.contains(&self.fact.fact_type()) {
            return Err(ControlUnavailable::new(
                UnavailableReason::NoCommonCapability,
                "fact type was not selected",
            ));
        }
        Ok(())
    }
}

fn check_selected(selected: &Selected) -> Result<(), ControlUnavailable> {
    if selected.protocol != PROTOCOL {
        return Err(violation("selected protocol is not supported"));
    }
    validate(
        "Offer",
        &serde_json::json!({"operations": selected.operations, "facts": selected.facts}),
        UnavailableReason::ProtocolViolation,
    )
}

/// A peer's advertisement carrying only this offer.
pub fn advertisement(offer: &Offer) -> Value {
    serde_json::json!({ PROTOCOL: offer })
}

/// Selects what `local` and a peer's advertisement can both use. Unknown or
/// newer entries are ignored; the v1 entry must be strict. Absent or
/// incompatible control capability is a [`ControlUnavailable`] diagnostic.
pub fn select(local: &Offer, remote: &Value) -> Result<Selected, ControlUnavailable> {
    validate(
        "Offer",
        &serde_json::to_value(local).expect("offer serializes"),
        UnavailableReason::InvalidAdvertisement,
    )
    .map_err(|_| ControlUnavailable::new(UnavailableReason::InvalidAdvertisement, "local offer"))?;
    let size = serde_json::to_string(remote)
        .map(|text| text.len())
        .unwrap_or(usize::MAX);
    if size > MAX_ADVERTISEMENT_BYTES {
        return Err(ControlUnavailable::new(
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
        return Err(ControlUnavailable::new(
            UnavailableReason::NoCommonVersion,
            format!("peer advertised {others} other protocol entries"),
        ));
    };
    let peer: Offer = admit("Offer", entry, UnavailableReason::InvalidAdvertisement)?;
    let mut operations: Vec<Operation> = local
        .operations
        .iter()
        .copied()
        .filter(|operation| peer.operations.contains(operation))
        .collect();
    operations.sort_unstable();
    let mut facts: Vec<FactType> = local
        .facts
        .iter()
        .copied()
        .filter(|fact| peer.facts.contains(fact))
        .collect();
    facts.sort_unstable();
    if operations.is_empty() {
        return Err(ControlUnavailable::new(
            UnavailableReason::NoCommonCapability,
            "no common operation",
        ));
    }
    Ok(Selected {
        protocol: PROTOCOL.to_owned(),
        operations,
        facts,
    })
}

/// How a second request relates to a first one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Repetition {
    /// Same key scope and identical content: the same request, which must get
    /// the same answers and cause no second transition.
    SameRequest,
    /// Same key scope with different content, including another addressed
    /// generation: refuse with `key_conflict`; never treat as a retry.
    KeyConflict,
    /// Another key scope: an independent request.
    Distinct,
}

/// Classifies repetition by `(requester, addressed root, request_key)`.
pub fn classify_repetition(first: &Request, again: &Request) -> Repetition {
    if first.requester != again.requester
        || first.addressed.root != again.addressed.root
        || first.request_key != again.request_key
    {
        Repetition::Distinct
    } else if first == again {
        Repetition::SameRequest
    } else {
        Repetition::KeyConflict
    }
}

/// What one record meant to a [`RequestTrace`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "step", rename_all = "snake_case")]
pub enum Step {
    /// Transport receipt only.
    Received {
        durable: bool,
    },
    Admitted,
    Acknowledged {
        from: HoldState,
        to: HoldState,
    },
    Refused {
        stage: RefusalStage,
        reason: RefusalReason,
    },
    Concluded {
        result: OutcomeResult,
    },
    /// An identical redelivery of a record already accepted. No new meaning.
    Duplicate,
}

/// Requester-side check of the claims answering one request: correlation,
/// claim order and non-contradiction. It is caller-owned per-request state,
/// not a registry or executor, and does not establish that any claim is true.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestTrace {
    request: Request,
    receipts: Vec<Receipt>,
    admission: Option<Admission>,
    acknowledgment: Option<Acknowledgment>,
    refusal: Option<Refusal>,
    outcome: Option<Outcome>,
}

impl RequestTrace {
    pub fn new(request: Request) -> Result<Self, ControlUnavailable> {
        request.admit()?;
        Ok(Self {
            request,
            receipts: Vec::new(),
            admission: None,
            acknowledgment: None,
            refusal: None,
            outcome: None,
        })
    }

    pub fn request(&self) -> &Request {
        &self.request
    }
    pub fn admission(&self) -> Option<&Admission> {
        self.admission.as_ref()
    }
    pub fn acknowledgment(&self) -> Option<&Acknowledgment> {
        self.acknowledgment.as_ref()
    }
    pub fn refusal(&self) -> Option<&Refusal> {
        self.refusal.as_ref()
    }
    pub fn outcome(&self) -> Option<&Outcome> {
        self.outcome.as_ref()
    }

    pub fn accept(&mut self, record: &Record) -> Result<Step, ControlUnavailable> {
        // Public structs/raw Serde may bypass decode. Admit before mutation.
        Record::decode_line(&record.encode_line())?;
        let Some((key, requester, addressed)) = record.correlation() else {
            return Err(violation("an observation is not a control response"));
        };
        if key != self.request.request_key
            || requester != self.request.requester
            || addressed != &self.request.addressed
        {
            return Err(violation("record answers another request"));
        }
        if self.is_duplicate(record) {
            return Ok(Step::Duplicate);
        }
        if self.outcome.is_some() {
            return Err(violation("record after the request's outcome"));
        }
        match record {
            Record::Request(_) => Err(violation("same key with a different request")),
            Record::Observation(_) => unreachable!("observations have no correlation"),
            Record::Receipt(receipt) => {
                if self.receipts.len() >= MAX_TRACE_RECEIPTS {
                    return Err(violation("receipt bound exceeded"));
                }
                self.receipts.push(receipt.clone());
                Ok(Step::Received {
                    durable: receipt.durable,
                })
            }
            Record::Admission(admission) => {
                if self.admission.is_some() || self.refusal.is_some() {
                    return Err(violation("admission contradicts an earlier claim"));
                }
                self.admission = Some(admission.clone());
                Ok(Step::Admitted)
            }
            Record::Acknowledgment(ack) => {
                if ack.operation != self.request.operation {
                    return Err(violation("acknowledgment of another operation"));
                }
                if self.admission.is_none() {
                    return Err(violation("acknowledgment before admission"));
                }
                if self.acknowledgment.is_some() || self.refusal.is_some() {
                    return Err(violation("acknowledgment contradicts an earlier claim"));
                }
                self.acknowledgment = Some(ack.clone());
                Ok(Step::Acknowledged {
                    from: ack.from,
                    to: ack.to,
                })
            }
            Record::Refusal(refusal) => {
                if self.refusal.is_some() || self.acknowledgment.is_some() {
                    return Err(violation("refusal contradicts an earlier claim"));
                }
                match (refusal.stage, self.admission.is_some()) {
                    (RefusalStage::Admission, true) => {
                        return Err(violation("admission refusal after admission"));
                    }
                    (RefusalStage::Transition, false) => {
                        return Err(violation("transition refusal before admission"));
                    }
                    _ => {}
                }
                self.refusal = Some(refusal.clone());
                Ok(Step::Refused {
                    stage: refusal.stage,
                    reason: refusal.reason,
                })
            }
            Record::Outcome(outcome) => {
                let consistent = match outcome.result {
                    OutcomeResult::Acknowledged => self.acknowledgment.is_some(),
                    OutcomeResult::Refused => self.refusal.is_some(),
                    // Uncertainty is retained alongside, never instead of,
                    // an earlier acknowledgment or refusal.
                    OutcomeResult::Unknown => true,
                };
                if !consistent {
                    return Err(violation("outcome contradicts the recorded claims"));
                }
                self.outcome = Some(outcome.clone());
                Ok(Step::Concluded {
                    result: outcome.result,
                })
            }
        }
    }

    fn is_duplicate(&self, record: &Record) -> bool {
        match record {
            Record::Request(request) => request == &self.request,
            Record::Receipt(receipt) => self.receipts.contains(receipt),
            Record::Admission(admission) => self.admission.as_ref() == Some(admission),
            Record::Acknowledgment(ack) => self.acknowledgment.as_ref() == Some(ack),
            Record::Refusal(refusal) => self.refusal.as_ref() == Some(refusal),
            Record::Outcome(outcome) => self.outcome.as_ref() == Some(outcome),
            Record::Observation(_) => false,
        }
    }
}

/// What the observations say about one settlement fact of one subject.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reading", content = "state", rename_all = "snake_case")]
pub enum FactReading<T> {
    /// No observation of this fact for the subject. Not a negative.
    NotObserved,
    Reported(T),
    /// Observations disagree. The disagreement is retained, not resolved.
    Conflicting,
}

impl<T: PartialEq + Copy> FactReading<T> {
    fn add(&mut self, state: T) {
        *self = match *self {
            Self::NotObserved => Self::Reported(state),
            Self::Reported(seen) if seen == state => Self::Reported(seen),
            _ => Self::Conflicting,
        };
    }
}

/// Logical settlement as read from insertion, tagged end and debt only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogicalReading {
    /// Insertion acknowledged, tagged end observed and logical debt settled.
    Settled,
    /// Debt is reported owed, or insertion was acknowledged with a known
    /// absent tagged end.
    Owed,
    /// Insertion positively reported absent and no tagged end observed.
    NotInserted,
    /// Anything else, including missing, redacted, uncertain or conflicting.
    Unknown,
}

/// The four settlement facts of one logical subject, kept apart, plus the
/// logical reading derived from the logical facts alone. Physical custody
/// never enters the logical reading: an exit or completed wait is not
/// logical settlement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementReading {
    pub insertion: FactReading<InsertionState>,
    pub tagged_end: FactReading<TaggedEndState>,
    pub logical_settlement: FactReading<LogicalSettlementState>,
    pub physical_custody: FactReading<PhysicalCustodyState>,
    pub logical: LogicalReading,
}

/// Reads the observations whose subject equals `subject`. Reporter authority
/// is provenance, not a filter: an earlier generation's acknowledgment is not
/// erased by a successor. This reads claims; it does not establish them.
pub fn read_settlement(subject: &LogicalRef, observations: &[Observation]) -> SettlementReading {
    let mut insertion = FactReading::NotObserved;
    let mut tagged_end = FactReading::NotObserved;
    let mut logical_settlement = FactReading::NotObserved;
    let mut physical_custody = FactReading::NotObserved;
    for observation in observations.iter().filter(|o| &o.subject == subject) {
        match observation.fact {
            Fact::Insertion { state, .. } => insertion.add(state),
            Fact::TaggedEnd { state, .. } => tagged_end.add(state),
            Fact::LogicalSettlement { state, .. } => logical_settlement.add(state),
            Fact::PhysicalCustody { state, .. } => physical_custody.add(state),
        }
    }
    use FactReading::Reported;
    let logical = match (insertion, tagged_end, logical_settlement) {
        (FactReading::Conflicting, _, _)
        | (_, FactReading::Conflicting, _)
        | (_, _, FactReading::Conflicting) => LogicalReading::Unknown,
        (
            Reported(InsertionState::Acknowledged),
            Reported(TaggedEndState::Observed),
            Reported(LogicalSettlementState::Settled),
        ) => LogicalReading::Settled,
        (Reported(InsertionState::Acknowledged), Reported(TaggedEndState::Absent), debt)
            if debt != Reported(LogicalSettlementState::Settled) =>
        {
            LogicalReading::Owed
        }
        (insertion, _, Reported(LogicalSettlementState::Owed))
            if insertion != Reported(InsertionState::NotInserted) =>
        {
            LogicalReading::Owed
        }
        (Reported(InsertionState::NotInserted), tagged, debt)
            if tagged != Reported(TaggedEndState::Observed)
                && debt != Reported(LogicalSettlementState::Owed) =>
        {
            LogicalReading::NotInserted
        }
        _ => LogicalReading::Unknown,
    };
    SettlementReading {
        insertion,
        tagged_end,
        logical_settlement,
        physical_custody,
        logical,
    }
}
