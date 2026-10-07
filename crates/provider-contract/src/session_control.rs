//! `oulipoly.session_control/v2`: one shared root control vocabulary.
//!
//! Records a requester and the existing root owner exchange about the root
//! control face: descriptive discovery ([`RootEntry`]), current-state
//! inspection ([`ControlState`]), control requests and the claims answering
//! them, and settlement observations about logical work. The same claims
//! serve session control and infrastructure control; there is no second
//! meaning of hold, acknowledgment or idempotency elsewhere. Peers select the
//! version by advertisement ([`select`]), not by provider describe or any CLI,
//! binary, package or source identity. Providers do not speak it: it is not a
//! provider/v1 subcommand or a second host/provider protocol.
//!
//! Every operation uses one claim ladder with distinct records for requester
//! intent ([`Request`]), transport receipt ([`Receipt`]), admission
//! ([`Admission`]), semantic transition acknowledgment ([`Acknowledgment`]) or
//! refusal ([`Refusal`]), and what is known of the request's outcome
//! ([`Outcome`]). A successor may report its own [`Fulfillment`] of inherited
//! admitted intent without asserting predecessor transition authority. An `unknown` outcome can later be refined for the same
//! immutable request; a definite outcome is final. An [`Observation`] reports
//! one settlement fact — insertion acknowledgment, tagged turn end, logical
//! settlement/debt or physical custody/waits — and is never a control.
//!
//! The operations are `input_hold`/`input_release` (admission of new input
//! only; running work may continue), `recover` (a successor owner attaches the
//! same surviving incarnation; never a new incarnation), `cancel` and `close`.
//! Each keeps its own transition; none is drain or execution pause.
//!
//! Every value here is a claim. Validation checks shape, cross-field meaning
//! and claim order ([`RequestTrace`]); it does not establish that a producer
//! told the truth, that a requester was authorized, that a control was
//! enforced or durable, or that any actor is in custody. Root, owner,
//! generation, incarnation, requester and logical ids are opaque host values
//! compared only for equality: the SDK is not their authority and keeps no
//! registry. A [`Lineage`] is the caller's statement of which authorities are
//! warranted reporters for a root; the SDK applies it and does not derive it.
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

pub const PROTOCOL: &str = "oulipoly.session_control/v2";
pub const SCHEMA_JSON: &str = include_str!("../contract/extensions/session-control/v2.schema.json");
/// Session-control versions this SDK release defines. v1 (hold-only) was
/// never adopted and is not retained.
pub const SUPPORTED_VERSIONS: &[u32] = &[2];
/// Upper bound of one serialized record line, checked before parsing.
pub const MAX_RECORD_BYTES: usize = 32_768;
/// Upper bound of one serialized peer advertisement.
pub const MAX_ADVERTISEMENT_BYTES: usize = 16_384;
/// Distinct receipts one [`RequestTrace`] retains for duplicate detection.
pub const MAX_TRACE_RECEIPTS: usize = 8;
/// Distinct unknown reports retained; one additional slot is reserved for
/// definite knowledge, which cannot be blocked by unknown history.
pub const MAX_TRACE_OUTCOMES: usize = 8;
/// Pending intents one [`ControlState`] lists.
pub const MAX_PENDING: usize = 8;
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

impl Authority {
    /// Another owner generation of the same root and the same incarnation:
    /// the only form of authority that may answer a `recover`.
    pub fn succeeds_within_incarnation(&self, addressed: &Authority) -> bool {
        self.root == addressed.root
            && self.incarnation == addressed.incarnation
            && (self.owner != addressed.owner || self.generation != addressed.generation)
    }
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

/// The immutable identity of one control intent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestRef {
    pub request_key: String,
    pub requester: String,
    pub addressed: Authority,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    /// Hold admission of new input at the scope. Running work may continue.
    InputHold,
    /// Clear an input hold through the same root authority.
    InputRelease,
    /// Attach a successor owner to the same surviving root incarnation.
    Recover,
    /// Cancel the scope's work.
    Cancel,
    /// Refuse new input, then end the scope once admitted work has ended.
    Close,
}

/// The state domain an operation transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Domain {
    Input,
    Lifecycle,
    Attachment,
}

impl Operation {
    pub fn domain(self) -> Domain {
        match self {
            Self::InputHold | Self::InputRelease => Domain::Input,
            Self::Cancel | Self::Close => Domain::Lifecycle,
            Self::Recover => Domain::Attachment,
        }
    }

    /// The state an acknowledgment of this operation moves to.
    pub fn target(self) -> State {
        match self {
            Self::InputHold => State::InputHeld,
            Self::InputRelease => State::InputOpen,
            Self::Recover => State::Attached,
            Self::Cancel => State::Cancelling,
            Self::Close => State::Closing,
        }
    }
}

/// Transition states. `input_held` is not paused execution; `closing` and
/// `cancelling` are requested transitions, not ended work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    InputHeld,
    InputOpen,
    Open,
    Closing,
    Cancelling,
    Attached,
    Unattached,
    Unknown,
}

impl State {
    /// The domain of a known state; `None` for `unknown`, which belongs to
    /// every domain.
    pub fn domain(self) -> Option<Domain> {
        match self {
            Self::InputHeld | Self::InputOpen => Some(Domain::Input),
            Self::Open | Self::Closing | Self::Cancelling => Some(Domain::Lifecycle),
            Self::Attached | Self::Unattached => Some(Domain::Attachment),
            Self::Unknown => None,
        }
    }

    fn in_domain(self, domain: Domain) -> bool {
        self.domain().is_none_or(|own| own == domain)
    }

    /// Lifecycle progression: open, then closing, then cancelling (cancel
    /// outranks close). Nothing returns to an earlier lifecycle state.
    fn lifecycle_rank(self) -> Option<u8> {
        match self {
            Self::Open => Some(0),
            Self::Closing => Some(1),
            Self::Cancelling => Some(2),
            _ => None,
        }
    }
}

/// A descriptive view a peer can produce or read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Report {
    /// [`RootEntry`] records.
    Discovery,
    /// [`ControlState`] records.
    Inspection,
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

impl Request {
    /// The immutable identity later knowledge refers to.
    pub fn reference(&self) -> RequestRef {
        RequestRef {
            request_key: self.request_key.clone(),
            requester: self.requester.clone(),
            addressed: self.addressed.clone(),
        }
    }
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

/// The answering authority admitted the request for its transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Admission {
    pub protocol: String,
    pub request_key: String,
    pub requester: String,
    pub addressed: Authority,
    pub operation: Operation,
    pub responder: Authority,
    pub observed_at_unix_ms: u64,
}

/// The operation's state moved from `from` to `to` at the responder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Acknowledgment {
    pub protocol: String,
    pub request_key: String,
    pub requester: String,
    pub addressed: Authority,
    pub responder: Authority,
    pub operation: Operation,
    pub from: State,
    pub to: State,
    pub observed_at_unix_ms: u64,
}

/// A successor reports its own present fulfillment of an inherited admitted
/// intent. This is not a predecessor acknowledgment or a new request. Runner
/// establishes inheritance, positive successor attribution and fencing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fulfillment {
    pub protocol: String,
    pub request_key: String,
    pub requester: String,
    pub addressed: Authority,
    pub reporter: Authority,
    pub operation: Operation,
    pub from: State,
    pub to: State,
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
    OwnerLive,
    KeyConflict,
    UnsupportedOperation,
    NotPermitted,
    UnknownScope,
    AlreadyTerminal,
    RootAbsent,
    TransitionFailed,
}

impl RefusalReason {
    pub fn stage(self) -> RefusalStage {
        match self {
            Self::AlreadyTerminal | Self::RootAbsent | Self::TransitionFailed => {
                RefusalStage::Transition
            }
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
    pub operation: Operation,
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
    Fulfilled,
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

/// What a reporter knows about one request's own transition.
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
    /// The authority reporting this knowledge, when it is one: the addressed
    /// authority or another authority of the same root. Reporting knowledge
    /// confers no authority over the request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reporter: Option<Authority>,
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

/// Discovery: a descriptive address of one existing root for `requester`.
/// `describer` is an opaque locator, never an authority. An entry is not
/// admission, scheduling, capacity or ownership, and its `authority` is the
/// last known one, not proof that it is current.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootEntry {
    pub protocol: String,
    pub describer: String,
    pub requester: String,
    pub authority: Authority,
    pub observed_at_unix_ms: u64,
}

/// One current state as the reporter knows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Current {
    pub state: State,
    /// The request that established this state, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<RequestRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingStatus {
    Received,
    Admitted,
}

/// A control intent the reporter holds without an acknowledgment or refusal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Pending {
    pub request: RequestRef,
    pub operation: Operation,
    pub status: PendingStatus,
}

/// Inspection: the reporting authority's current knowledge of a scope. A
/// report, not an effect or acknowledgment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlState {
    pub protocol: String,
    pub reporter: Authority,
    pub scope: ControlScope,
    pub input: Current,
    pub lifecycle: Current,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending: Vec<Pending>,
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
    Fulfillment(Fulfillment),
    Refusal(Refusal),
    Outcome(Outcome),
    Observation(Observation),
    RootEntry(RootEntry),
    ControlState(ControlState),
}

/// One peer's `oulipoly.session_control/v2` advertisement entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Offer {
    pub operations: Vec<Operation>,
    pub reports: Vec<Report>,
    pub facts: Vec<FactType>,
}

/// What two peers can both use.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Selected {
    pub protocol: String,
    pub operations: Vec<Operation>,
    pub reports: Vec<Report>,
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

/// Whether `responder` may answer `operation` addressed to `addressed`:
/// the addressed authority itself, except `recover`, which only a successor
/// owner of the same root incarnation answers.
fn may_answer(operation: Operation, addressed: &Authority, responder: &Authority) -> bool {
    if operation == Operation::Recover {
        responder.succeeds_within_incarnation(addressed)
    } else {
        responder == addressed
    }
}

/// One transition meaning across original ACKs and successor fulfillment.
fn check_transition(
    operation: Operation,
    from: State,
    to: State,
) -> Result<(), ControlUnavailable> {
    if to != operation.target() || !from.in_domain(operation.domain()) {
        return Err(invalid_record("transition outside its operation or domain"));
    }
    if matches!((from.lifecycle_rank(), to.lifecycle_rank()), (Some(prior), Some(next)) if next < prior)
    {
        return Err(invalid_record(
            "lifecycle transition regresses cancel precedence",
        ));
    }
    Ok(())
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
                if !may_answer(
                    admission.operation,
                    &admission.addressed,
                    &admission.responder,
                ) {
                    return Err(invalid_record(
                        "admission responder may not answer this operation",
                    ));
                }
            }
            Record::Acknowledgment(ack) => {
                if !may_answer(ack.operation, &ack.addressed, &ack.responder) {
                    return Err(invalid_record(
                        "acknowledgment responder may not answer this operation",
                    ));
                }
                check_transition(ack.operation, ack.from, ack.to)?;
            }
            Record::Fulfillment(fulfilled) => {
                if fulfilled.operation == Operation::Recover
                    || !fulfilled
                        .reporter
                        .succeeds_within_incarnation(&fulfilled.addressed)
                {
                    return Err(invalid_record(
                        "fulfillment needs a distinct same-incarnation successor",
                    ));
                }
                check_transition(fulfilled.operation, fulfilled.from, fulfilled.to)?;
            }
            Record::Refusal(refusal) => check_refusal(refusal)?,
            Record::Outcome(outcome) => {
                if let Some(reporter) = &outcome.reporter {
                    if reporter.root != outcome.addressed.root {
                        return Err(invalid_record("outcome reporter is another root"));
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
            Record::ControlState(state) => {
                if state.reporter.root != state.scope.root {
                    return Err(invalid_record("control state reporter is another root"));
                }
                if !state.input.state.in_domain(Domain::Input)
                    || !state.lifecycle.state.in_domain(Domain::Lifecycle)
                {
                    return Err(invalid_record("control state outside its domain"));
                }
                let refs = [&state.input.since, &state.lifecycle.since];
                if refs
                    .into_iter()
                    .flatten()
                    .chain(state.pending.iter().map(|pending| &pending.request))
                    .any(|request| request.addressed.root != state.scope.root)
                {
                    return Err(invalid_record("control state names another root's request"));
                }
                for (index, pending) in state.pending.iter().enumerate() {
                    if state.pending[..index]
                        .iter()
                        .any(|other| other.request == pending.request)
                    {
                        return Err(invalid_record("pending intent listed twice"));
                    }
                }
            }
            Record::Receipt(_) | Record::RootEntry(_) => {}
        }
        Ok(())
    }

    /// `(request_key, requester, addressed)` of a control record; `None` for
    /// observations, root entries and control states.
    pub fn correlation(&self) -> Option<(&str, &str, &Authority)> {
        let (key, requester, addressed) = match self {
            Record::Request(r) => (&r.request_key, &r.requester, &r.addressed),
            Record::Receipt(r) => (&r.request_key, &r.requester, &r.addressed),
            Record::Admission(r) => (&r.request_key, &r.requester, &r.addressed),
            Record::Acknowledgment(r) => (&r.request_key, &r.requester, &r.addressed),
            Record::Fulfillment(r) => (&r.request_key, &r.requester, &r.addressed),
            Record::Refusal(r) => (&r.request_key, &r.requester, &r.addressed),
            Record::Outcome(r) => (&r.request_key, &r.requester, &r.addressed),
            Record::Observation(_) | Record::RootEntry(_) | Record::ControlState(_) => return None,
        };
        Some((key, requester, addressed))
    }
}

fn check_refusal(refusal: &Refusal) -> Result<(), ControlUnavailable> {
    if refusal.stage != refusal.reason.stage() {
        return Err(invalid_record("refusal reason belongs to another stage"));
    }
    let recover = refusal.operation == Operation::Recover;
    match refusal.reason {
        RefusalReason::StaleAuthority if recover => {
            return Err(invalid_record("recover addresses a predecessor by design"));
        }
        RefusalReason::OwnerLive | RefusalReason::RootAbsent if !recover => {
            return Err(invalid_record("refusal reason applies only to recover"));
        }
        _ => {}
    }
    let Some(responder) = &refusal.responder else {
        return Ok(());
    };
    let ok = match refusal.reason {
        // Another authority of the same root says the addressed one is stale.
        RefusalReason::StaleAuthority => {
            responder != &refusal.addressed && responder.root == refusal.addressed.root
        }
        // The addressed owner still holds the root; the would-be successor
        // of the same root refuses.
        RefusalReason::OwnerLive => {
            responder != &refusal.addressed && responder.root == refusal.addressed.root
        }
        _ => may_answer(refusal.operation, &refusal.addressed, responder),
    };
    if ok {
        Ok(())
    } else {
        Err(invalid_record(
            "refusal responder may not answer this operation",
        ))
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

fn agree_record(
    record: Record,
    selected: &Selected,
    included: bool,
    what: &str,
) -> Result<(), ControlUnavailable> {
    Record::decode(&serde_json::to_value(record).expect("serializes"))?;
    check_selected(selected)?;
    if !included {
        return Err(ControlUnavailable::new(
            UnavailableReason::NoCommonCapability,
            format!("{what} was not selected"),
        ));
    }
    Ok(())
}

impl Observation {
    /// The observation is admissible and its fact type was selected.
    pub fn agree(&self, selected: &Selected) -> Result<(), ControlUnavailable> {
        let included = selected.facts.contains(&self.fact.fact_type());
        agree_record(
            Record::Observation(self.clone()),
            selected,
            included,
            "fact type",
        )
    }
}

impl RootEntry {
    /// The entry is admissible and discovery was selected.
    pub fn agree(&self, selected: &Selected) -> Result<(), ControlUnavailable> {
        let included = selected.reports.contains(&Report::Discovery);
        agree_record(
            Record::RootEntry(self.clone()),
            selected,
            included,
            "discovery",
        )
    }
}

impl ControlState {
    /// The report is admissible and inspection was selected.
    pub fn agree(&self, selected: &Selected) -> Result<(), ControlUnavailable> {
        let included = selected.reports.contains(&Report::Inspection);
        agree_record(
            Record::ControlState(self.clone()),
            selected,
            included,
            "inspection",
        )
    }
}

fn check_selected(selected: &Selected) -> Result<(), ControlUnavailable> {
    if selected.protocol != PROTOCOL {
        return Err(violation("selected protocol is not supported"));
    }
    validate(
        "Offer",
        &serde_json::json!({
            "operations": selected.operations,
            "reports": selected.reports,
            "facts": selected.facts,
        }),
        UnavailableReason::ProtocolViolation,
    )
}

/// A peer's advertisement carrying only this offer.
pub fn advertisement(offer: &Offer) -> Value {
    serde_json::json!({ PROTOCOL: offer })
}

fn common<T: Copy + Ord>(local: &[T], peer: &[T]) -> Vec<T> {
    let mut both: Vec<T> = local
        .iter()
        .copied()
        .filter(|item| peer.contains(item))
        .collect();
    both.sort_unstable();
    both
}

/// Selects what `local` and a peer's advertisement can both use. Unknown,
/// older or newer entries are ignored; the v2 entry must be strict. Absent or
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
    // Both offers pair input_hold with input_release, so the intersection
    // never selects a hold without its release.
    let operations = common(&local.operations, &peer.operations);
    let reports = common(&local.reports, &peer.reports);
    let facts = common(&local.facts, &peer.facts);
    if operations.is_empty() && reports.is_empty() {
        return Err(ControlUnavailable::new(
            UnavailableReason::NoCommonCapability,
            "no common operation or report",
        ));
    }
    Ok(Selected {
        protocol: PROTOCOL.to_owned(),
        operations,
        reports,
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
        from: State,
        to: State,
    },
    Fulfilled {
        from: State,
        to: State,
    },
    Refused {
        stage: RefusalStage,
        reason: RefusalReason,
    },
    /// What is now known of the outcome. `refines` is true when it follows an
    /// earlier `unknown` for the same request.
    Concluded {
        result: OutcomeResult,
        refines: bool,
    },
    /// An identical redelivery of a record already accepted. No new meaning.
    Duplicate,
}

/// Requester-side check of the claims answering one request: correlation,
/// claim order, refinement and non-contradiction. It is caller-owned
/// per-request state, not a registry or executor, and does not establish that
/// any claim is true.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestTrace {
    request: Request,
    receipts: Vec<Receipt>,
    admission: Option<Admission>,
    acknowledgment: Option<Acknowledgment>,
    fulfillment: Option<Fulfillment>,
    refusal: Option<Refusal>,
    outcomes: Vec<Outcome>,
}

impl RequestTrace {
    pub fn new(request: Request) -> Result<Self, ControlUnavailable> {
        request.admit()?;
        Ok(Self {
            request,
            receipts: Vec::new(),
            admission: None,
            acknowledgment: None,
            fulfillment: None,
            refusal: None,
            outcomes: Vec::new(),
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
    pub fn fulfillment(&self) -> Option<&Fulfillment> {
        self.fulfillment.as_ref()
    }
    pub fn refusal(&self) -> Option<&Refusal> {
        self.refusal.as_ref()
    }
    /// The latest outcome knowledge.
    pub fn outcome(&self) -> Option<&Outcome> {
        self.outcomes.last()
    }
    /// Accepted unknown reports (at most 8), then at most one definite report.
    pub fn outcomes(&self) -> &[Outcome] {
        &self.outcomes
    }

    fn concluded(&self) -> bool {
        self.outcome()
            .is_some_and(|outcome| outcome.result != OutcomeResult::Unknown)
    }

    pub fn accept(&mut self, record: &Record) -> Result<Step, ControlUnavailable> {
        // Public structs/raw Serde may bypass decode. Admit before mutation.
        Record::decode_line(&record.encode_line())?;
        let Some((key, requester, addressed)) = record.correlation() else {
            return Err(violation("not a control response"));
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
        if self.concluded() {
            return Err(violation("a definite outcome is final"));
        }
        let operation = self.request.operation;
        match record {
            Record::Request(_) => Err(violation("same key with a different request")),
            Record::Observation(_) | Record::RootEntry(_) | Record::ControlState(_) => {
                unreachable!("reports have no correlation")
            }
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
                if admission.operation != operation {
                    return Err(violation("admission of another operation"));
                }
                if self.admission.is_some() || self.refusal.is_some() {
                    return Err(violation("admission contradicts an earlier claim"));
                }
                self.admission = Some(admission.clone());
                Ok(Step::Admitted)
            }
            Record::Acknowledgment(ack) => {
                if ack.operation != operation {
                    return Err(violation("acknowledgment of another operation"));
                }
                let Some(admission) = &self.admission else {
                    return Err(violation("acknowledgment before admission"));
                };
                if admission.responder != ack.responder {
                    return Err(violation("acknowledgment from another responder"));
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
            Record::Fulfillment(fulfilled) => {
                if fulfilled.operation != operation {
                    return Err(violation("fulfillment of another operation"));
                }
                if self.admission.is_none() {
                    return Err(violation("fulfillment before inherited admission"));
                }
                if self.fulfillment.is_some() || self.refusal.is_some() {
                    return Err(violation("fulfillment contradicts an earlier claim"));
                }
                self.fulfillment = Some(fulfilled.clone());
                Ok(Step::Fulfilled {
                    from: fulfilled.from,
                    to: fulfilled.to,
                })
            }
            Record::Refusal(refusal) => {
                if refusal.operation != operation {
                    return Err(violation("refusal of another operation"));
                }
                if self.refusal.is_some()
                    || self.acknowledgment.is_some()
                    || self.fulfillment.is_some()
                {
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
                    OutcomeResult::Fulfilled => self.fulfillment.is_some(),
                    // Uncertainty is retained alongside, never instead of,
                    // an earlier acknowledgment or refusal.
                    OutcomeResult::Unknown => true,
                };
                if !consistent {
                    return Err(violation("outcome contradicts the recorded claims"));
                }
                if outcome.result == OutcomeResult::Unknown
                    && self.outcomes.len() >= MAX_TRACE_OUTCOMES
                {
                    return Err(violation("outcome bound exceeded"));
                }
                let refines = !self.outcomes.is_empty();
                self.outcomes.push(outcome.clone());
                Ok(Step::Concluded {
                    result: outcome.result,
                    refines,
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
            Record::Fulfillment(fulfilled) => self.fulfillment.as_ref() == Some(fulfilled),
            Record::Refusal(refusal) => self.refusal.as_ref() == Some(refusal),
            Record::Outcome(outcome) => self.outcomes.contains(outcome),
            Record::Observation(_) | Record::RootEntry(_) | Record::ControlState(_) => false,
        }
    }

    /// How a current-state report relates to this request's own claims.
    /// Current knowledge never erases an earlier acknowledgment: a report
    /// that has lost the state retains it, and only a report naming another
    /// request as the basis of a different state supersedes it.
    pub fn relate(&self, state: &ControlState) -> Relation {
        if state.scope != self.request.scope {
            return Relation::Unrelated;
        }
        let reference = self.request.reference();
        let current = match self.request.operation.domain() {
            Domain::Input => &state.input,
            Domain::Lifecycle => &state.lifecycle,
            Domain::Attachment => return Relation::Unrelated,
        };
        let target = self
            .acknowledgment
            .as_ref()
            .map(|ack| ack.to)
            .or_else(|| self.fulfillment.as_ref().map(|fulfilled| fulfilled.to));
        let pending = state
            .pending
            .iter()
            .find(|pending| pending.request == reference);
        if pending.is_some_and(|pending| pending.operation != self.request.operation)
            || (pending.is_some() && (target.is_some() || self.refusal.is_some()))
        {
            return Relation::Contradicts;
        }
        let Some(target) = target else {
            return if pending.is_some() {
                Relation::Pending
            } else {
                Relation::NoAcknowledgment
            };
        };
        if current.state == State::Unknown {
            return Relation::PriorRetained;
        }
        if current.state == target {
            return Relation::Current;
        }
        let explained = current
            .since
            .as_ref()
            .is_some_and(|since| since != &reference);
        let regresses = match (current.state.lifecycle_rank(), target.lifecycle_rank()) {
            (Some(now), Some(acked)) => now < acked,
            _ => false,
        };
        if explained && !regresses {
            Relation::Superseded
        } else {
            Relation::Contradicts
        }
    }
}

/// How a [`ControlState`] relates to one request's claims.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Relation {
    /// Another scope, or a domain the report does not describe.
    Unrelated,
    /// No ACK or fulfillment; the report does not list the intent as pending.
    NoAcknowledgment,
    /// No ACK or fulfillment yet; the report holds the intent as pending.
    Pending,
    /// The report shows the acknowledged or fulfilled state.
    Current,
    /// The report no longer knows the state; the ACK or fulfillment stands.
    PriorRetained,
    /// The reporter cites another request as superseding. The producer owes
    /// a real newer admitted intent in the same domain; this reader does not
    /// look up that intent or establish its temporal authority.
    Superseded,
    /// A different state without another request as its basis, or a
    /// lifecycle regression: the acknowledged transition silently vanished.
    Contradicts,
}

/// What the observations say about one settlement fact of one subject.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reading", content = "state", rename_all = "snake_case")]
pub enum FactReading<T> {
    /// No observation of this fact for the subject. Not a negative.
    NotObserved,
    /// The most refined state the observations support.
    Reported(T),
    /// Observations contradict each other. Retained, not resolved.
    Conflicting,
}

/// Knowledge order of one fact's states. A later report may refine an
/// earlier one; two states neither of which refines the other contradict.
trait Knowledge: Copy + PartialEq {
    /// `missing` or `redacted`: neither refines nor contradicts anything.
    fn withheld(self) -> bool;
    fn redacted(self) -> bool;
    /// Whether `self` may be refined to `later`.
    fn refines_to(self, later: Self) -> bool;
}

impl Knowledge for InsertionState {
    fn withheld(self) -> bool {
        matches!(self, Self::Missing | Self::Redacted)
    }
    fn redacted(self) -> bool {
        self == Self::Redacted
    }
    fn refines_to(self, later: Self) -> bool {
        self == later || (self == Self::Uncertain && later != Self::Uncertain)
    }
}

impl Knowledge for TaggedEndState {
    fn withheld(self) -> bool {
        matches!(self, Self::Missing | Self::Redacted)
    }
    fn redacted(self) -> bool {
        self == Self::Redacted
    }
    fn refines_to(self, later: Self) -> bool {
        self == later || (self == Self::Absent && later == Self::Observed)
    }
}

impl Knowledge for LogicalSettlementState {
    fn withheld(self) -> bool {
        matches!(self, Self::Missing | Self::Redacted)
    }
    fn redacted(self) -> bool {
        self == Self::Redacted
    }
    fn refines_to(self, later: Self) -> bool {
        self == later || (self == Self::Owed && later == Self::Settled)
    }
}

impl Knowledge for PhysicalCustodyState {
    fn withheld(self) -> bool {
        matches!(self, Self::Missing | Self::Redacted)
    }
    fn redacted(self) -> bool {
        self == Self::Redacted
    }
    fn refines_to(self, later: Self) -> bool {
        let rank = |state| match state {
            Self::Unsettled => 0,
            Self::Live => 1,
            Self::ExitedWaitPending => 2,
            _ => 3,
        };
        rank(self) <= rank(later)
    }
}

/// Adds one report. Order-independent: the result does not depend on report
/// order.
fn add<T: Knowledge>(reading: &mut FactReading<T>, state: T) {
    *reading = match *reading {
        FactReading::NotObserved => FactReading::Reported(state),
        FactReading::Conflicting => FactReading::Conflicting,
        FactReading::Reported(seen) => match (seen.withheld(), state.withheld()) {
            // Withheld states never conflict; redaction outranks missing
            // so that the result does not depend on order.
            (true, true) if seen != state => {
                FactReading::Reported(if state.redacted() { state } else { seen })
            }
            (true, false) => FactReading::Reported(state),
            (_, true) => FactReading::Reported(seen),
            _ if seen.refines_to(state) => FactReading::Reported(state),
            _ if state.refines_to(seen) => FactReading::Reported(seen),
            _ => FactReading::Conflicting,
        },
    };
}

/// Logical settlement as read from insertion, tagged end and debt only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogicalReading {
    /// Insertion acknowledged, tagged end observed and logical debt settled.
    Settled,
    /// Debt is reported owed, or insertion was acknowledged with a known
    /// absent tagged end and no contrary settled-debt claim.
    Owed,
    /// Insertion positively reported absent and no tagged end observed.
    NotInserted,
    /// Anything else, including missing, redacted, uncertain or conflicting.
    Unknown,
}

/// Whose reports a settlement reading composes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Basis {
    /// Only reports from the supplied root lineage. A consumer can interpret
    /// this reading with that lineage's warrant.
    Warranted,
    /// Reports from any authority of the subject's root. A description of
    /// claims, never retirement evidence.
    Unwarranted,
}

/// The authorities a consumer states are warranted reporters for one root:
/// its owner/generation/incarnation lineage. Supplied by the root owner's
/// realization; the SDK applies it and neither derives nor attests it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lineage {
    pub root: String,
    pub authorities: Vec<Authority>,
}

/// The four settlement facts of one logical subject, kept apart, plus the
/// logical reading derived from the logical facts alone and the basis of the
/// composition. Physical custody never enters the logical reading.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementReading {
    pub insertion: FactReading<InsertionState>,
    pub tagged_end: FactReading<TaggedEndState>,
    pub logical_settlement: FactReading<LogicalSettlementState>,
    /// Single exact actor-reference knowledge only, never subject discharge.
    /// Differing or ambiguous actor references also read `Conflicting`;
    /// retained raw observations carry the physical facts and references.
    pub physical_custody: FactReading<PhysicalCustodyState>,
    pub logical: LogicalReading,
    pub basis: Basis,
    /// Same-subject reports left out: another root's reporter, or outside the
    /// supplied lineage.
    pub excluded: usize,
}

/// Reads the observations whose subject equals `subject` as one evolving
/// account: a refinement (for example `uncertain` → `not_inserted`, `owed` →
/// `settled`) is not a conflict, and incompatible states are. A reporter of
/// another root is never composed. With a `lineage` for the subject's root,
/// only its authorities are composed and the basis is `warranted`; an earlier
/// generation in that lineage is not erased by its successor. Without one the
/// basis is `unwarranted`. This reads claims; it does not establish them.
pub fn read_settlement(
    subject: &LogicalRef,
    lineage: Option<&Lineage>,
    observations: &[Observation],
) -> SettlementReading {
    let lineage = lineage.filter(|lineage| lineage.root == subject.root);
    let mut insertion = FactReading::NotObserved;
    let mut tagged_end = FactReading::NotObserved;
    let mut logical_settlement = FactReading::NotObserved;
    let mut physical_custody = FactReading::NotObserved;
    let mut physical_actor: Option<&ActorEvidence> = None;
    let mut excluded = 0;
    for observation in observations.iter().filter(|o| &o.subject == subject) {
        let warranted = observation.reporter.root == subject.root
            && lineage.is_none_or(|lineage| lineage.authorities.contains(&observation.reporter));
        if !warranted {
            excluded += 1;
            continue;
        }
        match observation.fact {
            Fact::Insertion { state, .. } => add(&mut insertion, state),
            Fact::TaggedEnd { state, .. } => add(&mut tagged_end, state),
            Fact::LogicalSettlement { state, .. } => add(&mut logical_settlement, state),
            Fact::PhysicalCustody { state, .. } => {
                // Refinement is meaningful only within one exact actor reference.
                // Withheld evidence is not a negative physical state.
                if !state.withheld() {
                    match observation.evidence.as_slice() {
                        [actor]
                            if actor.exactness == Exactness::Exact
                                && matches!(actor.reference, DisclosedRef::Present { .. }) =>
                        {
                            if physical_actor.is_some_and(|seen| seen != actor) {
                                physical_custody = FactReading::Conflicting;
                            }
                            physical_actor = Some(actor);
                        }
                        _ => physical_custody = FactReading::Conflicting,
                    }
                }
                add(&mut physical_custody, state);
            }
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
        basis: if lineage.is_some() {
            Basis::Warranted
        } else {
            Basis::Unwarranted
        },
        excluded,
    }
}
