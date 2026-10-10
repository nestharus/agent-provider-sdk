//! `oulipoly.incident/v1`: bounded infrastructure incident claims.
//!
//! A companion of [`session_control`](crate::session_control) v3, not a
//! successor or a second control vocabulary. It adds what session control
//! does not define: an incident [`Report`] with typed process/provider
//! resource [`Evidence`] and claimed [`Severity`], [`Scope`] and [`Cause`];
//! collector [`ReportReceipt`] and [`ReportConflict`] answers; recovery
//! [`Verification`] checks; and the [`RecoveryAuthorization`] record. Root
//! authority, logical links, actor evidence, disclosure markers and
//! repetition meaning are session control's own types. Pause and resume are
//! session control `input_hold`/`input_release`: nothing here holds input,
//! acknowledges a transition or suspends native execution.
//!
//! Every value here is a claim. Validation checks shape and cross-field
//! meaning, including the minimum evidence a claimed cause needs and the
//! complete passing proof an authorization record must carry. It does not
//! establish that a reporter told the truth, that a cause is real, that an
//! issuer holds a current fence, or that any action may proceed. Reporter,
//! collector, verifier, coordinator, fence, incident and grouping references
//! are opaque host values compared only for equality. Deduplication,
//! classification, severity/scope assignment, escalation, target expansion,
//! broadcast, fencing and every recovery effect stay with Agent Runner; the
//! SDK executes nothing and stores nothing. Refusals of the contract itself
//! are [`IncidentUnavailable`] diagnostics with no conversion to control,
//! provider errors, launch events, launch unavailability or completion
//! outcomes.
//!
//! The structural schema plus the normative semantic rules in the adjacent
//! contract README define language-independent conformance. Raw Serde
//! supplies representation only.

use crate::session_control::{
    self, Actor, ActorEvidence, DisclosedRef, DisclosedText, Exactness, LogicalRef, MissingReason,
    Operation, Repetition, Request, UnavailableReason,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::OnceLock;

pub const PROTOCOL: &str = "oulipoly.incident/v1";
pub const SCHEMA_JSON: &str = include_str!("../contract/extensions/incident/v1.schema.json");
pub const SUPPORTED_VERSIONS: &[u32] = &[1];
/// Upper bound of one serialized record line, checked before parsing.
pub const MAX_RECORD_BYTES: usize = 65_536;
/// Upper bound of one serialized peer advertisement.
pub const MAX_ADVERTISEMENT_BYTES: usize = 16_384;
/// Upper bound of a `*_pressure_some_avg10` sample, in hundredths of a percent.
pub const MAX_PRESSURE: u64 = 10_000;
const MAX_DETAIL_CHARS: usize = 512;

/// Session-control definitions are referenced by this relative path in the
/// incident schema and imported under this prefix, so they keep their one
/// meaning without a copy.
const SESSION_CONTROL_REF: &str = "../session-control/v3.schema.json#/$defs/";
const IMPORTED: &str = "session_control.";

const DEFINITIONS: &[&str] = &[
    "Record",
    "Selector",
    "Offer",
    "Advertisement",
    "IncidentUnavailable",
];

/// Ordered severity label of a claim. A label, not a classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Notice,
    Degraded,
    Critical,
}

/// What a claim is about. `logical` is a session-control logical link; the
/// other named levels are opaque host groupings; `fleet` is everything.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "level", rename_all = "snake_case", deny_unknown_fields)]
pub enum Scope {
    Logical {
        subject: LogicalRef,
    },
    Account {
        #[serde(rename = "ref")]
        reference: String,
    },
    Provider {
        #[serde(rename = "ref")]
        reference: String,
    },
    Host {
        #[serde(rename = "ref")]
        reference: String,
    },
    Component {
        #[serde(rename = "ref")]
        reference: String,
    },
    Fleet,
}

/// How one scope relates to another, as far as the contract can know.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Containment {
    Contained,
    NotContained,
    /// Needs host topology the SDK does not have, such as which roots an
    /// account or host holds.
    Undetermined,
}

fn narrows(outer: &LogicalRef, inner: &LogicalRef) -> bool {
    outer.root == inner.root
        && [
            (&outer.child, &inner.child),
            (&outer.work, &inner.work),
            (&outer.input, &inner.input),
        ]
        .iter()
        .all(|(outer, inner)| outer.is_none() || outer == inner)
}

/// Whether `inner` lies within `outer`. Known only along the logical chain
/// (root, then each named child/work/input), for `fleet`, and by equality
/// within one opaque level; any other cross-level relation is undetermined.
pub fn contains(outer: &Scope, inner: &Scope) -> Containment {
    use Scope::*;
    match (outer, inner) {
        (Fleet, _) => Containment::Contained,
        (_, Fleet) => Containment::NotContained,
        (Logical { subject: outer }, Logical { subject: inner }) => {
            if narrows(outer, inner) {
                Containment::Contained
            } else {
                Containment::NotContained
            }
        }
        (Account { reference: a }, Account { reference: b })
        | (Provider { reference: a }, Provider { reference: b })
        | (Host { reference: a }, Host { reference: b })
        | (Component { reference: a }, Component { reference: b }) => {
            if a == b {
                Containment::Contained
            } else {
                Containment::NotContained
            }
        }
        _ => Containment::Undetermined,
    }
}

/// A reading filter over reports. Not a subscription, broadcast, target
/// expansion or authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Selector {
    pub at_least: Severity,
    pub scopes: Vec<Scope>,
}

impl Selector {
    pub fn admit(&self) -> Result<(), IncidentUnavailable> {
        validate(
            "Selector",
            &serde_json::to_value(self).expect("selector serializes"),
            UnavailableReason::InvalidRecord,
        )
    }

    /// Whether the report's claimed severity is at least `at_least` and its
    /// claimed scope lies within one of `scopes`. `undetermined` when no
    /// scope contains it and at least one relation needs host topology.
    pub fn matches(&self, report: &Report) -> Containment {
        if report.severity < self.at_least {
            return Containment::NotContained;
        }
        let mut result = Containment::NotContained;
        for scope in &self.scopes {
            match contains(scope, &report.scope) {
                Containment::Contained => return Containment::Contained,
                Containment::Undetermined => result = Containment::Undetermined,
                Containment::NotContained => {}
            }
        }
        result
    }
}

/// The reporter's claimed cause.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Cause {
    Unknown,
    Exit,
    Signal,
    OomKill,
    ProviderCondition,
    ComponentFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceType {
    ProcessExit,
    ProcessSignal,
    ProcessAbsent,
    CgroupMembership,
    MemoryEvent,
    ResourceSample,
    ProviderCondition,
    ComponentWitness,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryCounter {
    High,
    Max,
    Oom,
    OomKill,
}

/// `local` counts the cgroup itself; `hierarchical` includes descendants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Counting {
    Local,
    Hierarchical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Resource {
    RssBytes,
    MemoryPressureSomeAvg10,
    CpuPressureSomeAvg10,
    IoPressureSomeAvg10,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderCondition {
    Auth,
    Quota,
    RateLimited,
    ServiceUnavailable,
    InvalidInput,
    UnsupportedCapability,
    Timeout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComponentState {
    Healthy,
    Degraded,
    Failed,
}

/// A bounded host observation window, not an ordering clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Window {
    pub start_unix_ms: u64,
    pub end_unix_ms: u64,
}

/// Why an evidence item's content is absent. Neither is a negative.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum Withheld {
    Redacted,
    Missing { reason: MissingReason },
}

/// One typed process/provider resource observation. Each claims only its
/// own fact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Evidence {
    ProcessExit {
        actor: ActorEvidence,
        exit_code: i32,
    },
    /// The signal number only, not its sender or reason.
    ProcessSignal { actor: ActorEvidence, signal: u8 },
    /// No process found for the identity: not an exit or a cause.
    ProcessAbsent { actor: ActorEvidence },
    CgroupMembership {
        actor: ActorEvidence,
        cgroup: DisclosedRef,
    },
    MemoryEvent {
        cgroup: DisclosedRef,
        counter: MemoryCounter,
        counting: Counting,
        delta: u64,
        window: Window,
    },
    ResourceSample {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        actor: Option<ActorEvidence>,
        resource: Resource,
        value: u64,
    },
    ProviderCondition {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        actor: Option<ActorEvidence>,
        condition: ProviderCondition,
    },
    ComponentWitness {
        component: DisclosedRef,
        state: ComponentState,
    },
    /// An item of this type exists; its content is withheld.
    Withheld {
        evidence: EvidenceType,
        disclosure: Withheld,
    },
}

impl Evidence {
    /// The type this item reports or withholds.
    pub fn evidence_type(&self) -> EvidenceType {
        match self {
            Self::ProcessExit { .. } => EvidenceType::ProcessExit,
            Self::ProcessSignal { .. } => EvidenceType::ProcessSignal,
            Self::ProcessAbsent { .. } => EvidenceType::ProcessAbsent,
            Self::CgroupMembership { .. } => EvidenceType::CgroupMembership,
            Self::MemoryEvent { .. } => EvidenceType::MemoryEvent,
            Self::ResourceSample { .. } => EvidenceType::ResourceSample,
            Self::ProviderCondition { .. } => EvidenceType::ProviderCondition,
            Self::ComponentWitness { .. } => EvidenceType::ComponentWitness,
            Self::Withheld { evidence, .. } => *evidence,
        }
    }
}

/// A reporter's bounded typed incident observation. Not a classification,
/// hold, pause or request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Report {
    pub protocol: String,
    pub report_key: String,
    pub reporter: String,
    pub subject: LogicalRef,
    pub severity: Severity,
    pub scope: Scope,
    pub cause: Cause,
    pub evidence: Vec<Evidence>,
    /// Observer-only text; no rule reads it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<DisclosedText>,
    pub observed_at_unix_ms: u64,
}

impl Report {
    /// Admits a typed report through its wire form.
    pub fn admit(&self) -> Result<(), IncidentUnavailable> {
        Record::decode(&serde_json::to_value(Record::Report(self.clone())).expect("serializes"))
            .map(|_| ())
    }
}

/// A collector received the report. Retention claim only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportReceipt {
    pub protocol: String,
    pub report_key: String,
    pub reporter: String,
    pub collector: String,
    pub durable: bool,
    pub observed_at_unix_ms: u64,
}

impl ReportReceipt {
    /// Whether this receipt correlates with `report`'s key scope. It says
    /// nothing about the report's content, which a receipt does not carry.
    pub fn answers(&self, report: &Report) -> bool {
        self.reporter == report.reporter && self.report_key == report.report_key
    }
}

/// Answer to a changed submission under a used key; the original stands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportConflict {
    pub protocol: String,
    pub submitted: Box<Report>,
    pub original: Box<Report>,
    pub collector: String,
    pub observed_at_unix_ms: u64,
}

impl ReportConflict {
    /// Admits this conflict and checks it answers the caller's exact
    /// submission. It does not establish the collector's stored original.
    pub fn answer_to(&self, submitted: &Report) -> Result<(), IncidentUnavailable> {
        Record::decode_line(&Record::ReportConflict(self.clone()).encode_line())?;
        submitted.admit()?;
        if *self.submitted != *submitted {
            return Err(violation("conflict answers another submission"));
        }
        Ok(())
    }
}

/// One incident and epoch, as opaque coordinator values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IncidentRef {
    pub incident: String,
    pub epoch: u64,
}

impl IncidentRef {
    /// The same incident at a strictly greater epoch.
    pub fn supersedes(&self, other: &IncidentRef) -> bool {
        self.incident == other.incident && self.epoch > other.epoch
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckKind {
    Reproduction,
    Canary,
    ArtifactDigest,
    SchemaAgreement,
    Custody,
}

impl CheckKind {
    /// Every kind a complete proof satisfies.
    pub const REQUIRED: [CheckKind; 5] = [
        Self::Reproduction,
        Self::Canary,
        Self::ArtifactDigest,
        Self::SchemaAgreement,
        Self::Custody,
    ];
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckResult {
    Passed,
    Failed,
    NotRun,
    Unknown,
}

/// One recovery verification check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Check {
    pub check: CheckKind,
    pub subject: DisclosedRef,
    pub result: CheckResult,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
}

impl Check {
    /// Passed with a known subject. A redacted subject is known and
    /// withheld; a missing one is not known.
    fn satisfies(&self) -> bool {
        self.result == CheckResult::Passed && !matches!(self.subject, DisclosedRef::Missing { .. })
    }
}

/// A verifier's claimed recovery checks for one incident epoch and scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Verification {
    pub protocol: String,
    pub verifier: String,
    pub incident: IncidentRef,
    pub scope: Scope,
    pub checks: Vec<Check>,
    pub observed_at_unix_ms: u64,
}

/// What a verification's checks support.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reading", rename_all = "snake_case")]
pub enum ProofReading {
    /// Every required kind has a passed check with a known subject and no
    /// check failed.
    Complete,
    /// At least one check failed. A failure is not outweighed by a pass.
    Failed { failed: Vec<CheckKind> },
    /// No check failed, but these kinds have no satisfying check: absent,
    /// `not_run`, `unknown` or with a missing subject.
    Incomplete { unsatisfied: Vec<CheckKind> },
}

impl Verification {
    pub fn proof(&self) -> ProofReading {
        let mut failed: Vec<CheckKind> = self
            .checks
            .iter()
            .filter(|check| check.result == CheckResult::Failed)
            .map(|check| check.check)
            .collect();
        if !failed.is_empty() {
            failed.sort_unstable();
            failed.dedup();
            return ProofReading::Failed { failed };
        }
        let unsatisfied: Vec<CheckKind> = CheckKind::REQUIRED
            .into_iter()
            .filter(|kind| {
                !self
                    .checks
                    .iter()
                    .any(|check| check.check == *kind && check.satisfies())
            })
            .collect();
        if unsatisfied.is_empty() {
            ProofReading::Complete
        } else {
            ProofReading::Incomplete { unsatisfied }
        }
    }
}

/// The coordinator that claims to issue and the fence it claims to hold.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Issuer {
    pub coordinator: String,
    pub fence: String,
}

/// An issuer's claim that it authorized resuming input admission for a
/// scope, resting on a complete proof of the same incident epoch. It grants
/// nothing by existing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryAuthorization {
    pub protocol: String,
    pub authorization_key: String,
    pub issuer: Issuer,
    pub incident: IncidentRef,
    pub scope: Scope,
    pub proof: Verification,
    pub observed_at_unix_ms: u64,
}

/// How an authorization record relates to one session-control request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Coverage {
    /// An `input_release` within the authorization's scope. Fence, epoch
    /// currency and the release itself remain the host's.
    Covered,
    /// Any operation other than `input_release`, including `recover`.
    OperationNotCovered,
    OutsideScope,
    /// Scope relation needs host topology.
    Undetermined,
}

impl RecoveryAuthorization {
    pub fn admit(&self) -> Result<(), IncidentUnavailable> {
        Record::decode(
            &serde_json::to_value(Record::RecoveryAuthorization(self.clone())).expect("serializes"),
        )
        .map(|_| ())
    }

    /// Whether this record's claim covers `request`. Coverage is a reading
    /// of two claims, not permission: it does not check the issuer's fence
    /// or that the epoch is current.
    pub fn covers(&self, request: &Request) -> Result<Coverage, IncidentUnavailable> {
        self.admit()?;
        request.admit().map_err(|_| {
            IncidentUnavailable::new(UnavailableReason::InvalidRecord, "session control request")
        })?;
        if request.operation != Operation::InputRelease {
            return Ok(Coverage::OperationNotCovered);
        }
        let target = Scope::Logical {
            subject: LogicalRef {
                root: request.scope.root.clone(),
                child: request.scope.child.clone(),
                work: None,
                input: None,
            },
        };
        Ok(match contains(&self.scope, &target) {
            Containment::Contained => Coverage::Covered,
            Containment::NotContained => Coverage::OutsideScope,
            Containment::Undetermined => Coverage::Undetermined,
        })
    }

    /// Whether `current` is the same incident at a strictly greater epoch.
    pub fn superseded_by(&self, current: &IncidentRef) -> bool {
        current.supersedes(&self.incident)
    }
}

/// One incident record line.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Record {
    Report(Report),
    ReportReceipt(ReportReceipt),
    ReportConflict(ReportConflict),
    Verification(Verification),
    RecoveryAuthorization(RecoveryAuthorization),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordKind {
    Report,
    ReportReceipt,
    ReportConflict,
    Verification,
    RecoveryAuthorization,
}

/// One peer's `oulipoly.incident/v1` advertisement entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Offer {
    pub records: Vec<RecordKind>,
    pub evidence: Vec<EvidenceType>,
}

/// What two peers can both use.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Selected {
    pub protocol: String,
    pub records: Vec<RecordKind>,
    pub evidence: Vec<EvidenceType>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticKind {
    IncidentUnavailable,
}

/// The incident capability diagnostic. Observer-only: it disables incident
/// records and nothing else. SDK-generated detail never repeats submitted
/// values; caller-supplied detail is bounded, not sanitized.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(deny_unknown_fields)]
pub struct IncidentUnavailable {
    pub diagnostic: DiagnosticKind,
    pub reason: UnavailableReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl fmt::Display for IncidentUnavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reason = serde_json::to_value(self.reason).expect("reason serializes");
        write!(
            f,
            "incident unavailable: {}",
            reason.as_str().unwrap_or("?")
        )?;
        if let Some(detail) = &self.detail {
            write!(f, " ({detail})")?;
        }
        Ok(())
    }
}

impl IncidentUnavailable {
    /// Bounds caller-supplied detail; does not redact or sanitize it.
    pub fn new(reason: UnavailableReason, detail: impl Into<String>) -> Self {
        let mut detail: String = detail.into();
        if let Some((index, _)) = detail.char_indices().nth(MAX_DETAIL_CHARS) {
            detail.truncate(index);
        }
        Self {
            diagnostic: DiagnosticKind::IncidentUnavailable,
            reason,
            detail: (!detail.is_empty()).then_some(detail),
        }
    }
}

fn violation(detail: &str) -> IncidentUnavailable {
    IncidentUnavailable::new(UnavailableReason::ProtocolViolation, detail)
}

fn invalid_record(detail: &str) -> IncidentUnavailable {
    IncidentUnavailable::new(UnavailableReason::InvalidRecord, detail)
}

fn rewrite_refs(value: &mut Value, map: &dyn Fn(&str) -> Option<String>) {
    match value {
        Value::Object(object) => {
            if let Some(Value::String(reference)) = object.get_mut("$ref") {
                if let Some(rewritten) = map(reference) {
                    *reference = rewritten;
                }
            }
            for child in object.values_mut() {
                rewrite_refs(child, map);
            }
        }
        Value::Array(values) => {
            for child in values {
                rewrite_refs(child, map);
            }
        }
        _ => {}
    }
}

/// The incident `$defs` with the referenced session-control definitions
/// imported under their own prefix.
fn composed_defs() -> Value {
    let mut own: Value = serde_json::from_str(SCHEMA_JSON).expect("embedded schema JSON");
    rewrite_refs(&mut own, &|reference| {
        reference
            .strip_prefix(SESSION_CONTROL_REF)
            .map(|name| format!("#/$defs/{IMPORTED}{name}"))
    });
    let mut imported: Value =
        serde_json::from_str(session_control::SCHEMA_JSON).expect("embedded schema JSON");
    rewrite_refs(&mut imported, &|reference| {
        reference
            .strip_prefix("#/$defs/")
            .map(|name| format!("#/$defs/{IMPORTED}{name}"))
    });
    let mut defs = own["$defs"].take();
    let target = defs.as_object_mut().expect("incident $defs");
    for (name, definition) in imported["$defs"]
        .as_object()
        .expect("session-control $defs")
    {
        target.insert(format!("{IMPORTED}{name}"), definition.clone());
    }
    defs
}

fn validator(definition: &str) -> Option<&'static jsonschema::Validator> {
    static VALIDATORS: OnceLock<BTreeMap<&'static str, jsonschema::Validator>> = OnceLock::new();
    VALIDATORS
        .get_or_init(|| {
            let defs = composed_defs();
            DEFINITIONS
                .iter()
                .map(|name| {
                    let wrapper = serde_json::json!({
                        "$schema": "https://json-schema.org/draft/2020-12/schema",
                        "$defs": defs,
                        "$ref": format!("#/$defs/{name}"),
                    });
                    (
                        *name,
                        jsonschema::validator_for(&wrapper).expect("embedded incident schema"),
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
) -> Result<(), IncidentUnavailable> {
    let Some(validator) = validator(definition) else {
        return Err(IncidentUnavailable::new(reason, "unknown definition"));
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
    Err(IncidentUnavailable::new(
        reason,
        format!("{definition}: {}", keywords.join(", ")),
    ))
}

fn admit<T: serde::de::DeserializeOwned>(
    definition: &str,
    value: &Value,
    reason: UnavailableReason,
) -> Result<T, IncidentUnavailable> {
    validate(definition, value, reason)?;
    serde_json::from_value(value.clone())
        .map_err(|_| IncidentUnavailable::new(reason, format!("{definition}: representation")))
}

/// The reference of exact actor evidence with a present reference.
fn exact_ref(actor: &ActorEvidence) -> Option<&str> {
    match (&actor.exactness, &actor.reference) {
        (Exactness::Exact, DisclosedRef::Present { reference }) => Some(reference),
        _ => None,
    }
}

fn check_actor(actor: &ActorEvidence, expected: Actor) -> Result<(), IncidentUnavailable> {
    if actor.actor != expected {
        return Err(invalid_record("evidence names another actor kind"));
    }
    if actor.exactness == Exactness::Exact
        && !matches!(actor.reference, DisclosedRef::Present { .. })
    {
        return Err(invalid_record("exact actor evidence without a reference"));
    }
    Ok(())
}

fn check_evidence(evidence: &Evidence) -> Result<(), IncidentUnavailable> {
    match evidence {
        Evidence::ProcessExit { actor, .. }
        | Evidence::ProcessSignal { actor, .. }
        | Evidence::ProcessAbsent { actor }
        | Evidence::CgroupMembership { actor, .. } => check_actor(actor, Actor::OsProcess),
        Evidence::MemoryEvent { window, .. } => {
            if window.start_unix_ms > window.end_unix_ms {
                return Err(invalid_record("window ends before it starts"));
            }
            Ok(())
        }
        Evidence::ResourceSample {
            actor,
            resource,
            value,
        } => {
            if let Some(actor) = actor {
                check_actor(actor, Actor::OsProcess)?;
            }
            if *resource != Resource::RssBytes && *value > MAX_PRESSURE {
                return Err(invalid_record("pressure sample exceeds 100 percent"));
            }
            Ok(())
        }
        Evidence::ProviderCondition { actor, .. } => match actor {
            Some(actor) => check_actor(actor, Actor::ProviderSession),
            None => Ok(()),
        },
        Evidence::ComponentWitness { .. } | Evidence::Withheld { .. } => Ok(()),
    }
}

/// Whether the report's own observed evidence meets its claimed cause's
/// minimum. Withheld items never count.
fn cause_supported(cause: Cause, evidence: &[Evidence]) -> bool {
    match cause {
        Cause::Unknown => true,
        Cause::Exit => evidence
            .iter()
            .any(|item| matches!(item, Evidence::ProcessExit { .. })),
        Cause::Signal => evidence
            .iter()
            .any(|item| matches!(item, Evidence::ProcessSignal { .. })),
        Cause::ProviderCondition => evidence
            .iter()
            .any(|item| matches!(item, Evidence::ProviderCondition { .. })),
        Cause::ComponentFailure => evidence.iter().any(|item| {
            matches!(
                item,
                Evidence::ComponentWitness {
                    state: ComponentState::Failed,
                    ..
                }
            )
        }),
        // SIGKILL on one exact process, that process's leaf cgroup, and a
        // positive local oom_kill delta of that same cgroup.
        Cause::OomKill => evidence.iter().any(|signal| {
            let Evidence::ProcessSignal { actor, signal: 9 } = signal else {
                return false;
            };
            let Some(process) = exact_ref(actor) else {
                return false;
            };
            evidence.iter().any(|membership| {
                let Evidence::CgroupMembership {
                    actor,
                    cgroup: DisclosedRef::Present { reference: cgroup },
                } = membership
                else {
                    return false;
                };
                exact_ref(actor) == Some(process)
                    && evidence.iter().any(|event| {
                        matches!(event, Evidence::MemoryEvent {
                            cgroup: DisclosedRef::Present { reference },
                            counter: MemoryCounter::OomKill,
                            counting: Counting::Local,
                            delta,
                            ..
                        } if reference == cgroup && *delta >= 1)
                    })
            })
        }),
    }
}

fn check_report(report: &Report) -> Result<(), IncidentUnavailable> {
    if let Scope::Logical { subject } = &report.scope {
        if subject.root != report.subject.root {
            return Err(invalid_record("logical scope names another root"));
        }
    }
    for evidence in &report.evidence {
        check_evidence(evidence)?;
    }
    if !cause_supported(report.cause, &report.evidence) {
        return Err(invalid_record(
            "claimed cause lacks its minimum observed evidence",
        ));
    }
    Ok(())
}

fn check_verification(verification: &Verification) -> Result<(), IncidentUnavailable> {
    for check in &verification.checks {
        let allowed = match (check.check, check.result) {
            (CheckKind::ArtifactDigest, CheckResult::Passed) => check.digest.is_some(),
            (CheckKind::ArtifactDigest, CheckResult::Failed) => true,
            _ => check.digest.is_none(),
        };
        if !allowed {
            return Err(invalid_record(
                "digest belongs to a passed or failed artifact_digest check",
            ));
        }
    }
    Ok(())
}

impl Record {
    /// Admits one record line: bounded before parsing, schema-strict, then
    /// the normative cross-field rules.
    pub fn decode_line(line: &str) -> Result<Self, IncidentUnavailable> {
        if line.len() > MAX_RECORD_BYTES {
            return Err(IncidentUnavailable::new(
                UnavailableReason::InvalidRecord,
                format!("record exceeds {MAX_RECORD_BYTES} bytes"),
            ));
        }
        let value: Value =
            serde_json::from_str(line).map_err(|_| invalid_record("record is not JSON"))?;
        Self::decode(&value)
    }

    pub fn decode(value: &Value) -> Result<Self, IncidentUnavailable> {
        let record: Self = admit("Record", value, UnavailableReason::InvalidRecord)?;
        record.check()?;
        Ok(record)
    }

    pub fn encode_line(&self) -> String {
        serde_json::to_string(self).expect("record serializes")
    }

    pub fn kind(&self) -> RecordKind {
        match self {
            Self::Report(_) => RecordKind::Report,
            Self::ReportReceipt(_) => RecordKind::ReportReceipt,
            Self::ReportConflict(_) => RecordKind::ReportConflict,
            Self::Verification(_) => RecordKind::Verification,
            Self::RecoveryAuthorization(_) => RecordKind::RecoveryAuthorization,
        }
    }

    /// Normative cross-field rules beyond the structural schema.
    fn check(&self) -> Result<(), IncidentUnavailable> {
        match self {
            Record::Report(report) => check_report(report),
            Record::ReportReceipt(_) => Ok(()),
            Record::ReportConflict(conflict) => {
                check_report(&conflict.original)?;
                check_report(&conflict.submitted)?;
                if classify_report_repetition(&conflict.original, &conflict.submitted)
                    != Repetition::KeyConflict
                {
                    return Err(invalid_record(
                        "conflict needs changed content under the original key",
                    ));
                }
                Ok(())
            }
            Record::Verification(verification) => check_verification(verification),
            Record::RecoveryAuthorization(authorization) => {
                check_verification(&authorization.proof)?;
                if authorization.proof.incident != authorization.incident {
                    return Err(invalid_record("proof verifies another incident epoch"));
                }
                if contains(&authorization.proof.scope, &authorization.scope)
                    != Containment::Contained
                {
                    return Err(invalid_record("authorization scope exceeds its proof"));
                }
                if authorization.proof.proof() != ProofReading::Complete {
                    return Err(invalid_record("authorization rests on incomplete proof"));
                }
                Ok(())
            }
        }
    }

    /// The record is admissible and its kind and evidence types were
    /// selected.
    pub fn agree(&self, selected: &Selected) -> Result<(), IncidentUnavailable> {
        Record::decode(&serde_json::to_value(self).expect("serializes"))?;
        check_selected(selected)?;
        if !selected.records.contains(&self.kind()) {
            return Err(IncidentUnavailable::new(
                UnavailableReason::NoCommonCapability,
                "record kind was not selected",
            ));
        }
        let reports: Vec<&Report> = match self {
            Record::Report(report) => vec![report],
            Record::ReportConflict(conflict) => vec![&*conflict.submitted, &*conflict.original],
            _ => Vec::new(),
        };
        if reports
            .iter()
            .flat_map(|report| &report.evidence)
            .any(|evidence| !selected.evidence.contains(&evidence.evidence_type()))
        {
            return Err(IncidentUnavailable::new(
                UnavailableReason::NoCommonCapability,
                "evidence type was not selected",
            ));
        }
        Ok(())
    }
}

fn check_selected(selected: &Selected) -> Result<(), IncidentUnavailable> {
    if selected.protocol != PROTOCOL {
        return Err(violation("selected protocol is not supported"));
    }
    validate(
        "Offer",
        &serde_json::json!({ "records": selected.records, "evidence": selected.evidence }),
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
/// older or newer entries are ignored; the v1 entry must be strict. Absent or
/// incompatible incident capability is an [`IncidentUnavailable`] diagnostic.
pub fn select(local: &Offer, remote: &Value) -> Result<Selected, IncidentUnavailable> {
    validate(
        "Offer",
        &serde_json::to_value(local).expect("offer serializes"),
        UnavailableReason::InvalidAdvertisement,
    )
    .map_err(|_| {
        IncidentUnavailable::new(UnavailableReason::InvalidAdvertisement, "local offer")
    })?;
    let size = serde_json::to_string(remote)
        .map(|text| text.len())
        .unwrap_or(usize::MAX);
    if size > MAX_ADVERTISEMENT_BYTES {
        return Err(IncidentUnavailable::new(
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
        return Err(IncidentUnavailable::new(
            UnavailableReason::NoCommonVersion,
            format!("peer advertised {others} other protocol entries"),
        ));
    };
    let peer: Offer = admit("Offer", entry, UnavailableReason::InvalidAdvertisement)?;
    let records = common(&local.records, &peer.records);
    if records.is_empty() {
        return Err(IncidentUnavailable::new(
            UnavailableReason::NoCommonCapability,
            "no common record kind",
        ));
    }
    Ok(Selected {
        protocol: PROTOCOL.to_owned(),
        records,
        evidence: common(&local.evidence, &peer.evidence),
    })
}

/// Classifies report repetition by `(reporter, report_key)`, with session
/// control's meaning: `same_request` is the same report and counts once;
/// `key_conflict` is changed content, never a retry or a new report.
pub fn classify_report_repetition(first: &Report, again: &Report) -> Repetition {
    if first.reporter != again.reporter || first.report_key != again.report_key {
        Repetition::Distinct
    } else if first == again {
        Repetition::SameRequest
    } else {
        Repetition::KeyConflict
    }
}

/// Classifies authorization repetition by `(issuer coordinator, incident,
/// authorization_key)`. A re-issue at another epoch or fence under the same
/// key is a `key_conflict`.
pub fn classify_authorization_repetition(
    first: &RecoveryAuthorization,
    again: &RecoveryAuthorization,
) -> Repetition {
    if first.issuer.coordinator != again.issuer.coordinator
        || first.incident.incident != again.incident.incident
        || first.authorization_key != again.authorization_key
    {
        Repetition::Distinct
    } else if first == again {
        Repetition::SameRequest
    } else {
        Repetition::KeyConflict
    }
}
