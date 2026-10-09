//! Common bounded `session.read_turns` paging under
//! `oulipoly.session_turn_pages/v1`.
//!
//! The engine owns every paging mechanism: bounded newline-framed record scans,
//! retention and reconstruction of an unfinished final record, cursor, snapshot
//! and source-stamp validation, budgets and limits, canonical staging admission
//! with its durable cursor/prefix pool, `user_observation` HMAC cursors and
//! their binding digest and source reconstruction, response fitting and body
//! omission, exact read accounting, and recovery of interrupted publication.
//! Cursor contents stay opaque to the host.
//!
//! A [`PageAdapter`] supplies native facts only: its state namespace and default
//! data root, account and store resolution, ordered source candidates, the
//! session a first record claims, native session-id rules, and the projection of
//! one complete native record into a turn and its turn facts. The engine adds no
//! native path, identifier rule or turn-fact default. Facts the adapter reports
//! as [`NativeFact::Unavailable`] are refused, because v1 has no field state for
//! "unknown"; the engine never substitutes `false` or `null`.
//!
//! Byte offsets are locations, not proved logical identities. A cursor binds
//! the source's device/inode, its non-decreasing length, the digest of its first
//! record and a digest of the bytes just before the cursor's resume point;
//! continuation and resume re-check them before reading forward. These checks
//! are bounded: they detect replacement, truncation, a rewritten first record and
//! a rewrite that changes or shifts the verified boundary bytes, not a rewrite of
//! already-paged bytes that leaves both unchanged. See the README's paging
//! section for the accounting of those verification reads.
//!
//! This module advertises no capability and selects no reader authority,
//! requester policy, exposure destination or retention rule.

use crate::durable_fs;
use crate::encoding::sha256_hex;
use agent_provider_contract::generated::{
    ErrorCategory, ErrorObject, RequestEnvelope, SessionReadTurnsParams, SessionReadTurnsResult,
    SessionTurnBodyChunk, SessionTurnBodyState, SessionTurnPageStartMode, SessionTurnPageTurn,
    SessionTurnProjection, SuccessResponseEnvelope, TrueBool, CONTRACT_VERSION,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

mod observation;
mod staging;
#[cfg(test)]
mod tests;

pub use staging::StagingLimits;

/// The only read protocol this engine serves.
pub const READ_PROTOCOL: &str = "oulipoly.session_turn_pages/v1";
/// Records at or above this size are refused rather than skipped. At most one
/// unfinished record below it is retained between requests.
pub const MAX_RECORD_BYTES: usize = 8 * 1024 * 1024;
const MAX_TOKEN_BYTES: usize = 4096;
// Native bytes immediately before a cursor's resume point that the next
// continuation or resume re-reads and compares before reading forward.
const ANCHOR_BYTES: u64 = 64;
// Existing v1 declaration vocabulary, emitted only with its exact arithmetic:
// forward + metadata <= max_source_bytes, reconstruction < MAX_RECORD_BYTES,
// and their sum equals source_bytes_examined. The name is recorded vocabulary
// debt, not a provider choice.
const OBSERVATION_IO_DECLARATION: &str = "codex_observation_io_v1";

/// A native fact as the adapter knows it. `Unavailable` makes the engine refuse
/// the page through [`PageAdapter::unavailable_fact`]; it is never defaulted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NativeFact<T> {
    Known(T),
    Unavailable,
}

/// The page fields whose v1 shape has no "unknown" state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PageFact {
    ParentTurnId,
    IsSidechain,
    IsCompactionBoundary,
    SourceFinal,
}

/// Source discovery outcomes the adapter names in its own error vocabulary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceFailure {
    NotFound,
    Ambiguous,
    Io,
}

/// One complete native record, without its terminating newline.
pub struct NativeRecord<'a> {
    pub bytes: &'a [u8],
    /// Byte location of the record's first byte in the native source.
    pub start: u64,
    /// Byte location just past the record's terminating newline.
    pub end: u64,
    pub session_id: &'a str,
    pub projection: &'a SessionTurnProjection,
}

/// A native record projected into one turn. `text` holds the ordered text
/// parts of the turn body; an empty list is an absent body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeTurn {
    pub turn_id: String,
    pub timestamp: String,
    pub role: String,
    pub text: Vec<String>,
    pub parent_turn_id: NativeFact<Option<String>>,
    pub is_sidechain: NativeFact<bool>,
    pub is_compaction_boundary: NativeFact<bool>,
}

/// What the engine knows about the page when it asks for `source_final`.
pub struct SnapshotFacts<'a> {
    pub session_id: &'a str,
    pub snapshot_complete: bool,
}

/// Native facts a provider plugs into the common engine. Implementations read
/// no paging state and perform no budgeting, staging or token handling.
pub trait PageAdapter {
    /// Provider-scoped state namespace: one lowercase path component.
    fn namespace(&self) -> &str;

    /// Data root used when the host supplies no `host.data_root`.
    fn default_data_root(&self, request: &RequestEnvelope) -> Result<PathBuf, ErrorObject>;

    /// Native account/store root for `settings_id`. It is bound into every
    /// cursor, so a different account never continues another's cursor.
    fn account(&self, request: &RequestEnvelope, settings_id: &str)
        -> Result<PathBuf, ErrorObject>;

    /// Native session-id rules beyond the schema's 1..=1024 characters.
    fn validate_session_id(&self, session_id: &str) -> bool;

    /// Candidate native sources in examination order. `bound` is true when an
    /// authenticated cursor already binds one physical source; the engine then
    /// opens only the candidate with that device/inode.
    fn source_candidates(
        &self,
        account: &Path,
        session_id: &str,
        bound: bool,
        request: &RequestEnvelope,
    ) -> Result<Vec<PathBuf>, ErrorObject>;

    /// The native session a source's first record claims. A malformed first
    /// record is the adapter's own error.
    fn first_record_session(
        &self,
        record: &[u8],
        request: &RequestEnvelope,
    ) -> Result<String, ErrorObject>;

    fn source_error(&self, failure: SourceFailure, request: &RequestEnvelope) -> ErrorObject;

    /// Projects one complete record. `Ok(None)` is a record that is not a turn
    /// of this projection; under `user_observation` that includes any record
    /// whose whole textual input is not observable. A malformed record is an
    /// error, never a skip.
    fn project(
        &self,
        record: &NativeRecord<'_>,
        request: &RequestEnvelope,
    ) -> Result<Option<NativeTurn>, ErrorObject>;

    fn source_final(&self, facts: &SnapshotFacts<'_>) -> NativeFact<bool>;

    /// Refusal for a required page fact the adapter cannot supply.
    fn unavailable_fact(&self, fact: PageFact, request: &RequestEnvelope) -> ErrorObject;
}

/// Serves one `session.read_turns` page request with the default staging limits.
pub fn read_turns<A: PageAdapter + ?Sized>(
    adapter: &A,
    request: &RequestEnvelope,
) -> Result<SessionReadTurnsResult, ErrorObject> {
    read_turns_with_limits(adapter, request, StagingLimits::DEFAULT)
}

fn error(category: ErrorCategory, code: &str, message: &str) -> ErrorObject {
    ErrorObject {
        code: code.into(),
        category,
        message: message.into(),
        retryable: false,
        details: None,
        diagnostics: Vec::new(),
    }
}

fn invalid(message: &str) -> ErrorObject {
    error(
        ErrorCategory::InvalidRequest,
        "invalid_session_read_turns_params",
        message,
    )
}

fn stale() -> ErrorObject {
    error(
        ErrorCategory::Conflict,
        "session_turn_page_token_stale",
        "Cursor does not match the selected account, session, projection, budgets, or source generation",
    )
}

fn io_error() -> ErrorObject {
    error(
        ErrorCategory::Failed,
        "session_turn_page_io",
        "Could not read or persist bounded session paging state",
    )
}

fn capacity(message: &str) -> ErrorObject {
    error(
        ErrorCategory::InvalidRequest,
        "session_turn_page_budget_too_small",
        message,
    )
}

fn record_limit() -> ErrorObject {
    error(
        ErrorCategory::Unsupported,
        "session_turn_record_ceiling_exceeded",
        "Native record exceeds the supported 8388608-byte framing ceiling; checkpoint retained",
    )
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Binding {
    provider: String,
    account: PathBuf,
    settings: String,
    session: String,
    projection: String,
    nonce: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Budgets {
    turns: usize,
    response: usize,
    source: usize,
    inline: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Stamp {
    device: u64,
    inode: u64,
    len: u64,
    modified: u128,
}

/// Native bytes `[start, cursor offset)` and their digest.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Span {
    start: u64,
    sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Cursor {
    kind: String,
    binding: Binding,
    budgets: Budgets,
    stamp: Stamp,
    /// Digest of the source's first record, re-read by discovery each request.
    head: String,
    snapshot: String,
    offset: u64,
    page: u64,
    sequence: u64,
    partial_record: Option<Span>,
    anchor: Option<Span>,
}

fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn validate_span(span: &Span, offset: u64, maximum: u64) -> Result<u64, ErrorObject> {
    let len = offset.checked_sub(span.start).ok_or_else(stale)?;
    if len == 0 || len > maximum || !is_digest(&span.sha256) {
        return Err(stale());
    }
    Ok(len)
}

fn stamp(file: &File) -> Result<Stamp, ErrorObject> {
    let metadata = file.metadata().map_err(|_| io_error())?;
    let (device, inode) = file_identity(&metadata);
    Ok(Stamp {
        device,
        inode,
        len: metadata.len(),
        modified: metadata
            .modified()
            .map_err(|_| io_error())?
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| io_error())?
            .as_nanos(),
    })
}

fn file_identity(metadata: &std::fs::Metadata) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt;
    (metadata.dev(), metadata.ino())
}

// Same physical source, never shorter, and an unchanged length keeps its mtime.
fn require_generation(old: &Stamp, current: &Stamp) -> Result<(), ErrorObject> {
    if old.device != current.device
        || old.inode != current.inode
        || current.len < old.len
        || (current.len == old.len && old.modified != current.modified)
    {
        return Err(stale());
    }
    Ok(())
}

fn valid_namespace(namespace: &str) -> bool {
    !namespace.is_empty()
        && namespace.len() <= 64
        && namespace != "."
        && namespace != ".."
        && namespace
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
}

fn state_root<A: PageAdapter + ?Sized>(
    adapter: &A,
    request: &RequestEnvelope,
) -> Result<PathBuf, ErrorObject> {
    let namespace = adapter.namespace();
    if !valid_namespace(namespace) {
        return Err(io_error());
    }
    let root = match &request.host.data_root {
        Some(path) if Path::new(path).is_absolute() => PathBuf::from(path),
        Some(_) => return Err(invalid("host.data_root must be absolute")),
        None => adapter.default_data_root(request)?,
    }
    .join("provider-state")
    .join(namespace)
    .join("session-pages-v1");
    durable_fs::create_private_directories(&root).map_err(|_| io_error())?;
    Ok(root)
}

struct Located {
    file: File,
    metadata: usize,
    head: String,
    content_start: u64,
}

// Candidates come from the adapter; reading their first records is charged
// against this request's source quantum, one byte of buffering at a time so no
// uncharged native byte is prefetched. A bound source is selected by
// device/inode before any header is opened.
fn locate<A: PageAdapter + ?Sized>(
    adapter: &A,
    request: &RequestEnvelope,
    account: &Path,
    session_id: &str,
    maximum: usize,
    bound: Option<(u64, u64)>,
) -> Result<Option<Located>, ErrorObject> {
    let candidates = adapter.source_candidates(account, session_id, bound.is_some(), request)?;
    let io = || adapter.source_error(SourceFailure::Io, request);
    let mut examined = 0usize;
    let mut found: Option<(File, Vec<u8>)> = None;
    for path in candidates {
        if let Some(identity) = bound {
            let metadata = std::fs::symlink_metadata(&path).map_err(|_| io())?;
            if !metadata.is_file() || file_identity(&metadata) != identity {
                continue;
            }
        }
        let file = File::open(&path).map_err(|_| io())?;
        if bound.is_some_and(|identity| {
            file.metadata().map(|m| file_identity(&m)).ok() != Some(identity)
        }) {
            return Err(io());
        }
        let remaining = maximum.saturating_sub(examined);
        if remaining == 0 {
            return Err(capacity(
                "Source budget cannot admit source identity metadata",
            ));
        }
        let mut reader = BufReader::with_capacity(1, file.take(remaining as u64));
        let mut first = Vec::new();
        reader.read_until(b'\n', &mut first).map_err(|_| io())?;
        examined += first.len();
        if !first.ends_with(b"\n") {
            return Err(capacity(
                "Source budget cannot admit the complete source identity record",
            ));
        }
        let claimed = adapter.first_record_session(&first[..first.len() - 1], request)?;
        if claimed == session_id {
            if found.is_some() {
                return Err(adapter.source_error(SourceFailure::Ambiguous, request));
            }
            found = Some((reader.into_inner().into_inner(), first));
            if bound.is_some() {
                break;
            }
        }
    }
    Ok(found.map(|(file, first)| Located {
        file,
        metadata: examined,
        head: sha256_hex(&first),
        content_start: first.len() as u64,
    }))
}

// Canonical cursors live in the admitted durable pool; observation cursors are
// authenticated tokens reconstructed from native source and never stored.
enum PageStorage {
    Canonical {
        root: PathBuf,
        prefix: String,
        admission: staging::Admission,
        limits: StagingLimits,
    },
    Observation {
        prefix: String,
        key: [u8; 32],
    },
}

impl PageStorage {
    fn is_observation(&self) -> bool {
        matches!(self, Self::Observation { .. })
    }

    fn load(&self, value: &str, binding: &Binding) -> Result<Cursor, ErrorObject> {
        match self {
            Self::Observation { prefix, key } => observation::load(prefix, key, value, binding),
            Self::Canonical { root, prefix, .. } => staging::load(root, prefix, value),
        }
    }

    fn token(&self, cursor: &Cursor) -> Result<String, ErrorObject> {
        let token = match self {
            Self::Observation { prefix, key } => observation::token(prefix, key, cursor)?,
            Self::Canonical { prefix, .. } => staging::token(prefix, cursor)?,
        };
        if token.len() > MAX_TOKEN_BYTES {
            return Err(io_error());
        }
        Ok(token)
    }

    fn stage(&mut self, start: u64, bytes: &[u8]) -> Result<Span, ErrorObject> {
        if bytes.len() >= MAX_RECORD_BYTES {
            return Err(record_limit());
        }
        match self {
            Self::Observation { .. } => Ok(Span {
                start,
                sha256: sha256_hex(bytes),
            }),
            Self::Canonical {
                root,
                admission,
                limits,
                ..
            } => staging::stage_partial(root, admission, *limits, start, bytes),
        }
    }

    fn persist(&mut self, cursor: &Cursor) -> Result<(), ErrorObject> {
        match self {
            Self::Observation { .. } => Ok(()),
            Self::Canonical {
                root,
                admission,
                limits,
                ..
            } => staging::persist(root, admission, *limits, cursor),
        }
    }
}

/// Every native read of one request. `reconstruction` re-reads bytes an
/// earlier request already examined; only observation declares it separately.
#[derive(Default)]
struct ReadAccounting {
    metadata: usize,
    forward: usize,
    reconstruction: usize,
}

impl ReadAccounting {
    fn total(&self) -> usize {
        self.metadata + self.forward + self.reconstruction
    }

    fn warnings(&self, observation: bool) -> Vec<String> {
        if !observation {
            return Vec::new();
        }
        vec![format!(
            "{OBSERVATION_IO_DECLARATION}:forward={};reconstruction={};metadata={}",
            self.forward, self.reconstruction, self.metadata
        )]
    }
}

struct Request<'a> {
    envelope: &'a RequestEnvelope,
    params: SessionReadTurnsParams,
    budgets: Budgets,
}

fn parse(request: &RequestEnvelope) -> Result<Request<'_>, ErrorObject> {
    let wire = serde_json::to_value(request).map_err(|_| invalid("Invalid paging parameters"))?;
    agent_provider_contract::schemas::validate_request("session.read_turns", &wire)
        .map_err(|_| invalid("Invalid paging parameters"))?;
    let params: SessionReadTurnsParams = serde_json::from_value(request.params.clone())
        .map_err(|_| invalid("Invalid paging parameters"))?;
    let usize_of =
        |value: u64| usize::try_from(value).map_err(|_| invalid("Invalid paging budget"));
    let budgets = Budgets {
        turns: usize_of(params.max_turns)?,
        response: usize_of(params.max_response_bytes)?,
        source: usize_of(params.max_source_bytes)?,
        inline: usize_of(params.max_inline_body_bytes)?,
    };
    Ok(Request {
        envelope: request,
        params,
        budgets,
    })
}

fn projection_name(projection: &SessionTurnProjection) -> &'static str {
    match projection {
        SessionTurnProjection::CanonicalIngest => "canonical_ingest",
        SessionTurnProjection::UserObservation => "user_observation",
    }
}

/// [`read_turns`] with explicit canonical staging limits.
pub fn read_turns_with_limits<A: PageAdapter + ?Sized>(
    adapter: &A,
    envelope: &RequestEnvelope,
    limits: StagingLimits,
) -> Result<SessionReadTurnsResult, ErrorObject> {
    let request = parse(envelope)?;
    let p = &request.params;
    if !adapter.validate_session_id(&p.session_id) {
        return Err(invalid("Invalid paging protocol or identity"));
    }
    let provider = envelope
        .provider_instance_id
        .as_deref()
        .filter(|s| !s.trim().is_empty() && s.len() <= 1024)
        .ok_or_else(|| invalid("provider_instance_id is required"))?;
    let account = adapter.account(envelope, &p.settings_id)?;
    let binding = Binding {
        provider: provider.into(),
        account,
        settings: p.settings_id.clone(),
        session: p.session_id.clone(),
        projection: projection_name(&p.turn_projection).into(),
        nonce: p.expected_delivery_nonce.clone(),
    };
    let root = state_root(adapter, envelope)?;
    let namespace = adapter.namespace();
    let observation = p.turn_projection == SessionTurnProjection::UserObservation;
    let mut storage = if observation {
        let prefix = format!("{namespace}-obs1-");
        let issued = [&p.page_token, &p.after_token]
            .into_iter()
            .flatten()
            .any(|token| token.starts_with(&prefix));
        let key = observation::key(&root, issued)?;
        PageStorage::Observation { prefix, key }
    } else {
        let admission = staging::Admission::acquire(&root)?;
        PageStorage::Canonical {
            root,
            prefix: format!("{namespace}-stp1-"),
            admission,
            limits,
        }
    };
    let old = p
        .page_token
        .as_ref()
        .or(p.after_token.as_ref())
        .map(|token| storage.load(token, &binding))
        .transpose()?;
    if old.as_ref().is_some_and(|old| old.binding != binding) {
        return Err(stale());
    }
    let located = locate(
        adapter,
        envelope,
        &binding.account,
        &p.session_id,
        request.budgets.source,
        old.as_ref().map(|old| (old.stamp.device, old.stamp.inode)),
    )?
    .ok_or_else(|| {
        // A bound physical source that disappeared or was replaced is a stale
        // cursor, not a fresh lookup miss.
        if old.is_some() {
            stale()
        } else {
            adapter.source_error(SourceFailure::NotFound, envelope)
        }
    })?;
    let Located {
        mut file,
        metadata,
        head,
        content_start,
    } = located;
    let current = stamp(&file)?;
    if let Some(old) = &old {
        if old.offset > current.len || old.offset < content_start || old.head != head {
            return Err(stale());
        }
        require_generation(&old.stamp, &current)?;
    }
    let state = if p.start_mode == SessionTurnPageStartMode::Continuation {
        let old = old.ok_or_else(stale)?;
        if old.kind != "page"
            || old.budgets != request.budgets
            || Some(&old.snapshot) != p.snapshot_id.as_ref()
        {
            return Err(stale());
        }
        old
    } else {
        if old.as_ref().is_some_and(|s| s.kind != "resume") {
            return Err(stale());
        }
        let offset = old.as_ref().map_or(content_start, |s| s.offset);
        let snapshot = sha256_hex(
            &serde_json::to_vec(&serde_json::json!({
                "binding": binding, "stamp": current, "head": head,
                "offset": offset, "budgets": request.budgets,
            }))
            .map_err(|_| io_error())?,
        );
        let (partial_record, anchor) = old.map_or((None, None), |s| (s.partial_record, s.anchor));
        Cursor {
            kind: "page".into(),
            binding,
            budgets: request.budgets.clone(),
            stamp: current.clone(),
            head,
            snapshot,
            offset,
            page: 0,
            sequence: 0,
            partial_record,
            anchor,
        }
    };
    let mut scan = Scan {
        adapter,
        request: &request,
        storage: &mut storage,
        state: &state,
        file: &mut file,
        metadata,
    };
    let outcome = if p.start_mode == SessionTurnPageStartMode::Tail {
        scan.tail(&current)?
    } else {
        scan.forward()?
    };
    require_generation(&current, &stamp(&file)?)?;
    let result = fit(adapter, &request, &storage, &state, outcome)?;
    storage.persist(&result.1)?;
    Ok(result.0)
}

struct Outcome {
    next: Cursor,
    turns: Vec<SessionTurnPageTurn>,
    examined: ReadAccounting,
    complete: bool,
    framed_checkpoint: Option<Cursor>,
}

struct Scan<'a, A: PageAdapter + ?Sized> {
    adapter: &'a A,
    request: &'a Request<'a>,
    storage: &'a mut PageStorage,
    state: &'a Cursor,
    file: &'a mut File,
    metadata: usize,
}

impl<A: PageAdapter + ?Sized> Scan<'_, A> {
    fn tail(&mut self, current: &Stamp) -> Result<Outcome, ErrorObject> {
        let quantum = self.request.budgets.source - self.metadata;
        let start = current
            .len
            .saturating_sub(quantum as u64)
            .max(self.state.offset);
        self.file
            .seek(SeekFrom::Start(start))
            .map_err(|_| io_error())?;
        let mut bytes = Vec::new();
        (&mut *self.file)
            .take(current.len - start)
            .read_to_end(&mut bytes)
            .map_err(|_| io_error())?;
        if bytes.len() as u64 != current.len - start {
            return Err(stale());
        }
        let mut next = self.state.clone();
        match bytes.iter().rposition(|b| *b == b'\n') {
            Some(index) => {
                next.offset = start + index as u64 + 1;
                next.anchor = anchor(start, &bytes, next.offset, None);
            }
            None if start == current.len => (),
            None => {
                return Err(capacity(
                    "Tail budget cannot locate a complete record boundary",
                ))
            }
        }
        Ok(Outcome {
            next,
            turns: Vec::new(),
            examined: ReadAccounting {
                metadata: self.metadata,
                forward: bytes.len(),
                reconstruction: 0,
            },
            complete: true,
            framed_checkpoint: None,
        })
    }

    // Native bytes `[record_start, state.offset)` before forward reading: the
    // unfinished record, from staging (canonical) or native source
    // (observation), after the boundary anchor is verified against the source.
    fn prefix(
        &mut self,
        examined: &mut ReadAccounting,
        quantum: usize,
    ) -> Result<Vec<u8>, ErrorObject> {
        let state = self.state;
        let observation = self.storage.is_observation();
        let partial = state
            .partial_record
            .as_ref()
            .map(|span| {
                validate_span(span, state.offset, MAX_RECORD_BYTES as u64 - 1)
                    .map(|len| (span, len))
            })
            .transpose()?;
        if let Some(anchor) = &state.anchor {
            let len = validate_span(anchor, state.offset, ANCHOR_BYTES)?;
            if partial.is_some_and(|(span, _)| anchor.start < span.start) {
                return Err(stale());
            }
            // Observation re-reads the whole partial record from native source
            // below, which covers the anchor; otherwise read the window now.
            if !(observation && partial.is_some()) {
                if !observation && len > quantum as u64 {
                    return Err(capacity(
                        "Source budget cannot admit the cursor boundary check",
                    ));
                }
                let window = self.read_native(anchor.start, len)?;
                if observation {
                    examined.reconstruction += window.len();
                } else {
                    examined.forward += window.len();
                }
                if sha256_hex(&window) != anchor.sha256 {
                    return Err(stale());
                }
            }
        }
        let Some((span, len)) = partial else {
            return Ok(Vec::new());
        };
        let bytes = match &*self.storage {
            PageStorage::Observation { .. } => {
                let bytes = self.read_native(span.start, len)?;
                examined.reconstruction += bytes.len();
                if let Some(anchor) = &state.anchor {
                    let from = (anchor.start - span.start) as usize;
                    if sha256_hex(&bytes[from..]) != anchor.sha256 {
                        return Err(stale());
                    }
                }
                bytes
            }
            PageStorage::Canonical { root, .. } => staging::partial_bytes(root, span, len)?,
        };
        if bytes.len() as u64 != len || sha256_hex(&bytes) != span.sha256 || bytes.contains(&b'\n')
        {
            return Err(stale());
        }
        Ok(bytes)
    }

    fn read_native(&mut self, start: u64, len: u64) -> Result<Vec<u8>, ErrorObject> {
        self.file
            .seek(SeekFrom::Start(start))
            .map_err(|_| io_error())?;
        let mut bytes = Vec::new();
        (&mut *self.file)
            .take(len)
            .read_to_end(&mut bytes)
            .map_err(|_| io_error())?;
        if bytes.len() as u64 != len {
            return Err(stale());
        }
        Ok(bytes)
    }

    fn forward(&mut self) -> Result<Outcome, ErrorObject> {
        let state = self.state;
        let p = &self.request.params;
        let mut examined = ReadAccounting {
            metadata: self.metadata,
            ..ReadAccounting::default()
        };
        // Canonical verification reads share the source quantum; observation
        // reconstruction is declared separately and stays outside it.
        let quantum = self.request.budgets.source - self.metadata;
        let mut bytes = self.prefix(&mut examined, quantum)?;
        let quantum = quantum - examined.forward;
        let anchor_reads = examined.forward;
        let prefix_len = bytes.len();
        let record_start = state.offset - prefix_len as u64;
        let maximum = (state.stamp.len - state.offset)
            .min(quantum as u64)
            .min((MAX_RECORD_BYTES - prefix_len) as u64);
        self.file
            .seek(SeekFrom::Start(state.offset))
            .map_err(|_| io_error())?;
        (&mut *self.file)
            .take(maximum)
            .read_to_end(&mut bytes)
            .map_err(|_| io_error())?;
        let native_read = bytes.len() - prefix_len;
        examined.forward = anchor_reads + native_read;
        if native_read as u64 != maximum {
            return Err(stale());
        }
        let mut next = state.clone();
        let mut turns = Vec::new();
        let mut consumed = 0;
        for line in bytes.split_inclusive(|b| *b == b'\n') {
            if !line.ends_with(b"\n") {
                break;
            }
            let offset = record_start + consumed as u64;
            let end = offset + line.len() as u64;
            let record = NativeRecord {
                bytes: &line[..line.len() - 1],
                start: offset,
                end,
                session_id: &p.session_id,
                projection: &p.turn_projection,
            };
            let projected = self
                .adapter
                .project(&record, self.request.envelope)?
                .filter(|turn| {
                    p.turn_projection != SessionTurnProjection::UserObservation
                        || turn.role == "user"
                });
            if let Some(native) = projected {
                if turns.len() == self.request.budgets.turns {
                    break;
                }
                let mut turn = materialize(
                    self.adapter,
                    self.request,
                    native,
                    state.sequence + turns.len() as u64,
                )?;
                let mut candidate = next.clone();
                candidate.offset = end;
                candidate.partial_record = None;
                candidate.anchor = anchor(record_start, &bytes, end, None);
                turns.push(turn.clone());
                let complete = end == state.stamp.len;
                if !self.fits_candidate(&candidate, &turns, &examined, complete)? {
                    omit_body(&mut turn);
                    *turns.last_mut().expect("pushed") = turn;
                    if !self.fits_candidate(&candidate, &turns, &examined, complete)? {
                        turns.pop();
                        if turns.is_empty() {
                            return Err(capacity(
                                "Response budget cannot hold the next turn metadata",
                            ));
                        }
                        break;
                    }
                }
            }
            consumed += line.len();
            next.offset = end;
            next.partial_record = None;
            next.anchor = anchor(record_start, &bytes, end, None);
        }
        let mut complete = next.offset == state.stamp.len;
        let mut framed_checkpoint = None;
        // Only an unframed suffix is retained. If a turn or response limit
        // stopped the scan before a newline, that whole record is read again.
        if consumed < bytes.len() && !bytes[consumed..].contains(&b'\n') {
            if consumed > 0 {
                framed_checkpoint = Some(next.clone());
            }
            let start = record_start + consumed as u64;
            let partial = self.storage.stage(start, &bytes[consumed..])?;
            next.offset = record_start + bytes.len() as u64;
            next.anchor = anchor(record_start, &bytes, next.offset, Some(start));
            next.partial_record = Some(partial);
            // EOF coverage does not project an unfinished record; a later
            // append supplies its suffix.
            complete = next.offset == state.stamp.len;
        }
        if next.offset == state.offset && !complete {
            return Err(capacity(
                "Source budget cannot hold the next complete record",
            ));
        }
        Ok(Outcome {
            next,
            turns,
            examined,
            complete,
            framed_checkpoint,
        })
    }

    fn fits_candidate(
        &self,
        candidate: &Cursor,
        turns: &[SessionTurnPageTurn],
        examined: &ReadAccounting,
        complete: bool,
    ) -> Result<bool, ErrorObject> {
        let (result, _) = page_result(
            self.adapter,
            self.request,
            self.storage,
            self.state,
            candidate.clone(),
            turns,
            examined,
            complete,
        )?;
        fits(&result, self.request)
    }
}

// Up to ANCHOR_BYTES of in-memory native bytes ending at `end`, never before
// `floor` (the unfinished record's start, which its own digest covers).
fn anchor(memory_start: u64, bytes: &[u8], end: u64, floor: Option<u64>) -> Option<Span> {
    let low = memory_start
        .max(floor.unwrap_or(0))
        .max(end.saturating_sub(ANCHOR_BYTES));
    if end <= low {
        return None;
    }
    let window = &bytes[(low - memory_start) as usize..(end - memory_start) as usize];
    Some(Span {
        start: low,
        sha256: sha256_hex(window),
    })
}

fn omit_body(turn: &mut SessionTurnPageTurn) {
    if turn.body_state == SessionTurnBodyState::Inline {
        turn.body_state = SessionTurnBodyState::OmittedOversize;
        turn.body = None;
    }
}

fn known<T, A: PageAdapter + ?Sized>(
    adapter: &A,
    request: &RequestEnvelope,
    fact: PageFact,
    value: NativeFact<T>,
) -> Result<T, ErrorObject> {
    match value {
        NativeFact::Known(value) => Ok(value),
        NativeFact::Unavailable => Err(adapter.unavailable_fact(fact, request)),
    }
}

// The exact host chunk field order: type, then text.
#[derive(Serialize)]
struct Chunk<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    text: &'a str,
}

fn bounded(value: &str, maximum: usize) -> bool {
    !value.is_empty() && value.chars().count() <= maximum
}

fn materialize<A: PageAdapter + ?Sized>(
    adapter: &A,
    request: &Request<'_>,
    native: NativeTurn,
    sequence: u64,
) -> Result<SessionTurnPageTurn, ErrorObject> {
    let envelope = request.envelope;
    let parent = known(
        adapter,
        envelope,
        PageFact::ParentTurnId,
        native.parent_turn_id,
    )?;
    let is_sidechain = known(
        adapter,
        envelope,
        PageFact::IsSidechain,
        native.is_sidechain,
    )?;
    let is_compaction_boundary = known(
        adapter,
        envelope,
        PageFact::IsCompactionBoundary,
        native.is_compaction_boundary,
    )?;
    if !bounded(&native.turn_id, 1024)
        || !bounded(&native.timestamp, 128)
        || !bounded(&native.role, 64)
        || parent.as_deref().is_some_and(|id| !bounded(id, 1024))
    {
        return Err(invalid(
            "Native record projection violates the page contract",
        ));
    }
    let mut text = native.text;
    if let Some(nonce) = &request.params.expected_delivery_nonce {
        let joined = text.concat();
        let marker = format!("[OULIPOLY-DELIVERY {nonce}]");
        if let Some(prefix) = joined.trim_end().strip_suffix(&marker) {
            if prefix.is_empty() || prefix.ends_with(char::is_whitespace) {
                text = vec![prefix.trim_end().to_owned()];
            }
        }
    }
    let chunks: Vec<_> = text
        .iter()
        .map(|text| Chunk { kind: "text", text })
        .collect();
    let encoded = serde_json::to_vec(&chunks).map_err(|_| io_error())?;
    let canonical = text.concat().replace("\r\n", "\n").replace('\r', "\n");
    let empty = chunks.is_empty();
    let body_state = if empty {
        SessionTurnBodyState::Absent
    } else if encoded.len() > request.budgets.inline {
        SessionTurnBodyState::OmittedOversize
    } else {
        SessionTurnBodyState::Inline
    };
    let body = (body_state == SessionTurnBodyState::Inline).then(|| {
        text.iter()
            .map(|text| SessionTurnBodyChunk {
                kind: "text".into(),
                text: Some(text.clone()),
                extension_fields: Default::default(),
            })
            .collect()
    });
    Ok(SessionTurnPageTurn {
        session_id: request.params.session_id.clone(),
        turn_id: native.turn_id,
        snapshot_sequence: sequence,
        timestamp: native.timestamp,
        role: native.role,
        parent_turn_id: parent,
        is_sidechain,
        is_compaction_boundary,
        body_state,
        body,
        body_bytes: (!empty).then_some(encoded.len() as u64),
        body_sha256: (!empty).then(|| sha256_hex(&encoded)),
        canonical_text_sha256: (!empty).then(|| sha256_hex(canonical.trim().as_bytes())),
    })
}

#[allow(clippy::too_many_arguments)]
fn page_result<A: PageAdapter + ?Sized>(
    adapter: &A,
    request: &Request<'_>,
    storage: &PageStorage,
    state: &Cursor,
    mut next: Cursor,
    turns: &[SessionTurnPageTurn],
    examined: &ReadAccounting,
    complete: bool,
) -> Result<(SessionReadTurnsResult, Cursor), ErrorObject> {
    next.kind = if complete { "resume" } else { "page" }.into();
    next.page = state.page + 1;
    next.sequence = state.sequence + turns.len() as u64;
    let token = storage.token(&next)?;
    let source_final = known(
        adapter,
        request.envelope,
        PageFact::SourceFinal,
        adapter.source_final(&SnapshotFacts {
            session_id: &state.binding.session,
            snapshot_complete: complete,
        }),
    )?;
    let result = SessionReadTurnsResult {
        read_protocol: READ_PROTOCOL.into(),
        provider_instance_id: state.binding.provider.clone(),
        settings_id: state.binding.settings.clone(),
        session_id: state.binding.session.clone(),
        turn_projection: request.params.turn_projection.clone(),
        snapshot_id: state.snapshot.clone(),
        page_index: state.page,
        page_start_sequence: state.sequence,
        turns: turns.to_vec(),
        page_turn_count: turns.len() as u64,
        source_bytes_examined: examined.total() as u64,
        scan_progress: !complete && turns.is_empty() && next.offset > state.offset,
        snapshot_complete: complete,
        next_page_token: (!complete).then(|| token.clone()),
        resume_token: complete.then_some(token),
        source_final,
        warnings: examined.warnings(storage.is_observation()),
    };
    Ok((result, next))
}

fn response_bytes(
    result: &SessionReadTurnsResult,
    request: &Request<'_>,
) -> Result<Vec<u8>, ErrorObject> {
    serde_json::to_vec(&SuccessResponseEnvelope {
        contract: CONTRACT_VERSION.into(),
        request_id: request.envelope.request_id.clone(),
        ok: TrueBool,
        result,
    })
    .map_err(|_| io_error())
}

// The framed response plus its NDJSON newline must fit: `len + 1 <= budget`.
fn fits(result: &SessionReadTurnsResult, request: &Request<'_>) -> Result<bool, ErrorObject> {
    Ok(response_bytes(result, request)?.len() < request.budgets.response)
}

fn fit<A: PageAdapter + ?Sized>(
    adapter: &A,
    request: &Request<'_>,
    storage: &PageStorage,
    state: &Cursor,
    outcome: Outcome,
) -> Result<(SessionReadTurnsResult, Cursor), ErrorObject> {
    let Outcome {
        next,
        mut turns,
        examined,
        complete,
        framed_checkpoint,
    } = outcome;
    let page = |turns: &[SessionTurnPageTurn], next: Cursor, complete: bool| {
        page_result(
            adapter, request, storage, state, next, turns, &examined, complete,
        )
    };
    let mut result = page(&turns, next.clone(), complete)?;
    // A token grows when an unfinished suffix is retained: fit the final
    // cursor, keeping every turn and digest and omitting inline bodies only as
    // needed, newest first.
    for index in (0..turns.len()).rev() {
        if fits(&result.0, request)? {
            break;
        }
        if turns[index].body_state == SessionTurnBodyState::Inline {
            omit_body(&mut turns[index]);
            result = page(&turns, next.clone(), complete)?;
        }
    }
    if !fits(&result.0, request)? {
        if let Some(checkpoint) = framed_checkpoint {
            // Publish real progress at the preceding complete boundary and
            // leave the unfinished suffix for replay, charging every read.
            result = page(&turns, checkpoint, false)?;
        }
    }
    if !fits(&result.0, request)? {
        return Err(capacity("Response budget cannot hold paging metadata"));
    }
    let wire: Value =
        serde_json::from_slice(&response_bytes(&result.0, request)?).map_err(|_| io_error())?;
    agent_provider_contract::schemas::validate_response("session.read_turns", &wire)
        .map_err(|_| invalid("Native record projection violates the page contract"))?;
    Ok(result)
}
