use std::collections::HashMap;
use std::io::Read;

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Map, Value};

use super::transport::{Incoming, Transport};
use super::wire::{self, method};
use super::{
    DEDUP_CONTRACT_META, DEDUP_CONTRACT_VERSION, DUPLICATE_META, LIVE_REATTACH_META,
    LIVE_REATTACH_VERSION, MESSAGE_KEY_META, NATIVE_TURN_META, PROTOCOL_VERSION,
    RESIDENT_SESSION_META,
};
use crate::resident_session;

/// Identity this client reports in `initialize`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientInfo {
    pub name: String,
    pub version: String,
}

/// The agent after a successful v2 negotiation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegotiatedPeer {
    pub protocol_version: u16,
    pub agent_name: String,
    pub agent_version: String,
    /// The agent advertised the local dedup contract (version 1).
    pub dedup_contract: bool,
    /// The agent declared the live reattachment contract (version 1) on
    /// this connection.
    pub live_reattach: bool,
    /// The schema-valid `oulipoly.resident_session` declaration in the
    /// agent's `initialize` `_meta`. `None`: absent or invalid; nothing is
    /// claimed. A declaration is the agent's word, not native behaviour.
    pub resident_session: Option<ResidentDeclaration>,
}

/// An agent's `ResidentSessionMeta` declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResidentDeclaration {
    /// The resident contract served, e.g. `oulipoly.resident_session/v1`.
    pub protocol: String,
    /// The ACP draft schema tag served.
    pub acp_schema: String,
}

/// Custody of a resident native turn's provider/v1 launch, as the endpoint
/// reported it in `NativeTurnMeta`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeCustody {
    /// Exit delivered and journal sealed.
    Complete,
    CompleteWithoutExit,
    /// Stopped before admission: nothing ran.
    NotAdmitted,
    /// An earlier process's interrupted launch was discharged; nothing ran
    /// again.
    Reconciled,
    /// The lifecycle failed; custody may be incomplete.
    Incomplete,
}

/// A schema-valid `NativeTurnMeta` on a tagged idle or rejected attempt:
/// the endpoint's report
/// of the native turn's launch outcome. It is the endpoint's word, not an
/// observation of the native program.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeTurn {
    /// The provider/v1 launch request id of the native turn.
    pub request_id: String,
    pub custody: NativeCustody,
    /// The reported failure code, if any.
    pub failure_code: Option<String>,
    /// The whole reported value.
    pub report: Value,
}

/// Why `initialize` did not produce a usable v2 peer. None of these is a
/// v2 acceptance, and the client refuses to send prompts afterwards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NegotiationFailure {
    /// The agent answered with a version other than 2. `1` means the agent
    /// speaks only ACP v1; its turn-end response is not v2 consumption.
    UnsupportedVersion {
        agent_version: u16,
    },
    /// The agent advertised no `session` capability.
    NoSessionSurface,
    Rejected {
        code: i64,
        message: String,
    },
    PeerGone,
    ProtocolViolation(String),
}

/// Why a session request failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestFailure {
    NotNegotiated,
    Rejected { code: i64, message: String },
    PeerGone,
    ProtocolViolation(String),
}

/// A supplied identity for one communication; no history is implied.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MessageKey(String);

impl MessageKey {
    /// Returns `None` for an empty key.
    pub fn new(key: impl Into<String>) -> Option<Self> {
        let key = key.into();
        (!key.is_empty()).then_some(Self(key))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Evidence supporting a receiver-side at-most-once label. Neither basis is
/// measured receiver compliance or a guarantee about provider effects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AtMostOnceBasis {
    /// A fresh, unforked identity with exactly one counted attempt.
    SingleAttempt,
    /// Complete history: every attempt advertised our contract in this
    /// session, and the current response echoes the key.
    SessionContract,
}

/// A recorded insertion of an [`OutboundMessage`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Acceptance {
    /// The agent's `messageId` for the inserted user message.
    pub message_id: String,
    /// The agent declared, under the dedup contract, that this response
    /// returns an earlier insertion of the same key.
    pub recovered: bool,
    /// Whether this identity is known to have been inserted at most once.
    /// `false` after [`DeliveryOutcome::DuplicateUnknown`].
    pub at_most_once: bool,
    pub basis: Option<AtMostOnceBasis>,
}

/// One communication and its delivery history.
///
/// The key is fixed at construction, so every retry carries the same
/// identity. The message is owed until an attempt is acknowledged.
pub struct OutboundMessage {
    key: MessageKey,
    text: String,
    unacknowledged_attempts: u32,
    acceptance: Option<Acceptance>,
    complete_history: bool,
    all_attempts_dedup: bool,
    session_id: Option<String>,
}

impl OutboundMessage {
    /// A supplied, recovered or cloned key has unknown history. Current
    /// advertisements cannot recover that missing evidence.
    pub fn new(key: MessageKey, text: impl Into<String>) -> Self {
        Self {
            key,
            text: text.into(),
            unacknowledged_attempts: 0,
            acceptance: None,
            complete_history: false,
            all_attempts_dedup: true,
            session_id: None,
        }
    }

    /// Mint a fresh Linux identity. Random-key uniqueness is the minting
    /// assumption; this crate keeps no durable registry or restart history.
    /// The message cannot be cloned. Calling [`Self::key`] abandons its
    /// complete-history claim; callers/transports must not re-supply keys
    /// captured from the wire, which bypass that downgrade.
    pub fn fresh(text: impl Into<String>) -> std::io::Result<Self> {
        Self::fresh_recorded(text, |_| Ok(()))
    }

    /// [`Self::fresh`], handing the new key once to `record` so that the
    /// origin owner can write it to its own private durable history before
    /// the message exists. Nothing is created if `record` fails.
    ///
    /// The returned message keeps its complete-history claim: `record` is
    /// trusted to store the key only, not to build another live message
    /// from it while this one exists. A message later rebuilt from that
    /// record must use [`Self::new`] and has unknown history; durable
    /// storage does not make a restored key trusted origin history.
    pub fn fresh_recorded<E: From<std::io::Error>>(
        text: impl Into<String>,
        record: impl FnOnce(&MessageKey) -> Result<(), E>,
    ) -> Result<Self, E> {
        let mut bytes = [0u8; 32];
        std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
        let key = MessageKey(bytes.iter().map(|byte| format!("{byte:02x}")).collect());
        record(&key)?;
        let mut message = Self::new(key, text);
        message.complete_history = true;
        Ok(message)
    }

    /// This accessor permits re-supply/forking. Both future labels and any
    /// cached acceptance are conservatively downgraded. Wire capture does
    /// not call this accessor or reconcile fork attempts with this history.
    pub fn key(&mut self) -> &MessageKey {
        self.complete_history = false;
        if let Some(acceptance) = &mut self.acceptance {
            acceptance.at_most_once = false;
            acceptance.basis = None;
        }
        &self.key
    }

    /// No attempt has been acknowledged yet.
    pub fn is_owed(&self) -> bool {
        self.acceptance.is_none()
    }

    /// Attempts without an insertion ACK, including valid rejections. Each
    /// may or may not have been inserted.
    pub fn unacknowledged_attempts(&self) -> u32 {
        self.unacknowledged_attempts
    }

    pub fn acceptance(&self) -> Option<&Acceptance> {
        self.acceptance.as_ref()
    }
}

/// Why an attempt ended without an acknowledgement. The attempt may or may
/// not have been inserted; the message stays owed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NoAckCause {
    /// The peer exited or disconnected before acknowledging.
    PeerGone,
    /// The peer answered with something that is not a valid acknowledgement
    /// of this request.
    InvalidResponse(String),
}

/// What one `session/prompt` attempt established.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryOutcome {
    /// Inserted, and known to be inserted at most once: either this was the
    /// only counted attempt for a fresh unforked identity, or its complete
    /// same-session history is contract-covered. See `Acceptance::basis`.
    Accepted(Acceptance),
    /// An insertion ACK was received, but complete history is unknown or
    /// includes a non-contract attempt. Never at-most-once.
    DuplicateUnknown(Acceptance),
    /// No acknowledgement; still owed.
    NotAcknowledged(NoAckCause),
    /// A valid JSON-RPC error; insertion uncertain, still owed and counted.
    Rejected {
        code: i64,
        message: String,
        /// Untrusted error data, including the endpoint's nativeTurn report.
        /// Absence, explicit null and unknown shapes remain distinct.
        data: Option<Value>,
    },
    /// The client has no negotiated v2 peer, so nothing was sent.
    NotNegotiated,
    /// This communication belongs to a different session; nothing sent.
    SessionMismatch,
}

impl DeliveryOutcome {
    /// A rejected attempt's schema-valid native launch report, if supplied.
    /// This is the endpoint's word; it changes no insertion or retry label.
    /// Raw data stays available even when the report is absent or invalid.
    pub fn native_turn(&self) -> Option<NativeTurn> {
        match self {
            Self::Rejected {
                data: Some(data), ..
            } => NativeTurn::from_report(data.get("nativeTurn")?),
            _ => None,
        }
    }
}

/// A `session/update` the client observed, in arrival order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionEvent {
    UserMessage {
        session_id: String,
        message_id: String,
    },
    Running {
        session_id: String,
    },
    Idle {
        session_id: String,
        stop_reason: Option<String>,
        /// The agent's [`super::TURN_INPUT_META`] tag, if any.
        last_user_message_id: Option<String>,
        /// The agent's schema-valid [`super::NATIVE_TURN_META`] report, if any.
        native_turn: Option<NativeTurn>,
        /// The raw report, including null, invalid or future shapes. Retention
        /// establishes no custody or durability claim.
        native_turn_report: Option<Value>,
    },
    /// `agent_message`: the agent's output, its text blocks joined.
    AgentMessage {
        session_id: String,
        message_id: String,
        text: String,
        /// The agent's [`super::PARENT_MESSAGE_META`] tag, if any.
        parent_message_id: Option<String>,
    },
    /// `notice`, e.g. a turn's error or a refused permission.
    Notice {
        session_id: String,
        severity: String,
        title: String,
        description: Option<String>,
    },
    /// An agent-initiated request (e.g. `session/request_permission`),
    /// answered "method not found": this client grants nothing.
    RequestRefused {
        method: String,
        session_id: Option<String>,
    },
    Other {
        session_id: String,
        kind: String,
        /// The full unvalidated update, including session_info_update's
        /// message-scoped record_error. Not a NativeTurn or completion claim.
        update: Value,
    },
}

/// The next unconsumed idle since the session's first tracked attempt:
/// readiness evidence, without latest-message completion or physical drain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionIdle {
    pub stop_reason: Option<String>,
    /// The agent's [`super::TURN_INPUT_META`] tag, if any.
    pub last_user_message_id: Option<String>,
    /// The agent's schema-valid [`super::NATIVE_TURN_META`] report, if any.
    pub native_turn: Option<NativeTurn>,
    /// Raw metadata of this idle's own tagged turn, retained independently
    /// of schema validation. Never evidence of durable host completion.
    pub native_turn_report: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdleWaitFailure {
    /// No prompt attempt has been tracked for this session; nothing read.
    NoAttempt,
    /// The peer went away before reporting idle. Says nothing about whether
    /// an acknowledged message is still owed (it is not).
    PeerGone,
    ProtocolViolation(String),
}

enum Reply {
    Result(Value),
    Error {
        code: i64,
        message: String,
        data: Option<Value>,
    },
    Gone,
    Violation(String),
}

/// Client side of one ACP v2 connection.
pub struct AcpClient<T> {
    transport: T,
    info: ClientInfo,
    next_id: u64,
    peer: Option<NegotiatedPeer>,
    events: Vec<SessionEvent>,
    /// Independent observation cursors; one session never consumes another.
    idle_cursor: HashMap<String, usize>,
}

impl<T: Transport> AcpClient<T> {
    pub fn new(transport: T, info: ClientInfo) -> Self {
        Self {
            transport,
            info,
            next_id: 0,
            peer: None,
            events: Vec::new(),
            idle_cursor: HashMap::new(),
        }
    }

    pub fn peer(&self) -> Option<&NegotiatedPeer> {
        self.peer.as_ref()
    }

    /// Every session update observed so far, in arrival order.
    pub fn events(&self) -> &[SessionEvent] {
        &self.events
    }

    /// Receive one inbound record, retaining a recognized event in `events`.
    /// `None` means an ignored notification; EOF and malformed/unsolicited
    /// responses are errors. Does not send a request, consume history or move
    /// idle cursors. Agent-initiated requests are refused as usual.
    /// The host chooses when to receive; blocking and deadlines are its
    /// transport's. A later diagnostic may qualify an earlier tagged idle.
    pub fn receive_event(&mut self) -> Result<Option<SessionEvent>, IdleWaitFailure> {
        match self.transport.recv() {
            Incoming::Closed => Err(IdleWaitFailure::PeerGone),
            Incoming::Malformed(line) => Err(IdleWaitFailure::ProtocolViolation(format!(
                "malformed message: {line}"
            ))),
            Incoming::Message(message) if message.get("method").is_some() => {
                let previous = self.events.len();
                self.handle_inbound(&message);
                Ok(self.events.get(previous).cloned())
            }
            Incoming::Message(_) => Err(IdleWaitFailure::ProtocolViolation(
                "unsolicited response".to_owned(),
            )),
        }
    }

    pub fn into_transport(self) -> T {
        self.transport
    }

    /// Offers protocol version 2 and accepts only version 2 back.
    pub fn initialize(&mut self) -> Result<NegotiatedPeer, NegotiationFailure> {
        self.peer = None;
        let mut meta = Map::new();
        meta.insert(
            DEDUP_CONTRACT_META.to_owned(),
            json!({ "version": DEDUP_CONTRACT_VERSION }),
        );
        let request = wire::InitializeRequest {
            protocol_version: PROTOCOL_VERSION,
            info: wire::Implementation {
                name: self.info.name.clone(),
                version: self.info.version.clone(),
            },
            capabilities: Map::new(),
            meta,
        };
        let result = match self.call(method::INITIALIZE, &request) {
            Reply::Result(result) => result,
            Reply::Error { code, message, .. } => {
                return Err(NegotiationFailure::Rejected { code, message });
            }
            Reply::Gone => return Err(NegotiationFailure::PeerGone),
            Reply::Violation(why) => return Err(NegotiationFailure::ProtocolViolation(why)),
        };
        let version: wire::InitializeResponseVersion = parse(&result)
            .map_err(|why| NegotiationFailure::ProtocolViolation(format!("initialize: {why}")))?;
        if version.protocol_version != PROTOCOL_VERSION {
            return Err(NegotiationFailure::UnsupportedVersion {
                agent_version: version.protocol_version,
            });
        }
        if !result.get("info").is_some_and(Value::is_object) {
            return Err(NegotiationFailure::ProtocolViolation(
                "initialize info must be an object".to_owned(),
            ));
        }
        if result
            .get("capabilities")
            .is_some_and(|value| !value.is_object())
        {
            // alpha7 defaults invalid capabilities to {}; positional struct
            // deserialization must never turn them into session support.
            return Err(NegotiationFailure::NoSessionSurface);
        }
        let response: wire::InitializeResponse = parse(&result)
            .map_err(|why| NegotiationFailure::ProtocolViolation(format!("initialize: {why}")))?;
        if response
            .capabilities
            .session
            .as_ref()
            .is_none_or(Value::is_null)
        {
            return Err(NegotiationFailure::NoSessionSurface);
        }
        if !response
            .capabilities
            .session
            .as_ref()
            .is_some_and(Value::is_object)
        {
            return Err(NegotiationFailure::ProtocolViolation(
                "session capability must be an object".to_owned(),
            ));
        }
        let dedup_contract = response
            .meta
            .as_ref()
            .and_then(|meta| meta.get(DEDUP_CONTRACT_META))
            .and_then(|contract| contract.get("version"))
            .and_then(Value::as_u64)
            == Some(DEDUP_CONTRACT_VERSION);
        let live_reattach = response
            .meta
            .as_ref()
            .and_then(|meta| meta.get(LIVE_REATTACH_META))
            .and_then(|contract| contract.get("version"))
            .and_then(Value::as_u64)
            == Some(LIVE_REATTACH_VERSION);
        let resident_session = response
            .meta
            .as_ref()
            .and_then(|meta| meta.get(RESIDENT_SESSION_META))
            .filter(|value| resident_session::validate("ResidentSessionMeta", value).is_ok())
            .map(|value| ResidentDeclaration {
                protocol: value["protocol"].as_str().unwrap_or_default().to_owned(),
                acp_schema: value["acp_schema"].as_str().unwrap_or_default().to_owned(),
            });
        let peer = NegotiatedPeer {
            protocol_version: response.protocol_version,
            agent_name: response.info.name,
            agent_version: response.info.version,
            dedup_contract,
            live_reattach,
            resident_session,
        };
        self.peer = Some(peer.clone());
        Ok(peer)
    }

    /// `session/new`; returns the agent's session id.
    pub fn open_session(&mut self, cwd: &str) -> Result<String, RequestFailure> {
        let request = wire::NewSessionRequest {
            cwd: cwd.to_owned(),
        };
        let result = self.session_call(method::SESSION_NEW, &request)?;
        let response: wire::NewSessionResponse = parse(&result)
            .map_err(|why| RequestFailure::ProtocolViolation(format!("session/new: {why}")))?;
        Ok(response.session_id)
    }

    /// `session/resume` without history replay.
    pub fn resume_session(&mut self, session_id: &str, cwd: &str) -> Result<(), RequestFailure> {
        let request = wire::ResumeSessionRequest {
            session_id: session_id.to_owned(),
            cwd: cwd.to_owned(),
        };
        let result = self.session_call(method::SESSION_RESUME, &request)?;
        if !result.is_object() {
            return Err(RequestFailure::ProtocolViolation(
                "session/resume result must be an object".to_owned(),
            ));
        }
        Ok(())
    }

    /// Sends one `session/prompt` attempt for `message` and returns at its
    /// insertion acknowledgement (or at whatever ended the attempt). It
    /// does not wait for turn completion.
    ///
    /// A message that is already acknowledged is not sent again; its
    /// recorded acceptance is returned.
    pub fn submit(&mut self, session_id: &str, message: &mut OutboundMessage) -> DeliveryOutcome {
        if message
            .session_id
            .as_deref()
            .is_some_and(|id| id != session_id)
        {
            return DeliveryOutcome::SessionMismatch;
        }
        if let Some(acceptance) = &message.acceptance {
            return if acceptance.at_most_once {
                DeliveryOutcome::Accepted(acceptance.clone())
            } else {
                DeliveryOutcome::DuplicateUnknown(acceptance.clone())
            };
        }
        let Some(peer) = self.peer.clone() else {
            return DeliveryOutcome::NotNegotiated;
        };
        let mut meta = Map::new();
        meta.insert(
            MESSAGE_KEY_META.to_owned(),
            Value::String(message.key.as_str().to_owned()),
        );
        let request = wire::PromptRequest {
            session_id: session_id.to_owned(),
            prompt: vec![wire::TextBlock {
                kind: "text",
                text: message.text.clone(),
            }],
            meta,
        };
        // Preserve already-observed readiness even across subsequent attempts.
        self.idle_cursor
            .entry(session_id.to_owned())
            .or_insert(self.events.len());
        message.session_id = Some(session_id.to_owned());
        message.all_attempts_dedup &= peer.dedup_contract;
        let result = match self.call(method::SESSION_PROMPT, &request) {
            Reply::Result(result) => result,
            Reply::Error {
                code,
                message: reason,
                data,
            } => {
                message.unacknowledged_attempts += 1;
                return DeliveryOutcome::Rejected {
                    code,
                    message: reason,
                    data,
                };
            }
            Reply::Gone => {
                message.unacknowledged_attempts += 1;
                return DeliveryOutcome::NotAcknowledged(NoAckCause::PeerGone);
            }
            Reply::Violation(why) => {
                message.unacknowledged_attempts += 1;
                return DeliveryOutcome::NotAcknowledged(NoAckCause::InvalidResponse(why));
            }
        };
        let response = match parse::<wire::PromptResponse>(&result) {
            Ok(response) if !response.message_id.is_empty() => response,
            Ok(_) => {
                message.unacknowledged_attempts += 1;
                return DeliveryOutcome::NotAcknowledged(NoAckCause::InvalidResponse(
                    "session/prompt: empty messageId".to_owned(),
                ));
            }
            Err(why) => {
                message.unacknowledged_attempts += 1;
                return DeliveryOutcome::NotAcknowledged(NoAckCause::InvalidResponse(format!(
                    "session/prompt: {why}"
                )));
            }
        };
        let meta = response.meta.unwrap_or_default();
        // The contract counts only if the agent advertised it and this
        // response echoes our own key.
        let dedup_confirmed = peer.dedup_contract
            && meta.get(MESSAGE_KEY_META).and_then(Value::as_str) == Some(message.key.as_str());
        let recovered =
            dedup_confirmed && meta.get(DUPLICATE_META).and_then(Value::as_bool) == Some(true);
        let basis = if !message.complete_history {
            None
        } else if message.unacknowledged_attempts == 0 {
            Some(AtMostOnceBasis::SingleAttempt)
        } else if message.all_attempts_dedup && dedup_confirmed {
            Some(AtMostOnceBasis::SessionContract)
        } else {
            None
        };
        let at_most_once = basis.is_some();
        let acceptance = Acceptance {
            message_id: response.message_id,
            recovered,
            at_most_once,
            basis,
        };
        message.acceptance = Some(acceptance.clone());
        if at_most_once {
            DeliveryOutcome::Accepted(acceptance)
        } else {
            DeliveryOutcome::DuplicateUnknown(acceptance)
        }
    }

    /// Starts readiness observation for `session_id` without an attempt:
    /// for a session this client resumed on a live process (see the live
    /// reattachment contract), so that a wait between turns can begin
    /// before the first new prompt. Idles from then on are readiness only.
    /// An existing cursor is kept.
    pub fn observe_session(&mut self, session_id: &str) {
        let start = self.events.len();
        self.idle_cursor
            .entry(session_id.to_owned())
            .or_insert(start);
    }

    /// Observe the next unconsumed idle since this session's first tracked
    /// attempt. Later attempts do not reset the cursor; already-observed idle
    /// may precede the latest message. This is readiness, never its completion.
    pub fn await_session_idle(&mut self, session_id: &str) -> Result<SessionIdle, IdleWaitFailure> {
        self.await_session_idle_with_events(session_id, |_| {})
    }

    /// Wait with the same idle cursor and readiness semantics as
    /// [`Self::await_session_idle`], exposing collected events before the next
    /// blocking receive. Called once for the existing event history and after
    /// each inbound method; consumers keep their own reporting cursor.
    /// Observation does not consume events or imply idle or completion.
    pub fn await_session_idle_with_events(
        &mut self,
        session_id: &str,
        mut observe: impl FnMut(&Self),
    ) -> Result<SessionIdle, IdleWaitFailure> {
        let Some(&start) = self.idle_cursor.get(session_id) else {
            return Err(IdleWaitFailure::NoAttempt);
        };
        let mut scanned = start;
        loop {
            observe(self);
            if let Some((next, idle)) =
                self.events[scanned..]
                    .iter()
                    .enumerate()
                    .find_map(|(offset, event)| match event {
                        SessionEvent::Idle {
                            session_id: id,
                            stop_reason,
                            last_user_message_id,
                            native_turn,
                            native_turn_report,
                        } if id == session_id => Some((
                            scanned + offset + 1,
                            SessionIdle {
                                stop_reason: stop_reason.clone(),
                                last_user_message_id: last_user_message_id.clone(),
                                native_turn: native_turn.clone(),
                                native_turn_report: native_turn_report.clone(),
                            },
                        )),
                        _ => None,
                    })
            {
                self.idle_cursor.insert(session_id.to_owned(), next);
                return Ok(idle);
            }
            scanned = self.events.len();
            self.receive_event()?;
        }
    }

    /// Wait for a tagged idle that covers `message_id`: an idle of
    /// `session_id` whose [`super::TURN_INPUT_META`] tag is at or after it in
    /// the endpoint's ascending, fixed-width message-id order (the order a
    /// resident endpoint issues and Runner's harness compares). Every observed
    /// event, earlier ones included, is searched; the readiness cursor of
    /// [`Self::await_session_idle`] is neither used nor moved.
    ///
    /// The tag is the agent's report that its turn ended at or after this
    /// input, not processing, effects or physical drain. Untagged idles are
    /// readiness only and never match. Ids from an agent that does not issue
    /// fixed-width ascending ids are outside this comparison.
    pub fn await_turn_end(
        &mut self,
        session_id: &str,
        message_id: &str,
    ) -> Result<SessionIdle, IdleWaitFailure> {
        let mut scanned = 0;
        loop {
            if let Some(idle) = self.events[scanned..].iter().find_map(|event| match event {
                SessionEvent::Idle {
                    session_id: id,
                    stop_reason,
                    last_user_message_id: Some(last),
                    native_turn,
                    native_turn_report,
                } if id == session_id && message_id <= last.as_str() => Some(SessionIdle {
                    stop_reason: stop_reason.clone(),
                    last_user_message_id: Some(last.clone()),
                    native_turn: native_turn.clone(),
                    native_turn_report: native_turn_report.clone(),
                }),
                _ => None,
            }) {
                return Ok(idle);
            }
            scanned = self.events.len();
            self.receive_event()?;
        }
    }

    fn session_call<P: Serialize>(
        &mut self,
        name: &str,
        params: &P,
    ) -> Result<Value, RequestFailure> {
        if self.peer.is_none() {
            return Err(RequestFailure::NotNegotiated);
        }
        match self.call(name, params) {
            Reply::Result(result) => Ok(result),
            Reply::Error { code, message, .. } => Err(RequestFailure::Rejected { code, message }),
            Reply::Gone => Err(RequestFailure::PeerGone),
            Reply::Violation(why) => Err(RequestFailure::ProtocolViolation(why)),
        }
    }

    /// Sends a request and reads until its response, recording session
    /// updates and refusing agent-initiated requests on the way.
    fn call<P: Serialize>(&mut self, name: &str, params: &P) -> Reply {
        self.next_id += 1;
        let id = self.next_id;
        let request = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": name,
            "params": params,
        });
        if self.transport.send(&request).is_err() {
            return Reply::Gone;
        }
        loop {
            let message = match self.transport.recv() {
                Incoming::Closed => return Reply::Gone,
                Incoming::Malformed(line) => {
                    return Reply::Violation(format!("malformed message: {line}"));
                }
                Incoming::Message(message) => message,
            };
            if message.get("method").is_some() {
                self.handle_inbound(&message);
                continue;
            }
            if message.get("id").and_then(Value::as_u64) != Some(id) {
                return Reply::Violation(format!(
                    "response id {} does not match request id {id}",
                    message.get("id").unwrap_or(&Value::Null)
                ));
            }
            if message.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
                return Reply::Violation("response jsonrpc must be 2.0".to_owned());
            }
            if message.get("result").is_some() == message.get("error").is_some() {
                return Reply::Violation(
                    "response must have exactly one of result/error".to_owned(),
                );
            }
            if let Some(error) = message.get("error") {
                let (Some(code), Some(reason)) = (
                    error.get("code").and_then(Value::as_i64),
                    error.get("message").and_then(Value::as_str),
                ) else {
                    return Reply::Violation("invalid error object".to_owned());
                };
                return Reply::Error {
                    code,
                    message: reason.to_owned(),
                    data: error.get("data").cloned(),
                };
            }
            return match message.get("result") {
                Some(result) => Reply::Result(result.clone()),
                None => Reply::Violation("response has neither result nor error".to_owned()),
            };
        }
    }

    fn handle_inbound(&mut self, message: &Value) {
        let name = message.get("method").and_then(Value::as_str);
        if let Some(id) = message.get("id") {
            // This client implements no client-side methods, and keeps a
            // record that it refused.
            self.events.push(SessionEvent::RequestRefused {
                method: name.unwrap_or_default().to_owned(),
                session_id: message
                    .pointer("/params/sessionId")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            });
            let _ = self.transport.send(&json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": { "code": wire::METHOD_NOT_FOUND, "message": "method not found" },
            }));
            return;
        }
        if name != Some(method::SESSION_UPDATE) {
            return;
        }
        if message.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
            return;
        }
        let Some(Ok(notification)) = message
            .get("params")
            .map(parse::<wire::UpdateSessionNotification>)
        else {
            return;
        };
        if let Some(event) = session_event(notification) {
            self.events.push(event);
        }
    }
}

fn session_event(notification: wire::UpdateSessionNotification) -> Option<SessionEvent> {
    let session_id = notification.session_id;
    let update = notification.update;
    let kind = update.get("sessionUpdate")?.as_str()?.to_owned();
    Some(match kind.as_str() {
        "user_message" => SessionEvent::UserMessage {
            session_id,
            message_id: update.get("messageId")?.as_str()?.to_owned(),
        },
        "state_update" => match update.get("state")?.as_str()? {
            "running" => SessionEvent::Running { session_id },
            "idle" => SessionEvent::Idle {
                session_id,
                stop_reason: update
                    .get("stopReason")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                last_user_message_id: meta_str(&update, super::TURN_INPUT_META),
                native_turn: native_turn(&update),
                native_turn_report: update
                    .get("_meta")
                    .and_then(|meta| meta.get(NATIVE_TURN_META))
                    .cloned(),
            },
            other => SessionEvent::Other {
                session_id,
                kind: format!("state_update:{other}"),
                update,
            },
        },
        "agent_message" => SessionEvent::AgentMessage {
            session_id,
            message_id: update.get("messageId")?.as_str()?.to_owned(),
            text: update
                .get("content")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter(|block| block.get("type").and_then(Value::as_str) == Some("text"))
                .filter_map(|block| block.get("text").and_then(Value::as_str))
                .collect(),
            parent_message_id: meta_str(&update, super::PARENT_MESSAGE_META),
        },
        "notice" => SessionEvent::Notice {
            session_id,
            severity: update.get("severity")?.as_str()?.to_owned(),
            title: update.get("title")?.as_str()?.to_owned(),
            description: update
                .get("description")
                .and_then(Value::as_str)
                .map(str::to_owned),
        },
        _ => SessionEvent::Other {
            session_id,
            kind,
            update,
        },
    })
}

/// The schema-valid `NativeTurnMeta` in the update's own `_meta`; an absent
/// or invalid report claims nothing.
fn native_turn(update: &Value) -> Option<NativeTurn> {
    let report = update.get("_meta")?.get(NATIVE_TURN_META)?;
    NativeTurn::from_report(report)
}

impl NativeTurn {
    /// Validate an endpoint-reported value. This parses only the supported
    /// NativeTurnMeta schema; unknown, null or invalid values claim nothing.
    pub fn from_report(report: &Value) -> Option<Self> {
        resident_session::validate("NativeTurnMeta", report).ok()?;
        let custody = match report["custody"].as_str()? {
            "complete" => NativeCustody::Complete,
            "complete_without_exit" => NativeCustody::CompleteWithoutExit,
            "not_admitted" => NativeCustody::NotAdmitted,
            "reconciled" => NativeCustody::Reconciled,
            "incomplete" => NativeCustody::Incomplete,
            _ => return None,
        };
        Some(Self {
            request_id: report["request_id"].as_str()?.to_owned(),
            custody,
            failure_code: report
                .pointer("/failure/code")
                .and_then(Value::as_str)
                .map(str::to_owned),
            report: report.clone(),
        })
    }
}

/// A non-empty string under `key` in the update's own `_meta`.
fn meta_str(update: &Value, key: &str) -> Option<String> {
    update
        .get("_meta")?
        .get(key)?
        .as_str()
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn parse<D: DeserializeOwned>(value: &Value) -> Result<D, String> {
    // Serde-derived structs also accept positional arrays. ACP's consumed
    // response/notification structs require actual JSON objects on the wire.
    if !value.is_object() {
        return Err("expected an object".to_owned());
    }
    D::deserialize(value).map_err(|error| error.to_string())
}
