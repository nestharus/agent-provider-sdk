//! Start a session and send turns over a prepared resident endpoint.
//!
//! A host that selected `oulipoly.resident_session/v1`, had the provider
//! evaluate its policy ([`crate::resident_session::template_from_policy`])
//! and received a [`ResidentPrepareResult`] uses this module to agree on the
//! endpoint, start the endpoint process from the registered executable it
//! already invoked ([`PreparedEndpoint::argv`]), and then speak ACP v2 to it
//! through an [`AcpClient`] over that process's stdio.
//!
//! What is agreed is declared schema and contracts: the resident protocol,
//! ACP protocol version 2, an ACP schema tag this SDK implements
//! ([`super::SUPPORTED_SCHEMA_TAGS`]), the message-key dedup contract, and
//! the operations a start or turn needs. Only the fixed supported schema is
//! admitted. `initialize` must carry a valid resident declaration and dedup
//! capability; this does not prove process identity or that declared operations
//! are implemented. An undeclared resume is refused before it is sent; a
//! declared but unserved operation can fail when requested. The
//! endpoint's implementation name and version, the configuration digest and
//! any executable identity are attribution, never agreement keys.
//!
//! Identity: a [`NativeSession`] names a provider-native resident session
//! and a [`TurnRef`] one inserted input in it. Neither is a logical session,
//! chain, invocation or root identity. A host's canonical durable reference
//! is the host's own binding ([`StartedSession::bind`]); this module never
//! derives one, so a session the host has not bound stays
//! [`Binding::Unbound`]. Admission, scheduling, ancestry, custody of the
//! endpoint process and any durable record stay with the host.
//!
//! A turn end ([`TurnEnd`]) is the agent's tag that its turn ended at or
//! after the input. It is not completion of effects, physical drain or a
//! pause. This module offers no pause, drain or input hold: holding input is
//! a host admission decision and does not stop a running turn.

use std::collections::BTreeSet;

use super::client::{
    AcpClient, DeliveryOutcome, IdleWaitFailure, NativeTurn, NegotiatedPeer, NegotiationFailure,
    OutboundMessage, RequestFailure,
};
use super::transport::Transport;
use super::wire::method;
use super::{DEDUP_CONTRACT_VERSION, PROTOCOL_VERSION, SUPPORTED_SCHEMA_TAGS};
use crate::resident_session::{self, ResidentPrepareResult};

/// Operations every resident start and turn needs.
pub const REQUIRED_OPERATIONS: &[&str] = &[
    method::INITIALIZE,
    method::SESSION_NEW,
    method::SESSION_PROMPT,
];

/// Why a resident start, or the agreement before it, was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartRefusal {
    /// The prepared result is not a valid resident-session/v1 result.
    InvalidPrepared(String),
    /// No ACP protocol version or schema tag in common with this SDK.
    NoCommonAcp {
        protocol_version: u64,
        schema: String,
    },
    /// The endpoint does not serve an operation this start or turn needs.
    OperationNotServed(&'static str),
    /// `initialize` did not produce a usable v2 peer.
    Negotiation(NegotiationFailure),
    /// The endpoint's `initialize` contradicts what `resident.prepare`
    /// declared. Nothing was sent after `initialize`.
    DeclarationContradicted(String),
    /// `session/new` or `session/resume` failed.
    Request(RequestFailure),
}

/// A resident endpoint the host may start, as `resident.prepare` declared it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedEndpoint {
    /// The resident contract declared, `oulipoly.resident_session/v1`.
    pub protocol: String,
    /// The ACP schema tag declared and agreed.
    pub acp_schema: String,
    /// The declared configuration digest: provider attribution, not a key.
    pub config_sha256: String,
    /// Operations the endpoint declared it serves.
    pub operations: BTreeSet<String>,
    args: Vec<String>,
}

impl PreparedEndpoint {
    /// Agree on a `resident.prepare` result: valid v1 shape, ACP protocol
    /// version 2 on stdio, a schema tag this SDK implements, the dedup
    /// contract this client relies on, and [`REQUIRED_OPERATIONS`].
    pub fn agree(result: &ResidentPrepareResult) -> Result<Self, StartRefusal> {
        // Agreement first: a schema this SDK does not implement is no common
        // ACP, not merely an invalid v1 value.
        if result.acp.protocol_version != u64::from(PROTOCOL_VERSION)
            || !SUPPORTED_SCHEMA_TAGS.contains(&result.acp.schema.as_str())
        {
            return Err(StartRefusal::NoCommonAcp {
                protocol_version: result.acp.protocol_version,
                schema: result.acp.schema.clone(),
            });
        }
        let value = serde_json::to_value(result)
            .map_err(|error| StartRefusal::InvalidPrepared(error.to_string()))?;
        resident_session::validate("ResidentPrepareResult", &value)
            .map_err(|error| StartRefusal::InvalidPrepared(error.to_string()))?;
        if result.protocol != resident_session::PROTOCOL || result.invocation.endpoint != "stdio" {
            return Err(StartRefusal::InvalidPrepared(format!(
                "{} endpoint {:?} is not a stdio {}",
                result.protocol,
                result.invocation.endpoint,
                resident_session::PROTOCOL
            )));
        }
        if result.acp.dedup_contract != DEDUP_CONTRACT_VERSION {
            return Err(StartRefusal::InvalidPrepared(format!(
                "dedup contract {} is not version {DEDUP_CONTRACT_VERSION}",
                result.acp.dedup_contract
            )));
        }
        let operations: BTreeSet<String> = result.operations.iter().cloned().collect();
        if let Some(missing) = REQUIRED_OPERATIONS
            .iter()
            .find(|operation| !operations.contains(**operation))
        {
            return Err(StartRefusal::OperationNotServed(missing));
        }
        Ok(Self {
            protocol: result.protocol.clone(),
            acp_schema: result.acp.schema.clone(),
            config_sha256: result.config_sha256.clone(),
            operations,
            args: result.invocation.args.clone(),
        })
    }

    /// The endpoint's argv: `executable`, the same registered provider
    /// executable the host invoked for `resident.prepare`, followed by the
    /// declared arguments. Starting, placing and keeping custody of that
    /// process is the host's.
    pub fn argv(&self, executable: &str) -> Vec<String> {
        let mut argv = Vec::with_capacity(self.args.len() + 1);
        argv.push(executable.to_owned());
        argv.extend(self.args.iter().cloned());
        argv
    }

    pub fn serves(&self, operation: &str) -> bool {
        self.operations.contains(operation)
    }

    /// Check presence/validity of the fixed supported resident declaration
    /// and dedup capability. This establishes no endpoint-process identity;
    /// the host owns the association between prepare, argv and transport.
    /// Declared operations are not proven served by this check.
    pub fn check_peer(&self, peer: &NegotiatedPeer) -> Result<(), StartRefusal> {
        match &peer.resident_session {
            None => Err(StartRefusal::DeclarationContradicted(
                "initialize declares no valid resident session".to_owned(),
            )),
            Some(declared)
                if declared.protocol != self.protocol || declared.acp_schema != self.acp_schema =>
            {
                Err(StartRefusal::DeclarationContradicted(format!(
                    "initialize declares {} {}, prepared {} {}",
                    declared.protocol, declared.acp_schema, self.protocol, self.acp_schema
                )))
            }
            Some(_) if !peer.dedup_contract => Err(StartRefusal::DeclarationContradicted(
                "initialize does not advertise the prepared dedup contract".to_owned(),
            )),
            Some(_) => Ok(()),
        }
    }
}

/// How to start a session on a resident endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionStart {
    /// `session/new` in the absolute working directory `cwd`.
    New { cwd: String },
    /// `session/resume` of a resident session id the host kept, in its
    /// original `cwd`. The endpoint decides whether it holds that session.
    Resume { session_id: String, cwd: String },
}

/// A provider-native resident session, attributed to the endpoint that
/// answered it. Not a logical session or durable host reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeSession {
    /// The id answered by new, or supplied by the host to a successful resume.
    pub session_id: String,
    /// The endpoint's self-reported implementation (attribution only).
    pub agent_name: String,
    pub agent_version: String,
    /// The resident contract it was served under.
    pub protocol: String,
    /// The prepared configuration digest it was started from (attribution).
    pub config_sha256: String,
}

/// The host's binding of a native session to its own canonical reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Binding {
    /// No host reference is bound: missing, not fabricated.
    Unbound,
    /// The host-owned reference the host bound, as the host supplied it.
    Bound(String),
}

/// A started resident session on one connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartedSession {
    pub native: NativeSession,
    /// `true` when started by `session/resume`.
    pub resumed: bool,
    binding: Binding,
}

impl StartedSession {
    /// The host's binding; [`Binding::Unbound`] until the host binds one.
    pub fn binding(&self) -> &Binding {
        &self.binding
    }

    /// Records the host's own canonical reference for this native session.
    /// This module neither checks nor persists it; truth of the binding is
    /// the host's.
    pub fn bind(&mut self, host_reference: impl Into<String>) {
        self.binding = Binding::Bound(host_reference.into());
    }
}

/// Starts a session: negotiates the connection if it is not yet negotiated,
/// checks the endpoint against `endpoint`, then sends `session/new` or
/// `session/resume`. A resume the endpoint did not declare is refused before
/// it is sent.
pub fn start_session<T: Transport>(
    client: &mut AcpClient<T>,
    endpoint: &PreparedEndpoint,
    start: SessionStart,
) -> Result<StartedSession, StartRefusal> {
    if matches!(start, SessionStart::Resume { .. }) && !endpoint.serves(method::SESSION_RESUME) {
        return Err(StartRefusal::OperationNotServed(method::SESSION_RESUME));
    }
    let peer = match client.peer() {
        Some(peer) => peer.clone(),
        None => client.initialize().map_err(StartRefusal::Negotiation)?,
    };
    endpoint.check_peer(&peer)?;
    let (session_id, resumed) = match start {
        SessionStart::New { cwd } => (
            client.open_session(&cwd).map_err(StartRefusal::Request)?,
            false,
        ),
        SessionStart::Resume { session_id, cwd } => {
            client
                .resume_session(&session_id, &cwd)
                .map_err(StartRefusal::Request)?;
            (session_id, true)
        }
    };
    Ok(StartedSession {
        native: NativeSession {
            session_id,
            agent_name: peer.agent_name,
            agent_version: peer.agent_version,
            protocol: endpoint.protocol.clone(),
            config_sha256: endpoint.config_sha256.clone(),
        },
        resumed,
        binding: Binding::Unbound,
    })
}

/// One inserted input of a native session: the endpoint's `messageId`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnRef {
    pub session_id: String,
    pub message_id: String,
}

/// What one turn attempt established.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnDelivery {
    /// The insertion outcome with its at-most-once labels.
    pub outcome: DeliveryOutcome,
    /// The inserted input, when an insertion was acknowledged.
    pub turn: Option<TurnRef>,
}

/// Sends one turn attempt: a `session/prompt` of `message` in `session`,
/// returning at its insertion acknowledgement (or whatever ended the
/// attempt). It does not wait for the turn to end.
pub fn send_turn<T: Transport>(
    client: &mut AcpClient<T>,
    session: &StartedSession,
    message: &mut OutboundMessage,
) -> TurnDelivery {
    let outcome = client.submit(&session.native.session_id, message);
    let turn = match &outcome {
        DeliveryOutcome::Accepted(acceptance) | DeliveryOutcome::DuplicateUnknown(acceptance) => {
            Some(TurnRef {
                session_id: session.native.session_id.clone(),
                message_id: acceptance.message_id.clone(),
            })
        }
        _ => None,
    };
    TurnDelivery { outcome, turn }
}

/// The agent's tagged turn end covering an input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnEnd {
    pub turn: TurnRef,
    /// The tag of the idle that covered it: this input or a later one.
    pub covered_by: String,
    /// The stop reason and native report belong to `covered_by`'s turn.
    pub stop_reason: Option<String>,
    pub native_turn: Option<NativeTurn>,
    /// Raw metadata belongs to `covered_by`, even when invalid or unknown.
    /// Neither this report nor the idle certifies endpoint input persistence
    /// or host canonical publication; later events may report contrary facts.
    pub native_turn_report: Option<serde_json::Value>,
}

impl TurnEnd {
    /// The covering idle was tagged with this very input.
    pub fn is_own(&self) -> bool {
        self.covered_by == self.turn.message_id
    }
}

/// Waits for the agent's tagged idle covering `turn`
/// ([`AcpClient::await_turn_end`]).
pub fn await_turn_end<T: Transport>(
    client: &mut AcpClient<T>,
    turn: &TurnRef,
) -> Result<TurnEnd, IdleWaitFailure> {
    let idle = client.await_turn_end(&turn.session_id, &turn.message_id)?;
    Ok(TurnEnd {
        turn: turn.clone(),
        covered_by: idle.last_user_message_id.unwrap_or_default(),
        stop_reason: idle.stop_reason,
        native_turn: idle.native_turn,
        native_turn_report: idle.native_turn_report,
    })
}
