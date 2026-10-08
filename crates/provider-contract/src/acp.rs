//! ACP v2 **draft** vocabulary and client core shared by hosts and providers.
//!
//! This is the one SDK home of the Agent Client Protocol (ACP) v2 draft
//! subset that Oulipoly hosts and resident provider endpoints agree on: the
//! pinned schema, method names, the namespaced `_meta` contracts and the
//! endpoint error codes ([this module](self)); and the client side of one
//! connection ([`AcpClient`]), which delivers a communication to an agent and
//! learns, with honest labels, what happened to it. The agent side served by
//! provider adapters is `agent_provider_execution::resident`, which uses this
//! same vocabulary. [`resident`] starts sessions and sends turns over a
//! prepared `oulipoly.resident_session/v1` endpoint.
//!
//! ACP v2 itself is the session protocol; nothing here is a second general
//! host/provider protocol. The `_meta` contracts below are the additional
//! meaning ACP has no counterpart for. Compatibility is the negotiated
//! protocol version, declared schema tag and declared contracts, never a
//! native binary, package or source identity.
//!
//! The client core was moved from Agent Runner's `oulipoly-acp` crate
//! (Runner `2f6ec679f9e975d23d16784898cd21cb55197e8f`) with its semantics and
//! deterministic tests unchanged; see the crate `README.md`.
//!
//! # Pinned schema
//!
//! This implements a consumed subset of the published draft schema pinned in
//! [`pin`]. ACP v2 is a Draft; later `alpha` releases may change any shape
//! used here. The client has been exercised only against deterministic
//! peers: the reference peer in this crate's tests and the SDK's own resident
//! endpoint over stand-in native turns. No real harness was run.
//!
//! # What an outcome means
//!
//! * **Insertion acknowledgement.** A v2 `session/prompt` response carries a
//!   `messageId`. Per the pinned schema it means the agent inserted the user
//!   message into its ACP conversation. It is **not** turn completion,
//!   **not** physical drain of any queue, and **not** dedup.
//! * **Session readiness.** The next unconsumed idle `state_update` since the
//!   session's **first tracked attempt**, separately with
//!   [`AcpClient::await_session_idle`]. Later attempts do not reset this history;
//!   idle may precede the latest message. The draft provides no completion
//!   correlation with that message.
//!   Another foreground task may have caused idle; this is not physical drain.
//!   An agent may tag an idle with [`TURN_INPUT_META`] and its messages with
//!   [`PARENT_MESSAGE_META`]; the client only reports those tags.
//! * **Message identity.** [`OutboundMessage::fresh`] mints a Linux random
//!   identity for one communication and carries it in the prompt's `_meta` under
//!   [`MESSAGE_KEY_META`]. Retries reuse it; [`OutboundMessage`] has no way
//!   to change it.
//!
//! # The local dedup contract
//!
//! The schema's `messageId` is an acknowledgement, not an idempotency key: a
//! retry may be accepted as a distinct submission. Dedup is therefore a
//! receiver-side contract defined here, carried entirely in sanctioned
//! namespaced `_meta` keys:
//!
//! 1. A complying agent advertises [`DEDUP_CONTRACT_META`] with
//!    `{"version": 1}` in its `initialize` response `_meta`.
//! 2. On a `session/prompt` whose `_meta` carries a message key it has already
//!    inserted **in that session**, it inserts nothing and returns the original
//!    `messageId`. This promise lasts for the entire resumable session lifetime.
//!    A receiver unable to retain that memory across restart/resume must not
//!    advertise the contract for that session.
//! 3. Every prompt response echoes the key under [`MESSAGE_KEY_META`] and
//!    sets [`DUPLICATE_META`] to `true` when it returned an earlier
//!    insertion.
//!
//! [`Acceptance::basis`] identifies either a single counted attempt for a
//! fresh unforked identity, or a complete same-session history in which every
//! attempt advertised the contract and the current answer echoes the key.
//! Supplied/recreated/recovered/cloned keys have unknown history. Calling
//! [`OutboundMessage::key`] permits forks and abandons its complete-history
//! claim. Capturing a key from outgoing wire bytes or a custom transport bypasses
//! that downgrade; forks made from it are outside the original tracked history.
//! These labels therefore trust the caller/transport not to re-supply captured
//! keys. A current advertisement cannot repair earlier non-contract or unknown
//! attempts.
//! An insertion ACK without either basis is [`DeliveryOutcome::DuplicateUnknown`].
//! A valid error stays distinct but counts as an insertion-uncertain attempt.
//!
//! These are contractual evidence labels, not measured receiver compliance.
//! An advertisement says nothing about another receiver, session or store;
//! same-session resume relies on the receiver's lifetime promise, not a
//! client-invented continuity proof. No arbitrary provider-effects claim or
//! durable history is supplied: [`OutboundMessage::fresh_recorded`] only
//! lets an origin owner store a fresh key, and a message rebuilt from that
//! store is a supplied key with unknown history. Messages are bound to
//! their first session-id string, which is trusted contract scope, not proof
//! of receiver/store identity or cross-receiver continuity.
//!
//! Empty `messageId` is conservatively refused (stricter than the schema).
//! Unknown optional schema fields are ignored, not claimed fully validated.
//! Consumed response results, notification params and mandatory nested
//! initialize info require wire objects. Invalid capability shapes cannot
//! advertise session support. Session updates require `jsonrpc: "2.0"` and
//! object params/update with the consumed discriminators and fields; malformed
//! notifications are ignored without retaining events or readiness. Ignored
//! optional fields and unconsumed update variants are not fully validated.
//!
//! Transport limits carried from the moved primitive: synchronous blocking
//! reads, no deadline or line-size bound, read/UTF-8 errors reported as closed,
//! and an unbounded event Vec. Readiness evidence is retained per session.
//! A host that needs bounds supplies its own [`Transport`].
//!
//! # The local live reattachment contract
//!
//! Whether a live agent process can converse with a later client, after
//! the client it negotiated with went away while the process (and its
//! connection, such as retained stdio) lived on, is the agent's semantic,
//! never inferred by a client from a retained descriptor or a stored
//! session string. An agent declares it, per connection, with
//! [`LIVE_REATTACH_META`] `{"version": 1}` in its `initialize` response
//! `_meta`. The declaration promises, for the life of this process:
//!
//! 1. A later client on the same live process (the same retained stream,
//!    or a new connection to the same listener) may send `initialize`
//!    again; the agent answers it as the start of that client's use and
//!    keeps every session it holds.
//! 2. `session/resume` of a session this process holds continues that same
//!    in-process conversation: nothing is replayed to the agent or the
//!    client, and no earlier insertion is redone.
//! 3. Message ids keep ascending across clients, so idle tags stay
//!    comparable.
//!
//! Absent or another version: a later client must not converse with the
//! live process. The declaration is the agent's word, not measured
//! continuity; it says nothing about another process, a relaunch or a
//! stored session string resumed elsewhere.
//!
//! # Not implemented
//!
//! * ACP v1 is not accepted as v2 consumption. A peer that negotiates v1 gets
//!   [`NegotiationFailure::UnsupportedVersion`], and no prompt is sent.
//! * No v1 fallback, no busy-queue semantics (the draft leaves them
//!   unspecified), no authentication, no client-side tool methods, no
//!   harness spawning, no physical pause or drain.

mod client;
pub mod resident;
mod transport;
pub mod wire;

pub use client::{
    Acceptance, AcpClient, AtMostOnceBasis, ClientInfo, DeliveryOutcome, IdleWaitFailure,
    MessageKey, NativeCustody, NativeTurn, NegotiatedPeer, NegotiationFailure, NoAckCause,
    OutboundMessage, RequestFailure, ResidentDeclaration, SessionEvent, SessionIdle,
};
pub use transport::{Incoming, LineTransport, PeerClosed, Transport};

/// Provenance of the ACP v2 draft schema subset implemented here. These
/// identify the schema text, not a runtime compatibility key: an endpoint
/// declares [`pin::SCHEMA_TAG`] and the host agrees on it.
pub mod pin {
    /// Release tag in `agentclientprotocol/agent-client-protocol`.
    pub const SCHEMA_TAG: &str = "schema-v2.0.0-alpha.7";
    /// Commit that the tag points to.
    pub const SCHEMA_COMMIT: &str = "1761180eeddf0828d4ecc367106a632c61be06d9";
    /// The schema file at that commit.
    pub const SCHEMA_URL: &str = "https://github.com/agentclientprotocol/agent-client-protocol/blob/1761180eeddf0828d4ecc367106a632c61be06d9/schema/v2/schema.json";
    /// Git blob id of `schema/v2/schema.json` at [`SCHEMA_COMMIT`].
    pub const SCHEMA_BLOB: &str = "bc085103d0c2c5daf856447ed1b10bd8944250a2";
}

/// ACP draft schema tags whose consumed subset this module implements.
pub const SUPPORTED_SCHEMA_TAGS: &[&str] = &[pin::SCHEMA_TAG];

/// The only protocol version this client accepts and resident endpoints serve.
pub const PROTOCOL_VERSION: u16 = 2;

/// `_meta` key carrying the sender's stable message key on a prompt request,
/// and its echo on a complying prompt response.
pub const MESSAGE_KEY_META: &str = "oulipoly.ai/messageKey";

/// `_meta` key a complying agent uses in its `initialize` response to
/// advertise the dedup contract. The client sends it in its `initialize`
/// request to say that its prompts carry message keys.
pub const DEDUP_CONTRACT_META: &str = "oulipoly.ai/messageKeyDedup";

/// `_meta` key on a prompt response: `true` when the agent returned an
/// earlier insertion of the same message key instead of inserting again.
pub const DUPLICATE_META: &str = "oulipoly.ai/duplicate";

/// `_meta` key on an `agent_message` update: the inserted user message
/// (an earlier prompt response's `messageId`) this agent message answers,
/// as the agent itself recorded it. Absent: no attribution is claimed.
pub const PARENT_MESSAGE_META: &str = "oulipoly.ai/parentMessageId";

/// `_meta` key on an idle `state_update`: the latest inserted user message
/// the agent had processed when it went idle. With ascending message ids it
/// covers that message and every earlier one in the session. Absent: the
/// idle is readiness only, never any message's turn end.
pub const TURN_INPUT_META: &str = "oulipoly.ai/lastUserMessageId";

/// Version of the dedup contract described in the module documentation.
pub const DEDUP_CONTRACT_VERSION: u64 = 1;

/// `_meta` key a complying agent uses in its `initialize` response to
/// declare the live reattachment contract (see the module documentation).
pub const LIVE_REATTACH_META: &str = "oulipoly.ai/liveReattach";

/// Version of the live reattachment contract.
pub const LIVE_REATTACH_VERSION: u64 = 1;

/// `_meta` key on a resident endpoint's `initialize` response: the
/// `ResidentSessionMeta` of `oulipoly.resident_session/v1`, naming the
/// resident contract and ACP schema tag it serves.
pub const RESIDENT_SESSION_META: &str = "oulipoly.ai/residentSession";

/// `_meta` key on a resident endpoint's tagged idle: the `NativeTurnMeta`
/// of the native turn's provider/v1 launch outcome.
pub const NATIVE_TURN_META: &str = "oulipoly.ai/nativeTurn";

/// JSON-RPC and resident endpoint error codes.
pub mod code {
    pub const PARSE_ERROR: i64 = -32700;
    pub const METHOD_NOT_FOUND: i64 = -32601;
    pub const INVALID_PARAMS: i64 = -32602;
    /// A request other than `initialize` arrived before it.
    pub const NOT_INITIALIZED: i64 = -32002;
    /// The endpoint declares that the input was not inserted.
    pub const INPUT_NOT_INSERTED: i64 = -32010;
    /// The endpoint cannot tell whether the input was inserted.
    pub const INPUT_UNCERTAIN: i64 = -32011;
    /// The session is unknown, closed, held elsewhere or not settled.
    pub const SESSION_UNAVAILABLE: i64 = -32012;
}
