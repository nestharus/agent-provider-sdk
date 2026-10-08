# Agent Provider SDK

Shared infrastructure for external binaries that implement the
`oulipoly.provider/v1` Agent Runner contract.

The SDK is intended to centralize the parts every terminal provider needs:

- versioned request/response and launch-event types;
- expressive, versioned Session and Agent DSLs;
- bounded live-stream and infrastructure-evidence contracts;
- JSON/NDJSON process transport;
- subprocess containment, custody, cancellation, and durable state helpers;
- a provider-profile DSL with capability negotiation and validation;
- central system-prompt and tool-bridge policy, including the Agent Bash bridge;
- conformance fixtures and a reproducible memory benchmark harness.

Terminal-specific behavior remains in provider repositories. OpenCode, Pi,
Codex, and Claude Code adapters continue to own their native command lines,
authentication, account/config roots, quota APIs, and session formats.

Agent Runner remains the authority for logical agent/session ancestry,
scheduling, runtime request/session admission, pause/resume policy, mailbox
delivery, and incident classification. Agent Bash remains the generic
detached-process supervisor and output capture owner. The SDK supplies shared
contracts and conformance tests; it does not create a second runtime registry or
session database.

## Provider contract

The `agent-provider-contract` crate pins the current
`oulipoly.provider/v1` request, response, and launch-event contract. It exposes:

- a checked-in Rust DTO projection of the pinned schemas;
- the complete JSON Schema registry and validation helpers;
- strict launch-NDJSON correlation, ordering, base64, and finality validation;
- deterministic conformance fixtures through the `contract-test-fixtures`
  feature; and
- source identities and SHA-256 provenance in
  `crates/provider-contract/contract/v1/UPSTREAM.md`.

Public DTOs preserve schema-defined names and bounded fields. When the pinned
schema deliberately leaves an operation's whole parameter object open, the SDK
still exposes a distinct operation-specific parameter type and contains the open
content in `extension_fields`; the fixture vocabulary is not promoted into a
private constraint. `LaunchEvent` is the canonical typed launch-event sum and
its variants wrap the five schema-specific event DTOs. `src/generated.rs` is
maintained with the complete snapshot; the JSON Schemas remain the wire authority.
The imported launch schema's intrinsic phrase "request, event, and result
schemas" names the terminal `exit` event as the launch result; launch has no
separate result or response envelope.

Downstream hosts and providers should consume the released crate or copy the
complete versioned snapshot. Private schema edits are unsupported. This contract
does not own provider execution or Agent Runner's logical session state.

The `oulipoly.provider/v1` compatibility promise governs wire behavior, not the
Rust source API independently exposed by the crate. Rust consumers also select a
crate package version: source-compatible evolution follows Cargo's semantic
versioning rules, and a source-breaking DTO or operation-typing change requires
an appropriate package-version change even when its admitted wire JSON remains
compatible with provider/v1. Before `1.0.0`, a minor package-version change may
break the Rust API. Consumers that copy only the schema snapshot receive the wire
contract, not a Rust source-compatibility promise.

The workspace package version is **0.3.0**, signalling the source-breaking
live-stream v3 API: exhaustive matches on `live_stream::Channel` and
`DataChannel` must handle `Combined`, and users of the `contract-test-fixtures`
feature must use `LIVE_STREAM_ATTACHMENT_V3_JSON` and `attachment-v3.json` in
place of the removed v2 attachment fixture constant and path. The existing
live-stream API names now select and admit v3 only. Consumers must adapt their
source when rebuilding; no compatibility alias or v2 live fallback is supplied.
All workspace crates inherit this release version. This package signal is
separate from wire-schema selection and is never a runtime compatibility gate.

Provider responses can carry typed host-state proposals required by the pinned
wire snapshot. Those values are untrusted proposals, not executable commands:
Agent Runner validates authority and state preconditions, then translates an
accepted proposal into its private mutation protocol. Providers do not apply
host state, and schema or DTO admission alone never authorizes mutation.

Runtime compatibility must follow declared supported wire schemas and capability
agreement. Source/snapshot provenance, SDK package versions, native CLI banners,
and executable byte identity are not equality requirements for provider
compatibility. Consume SDK source without an explicit manifest source-revision
constraint; normal Cargo lockfile commits record resolved builds, not runtime
pins. Private schema edits remain unsupported; wire-semantic changes need an
explicit supported contract or extension.
Before rollout, the route owner must retain the previous working route through
replacement verification and the rollback decision. If that prerequisite cannot
be met, continue on the previous route. Rollback restores that working route.

Compatible provider rebuilds/updates must remain usable automatically without
manual restart, preserving compatible durable custody and replay state. Actor
identity checks and recovery bounds still protect process custody. Current
Runner integration uses fixed-v1 validation and capability agreement. The
`negotiation` module implements common-version selection (below); Runner's
host-side adoption and automatic refreshed agreement after replacement remain
unfinished. Do not advertise a second contract wire version before host
common-version selection exists. The shared one-shot launch lifecycle below keeps executable
and source identity out of request keys; cross-build replay/update
qualification across a full rebuild matrix remains unfinished.

The crate and its complete schema snapshot may be used and redistributed under
the MIT License, which is included in `crates/provider-contract` and in the
published crate. The imported schemas' source MIT grant and preserved notice are
recorded in `crates/provider-contract/contract/v1/UPSTREAM.md`.

The session runtime and infrastructure control-plane direction is documented in
[`docs/architecture/session-runtime-control-plane.md`](docs/architecture/session-runtime-control-plane.md).
Its implementation is tracked by APV-28 through APV-36 in the existing Agent
Provider SDK and Provider Runtime and Memory projects. Live output is deliberately
active-only and bounded; completed turns remain canonical in normal session
storage.

### Snapshot alignment, version selection and resident sessions

The v1 snapshot now carries Agent Runner's current host-selected v1 extensions
(`prompt_acceptance_v1`, `launch_output_v1`, `session_turn_pages_v1` and their
launch, marker and `session.read_turns` shapes, and the
`provider_storage_contention`/`provider_unavailable` terminal kinds), imported
from Runner commit `5d025b82` byte-for-byte, plus the host-selected
`capabilities.resident_session_v1` and advertisement-only tolerance for future
contract versions and capability keys. `UPSTREAM.md` records the
exact provenance. The DTO projection follows: `DescribeCapabilities` gains four
optional capability fields and an `additional` map of unknown advertisements, `LaunchParams` gains `prompt_acceptance` and
`output_delivery`, `TerminalSignalKind` gains two variants, and
`session.read_turns` uses the bounded page DTOs. These are Rust source-API
changes (struct literals and exhaustive matches must adjust), so that alignment
introduced workspace package version 0.2.0. This is semantic v1 snapshot
realignment to the host snapshot under fresh migration, not compatible evolution of the former SDK
`session.read_turns` fixtures. The Rust version does not version wire semantics;
other hosts remain unqualified.

`negotiation` selects common supported versions without identity equality:
`select_contract_version` uses the provider's preferred `oulipoly.provider/vN`
when the host supports it, otherwise the highest common one, and ignores
versions the host does not know. For host-selected extensions, a host offers
each supported version with its own `host.env` selector
(`<PREFIX>_V<n>=1`), a provider advertises `<capability>_v<n>: true` only for
offered versions it supports, and the host uses the highest offered version the
provider advertised; no common version is an explicit refusal. The describe
schema tolerates future advertisements while known capability types and the
selected v1 payload/envelope remain strict. `DescribeCapabilities.additional`
retains unknown keys through typed admission. `SchemaRegistry` and the chooser
require the preference to belong to the declared version list; schema-only
consumers must apply the chooser for this cross-field invariant. Providers still
advertise extension versions only when the request offered them. Golden cases live in `tests/fixtures/negotiation/` (also exported
through the `contract-test-fixtures` feature).

The [`resident-session/v1` extension](crates/provider-contract/contract/extensions/resident-session/README.md)
(`resident_session` module) is selected by
`host.env.OULIPOLY_HOST_RESIDENT_SESSION_V1=1` and advertised as
`capabilities.resident_session_v1`. Its `resident.prepare` subcommand admits a
policy-evaluated launch template and answers the arguments the host appends to
the same registered provider executable to start a resident ACP v2 endpoint on
stdio, the ACP subset served, and the operations offered. The endpoint is
`agent_provider_execution::resident` (below).

The `acp` module is the SDK's one home of the agreed ACP v2 draft subset
(`schema-v2.0.0-alpha.7`): method names, the namespaced `_meta` contracts
(message-key dedup, parent/turn-input tags, live reattachment, resident session
and native turn reports) and the resident endpoint error codes, which the
resident endpoint now takes from it. It also carries the host side:
`acp::AcpClient`, the ACP v2 client core moved unchanged in meaning from Agent
Runner's `oulipoly-acp` crate (Runner `2f6ec679`; its deterministic tests moved
with it), plus `acp::resident` to start sessions and send turns over a prepared
resident endpoint. `resident_session::template_from_policy` turns an accepted
`policy.evaluate` result into `resident.prepare` params and refuses a refused
policy, a missing argv, or a stdin/prompt transform a resident template cannot
carry. Exact echoes of the preparation prompt (`model.inputs.prompt`, falling
back to `launch.prompt`) are accepted because resident turns supply their own
prompt; differing values or values without that input basis are refused.
`acp::resident::PreparedEndpoint::agree` validates the declared fixed supported
schema and contracts (ACP version 2, `schema-v2.0.0-alpha.7`, the dedup contract,
the operations a start or turn needs). `check_peer` requires a valid resident
schema declaration and dedup capability at `initialize`; it proves neither
endpoint-process identity nor implementation of declared operations. An
undeclared resume is refused before it is sent; declared operations can fail
when requested.
Native resident session ids and message ids come back attributed to the
endpoint. Rejected prompt attempts retain untrusted error `data`, including
`nativeTurn`; `DeliveryOutcome::native_turn` validates a supplied report without
changing insertion/retry labels. Idles and `TurnEnd` retain raw
`native_turn_report` alongside the validated `native_turn`, distinguishing
absent, null and unsupported reports. Other updates retain their full payload,
including `session_info_update` record-write failures and the affected message
id. `AcpClient::receive_event` lets a host read one subsequent record without
sending a request or moving idle cursors; the host chooses event ordering and
transport deadlines. An idle can precede a final input-record write failure:
no await result certifies endpoint input durability, host canonical publication
or another input's custody. Later events can qualify an earlier report.
A host's canonical durable reference exists only when the host binds
one (`Binding::Unbound` otherwise). Turn ends are the agent's tags, not effect
completion, drain or pause; no pause, drain or input hold is offered. Admission,
scheduling, ancestry, endpoint process custody and durable delivery records stay
with the host. Exercised only against scripted peers, source-derived adapter evaluation
shapes and the SDK endpoint over stand-in native turns; actual adapter
policy/prepare execution and host adoption remain unfinished. Added outcome
fields require exhaustive Rust patterns/literals to adapt when rebuilding;
wire schemas and package version remain unchanged in this unreleased 0.3.0
source contribution.

The [`tool-mediation/v1` extension](crates/provider-contract/contract/extensions/tool-mediation/README.md)
(`tool_mediation` module) is selected by
`host.env.OULIPOLY_HOST_TOOL_MEDIATION_V1=1` and advertised as
`capabilities.tool_mediation_v1`. The host supplies its Bash policy
(`bash_allow` as `{"allow": [...]}` or `{"authority": "trusted-task"}`), its
Bash requester and the name of its Bash ingress variable as one JSON object in
the launch environment variable `OULIPOLY_TOOL_MEDIATION_V1`, so it reaches
launches and resident templates through `policy.evaluate` `launch.env` without
a new prepare field. A provider honours it with
`agent_provider_execution::tool_bridge` as its native agent's only command
tool, or refuses; it never ignores it.

The [`exploration/v1` extension](crates/provider-contract/contract/extensions/exploration/README.md)
(`exploration` module) is selected by
`host.env.OULIPOLY_HOST_EXPLORATION_V1=1` and advertised as
`capabilities.exploration_v1`.
- **The offer.** The host may offer a registered parent opaque child route
  labels, its child requester (the root-child v1 requester surface) and the
  name of its owner ingress variable. These go in one JSON object in
  `OULIPOLY_EXPLORATION_V1`, beside the tool-mediation object.
- **No offer.** A launch without the object keeps the mediated `bash` tool
  alone.
- **Admission.** An offer the request's host did not select, or one without
  tool mediation, is refused (`exploration::admit`).
- **Serving.** The provider serves the offer with the same `tool_bridge`. The
  added `explore` tool runs no command. The host's owner admits or refuses
  each child, and the bridge renders its outcome with the launch, end, stop
  and lifecycle qualifiers. An absent process-creation fact stays unknown;
  the final turn-end reason is shown independently of bounded stage stderr.
  Route labels carry no provider, model or account
  meaning here, and a child is never offered exploration.

The [`live-stream/v3` contract](crates/provider-contract/contract/extensions/live-stream/README.md)
(`live_stream` module) is the record vocabulary of the optional live-output
plane. It is defined but not adopted yet.
- **What it covers.** Stream identity, publisher incarnation and sequence;
  `stdout`, `stderr`, `combined`, `pty` and typed `control` channels; exact gaps and
  restarts; the opaque durable reference a stream finalizes to; caller-owned
  cursors with terminal knowledge; and visibility claims. Structural schema plus
  documented normative semantics define conformance in every language.
- **Combined origin.** `combined` carries stdout and stderr joined before
  capture, such as both written to one pipe. Each byte's origin stays unknown:
  a descriptor never declares `combined` with `stdout` or `stderr`, and the
  SDK never splits or relabels it. A subscriber that did not select
  `combined` gets a live-only diagnostic.
- **Selection.** Peers select it by a bounded advertisement, not provider
  `describe`. Unknown, older or newer entries are ignored, and the selected v3
  entry and records are strict.
- **Admission.** Descriptor/follow setup joins selected channel, byte and
  advertised audience support. Resuming at a final cursor retains the durable
  reference and terminal state. Audience agreement does not authorize access.
- **Failures.** Fallible follower/replay APIs validate public typed inputs and
  return `live_unavailable` diagnostics. SDK-generated validation details do not
  echo submitted values; arbitrary caller detail is bounded, not sanitized.
  Diagnostics have no conversion to provider error, launch or completion outcomes.
- **What it does not replace.** Launch events and request custody, resident
  records, transcript page tokens and retained Bash output stay as they are,
  and the contract stores nothing.
- **Terminal claims.** `finalized` names a durably published record and
  claims no known or successful exit, complete bytes or report delivery; the
  durable record keeps those classifications. `ended` names none.
  A delivered `exit_observed` reports a publisher-observed exit. Without that
  delivered fact, the subscriber cannot infer whether an exit was observed:
  control may be unselected or the fact lost. Known or unknown command wait
  remains a classification in the durable record.
- **Version agreement.** `combined`, attachment and `not_authorized` use the
  optional `oulipoly.live_stream/v3` schema. Only v3 is selected; a baseline
  v1- or v2-only peer gets `no_common_version` for live viewing. Provider/v1 and
  session-control v3 remain independent. The retained v1 and v2 schemas are
  baseline evidence, with no runtime fallback or source/binary compatibility
  promise.
- **Attachment.** `live_stream::attachment` gives the publisher, broker and
  subscriber messages: hello, register, list/directory, attach/attached,
  record and unavailable. Registration and attachment need an explicit host
  decision, which the SDK never constructs. The contract states the host's
  scope, identity, finalization and drop-not-block duties without performing
  them.
- **Not established.** Capture, the broker, endpoints and sockets, enforcement
  of visibility, truthful origin labelling and runtime non-blocking behaviour
  remain later work. Agent Runner's interim root Bash view still uses its own
  wire; its convergence onto v3 is separate consumer work. Control
  claims live in `session-control/v3`; incident DTOs are not yet defined.

The [`session-control/v3` contract](crates/provider-contract/contract/extensions/session-control/README.md)
(`session_control` module) is the one provider-neutral vocabulary of root
control claims, shared by session control and infrastructure control. It is
versioned v3; it replaces unqualified v2 terminal/conflict meanings. Existing
source consumers must select and adopt v3; v1/v2 records are not retained.
- **What it covers.** Descriptive root discovery, current-state inspection
  and pending control intent, and requests for `input_hold`/`input_release`,
  same-incarnation `recover`, `cancel` and `close` on one claim ladder:
  requester intent, transport receipt, admission, semantic transition
  acknowledgment, inherited-intent fulfillment/non-fulfillment, refusal and outcome as distinct
  records. Lifecycle transitions preserve cancel precedence. The existing
  root owner/generation/incarnation is the addressed and answering authority;
  logical root/child/work/input links stay apart from attached process, Bash
  handle, provider-session and stream evidence; insertion, tagged end, logical
  debt and physical custody/wait facts stay apart.
- **Knowledge over time.** An `unknown` outcome can later be refined for the
  same immutable request; eight retained unknown reports cannot consume the
  reserved definite-outcome slot. Definite outcomes are final and contradictions
  are refused. A successor owner reports knowledge without acknowledging or
  erasing its predecessor's claims. It can report its own present fulfillment of
  an inherited admitted intent under the original immutable correlation, without
  predecessor transition authority. It can also report attributed terminal
  non-fulfillment of that admitted intent, never alongside a known positive. Late
  positive evidence contradicts the negative. A changed-key submission receives
  a conflict containing both complete requests; its answer is readable without
  changing a final original trace. Current-state reports relate to prior ACKs
  and fulfillment (current, retained, reporter-claimed supersession or contradiction).
- **Settlement.** Observations of one subject read as one order-independent
  evolving account; other roots' reporters are never composed, and a
  caller-supplied root lineage marks a reading `warranted` only under the
  caller’s warrant and coherent encounter selection. Physical summaries compose
  one exact actor reference; distinct or ambiguous references read conflicting.
  A waited actor is never subject-wide custody or logical settlement.
- **Selection.** Peers select it by a bounded advertisement, not provider
  `describe`; providers do not speak it. Hold is offered only with release.
  Absent or incompatible control capability is a `control_unavailable`
  diagnostic, never provider launch or completion unavailability.
- **Not established.** Records are claims: validation does not prove producer
  truth, authorization, enforcement, durability, lineage warrant or custody.
  Root authority, discovery indexing, durable control intent and execution
  stay with Agent Runner. Incident reports, evidence severity/scope and
  recovery authorization remain later work.

The independently versioned
[`terminal-unavailable/v1` extension](crates/provider-contract/contract/extensions/terminal-unavailable/README.md)
adds an explicitly selected `provider_unavailable` terminal result for temporary
model-service unavailability, distinct from account quota and rate limiting.
The `terminal_unavailable` module exposes its DTO, standalone schema, and
selection-aware payload admission. The aligned base snapshot structurally admits
the `provider_unavailable` kind, as Runner's host schema does; selection, not the
base schema, gates it. Existing routes can adopt this complete extension without
importing unrelated base-contract revisions. Runtime admission follows supported
wire schema/capability agreement as described above.

## Provider execution

The `agent-provider-execution` crate supplies provider-neutral machinery for one
native invocation per provider process. It was seeded from the Codex adapter's
launch path and generalized by parameterizing provider identity. The Codex and
Claude adapters both run their launches through `lifecycle::run_launch`:

- `lifecycle`: the shared one-shot launch lifecycle. `run_launch` holds request
  custody; compares the adapter's request digest; replays a matching complete
  journal byte-for-byte; discharges the recorded actor of an interrupted launch
  and then reports `ReconciliationRequired` instead of starting another native
  turn; checks termination requests and the host deadline before preparation,
  before and after publishing prepared state, and before opening the gate; a
  refusal after preparation discards adapter sidecars and leaves no state, for
  settled outcomes as well as native ones; publishes the running actor before
  the gate opens; observes the actual native start at the gate, where either
  spawning the gate process fails or the gate reports that its `exec` failed,
  and offers that start failure to the adapter, which may settle the launch
  with its own terminal outcome instead of the gate's exit 126 (a native
  program that ran and exited 126 is not a start failure); drains stdout/stderr as
  lines or raw chunks; emits heartbeats; terminates the native group on
  cancellation or deadline and after the leader exits, then drains until output
  closes or stays silent past the drain grace; checks input delivery; writes
  the final `exit` event itself; and seals the completion receipt. Adapters
  implement `LaunchAdapter`: request digest, native preparation (or a settled
  terminal outcome without a native process, journaled and replayed like any
  other), sidecar discard on refused admission, the meaning of a start
  failure, start markers, native output translation and the terminal
  status/signal. Each adapter maps `LifecycleError` variants to its own
  contract failures.

  Adapters are trusted code in the provider process, not a contained boundary.
  An adapter must not start native effects outside its gated command, must
  return every `EventSink` error instead of continuing to emit, must send
  native data through `EventSink::data` so the output accounting covers it,
  and must digest every input that determines the native effect.
  `run_launch` does not validate adapter events or terminal values against the
  contract schema and does not add an error event: after a failure, what
  follows the events already delivered is the adapter's caller's choice.

- `process`: an effect gate that withholds the native program's `exec` until
  the caller has published the process-group actor, Linux process-group custody
  with parent-death `SIGKILL` on the leader, a boot-scoped start-time actor
  incarnation, and recovery that skips a changed live leader. Recovery accepts
  only PGIDs `2..=i32::MAX`: zero and one produce reserved kill selectors `0`
  (caller's group) and `-1` (all permitted processes), and are rejected with
  `InvalidInput`, as are values above `i32::MAX`, before any probe or signal.
  This validation boundary applies to recovery, not the other process helpers.
  Incarnation checks and signals are separate syscalls, so recovery is not
  atomic protection against recycling. The provider
  chooses its gate argument and descriptor variable and dispatches
  `process::run_effect_gate` from `main` before other argument handling.
  After release the gate keeps its descriptor close-on-exec: a successful
  `exec` closes it, while a failed `exec` writes the `errno` there before the
  gate exits 126. `ExecObserver` distinguishes a reported exec failure from
  EOF without a report. EOF can also mean the gate ended before exec, or a
  report was unavailable or could not be delivered; `NoFailureReported` does
  not attest successful exec. Reports come from the exec attempt, not a `PATH`
  or permission prediction, and native exit statuses are not interpreted as
  start failures. Gates and callers from builds without
  the report fall back to the earlier exit-126 diagnostic in either direction.
  `ExecGate::release` now returns `io::Result<ExecObserver>` instead of
  `io::Result<()>`. Rust callers forwarding or binding the old typed result
  must adjust; statement callers can discard the observer. This source-API
  change does not change the wire format.
- `delivery`: `BoundedOutput`, a writer over a private duplicate of the host
  output descriptor. FIFO/socket writes fail after the no-progress stall limit
  (two seconds by default), and failure is sticky. Each successful partial write
  starts a new interval; this is not a total delivery deadline. Regular files
  and other descriptors can block inside a write beyond that limit. Inherited
  descriptor flags are never changed. Arbitrary `Write` sinks have no delivery
  bound supplied by framing or replay.
- `custody`: per-request exclusive locks, durable `prepared`/`running`/`complete`
  launch state, and an append-only journal sealed with its length and SHA-256.
  A complete journal is replayed only after it matches that receipt. The
  provider chooses the request digest inputs and maps outcomes to its failures.
- `framing`: `oulipoly.provider/v1` launch-event framing (contract, request ID,
  sequence, timestamp) written to the journal and then delivered. Sequence
  numbers are allocated before writing, including failed attempts; `seq()`
  does not certify successful journaling or delivery. The caller supplies
  object events and validates schema, ordering and finality.
- `cancellation`: process-scoped recording of `SIGTERM`/`SIGINT` so launch
  custody can terminate the native group and still publish its terminal state.
- `durable_fs` and `encoding`: ordered directory publication with parent
  synchronization, bounded reads and digests, base64, SHA-256, bounded text,
  and canonical JSON.

Directory creation syncs each missing directory and its containing parent before
creating the next level. An existing target is synced itself (after mode 0700 for
private creation); pre-existing ancestors are not reopened or synced. Creators
must publish pre-existing ancestor links durably before handing them over if
crash durability is required. This includes caller-created state roots: existence
alone is not a durability receipt. A containing parent of a newly created link
must be readable for directory sync on Linux, even if search/write permissions
allowed creation. Denial and other sync errors remain failures, possibly after
visible partial creation. A later successful call on that visible path does not
certify the failed earlier publication. No crash-durability guarantee is supplied
for a failed lineage by retry or by a later successful sync. Current per-call
uses discard a failed root and create a fresh one; diagnostic retention is for
inspection. Deliberate recovery must name the roots and lineage it relies on
and establish its required publication guarantee at that consumer boundary.
The SDK supplies no whole-lineage recovery mechanism or receipt for host-created
administrative links. The former eight-level ancestor sweep and implicit retry
repair are removed. File publication, custody and lock ordering retain their
existing semantics.

The resident's durable session, input, insertion and ACK claims are conditional
on this publication boundary. `serve` publishes its newly created links; existing
incoming links must already belong to a successfully published lineage. Under a
failed/unproved lineage, `initialize`, `session/new`, `session/resume`, a visible
record or an insertion ACK may still be produced: they are not proof of
whole-lineage host-crash durability. No runtime precondition check is added.
A consumer unable to establish that lineage must use a fresh per-call root and
carry prior input uncertainty as do-not-replay; it must not silently resume it
as durable. Host-crash recovery of retained run trees is unqualified and needs
an explicit consumer decision. No host crash or loss-of-data experiment is
claimed by the directory controls.

Native argv, authentication, account and config roots, model aliases, tool
restrictions, native session formats, and native event translation remain in
adapters. It does not depend on `agent-provider-contract` (its tests do) and
does not change the pinned v1 snapshot.

- `lifecycle::run_launch_until` is `run_launch` with one caller-scoped stop
  flag, observed at the same admission checks and poll turns as the
  process-scoped termination latch. Before admission it refuses with
  `LifecycleError::Cancelled`; once the native group runs it terminates that
  group and reports the new `StopCause::Requested` to `finish`. Setting it never
  affects another launch. `StopCause::Requested` is a Rust source-API addition:
  exhaustive matches on `StopCause` must add it; the wire is unchanged.
- `lifecycle::reconcile_interrupted_launch(state_root, provider_instance_id,
  request_id)`: discharges an already-recorded incomplete actor under its
  custody lock, preserving original digest/evidence, without replay or admission.
  Resident recovery invokes this before adapter policy validation.
- `tool_bridge`: the mediated `bash` tool of `tool-mediation/v1`, a stdio MCP
  server with one tool that a provider registers natively and serves as its own
  executable's `tool.bridge` subcommand (`tool_bridge::main`). Under an allow
  list a command not named exactly is refused and no requester starts; without
  the ingress variable no requester starts. Otherwise the call runs only
  `requester run --delivery sync|async -- bash -lc COMMAND` (or the retained
  output reads `native-output` / `native-accept`) and renders what the
  requester's root v1 result established; a missing or malformed result is
  "may have run; do not replay". A positive ordered `started.exec_error` is
  rendered as failed exec with its diagnostic and `isError: true`, separately
  from the physical work's wait and output facts. Accepted custody, possible
  setup effects and unsafe retry are preserved. Async failed exec never claims
  running; a validated detach/identity preserves the later completion obligation,
  while incomplete delivery remains unconfirmed. Code 127 alone is an ordinary
  numeric wait, not evidence of failed start. These are existing v1 stage facts,
  not a new wire version or capability.
  - **`serve_offered` and `explore_tool`.** When the bridge's environment
    carries an `exploration/v1` offer, the same server adds `explore`. It
    runs only `requester ROUTE QUESTION` for an offered route.
  - **Rendering.** It renders the root-child v1 requester's owner `result` or
    `refused` (`explore_tool::render_child`). `lost`, mismatched, unknown or
    inconsistent replies are unresolved: a child may have been admitted and
    run, and is never asked for again automatically.
  - **No offer.** The bridge is `bash` alone, as before. `serve` keeps its
    signature. The server and requesters stay in the native
  process group, so turn cancellation ends them; `notifications/cancelled`
  ends one call's requester without an answer.
- `resident`: the agent side of the ACP v2 draft subset Agent Runner's root
  supervisor consumes (`schema-v2.0.0-alpha.7`), served by `resident::serve`
  over one newline-delimited JSON-RPC connection. It serves `initialize`
  (protocol 2, `session: {}`, the message-key dedup contract and the
  resident-session contract in `_meta`), `session/new`, `session/resume`,
  `session/prompt`, `session/cancel`, `session/close` and `session/list`.
  Every turn is one provider/v1 launch the adapter runs through
  `run_launch_until` under the session's own launch state root
  (`ResidentTurns::run_turn`), so custody, the seven-field launch state,
  replay and reconciliation are the shared lifecycle's. Turns of a session run
  in arrival order. An input is durably recorded with an ascending `messageId`
  before its turn may start; the prompt is acknowledged only when the turn
  emits `oulipoly.submitted_user_turn` and insertion is durably recorded:
  `user_message` then the ACK then `state_update: running`. Native consumption
  with an unavailable insertion store yields `-32011` (unknown), without ACK or
  output delivery; it can never become retryable `-32010`. Each `stdout` data event is one
  `agent_message` tagged `oulipoly.ai/parentMessageId`; the turn ends with one
  idle `state_update` tagged `oulipoly.ai/lastUserMessageId`, a stop reason
  (`end_turn`, `cancelled`, `_oulipoly_native_failed`, `_oulipoly_turn_failed`,
  `_oulipoly_reconciliation_required`) and `oulipoly.ai/nativeTurn` (launch
  request id, status, terminal signal, launch-output accounting and custody).
  A completed turn, or a justified refusal before start, without consumption answers
  a JSON-RPC error and records the input as not inserted; a failure whose
  consumption is unknown is never rerun. A resent message key inserts nothing:
  it answers the original `messageId` once insertion is known and repeats the
  ended turn's tagged idle (outputs are not re-sent). This attests key identity
  only; a duplicate ACK does not accept the current prompt bytes. `session/cancel` stops
  only that session's running turn (process group terminated and drained) and
  refuses its queued inputs; `session/close` answers success after settling
  session custody and releasing the lock, with its worker joined and
  worker resources dropped. Settlement also scans this session's recorded launches
  independently of input reconstruction, using their custody locks and recorded
  actor incarnations. Connection end or `SIGTERM`/`SIGINT` joins all session workers;
  unresolved custody returns an I/O error from `serve`, including after a refused
  close released session ownership. A later successful settlement of that session
  in the same connection can discharge this close failure.
  `session/resume` (same working directory, one holding process per session
  through an exclusive lock) reconstructs readable inputs an earlier process
  left unfinished and settles them through the lifecycle without readmission:
  a complete equal-request journal replays without native effects; an interrupted
  one has its recorded process group discharged before any current-template
  digest comparison,
  and the interrupted journal's markers recover native session identity and
  consumption. Native create/resume selection is bound and persisted at
  dispatch from the preceding settled session state, so queued turns continue
  it. Native-session publication errors surface as failures; incomplete custody
  stays unsettled, without an ended record or idle completion. Immediate
  reconciliation may discharge it; unresolved custody, including unreadable launch
  evidence, refuses subsequent native dispatch and successful close/resume. Config content
  hashes protect private prepare records, not compatibility or recovery admission.
  Limits: no per-turn deadline or silence kill; only text prompt
  content. ACP records are bounded at 32 MiB (including newline); an over-bound
  record receives an error with null request ID and closes the connection after
  settling its running turns. Ingress buffering holds at most one queued record.
  New serialized input records are limited to 8 MiB before admission, reserving
  room for updates under the 16 MiB recovery/publication bound; session records
  are limited to 64 KiB. Refused input starts nothing and reserves no message key.
  Interrupted journal recovery streams records under a 16 MiB per-record bound,
  independent of total journal length. Missing/cut/invalid/over-bound evidence is
  explicit uncertainty, preserves known insertion and reconciled actor custody,
  clears the native ID and blocks new input. Resume settles readable inputs even
  alongside an unreadable input, then reports `-32012`; known duplicate ACK/end
  remains available, while new input is blocked. No corrupt evidence is deleted.
  Missing, busy, unreadable, invalid or over-bound launch evidence never turns
  a recorded insertion into non-insertion. Known duplicate ACKs remain usable;
  inputs without a justified ACK retain explicit uncertainty, with no new-key
  advice. Unusable recovery evidence leaves input bytes intact and the input
  eligible for settlement after restoration. Non-start requires a recorded
  undispatched input with no consumption evidence and neither launch nor journal
  evidence under its request lock, or a fresh locally refused attempt under
  that same absence check. Dispatch is durably recorded before adapter entry;
  refusal/cancellation uncertainty alone does not erase the undispatched premise.
  After temporary custody unavailability ends, duplicate lookup or settlement
  can refine `-32011` to `-32010` only with that positive non-start evidence.
  Recorded dispatch, insertion or turn evidence prevents this refinement even
  when both custody paths are absent. A completed non-consuming turn remains
  non-inserted and retains exact completed replay.
  An unreadable input is not reconstructed, but readable launch evidence still
  discharges its recorded interrupted actor without prompt reconstruction, replay,
  inferred insertion or rewriting custody bytes. Unreadable/invalid launch evidence,
  a missing launch record alongside its journal, or failed actor discharge remains
  explicitly unsettled: close returns `-32012` and connection end returns an I/O
  error. Physical settlement and later successful close/EOF do not clear earlier
  `-32012` insertion/continuity uncertainty or justify deleting the retained root.
  These controls use fake actors and synthetic corruption; they establish neither
  natural fault frequency nor real-native recovery qualification.
  These errors do not prove non-insertion. These are per-record bounds, not a
  session-count or retention policy. Outputs are not replayed to a later
  connection; session records and
  turn journals are never pruned; the provider's own process loss leaves
  descendants outside a PID namespace running; later resume reconciles recorded
  session actors, subject to the unreadable-launch-evidence limits above;
  Linux-tested only.

A missing launch state alongside a retained journal is unusable custody evidence;
the shared lifecycle refuses before adapter preparation or a fresh native launch,
preserving that journal for restoration and reconciliation.

Adapters that compose the individual modules instead of `run_launch` own the
lifecycle themselves: keep request custody held, check the digest
and complete phase before replay, publish prepared state, spawn behind the
gate, capture and publish the running actor, create the journal and finish
admission before releasing the gate. Retain a child cleanup guard. After
successful final-event delivery, sync/seal the journal and publish complete
state with its receipt and cleared actor. These helpers do not validate state
transitions, enforce path confinement or prevent external journal mutation;
locks coordinate cooperating callers only.

During an active launch, any journal, framing or delivery error requires the
caller to stop emitting and unwind native custody, leaving incomplete evidence
for reconciliation. A failed replay of an already-complete journal preserves
its complete state and receipt for another replay attempt. Framing errors are
not latched or rolled back; a journal append error may leave length/hash
accounting inconsistent, and host delivery may have exposed only a prefix.
Do not continue or publish completion after an error. `seal(&mut self)` syncs
and returns the current receipt without closing append access: stop appending
before sealing and never append after publishing that receipt. Journaling
before delivery is write ordering, not an fsync before each event. Cancellation
is a process-global latch without reset; handler installation does not check
errors or restore previous signal dispositions, and the caller performs cleanup.

`run_launch` guarantees and limits: it serves one launch per provider process
and installs the process-scoped cancellation handlers. The state root must be a
trusted, existing, provider-private directory; the request key is the provider
instance and request ID, and the durable record keeps its seven fields
(`digest`, `phase`, `actor_id`, `incarnation`, `exit_code`, `journal_sha256`,
`journal_len`), so a compatible rebuild that keeps an adapter's digest inputs
replays and reconciles earlier records; completed records are replayed as
their original bytes, never reinterpreted. Host output should be
`BoundedOutput`. A failure before completion leaves incomplete evidence (or
none, after an admission refusal) and terminates the native group while
unwinding; recovery after provider loss happens on the next retry of the same
request, not in the background. Completion delivers the `exit` event, then
seals the journal, then atomically replaces the state record: a seal or state
failure can follow a delivered `exit`, and a directory-sync error can be
returned after complete state is already visible, so neither a delivered
`exit` nor an error proves which state was published. Adapter hooks, digests,
filesystem operations and host writes run synchronously; termination requests
and the deadline are observed at admission checks and poll turns, not
preemptively. Drain bounds measure silence rather than total time. Linux-tested
only; `lifecycle` is compiled on Unix.

## Provider memory harness

`provider-memory` measures a complete Linux process tree using
`/proc/<pid>/smaps_rollup`. Reports keep RSS and PSS separate so shared pages are
not presented as private provider cost. They also record private/shared memory,
swap, process roles, PID start times, executable SHA-256, and caller-supplied
non-secret version/config identities. Time-series retention is bounded by
`--max-samples` (default 4096); peak totals still include samples dropped from
the retained tail.

```bash
# One point-in-time sample of an existing Agent Runner process tree
cargo run -p agent-provider-memory -- snapshot --root-pid 12345

# Bounded time series for an existing process tree
cargo run -p agent-provider-memory -- attach \
  --root-pid 12345 --duration-ms 5000 --interval-ms 100 \
  --identity terminal_version=1.18.23

# Launch and measure a workload. Child stdout/stderr stay attached; the report
# is written atomically to the requested path and never records child argv.
cargo run -p agent-provider-memory -- run \
  --output ./memory-report.json --interval-ms 100 \
  --identity workload=bash-only -- /usr/bin/sleep 1
```

Attach to the long-lived `agents`/Agent Runner PID rather than a short-lived
provider bridge. Descendant discovery follows parent relationships and therefore
continues across child-created process groups, including native terminal, LSP,
and MCP children.

## Related repositories

- `agent-runner` — host, routing, lifecycle, and provider registry
- `agent-runner-opencode` — OpenCode adapter
- `agent-runner-pi` — Pi adapter
- `agent-runner-codex` — Codex adapter
- `agent-runner-claude` — Claude Code adapter

## Local layout

```text
~/projects/agent-provider-sdk/
├── trunk/       # clean main integration checkout
└── worktrees/   # isolated ticket branches
```

The mediated bridge serializes requester spawn with cancellation/connection
closure, collects each direct requester exit, and bounds stopped collection and
post-exit pipe drain to two seconds (unconfirmed exit is an explicit bridge
error). Requester termination does not revoke independently accepted owner work.
Malformed waited/output/acceptance facts are unresolved; retained continuation
uses the bytes actually displayed, including UTF-8 and hex display limits.
Linux durable actor capture/recovery refuses a proc view whose own `NSpid` and
`/proc/self` do not identify the caller's PID namespace. Hosts must supply a
matching proc mount; the SDK does not translate namespace-local PIDs.
