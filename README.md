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
from Runner commit `5d025b82` byte-for-byte, plus one SDK addition: the
host-selected `capabilities.resident_session_v1`. `UPSTREAM.md` records the
exact provenance. The DTO projection follows: `DescribeCapabilities` gains four
optional capability fields, `LaunchParams` gains `prompt_acceptance` and
`output_delivery`, `TerminalSignalKind` gains two variants, and
`session.read_turns` uses the bounded page DTOs. These are Rust source-API
changes (struct literals and exhaustive matches must adjust), so the workspace
package version is 0.2.0; admitted wire JSON follows the aligned host snapshot.

`negotiation` selects common supported versions without identity equality:
`select_contract_version` uses the provider's preferred `oulipoly.provider/vN`
when the host supports it, otherwise the highest common one, and ignores
versions the host does not know. For host-selected extensions, a host offers
each supported version with its own `host.env` selector
(`<PREFIX>_V<n>=1`), a provider advertises `<capability>_v<n>: true` only for
offered versions it supports, and the host uses the highest offered version the
provider advertised; no common version is an explicit refusal. Because the v1
describe schema is closed, a provider never sends a capability its host did not
offer. Golden cases live in `tests/fixtures/negotiation/` (also exported
through the `contract-test-fixtures` feature).

The [`resident-session/v1` extension](crates/provider-contract/contract/extensions/resident-session/README.md)
(`resident_session` module) is selected by
`host.env.OULIPOLY_HOST_RESIDENT_SESSION_V1=1` and advertised as
`capabilities.resident_session_v1`. Its `resident.prepare` subcommand admits a
policy-evaluated launch template and answers the arguments the host appends to
the same registered provider executable to start a resident ACP v2 endpoint on
stdio, the ACP subset served, and the operations offered. The endpoint is
`agent_provider_execution::resident` (below).

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
  emits `oulipoly.submitted_user_turn` (recorded first), followed by
  `user_message` and `state_update: running`. Each `stdout` data event is one
  `agent_message` tagged `oulipoly.ai/parentMessageId`; the turn ends with one
  idle `state_update` tagged `oulipoly.ai/lastUserMessageId`, a stop reason
  (`end_turn`, `cancelled`, `_oulipoly_native_failed`, `_oulipoly_turn_failed`,
  `_oulipoly_reconciliation_required`) and `oulipoly.ai/nativeTurn` (launch
  request id, status, terminal signal, launch-output accounting and custody).
  A turn that ends, or is refused before it starts, without consumption answers
  a JSON-RPC error and records the input as not inserted; a failure whose
  consumption is unknown is never rerun. A resent message key inserts nothing:
  it answers the original `messageId` once insertion is known and repeats the
  ended turn's tagged idle (outputs are not re-sent). `session/cancel` stops
  only that session's running turn (process group terminated and drained) and
  refuses its queued inputs; `session/close` answers after settlement;
  connection end or `SIGTERM`/`SIGINT` stops and settles every running turn.
  `session/resume` (same working directory, one holding process per session
  through an exclusive lock) settles inputs an earlier process left unfinished
  by rerunning their exact launch request: a complete journal replays without
  native effects, an interrupted one has its recorded process group discharged,
  and the interrupted journal's markers recover native session identity and
  consumption. Limits: no per-turn deadline or silence kill; only text prompt
  content; outputs are not replayed to a later connection; session records and
  turn journals are never pruned; the provider's own process loss leaves
  descendants outside a PID namespace running until a later resume discharges
  them; Linux-tested only.

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
