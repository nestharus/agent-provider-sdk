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

One active host/provider route must use the same complete v1 snapshot on both
sides. Mixed-snapshot operation under the shared `oulipoly.provider/v1`
discriminator is unsupported. Before rollout, the route owner must be able to
keep the previous matched pair available through replacement verification and
the rollback decision. If it cannot meet that prerequisite, the upgrade does not
start and the route continues on its previous pair. Exact snapshot coherence
takes priority over independently upgrading peers that merely share the v1
discriminator, even when a revision is otherwise wire-compatible; mixed
revisions are not an availability fallback. Rollback restores the retained
previous pair as one unit.

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

The independently versioned
[`terminal-unavailable/v1` extension](crates/provider-contract/contract/extensions/terminal-unavailable/README.md)
adds an explicitly selected `provider_unavailable` terminal result for temporary
model-service unavailability, distinct from account quota and rate limiting.
The `terminal_unavailable` module exposes its DTO, standalone schema, and
selection-aware payload admission. The pinned base snapshot and base admission
APIs remain unchanged. Existing routes can adopt this complete extension without
importing unrelated base-contract revisions; this does not change the
matched-snapshot requirements for a base-contract upgrade.

## Provider execution

The `agent-provider-execution` crate supplies provider-neutral machinery for one
native invocation per provider process. It was seeded from the Codex adapter's
launch path, which consumes it, and generalized by parameterizing provider
identity:

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
adapters. The crate is not a resident runtime: signal handling is per process,
and a shared multi-session runtime needs session-scoped cancellation and its
own lifecycle tests. It does not depend on `agent-provider-contract` and does
not change the pinned v1 snapshot.

The adapter owns the lifecycle: keep request custody held, check the digest
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
