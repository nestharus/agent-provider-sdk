# Session Runtime and Infrastructure Control Plane

Status: accepted program direction; implementation tracked by APV-28 through
APV-36.

## Decision

Extend the existing Agent Provider projects and repositories. Do not introduce
another provider project, logical-agent registry, canonical session store, or
provider-specific control plane.

- Agent Provider SDK defines the versioned contracts and conformance fixtures.
- Agent Runner remains the logical session/agent authority and owns scheduling,
  admission, ancestry, mailbox/wake routing, and incident policy.
- Agent Bash remains the generic detached-process supervisor and bounded output
  capture source.
- Provider Runtime and Memory owns provider-neutral brokers, OS resource
  witnesses, isolated recovery services, and lifecycle/update mechanisms.
- Provider adapters translate native terminal semantics and supply deterministic
  reproduction/canary fixtures. They do not implement fleet policy.

Completed turns remain canonical in the normal provider and Runner session
files. The live plane retains only bounded active-turn output plus a short
finalization grace. It does not add a permanent high-rate output journal.

## Identity model

The following identifiers correlate but are not interchangeable:

| Identity | Meaning | Authority |
|---|---|---|
| Agent definition ID and digest | Declarative agent behavior and policy | Agent DSL |
| Logical agent run / invocation UUID | One durable node in the agent tree | Agent Runner |
| Session chain ID | Runner continuity across provider sessions | Agent Runner |
| Provider session and turn IDs | Native provider continuity | Provider adapter/session storage |
| Agent Bash handle | One detached generic workload | Agent Bash |
| PID, start ticks, boot ID, pidfd/cgroup | Exact live OS execution evidence | Process custodian |
| Stream ID and publisher incarnation | One ephemeral live-output source | Live stream protocol |

Late or unresolved provider identity is an explicit state. A PID, process handle,
or stream ID must never be substituted for a logical agent or provider session.

## Session DSL v1

The Session DSL is a versioned provider-neutral request and observation model. It
supports:

- `start`, `send`/`turn`, `resume`, `fork`, `pause`, `drain`, `cancel`,
  `inspect`, `list`, and `subscribe`;
- model, provider, profile, and existing-session selectors;
- workspace, file, artifact, and durable-session references;
- settings/profile digests, tool/capability requirements, explicit environment
  transforms, resource budgets, deadlines, and stream policy;
- idempotency keys, caller/custody lineage, lifecycle evidence, provider-native
  session/turn identity, and terminal outcomes.

The DSL negotiates a major/minor range before work. Major incompatibility rejects
the requested operation with an actionable capability error. An optional
observability incompatibility disables only that feature and cannot reject the
underlying session launch or completion.

## Agent DSL v1

The Agent DSL describes logical behavior without encoding process topology:

- stable definition identity, version, and content digest;
- instructions and system/profile digests;
- allowed model/provider/capability ranges and escalation rules;
- tool policy, workspace/file scope, and explicit environment transforms;
- parent/child delegation constraints, result custody, and return obligations;
- retry, cancellation, recovery, resource, timeout, and observability policy.

Agent Runner materializes the logical tree, maps it to invocations and session
chains, and correlates exact provider/Agent Bash/process identities. Providers
only attest which requested DSL semantics they can implement.

## Active live-output plane

The first multi-process topology is an account-local broker that starts on demand
and reaps after 60 minutes with no publishers, subscribers, registrations,
handoff work, or recovery obligations:

```text
provider / PTY / Agent Bash capture
              |
       nonblocking publisher ring
              |
       lazy local broker
              |
      bounded subscriber queues
```

Every producer retains a bounded local replay window and a no-broker fallback.
The broker is not launch custody, session truth, or a completion oracle. Its
absence, crash, incompatibility, update, or memory pressure produces a
`live_unavailable` diagnostic while execution continues.

Each stream has a random 128-bit stream ID, publisher incarnation, monotonically
increasing sequence, channel (`pty`, `stdout`, `stderr`, or typed control),
optional logical correlations, and explicit `gap` and `finalized` events.
Separate stdout/stderr pipes preserve host observation order only. Reconnect is
at-least-once by cursor and deduplicated by `(stream_id, seq)`.

All limits are byte-accounted. The initial values to benchmark are:

| Limit | Provisional value |
|---|---:|
| Maximum frame | 64 KiB |
| Active tail per stream | 1 MiB |
| Aggregate broker payload | 64 MiB |
| Queued writes per connection | 256 KiB |
| Broker RSS soft ceiling | 128 MiB |

Capture uses `try_publish` and always drains child output. Overflow advances the
oldest retained sequence and emits an exact gap. A slow subscriber is marked
lagged and disconnected without stalling the publisher. `finalized` is emitted
only after normal durable session publication and carries its durable reference.

A permanently resident service, durable stream index, and active-stream
migration are later optimizations. They must retain this protocol and fallback.

## Update and schema availability

Shared runtimes use content-addressed, verified A/B artifacts. Generation A stops
accepting new streams but keeps active connections; generation B becomes current
for new registrations. A and its executable remain until their obligations
drain. Rollback changes current selection without disturbing A. Version 1 does
not migrate active streams.

The first broker has no database. Unknown protocol or broker state disables live
viewing only. A later metadata index must be separate from Runner state, retain an
overlapping A/B compatibility window, use additive expand/contract migrations,
and never hold a transaction across IPC or launch. Raw active output never moves
into the incident ledger.

## Infrastructure incidents

Task agents emit bounded typed observations and continue until the control plane
instructs them to quiesce. An independent coordinator deduplicates and classifies
reports, determines severity/scope, snapshots targets, coordinates repair, and
authorizes resume.

The durable state machine is:

```text
observing -> correlating -> confirmed -> pausing -> paused
          -> repairing -> verifying -> resume_authorized
          -> resuming -> monitoring -> resolved
```

It also supports `false_positive`, `superseded`, and `manual_hold`. Failed
verification or recurrence returns to `pausing` under a strictly greater epoch.
All mutations compare-and-set incident ID, revision, and coordinator fence.
Lease expiry stops new coordinator/repair effects but never resumes execution.

Pause is an admission-and-drain protocol. Every run, root/child spawn, wake,
resume, queued turn, tool/effect boundary, provider retry/rotation, and repair
launch checks the greatest applicable epoch. Targets acknowledge
`paused_safe`, `pause_blocked`, `exited`, `already_terminal`, or
`offline_fenced`; transport receipt is not a semantic pause acknowledgement.
Offline roots reconcile the current epoch before they may execute again.

Already-running work reaches a declared commit/rollback safe point while new
work is fenced. Exact-process hard cancellation is reserved for explicit
critical policy. A readiness sentinel does not imply an Agent Bash service is
paused.

## Causal process evidence

`SIGKILL` proves only signal 9. Runner and Agent Bash also use it for timeout and
cancellation, so OOM attribution requires correlated evidence: exact PID/start
ticks/boot ID, leaf-cgroup membership and `memory.events.local` deltas, the
launch/exit window, and optionally a service-manager witness. RSS, one PSI spike,
stale trace age, process absence, or hierarchical counters alone remain
`cause=unknown`.

Provider auth/quota errors, invalid input, unsupported capabilities, and an
isolated timeout stay task/account scoped. Fleet escalation requires an
authoritative shared-component witness or corroboration across independent roots.

## Independent recovery

The coordinator runs outside Runner/provider/Agent Bash process groups and
cgroups with reserved resources, bounded queues, fenced leases, a low-rate
ledger/ticket outbox, and restart limits. There is one incident ticket, repair
lease, and resume authority per deduplication key. An agent-assisted repair uses
only a resource-bounded lane on a known-healthy provider; otherwise the incident
enters `manual_hold`.

Resume authorization requires the formerly failing reproduction to pass, exact
artifact/config digests, schema compatibility, clean custody, and a bounded
canary. Final resolution additionally requires semantic acknowledgements or
offline fences, backlog reconciliation, independent canaries, and a quiet window.
Ticket API failure cannot block control transitions.

## Delivery plan

| Issue | Existing project | Result |
|---|---|---|
| APV-28 | Agent Provider SDK | Session DSL v1 and Agent DSL v1 |
| APV-29 | Agent Provider SDK | Live-stream and infrastructure-control contracts v1 |
| APV-30 | Provider Runtime and Memory | Authoritative Runner session runtime |
| APV-31 | Provider Runtime and Memory | Bounded capture and lazy local broker |
| APV-32 | Provider Runtime and Memory | Subscription surfaces and safe A/B drain |
| APV-33 | Provider Runtime and Memory | Causal process/OOM/pressure witnesses |
| APV-34 | Provider Runtime and Memory | Incident ledger, classifier, and epoch admission fence |
| APV-35 | Provider Runtime and Memory | Isolated coordinator and recovery verifier |
| APV-36 | Agent Provider SDK | End-to-end session and outage conformance |

Existing APV-10 owns shared warm-runtime generations, idle lifecycle, and safe
auto-update handoff. Existing APV-11 owns bounded hot/cold session bookkeeping and
reaping. Provider-specific implementation stays in the existing OpenCode, Pi,
Codex, Claude Code, and Direct API Provider projects.

## Rejected approaches

- a permanent high-rate output-event journal duplicating normal session files;
- making the broker or resident service a provider-launch dependency;
- per-producer mutable mmap/shared-file transport as the first public design;
- active-stream migration before drain-only A/B updates survive crash testing;
- provider/SDK/Agent Bash-owned fleet broadcasts or incident classification;
- reusing the mutable mailbox pause bit or allowing a lease timeout to resume;
- inferring OOM from SIGKILL or inferring child death from a vanished root;
- allowing affected task agents to recursively own infrastructure repair.
