# Agent Provider Project Handoff

Last updated: 2026-08-25

Use this file as the starting point for the next Agent Provider work session.
Read it completely before changing Linear, provider routing, installed binaries,
or any product repository.

## Program objective

Build a provider-neutral platform that can start and control agent sessions,
describe agents declaratively, preserve prompt/tool safety across terminal
providers, and reduce total system memory without coupling Agent Runner to one
terminal implementation.

The program is owned by the Linear team `agent-provider` (`APV`, team ID
`661682fb-ee78-4568-917c-ece5205b27fc`). It has seven existing projects:

1. Agent Provider SDK
2. Provider Runtime and Memory
3. OpenCode Provider
4. Pi Provider
5. Codex Provider
6. Claude Code Provider
7. Direct API Provider

Session management, live streaming, logical agent trees, memory-pressure
observability, and infrastructure outage recovery are enhancements within these
projects. Do not create another project layer for them.

## Read first

- `AGENTS.md` — repository ownership, worktree, safety, and compatibility rules.
- `README.md` — implemented SDK surface and memory-harness usage.
- `docs/architecture/session-runtime-control-plane.md` — accepted direction for
  the Session/Agent DSLs, live streaming, safe updates, and outage control.

The architecture document was merged by GitHub PR #1 at commit
`d0430f5398a8205b8958dafcefa3436497f29916`. Main CI run `32909988877` passed.

## Exact current state

The SDK remains in bootstrap. The architecture and work graph are established,
but most shared contracts and runtimes are not implemented yet.

Implemented and shipped:

- `provider-memory`, a Linux process-tree RSS/PSS sampler with bounded retained
  samples, peak accounting, exact PID/start-time handling, executable digest,
  and launch/attach/snapshot modes.
- Installed binary: `/home/nes/.local/bin/provider-memory`.
- Installed SHA-256:
  `364a9113ffcef5e39ffea62a3a5c0d22c78e6284e53877a7ce5b79fc4e43cadc`.

Planned, not shipped:

- Session DSL v1 and Agent DSL v1;
- live event protocol, subscription API, or lazy stream broker;
- a permanent/resident shared session server;
- authoritative production wiring of Runner's durable SessionSupervisor;
- incident ledger, fleet pause/resume epoch, repair coordinator, or automated
  recovery verifier;
- shared LSP or MCP multiplexing;
- Pi, Codex, Claude Code, or Direct API production routing through this SDK.

Do not describe any planned surface as available until its ticket acceptance
tests and provider canary have passed.

## Repository layout and ownership

All new repositories use a clean integration trunk and isolated worktrees:

```text
~/projects/<repository>/
├── trunk/
└── worktrees/
```

Primary repositories:

| Repository | Role | Known integration state at handoff |
|---|---|---|
| `agent-provider-sdk` | Shared contracts, DSLs, testkits, memory/runtime helpers | clean `main` at `d0430f5`, remote equal |
| `agent-runner` | Routing and logical session/agent authority | clean `main` at `afdd74f`, remote equal |
| `agent-bash-tool` | Detached generic process custody and capture | clean `main` at `2cc7811`, remote equal |
| `agent-runner-opencode` | Production OpenCode adapter | clean `main` at `254925f`, remote equal |
| `agent-runner-pi` | Isolated Pi adapter | clean `main` at `690844a`, remote equal |
| `agent-runner-codex` | Isolated Codex adapter | clean `main` at `89198e2`, remote equal |
| `agent-runner-claude` | Existing Claude Code adapter | pre-existing `trunk` checkout is on `s9a-claudecode-rebuild`; do not normalize or overwrite it without resolving its ownership |

No Direct API provider repository exists yet. APV-25 threat-models that provider
before APV-26 creates the prototype.

Perform implementation in `worktrees/<ticket-or-task>`, merge the verified PR to
remote `main`, fast-forward the clean trunk, then remove the worktree and local
and remote task branches. Preserve unrelated dirty worktrees.

## Ownership boundaries

### Agent Provider SDK

Owns versioned provider-neutral schemas, Session/Agent DSLs, profile/capability
negotiation, live-event and incident-evidence DTOs, compatibility fixtures,
central system-prompt/tool declarations, and conformance testkits.

It does not own Runner scheduling, ancestry, mailbox routing, process custody,
broker residency, provider-native authentication/state, or fleet repair policy.

### Agent Runner

Remains the only logical authority for invocation/session/turn/chain identity,
agent ancestry, model/profile selection, admission, runtime generations,
mailbox/wake/PTY routing, logical tree inspection, incident classification,
broadcast target expansion, and cancellation routing.

Runner already contains durable session-supervisor and provider-turn substrate,
but it is not authoritative on every production entry point. Do not create a
third runtime registry; reconcile the existing lifecycle representations under
APV-30.

### Agent Bash

Owns generic detached workload handles, guardian/subreaper/pidfd/cgroup custody,
bounded stdout/stderr capture, exact terminal evidence, and workload
cancellation. It is the correct capture point for streaming arbitrary process
output.

Agent Bash must not turn a handle or PID into an agent identity, interpret
provider sessions, classify incidents, or fan broadcasts directly to agents.

### Provider adapters

Own native argv, authentication, quota, transcript/session formats, config/state
roots, and mappings from native errors/events into shared contracts. Adapters
must remain thin and must not implement their own broker, incident ledger, or
fleet policy.

## User directives that remain binding

- Do not filter provider environments through an allow list. Providers cannot
  know every variable a terminal needs; preserve inherited environment variables
  unless a contract explicitly transforms one.
- Do not disable automatic updates. Make them safe with content-addressed A/B
  artifacts, compatibility negotiation, new-work cutover, and old-work drain.
- Unpinned dispatch is the normal routing mode. Do not require pinning as a
  workaround for native runtime identity.
- Live provider tests use `gpt-luna-low` unless the user explicitly requests a
  different model.
- Registration/bookkeeping contention may be bounded, but accumulated completed
  sessions or running processes must not cause launch hard failures. Use bounded
  hot metadata, cold durable state, LRU/reaping, and concurrency-safe cleanup.
- A numeric capacity increase is not a complete fix. Stress beyond the old
  64/80/100 limits and prove graceful behavior at 300+ sessions.
- Live observability is optional. Broker absence, incompatibility, update,
  capacity, or failure must never reject provider launch, Agent Bash custody,
  terminalization, canonical session publication, or completion notification.
- Do not run large review fan-outs unless explicitly requested. For urgent
  runtime work, prioritize focused tests, build/install, and a real smoke test.
- When implementation changes `agents` behavior, build from the correct
  worktree, install the resulting `agents` binary, and verify the installed
  artifact. Do not claim installation after source-only tests.

## Identity and session invariants

These identifiers correlate but are not interchangeable:

- Agent definition ID/digest — declarative behavior.
- Runner invocation UUID — durable logical agent/run node.
- Runner session-chain ID — logical continuity across provider sessions.
- Provider session/turn ID — native provider continuity.
- Agent Bash handle — one detached generic workload.
- PID/start ticks/boot ID/pidfd/cgroup — exact live OS execution.
- Live stream ID/publisher incarnation — one ephemeral output source.

A PID, Agent Bash handle, or stream ID must never substitute for a logical agent
or provider session. Late/missing provider identity is an explicit unresolved
state.

## Session and Agent DSL direction

Session DSL v1 should cover `start`, `send`/`turn`, `resume`, `fork`, `pause`,
`drain`, `cancel`, `inspect`, `list`, and `subscribe`, including model/profile
selectors, workspace/files/artifacts, settings digests, capability requirements,
explicit environment transforms, resources/deadlines, idempotency, custody,
provider session/turn identity, and durable completion references.

Agent DSL v1 should cover definition identity/digest, instructions and profile
digests, model/capability constraints, tools, workspace/file scope, explicit
environment transforms, child-delegation and return obligations, retry/recovery,
resources, timeout, and observability policy.

Both DSLs require major/minor negotiation, bounded fields, stable enums,
unknown-field behavior, and golden mixed-version fixtures. Providers must attest
requested semantics or reject them clearly; they must never silently ignore a
system-prompt or tool safety override.

## Live-stream direction

The first multi-process implementation is a same-user lazy local broker:

```text
provider / PTY / Agent Bash capture
              |
       nonblocking publisher ring
              |
       lazy local broker
              |
      bounded subscriber queues
```

Required properties:

- publisher capture is `try_publish` and never blocks pipe drainage;
- random stream ID and publisher incarnation;
- monotonic per-stream sequence with explicit channel and gap frames;
- caller-owned replay cursor with at-least-once reconnect/deduplication;
- byte-bounded per-stream, aggregate, and per-subscriber queues;
- slow subscribers disconnect without affecting publishers;
- one-hour reap only after zero publishers, subscribers, registrations, handoff
  work, and recovery obligations;
- completed turns redirect to normal durable session files and the active tail is
  evicted after a short grace.

Provisional benchmark limits are 64 KiB/frame, 1 MiB/active stream, 64 MiB
aggregate payload, 256 KiB queued/connection, and a 128 MiB broker RSS soft
ceiling. They are hypotheses, not permanent constants.

Do not add a permanent high-rate output event journal. Agent Bash's existing
bounded log remains a custody/diagnostic fallback, not canonical structured
session history.

The first broker has no database and uses a no-broker execution fallback. A
permanently resident service, database index, terminal-as-HUD client model, and
active stream migration are later evidence-driven optimizations behind the same
protocol.

## Shared LSP and memory direction

A single LSP server can be shared by compatible agents and can save substantial
memory, but only when keyed by workspace snapshot, language, toolchain, config,
and overlay semantics. Request IDs, document versions, diagnostics, progress,
cancellation, and per-agent unsaved overlays must remain isolated. Clean or
read-only compatible worktrees are the safest first case. APV-8 owns this work.

The memory baseline that motivated the program found:

- 12 OpenCode process trees at roughly 24.73 GiB aggregate PSS;
- OpenCode root processes accounted for roughly 96% of that PSS, with one root
  near 9.73 GiB;
- four agents on one checkout produced seven separate LSP processes;
- disabling an unused Firecrawl MCP saved roughly 100-167 MiB and 5,587 input
  tokens in the measured comparison.

Reproduce and extend these observations with `provider-memory`; do not treat one
snapshot as a universal attribution. Measure complete process-tree PSS, keep
shared and private memory separate, freeze workloads, and use `gpt-luna-low` for
cross-provider comparisons.

## Infrastructure outage direction

Task agents report bounded typed evidence and continue until the control plane
instructs them to quiesce. They do not decide fleet scope or repair the same
infrastructure they depend on.

The durable incident progression is:

```text
observing -> correlating -> confirmed -> pausing -> paused
          -> repairing -> verifying -> resume_authorized
          -> resuming -> monitoring -> resolved
```

It also supports `false_positive`, `superseded`, and `manual_hold`. Recurrence or
failed proof returns to `pausing` with a strictly greater epoch. Lease expiry
stops new coordinator/repair effects but never resumes execution.

Pause is an admission-and-drain protocol. Every run, root/child spawn, wake,
resume, queued turn, tool/effect boundary, provider retry/rotation, and repair
launch checks the greatest applicable epoch. Offline roots remain fenced until
they reconcile. Transport receipt is not a semantic pause acknowledgement.

Do not classify signal 9 as OOM by itself. Runner and Agent Bash intentionally
send SIGKILL in timeout/cancel paths. OOM attribution needs correlated exact
process identity, leaf-cgroup membership and `memory.events.local` deltas, the
launch/exit window, and optionally an independent service-manager witness. When
that evidence is missing, the cause remains unknown.

The repair coordinator is a small OS-supervised process outside affected process
groups/cgroups with reserved resources, a low-rate durable ledger/ticket outbox,
bounded queues, fenced leases, restart limits, storm suppression, and a
resource-bounded repair lane. Ticket API failure must not block pause, repair, or
resume safety.

## Linear roadmap

All tickets below were verified in Linear at handoff. APV-28 through APV-36 are
Backlog and unassigned. The merged architecture PR is a planning resource on
APV-28; APV-28 is not complete.

### SDK and provider foundations

- APV-1 — pin provider/v1 schemas and cross-repository fixtures.
- APV-2 — extract provider-neutral transport/containment.
- APV-3 — define provider profile DSL and capability negotiation.
- APV-4 — centralize Agent Bash and future tool declarations.
- APV-28 — define Session DSL v1 and Agent DSL v1.
- APV-29 — define live-stream and infrastructure-control contracts v1; blocked
  by APV-28.
- APV-36 — end-to-end session/outage conformance; blocked by APV-32 and APV-35.

### Runtime and memory

- APV-5 — process-tree memory harness; Done.
- APV-6 — frozen `gpt-luna-low` cross-provider benchmark matrix.
- APV-7 — capability tiers and safe escalation.
- APV-8 — workspace-keyed shared LSP broker.
- APV-9 — shared MCP/tool-service evaluation.
- APV-10 — safe warm-runtime lifecycle and auto-update handoff.
- APV-11 — bounded hot/cold session bookkeeping and reaping.
- APV-30 — authoritative Runner session runtime; blocked by APV-28.
- APV-31 — bounded capture and lazy broker; blocked by APV-29 and APV-30.
- APV-32 — subscriptions and A/B broker drain; blocked by APV-31 and APV-10.
- APV-33 — causal process/OOM/pressure witnesses; blocked by APV-29.
- APV-34 — incident ledger/classifier/epoch fence; blocked by APV-30 and APV-33.
- APV-35 — isolated coordinator and verifier; blocked by APV-34.

### Provider work

- OpenCode: APV-12 through APV-15.
- Pi: APV-16 through APV-18.
- Codex: APV-19 through APV-21.
- Claude Code: APV-22 through APV-24.
- Direct API: APV-25 through APV-27.

Adapters consume shared SDK/runtime contracts and provide native
reproduction/canary fixtures. They do not reimplement common control behavior.

## Recommended execution sequence

1. Start APV-1 and APV-28 as coordinated foundation work. APV-1 freezes the
   existing provider/v1 boundary; APV-28 defines the higher-level DSL without
   replacing it.
2. Complete APV-2 through APV-4 and APV-29 so all adapters share one transport,
   profile/tool policy, and live/incident contract vocabulary.
3. Run APV-6 while provider integrations remain isolated. Use its frozen matrix
   to compare OpenCode, Pi, Codex, Claude Code, and later Direct API memory.
4. Complete APV-30 before any fleet pause or shared broker becomes authoritative.
   Reconcile Runner's overlapping lifecycle representations first.
5. Implement APV-31 and APV-33 in parallel after their contracts land.
6. Add subscription/A-B handling under APV-32 and incident fencing under APV-34.
7. Add the isolated coordinator/verifier under APV-35.
8. Run APV-36 fault injection and one-provider canary before wider rollout.
9. Adopt a permanently resident server only if measurements show that the lazy
   broker and existing supervisors cannot meet memory/latency/observability needs.

APV-8 shared LSP and APV-9 shared tool services may be prototyped against the
frozen APV-6 workload matrix, but they must not become provider-launch
dependencies before isolation and fallback are proven.

## Evidence and audit artifacts

The detailed design audit is outside this repository at:

`/home/nes/projects/agent-runner/.tmp/audit/session-runtime-control-plane-20260825/`

Important files:

- `current-surfaces.md` — shipped Runner/Agent Bash inventory and ownership.
- `stream-broker-design.md` — topology, protocol, lifecycle, update, and resource
  analysis.
- `outage-control.md` — incident classification, pause/resume, evidence, and
  recovery design.
- `synthesis.md` — reconciled architecture and ticket plan.

The earlier memory/provider audit is at:

`/home/nes/projects/agent-runner/.tmp/audit/agent-provider-platform-20260825/`

These `.tmp` reports are supporting evidence; the architecture file in this
repository and Linear are the durable program record.

## Validation already completed

For the architecture merge:

- `cargo fmt --all -- --check` passed.
- `cargo test --workspace` passed: six tests.
- `cargo clippy --workspace --all-targets --locked -- -D warnings` passed.
- GitHub PR #1 CI and merged-main CI passed.
- The APV-28 task worktree and local/remote branch were removed.
- SDK `main` and `origin/main` matched at `d0430f5` before this handoff edit.

No runtime source changed in that merge, so no production provider or `agents`
binary was rebuilt or installed. Only the existing `provider-memory` artifact is
installed from this SDK.

## Start-of-session checks

Run read-only checks before starting new work:

```bash
git -C ~/projects/agent-provider-sdk/trunk status --short --branch
git -C ~/projects/agent-provider-sdk/trunk fetch origin main
git -C ~/projects/agent-provider-sdk/trunk rev-parse HEAD origin/main
git -C ~/projects/agent-provider-sdk/trunk worktree list
env LINEAR_TEAM=APV linear issues --all --no-project
sha256sum ~/.local/bin/provider-memory
```

Before touching Runner or Agent Bash, inspect their current trunk and task
worktrees. Other active work has existed there; do not assume a dirty file belongs
to this program and do not discard it.

## Do not do

- Do not create another Agent Provider project for session service, streaming,
  HUD, broker, or outage automation.
- Do not replace the current provider/v1 contract with an incompatible second
  transport.
- Do not add an environment-variable allow list.
- Do not disable automatic updates or update a loaded runtime in place.
- Do not make optional live streaming a launch/completion dependency.
- Do not add a permanent high-rate output database or promise byte-exact history
  after the bounded live window.
- Do not use a numeric process/session cap as outage behavior.
- Do not treat a PID, handle, stream, or provider session as the logical agent.
- Do not treat SIGKILL as proof of OOM or a missing root as proof that children
  are dead.
- Do not let a stale resume clear a newer incident epoch.
- Do not let affected task agents recursively own infrastructure repair.
- Do not alter production routing while benchmarking experimental adapters.
- Do not merge work only into a local trunk; remote `main` is the delivery target.

Update this handoff when a contract version, ownership boundary, repository,
installed artifact, dependency edge, or rollout decision changes.
