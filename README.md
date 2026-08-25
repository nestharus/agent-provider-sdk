# Agent Provider SDK

Shared infrastructure for external binaries that implement the
`oulipoly.provider/v1` Agent Runner contract.

The SDK is intended to centralize the parts every terminal provider needs:

- versioned request/response and launch-event types;
- JSON/NDJSON process transport;
- subprocess containment, custody, cancellation, and durable state helpers;
- a provider-profile DSL with capability negotiation and validation;
- central system-prompt and tool-bridge policy, including the Agent Bash bridge;
- conformance fixtures and a reproducible memory benchmark harness.

Terminal-specific behavior remains in provider repositories. OpenCode, Pi,
Codex, and Claude Code adapters continue to own their native command lines,
authentication, account/config roots, quota APIs, and session formats.

## Status

Bootstrap phase. The first work units pin the current external-provider
contract, add conformance fixtures, and establish a process-tree memory baseline
before reusable modules are extracted from `agent-runner-opencode`.

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
