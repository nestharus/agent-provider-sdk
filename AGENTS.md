# Agent Provider SDK — Agent Entry Point

## Purpose

This repository owns the provider-neutral SDK and profile DSL used by external
agent-provider binaries. It evolves the versioned `oulipoly.provider/v1`
out-of-process contract already consumed by Agent Runner; it must not introduce
a second, incompatible host/provider protocol.

## Repository layout

The checkout lives at `~/projects/agent-provider-sdk/trunk`. Isolated task
branches belong in `~/projects/agent-provider-sdk/worktrees/<ticket-or-task>`.
Keep `trunk` on `main` as the clean integration checkout. Do implementation work
in a worktree, merge and push the verified result to remote `main`, then remove
the task worktree and branch when the work is complete.

## Design boundaries

- Keep terminal-specific argv, authentication, quota, transcript, and native
  state formats in provider adapters.
- Put versioned DTOs, NDJSON framing, subprocess containment, request custody,
  durable state helpers, profile validation, and capability negotiation here
  only when their semantics are provider-neutral.
- Define provider-neutral Session and Agent DSLs here, but keep logical runtime
  authority, ancestry, admission, scheduling, and fleet policy in Agent Runner.
- Define live-event, incident-evidence, and control DTOs here. Implement capture
  and process custody in Agent Bash/Runner, and implement incident classification
  and broadcast expansion in Runner rather than provider adapters.
- Keep completed turns canonical in normal session storage. Live output is a
  bounded, optional observability plane and must never become a launch,
  terminalization, or completion dependency.
- Treat system-prompt and tool overrides as validated semantic capabilities.
  A provider must report support or reject a profile clearly; it must never
  silently ignore a requested safety boundary.
- Preserve environment variables unless a contract explicitly transforms one.
  Do not use an allow list for a provider process environment.
- Keep credentials, tokens, account state, and machine-specific config out of
  source control and test output.

## Compatibility and tests

- Version wire-format changes and retain fixtures for older supported hosts.
- Prefer deterministic unit and contract tests that do not require a live model.
- Live provider smoke tests must use an explicitly low-cost model and must not
  mutate user auth or provider session state unexpectedly.
- Any background-server or shared-resource feature needs lifecycle, crash,
  ownership, cancellation, isolation, and auto-update tests before adoption.
- Shared runtimes use versioned side-by-side generations and drain old work.
  Optional observability incompatibility must degrade to a clear diagnostic,
  never to provider or Agent Bash unavailability.

## Documentation

Update `README.md` when the public SDK surface, profile DSL, supported provider
capabilities, or validation guarantees change.
