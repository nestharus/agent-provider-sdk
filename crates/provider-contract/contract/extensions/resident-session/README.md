# Resident session v1

`oulipoly.resident_session/v1` is a host-selected provider/v1 extension. It is
not a second host/provider protocol: selection and preparation use provider/v1
envelopes, and the resident endpoint speaks the ACP v2 draft subset that Agent
Runner's root supervisor already consumes (`schema-v2.0.0-alpha.7`).

## Selection

A host offers version 1 in a `describe` request with
`host.env.OULIPOLY_HOST_RESIDENT_SESSION_V1=1` (exact value `1`). A provider
that supports it answers `capabilities.resident_session_v1: true`; otherwise it
omits the property. A host offers every version it supports with its own
selector and uses the highest one advertised; a provider never advertises a
version the request did not offer, so a closed v1 describe schema never
receives an unknown capability. The `resident.prepare` request must carry the
same selector.

## `resident.prepare`

Params ([v1.schema.json](v1.schema.json) `ResidentPrepareParams`): `protocol`
and `launch`, the policy-evaluated provider/v1 launch inputs every turn reuses
(`settings_id`, `mode`, `model`, `argv`, optional `env`). A turn adds its
working directory (the session's `cwd`), its prompt, its native session and a
launch-output request. Providers check the template as a launch would and
refuse what they cannot honour; they never silently drop a requested boundary.

Result (`ResidentPrepareResult`): `invocation.args` the host appends to the
same registered provider executable it invoked (the provider never names its
own executable, so a compatible replacement at that path serves later
relaunches), `invocation.endpoint: "stdio"`, the `acp` subset (protocol 2,
schema tag, dedup contract 1), the recorded configuration's SHA-256 and the
served `operations`. Providers record the configuration durably and
content-addressed; an endpoint refuses a record whose content no longer
matches its digest.

## Endpoint

The host starts the endpoint as an ACP v2 harness on stdio (for Agent Runner's
root supervisor, a `HarnessSpec` with that argv and `"endpoint": "stdio"`). The
SDK's `agent_provider_execution::resident` documents the served semantics:
insertion acknowledgement on native consumption, `oulipoly.ai/parentMessageId`
and `oulipoly.ai/lastUserMessageId` attribution, the message-key dedup
contract, session-scoped cancel, settlement on close/connection end, and
resume-time reconciliation of interrupted turns. `ResidentSessionMeta`,
`TurnStopReason` and `NativeTurnMeta` define the `_meta` payloads it emits.

Logical session identity, ancestry, admission, scheduling and delivery policy
remain the host's. A resident session id names a provider-native resident
session, not a second logical registry.

Changing selection, payload or endpoint semantics incompatibly requires a new
extension version; keep version-1 fixtures for older hosts.
