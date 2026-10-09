# Resident session v1

`oulipoly.resident_session/v1` is a host-selected provider/v1 extension. It is
not a second host/provider protocol: selection and preparation use provider/v1
envelopes, and the resident endpoint speaks the ACP v2 draft subset that Agent
Runner's root supervisor already consumes (`schema-v2.0.0-alpha.7`). That
subset's vocabulary and a host client live in the contract crate's `acp`
module; `acp::resident` starts sessions and sends turns over a prepared
endpoint.

## Selection

A host offers version 1 in a `describe` request with
`host.env.OULIPOLY_HOST_RESIDENT_SESSION_V1=1` (exact value `1`). A provider
that supports it answers `capabilities.resident_session_v1: true`; otherwise it
omits the property. A host offers every version it supports with its own
selector and uses the highest one advertised; a provider never advertises a
version the request did not offer. Describe validation tolerates unknown future
advertisements for chooser intersection; selected v1 payload validation stays
strict. The `resident.prepare` request must carry the
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

The host helper `resident_session::template_from_policy` accepts only an
accepted evaluation with argv and representable prompt semantics. Non-null
stdin/prompt values must exactly echo the preparation input
(`model.inputs.prompt`, or `launch.prompt` if absent); resident turns replace
that prompt. Differing values or values without a preparation input are
refused. Environment and opaque model/settings values pass through.

`acp::resident::PreparedEndpoint` validates the fixed supported schema and
required declared operations. Its peer check requires a valid resident
schema declaration and dedup capability, not endpoint-process identity or
proof of operation implementation. The host associates the prepared argv
with its transport and keeps its admission and durable-record ordering.

## Endpoint

The host starts the endpoint as an ACP v2 harness on stdio (for Agent Runner's
root supervisor, a `HarnessSpec` with that argv and `"endpoint": "stdio"`). The
SDK's `agent_provider_execution::resident` documents the served semantics:
insertion acknowledgement on native consumption, `oulipoly.ai/parentMessageId`
and `oulipoly.ai/lastUserMessageId` attribution, the message-key dedup
contract, session-scoped cancel, settlement on close/connection end, and
resume-time reconciliation of interrupted turns. Native session selection is
bound at dispatch, after prior turns settle. Interrupted actor discharge is
independent of current template equality and never readmits the old input.
Readable interrupted evidence without an observed native session identity
reports `-32012` and blocks new input when native work cannot be excluded.
Valid complete launch custody without observed identity is blocked the same way
whether its receipt replays or the replay errors; the error remains the original
input's failure. A live turn that settles its own custody without an observed
identity blocks later input the same way and keeps its own result. A chosen
create id remains a candidate, not observed identity or permission to probe
create/resume. Identity is known only once an observed provider-session marker
has reached the session record: a record's identity, or a journal marker that
interrupted-journal recovery or a successful receipt replay delivers, still
recovers, but a complete-custody replay that errors before delivering events
cannot deliver the journal's marker, so the session reports `-32012` there.
Known duplicate insertion ACKs remain available. A justified pre-start refusal,
validated prepared custody without consumption, or the lifecycle's SDK-private
proof in a valid complete launch record (settled preparation, an adapter-settled
spawn failure, the gate's failed `exec` of the configured program) permits fresh
work, live or after recovery; actor discharge, insertion ACK, missing
consumption, a nonzero exit, complete custody, a wrapper's failed inner command
and a complete record without that proof do not prove no native effects. A
decided block is also kept on the settled input's own record and restored on
reopen, so a failed session-record write does not lose it; the turn then ends
`_oulipoly_turn_failed` with `native_session_record_failed`, keeping its known
native status and `complete` custody when the launch returned its result. No
path clears the block.
Unsettled custody cannot produce an ended record or successful close; close
releases its session lock and worker. Consumption evidence with a failed
insertion store is unknown (`-32011`), without a consumption ACK. Duplicate ACKs
attest the original key's insertion only, not the resubmission's bytes. `ResidentSessionMeta`,
`TurnStopReason` and `NativeTurnMeta` define the `_meta` payloads it emits.

Directory publication scopes every durable configuration/session/input/ACK claim:
the incoming root lineage must have been successfully published, and the SDK
publishes only the links it creates. A visible path or later successful
initialize/new/resume/ACK does not certify an earlier failed publication. These
preconditions are not runtime-enforced by the endpoint. Current per-call uses
fresh roots and discards failed publication; diagnostic keep is inspection.
Deliberate recovery must name its roots and lineage and establish its required
guarantee at that consumer boundary. Consumers unable to establish it use fresh
roots and carry prior input uncertainty as do-not-replay, rather than treating a
retry as durable recovery. No whole-lineage recovery machinery, receipt for
host-created administrative links or hardware-crash qualification is supplied.
This scopes documentation of the existing publication behavior; it changes no
resident wire payload, selector, ACK ordering or runtime operation.

## Host outcome evidence

The client retains rejected prompt error data and complete unrecognized update
payloads. `DeliveryOutcome::native_turn()` and `NativeTurn::from_report()`
validate only the supported native report schema. Raw `native_turn_report` on
idles/turn ends preserves absent versus null, invalid and unknown shapes without
turning them into custody claims. Turn-end reports belong to the covering
idle's tag; a later tag is not the earlier input's own custody.

Native launch completion and endpoint input publication are separate. A tagged
idle may be followed by `session_info_update` with
`_meta["oulipoly.ai/nativeTurn"].record_error` and `message_id`. On a rejected
prompt, failed final input publication is carried separately in error
`data.recordError` with those same fields, alongside `data.nativeTurn`.
These diagnostics remain endpoint reports, not host canonical-completion facts.
`AcpClient::receive_event` reads one subsequent record without sending a
request or consuming existing history; transport deadlines and the decision to
continue reading remain host-owned. No wait for tagged idle guarantees that
later contrary information has arrived or certifies durable completion.

Logical session identity, ancestry, admission, scheduling and delivery policy
remain the host's. A resident session id names a provider-native resident
session, not a second logical registry.

Changing selection, payload or endpoint semantics incompatibly requires a new
extension version; keep version-1 fixtures for older hosts.
