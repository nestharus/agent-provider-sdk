# Incident v1

`oulipoly.incident/v1` is the provider-neutral vocabulary of infrastructure
incident claims. Reporters, collectors, verifiers and the recovery
coordinator use it to exchange:

- **reports**: a reporter's bounded typed observation of a suspected
  infrastructure problem, with typed process/provider resource evidence and a
  claimed severity, scope and cause;
- **collector answers**: a report receipt, or a key conflict that carries both
  complete reports;
- **verifications**: a verifier's recovery checks for one incident epoch and
  scope, passing or failing;
- **recovery authorizations**: an issuer's claim that it authorized resuming
  input admission for a scope, resting on a complete passing verification of
  the same incident epoch.

It is a **companion** of [`session-control/v3`](../session-control/README.md),
not a successor or a second control vocabulary. Root authority, logical links
(`LogicalRef`), actor identity evidence (`ActorEvidence`), the disclosure
markers (`DisclosedText`, `DisclosedRef`, `MissingReason`), the host value and
key bounds, and the meaning of repetition (`Repetition`) are session control's
own definitions. [v1.schema.json](v1.schema.json) references them by
`../session-control/v3.schema.json#/$defs/…` and the `incident` module imports
them, so they keep one meaning and are not copied.

That schema **plus the normative semantic rules below** defines v1
conformance in every language. Raw JSON Schema validation alone is
insufficient. Raw Serde deserialization supplies representation only.

Status: source contract with no consumer. No host, collector, coordinator or
verifier produces or reads it yet. Adopting it, and relying on it for any
pause, repair or resume, needs its own consumer work and the lifecycle,
crash, ownership, cancellation and isolation evidence in this repository's
`AGENTS.md`.

## What it is and is not

Every record is a **claim**. Validation checks its shape and cross-field
meaning, including the minimum evidence a claimed cause needs and the
complete passing proof an authorization record must carry. It does not
establish:

- that the reporter told the truth, or that a claimed cause is the real one;
- that a verifier ran a check or that a check means what its subject names;
- that an issuer is the coordinator, holds the fence it names, or is current;
- that any hold, release, repair or other action may proceed or happened.

Deduplication, correlation across reports, classification, severity and
scope assignment, escalation, snapshotting, target expansion, broadcast,
fencing, leases, the incident state machine and every recovery effect stay
with Agent Runner and its coordinator. The SDK executes nothing, keeps no
ledger, registry, index or queue, applies no policy and stores nothing.

This is not a provider/v1 subcommand and not a second host/provider protocol:
providers neither speak nor advertise it, and provider/v1 envelopes are
unchanged. An adapter's native failure reaches a report only as a host's
`provider_condition` evidence.

**Pause and resume are session control.** The "pause/resume
acknowledgements" in the infrastructure-control ticket are session-control
`input_hold`/`input_release` acknowledgments with their admission-only
meaning. No incident record holds input, releases input, acknowledges a
transition, settles work, drains, or suspends native execution or effects.
A report never pauses anything, and an authorization never resumes anything.

**No output journal.** Reports carry bounded typed evidence and at most 512
characters of observer-only detail. They have no field for output bytes, and
raw live output never enters a report. Completed turns remain canonical in
normal session storage; reports are low-rate claims, not a transcript.

| Existing surface | What it stays |
| --- | --- |
| `session_control/v3` requests, acknowledgments and observations | Hold/release/cancel/close/recover intent, transition acknowledgment and settlement |
| `session_control/v3` `recover` | Same-root, same-incarnation owner re-attachment; never recovery authorization |
| `live_stream/v3` `ControlFact` and `live_unavailable` | Report-only live-output observation and diagnostics |
| `terminal_unavailable/v1` | Provider-side native-service unavailability on a provider result |
| provider/v1 process status and `TerminalSignal` | Provider/process outcome facts |

## Records

| Record | Claims | Does not claim |
| --- | --- | --- |
| `report` | A reporter's observation: subject, claimed severity/scope/cause, typed evidence, optional observer-only detail | Classification, confirmation, a hold, a pause or a request |
| `report_receipt` | A collector received the report; `durable` is its retention claim | Confirmation, classification, agreement with the claimed severity or scope, a hold or an authorization |
| `report_conflict` | A changed submission under a used key, answered with both complete reports | A refusal of the original, a retry or a second report |
| `verification` | A verifier's checks for one incident epoch and scope, which may fail | Authority, an authorization or an effect |
| `recovery_authorization` | An issuer authorized resuming input admission for a scope, on the embedded complete proof | A request, an acknowledgment, a settlement, a `recover`, a replay, a current fence, or any grant by existing |

Reporter, collector, verifier, coordinator, fence, incident, account,
provider, host and component values are opaque host values (1–256 printable
non-space ASCII characters) compared only for equality. A `provider` scope
names an opaque host grouping, never a hard-coded provider identity.

## Evidence

Each evidence item claims only its own fact. Process items name an
`os_process` actor; provider items, when they name an actor, a
`provider_session` actor. `exact` actor evidence must carry a present
reference.

| `type` | Claims | Does not claim |
| --- | --- | --- |
| `process_exit` | The actor exited with `exit_code` | Success, completion or settlement |
| `process_signal` | The actor ended by `signal` (1–64) | Who sent it or why: SIGKILL is also timeout and cancellation |
| `process_absent` | A probe found no process for the identity | An exit, a cause, or that any other actor ended |
| `cgroup_membership` | The actor was a member of this leaf cgroup | Memory pressure or a kill |
| `memory_event` | A `high`/`max`/`oom`/`oom_kill` counter delta for a cgroup over a `window`; `local` counts the cgroup only, `hierarchical` includes descendants | Which process was affected |
| `resource_sample` | `rss_bytes`, or a `memory`/`cpu`/`io` pressure `some avg10` in hundredths of a percent (0–10000) | A cause |
| `provider_condition` | A provider-neutral `auth`, `quota`, `rate_limited`, `service_unavailable`, `invalid_input`, `unsupported_capability` or `timeout` condition, as the host or adapter reported it | Infrastructure failure or scope |
| `component_witness` | A witness's `healthy`/`degraded`/`failed` claim about a shared component | Authority over the component |
| `withheld` | An item of the named type exists; its content is `redacted` or `missing` with a reason | Any negative: withheld is not absent, not zero and not healthy |

### Claimed cause

A report's `cause` is the reporter's claim. `unknown` is always admissible.
Each definite cause needs its minimum **observed** evidence in the same
report; `withheld` items never count:

| Cause | Minimum evidence |
| --- | --- |
| `exit` | a `process_exit` |
| `signal` | a `process_signal` |
| `provider_condition` | a `provider_condition` |
| `component_failure` | a `component_witness` with state `failed` |
| `oom_kill` | a `process_signal` 9 on an `exact` actor with a present reference; a `cgroup_membership` of **that same actor reference** in a present cgroup; and a `memory_event` `oom_kill` with `local` counting and delta ≥ 1 for **that same cgroup** |

So SIGKILL alone, RSS, a pressure spike, process absence, a hierarchical
counter, a legacy or incomplete identity, or a withheld counter each remain
`cause: unknown` (or `signal` for SIGKILL). Meeting the minimum makes the claim
admissible; it does not make it true. The contract does not compare the
counter's window with the exit time: that correlation, a service-manager
witness and attribution itself are the producer's and the classifier's.

## Severity, scope and selectors

`severity` is ordered `notice` < `degraded` < `critical`. It labels a claim;
it is not a classification, escalation or policy.

`scope` is what a claim is about:

| `level` | Names |
| --- | --- |
| `logical` | A session-control `LogicalRef` (`root`, optional `child`, `work`, `input`) |
| `account`, `provider`, `host`, `component` | An opaque host grouping by `ref` |
| `fleet` | Everything |

A report's `logical` scope names the report subject's own root. A wider
problem uses a non-logical level.

`contains(outer, inner)` is known only where the contract can know it:

- `fleet` contains everything; nothing else contains `fleet`;
- a logical scope contains another of the same root when each `child`,
  `work` and `input` it names is equal in the other (a root contains its
  children; a child does not contain its root, a sibling, or root-level work);
- within one opaque level, equal references contain each other and different
  references do not;
- any other cross-level relation, such as an account over a root, is
  `undetermined`: it needs host topology the SDK does not have.

A `Selector {at_least, scopes}` (1–16 scopes) reads reports: `contained`
when the claimed severity is at least `at_least` and one scope contains the
claimed scope; otherwise `undetermined` if some relation needs topology, else
`not_contained`. A selector is a reading filter. It is not a subscription,
a broadcast, target expansion or authority.

## Verification and recovery authorization

A `verification` names its `verifier`, `incident {incident, epoch ≥ 1}`,
`scope` and 1–16 `checks`. Each check names its `check` kind, its `subject`
(a `DisclosedRef`) and its `result`: `passed`, `failed`, `not_run` or
`unknown`. A passed `artifact_digest` check carries its `sha256:` digest; a
failed one may carry the observed digest; no other check carries one.

`Verification::proof()` reads the checks:

- **`failed`** if any check failed. A failure is not outweighed by a pass of
  the same kind.
- Otherwise **`complete`** if each of `reproduction`, `canary`,
  `artifact_digest`, `schema_agreement` and `custody` has a passed check with
  a known subject. A `redacted` subject is known and withheld; a `missing`
  subject is not known.
- Otherwise **`incomplete`**, listing the unsatisfied kinds: absent,
  `not_run`, `unknown` or a missing subject.

A `recovery_authorization` names its `authorization_key`, its `issuer
{coordinator, fence}`, the `incident` epoch, the `scope` and the embedded
`proof` verification. It is admissible only when the proof names the same
incident and epoch, the proof's scope contains the authorization's scope, and
the proof reads `complete`. A failing, incomplete, other-epoch, wider or
topology-dependent authorization is inadmissible.

`RecoveryAuthorization::covers(request)` relates the claim to one
session-control request:

| Coverage | When |
| --- | --- |
| `covered` | An `input_release` whose scope lies within the authorization's scope |
| `operation_not_covered` | Any other operation: `input_hold`, `cancel`, `close` and `recover` |
| `outside_scope` | A release outside the scope, including a root-level release under a child authorization |
| `undetermined` | The scope relation needs host topology |

`covered` is a reading of two claims, not permission. It does not check the
issuer's fence or currency, and it does not issue, admit or acknowledge the
release, which travels on the session-control claim ladder. A **greater epoch
of the same incident supersedes** a lesser one (`IncidentRef::supersedes`,
`RecoveryAuthorization::superseded_by`); a stale authorization must not clear a
newer epoch's hold. The contract states this but cannot enforce it, and a
superseded record still reads `covered`: the host checks currency and fencing.

## Repetition and idempotency

Repetition reuses session control's `Repetition` meaning:

- reports are keyed by `(reporter, report_key)`;
- authorizations by `(issuer.coordinator, incident.incident, authorization_key)`.

`same_request` is an identical record: it counts once and changes nothing.
`key_conflict` is the same key with any different content, including a
changed severity, evidence, observation time, fence or epoch: never a retry
and never a new record. A re-issue at a greater epoch needs a new key.
`distinct` is another key scope.

A collector answers a changed report submission with `report_conflict`,
carrying both complete reports. Both must be admissible and classify as
`key_conflict`. `ReportConflict::answer_to(submitted)` checks the caller's
exact submission. The collector must preserve the original and apply no
effect of the changed submission; the SDK stores or enforces neither.
`ReportReceipt::answers(report)` correlates a receipt with its report's key
scope; a receipt carries no report content.

## Selection

Peers select by a bounded advertisement (at most 32 entries and 16 KiB)
mapping a protocol identifier to its offer, not through provider `describe`.
A CLI banner, binary, package or source revision plays no part.

```json
{"oulipoly.incident/v1": {
  "records": ["report", "report_receipt", "report_conflict", "verification", "recovery_authorization"],
  "evidence": ["process_exit", "process_signal", "process_absent", "cgroup_membership",
               "memory_event", "resource_sample", "provider_condition", "component_witness"]}}
```

- Unknown entries, including other versions and other families, are ignored.
  The v1 entry must be a strict `Offer` naming at least one record kind; a
  malformed entry is `invalid_advertisement`, not skipped.
- The selection is the common record kinds and evidence types. No v1 entry is
  `no_common_version`; no common record kind is `no_common_capability`. The
  common evidence set may be empty, for example for an authorization reader.
- `Record::agree(selected)` refuses an unselected record kind, and a report
  or conflict carrying an unselected evidence type. A `withheld` item counts as
  the type it names.

Incident v1 is optional. Offering it makes nothing mandatory for a peer that
does not, and its absence selects nothing.

## Diagnostics

Contract refusals are `incident_unavailable` diagnostics with session
control's reasons: `no_common_version`, `invalid_advertisement`,
`no_common_capability`, `invalid_record` or `protocol_violation`. They are
observer-only. An absent or incompatible incident capability disables incident
records only. It is not a control diagnostic, a hold, a provider error
response, a launch event, provider launch unavailability or a completion
outcome, and the SDK has no conversion between them. SDK-generated details use
schema keywords or fixed text and do not echo submitted values or property
names. `IncidentUnavailable::new` bounds caller detail to 512 characters
without sanitizing it.

## Normative semantic rules

| Operation | Required semantic checks / result |
| --- | --- |
| Record admission | Record lines are at most 65536 UTF-8 bytes before parsing. Evidence actors have the kind their type names; `exact` actor evidence has a present reference. A pressure sample is at most 10000. A memory event window does not end before it starts. A report's `logical` scope has the subject's root. A definite cause meets its minimum observed evidence. A conflict embeds admissible reports that classify as `key_conflict`. A digest appears only on a passed (required) or failed (optional) `artifact_digest` check. An authorization's proof is admissible, names the same incident and epoch, has a scope that contains the authorization's, and reads `complete`. |
| Proof | `failed`, then `complete`, then `incomplete`, as above. |
| Containment | As above; cross-level relations other than `fleet` are `undetermined`. |
| Selector | Severity threshold, then scope containment, as above. |
| Coverage | The authorization is admissible; the request is an admissible session-control request; only `input_release` can be covered; scope by containment. |
| Selection | Validate the local offer, the advertisement bound and shape, and the strict v1 entry; ignore unknown entries; intersect record kinds and evidence types; at least one record kind is common. |
| Agreement | The selected protocol is v1 and its offer is structurally valid; the record kind is selected; every report evidence type is selected. |
| Repetition | Classify by key scope, then exact content, as above. |

## Bounds and redaction

| Bound | Value |
| --- | --- |
| Record line, checked before parsing | 65536 bytes |
| Host values | 1–256 printable non-space ASCII characters |
| Report and authorization keys | 1–128 printable non-space ASCII characters |
| Detail text | 1–512 characters |
| Evidence items per report | 16 |
| Checks per verification | 1–16 |
| Selector scopes | 1–16 |
| Signal | 1–64 |
| Pressure sample | 0–10000 hundredths of a percent |
| Advertisement | 32 entries, 16384 bytes |
| Diagnostic detail | 512 characters |

A maximal report conflict (two reports with sixteen of the widest evidence
items, widest host values and four-byte detail) fits the line bound. Detail,
cgroup, component and check-subject references use session control's
disclosure markers: present, `redacted` or `missing` with a reason. A
redaction carries no remnant. Bounds are not sanitization: present values pass
unchanged, and the producer owns what it discloses.

## Limits

The golden vectors in `tests/fixtures/incident/v1.json` and their tests
establish, over claims:

- classified structural acceptance versus normative semantic admission;
- the minimum evidence of each claimed cause, including that SIGKILL alone,
  hierarchical counters, another process's membership, another cgroup's
  counter, a zero delta, a legacy identity and withheld evidence do not
  support `oom_kill`;
- withheld versus observed evidence, and redacted versus missing;
- proof readings, and authorization admission only on complete proof of the
  same epoch within the proof's scope;
- authorization coverage of releases only, never hold, cancel, close or
  `recover`;
- containment and selector readings, including undetermined topology;
- selection, agreement, report and authorization repetition, conflict
  answers, bounds and diagnostics;
- separation from session-control records, control diagnostics and provider
  launch events and responses.

They do not establish:

- that any reporter, verifier or issuer tells the truth or holds authority;
- correct classification, deduplication, escalation or target expansion;
- that a host checks fences and epochs, enforces a hold or performs a release;
- any producer, collector, coordinator or consumer, or runtime behaviour.
