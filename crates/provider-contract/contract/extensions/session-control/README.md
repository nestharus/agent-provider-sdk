# Session control v1

`oulipoly.session_control/v1` is the provider-neutral vocabulary of
session/root control claims. A requester and the existing root owner use it to
exchange:

- a control request and its idempotency key;
- transport receipt, admission, semantic transition acknowledgment or refusal,
  and the request's outcome, each as a separate claim;
- settlement observations about logical work, with actor identity evidence
  attached.

It is the one control vocabulary for session control (the Session DSL's
control half) and infrastructure control. Hold, acknowledgment, refusal and
idempotency have one meaning here; no parallel control vocabulary defines
them differently.

[v1.schema.json](v1.schema.json) defines structural validation. That schema
**plus the normative semantic rules below** defines v1 conformance in every
language. Raw JSON Schema validation alone is insufficient. `session_control`
implements both layers and the context-dependent selection, agreement,
repetition, trace and settlement-reading operations. Raw Serde deserialization
supplies representation only.

Status: defined and unadopted. No requester, root owner or host uses this
contract yet. The root discovery/control face that adopts it, and durable
control realization, belong to later Runner work. That later work is what
establishes runtime behaviour.

## What it is and is not

Every record is a **claim**. Validation checks its shape, its cross-field
meaning and the order of claims answering one request. It does not establish:

- that the producer told the truth;
- that the requester was authenticated or authorized;
- that a hold was enforced, survives owner death or is durable;
- that any actor is in custody or any process is in the state reported.

Root authority, requester attestation, generation fencing, durable control
intent, admission, scheduling and process custody stay with Agent Runner. The
SDK executes no command, keeps no registry, ledger or queue, and stores
nothing.

This is not a provider/v1 subcommand and not a second host/provider protocol:
providers neither speak nor advertise it, and provider/v1 envelopes are
unchanged. A `resident_session/v1` session id names a provider-native resident
session; it can appear here only as `provider_session` actor evidence, never as
a logical root, scope or authority.

| Existing surface | What it stays |
| --- | --- |
| provider/v1 process status and `TerminalSignal` | Provider/process outcome facts |
| `terminal_unavailable/v1` | Provider-side native-service unavailability |
| `live_stream/v1` `ControlFact` | Report-only live-output observation; never a command or acknowledgment |
| `resident_session/v1` ACP insertion acknowledgment | The endpoint's own insertion evidence, which a host may report here as an `insertion` observation |

Contract refusals are `control_unavailable` diagnostics: no common version, a
malformed advertisement, no common capability, a malformed record or a
protocol violation. An absent or incompatible control capability disables
control through this vocabulary only. It is not a provider error response, a
launch event, provider launch unavailability or a completion outcome, and the
SDK has no conversion between them.

## The input hold

v1 defines one operation pair:

- **`input_hold`** holds admission of new input at a logical scope (a root,
  optionally one logical child).
- **`input_release`** clears that hold through the same root authority.

The hold is about admission and input only. Running provider turns and tools
may continue. An acknowledged hold (`to: input_held`) does not claim that
work is paused, drained, stopped or at a safe boundary, and no record reports
running work as part of an acknowledgment. Running work is reported separately
through settlement observations.

Drain, close, cancel and physical execution pause are different operations.
They are not defined in v1, and none of them may be expressed as an input
hold. A drained-to-safe-boundary claim (such as the architecture document's
`paused_safe`) is a different claim from an input hold and must not share its
acknowledgment. Additional operations join this same claim ladder in a later
version, not a separate vocabulary.

Ticket mapping: the "pause/resume acknowledgements" named in the session and
infrastructure-control tickets are realized here as input hold/release
acknowledgments with this admission-only meaning.

## Selection

Peers select a version by advertisement, not through provider `describe`. A
CLI banner, binary, package or source revision plays no part. An advertisement
is a bounded JSON object (at most 32 entries and 16 KiB) mapping a protocol
identifier to its offer:

```json
{"oulipoly.session_control/v1": {"operations": ["input_hold", "input_release"],
                                  "facts": ["insertion", "tagged_end",
                                            "logical_settlement", "physical_custody"]}}
```

- **Unknown entries** (other families and newer versions) are ignored.
- **The v1 entry** must be a strict `Offer`; a malformed entry is refused
  (`invalid_advertisement`), not skipped.
- **The selection** is the common operations and the common fact types. No
  common operation is `no_common_capability`; no v1 entry is
  `no_common_version`. The common fact set may be empty.
- **Agreement.** `Request::agree` and `Observation::agree` refuse an
  unselected operation or fact type with `no_common_capability`.

## Addressing and correlation

- **`Authority {root, owner, generation, incarnation}`** is the existing Runner
  root authority. A request names the authority it addresses (`addressed`).
  Admissions, acknowledgments and refusals name the authority that answered
  (`responder`). The values are opaque host values compared only for equality.
  Carrying them makes a stale answer detectable; it does not fence anything.
- **Logical links.** `ControlScope {root, child?}` is what a control applies
  to; `LogicalRef {root, child?, work?, input?}` is what an observation is
  about. They are logical identities only.
- **Actor evidence.** OS process identity, Agent Bash handle, provider-native
  session and live stream are `ActorEvidence {actor, exactness, ref}` attached
  to an observation. `exactness` is `exact`, `legacy` or `incomplete` as the
  producer claims. Actor evidence is never a logical key, scope or authority,
  and the SDK does not define correlation across these domains.
- **Correlation.** Every response repeats the request's `request_key`,
  `requester` and `addressed` authority exactly.

## The claim ladder

| Record | Claims | Does not claim |
| --- | --- | --- |
| `request` | Requester intent: operation, scope, addressed authority, optional reason | Receipt, admission or effect |
| `receipt` | The request reached a queue or endpoint; `durable` is the receiver's retention claim | Admission, acknowledgment, execution or outcome |
| `admission` | The addressed authority admitted the request for its transition | That the transition happened |
| `acknowledgment` | The input hold state moved `from` → `to` at the addressed authority | Anything about running work |
| `refusal` | Explicit refusal at `admission` or `transition`, with a reason | — a refusal is an outcome, not an absent acknowledgment |
| `outcome` | What is finally known: `acknowledged`, `refused` or `unknown` with an uncertainty reason | — `unknown` never erases an earlier acknowledgment or refusal |
| `observation` | One settlement fact about a logical link, with actor evidence | Any control, request, admission or acknowledgment |

Refusal reasons: admission stage `stale_authority`, `key_conflict`,
`unsupported_operation`, `not_permitted`, `unknown_scope`; transition stage
`already_terminal`, `transition_failed`. The reasons name the root owner's
decision. The SDK defines no authorization policy.

Uncertainty reasons: `authority_changed`, `transport_lost`,
`evidence_unavailable`.

## Settlement facts

Four fact types stay distinct:

| Fact | States |
| --- | --- |
| `insertion` | `acknowledged`, `not_inserted`, `uncertain` |
| `tagged_end` | `observed`, `absent` |
| `logical_settlement` | `settled`, `owed` |
| `physical_custody` | `live`, `exited_wait_pending`, `exited_waited`, `unsettled` |

Every fact may also be `missing` (with `missing_reason`: `not_captured`,
`access_denied`, `unsupported`, `not_applicable`) or `redacted`. Neither is a
negative: missing insertion evidence is not `not_inserted`, and a missing
tagged end is not `absent`.

`read_settlement` reads the observations of one exact logical subject and
returns the four facts separately plus a logical reading derived from
insertion, tagged end and logical debt only:

- `settled` needs insertion `acknowledged`, tagged end `observed` and logical
  settlement `settled`;
- `owed` when debt is reported owed, or insertion was acknowledged with a known
  `absent` tagged end;
- `not_inserted` when insertion is positively `not_inserted` with no observed
  tagged end and no owed debt;
- `unknown` otherwise, including missing, redacted, uncertain, contradictory or
  conflicting reports.

Physical custody never enters the logical reading: an observed exit, a
completed wait or a closed peer is not logical settlement. Reporter authority
is provenance, not a filter, so an earlier generation's acknowledgment is not
erased when a successor reports later; disagreeing reports read as
`conflicting`, not as the latest one.

## Repetition and idempotency

A request's key scope is `(requester, addressed root, request_key)`.
`classify_repetition` gives:

- **`same_request`**: same key scope and identical content. It is the same
  request. A conforming root owner answers it with the same claims and makes
  no second transition. That is a producer obligation the SDK cannot verify.
- **`key_conflict`**: same key scope, different content — including the same
  key re-addressed to a successor generation, or a changed reason or
  redaction. It must be refused with `key_conflict`, never treated as a retry.
  After an authority change, read the old request's outcome and send a new key.
- **`distinct`**: another key scope; an independent request.

State repetition is separate from key repetition. A new hold while the input
is already held is acknowledged `from: input_held` → `to: input_held`; a
release while open is acknowledged `input_open` → `input_open`.

`RequestTrace` treats an identical redelivery of an already accepted record
as `duplicate`, with no new meaning or state change.

## Normative semantic rules

| Operation | Required semantic checks / result |
| --- | --- |
| Record admission | Record lines are at most 16384 UTF-8 bytes before parsing. A request's `scope.root` equals its `addressed.root`. Admission and acknowledgment `responder` equals `addressed`. An acknowledgment's `to` is `input_held` for `input_hold` and `input_open` for `input_release`. A refusal's stage matches its reason. A `stale_authority` refusal's responder, when present, differs from `addressed` with the same root; any other refusal's responder, when present, equals `addressed`. `exact` actor evidence has a present reference. |
| Selection | Validate the local offer, the advertisement bound and shape, and the strict v1 entry; ignore unknown entries; intersect operations (nonempty) and facts. |
| Agreement | The selected protocol is v1 and its offer is structurally valid; the request's operation or the observation's fact type is selected. |
| Repetition | Classify by key scope then exact content, as above. |
| Trace | A trace starts from an admitted request. Every record must be admissible and correlate exactly; observations are refused. Identical redelivery is `duplicate`. Nothing new follows the outcome. Receipts may arrive at any time before the outcome (at most 8 distinct per trace). Admission at most once and not after a refusal. Acknowledgment only after admission, for the requested operation, at most once, never with a refusal. Admission-stage refusal only before admission; transition-stage refusal only after admission; never after an acknowledgment. Outcome `acknowledged` needs an acknowledgment, `refused` needs a refusal, `unknown` is always admissible and retains earlier claims. Refused records leave the trace unchanged. |
| Settlement reading | As above; physical custody does not affect the logical reading. |

Timestamps are bounded host observations, not ordered clocks or causal
evidence.

## Bounds and redaction

| Bound | Value |
| --- | --- |
| Record line, checked before parsing | 16384 bytes |
| Host values | 1–256 printable non-space ASCII characters |
| Request key | 1–128 printable non-space ASCII characters |
| Reason/detail text | 1–512 characters |
| Actor evidence per observation | 16 |
| Advertisement | 32 entries, 16384 bytes |
| Diagnostic detail | 512 characters |

Free text and actor references are `DisclosedText` / `DisclosedRef`: present,
`redacted` or `missing` with a reason. A redacted value carries no remnant of
the withheld value. The bounds are not sanitization: present text and
references are passed unchanged, and the producer owns what it discloses.
SDK-generated validation details use schema keywords or fixed diagnostic text
and do not echo submitted values, including arbitrary property names.
`ControlUnavailable::new` bounds caller detail to 512 characters without
sanitizing it.

## Limits

The golden vectors in `tests/fixtures/session_control/v1.json` and their tests
establish the following, over claims:

- classified structural acceptance versus normative semantic admission;
- the bounds and redaction states above;
- selection, agreement and its diagnostics;
- repetition classification;
- claim-ladder order, correlation, duplicates and non-contradiction;
- the separation of insertion, tagged end, logical debt and physical custody;
- separation from provider launch outcomes and error responses.

They do not establish any of the following:

- that a root owner enforces a hold, keeps it durable or answers duplicates
  identically;
- that any requester is authorized or any claim is true;
- adoption by the root discovery/control face, or runtime behaviour;
- drain, close, cancel or physical pause semantics, which v1 does not define.

Incident reports, typed resource evidence, severity and scope, and recovery
authorization or proof are not defined here. They belong to a later bounded
contract slice with the incident ledger, witness and coordinator work.
