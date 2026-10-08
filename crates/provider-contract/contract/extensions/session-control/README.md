# Session control v3

`oulipoly.session_control/v3` is the provider-neutral vocabulary of root
control claims. A requester and the existing root owner use it for the whole
root control face:

- **discovery**: descriptive addresses of the requester's existing roots;
- **inspection**: an authority's current knowledge of a scope's input hold,
  lifecycle state and pending control intents;
- **control requests** — input hold/release, same-incarnation recover, cancel
  and close — answered on one claim ladder: transport receipt, admission,
  semantic transition acknowledgment or refusal, and what is known of the
  outcome, each a separate claim;
- **settlement observations** about logical work, with actor identity
  evidence attached.

It is the one control vocabulary for session control (the Session DSL's
control half) and infrastructure control. Hold, acknowledgment, refusal,
idempotency, refinement and settlement have one meaning here; no parallel
control vocabulary defines them differently.

[v3.schema.json](v3.schema.json) defines structural validation. That schema
**plus the normative semantic rules below** defines v3 conformance in every
language. Raw JSON Schema validation alone is insufficient. `session_control`
implements both layers and the context-dependent selection, agreement,
repetition, trace, current-state relation and settlement-reading operations.
Raw Serde deserialization supplies representation only.

Status: source contract with one scoped, unqualified source consumer. v3
replaces v2's incompatible terminal and conflict meanings; no v1/v2
representation is retained. The original R3 consumer at `c88e567e` used v2,
including an emitted key-conflict refusal sharing a final original trace.
Agent Runner's root owner has since moved to v3 at root scope in source and
refuses child-scoped requests. That is source uptake only: this SDK has not
qualified it, native operation, an account-level control broker, or any
provider adapter, and none of those is required to consume this contract. A
v2-only peer selects nothing (`no_common_version`), disabling control only.

## What it is and is not

Every record is a **claim**. Validation checks its shape, its cross-field
meaning and the order of claims answering one request. It does not establish:

- that the producer told the truth;
- that the requester was authenticated or authorized;
- that a control was enforced, survives owner death or is durable;
- that a lineage is warranted, or that any actor is in custody or any process
  is in the state reported.

Root authority, requester attestation, generation fencing, lineage, durable
control intent, admission, scheduling, capacity and process custody stay with
Agent Runner. The SDK executes no command, keeps no registry, index, ledger or
queue, applies no authorization policy and stores nothing.

This is not a provider/v1 subcommand and not a second host/provider protocol:
providers neither speak nor advertise it, and provider/v1 envelopes are
unchanged. A `resident_session/v1` session id names a provider-native resident
session; it can appear here only as `provider_session` actor evidence, never as
a logical root, scope or authority.

| Existing surface | What it stays |
| --- | --- |
| provider/v1 process status and `TerminalSignal` | Provider/process outcome facts |
| `terminal_unavailable/v1` | Provider-side native-service unavailability |
| `live_stream/v3` `ControlFact` | Report-only live-output observation; never a command or acknowledgment |
| `resident_session/v1` ACP insertion acknowledgment | The endpoint's own insertion evidence, which a host may report here as an `insertion` observation |

Contract refusals are `control_unavailable` diagnostics: no common version, a
malformed advertisement, no common capability, a malformed record or a
protocol violation. An absent or incompatible control capability disables
control through this vocabulary only. It is not a provider error response, a
launch event, provider launch unavailability or a completion outcome, and the
SDK has no conversion between them.

## Operations

Each operation has its own transition. None is drain or physical execution
pause, and none may be expressed as another.

| Operation | Acknowledged transition | Answered by |
| --- | --- | --- |
| `input_hold` | input → `input_held` | the addressed authority |
| `input_release` | input → `input_open` | the addressed authority |
| `close` | lifecycle → `closing` | the addressed authority |
| `cancel` | lifecycle → `cancelling` | the addressed authority |
| `recover` | attachment → `attached` | a successor owner of the same root **and the same incarnation** |

- **Input hold** is about admission and input only. Running provider turns
  and tools may continue. `input_held` does not claim that work is paused,
  drained, stopped or at a safe boundary. A drained-to-safe-boundary claim
  (such as the architecture document's `paused_safe`) is a different claim.
- **Close** refuses new input for the scope; admitted work then ends and its
  harnesses stop. **Cancel** cancels the scope's work and outranks a close in
  progress. `closing` and `cancelling` are acknowledged requests, not ended
  work: ended work is read from settlement observations.
- **Recover** continues the surviving recorded incarnation under a new owner
  generation. It never starts a new incarnation: an answer from another
  incarnation is invalid, and finding no survivor is the transition refusal
  `root_absent`. An owner that still holds the root makes it `owner_live`.
  After recovery, new control intents address the successor. Already admitted
  durable intent retains its original identity and can be fulfilled there.

The acknowledgment's `from` is the prior state in the operation's domain, or
`unknown`. Lifecycle transitions cannot regress: a close from `cancelling`
is invalid,
including in a successor fulfillment. Admission only admits an attempt; it
does not claim a transition and does not defeat cancel precedence.

State repetition is separate from key repetition: a hold while
already held is acknowledged `input_held` → `input_held`.

Ticket mapping: the "pause/resume acknowledgements" named in the session and
infrastructure-control tickets are input hold/release acknowledgments with this
admission-only meaning.

## Selection

Peers select a version by advertisement, not through provider `describe`. A
CLI banner, binary, package or source revision plays no part. An advertisement
is a bounded JSON object (at most 32 entries and 16 KiB) mapping a protocol
identifier to its offer:

```json
{"oulipoly.session_control/v3": {
  "operations": ["input_hold", "input_release", "recover", "cancel", "close"],
  "reports": ["discovery", "inspection"],
  "facts": ["insertion", "tagged_end", "logical_settlement", "physical_custody"]}}
```

- **Unknown entries** (other families, the old v1 and newer versions) are
  ignored.
- **The v3 entry** must be a strict `Offer`; a malformed entry is refused
  (`invalid_advertisement`), not skipped. An offer names at least one
  operation or report, and **offers `input_hold` only together with
  `input_release`**, so no selection ever carries a hold without its release.
- **The selection** is the common operations, reports and fact types. With
  no common operation and no common report it is `no_common_capability`;
  with no v3 entry `no_common_version`. A read-only peer may select only
  discovery and inspection; the common fact set may be empty.
- **Agreement.** `Request::agree`, `Observation::agree`, `RootEntry::agree`
  and `ControlState::agree` refuse an unselected operation, fact type or
  report with `no_common_capability`.

## Addressing, discovery and correlation

- **`Authority {root, owner, generation, incarnation}`** is the existing Runner
  root authority. A request names the authority it addresses (`addressed`).
  Admissions, acknowledgments and refusals name the authority that answered
  (`responder`), inspection and observations their `reporter`. The values are
  opaque host values compared only for equality. Carrying them makes a stale
  answer detectable; it does not fence anything.
- **Discovery** (`root_entry`) is a descriptive address: one existing root of
  `requester`, with its last known `authority`, from a `describer` that is an
  opaque locator and never an authority. An entry claims no admission,
  scheduling, capacity, reservation or ownership, and does not show its
  authority is current: a request addressed from it can meet a
  `stale_authority` refusal or need a `recover`. An entry answers no request
  and settles nothing.
- **Logical links.** `ControlScope {root, child?}` is what a control applies
  to; `LogicalRef {root, child?, work?, input?}` is what an observation is
  about. They are logical identities only.
- **Actor evidence.** OS process identity, Agent Bash handle, provider-native
  session and live stream are `ActorEvidence {actor, exactness, ref}` attached
  to an observation. `exactness` is `exact`, `legacy` or `incomplete` as the
  producer claims. Actor evidence is never a logical key, scope or authority,
  and the SDK does not define correlation across these domains.
- **Correlation.** A request's immutable identity is its `RequestRef
  {request_key, requester, addressed}`. Every response repeats it exactly and
  names the operation it answers. Inspection cites requests by `RequestRef`.

## The claim ladder

| Record | Claims | Does not claim |
| --- | --- | --- |
| `request` | Requester intent: operation, scope, addressed authority, optional reason | Receipt, admission or effect |
| `receipt` | The request reached a queue or endpoint; `durable` is the receiver's retention claim | Admission, acknowledgment, execution or outcome |
| `admission` | The answering authority admitted the request for its transition | That the transition happened |
| `acknowledgment` | The operation's state moved `from` → `to` at the answering authority | Running work, ended work or physical custody |
| `fulfillment` | A same-incarnation successor reports its own present `from` → `to` fulfillment of an inherited admitted intent, correlated to the original request | Predecessor ACK, predecessor authority, new admission or a new intent |
| `non_fulfillment` | A same-incarnation successor reports its own present terminal inability to fulfill the original inherited admitted intent | Predecessor refusal, predecessor authority, absence of admission, or erasure of a known positive |
| `conflict` | A changed submission is refused with both exact submitted and preserved original request contents | A refusal/outcome of the original intent or a second transition |
| `refusal` | Explicit refusal at `admission` or `transition`, with a reason | — a refusal is an outcome, not an absent acknowledgment |
| `outcome` | What a reporter knows of the request's own transition: `acknowledged`, `fulfilled`, `unfulfilled`, `refused`, or `unknown` with an uncertainty reason | — `unknown` never erases an earlier acknowledgment or refusal; reporting confers no authority |
| `observation` | One settlement fact about a logical link, with actor evidence | Any control, request, admission or acknowledgment |
| `root_entry` | Discovery: a descriptive root address | Authority, admission, scheduling, capacity or ownership |
| `control_state` | Inspection: an authority's current knowledge | An effect or acknowledgment |

Refusal reasons: admission stage `stale_authority`, `owner_live`,
`unsupported_operation`, `not_permitted`, `unknown_scope`;
transition stage `already_terminal`, `root_absent`, `transition_failed`. The
reasons name the root owner's decision. The SDK defines no authorization
policy.

Uncertainty reasons: `authority_changed`, `transport_lost`,
`evidence_unavailable`.

## Later knowledge of the same intent

A request's identity is immutable, and it stays addressable after an outcome
reported `unknown`:

- **Refinement.** After `unknown`, later claims for the same request — a
  receipt, the admission, the acknowledgment, fulfillment or refusal, and a definite
  outcome — are accepted in ladder order. `unknown` may also be repeated with
  another reason. The trace retains at most 8 distinct unknown reports and
  reserves one additional slot for definite knowledge. A ninth distinct
  unknown is refused without mutation; it cannot block later definite knowledge.
  Exact retained reports still replay as duplicates.
- **Contradiction.** `acknowledged`, `fulfilled`, `unfulfilled` and `refused` are final. A later
  different outcome, an acknowledgment after a refusal, a refusal after an
  acknowledgment, or a second different admission or acknowledgment is a
  protocol violation and leaves the trace unchanged.
- **Successors.** Only the addressed authority admits, acknowledges or
  refuses (other than `stale_authority`) a non-recover request. A successor
  may faithfully replay persisted predecessor claims unchanged, preserving
  their responder, and report later outcome knowledge. It cannot produce a
  predecessor ACK. Separately, a `fulfillment` names the successor as `reporter`
  and claims its own present fulfillment of the original admitted intent.
  The reporter must differ in owner or generation within the same root and
  incarnation. `recover` uses its own original attachment ladder, not fulfillment.
  The trace requires the inherited admission, matching operation/domain/target,
  at most one fulfillment, and no refusal. It preserves the original admission,
  immutable correlation and any predecessor ACK; a truthful prior ACK may also
  be replayed before a definite outcome. `outcome: fulfilled` requires that
  fulfillment; `outcome: acknowledged` still requires the predecessor ACK.
  Neither inspection nor receipt alone supplies inherited admission.
  Inheritance, positive newer-owner attribution, fencing and actual fulfillment
  are Runner realization. Equality checks establish none of those truths.

### Terminal non-fulfillment by a successor

`non_fulfillment` names the successor as `reporter`, the original correlation
and operation, and `already_terminal`, `root_absent` or `transition_failed` as
its own present cause. For example, a successor finding an inherited admitted
close while the lifecycle is already cancelling can report `already_terminal`;
it must not regress cancellation, manufacture a predecessor refusal or ask the
requester to reissue merely to finish this intent. The SDK checks the distinct
same-root/same-incarnation reporter, inherited admission and operation. It
verifies neither positively newer ownership nor the truth of the cause.
`recover` retains its separate attachment/refusal ladder.

No prior ACK, fulfillment or refusal may coexist with non-fulfillment; a
negative attempt against a known positive fails without erasing that positive.
`outcome: unfulfilled` requires this attributed claim. Unknown history retains
its eight-report bound and reserved definite slot. Exact replay is still a
duplicate. A transition refusal without `responder` is inadmissible, even through
raw typed trace admission; an anonymous admission-stage refusal is still a
refusal of an unadmitted attempt, not terminal resolution of admitted intent.
New ACK/fulfillment or positive outcome evidence after non-fulfillment is a
protocol contradiction, including after the negative conclusion. The rejected
record and diagnostic belong in the consumer's encounter evidence; rejecting
it leaves the preserved trace unchanged, and never validates the negative as
actual truth. Positive-after-positive finality retains its existing meaning.
Inspection listing a terminal non-fulfilled intent pending contradicts it.

Faithful persisted replay, durable intent and the acknowledgment surviving
owner death are producer realizations. The contract states what such claims
mean and refuses ones that contradict; it cannot make a producer keep them.

## Current state and prior acknowledgments

`control_state` reports what its `reporter` knows now about one scope:

- `input` and `lifecycle`: each a state of its domain or `unknown`, with an
  optional `since` naming the request that established it;
- `pending`: up to 8 intents the reporter holds without an acknowledgment, fulfillment, non-fulfillment or
  refusal (`received` or `admitted`).

Current knowledge is not an acknowledgment or fulfillment and never erases
either. The relation reader uses a retained ACK or fulfillment target. A
pending entry naming this request with another operation, or listing it pending
after its ACK/fulfillment/non-fulfillment/refusal, reads `contradicts`. Producers owe coherent
pending reports, including immutable keys across generations.
`RequestTrace::relate` states how a report relates to one request:

| Relation | Meaning |
| --- | --- |
| `current` | The report shows the acknowledged or fulfilled state |
| `prior_retained` | The report no longer knows the state (`unknown`); the acknowledgment or fulfillment stands |
| `superseded` | The reporter cites **another** request in `since` as superseding (a later release; a cancel over a close). The producer owes a real, newer, same-domain admitted intent; this reader does not look it up or verify its ordering |
| `contradicts` | A different state with no other request as its basis, or a lifecycle regression (`cancelling` → `closing` → `open`): the acknowledged transition silently vanished, as an acknowledged close becoming open after successor attach would |
| `pending` / `no_acknowledgment` | No retained ACK or fulfillment; the report does or does not hold it as pending |
| `unrelated` | Another scope, or the attachment domain, which inspection does not describe |

## Settlement facts

Four fact types stay distinct:

| Fact | States, earlier → later knowledge |
| --- | --- |
| `insertion` | `uncertain` → `acknowledged` or `not_inserted` |
| `tagged_end` | `absent` → `observed` |
| `logical_settlement` | `owed` → `settled` |
| `physical_custody` | Within **one exact actor reference**: `unsettled` → `live` → `exited_wait_pending` → `exited_waited` |

Every fact may also be `missing` (with `missing_reason`: `not_captured`,
`access_denied`, `unsupported`, `not_applicable`) or `redacted`. Neither is a
negative: missing insertion evidence is not `not_inserted`, and a missing
tagged end is not `absent`.

### Coherent current evidence

`read_settlement(subject, lineage, observations)` reads the observations of
one exact logical subject as **one evolving account**, not a set of competing
states:

- A report that refines an earlier one (`uncertain` → `not_inserted`, `owed`
  → `settled`, `absent` → `observed`) is not a conflict; the most refined
  state is read. Missing and redacted reports neither refine nor contradict.
- States neither of which refines the other (`acknowledged` and
  `not_inserted`) are retained as `conflicting`, never resolved to the latest.
- The reading does not depend on report order. Timestamps are bounded host
  observations, not ordered clocks or causal evidence.

### Physical correlation

The physical summary is knowledge about one actor reference, never subject-wide
or all-actor discharge. For a non-withheld physical state, the report must carry
exactly one `exact` actor evidence item with a present reference to compose.
Different actor references, missing/ambiguous identity, or legacy/incomplete
identity yield `conflicting` in the summary. The raw reports keep their separate
facts and references; this label includes inability to correlate and does not
assert the raw facts are false. Different reporters of the same exact actor
can compose; reporter authority is not actor identity. Missing/redacted states
remain withheld, and never establish custody. Same-actor refinement presupposes
truthful incarnation-sensitive references and coherent encounter selection.
Runner establishes actual actor relations and independently appropriate
all-actor custody; this reader neither inspects processes nor attests identity.

### Warranted reporters

- A reporter of another root is never composed.
- A `Lineage {root, authorities}` is the consumer's statement of which owner
  generations and incarnations are warranted reporters for the root — its
  owner/generation lineage. With a lineage for the subject's root, only its
  authorities are composed and the reading's `basis` is `warranted`, meaning only that the caller supplied a matching
  lineage filter; the SDK has verified no warrant or coherent encounter.
  Old histories and arbitrary same-root reporters outside it are left out
  and counted in `excluded`. An earlier generation in the lineage is not
  erased by its successor.
- Without a lineage the `basis` is `unwarranted`: a description of claims.

The SDK applies a lineage; it does not derive, attest or fence one. Selecting
the coherent current encounter and the warranted lineage is Runner
realization.

### Logical reading

The logical reading is derived from insertion, tagged end and logical debt
only:

- `settled` needs insertion `acknowledged`, tagged end `observed` and logical
  settlement `settled`;
- `owed` when debt is reported owed, or insertion was acknowledged with a
  known `absent` tagged end and no contrary settled-debt claim;
- `not_inserted` when insertion is positively `not_inserted` with no observed
  tagged end and no owed debt;
- `unknown` otherwise, including missing, redacted, uncertain, contradictory or
  conflicting reports.

Physical custody never enters the logical reading: an observed exit, a
completed wait or a closed peer is not logical settlement. An acknowledged
input with no tagged end and owed debt reads `owed` whatever its physical exit
and wait.

**Retirement.** A `settled` reading with `warranted` basis is the only
reading this contract describes as fit to support retirement, and only under
the warrant the consumer supplied. An `unwarranted` reading, a physical exit,
an acknowledgment absence or a single latest report is not retirement
evidence. The decision to retire remains the root owner's.

## Repetition and idempotency

A request's key scope is `(requester, addressed root, request_key)`.
`classify_repetition` gives:

- **`same_request`**: same key scope and identical content. It is the same
  request. A conforming root owner answers it with the same claims and makes
  no second transition. That is a producer obligation the SDK cannot verify.
- **`key_conflict`**: same key scope, different content — including the same
  key re-addressed to a successor generation, or a changed reason or
  redaction. It receives a `conflict` answer to that submission, never an original-intent
  `refusal` or a retry.
  After an authority change, read the old request's outcome and send a new key.
- **`distinct`**: another key scope; an independent request.

A `conflict` embeds the complete `submitted` and `original` Requests without
record-kind tags, a same-root `responder` and an observation time. Both requests
must be admitted structurally/semantically and classify as `key_conflict`.
Whole contents distinguish changed operation, scope, reason/redaction or
addressed generation even when their `RequestRef` is identical. No digest,
new authority, request key or private protocol is introduced.

`Conflict::answer_to(submitted)` checks the exact current submission.
`RequestTrace::accept(conflict)` checks its exact original against the trace and
returns `submission_conflict {submitted}` without mutation, even after the
original's definite outcome. Consumers join both checks for their current
submission and preserved trace. A mismatched original or current submission
is a protocol violation. `Record::correlation()` returns no original-intent
correlation for a conflict: it answers the embedded submission, not that ladder.
Conflicts consume no trace slot and are not stored as original claims; their
exact replay returns the same submission answer. A restamped conflict remains
a submission answer, never a restamped original ACK. Ordinary `refusal` with
`key_conflict` is inadmissible. The owner must preserve the original and apply
no effect of the changed submission; the SDK stores or enforces neither.

`RequestTrace` treats an identical redelivery of an already accepted record
as `duplicate`, with no new meaning or state change. Identity is the whole
record, timestamps and disclosure state included: a re-stamped acknowledgment
contradicts the first, so replay of persisted records must be byte-faithful.
Distinct receipts count toward the trace's bound of 8.

## Normative semantic rules

| Operation | Required semantic checks / result |
| --- | --- |
| Record admission | Record lines are at most 32768 UTF-8 bytes before parsing. A request's `scope.root` equals its `addressed.root`. An admission's or acknowledgment's responder may answer its operation: the addressed authority, or for `recover` an authority with the same root and incarnation and a different owner or generation. ACK and fulfillment `to` match the operation target; `from` belongs to the operation domain or is `unknown`, and lifecycle cannot regress. Fulfillment excludes recover and names a distinct same-incarnation successor reporter. A refusal's stage matches its reason. Non-fulfillment excludes recover and requires a distinct same-incarnation successor. Conflict embeds admissible complete Requests that classify as key_conflict and a same-root responder. `owner_live` and `root_absent` refuse only `recover`; `stale_authority` never refuses `recover`. A `stale_authority` or `owner_live` responder, when present, differs from `addressed` with the same root; any other refusal's responder, when present, may answer the operation. Transition refusals require a responder. Key conflicts use the separate submission record. An outcome's `reporter`, when present, has the addressed root. `exact` actor evidence has a present reference. A control state's reporter and every cited request have the scope's root; `input` and `lifecycle` are in their domains; a pending intent is listed once. |
| Selection | Validate the local offer, the advertisement bound and shape, and the strict v3 entry; ignore unknown entries; intersect operations, reports and facts; at least one operation or report is common. |
| Agreement | The selected protocol is v3 and its offer is structurally valid (including the hold/release pairing); the record's operation, fact type or report is selected. |
| Repetition | Classify by key scope then exact content, as above. |
| Trace | A trace starts from an admitted request. Every record must be admissible, correlate exactly and name the request's operation; observations, root entries and control states are refused. Identical redelivery is `duplicate`. Nothing new follows a definite (`acknowledged`/`fulfilled`/`unfulfilled`/`refused`) outcome in the original ladder; correlated submission conflicts remain readable without changing it; after `unknown` the ladder continues. Receipts at most 8 distinct, unknown reports at most 8 plus one reserved definite outcome. Admission at most once and not after a refusal. Acknowledgment only after admission, from the admitting responder, at most once, never with a refusal. Admission-stage refusal only before admission; transition-stage refusal only after admission; never after an acknowledgment or fulfillment. Fulfillment needs inherited admission, a distinct same-incarnation successor, the same operation and a valid non-regressing transition, at most once and never with refusal. Non-fulfillment needs inherited admission, the original operation and a distinct same-incarnation successor, at most once, with no ACK/fulfillment/refusal. Outcome `unfulfilled` needs non-fulfillment. Outcome `fulfilled` needs fulfillment; `acknowledged` needs an acknowledgment, `refused` needs a refusal, `unknown` is admissible within its separate bound and retains earlier claims. Refused records leave the trace unchanged. |
| Relation | As in the table above, for the request's scope and domain. |
| Settlement reading | As above: exact subject; refinement order; same-root reporters only; lineage filter when supplied; physical states compose only within one exact actor reference; ambiguous/differing actor evidence reads conflicting; physical custody does not affect the logical reading. |

## Bounds and redaction

| Bound | Value |
| --- | --- |
| Record line, checked before parsing | 32768 bytes |
| Host values | 1–256 printable non-space ASCII characters |
| Request key | 1–128 printable non-space ASCII characters |
| Reason/detail text | 1–512 characters |
| Actor evidence per observation | 16 |
| Pending intents per control state | 8 |
| Receipts / unknown outcome reports per trace | 8 / 8, plus one reserved definite outcome |
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

The golden vectors in `tests/fixtures/session_control/v3.json` and their tests
establish the following, over claims:

- classified structural acceptance versus normative semantic admission;
- the bounds and redaction states above;
- selection, version negotiation, the hold/release pairing, agreement and
  their diagnostics;
- repetition classification;
- one claim ladder per operation, recover's same-incarnation answering rule,
  refinement of the same intent versus contradiction, and successor knowledge
  that neither acknowledges nor erases;
- relations between current-state reports and prior acknowledgments;
- order-independent settlement reading, the lineage filter and the exclusion
  of other roots' reporters, and the separation of insertion, tagged end,
  logical debt and physical custody — including the actual U112
  acknowledged/no-end/owed/exited-and-waited records, mapped by an earlier
  independent observation;
- separation from provider launch outcomes and error responses.

They do not establish any of the following:

- that a root owner enforces a hold, close or cancel, keeps intent durable,
  replays faithfully or answers duplicates identically;
- that any requester is authorized, any lineage warranted or any claim true;
- adoption by the root discovery/control face, or runtime behaviour.

Incident reports, typed resource evidence, severity and scope, and recovery
authorization or proof are not defined here. They belong to a later bounded
contract slice with the incident ledger, witness and coordinator work.
