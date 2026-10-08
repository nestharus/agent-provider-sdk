# Live stream v3

`oulipoly.live_stream/v3` is the provider-neutral record vocabulary of the
optional live-output plane. A publisher, a broker and subscribers use it to
exchange the following about live output:

- stream identity;
- publisher incarnation and sequence;
- channels, including output whose stdout and stderr were combined before
  capture;
- exact gaps and restarts;
- the opaque durable reference a stream finalizes to;
- caller-owned cursors;
- advertised visibility claims.

[v3.schema.json](v3.schema.json) defines structural validation. That schema
**plus the normative semantic rules below** defines v3 conformance in every
language. Raw JSON Schema validation alone is insufficient. `live_stream`
implements both layers and their context-dependent agreement/follow/replay
operations. Raw Serde deserialization supplies representation only.

Status: defined and unadopted. No publisher, broker, host or provider uses this
contract yet. Agent Runner's interim root Bash view
(`oulipoly.root_bash_live/v1`) carries combined output on its own wire because
v2 could not name it; converging that view onto this family is Runner's
separate work (see [Consumer convergence](#consumer-convergence)). [Attachment](#publisher-broker-and-subscriber) defines what the
three roles say to each other and which side owns each check. Capture, rings,
the broker itself, endpoints, sockets, subscription surfaces and retention
belong to later work. That later work is what establishes runtime behaviour.

## What it is not

This plane is optional observability. It does not replace or relabel the
following existing surfaces:

| Existing surface | What it stays |
| --- | --- |
| provider/v1 launch events and the request custody journal | The required launch result channel and exact completed replay |
| `resident_session/v1` records | Durable custody of resident input and turns |
| `session.read_turns` page and resume tokens | Pages over a durable transcript snapshot |
| tool-mediation retained output offsets | Retrieval of a requester's retained Bash output |

The contract stores nothing. Completed turns stay canonical in the host's
normal durable session storage. A `finalized` frame names that storage only by
an opaque `durable_reference`. It adds no permanent output journal or event
database.

Contract refusals are `live_unavailable` diagnostics: an absent
broker, no common version, a malformed advertisement or record, a protocol
violation, an unknown stream or a missing host decision (`not_authorized`). The diagnostic disables live viewing only. It
is not a provider error response, a launch event or a completion outcome, and
the SDK has no conversion between them.

## Selection

Peers select a version by advertisement, not through provider `describe`. A
CLI banner, binary, package or source revision plays no part. An advertisement
is a bounded JSON object (at most 32 entries and 16 KiB) mapping a protocol
identifier to that protocol's offer:

```json
{"oulipoly.live_stream/v3": {"channels": ["combined", "control"],
                              "audiences": ["owner", "same_user"],
                              "max_data_bytes": 65536},
 "oulipoly.live_stream/v2": {"channels": ["stdout", "stderr", "control"],
                              "audiences": ["owner"], "max_data_bytes": 4096},
 "oulipoly.live_stream/v4": {"anything": "a newer peer adds"}}
```

`live_stream::select` handles each kind of entry as follows:

- **Unknown entries** (other families, and older or newer versions of this
  one) are kept in the advertisement and ignored, whatever their shape.
- **The v3 entry** must be a strict `Offer`. A malformed v3 entry is refused
  (`invalid_advertisement`), not skipped.
- **The selection** is the common channels, the common audiences and the lower
  `max_data_bytes`. If no channel or no audience is common, the result is
  `no_common_capability`.

A peer that cannot carry combined output simply does not offer `combined`;
selection then leaves it out, and a combined stream cannot be followed under
that selection (see [Combined origin](#combined-origin)).

The `combined` channel, the attachment messages and the `not_authorized`
diagnostic belong to `oulipoly.live_stream/v3`, schema identity
`urn:oulipoly:live-stream:v3`. This SDK selects only v3
(`SUPPORTED_VERSIONS = [3]`). A peer advertising only baseline
`oulipoly.live_stream/v1` or `oulipoly.live_stream/v2` gets
`no_common_version`, disabling optional live viewing. The retained
[v1 schema](v1.schema.json) and [v2 schema](v2.schema.json) describe earlier
vocabularies (v1 without attachment messages or `not_authorized`, v2 without
`combined`); they are not runtime fallbacks. There is no source, package or
binary compatibility guarantee or requirement: schema/capability agreement is
the compatibility relation. Required `oulipoly.provider/v1` and session-control
v3 negotiation are unrelated, and no live-stream outcome changes them.

## Identity and sequence

`stream_id` and `incarnation` are each 128 random bits, written as 32 lowercase
hex digits.

- **Stream.** A stream is one ephemeral live-output source. It is not a logical
  agent, session, turn, process or Agent Bash handle.
- **Incarnation.** A publisher chooses a new incarnation each time it starts.
  A publisher incarnation is not a process actor incarnation (PID plus start
  ticks), an owner generation or an incident epoch.
- **Sequence.** Publisher frames (`data`, `control`, `finalized`, `ended`) are
  numbered per `(stream_id, incarnation)` from 1, adding exactly 1 each time.

A `Descriptor` declares the following for one incarnation:

- its channels;
- its `max_data_bytes`;
- a `VisibilityClaim`, whose exposed channels must be among the declared
  channels;
- optional opaque host `Correlation` values: `root`, `session`, `work`,
  `generation` and `epoch`.

The SDK compares correlations only for equality. It is not their authority and
keeps no registry of them. The correlation slots are closed. Hosts must encode
spaces or Unicode into the admitted non-space ASCII alphabet before carrying
such identifiers; the SDK does not select or interpret that host encoding.

## Channels

- **`stdout`, `stderr`, `combined`, `pty`.** These carry 1–65536 bytes of
  base64 data per frame, or fewer under the selected `max_data_bytes`. Bytes of
  one channel keep their capture order. Separate channels preserve host
  observation order only.
  - `stdout` and `stderr` carry bytes captured from a descriptor that carried
    only that origin.
  - `combined` carries one byte stream into which the producer's stdout and
    stderr were joined before capture. See [Combined origin](#combined-origin).
  - `pty` is a kind a publisher may declare.
  - No publisher is required to produce `pty`, and this contract revives no
    PTY capture.
- **`control`.** This carries typed facts the publisher observed:
  - `heartbeat`;
  - `channel_closed`;
  - `exit_observed`, with an optional `code` or `signal`. An observed exit is
    not completion. `channel_closed` reports an observation; it does not
    prohibit subsequent data or change follower channel state.

  A control fact never commands, authorizes or acknowledges anything, and its
  receipt is not a semantic acknowledgement. Pause, resume and incident
  dispositions are not part of this contract.

### Combined origin

When a producer's stdout and stderr share one descriptor before capture (for
example, both duplicated onto one pipe), the capture cannot tell which origin
wrote each byte. Such output is carried on the `combined` data channel:

- Its bytes are in the order read from that one descriptor. Interleaving
  reflects the producer's writes as the descriptor delivered them; it says
  nothing about which origin wrote any byte.
- A descriptor that declares `combined` never also declares `stdout` or
  `stderr`. One incarnation either knows each byte's origin or carries it
  combined; it never claims both. The schema refuses such a descriptor, and
  no follow context may be set up with both.
- `channel_closed` may name `combined`. It reports that the shared
  descriptor closed, not that either origin did.
- The SDK never splits, infers or relabels combined bytes, and a subscriber
  that did not select `combined` gets a live-only diagnostic rather than the
  bytes under another channel name.
- Selecting `combined` asserts a capability shape only. Whether a publisher
  labelled its capture truthfully is a host obligation (below), not something
  validation can establish.

## Gaps, restarts, cursors and the end

- **`gap {first, last, reason}`.** Frames `first..=last` of that incarnation
  will never be delivered. The reason is `evicted` or `capture_overflow`. A gap
  is a delivery record and has no sequence of its own.
- **`discontinuity`.** This answers a cursor whose incarnation is not current.
  Delivery continues only from the current incarnation's start. The lost tail
  of the previous incarnation is reported as one of three cases:
  - exact, when `previous_last_seq` is known;
  - nothing, when `previous_last_seq` equals the cursor;
  - unknown, when `previous_last_seq` is absent.
- **`Cursor {stream_id, incarnation, after_seq, terminal?}`.** The caller owns
  and persists the whole cursor. `terminal`, when present, is the complete
  `finalized` or `ended` frame at exactly `after_seq` for this identity.
  Reconstructing a follower preserves this knowledge and its durable reference.
  `Duplicate` is positional suppression at or behind the cursor, including
  positions advanced through a gap. It establishes neither identical content,
  duplicate purity nor broker truth. A terminal frame at the current position
  can supply previously unknown terminal knowledge; contradictory terminal
  metadata is refused. Ordinary old content is not compared or rechecked
  against selected channel/byte limits after structural and semantic admission.
- **`finalized`.** This ends an incarnation. It is published only after the
  host's normal durable publication and carries the durable reference.
  [What a terminal claims](#what-a-terminal-claims) bounds its meaning.
  - Brokers must retain the terminal frame while they know the stream, so a
    cursor behind eviction reaches it after an exact gap. `RetainedWindow`
    carries the complete terminal frame once the incarnation ends. Its window
    cannot claim the terminal position was evicted. Actual retention and durable
    publication remain runtime obligations.
- **`ended`.** This ends an incarnation that claims no durable reference and
  carries the same resumable terminal knowledge without inventing a reference.

### What a terminal claims

A terminal frame claims only what its kind states:

- `finalized` claims that the host durably published the record its
  `durable_reference` names, before sending the frame. It does not claim a
  known or successful command exit, complete or readable retained bytes, or
  delivery or acknowledgement of any report to anyone. The durable record
  carries those classifications. A record that classifies its own command
  wait as unknown, or its retention as partial or unsealed, keeps that
  classification; finalization does not upgrade it.
- `ended` claims only that this incarnation published its last frame. It
  names no durable record and implies no failure: the work may still be
  settled later through the host's durable storage, outside this stream.
- A delivered `exit_observed` reports an exit the publisher observed; it does
  not establish successful completion. Its absence means only that no exit
  fact was delivered on this live plane. Control may be unselected, or a gap
  may hide the fact even when the publisher observed an exit. The subscriber
  cannot infer the publisher's wait knowledge from silence or a terminal
  frame. When the command wait is unknown, the publisher sends no
  `exit_observed` and invents no code or signal. Known or unknown command wait
  remains a classification in the matching durable record; `finalized` does
  not convey that record's content or grant access to it.
- A cursor's retained terminal is knowledge of the terminal frame, not of
  the durable record's content. Receiving any record is not an
  acknowledgement.

An ended/finalized follow context is terminal. It cannot silently move to a
later publisher run, even via discontinuity. Following a later observation
context requires explicit descriptor/follow setup with a fresh cursor; it does
not infer logical work continuity.

The following APIs give the reference semantics:

- **`Descriptor::agree` / `Follower::from_descriptor`** join descriptor admission
  with a selected protocol, channels, byte limit and audience support. A supported
  advertised audience shape is compatible; this does not authorize an observer.
  `Follower::new` is a validated low-level setup for already-composed contexts.
- **`live_stream::Follower`** checks delivery from a cursor. It enforces three
  rules:
  - every sequence is delivered, covered by a gap, or redelivered;
  - an incarnation changes only through a discontinuity that answers the
    cursor;
  - nothing follows the end.
- **`live_stream::plan_replay`** gives what a broker owes a cursor against its
  retained window: an exact eviction gap, a discontinuity, or a diagnostic. It
  never produces content the window does not hold. The plan carries retained
  terminal metadata even at an already-final cursor. At that position its prefix
  reoffers the complete terminal frame to restore unknown knowledge, rather than
  returning an ordinary empty replay. For a lagging cursor, `deliver_from` still
  names retained delivery; the follower learns terminal state when that frame
  is delivered, not merely from planning metadata.

## Publisher, broker and subscriber

`live_stream::attachment` gives the connection-level meaning. Every connection
joins a broker to one publisher or to one subscriber; there is no direct
publisher-to-subscriber connection. Each line is one `Message`, tagged by `op`,
at most 91136 bytes (the largest record line plus its envelope) checked before
parsing.

| Step | Sender | Message | Meaning |
| --- | --- | --- | --- |
| Open | both ends | `hello {role, advertisement}` | Selection as above. The role pair must join a broker to a peer. |
| Register | publisher | `register {descriptor, finalization}` | Attach one incarnation. `finalization` is `custody_owner` or `never`. |
| | broker | `registered {stream_id, incarnation}` | Attached. Not an observer grant, durable custody or finalization. |
| Publish | publisher | `record` | Data, control, `ended`, `finalized` (only if `custody_owner`) and `capture_overflow` gaps for frames it dropped. |
| Discover | subscriber, broker | `list`, `directory {streams}` | At most 32 descriptors the host chose to list. Listing is not permission. |
| Attach | subscriber | `attach {cursor}` | Follow one stream from a caller-owned cursor. |
| | broker | `attached {descriptor, plan}` | The current descriptor and `plan_replay`'s plan; retained `record`s follow from `deliver_from`. |
| Refuse | either | `unavailable {diagnostic}` | The receiver stops live viewing; nothing else changes. |

The broker admits a registration (`Ingest::register`) or an attachment
(`attach`) only with a host-supplied `HostDecision`. `Granted` names the stream
and an opaque host scope reference; for a `scoped` claim it must equal the
claimed scope. The SDK never constructs `Granted`. Nothing on the wire becomes
one: not a peer UID, an advertisement, a descriptor, a correlation, a control
fact or an exit. The decision is a local input, not a message.

`Ingest` binds a publisher connection to its registered stream and incarnation
and reuses `Follower` for sequence checks. It refuses eviction gaps and
discontinuities, which are the broker's own delivery records. An observed exit
or a closed connection does not end the incarnation. `Ingest::retire` reports
the incarnation's last sequence to a later window only when a terminal frame
arrived. Otherwise frames the publisher sent but the broker never received
leave the lost tail unknown. `follow_attached` builds the subscriber's
follower, applies the plan prefix and requires retained delivery to start
exactly at the follower's next position.

### Host obligations

These are runtime duties. The contract names them; it does not perform them,
and claim-shape validation does not discharge them.

- **Endpoint and transport.** The host supplies the broker endpoint and an
  ordered, reliable byte stream. A missing or unreachable broker is
  `broker_absent`: the publisher stops publishing and continues its work.
- **Scope.** The host establishes who may publish or observe before granting.
  There is no implicit cross-UID publication, including from a host-root
  custodian to another user's subscriber or the reverse. A bridge across
  users needs its own explicit authority, which this contract does not define.
- **Genuine identity.** A publisher chooses a fresh random incarnation on each
  start. The broker cannot tell a plausible made-up cursor position from a
  genuine one; stale, foreign or ahead cursors are refused only as far as
  `plan_replay` can see.
- **Finalization.** `finalized` comes only from the custody owner's terminal
  retained result after normal durable publication. Pipe or descriptor
  closure, a ready sentinel, an observed exit, or broker or process exit is
  never finalization. A publisher without access to that result registers
  `never`. The host must itself warrant that the publication happened and
  that the named record is this incarnation's: a committed transaction that
  may not have written the matching record, or a report only queued locally,
  does not warrant it. When the host cannot name a durably published record,
  the incarnation ends with `ended`.
- **Origin.** A publisher uses `stdout` or `stderr` only for bytes captured
  from a descriptor that carried only that origin. Output joined before
  capture is `combined`. Relabelling combined bytes as one origin, or
  splitting them by guess, misstates the capture.
- **Drop, never block.** Publishing must not add I/O or backpressure to the
  required output drain, terminal publication or close. When a publisher cannot
  hand frames off, it drops them and later sends an exact `capture_overflow`
  gap. Enabling live publication is optional and consumer-driven.
- **Optional, live-only refusal.** A host that carries live-stream offers or
  requests inside a required request (launch, recovery, control) must admit
  that part separately, so a malformed, unknown or unsupported live part
  becomes a `live_unavailable` diagnostic and leaves the required request
  unaffected. `select` takes the peer's advertisement as an unvalidated JSON
  value for this reason. Embedding these DTOs as strict fields whose
  deserialization fails the whole required request does not meet this
  contract.
- **Authentication and custody.** Peer authentication, endpoint custody and
  authority to grant are host and runtime facts. Constructing or validating a
  DTO, a fixture or a `HostDecision` never attests them.

## Normative semantic rules

Conforming implementations apply these rules in addition to the structural
schema. Diagnostic reasons describe optional observation failure only.

| Operation | Required semantic checks / result |
| --- | --- |
| Record admission | Standard padded canonical base64 decodes to 1–65536 bytes. `gap.first <= gap.last`. A discontinuity changes incarnation; known `previous_last_seq >= after_seq`. Record lines are at most 90112 UTF-8 bytes before parsing. |
| Descriptor admission | `visibility.channels` is a subset of declared `channels`. `combined` is never declared with `stdout` or `stderr` (structural). This checks the claim, not enforcement or origin truth. |
| Cursor admission | Any terminal frame matches the cursor's stream/incarnation and has `seq == after_seq`. Persist all terminal metadata. |
| Selection | The serialized advertisement is at most 16384 UTF-8 bytes. Validate the local offer and the advertised v3 entry; ignore unknown, older and newer entries. Intersect channels and audiences (both nonempty); choose the smaller byte limit. An offer may name both `combined` and separated channels; it states capability, not one incarnation's origin. |
| Joined agreement | Selected protocol is v3 and its offer fields are structurally valid. Descriptor channels are a subset of selected channels, descriptor byte limit is no greater than selected limit, and its audience is supported by the selection. The cursor names the descriptor stream. A terminal cursor cannot start following another incarnation. |
| Typed follower/replay entry | Validate public typed inputs even if constructed directly or through raw Serde. A follow channel set never contains `combined` with `stdout` or `stderr`. Malformations return bounded diagnostics before state mutation or generated output. |
| Follow | Stream/incarnation must match; incarnation changes only through a matching discontinuity. Forward sequence is exactly `after_seq + 1`; fresh gaps start there and advance through their last sequence. Old sequenced positions / fully covered gaps are positional duplicates. Fresh data/control uses declared channels; fresh data respects the context byte limit; closure facts name declared data channels. Learn terminal knowledge at its position, retain it in the cursor, and refuse conflicting terminal knowledge or forward frames/gaps/discontinuities after it. |
| Discontinuity | It answers the cursor's incarnation and position. Known old last equal to the cursor means nothing lost; a greater value yields the exact remaining range; absent means unknown. Reset only an open follow context to the new incarnation and position zero. |
| Retained window | `1 <= first_retained <= last_published + 1`; `last_published` fits the sequence ceiling. Previous incarnation differs from current and known previous last fits the ceiling. Terminal metadata, required once ended, matches window identity and last published sequence; the terminal position is retained (`first_retained <= last_published`). |
| Message admission | Line at most 91136 bytes before parsing; schema-strict; every carried record, descriptor and cursor passes its own admission; `hello` advertisement at most 16384 bytes. A role sends only its own messages. |
| Host decision | `Granted` names the descriptor's stream and a valid host reference; for `scoped` claims it equals the claimed scope. `Refused` or a mismatch is `not_authorized`. |
| Ingest | Require each publisher frame or overflow gap to start at the next new position in the registered incarnation; refuse redelivery and same-position terminal replacement without mutation. Refuse eviction gaps, discontinuities, and `finalized` from a `never` publisher. A retired incarnation's last sequence is known only after its terminal frame. |
| Attached | Descriptor agrees with the subscriber selection; the window is the descriptor's. Require the canonical replay prefix and terminal/cursor consistency, including the terminal prefix at an at-final cursor; after the prefix, `deliver_from` is exactly the follower's next position. |
| Replay | Refuse a cursor ahead of known publication, another stream, or a window contradicting terminal cursor knowledge (including a later incarnation). An open old cursor gets a discontinuity with known/unknown old tail, then any exact eviction gap. `deliver_from` is the next retained position or the sentinel `last_published + 1`. Terminal metadata is retained in the plan; at-final replay reoffers the terminal frame in its prefix. |

The sentinel may equal 2^53 at the maximum sequence. It is an exactly
representable delivery position, never a legal publisher sequence. A publisher
must end within the sequence ceiling; no wraparound or implicit new incarnation
is defined. Timestamps are bounded host observations, not ordered clocks or
cross-pipe causal evidence. Gaps and terminal metadata are publisher/broker
assertions: validation cannot prove their truth, publication or reference existence.

## Bounds

| Bound | Value |
| --- | --- |
| Data per frame | 1–65536 bytes |
| Record line, checked before parsing | 90112 bytes |
| Sequence | 1 to 2^53−1 |
| Host values | 1–256 printable non-space ASCII characters |
| Durable reference | 1–1024 printable non-space ASCII characters |
| Diagnostic detail | 512 characters |

SDK-generated validation details use schema keywords or fixed diagnostic text
and do not echo submitted values, including arbitrary property names.
`LiveUnavailable::new` bounds arbitrary caller detail to 512 characters; it does
not sanitize or redact that detail. Valid captured bytes are preserved unchanged.
The caller/host owns any content handling and authorization.

The 64 KiB data and 90112-byte line ceilings are provisional contract choices,
not runtime-qualified thresholds. Peers can lower the data limit; a higher
ceiling requires a newly selected schema family if later benchmarks warrant it.

## Limits

The golden vectors in `tests/fixtures/live_stream/v3.json` and
`attachment-v3.json` and their tests establish the following:

- classified structural acceptance versus normative semantic admission and DTO
  projections (including vectors deliberately accepted by raw schema);
- the bounds above;
- the gap, cursor, incarnation and finalization outcomes;
- version selection, including refusal of baseline v1/v2-only peers as
  `no_common_version`;
- combined-origin carriage: declaration, selection, follow and attachment,
  and refusal of mixed declarations and of combined bytes under another
  channel name;
- separation from provider launch outcomes.

They do not establish any of the following:

- that a real publisher or broker reports gaps truthfully;
- that a publisher labels origin truthfully, or that its `finalized` follows a
  warranted durable publication;
- that capture never backpressures drainage, terminalization or completion;
- that a broker's absence or slowness leaves a launch unaffected at runtime;
- that a visibility claim or host decision is enforced or correct;
- any endpoint, socket, broker process, transport latency or throughput;
- bounded memory under load;
- host adoption or refresh after a replacement.

Visibility `channels` narrower than the declared channels are not yet filtered
for subscribers; the follower uses the declared channels. Directory paging
beyond 32 entries is not defined.

Those belong to the capture, broker, subscription and conformance work.
Incident reports and recovery authorization are not defined here. Control
claims, including input hold/release acknowledgements, are defined by
[`session-control/v3`](../session-control/README.md); a live-stream
`ControlFact` stays report-only.

### Publisher and attachment consistency

Publisher ingest requires every frame, or the first position of a
`capture_overflow` gap, to start exactly one past the last ingested position.
Duplicates and same-position terminal replacements are refused without mutation.
The reader's positional redelivery and terminal catch-up semantics remain valid
for delivered replay; they do not authorize a publisher to replace a position.

Subscriber attachment checks canonical replay consistency using the window facts
expressed by the answer. A declared terminal must belong to the descriptor's
stream/incarnation, agree with any known cursor terminal and the prefix, and not
lie behind the continuation. An at-final answer must carry its terminal prefix;
omission is refused. Lagging followers learn terminal knowledge on actual
terminal delivery. These checks cannot authenticate retention, a fabricated
in-range cursor, truthful gaps, random identities or durable-reference existence.

Selection covers the descriptor's full channel set. A stdout-only selection can
therefore fail for a stdout+control descriptor even when visibility claims only
stdout. Widening selection permits those declared channels; visibility filtering
is not implemented, and the host must restrict delivery. `HostDecision` and
`custody_owner` remain publicly constructible claims and host duties, not
SDK authentication or proof of authority/custody.

## Consumer convergence

Defining v3 does not adopt it. A host that replaces a private live wire with
this family still has to:

- offer and select `oulipoly.live_stream/v3` by advertisement, and carry
  combined capture on `combined`, never as `stdout` or `stderr`;
- assign publisher sequence numbers per read chunk, and report dropped chunks
  and evicted ranges as exact sequence gaps (`capture_overflow`, `evicted`);
  byte offsets, if the host keeps them, stay host-local;
- start a fresh random incarnation for each capture start, including takeover
  by a successor owner, and let `plan_replay` answer old cursors with a
  discontinuity whose lost tail is unknown unless the host truly knows it;
- send `finalized` only under the finalization obligation above, otherwise
  `ended`, and keep command-wait and retention classifications in its durable
  record;
- obtain a `HostDecision` from its own authority (for example kernel peer
  credentials checked against an explicit grant), never from wire content;
- admit the live part of any required request separately, so live refusal is
  live-only;
- meet the drop-never-block, bounded-resource and endpoint obligations at
  runtime.

Those are runtime properties the contract names and cannot establish.

## Version history

- **v1**: record vocabulary, selection, follow and replay.
- **v2**: adds publisher/broker/subscriber attachment messages and the
  `not_authorized` diagnostic.
- **v3**: adds the `combined` data channel for stdout/stderr joined before
  capture, refuses descriptors that claim both combined and separated origin,
  and states what terminal frames claim. Record, cursor, gap, discontinuity,
  replay and attachment semantics are otherwise unchanged from v2.
