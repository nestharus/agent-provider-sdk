# Live stream v1

`oulipoly.live_stream/v1` is the provider-neutral record vocabulary of the
optional live-output plane. A publisher, a broker and subscribers use it to
exchange the following about live output:

- stream identity;
- publisher incarnation and sequence;
- channels;
- exact gaps and restarts;
- the opaque durable reference a stream finalizes to;
- caller-owned cursors;
- advertised visibility claims.

[v1.schema.json](v1.schema.json) is the wire authority, and the
`live_stream` module is its typed projection and admission.

Status: defined and unadopted. No publisher, broker, host or provider uses this
contract yet. Capture, rings, the broker, subscription surfaces and retention
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

Every failure of this plane is a `live_unavailable` diagnostic: an absent
broker, no common version, a malformed advertisement or record, a protocol
violation or an unknown stream. The diagnostic disables live viewing only. It
is not a provider error response, a launch event or a completion outcome, and
the SDK has no conversion between them.

## Selection

Peers select a version by advertisement, not through provider `describe`. A
CLI banner, binary, package or source revision plays no part. An advertisement
is a bounded JSON object (at most 32 entries and 16 KiB) mapping a protocol
identifier to that protocol's offer:

```json
{"oulipoly.live_stream/v1": {"channels": ["stdout", "stderr", "control"],
                              "audiences": ["owner", "same_user"],
                              "max_data_bytes": 65536},
 "oulipoly.live_stream/v2": {"anything": "a newer peer adds"}}
```

`live_stream::select` handles each kind of entry as follows:

- **Unknown entries** (other families and newer versions) are kept in the
  advertisement and ignored.
- **The v1 entry** must be a strict `Offer`. A malformed v1 entry is refused
  (`invalid_advertisement`), not skipped.
- **The selection** is the common channels, the common audiences and the lower
  `max_data_bytes`. If no channel or no audience is common, the result is
  `no_common_capability`.

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
keeps no registry of them.

## Channels

- **`stdout`, `stderr`, `pty`.** These carry 1–65536 bytes of base64 data per
  frame, or fewer under the selected `max_data_bytes`. Separate channels
  preserve host observation order only.
  - `pty` is a kind a publisher may declare.
  - No publisher is required to produce `pty`, and this contract revives no
    PTY capture.
- **`control`.** This carries typed facts the publisher observed:
  - `heartbeat`;
  - `channel_closed`;
  - `exit_observed`, with an optional `code` or `signal`. An observed exit is
    not completion.

  A control fact never commands, authorizes or acknowledges anything, and its
  receipt is not a semantic acknowledgement. Pause, resume and incident
  dispositions are not part of this contract.

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
- **`Cursor {stream_id, incarnation, after_seq}`.** The caller owns it.
  Redelivery is at-least-once and is deduplicated by sequence.
- **`finalized`.** This ends an incarnation. It is published only after the
  host's normal durable publication and carries the durable reference.
  - Brokers keep the finalized frame while they know the stream, so a cursor
    behind eviction reaches it after an exact gap.
- **`ended`.** This ends an incarnation that claims no durable reference.

Two pure functions give the reference semantics:

- **`live_stream::Follower`** checks delivery from a cursor. It enforces three
  rules:
  - every sequence is delivered, covered by a gap, or redelivered;
  - an incarnation changes only through a discontinuity that answers the
    cursor;
  - nothing follows the end.
- **`live_stream::plan_replay`** gives what a broker owes a cursor against its
  retained window: an exact eviction gap, a discontinuity, or a diagnostic. It
  never produces content the window does not hold.

## Bounds

| Bound | Value |
| --- | --- |
| Data per frame | 1–65536 bytes |
| Record line, checked before parsing | 90112 bytes |
| Sequence | 1 to 2^53−1 |
| Host values | 1–256 printable non-space ASCII characters |
| Durable reference | 1–1024 printable non-space ASCII characters |
| Diagnostic detail | 512 characters |

Diagnostic details name schema locations and keywords only. They never repeat
submitted values.

## Limits

The golden vectors in `tests/fixtures/live_stream/v1.json` and their tests
establish the following:

- schema, DTO and fixture agreement;
- the bounds above;
- the gap, cursor, incarnation and finalization outcomes;
- version selection;
- separation from provider launch outcomes.

They do not establish any of the following:

- that a real publisher or broker reports gaps truthfully;
- that capture never backpressures drainage, terminalization or completion;
- that a broker's absence or slowness leaves a launch unaffected at runtime;
- that a visibility claim is enforced;
- bounded memory under load;
- host adoption or refresh after a replacement.

Those belong to the capture, broker, subscription and conformance work.
Incident reports, pause and resume acknowledgements, and recovery
authorization are not defined here. They wait on the Runner decisions they
would encode.
