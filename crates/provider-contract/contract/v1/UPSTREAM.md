# Contract snapshot provenance

These schemas were first imported as an exact snapshot of `contract/v1` from
`nestharus/agent-runner` commit
`afdd74fc4ab658a6be0441c7ca5bfb5cb8bafdbb`.

The same 13 schema files were independently verified byte-for-byte against
`nestharus/agent-runner-opencode` commit
`254925f22260afd0b2c71ad2319c088fdf69a9c3` during the import.

## Alignment with the current host snapshot (resident-session release)

Agent Runner later evolved its own copy with host-selected v1 extensions that
Codex already advertises and Runner's launch dispatch requires: the
`prompt_acceptance_v1`, `launch_output_v1` and `session_turn_pages_v1`
describe capabilities and their selectors, the launch `prompt_acceptance` and
`output_delivery` requests and reserved marker values, bounded
`oulipoly.session_turn_pages/v1` `session.read_turns` pages, and the
`provider_storage_contention` and `provider_unavailable` terminal-signal kinds.
This release imports all 13 schema files from `nestharus/agent-runner` commit
`5d025b82784556fe47521b1419c8eee574ddf13a` byte-for-byte, then applies SDK changes in two files:

- `describe.schema.json`: the optional host-selected
  `capabilities.resident_session_v1` boolean, open unknown capability
  advertisements, and numeric advertised contract versions;
- `common.schema.json`: the `host.env` description naming its selector
  `OULIPOLY_HOST_RESIDENT_SESSION_V1=1`.

This is a semantic v1 snapshot realignment under the fresh migration / no
legacy scope, not a compatible evolution of the old SDK snapshot. The former
`session.read_turns` request/result fixtures are replaced by bounded pages and
are rejected by this snapshot. Rust 0.2.0 versions the source API, not a wire
major. Other hosts have not been qualified. Structurally admitted
`provider_unavailable` / `provider_storage_contention` kinds still require
per-request extension selection before a consumer acts on them.

Advertisement tolerance does not widen selected v1 envelopes or payloads.
Known capabilities remain boolean. `SchemaRegistry` admission and the contract
chooser additionally enforce that `preferred_contract` belongs to the declared
list; this cross-field invariant is not expressible in standard JSON Schema.
Schema-only consumers must enforce it during selection.

Every other file is byte-identical to that Runner commit. The added capability
is omitted unless selected, so an unselecting v1 host never receives it; a host
must adopt these two files before selecting it. The checked-in DTO projection,
fixtures and tests were updated in the same release unit.

## License provenance

The source `nestharus/agent-runner` commit carries a root MIT license naming
`nestharus` as the 2026 copyright holder. Its grant expressly permits use,
copying, modification, publication, distribution, sublicensing, and sale. The
packaged `LICENSE-MIT` preserves that upstream license and notice byte-for-byte.

- Source license commit: `afdd74fc4ab658a6be0441c7ca5bfb5cb8bafdbb`
- Source license blob: `8e633dbfaf2a6df6141162938750f9a84e986b06`
- Source license SHA-256: `a325a8703bca9047dde855db64e2ed00bfdd2546be5981a55f720bfe01a6f3a7`

The independently verified `nestharus/agent-runner-opencode` snapshot declares
`license = "MIT"` in its package metadata and records the same Agent Runner
commit as the source of these schema bytes. The provider-contract package keeps
the upstream MIT terms and notice in every package archive.

The SDK is the source of truth after this import. Update the snapshot as one
versioned unit here, record the compatibility decision and complete schema
digests, then update hosts and providers from the released SDK contract. A host
or provider must not privately edit its imported copy. The recorded source and
digests establish the provenance of local schema files; they do not require peer
source or snapshot equality. Each active route must have compatible declared
wire schemas and capability meaning under [COMPATIBILITY.md](COMPATIBILITY.md).
The shared v1 discriminator alone does not establish that compatibility. Keep
the previous working route available through replacement verification and the
rollback decision, and restore that route if rollback is required, including
its compatible readers and durable state.

The JSON Schemas are the wire authority. `src/generated.rs` is their checked-in
Rust DTO projection and is updated in the same release unit; it is not an
independent generated source of contract semantics. `src/operations.rs` owns the
production operation-to-schema/DTO binding. The test matrix and typed round-trip
list are intentionally independent conformance projections that detect drift.
The imported launch schema's intrinsic phrase "request, event, and result
schemas" uses result for its terminal `exit` event; the v1 launch surface has no
separate result or response envelope.

SHA-256 identities:

```text
a6760352a585883708d0eb538c1dd2cd7572995b8288c440ba2635fa7f7b2866  common.schema.json
a501bc9a83b602d47e8dde7c3b12f7e0d4a8296bed73e3da882749ab596da8e8  describe.schema.json
c39d0c97e3f74b102e08bff14bb28baefdfa23f2fef7fa7fb67c308af05b049b  discovery.schema.json
b04462a3bd7020d2c3886f554f67cbf0941a638c6a6ce135be3edbb06a9a5840  launch.schema.json
25144a109c8dd4d56c6268d0e89f562b8dca1b3bb8cc5ce1e0f9ef09ac80433d  migration.schema.json
6bfd306db0d06c2837513a975046a7b2f908e426c2b50f26a5692883d33cc875  policy.schema.json
e33411bf286d74c64118b597d7fffc7e7c68d456f25fd48c27a4738224d6ddd4  quota.schema.json
762d361115fb42ec708fb10fe93834955e94341b3faee7112b3ffaae211eb190  rotation.schema.json
ea190f0eebf373cac05d84135ced6003a14faeddbd992453314596603def8b67  schema.schema.json
297ecf77f3dcd2a5a2b1f4f71ce61d25520d74a6b4349f544a0ea3a9d867159d  session.schema.json
f844876032d7ce0f289fec571823026758b1349d8dfe0b7bbcb6e7197a78e9d8  settings.schema.json
2e515d18166740c807a03f26454ed4e5857f7eea6759aa65a857476aee11c953  setup.schema.json
8dd39342bd7177cfd92df52046f4912555971418d0a95fa98074db8235196c6c  terminal.schema.json
```
