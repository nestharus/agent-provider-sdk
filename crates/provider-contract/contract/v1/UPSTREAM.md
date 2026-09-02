# Contract snapshot provenance

These schemas were imported as an exact snapshot of `contract/v1` from
`nestharus/agent-runner` commit
`afdd74fc4ab658a6be0441c7ca5bfb5cb8bafdbb`.

The same 13 schema files were independently verified byte-for-byte against
`nestharus/agent-runner-opencode` commit
`254925f22260afd0b2c71ad2319c088fdf69a9c3` during the import.

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
or provider must not privately edit its pinned copy. Each active host/provider
route uses one matching snapshot on both sides. Keep the prior matched pair
available while a replacement pair is prepared, and restore that pair as a unit
if rollback is required. The shared v1 discriminator alone does not authorize
mixed-snapshot operation.

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
3ce23f580ef7bf896e2c12f43a91a777945c8f67d9e1981ca728217e9f0b5a10  common.schema.json
69e413286bc1376b48e79eb63d6da8debed6257a627c7d50152ac931f2b93954  describe.schema.json
c39d0c97e3f74b102e08bff14bb28baefdfa23f2fef7fa7fb67c308af05b049b  discovery.schema.json
faf3b06a455e8a00a9f10c36b0ecf3038d6de6627873cf3cba3368a050ff8e9e  launch.schema.json
25144a109c8dd4d56c6268d0e89f562b8dca1b3bb8cc5ce1e0f9ef09ac80433d  migration.schema.json
292412aed125b9bf9dfaebbd239faef969a47d699ba5a95b91871964a9cb6eb7  policy.schema.json
e33411bf286d74c64118b597d7fffc7e7c68d456f25fd48c27a4738224d6ddd4  quota.schema.json
762d361115fb42ec708fb10fe93834955e94341b3faee7112b3ffaae211eb190  rotation.schema.json
ea190f0eebf373cac05d84135ced6003a14faeddbd992453314596603def8b67  schema.schema.json
71385c9ed6f8e935560691fd57e1f072a6f0fe1f3323b125e13515ea8b03b3ac  session.schema.json
f844876032d7ce0f289fec571823026758b1349d8dfe0b7bbcb6e7197a78e9d8  settings.schema.json
2e515d18166740c807a03f26454ed4e5857f7eea6759aa65a857476aee11c953  setup.schema.json
8dd39342bd7177cfd92df52046f4912555971418d0a95fa98074db8235196c6c  terminal.schema.json
```
