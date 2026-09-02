# Provider v1 Compatibility

`oulipoly.provider/v1` is the existing Agent Runner external-provider contract.
The SDK pins that boundary; it does not add a second host/provider protocol.

## Policy

- The 13 schemas, checked-in Rust DTO projection, subcommand matrix, and conformance fixtures
  form one release unit.
- Hosts and providers consume a released SDK version or an exact pinned snapshot
  whose digest matches that release. They do not maintain private schema edits.
- Host and provider peers on one route must use the same complete v1 snapshot.
  Sharing the `oulipoly.provider/v1` discriminator does not establish snapshot
  compatibility, and mixed-snapshot v1 operation is unsupported.
- Before rollout, the route owner must be able to keep the previous matched
  host/provider pair available through replacement verification and the rollback
  decision. If it cannot meet that prerequisite, the upgrade does not start and
  the route continues on its previous pair. Rollback restores both sides to the
  retained previous matched snapshot; it does not mix releases.
- Exact snapshot coherence intentionally takes priority over independently
  upgrading peers that merely share the v1 discriminator, including revisions
  that are otherwise wire-compatible. The route owner is responsible for
  coordinating host and provider deployment and retaining that previous complete
  pair. Mixed revisions are not an availability fallback.
- The crate and complete schema snapshot may be used and redistributed under
  the MIT License. Redistributors preserve the packaged upstream MIT notice.
  `UPSTREAM.md` records the imported schemas' source grant.
- A compatible v1 change must retain the contract discriminator and every
  established required behavior. Schema-specific unknown-field rules remain
  authoritative.
- A breaking wire change requires a new contract version and explicit
  negotiation. It must not be published as a silent v1 replacement.
- The `oulipoly.provider/v1` compatibility promise covers wire behavior, not the
  crate's Rust source API as a separate surface. Rust API compatibility follows
  the crate package version and Cargo's semantic-versioning rules. A
  source-breaking DTO or operation-typing change requires an appropriate package
  version change even when the admitted wire JSON remains compatible with v1;
  before `1.0.0`, a minor package-version change may break the Rust API.
  Consumers that copy only the schemas receive no Rust source-compatibility
  promise.
- Launch NDJSON uses one request identity, starts at sequence 1, increments by
  one, and ends with exactly one `exit` event. No event follows `exit`. The
  imported launch schema's intrinsic "result" wording refers to that terminal
  event; launch has no separate result or response envelope.
- Contract compatibility does not confer runtime authority. Agent Runner remains
  responsible for logical session identity, ancestry, runtime request/session
  admission, scheduling, mailbox delivery, and provider selection. The contract
  crate separately owns provider/v1 wire/schema admission through its
  operation-bound decode and encode APIs.
- A provider-produced `host_state_plan` is an untrusted proposal under this wire
  contract, not an executable Runner command. Schema and DTO admission establish
  its representation only. Runner owns precondition and authority validation and
  translates an accepted proposal into its private state mutation protocol;
  providers never apply host state and consumers must not execute `db_apply`
  directly.

The SDK contract contains no model-label migration. Existing model and provider
labels remain outside this wire contract and are not renamed or overridden.
