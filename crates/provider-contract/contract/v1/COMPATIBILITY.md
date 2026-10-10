# Provider v1 Compatibility

`oulipoly.provider/v1` is the existing Agent Runner external-provider contract.
The SDK pins that boundary; it does not add a second host/provider protocol.

## Policy

- The 13 schemas, checked-in Rust DTO projection, subcommand matrix, and conformance fixtures
  form one release unit.
- Hosts and providers consume a released SDK version or a complete versioned
  schema snapshot whose local digests match its recorded provenance. They do not
  maintain private schema edits. These identities establish local provenance,
  not peer equality.
- Runtime compatibility follows declared supported wire schemas and capability
  agreement, including their established semantics. Peers may upgrade
  independently when that agreement remains compatible; source revisions,
  snapshot digests, SDK package versions, native CLI banners, and executable
  bytes must not be equality requirements for otherwise compatible updates.
  Sharing the `oulipoly.provider/v1` discriminator alone does not establish
  compatibility. Unsupported schema or capability meaning must fail closed.
- Before rollout, the route owner must retain the previous working route through
  replacement verification and the rollback decision. If it cannot meet that
  prerequisite, the upgrade does not start and the route continues on its
  previous configuration. Rollback restores that working route, including the
  compatible readers and durable state it requires; it does not require equal
  peer source revisions or snapshot digests.
- The crate and complete schema snapshot may be used and redistributed under
  the MIT License. Redistributors preserve the packaged upstream MIT notice.
  `UPSTREAM.md` records the imported schemas' source grant.
- A compatible v1 change must retain the contract discriminator and every
  established required behavior. Schema-specific unknown-field rules remain
  authoritative.
- A breaking wire change requires a new contract version and explicit
  negotiation. It must not be published as a silent v1 replacement. Version
  wire-format changes and retain conformance fixtures for older supported hosts.
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

Schema and capability declarations and structural validation do not prove
behavioral semantic preservation. Review and behavioral verification remain
necessary; different snapshots are not automatically compatible. The snapshot
is aligned with Agent Runner's current host copy plus the host-selected
`resident_session_v1` capability (see `UPSTREAM.md`); a host must adopt those
describe and common bytes before selecting that capability. This policy does
not qualify cross-build replay or updates.

Runner's registered native-root path uses SDK contract and host-selected
extension version selection with fresh schema/capability agreement per
invocation, reassessing a replacement on the next invocation. The ordinary
provider registry remains on fixed-v1 validation and capability agreement;
artifact-currentness checks invalidate cached describes/endpoints and trigger a
fresh describe, not common-version selection. The SDK's `negotiation` module
provides common-version selection. Do not advertise a second contract wire
version before the host can select a common supported version; extension
versions are advertised only when the host offered them.
