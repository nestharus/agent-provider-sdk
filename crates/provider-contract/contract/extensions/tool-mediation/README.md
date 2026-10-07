# Tool mediation v1

`oulipoly.tool_mediation/v1` is a host-selected provider/v1 extension by which a
host gives a provider its Bash tool policy and its Bash requester and ingress,
and the provider makes the mediated `bash` tool its native agent's only command
tool. It adds no envelope, subcommand or prepare field.

## Selection

A host offers version 1 in `describe` with
`host.env.OULIPOLY_HOST_TOOL_MEDIATION_V1=1`. A provider that implements it
answers `capabilities.tool_mediation_v1: true` and otherwise omits it. A host
must not give a policy to a provider that did not advertise it: that provider
would not know to honour it. A request whose own `host.env` selects version 1
must carry a policy; one that does not is refused.

## Payload

[v1.schema.json](v1.schema.json) `ToolMediation`, JSON-encoded in the launch
environment variable `OULIPOLY_TOOL_MEDIATION_V1`:

```json
{"protocol": "oulipoly.tool_mediation/v1",
 "bash": {"allow": ["cargo test --locked"]},
 "requester": "/opt/agent-bash/agent-bash",
 "ingress_env": "OULIPOLY_ROOT_BASH_V1"}
```

`bash` is exactly one of `{"allow": [whole command strings]}` (exact match;
anything else is refused and nothing runs) or `{"authority": "trusted-task"}`
(any command, as the host's once-per-task authority). `requester` is the host's
Bash requester with the agent-bash root v1 requester surface. `ingress_env`
names the variable through which the host gives the serving process its Bash
ingress; its value is not part of the policy.

The host places the object in `policy.evaluate` `launch.env`. Providers echo
launch environment entries into the evaluated `env`, so the same object reaches
one-shot launches and `resident.prepare` templates (`ResidentLaunchTemplate.env`)
and every resident turn.

## Provider obligations

- Admit the object strictly (unknown fields, another protocol version or a
  second policy form are refused), in `policy.evaluate` (`accepted: false` with
  an error diagnostic), at `resident.prepare`, and before every native start.
- Configure the native agent so the mediated tool is its only command tool,
  with the provider's own native tool names and switches, and report it as the
  `oulipoly.tool_mediation/v1` marker of `policy.evaluate`
  (`EffectiveMediation`: the policy, the mediated tool's native name and every
  native tool offered). What `trusted-task` adds besides the mediated tool is
  the provider's documented meaning. Refuse a template whose own native tool,
  permission, settings or MCP options would conflict.
- A launch that also carries an admitted
  [`oulipoly.exploration/v1`](../exploration/README.md) offer adds that
  extension's non-command `explore` tool from the same bridge. It runs no
  command, so the mediated tool remains the only command tool; its native name
  is listed in `native_tools`. Without that offer the bridge serves `bash`
  alone.
- Refuse a turn whose serving process lacks `ingress_env` before any native
  effect, so no requester can fall back to another route.
- Serve the tool with `agent_provider_execution::tool_bridge` or an equivalent
  that never starts the requester for a refused command.

The marker and refusals are construction evidence. Whether a native CLI
actually offers no other tool must be qualified against that CLI; logical
authority, the ingress, attribution and owner-side run custody remain the
host's.
