# Exploration v1

`oulipoly.exploration/v1` is a host-selected provider/v1 extension by which a
host offers a registered parent agent child exploration: opaque route labels,
the host's child requester and the name of its owner ingress variable. The
provider exposes it as one more tool of the same mediated bridge that serves
`bash`. The tool runs no command. Bash stays the only command tool, and the
host's owner admits or refuses every child.

## Selection and offer

A host offers version 1 in `describe` with
`host.env.OULIPOLY_HOST_EXPLORATION_V1=1`. A provider that implements it
answers `capabilities.exploration_v1: true` and otherwise omits it. The version
is selected independently of tool mediation and is not a binary, package or
source revision.

Selection is negotiation, not an offer. The offer is the object below in a
launch's environment:

- **Absent:** the launch has no exploration. The agent keeps the mediated
  `bash` tool alone, exactly as before this extension. A host offers nothing
  to a parent with no permitted child routes.
- **Present:** the request's own `host.env` must select version 1, and the
  launch must also carry `oulipoly.tool_mediation/v1`. Otherwise the provider
  refuses; it never ignores the offer or serves it unmediated.
- **Never offered to a child:** a host never offers exploration to a child's
  own launch. The owner refuses a child's request (`depth`) regardless.

A host must not offer exploration to a provider that did not advertise it.

## Payload

[v1.schema.json](v1.schema.json) `Exploration`, JSON-encoded in the launch
environment variable `OULIPOLY_EXPLORATION_V1`:

```json
{"protocol": "oulipoly.exploration/v1",
 "routes": ["luna-max"],
 "requester": "/opt/agent-runner/oulipoly-root-child",
 "ingress_env": "OULIPOLY_ROOT_BASH_V1",
 "limits": {"max_starts": 4, "max_concurrent": 2}}
```

- `routes`: opaque, unique site labels (1–64 characters of `[A-Za-z0-9._-]`,
  not starting with `.`, `_` or `-`). They name no provider, model or account
  to the provider. Offering a label is not admission.
- `requester`: the host's child requester (below).
- `ingress_env`: names the variable through which the host gives the serving
  process its owner ingress. Its value is not part of the object.
- `limits` (optional): the root's child limits, for the agent's information.
  The owner may refuse sooner.

Admission is schema-strict: unknown fields, another version or a malformed or
duplicate label are refused. Later evolution is a new version, selected the
same way.

The host places the object in `policy.evaluate` `launch.env` beside the tool
mediation object. Providers echo launch environment entries into the evaluated
`env`, so it reaches `resident.prepare` templates and every resident turn
unchanged.

## Root-child v1 requester surface

`REQUESTER ROUTE PROMPT`. The requester connects to the owner named by
`ingress_env` from the caller's own process tree, so the owner attributes the
request to that agent's work. It asks for one child on ROUTE and keeps the
request open while the child lives; the owner stops the child if the
requester goes away.

| Exit | Stdout | Meaning |
| --- | --- | --- |
| 0 | the owner's final `result` object | The child's outcome and its facts (below). |
| 65 | the owner's `refused` object with `reason` | Nothing was admitted or started; no start or slot was used. |
| 75 | a `lost` object | The request may have reached the owner; a child may have been admitted and run. |
| 69 | nothing | No ingress, or the owner was unreachable before anything was sent. |
| 64 | nothing | Arguments refused; nothing was sent. |

Stderr may relay the owner's stage lines as `LABEL: JSON` (`accepted`,
`started`, `launch-failed`, `launch-unknown`, `turn-end`, `stopping`, `end`,
`end-unknown`, `agent-message`, …).

The owner's `result` carries:

- `child`, `route` and `outcome`: one of `answered`, `no-answer`,
  `launch-failed`, `launch-unknown`, `stopped` or `ended-without-turn-end`;
- `answer`: the child's last linked text before its tagged turn end;
- `turn_end`, `stopped`, `launch` (with `not_started` and `setup_effects`),
  `end` (`end` with the waiter's status, or `end-unknown`);
- `lifecycle`: `end` (`observed`, `unknown` or `no-process`),
  `bash_runs_open`, `bash_run_end_unknown` and `budget`.

The delivered Agent Runner `oulipoly-root-child` is this surface.

## Provider obligations

- Admit the object with `exploration::admit` (selection and tool mediation
  required, strict payload) in `policy.evaluate` (`accepted: false` with an
  error diagnostic) and at `resident.prepare`. Re-decode it before every
  native start.
- Serve it with `agent_provider_execution::tool_bridge` (the same `tool.bridge`
  subcommand as `bash`). The bridge reads `OULIPOLY_EXPLORATION_V1` from its
  own environment and adds the `explore` tool only when the object is present.
  An invalid object makes the bridge refuse to start (exit 2), as an invalid
  tool mediation policy does.
- Pass the object to the bridge only when admitted, and allow the native agent
  that one additional tool under the provider's own native name.
- Report the `oulipoly.exploration/v1` marker of `policy.evaluate`
  (`EffectiveExploration`: the routes and the tool's native name), and list
  that native name in the tool-mediation marker's `native_tools`.
- Add no other tool, route, model or account choice for it.

## What the bridge renders

The `explore` tool takes `{"question": Q, "route"?: R}`. R must be an offered
label, or omitted when exactly one route is offered. A missing, unoffered or
ambiguous route, invalid arguments and a missing ingress start no requester
and say that nothing was asked.

Otherwise the bridge runs only `requester ROUTE Q` and renders what that
requester established:

- **Refusal** (exit 65): the owner's reason, with a meaning for the known
  kinds. These stay distinct: `route-slots-used` (a route's prepared slots,
  which failed launches also use), `budget-starts` (the root's total starts),
  `budget-concurrent` (children at once, unknown ends included),
  `route-not-allowed`, `depth`, `parent-not-current` and closing. Nothing was
  admitted.
- **Result** (exit 0): the outcome; the answer, marked unverified;
  `stopped`; the launch facts; the end; and the lifecycle qualifiers.
  - Launch facts: a positive no-start; work created but launch failed, which
    is not a no-start and may have setup effects; or launch unknown.
  - The end: the waiter's status, or unknown.
  - Lifecycle: end observed, unknown or no process; open Bash runs; budget.
  - `answered` is the only non-error outcome, and it concerns the answer's
    content only, not the child's end, its Bash's drain, correctness, or the
    parent's use of it.
  - Example: a work-created exec failure is rendered as
    `ended-without-turn-end` with "work process created, launch failed",
    `code:127` and no answer. Neither the final label nor the number alone is
    read as a no-start or as running.
- **Not sent** (69 or 64, empty stdout, no relayed stage): nothing was asked.
- **Anything else is unresolved:** `lost`, another exit or signal, a
  malformed or mismatched object, a result for another route, an unknown
  outcome, or facts inconsistent with the outcome. A child may have been
  admitted and run. When a relayed `accepted` stage was seen, the bridge says
  it was durably admitted. The bridge never asks again automatically; a new
  call is a new admission.

Unknown additional result fields are ignored. Relayed stages are shown as the
lifecycle and never replace the final result.

MCP cancellation and the bridge's connection end kill the call's requester and
send no answer. The owner then stops the child (`requester-gone`), so a
cancelled child is not replayed and its outcome is the owner's.

## Limits

These are construction evidence. The following stay with their owners:

- **Providers:** whether a native CLI offers exactly the reported tools,
  honours the native name, and passes the object only when admitted. This
  must be qualified against each CLI.
- **The host:** logical authority, route meaning, admission, slots, budgets,
  attribution and child custody.
- **Not established by this extension:** a technical read-only barrier (a
  child inherits its parent's Bash authority), confidentiality between
  same-UID work, or the parent model's comprehension.
