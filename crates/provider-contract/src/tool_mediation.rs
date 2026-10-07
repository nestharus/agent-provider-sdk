//! Host-selected `oulipoly.tool_mediation/v1` extension.
//!
//! A host that selects the extension in `describe`
//! (`host.env.OULIPOLY_HOST_TOOL_MEDIATION_V1=1`) and receives
//! `capabilities.tool_mediation_v1: true` supplies its Bash tool policy, its
//! Bash requester and the name of its Bash ingress variable as one
//! [`ToolMediation`] object, JSON-encoded in the launch environment variable
//! [`ENV`]. It travels through the existing channels unchanged: the host puts
//! it in `policy.evaluate` `launch.env`, the evaluated `env` carries it into a
//! launch or a resident template (`ResidentLaunchTemplate.env`), and every
//! resident turn reuses it. No `ResidentPrepare` field is added.
//!
//! A provider that finds the variable either configures its native agent so
//! that the mediated `bash` tool is its only command tool, under exactly that
//! policy and through exactly that requester, or refuses the request clearly.
//! It never ignores it and never widens it. A host that selected the
//! extension and supplies no policy is refused too
//! ([`required_by_host`]). The native configuration (tool names, CLI flags,
//! MCP registration, built-in tool switches) is the provider's; this module
//! owns only the vocabulary and its admission. Logical authority, the ingress
//! itself and the requester stay the host's.
//!
//! The ingress variable's *value* is not part of the policy: the host gives it
//! to the process that serves a turn (for a resident endpoint, the harness
//! environment), and a turn whose serving process lacks it is refused before
//! any native effect ([`ToolMediation::ingress`]), so a requester never falls
//! back to another route.

use crate::negotiation::{NoCommonVersion, VersionFamily};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::sync::OnceLock;

pub const PROTOCOL: &str = "oulipoly.tool_mediation/v1";
/// Launch environment variable carrying the JSON-encoded [`ToolMediation`].
pub const ENV: &str = "OULIPOLY_TOOL_MEDIATION_V1";
/// Name of the `policy.evaluate` marker whose value is an
/// [`EffectiveMediation`].
pub const MARKER: &str = PROTOCOL;
pub const SCHEMA_JSON: &str = include_str!("../contract/extensions/tool-mediation/v1.schema.json");
/// Tool-mediation versions this SDK release defines.
pub const SUPPORTED_VERSIONS: &[u32] = &[1];
/// Version family: selector `OULIPOLY_HOST_TOOL_MEDIATION_V<n>`, capability
/// `tool_mediation_v<n>`.
pub const FAMILY: VersionFamily = VersionFamily {
    selector_prefix: "OULIPOLY_HOST_TOOL_MEDIATION_V",
    capability_prefix: "tool_mediation_v",
};
/// The one wider Bash authority v1 defines.
pub const TRUSTED_TASK: &str = "trusted-task";

/// What the mediated `bash` tool may run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum BashPolicy {
    /// Only these whole command strings, matched exactly.
    Allow { allow: Vec<String> },
    /// Any command, as the host's once-per-task authority (`trusted-task`).
    Authority { authority: String },
}

/// The host's tool mediation for one launch or resident template.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolMediation {
    pub protocol: String,
    pub bash: BashPolicy,
    /// Absolute path of the host's Bash requester (agent-bash root v1
    /// requester surface).
    pub requester: String,
    /// Name of the variable that names the host's Bash ingress.
    pub ingress_env: String,
}

/// What a provider reports it configured for a policy (the
/// [`MARKER`] value).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectiveMediation {
    pub protocol: String,
    pub bash: BashPolicy,
    /// Native name of the mediated bash tool.
    pub tool: String,
    /// Every native tool the agent is offered.
    pub native_tools: Vec<String>,
    pub ingress_env: String,
}

/// The mediated tool's decision for one command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    Run,
    /// Refused by policy; nothing may run.
    Refuse(String),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MediationError {
    #[error("{ENV} is not a valid {PROTOCOL} object: {0}")]
    Invalid(String),
    #[error("host selected {PROTOCOL} but supplied no {ENV}")]
    Missing,
    #[error("{0} names no Bash ingress in this process; nothing may run")]
    NoIngress(String),
}

fn validator(definition: &'static str) -> &'static jsonschema::Validator {
    static VALIDATORS: OnceLock<BTreeMap<&'static str, jsonschema::Validator>> = OnceLock::new();
    let validators = VALIDATORS.get_or_init(|| {
        let schema: Value = serde_json::from_str(SCHEMA_JSON).expect("embedded extension JSON");
        ["ToolMediation", "EffectiveMediation", "BashPolicy"]
            .into_iter()
            .map(|name| {
                let wrapper = serde_json::json!({
                    "$schema": "https://json-schema.org/draft/2020-12/schema",
                    "$defs": schema["$defs"],
                    "$ref": format!("#/$defs/{name}"),
                });
                (
                    name,
                    jsonschema::validator_for(&wrapper).expect("embedded extension schema"),
                )
            })
            .collect()
    });
    &validators[definition]
}

/// Validates `value` against one extension definition.
pub fn validate(definition: &'static str, value: &Value) -> Result<(), MediationError> {
    let mut errors: Vec<String> = validator(definition)
        .iter_errors(value)
        .map(|error| error.to_string())
        .collect();
    errors.sort();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(MediationError::Invalid(format!("{definition}: {errors:?}")))
    }
}

impl ToolMediation {
    /// Admits the JSON text of [`ENV`]: schema-strict, so an unknown field,
    /// another protocol version or a second policy form is refused.
    pub fn decode(text: &str) -> Result<Self, MediationError> {
        let value: Value = serde_json::from_str(text)
            .map_err(|error| MediationError::Invalid(error.to_string()))?;
        validate("ToolMediation", &value)?;
        serde_json::from_value(value).map_err(|error| MediationError::Invalid(error.to_string()))
    }

    /// The JSON text a host places in [`ENV`].
    pub fn encode(&self) -> String {
        serde_json::to_string(self).expect("tool mediation serializes")
    }

    /// The policy carried by a launch environment, if any.
    pub fn from_env(
        env: Option<&BTreeMap<String, String>>,
    ) -> Result<Option<Self>, MediationError> {
        env.and_then(|env| env.get(ENV))
            .map(|text| Self::decode(text))
            .transpose()
    }

    /// Whether the policy is the wider `trusted-task` authority.
    pub fn trusted_task(&self) -> bool {
        matches!(&self.bash, BashPolicy::Authority { .. })
    }

    /// The policy decision for one whole command string.
    pub fn decide(&self, command: &str) -> Decision {
        match &self.bash {
            BashPolicy::Authority { .. } => Decision::Run,
            BashPolicy::Allow { allow } if allow.iter().any(|named| named == command) => {
                Decision::Run
            }
            BashPolicy::Allow { .. } => Decision::Refuse(format!(
                "Denied by this root's bash policy: {} is not a command it names. Nothing was run.",
                serde_json::to_string(command).expect("string serializes")
            )),
        }
    }

    /// The ingress value from `lookup` (the serving process's environment),
    /// or a refusal: without it a requester could reach no owner or another
    /// route, so nothing may run.
    pub fn ingress(
        &self,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> Result<String, MediationError> {
        lookup(&self.ingress_env)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| MediationError::NoIngress(self.ingress_env.clone()))
    }

    /// The marker value a provider reports for this policy.
    pub fn effective(&self, tool: &str, native_tools: Vec<String>) -> EffectiveMediation {
        EffectiveMediation {
            protocol: PROTOCOL.to_owned(),
            bash: self.bash.clone(),
            tool: tool.to_owned(),
            native_tools,
            ingress_env: self.ingress_env.clone(),
        }
    }
}

/// Provider side: the launch's policy, refusing when the request's host
/// selected the extension (`host_env`) and the launch env supplies none.
pub fn required_by_host(
    host_env: Option<&BTreeMap<String, String>>,
    launch_env: Option<&BTreeMap<String, String>>,
) -> Result<Option<ToolMediation>, MediationError> {
    let policy = ToolMediation::from_env(launch_env)?;
    if policy.is_none() && FAMILY.selected_by_host(host_env).contains(&1) {
        return Err(MediationError::Missing);
    }
    Ok(policy)
}

/// Provider side: advertise `tool_mediation_v<n>` for offered versions in
/// `provider_supported`.
pub fn advertise(
    capabilities: &mut Map<String, Value>,
    provider_supported: &[u32],
    host_env: Option<&BTreeMap<String, String>>,
) {
    FAMILY.advertise_into(capabilities, provider_supported, host_env);
}

/// Host side: the tool-mediation version to use with a described provider.
/// A provider that does not advertise one cannot be given a policy: it would
/// not know to honour it.
pub fn select(
    host_supported: &[u32],
    capabilities: &Map<String, Value>,
) -> Result<u32, NoCommonVersion> {
    FAMILY.select(host_supported, capabilities)
}
