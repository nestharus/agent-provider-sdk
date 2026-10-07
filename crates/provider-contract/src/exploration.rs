//! Host-selected `oulipoly.exploration/v1` extension.
//!
//! A host that selects the extension in `describe`
//! (`host.env.OULIPOLY_HOST_EXPLORATION_V1=1`) and receives
//! `capabilities.exploration_v1: true` may offer a registered parent agent
//! child exploration: opaque route labels, its child requester and the name
//! of its owner ingress variable, as one [`Exploration`] object JSON-encoded
//! in the launch environment variable [`ENV`]. It travels like
//! [`crate::tool_mediation`]'s object (`policy.evaluate` `launch.env`, the
//! evaluated `env`, `ResidentLaunchTemplate.env`, every resident turn) and
//! only together with it ([`admit`]).
//!
//! Selection is negotiation, the object is the offer. A launch without the
//! object has no exploration: the agent keeps the mediated `bash` tool alone,
//! exactly as before. A launch with it gets one more tool from the same
//! bridge (`agent_provider_execution::tool_bridge`): a logical exploration
//! tool that runs no command and asks the host's owner, through the host's
//! requester, for one child on an offered route. Bash stays the only command
//! tool.
//!
//! Route labels are opaque: they name no provider, model or account here.
//! The owner admits or refuses every child (route, depth, starts,
//! concurrency, prepared slots) and reports its outcome; offering a label is
//! not admission. A host never offers exploration to a child's own launch,
//! and the owner refuses a child's request regardless. The native tool name,
//! CLI switches and inventory are the provider's.

use crate::negotiation::{NoCommonVersion, VersionFamily};
use crate::tool_mediation::ToolMediation;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::sync::OnceLock;

pub const PROTOCOL: &str = "oulipoly.exploration/v1";
/// Launch environment variable carrying the JSON-encoded [`Exploration`].
pub const ENV: &str = "OULIPOLY_EXPLORATION_V1";
/// Name of the `policy.evaluate` marker whose value is an
/// [`EffectiveExploration`].
pub const MARKER: &str = PROTOCOL;
pub const SCHEMA_JSON: &str = include_str!("../contract/extensions/exploration/v1.schema.json");
/// Exploration versions this SDK release defines.
pub const SUPPORTED_VERSIONS: &[u32] = &[1];
/// Version family: selector `OULIPOLY_HOST_EXPLORATION_V<n>`, capability
/// `exploration_v<n>`.
pub const FAMILY: VersionFamily = VersionFamily {
    selector_prefix: "OULIPOLY_HOST_EXPLORATION_V",
    capability_prefix: "exploration_v",
};

/// The root's child limits as offered, for the agent's information only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_starts: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_concurrent: Option<u32>,
}

/// The host's exploration offer for one launch or resident template.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Exploration {
    pub protocol: String,
    /// Opaque offered route labels, unique, in the host's order.
    pub routes: Vec<String>,
    /// Absolute path of the host's child requester (root-child v1
    /// requester surface).
    pub requester: String,
    /// Name of the variable that names the host's owner ingress.
    pub ingress_env: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits: Option<Limits>,
}

/// What a provider reports it configured for an offer (the [`MARKER`]
/// value).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectiveExploration {
    pub protocol: String,
    pub routes: Vec<String>,
    /// Native name of the exploration tool.
    pub tool: String,
    pub ingress_env: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ExplorationError {
    #[error("{ENV} is not a valid {PROTOCOL} object: {0}")]
    Invalid(String),
    #[error("{ENV} offered, but this request's host did not select {PROTOCOL}")]
    NotSelected,
    #[error("{PROTOCOL} is offered only with oulipoly.tool_mediation/v1, which this launch lacks")]
    WithoutMediation,
    #[error("{0} names no owner ingress in this process; nothing may be asked")]
    NoIngress(String),
}

fn validator(definition: &'static str) -> &'static jsonschema::Validator {
    static VALIDATORS: OnceLock<BTreeMap<&'static str, jsonschema::Validator>> = OnceLock::new();
    let validators = VALIDATORS.get_or_init(|| {
        let schema: Value = serde_json::from_str(SCHEMA_JSON).expect("embedded extension JSON");
        ["Exploration", "EffectiveExploration", "RouteLabel"]
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
pub fn validate(definition: &'static str, value: &Value) -> Result<(), ExplorationError> {
    let mut errors: Vec<String> = validator(definition)
        .iter_errors(value)
        .map(|error| error.to_string())
        .collect();
    errors.sort();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ExplorationError::Invalid(format!(
            "{definition}: {errors:?}"
        )))
    }
}

impl Exploration {
    /// Admits the JSON text of [`ENV`]: schema-strict, so an unknown field,
    /// another protocol version, a duplicate or malformed label is refused.
    pub fn decode(text: &str) -> Result<Self, ExplorationError> {
        let value: Value = serde_json::from_str(text)
            .map_err(|error| ExplorationError::Invalid(error.to_string()))?;
        validate("Exploration", &value)?;
        serde_json::from_value(value).map_err(|error| ExplorationError::Invalid(error.to_string()))
    }

    /// The JSON text a host places in [`ENV`].
    pub fn encode(&self) -> String {
        serde_json::to_string(self).expect("exploration serializes")
    }

    /// The offer carried by a launch environment, if any.
    pub fn from_env(
        env: Option<&BTreeMap<String, String>>,
    ) -> Result<Option<Self>, ExplorationError> {
        env.and_then(|env| env.get(ENV))
            .map(|text| Self::decode(text))
            .transpose()
    }

    /// The offered route a call names, or the only one offered when it
    /// names none. `Err` says why nothing may be asked.
    pub fn route(&self, named: Option<&str>) -> Result<String, String> {
        match named {
            Some(route) if self.routes.iter().any(|offered| offered == route) => {
                Ok(route.to_owned())
            }
            Some(route) => Err(format!(
                "route {} is not offered to this agent (offered: {})",
                serde_json::to_string(route).expect("string serializes"),
                self.routes.join(", ")
            )),
            None if self.routes.len() == 1 => Ok(self.routes[0].clone()),
            None => Err(format!(
                "no route named and more than one is offered ({})",
                self.routes.join(", ")
            )),
        }
    }

    /// The ingress value from `lookup` (the serving process's environment),
    /// or a refusal: without it a requester could reach no owner.
    pub fn ingress(
        &self,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> Result<String, ExplorationError> {
        lookup(&self.ingress_env)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| ExplorationError::NoIngress(self.ingress_env.clone()))
    }

    /// The marker value a provider reports for this offer.
    pub fn effective(&self, tool: &str) -> EffectiveExploration {
        EffectiveExploration {
            protocol: PROTOCOL.to_owned(),
            routes: self.routes.clone(),
            tool: tool.to_owned(),
            ingress_env: self.ingress_env.clone(),
        }
    }
}

/// Provider side: the launch's offer, if any. Absent is no exploration (the
/// mediated bash tool alone). Present, it must have been selected by this
/// request's own `host_env` and accompany a tool mediation policy
/// (`mediation`, as admitted by [`crate::tool_mediation::required_by_host`]);
/// otherwise the request is refused rather than the offer ignored or served
/// unmediated.
pub fn admit(
    host_env: Option<&BTreeMap<String, String>>,
    launch_env: Option<&BTreeMap<String, String>>,
    mediation: Option<&ToolMediation>,
) -> Result<Option<Exploration>, ExplorationError> {
    let Some(offer) = Exploration::from_env(launch_env)? else {
        return Ok(None);
    };
    if !FAMILY.selected_by_host(host_env).contains(&1) {
        return Err(ExplorationError::NotSelected);
    }
    if mediation.is_none() {
        return Err(ExplorationError::WithoutMediation);
    }
    Ok(Some(offer))
}

/// Provider side: advertise `exploration_v<n>` for offered versions in
/// `provider_supported`.
pub fn advertise(
    capabilities: &mut Map<String, Value>,
    provider_supported: &[u32],
    host_env: Option<&BTreeMap<String, String>>,
) {
    FAMILY.advertise_into(capabilities, provider_supported, host_env);
}

/// Host side: the exploration version to use with a described provider. A
/// provider that does not advertise one is offered no exploration: its
/// parent keeps Bash alone.
pub fn select(
    host_supported: &[u32],
    capabilities: &Map<String, Value>,
) -> Result<u32, NoCommonVersion> {
    FAMILY.select(host_supported, capabilities)
}
