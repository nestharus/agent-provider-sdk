//! Host-selected `oulipoly.resident_session/v1` extension.
//!
//! A host that selects the extension in `describe`
//! (`host.env.OULIPOLY_HOST_RESIDENT_SESSION_V1=1`) and receives
//! `capabilities.resident_session_v1: true` may call the `resident.prepare`
//! subcommand with [`ResidentPrepareParams`]. The provider validates the
//! policy-evaluated launch template, durably records the resident
//! configuration and answers [`ResidentPrepareResult`]: the arguments the host
//! appends to the same registered provider executable to start a resident ACP
//! v2 endpoint on stdio, the ACP subset served and the operations offered.
//! The endpoint itself is `agent_provider_execution::resident`; this module
//! owns the wire vocabulary, its schema and version selection. Envelopes stay
//! the base provider/v1 request and success/error envelopes.
//!
//! Resident turns are provider/v1 launches of the recorded template, so their
//! custody, replay and output accounting are the shared one-shot lifecycle's.
//! Logical session identity, ancestry, admission and scheduling remain the
//! host's: the endpoint's session ids name provider-native resident sessions.

use crate::negotiation::{NoCommonVersion, VersionFamily};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::sync::OnceLock;

pub const PROTOCOL: &str = "oulipoly.resident_session/v1";
/// The provider subcommand that prepares a resident endpoint.
pub const PREPARE_SUBCOMMAND: &str = "resident.prepare";
pub const SCHEMA_JSON: &str =
    include_str!("../contract/extensions/resident-session/v1.schema.json");
/// ACP protocol version served by a resident endpoint.
pub const ACP_PROTOCOL_VERSION: u64 = 2;
/// ACP v2 draft schema tag whose subset is served.
pub const ACP_SCHEMA_TAG: &str = "schema-v2.0.0-alpha.7";
/// Resident-session versions this SDK release defines.
pub const SUPPORTED_VERSIONS: &[u32] = &[1];
/// Version family: selector `OULIPOLY_HOST_RESIDENT_SESSION_V<n>`, capability
/// `resident_session_v<n>`.
pub const FAMILY: VersionFamily = VersionFamily {
    selector_prefix: "OULIPOLY_HOST_RESIDENT_SESSION_V",
    capability_prefix: "resident_session_v",
};
/// Operations a v1 endpoint serves.
pub const OPERATIONS: &[&str] = &[
    "initialize",
    "session/new",
    "session/resume",
    "session/prompt",
    "session/cancel",
    "session/close",
    "session/list",
];

/// Policy-evaluated launch inputs reused by every resident turn.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResidentLaunchTemplate {
    pub settings_id: String,
    pub mode: String,
    pub model: Value,
    pub argv: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResidentPrepareParams {
    pub protocol: String,
    pub launch: ResidentLaunchTemplate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResidentInvocation {
    /// Appended to the registered provider executable the host invoked.
    pub args: Vec<String>,
    /// `stdio`: ACP v2 as newline-delimited JSON on the process's stdin/stdout.
    pub endpoint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResidentAcp {
    pub protocol_version: u64,
    pub schema: String,
    pub dedup_contract: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResidentPrepareResult {
    pub protocol: String,
    pub invocation: ResidentInvocation,
    pub acp: ResidentAcp,
    pub config_sha256: String,
    pub operations: Vec<String>,
}

impl ResidentPrepareResult {
    /// The v1 result for `args` and the recorded configuration digest.
    pub fn v1(args: Vec<String>, config_sha256: String) -> Self {
        Self {
            protocol: PROTOCOL.to_owned(),
            invocation: ResidentInvocation {
                args,
                endpoint: "stdio".to_owned(),
            },
            acp: ResidentAcp {
                protocol_version: ACP_PROTOCOL_VERSION,
                schema: ACP_SCHEMA_TAG.to_owned(),
                dedup_contract: 1,
            },
            config_sha256,
            operations: OPERATIONS.iter().map(|op| (*op).to_owned()).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AdmissionError {
    #[error("resident-session/v1 {definition} is invalid: {errors:?}")]
    Invalid {
        definition: &'static str,
        errors: Vec<String>,
    },
}

fn validator(definition: &'static str) -> &'static jsonschema::Validator {
    static VALIDATORS: OnceLock<BTreeMap<&'static str, jsonschema::Validator>> = OnceLock::new();
    let validators = VALIDATORS.get_or_init(|| {
        let schema: Value = serde_json::from_str(SCHEMA_JSON).expect("embedded extension JSON");
        [
            "ResidentPrepareParams",
            "ResidentPrepareResult",
            "ResidentSessionMeta",
            "NativeTurnMeta",
            "TurnStopReason",
        ]
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
pub fn validate(definition: &'static str, value: &Value) -> Result<(), AdmissionError> {
    let mut errors: Vec<String> = validator(definition)
        .iter_errors(value)
        .map(|error| error.to_string())
        .collect();
    errors.sort();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(AdmissionError::Invalid { definition, errors })
    }
}

/// Admits `resident.prepare` params.
pub fn decode_prepare_params(value: &Value) -> Result<ResidentPrepareParams, AdmissionError> {
    validate("ResidentPrepareParams", value)?;
    serde_json::from_value(value.clone()).map_err(|error| AdmissionError::Invalid {
        definition: "ResidentPrepareParams",
        errors: vec![error.to_string()],
    })
}

/// Admits a `resident.prepare` result.
pub fn decode_prepare_result(value: &Value) -> Result<ResidentPrepareResult, AdmissionError> {
    validate("ResidentPrepareResult", value)?;
    serde_json::from_value(value.clone()).map_err(|error| AdmissionError::Invalid {
        definition: "ResidentPrepareResult",
        errors: vec![error.to_string()],
    })
}

/// Provider side: advertise `resident_session_v<n>` for offered versions in
/// `provider_supported`.
pub fn advertise(
    capabilities: &mut Map<String, Value>,
    provider_supported: &[u32],
    host_env: Option<&BTreeMap<String, String>>,
) {
    FAMILY.advertise_into(capabilities, provider_supported, host_env);
}

/// Host side: the resident version to use with a described provider.
pub fn select(
    host_supported: &[u32],
    capabilities: &Map<String, Value>,
) -> Result<u32, NoCommonVersion> {
    FAMILY.select(host_supported, capabilities)
}
