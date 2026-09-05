//! Opt-in provider-neutral terminal-unavailable/v1 extension.
//!
//! This module does not change the pinned provider/v1 schema registry. Consumers
//! must still admit the enclosing response or event using their selected route
//! schema; validating this payload alone does not admit an envelope.

use crate::generated::HostContext;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::OnceLock;

pub const PROTOCOL: &str = "oulipoly.terminal_unavailable/v1";
pub const HOST_SELECTION_ENV: &str = "OULIPOLY_HOST_TERMINAL_UNAVAILABLE_V1";
pub const HOST_SELECTION_VALUE: &str = "1";
pub const SCHEMA_JSON: &str =
    include_str!("../contract/extensions/terminal-unavailable/v1.schema.json");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderUnavailableKind {
    #[serde(rename = "provider_unavailable")]
    ProviderUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderUnavailableSignal {
    pub kind: ProviderUnavailableKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evidence: Option<String>,
    pub observed_at_unix_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AdmissionError {
    #[error("terminal-unavailable/v1 was not selected by this request's host")]
    NotSelected,
    #[error("invalid terminal-unavailable/v1 signal")]
    InvalidSignal,
}

/// Uses only the explicit current request, never the ambient process environment.
pub fn host_selected(host: &HostContext) -> bool {
    host.env.get(HOST_SELECTION_ENV).map(String::as_str) == Some(HOST_SELECTION_VALUE)
}

/// Admits the extension payload after checking explicit host selection.
/// Error messages never include the submitted evidence or other payload values.
pub fn decode_signal(
    host: &HostContext,
    value: &Value,
) -> Result<ProviderUnavailableSignal, AdmissionError> {
    if !host_selected(host) {
        return Err(AdmissionError::NotSelected);
    }
    static VALIDATOR: OnceLock<jsonschema::Validator> = OnceLock::new();
    let validator = VALIDATOR.get_or_init(|| {
        let schema: Value = serde_json::from_str(SCHEMA_JSON).expect("embedded extension JSON");
        jsonschema::validator_for(&schema).expect("embedded extension schema")
    });
    if !validator.is_valid(value) {
        return Err(AdmissionError::InvalidSignal);
    }
    serde_json::from_value(value.clone()).map_err(|_| AdmissionError::InvalidSignal)
}
