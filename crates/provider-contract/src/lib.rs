//! Versioned provider-neutral contract shared by Agent Runner and provider adapters.
//!
//! This crate describes and validates `oulipoly.provider/v1`. It does not own
//! provider execution, logical sessions, scheduling, runtime request/session
//! admission, or process custody.
//! Raw Serde deserialization is a representation operation, not wire admission;
//! use the operation-bound `decode_*` and `encode_*` APIs at wire boundaries.

pub mod generated;
pub mod launch_stream;
pub mod operations;
pub mod schemas;
pub mod terminal_unavailable;

#[cfg(feature = "contract-test-fixtures")]
pub mod fixtures {
    use serde_json::Value;

    /// Canonical valid conformance fixtures for every v1 subcommand and launch event.
    pub const CONTRACT_V1_JSON: &str = include_str!("../tests/fixtures/contract_v1/fixtures.json");
    /// Canonical invalid request and response fixtures.
    pub const INVALID_CONTRACT_V1_JSON: &str =
        include_str!("../tests/fixtures/contract_v1/invalid.json");
    /// Canonical invalid launch stream with a mismatched request identity.
    pub const INVALID_LAUNCH_REQUEST_MISMATCH_NDJSON: &str =
        include_str!("../tests/fixtures/contract_v1/invalid-launch-request-mismatch.ndjson");

    pub fn contract_v1() -> Value {
        serde_json::from_str(CONTRACT_V1_JSON).expect("embedded contract fixtures are valid JSON")
    }

    pub fn invalid_contract_v1() -> Value {
        serde_json::from_str(INVALID_CONTRACT_V1_JSON)
            .expect("embedded invalid contract fixture document is valid JSON")
    }
}

pub use generated::CONTRACT_VERSION;
pub use launch_stream::{validate_launch_ndjson, LaunchStreamError, ValidatedLaunchStream};
pub use operations::{Admitted, RequestOperation, ResponseOperation};
pub use schemas::{
    decode_error_response, decode_request, decode_response, encode_error_response, encode_request,
    encode_response, validate_error_response, validate_launch_event, validate_request,
    validate_response, ContractAdmissionError, SchemaRegistry, SchemaValidationError,
};
