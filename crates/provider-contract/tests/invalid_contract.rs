pub mod support {
    pub mod contract_matrix;
}

use agent_provider_contract::generated as dto;
use agent_provider_contract::operations::{
    Describe, DiscoveryAccounts, DiscoveryModels, MigrationApply, MigrationPlan, SessionReplace,
    SettingsMigrate, SetupDetect,
};
use agent_provider_contract::{
    validate_launch_ndjson, LaunchStreamError, ResponseOperation, SchemaRegistry,
};
use serde_json::{json, Value};
use std::fs;
use std::path::PathBuf;
use support::contract_matrix::{fixtures, launch_event_fixture, non_launch_fixture};

const REQUEST_ID: &str = "req-launch";

#[test]
fn schema_validation_rejects_wrong_contract_unknown_fields_and_wrong_ok_discriminators() {
    let registry = SchemaRegistry::new();
    let fixtures = fixtures();
    let invalid = load_invalid_json();

    let wrong_contract = &invalid["describe_request_wrong_contract"];
    assert!(registry
        .validate_request("describe", wrong_contract)
        .is_err());

    let mut unknown_field = non_launch_fixture(&fixtures, "describe", "request").clone();
    unknown_field["unexpected"] = json!(true);
    assert!(registry
        .validate_request("describe", &unknown_field)
        .is_err());

    let false_success = &invalid["describe_response_wrong_ok"];
    assert!(registry
        .validate_response("describe", false_success)
        .is_err());

    let mut true_error = non_launch_fixture(&fixtures, "describe", "error_response").clone();
    true_error["ok"] = json!(true);
    assert!(registry
        .validate_error_response("describe", &true_error)
        .is_err());
}

#[test]
fn admitted_dtos_use_schema_rules_in_both_directions() {
    let registry = SchemaRegistry::new();
    let fixtures = fixtures();

    let mut wrong_contract = non_launch_fixture(&fixtures, "describe", "request").clone();
    wrong_contract["contract"] = json!("oulipoly.provider/v0");
    let wrong_contract_dto = serde_json::from_value::<dto::DescribeRequest>(wrong_contract.clone())
        .expect("raw DTO decoding is only representation");
    assert!(registry
        .decode_request::<Describe>(&wire_bytes(&wrong_contract))
        .is_err());
    assert!(registry
        .encode_request::<Describe>(&wrong_contract_dto)
        .is_err());

    let mut unknown_field = non_launch_fixture(&fixtures, "describe", "request").clone();
    unknown_field["unexpected"] = json!(true);
    assert!(registry
        .decode_request::<Describe>(&wire_bytes(&unknown_field))
        .is_err());

    let mut schema_open_error = non_launch_fixture(&fixtures, "describe", "error_response").clone();
    schema_open_error["process_status"] = json!({});
    let decoded = registry
        .decode_error_response::<Describe>(&wire_bytes(&schema_open_error))
        .expect("schema-valid opaque process status must decode");
    assert_eq!(decoded.value().process_status, Some(json!({})));
    assert!(registry
        .encode_error_response::<Describe>(decoded.value())
        .is_ok());

    let mut invalid_replace = non_launch_fixture(&fixtures, "session.replace", "request").clone();
    invalid_replace["params"]["replace_protocol"] = json!("private.replace/v1");
    assert!(registry
        .decode_request::<SessionReplace>(&wire_bytes(&invalid_replace))
        .is_err());
}

#[test]
fn schema_open_values_survive_typed_admission() {
    let registry = SchemaRegistry::new();
    let fixtures = fixtures();

    let mut concurrency = non_launch_fixture(&fixtures, "describe", "success_response").clone();
    concurrency["result"]["concurrency"] = json!({
        "safe_for_parallel_invocation": "provider-defined",
        "state_locking": {"kind": "external"}
    });
    registry
        .decode_response::<Describe>(&wire_bytes(&concurrency))
        .expect("schema-open concurrency must survive typed admission");

    assert_open_warnings_admit::<SettingsMigrate>(&registry, &fixtures);
    assert_open_warnings_admit::<DiscoveryModels>(&registry, &fixtures);
    assert_open_warnings_admit::<DiscoveryAccounts>(&registry, &fixtures);
    assert_open_warnings_admit::<SetupDetect>(&registry, &fixtures);
    assert_open_warnings_admit::<MigrationPlan>(&registry, &fixtures);
    assert_open_warnings_admit::<MigrationApply>(&registry, &fixtures);
}

#[test]
fn launch_ndjson_accepts_the_canonical_sequence() {
    let fixtures = fixtures();
    let stream = canonical_stream(&fixtures);
    let validated = validate_launch_ndjson(stream.as_bytes(), REQUEST_ID)
        .expect("canonical launch sequence must validate");
    assert_eq!(validated.request_id(), REQUEST_ID);
    assert_eq!(validated.event_count(), 5);
    assert_eq!(validated.events().len(), 5);
    assert!(matches!(validated.events()[0], dto::LaunchEvent::Stdout(_)));
    assert!(matches!(validated.events()[1], dto::LaunchEvent::Stderr(_)));
    assert!(matches!(validated.events()[2], dto::LaunchEvent::Marker(_)));
    assert!(matches!(
        validated.events()[3],
        dto::LaunchEvent::Heartbeat(_)
    ));
    assert!(matches!(validated.events()[4], dto::LaunchEvent::Exit(_)));
    assert_eq!(validated.exit_seq(), 5);
}

#[test]
fn launch_ndjson_rejects_malformed_correlation_payload_and_finality_neighbors() {
    assert!(matches!(
        validate_launch_ndjson(b"{not-json}\n", REQUEST_ID),
        Err(LaunchStreamError::MalformedLine { .. })
    ));

    let fixtures = fixtures();
    let mut wrong_contract = launch_event_fixture(&fixtures, "stdout").clone();
    wrong_contract["contract"] = json!("oulipoly.provider/v0");
    assert!(matches!(
        validate_launch_ndjson(json_line(&wrong_contract).as_bytes(), REQUEST_ID),
        Err(LaunchStreamError::MismatchedContract { .. })
    ));

    let wrong_request = fs::read(fixture_dir().join("invalid-launch-request-mismatch.ndjson"))
        .expect("read invalid launch fixture");
    assert!(matches!(
        validate_launch_ndjson(&wrong_request, REQUEST_ID),
        Err(LaunchStreamError::MismatchedRequestId { .. })
    ));

    let mut invalid_base64 = launch_event_fixture(&fixtures, "stdout").clone();
    invalid_base64["data_base64"] = json!("@@@");
    assert!(matches!(
        validate_launch_ndjson(json_line(&invalid_base64).as_bytes(), REQUEST_ID),
        Err(LaunchStreamError::InvalidBase64 { .. })
    ));

    let mut skipped_sequence = launch_event_fixture(&fixtures, "stdout").clone();
    skipped_sequence["seq"] = json!(2);
    assert!(matches!(
        validate_launch_ndjson(json_line(&skipped_sequence).as_bytes(), REQUEST_ID),
        Err(LaunchStreamError::InvalidSequence { .. })
    ));

    let stdout_only = json_line(launch_event_fixture(&fixtures, "stdout"));
    assert!(matches!(
        validate_launch_ndjson(stdout_only.as_bytes(), REQUEST_ID),
        Err(LaunchStreamError::MissingFinalExit)
    ));

    let exit = launch_event_fixture(&fixtures, "exit");
    let mut second_exit = exit.clone();
    second_exit["seq"] = json!(6);
    let duplicate = format!("{}{}", canonical_stream(&fixtures), json_line(&second_exit));
    assert!(matches!(
        validate_launch_ndjson(duplicate.as_bytes(), REQUEST_ID),
        Err(LaunchStreamError::DuplicateExit { .. })
    ));

    let mut after_exit = launch_event_fixture(&fixtures, "stdout").clone();
    after_exit["seq"] = json!(6);
    let after = format!("{}{}", canonical_stream(&fixtures), json_line(&after_exit));
    assert!(matches!(
        validate_launch_ndjson(after.as_bytes(), REQUEST_ID),
        Err(LaunchStreamError::EventAfterExit { .. })
    ));
}

fn canonical_stream(fixtures: &Value) -> String {
    ["stdout", "stderr", "marker", "heartbeat", "exit"]
        .iter()
        .map(|kind| json_line(launch_event_fixture(fixtures, kind)))
        .collect()
}

fn assert_open_warnings_admit<O>(registry: &SchemaRegistry, fixtures: &Value)
where
    O: ResponseOperation,
{
    let mut response = non_launch_fixture(fixtures, O::SUBCOMMAND, "success_response").clone();
    response["result"]["warnings"] = json!(["text", {"code": 7}, null]);
    registry
        .decode_response::<O>(&wire_bytes(&response))
        .unwrap_or_else(|error| {
            panic!(
                "schema-open warnings failed typed admission for {}: {error}",
                O::SUBCOMMAND
            )
        });
}

fn json_line(value: &Value) -> String {
    format!(
        "{}\n",
        serde_json::to_string(value).expect("fixture must serialize")
    )
}

fn wire_bytes(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).expect("fixture must serialize")
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/contract_v1")
}

fn load_invalid_json() -> Value {
    let contents = fs::read_to_string(fixture_dir().join("invalid.json"))
        .expect("read invalid request/response fixtures");
    serde_json::from_str(&contents).expect("invalid conformance fixture document must be JSON")
}
