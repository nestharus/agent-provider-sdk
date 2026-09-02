pub mod support {
    pub mod contract_matrix;
}

use agent_provider_contract::generated as dto;
use agent_provider_contract::operations as op;
use agent_provider_contract::{ResponseOperation, SchemaRegistry};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use std::any::TypeId;
use support::contract_matrix::{
    fixtures, launch_event_fixture, launch_fixture, non_launch_fixture, LAUNCH_EVENT_ROWS,
    NON_LAUNCH_ROWS,
};

macro_rules! operation_round_trips {
    ($fixtures:expr; $($operation:ty => $result:ty, $subcommand:literal;)+) => {
        $(assert_non_launch_round_trip::<$operation, $result>($fixtures, $subcommand);)+
    };
}

#[test]
fn dto_roundtrip_covers_every_s2_contract_type() {
    let fixtures = fixtures();

    assert_eq!(NON_LAUNCH_ROWS.len(), 30);
    operation_round_trips!(&fixtures;
        op::Describe => dto::DescribeResult, "describe";
        op::Schema => dto::SchemaResult, "schema";
        op::SettingsList => dto::SettingsListResult, "settings.list";
        op::SettingsGet => dto::SettingsGetResult, "settings.get";
        op::SettingsCreate => dto::SettingsCreateResult, "settings.create";
        op::SettingsUpdate => dto::SettingsUpdateResult, "settings.update";
        op::SettingsDelete => dto::SettingsDeleteResult, "settings.delete";
        op::SettingsValidate => dto::SettingsValidateResult, "settings.validate";
        op::SettingsMigrate => dto::SettingsMigrateResult, "settings.migrate";
        op::PolicyEvaluate => dto::PolicyEvaluateResult, "policy.evaluate";
        op::TerminalClassify => dto::TerminalClassifyResult, "terminal.classify";
        op::QuotaSource => dto::QuotaSourceResult, "quota.source";
        op::QuotaProbe => dto::QuotaProbeResult, "quota.probe";
        op::QuotaRefreshAuth => dto::QuotaRefreshAuthResult, "quota.refresh_auth";
        op::SessionLocateTranscript => dto::SessionLocateTranscriptResult, "session.locate_transcript";
        op::SessionEnumerate => dto::SessionEnumerateResult, "session.enumerate";
        op::SessionReadTurns => dto::SessionReadTurnsResult, "session.read_turns";
        op::SessionCapture => dto::SessionCaptureResult, "session.capture";
        op::SessionExport => dto::SessionExportResult, "session.export";
        op::SessionReplace => dto::SessionReplaceResult, "session.replace";
        op::RotationAssess => dto::RotationAssessResult, "rotation.assess";
        op::RotationMaterialize => dto::RotationMaterializeResult, "rotation.materialize";
        op::DiscoveryModels => dto::DiscoveryModelsResult, "discovery.models";
        op::DiscoveryAccounts => dto::DiscoveryAccountsResult, "discovery.accounts";
        op::SetupDetect => dto::SetupDetectResult, "setup.detect";
        op::SetupInstallPlan => dto::SetupInstallPlanResult, "setup.install_plan";
        op::SetupSyncPlan => dto::SetupSyncPlanResult, "setup.sync_plan";
        op::SetupBrainTurn => dto::SetupBrainTurnResult, "setup_brain.turn";
        op::MigrationPlan => dto::MigrationPlanResult, "migration.plan";
        op::MigrationApply => dto::MigrationApplyResult, "migration.apply";
    );

    assert_eq!(LAUNCH_EVENT_ROWS.len(), 5);
    let launch_request = launch_fixture(&fixtures, "request");
    assert_json_round_trip::<dto::LaunchRequest>(launch_request);
    let registry = SchemaRegistry::new();
    let admitted = registry
        .decode_request::<op::Launch>(&wire_bytes(launch_request))
        .expect("launch request must admit through its operation");
    let encoded = registry
        .encode_request::<op::Launch>(admitted.value())
        .expect("launch request must encode through its operation");
    assert_eq!(wire_value(&encoded), *launch_request);
    assert_json_round_trip::<dto::LaunchStdoutEvent>(launch_event_fixture(&fixtures, "stdout"));
    assert_json_round_trip::<dto::LaunchStderrEvent>(launch_event_fixture(&fixtures, "stderr"));
    assert_json_round_trip::<dto::LaunchMarkerEvent>(launch_event_fixture(&fixtures, "marker"));
    assert_json_round_trip::<dto::LaunchHeartbeatEvent>(launch_event_fixture(
        &fixtures,
        "heartbeat",
    ));
    assert_json_round_trip::<dto::LaunchExitEvent>(launch_event_fixture(&fixtures, "exit"));
    for kind in ["stdout", "stderr", "marker", "heartbeat", "exit"] {
        assert_json_round_trip::<dto::LaunchEvent>(launch_event_fixture(&fixtures, kind));
    }
}

#[test]
fn public_dtos_preserve_named_contract_entities_and_operation_roles() {
    let fixtures = fixtures();
    let request: dto::LaunchRequest =
        serde_json::from_value(launch_fixture(&fixtures, "request").clone())
            .expect("launch request must have a typed public representation");

    assert_eq!(
        request.params.model.inputs.prompt.as_deref(),
        Some("Summarize this repo")
    );
    let policy: dto::PolicyEvaluateRequest =
        serde_json::from_value(non_launch_fixture(&fixtures, "policy.evaluate", "request").clone())
            .expect("policy request must have a typed model input");
    assert!(policy.params.model.inputs.named.contains_key("temperature"));
    assert!(matches!(
        request
            .params
            .stdin
            .as_ref()
            .map(|payload| &payload.encoding),
        Some(dto::BytePayloadEncoding::Utf8)
    ));
    assert!(matches!(
        request
            .params
            .session
            .as_ref()
            .and_then(|session| session.start_mode.as_ref()),
        Some(dto::LaunchSessionStartMode::Resume)
    ));

    assert_distinct::<dto::QuotaSourceParams, dto::QuotaProbeParams>();
    assert_distinct::<dto::SessionLocateTranscriptParams, dto::SessionReadTurnsParams>();
    assert_distinct::<dto::SessionReadTurnsParams, dto::SessionExportParams>();
    assert_distinct::<dto::RotationAssessParams, dto::RotationMaterializeParams>();
    assert_distinct::<dto::DiscoveryModelsParams, dto::DiscoveryAccountsParams>();
    assert_distinct::<dto::SetupDetectParams, dto::SetupInstallPlanParams>();
    assert_distinct::<dto::SetupInstallPlanParams, dto::SetupSyncPlanParams>();
    assert_distinct::<dto::SetupSyncPlanParams, dto::SetupBrainTurnParams>();
    assert_distinct::<dto::MigrationPlanParams, dto::MigrationApplyParams>();
}

#[test]
fn dto_discriminants_reject_schema_invalid_values() {
    let fixtures = fixtures();

    let mut stdout = launch_event_fixture(&fixtures, "stdout").clone();
    stdout["kind"] = json!("stderr");
    assert_deserialize_error::<dto::LaunchStdoutEvent>(&stdout);

    let mut terminal = launch_event_fixture(&fixtures, "exit").clone();
    terminal["terminal_signal"]["kind"] = json!("not_a_terminal_signal");
    assert_deserialize_error::<dto::LaunchExitEvent>(&terminal);

    let mut status = launch_event_fixture(&fixtures, "exit").clone();
    status["status"] = json!({"kind": "exited"});
    assert_deserialize_error::<dto::LaunchExitEvent>(&status);

    let invalid_severity = json!({"severity": "fatal", "message": "invalid"});
    assert_deserialize_error::<dto::Diagnostic>(&invalid_severity);

    let invalid_encoding = json!({"encoding": "hex", "data": "00"});
    assert_deserialize_error::<dto::BytePayload>(&invalid_encoding);

    let mut success = non_launch_fixture(&fixtures, "describe", "success_response").clone();
    success["ok"] = json!(false);
    assert_deserialize_error::<dto::DescribeResponse>(&success);

    let mut error = non_launch_fixture(&fixtures, "describe", "error_response").clone();
    error["ok"] = json!(true);
    assert_deserialize_error::<dto::DescribeErrorResponse>(&error);

    let mut describe = non_launch_fixture(&fixtures, "describe", "success_response").clone();
    describe["result"]
        .as_object_mut()
        .expect("describe result must be object")
        .remove("provider_id");
    assert_deserialize_error::<dto::DescribeResponse>(&describe);

    let mut schema = non_launch_fixture(&fixtures, "schema", "request").clone();
    schema["params"]
        .as_object_mut()
        .expect("schema params must be object")
        .remove("schema_id");
    assert_deserialize_error::<dto::SchemaRequest>(&schema);
}

#[test]
fn session_replace_provider_owned_protocol_dtos_round_trip_new_evidence_shape() {
    let fixtures = fixtures();
    let request = non_launch_fixture(&fixtures, "session.replace", "request");
    let success = non_launch_fixture(&fixtures, "session.replace", "success_response");
    let result = success.get("result").expect("replace result");

    assert_json_round_trip::<dto::SessionReplaceRequest>(request);
    assert_json_round_trip::<dto::SessionReplaceParams>(&request["params"]);
    assert_json_round_trip::<dto::SessionReplaceCanonicalTranscript>(
        &request["params"]["canonical_transcript"],
    );
    assert_json_round_trip::<dto::SessionReplaceResult>(result);
    assert_json_round_trip::<dto::SessionReplaceCanonicalPostimage>(&result["canonical_postimage"]);
    assert_json_round_trip::<dto::SessionReplaceArtifactEvidence>(
        &result["provider_preimage_artifact"],
    );
    assert_json_round_trip::<dto::SessionReplaceArtifactEvidence>(
        &result["provider_postimage_artifact"],
    );
    assert_json_round_trip::<dto::SessionReplaceResponse>(success);

    let typed: dto::SessionReplaceResponse =
        serde_json::from_value(success.clone()).expect("typed replace response");
    let proposal = typed
        .result
        .host_state_plan
        .expect("changed response must carry host-state proposal");
    assert!(matches!(
        proposal.plan(),
        dto::SessionReplaceHostStatePlan::V2(_)
    ));
}

#[test]
fn session_replace_legacy_result_fixture_still_round_trips_after_optional_evidence_fields() {
    let fixtures = fixtures();
    let legacy = non_launch_fixture(&fixtures, "session.replace", "legacy_success_response");

    assert_json_round_trip::<dto::SessionReplaceResult>(
        legacy.get("result").expect("legacy replace result"),
    );
    assert_json_round_trip::<dto::SessionReplaceResponse>(legacy);

    let typed: dto::SessionReplaceResponse =
        serde_json::from_value(legacy.clone()).expect("typed legacy replace response");
    let proposal = typed
        .result
        .host_state_plan
        .expect("changed response must carry host-state proposal");
    assert!(matches!(
        proposal.plan(),
        dto::SessionReplaceHostStatePlan::V1(_)
    ));
}

fn assert_non_launch_round_trip<O, Result>(fixtures: &Value, subcommand: &str)
where
    O: ResponseOperation,
    Result: DeserializeOwned + Serialize,
{
    assert_eq!(O::SUBCOMMAND, subcommand);
    let registry = SchemaRegistry::new();
    let request = non_launch_fixture(fixtures, subcommand, "request");
    assert_json_round_trip::<O::Request>(request);
    let admitted_request = registry
        .decode_request::<O>(&wire_bytes(request))
        .unwrap_or_else(|error| panic!("request admission failed for {subcommand}: {error}"));
    let encoded_request = registry
        .encode_request::<O>(admitted_request.value())
        .unwrap_or_else(|error| panic!("request encoding failed for {subcommand}: {error}"));
    assert_eq!(wire_value(&encoded_request), *request);

    let success = non_launch_fixture(fixtures, subcommand, "success_response");
    assert_json_round_trip::<Result>(
        success
            .get("result")
            .unwrap_or_else(|| panic!("missing result fixture for {subcommand}")),
    );
    assert_json_round_trip::<O::Response>(success);
    let admitted_response = registry
        .decode_response::<O>(&wire_bytes(success))
        .unwrap_or_else(|error| panic!("response admission failed for {subcommand}: {error}"));
    let encoded_response = registry
        .encode_response::<O>(admitted_response.value())
        .unwrap_or_else(|error| panic!("response encoding failed for {subcommand}: {error}"));
    assert_eq!(wire_value(&encoded_response), *success);

    let error = non_launch_fixture(fixtures, subcommand, "error_response");
    assert_json_round_trip::<O::ErrorResponse>(error);
    let admitted_error = registry
        .decode_error_response::<O>(&wire_bytes(error))
        .unwrap_or_else(|failure| panic!("error admission failed for {subcommand}: {failure}"));
    let encoded_error = registry
        .encode_error_response::<O>(admitted_error.value())
        .unwrap_or_else(|failure| panic!("error encoding failed for {subcommand}: {failure}"));
    assert_eq!(wire_value(&encoded_error), *error);
}

fn assert_distinct<A: 'static, B: 'static>() {
    assert_ne!(TypeId::of::<A>(), TypeId::of::<B>());
}

fn wire_bytes(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).expect("fixture must serialize")
}

fn wire_value(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).expect("wire output must be JSON")
}

fn assert_json_round_trip<T>(value: &Value)
where
    T: DeserializeOwned + Serialize,
{
    let encoded = serde_json::to_string(value).expect("fixture must serialize");
    let typed: T = serde_json::from_str(&encoded).expect("fixture must deserialize through DTO");
    let reencoded = serde_json::to_value(typed).expect("DTO must serialize");
    assert_eq!(reencoded, *value);
}

fn assert_deserialize_error<T>(value: &Value)
where
    T: DeserializeOwned,
{
    let encoded = serde_json::to_string(value).expect("fixture must serialize");
    assert!(
        serde_json::from_str::<T>(&encoded).is_err(),
        "DTO accepted schema-invalid JSON: {value}"
    );
}
