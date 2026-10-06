//! Golden version-selection and resident-session/v1 extension fixtures.

use agent_provider_contract::negotiation::select_contract_version;
use agent_provider_contract::resident_session::{
    self, advertise, decode_prepare_params, decode_prepare_result, select, validate,
    ResidentPrepareResult, FAMILY, SUPPORTED_VERSIONS,
};
use agent_provider_contract::SchemaRegistry;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

fn fixtures(name: &str) -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/negotiation")
        .join(name);
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

#[test]
fn contract_version_selection_matches_golden_cases() {
    let cases = fixtures("version_selection.json");
    for case in cases["contract"].as_array().unwrap() {
        let host: Vec<&str> = case["host"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        let advertised: Vec<String> = serde_json::from_value(case["advertised"].clone()).unwrap();
        let selected =
            select_contract_version(&host, &advertised, case["preferred"].as_str().unwrap()).ok();
        assert_eq!(
            selected.map(Value::String).unwrap_or(Value::Null),
            case["expect"],
            "{}",
            case["name"]
        );
    }
}

#[test]
fn providers_advertise_only_offered_supported_resident_versions() {
    let cases = fixtures("version_selection.json");
    for case in cases["resident_session_provider"].as_array().unwrap() {
        let supported: Vec<u32> =
            serde_json::from_value(case["provider_supported"].clone()).unwrap();
        let env: Option<BTreeMap<String, String>> =
            serde_json::from_value(case["host_env"].clone()).unwrap();
        let mut capabilities = Map::new();
        advertise(&mut capabilities, &supported, env.as_ref());
        let names: Vec<&String> = capabilities.keys().collect();
        let expected: Vec<String> = serde_json::from_value(case["advertised"].clone()).unwrap();
        assert_eq!(
            names,
            expected.iter().collect::<Vec<_>>(),
            "{}",
            case["name"]
        );
        assert!(capabilities.values().all(|v| v == &json!(true)));
    }
}

#[test]
fn hosts_select_the_highest_common_resident_version() {
    let cases = fixtures("version_selection.json");
    for case in cases["resident_session_host"].as_array().unwrap() {
        let supported: Vec<u32> = serde_json::from_value(case["host_supported"].clone()).unwrap();
        let capabilities = case["capabilities"].as_object().unwrap();
        let selected = select(&supported, capabilities).ok();
        assert_eq!(
            selected.map(Value::from).unwrap_or(Value::Null),
            case["expect"],
            "{}",
            case["name"]
        );
    }
}

/// The closed v1 describe schema admits the selected v1 capability and refuses
/// an unselected newer one, which is why providers advertise offered versions only.
#[test]
fn describe_schema_admits_selected_v1_and_refuses_unoffered_versions() {
    let registry = SchemaRegistry::new();
    let mut response = json!({
        "contract": "oulipoly.provider/v1", "request_id": "describe", "ok": true,
        "result": {
            "provider_id": "fixture", "display_name": "Fixture",
            "contract_versions": ["oulipoly.provider/v1"],
            "preferred_contract": "oulipoly.provider/v1",
            "capabilities": {"launch": true, "policy": true, "quota": false, "session": true,
                "terminal": true, "rotation": false, "discovery": false, "settings": false,
                "setup_brain": false, "setup": false, "migration": false,
                "launch_output_v1": true, "session_turn_pages_v1": true,
                "resident_session_v1": true}
        }
    });
    registry.validate_response("describe", &response).unwrap();
    response["result"]["capabilities"]["resident_session_v2"] = json!(true);
    assert!(registry.validate_response("describe", &response).is_err());
    response["result"]["capabilities"]
        .as_object_mut()
        .unwrap()
        .remove("resident_session_v2");
    response["result"]["contract_versions"] =
        json!(["oulipoly.provider/v1", "oulipoly.provider/v2"]);
    assert!(
        registry.validate_response("describe", &response).is_err(),
        "a v1 host refuses a second advertised contract version"
    );
}

#[test]
fn extension_payloads_match_golden_valid_and_invalid_fixtures() {
    let cases = fixtures("resident_session_v1.json");
    for (definition, expect_valid) in [("valid", true), ("invalid", false)] {
        for (name, values) in cases[definition].as_object().unwrap() {
            let name: &'static str = match name.as_str() {
                "ResidentPrepareParams" => "ResidentPrepareParams",
                "ResidentPrepareResult" => "ResidentPrepareResult",
                "ResidentSessionMeta" => "ResidentSessionMeta",
                "NativeTurnMeta" => "NativeTurnMeta",
                "TurnStopReason" => "TurnStopReason",
                other => panic!("unknown definition {other}"),
            };
            for value in values.as_array().unwrap() {
                assert_eq!(
                    validate(name, value).is_ok(),
                    expect_valid,
                    "{name} {value}"
                );
            }
        }
    }
    let params = &cases["valid"]["ResidentPrepareParams"][1];
    assert_eq!(
        decode_prepare_params(params).unwrap().launch.argv,
        vec!["claude", "-p"]
    );
    let result = &cases["valid"]["ResidentPrepareResult"][0];
    let decoded = decode_prepare_result(result).unwrap();
    assert_eq!(
        decoded,
        ResidentPrepareResult::v1(decoded.invocation.args.clone(), "0".repeat(64))
    );
    assert_eq!(serde_json::to_value(&decoded).unwrap(), *result);
}

#[test]
fn sdk_constants_match_the_endpoint_vocabulary() {
    assert_eq!(SUPPORTED_VERSIONS, &[1]);
    assert_eq!(FAMILY.selector(1), "OULIPOLY_HOST_RESIDENT_SESSION_V1");
    assert_eq!(FAMILY.capability(1), "resident_session_v1");
    assert_eq!(resident_session::PROTOCOL, "oulipoly.resident_session/v1");
    assert_eq!(resident_session::ACP_SCHEMA_TAG, "schema-v2.0.0-alpha.7");
}
