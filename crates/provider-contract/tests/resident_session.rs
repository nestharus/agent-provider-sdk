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

#[test]
fn describe_advertisements_decode_and_select_while_v1_payload_stays_strict() {
    use agent_provider_contract::operations::Describe;
    let registry = SchemaRegistry::new();
    let mut response = json!({
        "contract":"oulipoly.provider/v1", "request_id":"describe", "ok":true,
        "result": {"provider_id":"fixture", "display_name":"Fixture",
            "contract_versions":["oulipoly.provider/v2", "oulipoly.provider/v1"],
            "preferred_contract":"oulipoly.provider/v2",
            "capabilities":{"launch":true,"policy":true,"quota":false,"session":true,
                "terminal":true,"rotation":false,"discovery":false,"settings":false,
                "setup_brain":false,"setup":false,"migration":false,
                "resident_session_v1":true,"resident_session_v2":true,
                "future_capability":{"new_shape":42}}}
    });
    let decoded = registry
        .decode_response::<Describe>(&serde_json::to_vec(&response).unwrap())
        .unwrap();
    let result = &decoded.value().result;
    assert_eq!(
        select_contract_version(
            &["oulipoly.provider/v1"],
            &result.contract_versions,
            &result.preferred_contract
        ),
        Ok("oulipoly.provider/v1".into())
    );
    let capabilities = serde_json::to_value(&result.capabilities).unwrap();
    assert_eq!(select(&[1], capabilities.as_object().unwrap()), Ok(1));
    assert_eq!(
        capabilities["future_capability"],
        response["result"]["capabilities"]["future_capability"]
    );
    response["result"]["preferred_contract"] = json!("oulipoly.provider/v3");
    assert!(registry.validate_response("describe", &response).is_err());
    assert!(select_contract_version(
        &["oulipoly.provider/v1"],
        &result.contract_versions,
        "oulipoly.provider/v3"
    )
    .is_err());
    response["result"]["preferred_contract"] = json!("oulipoly.provider/v2");
    response["result"]["capabilities"]["resident_session_v1"] = json!("true");
    assert!(registry.validate_response("describe", &response).is_err());
    response["result"]["capabilities"]["resident_session_v1"] = json!(true);
    response["result"]["unknown_selected_payload_field"] = json!(true);
    assert!(registry.validate_response("describe", &response).is_err());
    response["result"]
        .as_object_mut()
        .unwrap()
        .remove("unknown_selected_payload_field");
    response["contract"] = json!("oulipoly.provider/v2");
    assert!(registry.validate_response("describe", &response).is_err());
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
