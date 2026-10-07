//! tool-mediation/v1 extension: payload admission, policy decisions, host
//! requirement and version selection.

use agent_provider_contract::tool_mediation::{
    self, advertise, required_by_host, select, validate, BashPolicy, Decision, MediationError,
    ToolMediation, ENV, FAMILY, SUPPORTED_VERSIONS,
};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

fn fixtures() -> Value {
    serde_json::from_str(include_str!("fixtures/negotiation/tool_mediation_v1.json")).unwrap()
}

fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

#[test]
fn payloads_are_admitted_strictly() {
    let cases = fixtures();
    for (definition, values) in cases["valid"].as_object().unwrap() {
        let definition: &'static str = Box::leak(definition.clone().into_boxed_str());
        for value in values.as_array().unwrap() {
            validate(definition, value).unwrap_or_else(|e| panic!("{definition} {value}: {e}"));
        }
    }
    for (definition, values) in cases["invalid"].as_object().unwrap() {
        let definition: &'static str = Box::leak(definition.clone().into_boxed_str());
        for value in values.as_array().unwrap() {
            assert!(validate(definition, value).is_err(), "{definition} {value}");
        }
    }
    for value in cases["valid"]["ToolMediation"].as_array().unwrap() {
        let decoded = ToolMediation::decode(&value.to_string()).unwrap();
        assert_eq!(serde_json::to_value(&decoded).unwrap(), *value);
        assert_eq!(ToolMediation::decode(&decoded.encode()).unwrap(), decoded);
    }
    for value in cases["invalid"]["ToolMediation"].as_array().unwrap() {
        assert!(
            ToolMediation::decode(&value.to_string()).is_err(),
            "{value}"
        );
    }
    assert!(ToolMediation::decode("not json").is_err());
}

#[test]
fn allow_lists_match_whole_commands_and_trusted_task_runs_any() {
    let cases = fixtures();
    let allow = ToolMediation::decode(&cases["valid"]["ToolMediation"][0].to_string()).unwrap();
    assert_eq!(allow.decide("git status"), Decision::Run);
    for other in [
        "git status ",
        "git  status",
        "git status; rm -rf /",
        "git",
        "",
    ] {
        match allow.decide(other) {
            Decision::Refuse(why) => assert!(why.contains("Nothing was run"), "{why}"),
            Decision::Run => panic!("{other:?} ran"),
        }
    }
    assert!(!allow.trusted_task());
    let trusted = ToolMediation::decode(&cases["valid"]["ToolMediation"][1].to_string()).unwrap();
    assert!(trusted.trusted_task());
    assert_eq!(trusted.decide("anything at all"), Decision::Run);
    assert_eq!(
        trusted.bash,
        BashPolicy::Authority {
            authority: tool_mediation::TRUSTED_TASK.into()
        }
    );
}

#[test]
fn ingress_must_be_present_in_the_serving_process() {
    let policy =
        ToolMediation::decode(&fixtures()["valid"]["ToolMediation"][0].to_string()).unwrap();
    assert_eq!(
        policy.ingress(|name| (name == "OULIPOLY_ROOT_BASH_V1").then(|| "/ipc/bash.sock".into())),
        Ok("/ipc/bash.sock".into())
    );
    assert_eq!(
        policy.ingress(|_| None),
        Err(MediationError::NoIngress("OULIPOLY_ROOT_BASH_V1".into()))
    );
    assert!(policy.ingress(|_| Some(String::new())).is_err());
}

#[test]
fn a_host_selection_without_a_policy_is_refused_and_a_policy_is_never_ignored() {
    let text = fixtures()["valid"]["ToolMediation"][1].to_string();
    let selected = env(&[("OULIPOLY_HOST_TOOL_MEDIATION_V1", "1")]);
    let with = env(&[(ENV, &text)]);
    assert!(required_by_host(Some(&selected), Some(&with))
        .unwrap()
        .is_some());
    // Unselected but supplied: still honoured, never dropped.
    assert!(required_by_host(None, Some(&with)).unwrap().is_some());
    assert_eq!(
        required_by_host(Some(&selected), Some(&env(&[]))),
        Err(MediationError::Missing)
    );
    assert_eq!(required_by_host(None, None), Ok(None));
    let bad = env(&[(ENV, "{\"protocol\":\"oulipoly.tool_mediation/v2\"}")]);
    assert!(matches!(
        required_by_host(None, Some(&bad)),
        Err(MediationError::Invalid(_))
    ));
}

#[test]
fn providers_advertise_only_offered_versions_and_hosts_select_them() {
    let mut capabilities = Map::new();
    advertise(&mut capabilities, SUPPORTED_VERSIONS, None);
    assert!(capabilities.is_empty());
    advertise(
        &mut capabilities,
        SUPPORTED_VERSIONS,
        Some(&env(&[
            ("OULIPOLY_HOST_TOOL_MEDIATION_V1", "1"),
            ("OULIPOLY_HOST_TOOL_MEDIATION_V2", "1"),
        ])),
    );
    assert_eq!(
        Value::Object(capabilities.clone()),
        json!({"tool_mediation_v1": true})
    );
    assert_eq!(select(&[1], &capabilities), Ok(1));
    assert!(select(&[1], &Map::new()).is_err());
    assert_eq!(FAMILY.capability(1), "tool_mediation_v1");
}
