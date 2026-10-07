//! exploration/v1 extension: payload admission, route choice, the offer's
//! negotiation and mediation requirements, and version selection.

use agent_provider_contract::exploration::{
    self, admit, advertise, select, validate, Exploration, ExplorationError, ENV, FAMILY,
    SUPPORTED_VERSIONS,
};
use agent_provider_contract::tool_mediation::{self, ToolMediation};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

fn fixtures() -> Value {
    serde_json::from_str(include_str!("fixtures/negotiation/exploration_v1.json")).unwrap()
}

fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

fn mediation() -> ToolMediation {
    ToolMediation::decode(
        &json!({"protocol":"oulipoly.tool_mediation/v1","bash":{"allow":["true"]},
            "requester":"/b","ingress_env":"OULIPOLY_ROOT_BASH_V1"})
        .to_string(),
    )
    .unwrap()
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
    for value in cases["valid"]["Exploration"].as_array().unwrap() {
        let decoded = Exploration::decode(&value.to_string()).unwrap();
        assert_eq!(serde_json::to_value(&decoded).unwrap(), *value);
        assert_eq!(Exploration::decode(&decoded.encode()).unwrap(), decoded);
    }
    for value in cases["invalid"]["Exploration"].as_array().unwrap() {
        assert!(Exploration::decode(&value.to_string()).is_err(), "{value}");
    }
    assert!(Exploration::decode("not json").is_err());
}

#[test]
fn routes_are_chosen_only_from_the_offer() {
    let cases = fixtures();
    let one = Exploration::decode(&cases["valid"]["Exploration"][0].to_string()).unwrap();
    assert_eq!(one.route(None).unwrap(), "luna-max");
    assert_eq!(one.route(Some("luna-max")).unwrap(), "luna-max");
    let unoffered = one.route(Some("opus")).unwrap_err();
    assert!(unoffered.contains("not offered"), "{unoffered}");
    let two = Exploration::decode(&cases["valid"]["Exploration"][1].to_string()).unwrap();
    assert!(two.route(None).unwrap_err().contains("more than one"));
    assert_eq!(two.route(Some("site.route_2")).unwrap(), "site.route_2");
    assert_eq!(
        two.ingress(|name| (name == "OULIPOLY_ROOT_BASH_V1").then(|| "/s".into())),
        Ok("/s".into())
    );
    assert_eq!(
        two.ingress(|_| Some(String::new())),
        Err(ExplorationError::NoIngress("OULIPOLY_ROOT_BASH_V1".into()))
    );
    let marker = serde_json::to_value(two.effective("mcp__x__explore")).unwrap();
    validate("EffectiveExploration", &marker).unwrap();
    assert_eq!(marker["routes"], json!(["luna-max", "site.route_2"]));
}

#[test]
fn absence_is_no_offer_and_an_unnegotiated_or_unmediated_offer_is_refused() {
    let offer = fixtures()["valid"]["Exploration"][0].to_string();
    let selected = env(&[("OULIPOLY_HOST_EXPLORATION_V1", "1")]);
    let mediation = mediation();
    // No object: Bash alone, whether or not the host selected the family.
    assert_eq!(
        admit(Some(&selected), Some(&env(&[])), Some(&mediation)),
        Ok(None)
    );
    assert_eq!(admit(None, None, None), Ok(None));
    let launch = env(&[(ENV, &offer)]);
    assert!(admit(Some(&selected), Some(&launch), Some(&mediation))
        .unwrap()
        .is_some());
    assert_eq!(
        admit(None, Some(&launch), Some(&mediation)),
        Err(ExplorationError::NotSelected)
    );
    assert_eq!(
        admit(
            Some(&env(&[("OULIPOLY_HOST_EXPLORATION_V1", "true")])),
            Some(&launch),
            Some(&mediation)
        ),
        Err(ExplorationError::NotSelected)
    );
    assert_eq!(
        admit(Some(&selected), Some(&launch), None),
        Err(ExplorationError::WithoutMediation)
    );
    let bad = env(&[(ENV, "{\"protocol\":\"oulipoly.exploration/v2\"}")]);
    assert!(matches!(
        admit(Some(&selected), Some(&bad), Some(&mediation)),
        Err(ExplorationError::Invalid(_))
    ));
}

#[test]
fn versions_are_negotiated_independently_of_tool_mediation() {
    assert_eq!(SUPPORTED_VERSIONS, &[1]);
    assert_eq!(FAMILY.selector(1), "OULIPOLY_HOST_EXPLORATION_V1");
    assert_eq!(FAMILY.capability(1), "exploration_v1");
    assert_eq!(exploration::PROTOCOL, "oulipoly.exploration/v1");
    assert_eq!(exploration::MARKER, exploration::PROTOCOL);
    // Offered by the host: advertised; not offered: nothing.
    let mut caps = Map::new();
    advertise(
        &mut caps,
        SUPPORTED_VERSIONS,
        Some(&FAMILY.host_selectors(&[1, 2])),
    );
    assert_eq!(Value::Object(caps.clone()), json!({"exploration_v1": true}));
    let mut none = Map::new();
    advertise(
        &mut none,
        SUPPORTED_VERSIONS,
        Some(&tool_mediation::FAMILY.host_selectors(&[1])),
    );
    assert!(none.is_empty());
    // A newer advertisement the host does not know is ignored; tool
    // mediation alone selects no exploration.
    caps.insert("exploration_v2".into(), json!(true));
    assert_eq!(select(&[1], &caps), Ok(1));
    let mediated_only = json!({"tool_mediation_v1": true});
    assert!(select(&[1], mediated_only.as_object().unwrap()).is_err());
    let not_bool = json!({"exploration_v1": "true"});
    assert!(select(&[1], not_bool.as_object().unwrap()).is_err());
}
