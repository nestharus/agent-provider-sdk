//! Synthetic common specimens: selection and wire/typed admission are distinct
//! from specimen coherence, and neither establishes native emission or custody.
use agent_provider_contract::generated::{
    LaunchEvent, LaunchOutputCompleteMarkerValueV1, PromptAcceptedMarkerValueV1,
};
use agent_provider_contract::host_extensions::{
    launch_output, prompt_acceptance, session_turn_pages, OPT_IN_VALUE,
};
use agent_provider_contract::negotiation::VersionFamily;
use agent_provider_contract::operations::{Describe, Launch, SessionReadTurns};
use agent_provider_contract::{
    validate_launch_ndjson, LaunchStreamError, RequestOperation, ResponseOperation, SchemaRegistry,
};
use base64::Engine;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const GOLDEN: &str = include_str!("fixtures/host_extensions/v1.json");

fn golden() -> Value {
    serde_json::from_str(GOLDEN).unwrap()
}
fn bytes(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).unwrap()
}
fn sha(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn ndjson(events: &Value) -> Vec<u8> {
    events
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|event| {
            let mut encoded = bytes(event);
            encoded.push(b'\n');
            encoded
        })
        .collect()
}
fn family(name: &str) -> (VersionFamily, &'static str, &'static str, &'static [u32]) {
    match name {
        "prompt_acceptance" => (
            prompt_acceptance::FAMILY,
            prompt_acceptance::SELECTOR,
            prompt_acceptance::CAPABILITY,
            prompt_acceptance::SUPPORTED_VERSIONS,
        ),
        "launch_output" => (
            launch_output::FAMILY,
            launch_output::SELECTOR,
            launch_output::CAPABILITY,
            launch_output::SUPPORTED_VERSIONS,
        ),
        "session_turn_pages" => (
            session_turn_pages::FAMILY,
            session_turn_pages::SELECTOR,
            session_turn_pages::CAPABILITY,
            session_turn_pages::SUPPORTED_VERSIONS,
        ),
        _ => panic!("unknown fixture extension {name}"),
    }
}

// Fixture edit vectors are test data, not a new SDK wire/admission operation.
fn edited(mut target: Value, edits: &Value) -> Value {
    for edit in edits.as_array().unwrap() {
        let path = edit["path"].as_str().unwrap();
        let (parent, key) = path.rsplit_once('/').unwrap();
        let parent = target.pointer_mut(parent).unwrap();
        if let Some(array) = parent.as_array_mut() {
            let index: usize = key.parse().unwrap();
            if edit["remove"] == true {
                array.remove(index);
            } else {
                array[index] = edit["value"].clone();
            }
        } else {
            let object = parent.as_object_mut().unwrap();
            if edit["remove"] == true {
                assert!(object.remove(key).is_some());
            } else {
                object.insert(key.to_owned(), edit["value"].clone());
            }
        }
    }
    target
}

fn request_roundtrip<O: RequestOperation>(registry: &SchemaRegistry, value: &Value) {
    let admitted = registry.decode_request::<O>(&bytes(value)).unwrap();
    let encoded = registry.encode_request::<O>(admitted.value()).unwrap();
    let decoded = registry.decode_request::<O>(&encoded).unwrap();
    // HostContext's existing representation omits an empty env map. That is
    // not loss of an offer and does not change wire-admission semantics.
    let mut expected = value.clone();
    if expected["host"].get("env") == Some(&json!({})) {
        expected["host"].as_object_mut().unwrap().remove("env");
    }
    assert_eq!(serde_json::to_value(decoded.value()).unwrap(), expected);
}
fn response_roundtrip<O: ResponseOperation>(registry: &SchemaRegistry, value: &Value) {
    let admitted = registry.decode_response::<O>(&bytes(value)).unwrap();
    let encoded = registry.encode_response::<O>(admitted.value()).unwrap();
    let decoded = registry.decode_response::<O>(&encoded).unwrap();
    assert_eq!(serde_json::to_value(decoded.value()).unwrap(), *value);
}

#[test]
fn public_vocabulary_matches_paired_admitted_shapes() {
    let registry = SchemaRegistry::new();
    let valid = &golden()["valid"];
    request_roundtrip::<Describe>(&registry, &valid["describe_request"]);
    response_roundtrip::<Describe>(&registry, &valid["describe_response"]);
    request_roundtrip::<Launch>(&registry, &valid["launch_request"]);
    request_roundtrip::<SessionReadTurns>(&registry, &valid["pages_request"]);
    response_roundtrip::<SessionReadTurns>(&registry, &valid["pages_response"]);
    assert_eq!(
        valid["launch_request"]["params"]["prompt_acceptance"]["protocol"],
        prompt_acceptance::PROTOCOL
    );
    assert_eq!(
        valid["launch_request"]["params"]["output_delivery"]["protocol"],
        launch_output::PROTOCOL
    );
    assert_eq!(
        valid["pages_request"]["params"]["read_protocol"],
        session_turn_pages::PROTOCOL
    );
    assert_eq!(
        valid["pages_response"]["result"]["read_protocol"],
        session_turn_pages::PROTOCOL
    );
    assert_eq!(
        valid["launch_events"][0]["name"],
        prompt_acceptance::MARKER_NAME
    );
    assert_eq!(
        valid["launch_events"][4]["name"],
        launch_output::MARKER_NAME
    );
    let request = registry
        .decode_request::<Describe>(&bytes(&valid["describe_request"]))
        .unwrap();
    let response = registry
        .decode_response::<Describe>(&bytes(&valid["describe_response"]))
        .unwrap();
    let caps = serde_json::to_value(&response.value().result.capabilities).unwrap();
    for name in ["prompt_acceptance", "launch_output", "session_turn_pages"] {
        let (family, selector, capability, defined) = family(name);
        assert_eq!(defined, &[1]);
        assert_eq!(family.selector(1), selector);
        assert_eq!(family.capability(1), capability);
        assert_eq!(request.value().host.env[selector], OPT_IN_VALUE);
        let offered = family.selected_by_host(Some(&request.value().host.env));
        assert_eq!(family.select(&offered, caps.as_object().unwrap()), Ok(1));
    }
    assert_eq!(
        response.value().result.capabilities.additional["future_extension"],
        json!({"opaque":true})
    );
}

#[test]
fn paired_selection_controls_do_not_pretend_to_be_schema_rejections() {
    let registry = SchemaRegistry::new();
    let fixture = golden();
    for case in fixture["selection"].as_array().unwrap() {
        let (family, _, capability, _) = family(case["extension"].as_str().unwrap());
        let env: Option<BTreeMap<String, String>> =
            serde_json::from_value(case["host_env"].clone()).unwrap();
        let supported: Vec<u32> =
            serde_json::from_value(case["provider_supported"].clone()).unwrap();
        let mut advertised = Map::new();
        family.advertise_into(&mut advertised, &supported, env.as_ref());
        let expected = if case["expect_advertised"] == true {
            json!({capability: true})
        } else {
            json!({})
        };
        assert_eq!(Value::Object(advertised), expected, "{case}");
        let offered = family.selected_by_host(env.as_ref());
        let selected = family
            .select(&offered, case["capabilities"].as_object().unwrap())
            .ok();
        assert_eq!(
            selected.is_some(),
            case["expect_selected"].as_bool().unwrap(),
            "{case}"
        );
        if selected.is_some() {
            assert_eq!(selected, Some(1));
        }

        // Even an unoffered true advertisement has a valid isolated shape.
        // Pair selection, not schema validation, rules out that agreement.
        let mut request = fixture["valid"]["describe_request"].clone();
        request["host"]["env"] = case["host_env"].clone();
        if env.is_none() {
            request["host"].as_object_mut().unwrap().remove("env");
        }
        request_roundtrip::<Describe>(&registry, &request);
        let mut response = fixture["valid"]["describe_response"].clone();
        for key in [
            prompt_acceptance::CAPABILITY,
            launch_output::CAPABILITY,
            session_turn_pages::CAPABILITY,
        ] {
            response["result"]["capabilities"]
                .as_object_mut()
                .unwrap()
                .remove(key);
        }
        if let Some(value) = case["capabilities"].get(capability) {
            response["result"]["capabilities"][capability] = value.clone();
        }
        response_roundtrip::<Describe>(&registry, &response);
    }
}

#[test]
fn invalid_shapes_fail_existing_schema_and_typed_admission() {
    let registry = SchemaRegistry::new();
    let fixture = golden();
    for case in fixture["invalid_admission"].as_array().unwrap() {
        let target = case["target"].as_str().unwrap();
        let value = edited(
            fixture["valid"].pointer(target).unwrap().clone(),
            &case["edits"],
        );
        let rejected = match target {
            "/describe_response" => {
                registry.validate_response("describe", &value).is_err()
                    && registry
                        .decode_response::<Describe>(&bytes(&value))
                        .is_err()
            }
            "/launch_request" => {
                registry.validate_request("launch", &value).is_err()
                    && registry.decode_request::<Launch>(&bytes(&value)).is_err()
            }
            "/pages_request" => {
                registry
                    .validate_request("session.read_turns", &value)
                    .is_err()
                    && registry
                        .decode_request::<SessionReadTurns>(&bytes(&value))
                        .is_err()
            }
            "/pages_response" => {
                registry
                    .validate_response("session.read_turns", &value)
                    .is_err()
                    && registry
                        .decode_response::<SessionReadTurns>(&bytes(&value))
                        .is_err()
            }
            "/launch_events/0" | "/launch_events/4" => {
                let mut events = fixture["valid"]["launch_events"].clone();
                let index = if target.ends_with('0') { 0 } else { 4 };
                events[index] = value.clone();
                registry.validate_launch_event("marker", &value).is_err()
                    && matches!(
                        validate_launch_ndjson(&ndjson(&events), "launch-host-extensions"),
                        Err(LaunchStreamError::Schema { .. })
                    )
            }
            _ => panic!("unknown admission target {target}"),
        };
        assert!(rejected, "{case}");
    }
    // Raw representation admits these strings; validated encoding must not.
    let mut request = registry
        .decode_request::<Launch>(&bytes(&fixture["valid"]["launch_request"]))
        .unwrap()
        .into_inner();
    request.params.prompt_acceptance.as_mut().unwrap().protocol = "wrong".into();
    assert!(registry.encode_request::<Launch>(&request).is_err());
    let mut response = registry
        .decode_response::<SessionReadTurns>(&bytes(&fixture["valid"]["pages_response"]))
        .unwrap()
        .into_inner();
    response.result.read_protocol = "wrong".into();
    assert!(registry
        .encode_response::<SessionReadTurns>(&response)
        .is_err());
}

#[test]
fn coherent_specimen_digests_counts_and_correlations_are_not_validator_guarantees() {
    let fixture = golden();
    let valid = &fixture["valid"];
    let request = &valid["launch_request"];
    let admitted = validate_launch_ndjson(
        &ndjson(&valid["launch_events"]),
        request["request_id"].as_str().unwrap(),
    )
    .unwrap();
    assert_eq!(admitted.event_count(), 6);
    assert_eq!(admitted.exit_seq(), 6);
    for (typed, original) in admitted
        .events()
        .iter()
        .zip(valid["launch_events"].as_array().unwrap())
    {
        assert_eq!(serde_json::to_value(typed).unwrap(), *original);
    }
    let accepted: PromptAcceptedMarkerValueV1 =
        serde_json::from_value(valid["launch_events"][0]["value"].clone()).unwrap();
    let output: LaunchOutputCompleteMarkerValueV1 =
        serde_json::from_value(valid["launch_events"][4]["value"].clone()).unwrap();
    let prompt = request["params"]["model"]["inputs"]["prompt"]
        .as_str()
        .unwrap();
    assert_eq!(request["params"]["stdin"]["data"], prompt);
    assert_eq!(accepted.protocol, prompt_acceptance::PROTOCOL);
    assert_eq!(accepted.prompt_sha256, sha(prompt.as_bytes()));
    assert_eq!(
        request["params"]["prompt_acceptance"]["prompt_sha256"],
        accepted.prompt_sha256
    );
    assert_eq!(
        request["params"]["prompt_acceptance"]["delivery_nonce"],
        serde_json::to_value(&accepted.delivery_nonce).unwrap()
    );
    assert_eq!(
        request["params"]["session"]["known_provider_session_id"],
        accepted.provider_session_id
    );
    assert_eq!(
        valid["launch_events"][5]["session"]["provider_session_id"],
        accepted.provider_session_id
    );
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut count = 0;
    for event in admitted.events() {
        let (payload, channel) = match event {
            LaunchEvent::Stdout(event) => (&event.data_base64, &mut stdout),
            LaunchEvent::Stderr(event) => (&event.data_base64, &mut stderr),
            _ => continue,
        };
        channel.extend(
            base64::engine::general_purpose::STANDARD
                .decode(payload)
                .unwrap(),
        );
        count += 1;
    }
    assert_eq!(output.protocol, launch_output::PROTOCOL);
    assert_eq!(output.stdout.bytes, stdout.len() as u64);
    assert_eq!(output.stdout.sha256, sha(&stdout));
    assert_eq!(output.stderr.bytes, stderr.len() as u64);
    assert_eq!(output.stderr.sha256, sha(&stderr));
    assert_eq!(output.data_event_count, count);
    let pages = &valid["pages_response"]["result"];
    assert_eq!(pages["session_id"], accepted.provider_session_id);
    assert_eq!(
        pages["session_id"],
        valid["pages_request"]["params"]["session_id"]
    );
    assert_eq!(
        pages["settings_id"],
        valid["pages_request"]["params"]["settings_id"]
    );
    assert_eq!(
        pages["provider_instance_id"],
        valid["pages_request"]["provider_instance_id"]
    );
    assert_eq!(
        valid["pages_response"]["request_id"],
        valid["pages_request"]["request_id"]
    );
    assert_eq!(
        valid["describe_response"]["request_id"],
        valid["describe_request"]["request_id"]
    );
    let turns = pages["turns"].as_array().unwrap();
    assert_eq!(pages["page_turn_count"], turns.len());
    let turn = &turns[0];
    assert_eq!(turn["session_id"], pages["session_id"]);
    assert_eq!(turn["body"][0]["text"], prompt);
    // Body accounting uses the typed compact body representation (type, text),
    // not the key ordering of a serde_json::Value map.
    let pages = SchemaRegistry::new()
        .decode_response::<SessionReadTurns>(&bytes(&valid["pages_response"]))
        .unwrap();
    let body = serde_json::to_vec(pages.value().result.turns[0].body.as_ref().unwrap()).unwrap();
    assert_eq!(turn["body_bytes"], body.len());
    assert_eq!(turn["body_sha256"], sha(&body));
    assert_eq!(turn["canonical_text_sha256"], sha(prompt.as_bytes()));
}

#[test]
fn stream_controls_discriminate_existing_enforcement_and_explicit_semantic_limits() {
    let fixture = golden();
    for case in fixture["invalid_streams"].as_array().unwrap() {
        let events = edited(fixture["valid"]["launch_events"].clone(), &case["edits"]);
        let error = validate_launch_ndjson(&ndjson(&events), "launch-host-extensions").unwrap_err();
        let category = match error {
            LaunchStreamError::MismatchedRequestId { .. } => "request_id",
            LaunchStreamError::InvalidSequence { .. } => "sequence",
            LaunchStreamError::Schema { .. } => "schema",
            LaunchStreamError::InvalidBase64 { .. } => "base64",
            LaunchStreamError::MissingFinalExit => "missing_exit",
            LaunchStreamError::EventAfterExit { .. } => "after_exit",
            _ => panic!("unexpected error {error} for {case}"),
        };
        assert_eq!(category, case["expect"], "{case}");
    }
    for case in fixture["semantic_limits"].as_array().unwrap() {
        let events = edited(fixture["valid"]["launch_events"].clone(), &case["edits"]);
        assert_ne!(
            events, fixture["valid"]["launch_events"],
            "control must change material"
        );
        match case["name"].as_str().unwrap() {
            "shape_valid_false_accounting" => {
                assert_ne!(
                    events[4]["value"]["stdout"],
                    fixture["valid"]["launch_events"][4]["value"]["stdout"]
                );
                assert_ne!(events[4]["value"]["data_event_count"], json!(3));
            }
            "mismatched_attestation" => {
                assert_ne!(
                    events[0]["value"]["delivery_nonce"],
                    fixture["valid"]["launch_request"]["params"]["prompt_acceptance"]
                        ["delivery_nonce"]
                );
                assert_ne!(
                    events[0]["value"]["prompt_sha256"],
                    fixture["valid"]["launch_request"]["params"]["prompt_acceptance"]
                        ["prompt_sha256"]
                );
            }
            "premature_completion_marker" => {
                assert_eq!(events[1]["name"], launch_output::MARKER_NAME);
                assert_eq!(events[4]["kind"], "stdout");
            }
            "missing_completion_marker" => {
                assert!(events
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|event| event["name"] != launch_output::MARKER_NAME));
            }
            name => panic!("unknown semantic-limit control {name}"),
        }
        assert!(
            validate_launch_ndjson(&ndjson(&events), "launch-host-extensions").is_ok(),
            "generic validation does not enforce {}",
            case["name"]
        );
    }
}

#[test]
fn ordinary_unselected_and_generic_marker_fixtures_remain_admitted() {
    let original: Value =
        serde_json::from_str(include_str!("fixtures/contract_v1/fixtures.json")).unwrap();
    let registry = SchemaRegistry::new();
    let request = &original["launch"]["request"];
    request_roundtrip::<Launch>(&registry, request);
    assert!(request["params"].get("prompt_acceptance").is_none());
    assert!(request["params"].get("output_delivery").is_none());
    let describe = &original["non_launch"]["describe"];
    request_roundtrip::<Describe>(&registry, &describe["request"]);
    response_roundtrip::<Describe>(&registry, &describe["success_response"]);
    for name in ["prompt_acceptance", "launch_output", "session_turn_pages"] {
        let (family, _, capability, _) = family(name);
        let request = registry
            .decode_request::<Describe>(&bytes(&describe["request"]))
            .unwrap();
        assert!(family
            .advertised(&[1], Some(&request.value().host.env))
            .is_empty());
        assert!(describe["success_response"]["result"]["capabilities"]
            .get(capability)
            .is_none());
    }
    let events: Vec<Value> = original["launch"]["sequence"]
        .as_array()
        .unwrap()
        .iter()
        .map(|kind| original["launch"]["events"][kind.as_str().unwrap()].clone())
        .collect();
    let marker = &original["launch"]["events"]["marker"];
    assert_ne!(marker["name"], prompt_acceptance::MARKER_NAME);
    assert_ne!(marker["name"], launch_output::MARKER_NAME);
    assert!(validate_launch_ndjson(
        &ndjson(&json!(events)),
        request["request_id"].as_str().unwrap()
    )
    .is_ok());
}

#[cfg(feature = "contract-test-fixtures")]
#[test]
fn exported_fixture_is_the_exercised_golden() {
    assert_eq!(
        agent_provider_contract::fixtures::HOST_EXTENSIONS_V1_JSON,
        GOLDEN
    );
}
