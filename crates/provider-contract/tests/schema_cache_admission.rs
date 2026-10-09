pub mod support {
    pub mod contract_matrix;
}

use agent_provider_contract::schemas::{SchemaRegistry, SchemaValidationError};
use serde_json::{json, Value};
use std::sync::Barrier;
use support::contract_matrix::{
    fixtures, launch_event_fixture, launch_fixture, non_launch_fixture, LAUNCH_EVENT_ROWS,
    NON_LAUNCH_ROWS,
};

type Validate = fn(&SchemaRegistry, &str, &Value) -> Result<(), SchemaValidationError>;

#[test]
fn concurrent_registries_keep_valid_invalid_and_definition_outcomes_separate() {
    let fixtures = fixtures();
    let barrier = Barrier::new(8);
    std::thread::scope(|scope| {
        for worker in 0..8 {
            let fixtures = &fixtures;
            let barrier = &barrier;
            scope.spawn(move || {
                barrier.wait();
                for offset in 0..NON_LAUNCH_ROWS.len() {
                    let row = &NON_LAUNCH_ROWS[(offset + worker) % NON_LAUNCH_ROWS.len()];
                    let registry = SchemaRegistry::new().clone();
                    for (part, definition, validate) in [
                        (
                            "request",
                            row.request_schema_def,
                            SchemaRegistry::validate_request as Validate,
                        ),
                        (
                            "success_response",
                            row.success_response_schema_def,
                            SchemaRegistry::validate_response as Validate,
                        ),
                        (
                            "error_response",
                            row.error_response_schema_def,
                            SchemaRegistry::validate_error_response as Validate,
                        ),
                    ] {
                        assert_outcomes(
                            &registry,
                            row.subcommand,
                            &format!("{}.schema.json", row.schema_file),
                            definition,
                            validate,
                            non_launch_fixture(fixtures, row.subcommand, part),
                        );
                    }
                }
                assert_outcomes(
                    &SchemaRegistry::default(),
                    "launch",
                    "launch.schema.json",
                    "LaunchRequest",
                    SchemaRegistry::validate_request,
                    launch_fixture(fixtures, "request"),
                );
                for row in LAUNCH_EVENT_ROWS {
                    assert_outcomes(
                        &SchemaRegistry::new(),
                        row.kind,
                        "launch.schema.json",
                        row.schema_def,
                        SchemaRegistry::validate_launch_event,
                        launch_event_fixture(fixtures, row.kind),
                    );
                }
                for index in 0..32 {
                    let unknown = format!("untrusted-{worker}-{index}");
                    let registry = SchemaRegistry::new();
                    assert_eq!(
                        registry.validate_request(&unknown, &json!({})),
                        Err(SchemaValidationError::UnknownSubcommand(unknown.clone()))
                    );
                    assert_eq!(
                        registry.validate_launch_event(&unknown, &json!({})),
                        Err(SchemaValidationError::UnknownLaunchEventKind(unknown))
                    );
                }
                let registry = SchemaRegistry::new();
                assert_eq!(
                    registry.validate_response("launch", &json!({})),
                    Err(SchemaValidationError::MissingResponseEnvelope(
                        "launch".into()
                    ))
                );
                assert_eq!(
                    registry.validate_error_response("launch", &json!({})),
                    Err(SchemaValidationError::MissingResponseEnvelope(
                        "launch".into()
                    ))
                );
            });
        }
    });
}

fn assert_outcomes(
    registry: &SchemaRegistry,
    target: &str,
    schema_file: &str,
    definition: &str,
    validate: Validate,
    valid: &Value,
) {
    let mut invalid = valid.clone();
    invalid["contract"] = json!("untrusted.invalid/v1");
    let first_error = validate(registry, target, &invalid).unwrap_err();
    match &first_error {
        SchemaValidationError::Validation {
            schema_file: actual_file,
            definition: actual_def,
            errors,
        } => {
            assert_eq!(*actual_file, schema_file);
            assert_eq!(*actual_def, definition);
            assert!(!errors.is_empty());
            assert!(errors.windows(2).all(|pair| pair[0] <= pair[1]));
        }
        other => panic!("expected payload refusal for {target}: {other}"),
    }
    validate(registry, target, valid).unwrap();
    validate(&SchemaRegistry::new(), target, valid).unwrap();
    assert_eq!(validate(registry, target, &invalid), Err(first_error));
}
