use crate::schemas::{SchemaRegistry, SchemaValidationError};
use crate::{generated::LaunchEvent, CONTRACT_VERSION};
use base64::Engine;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub struct ValidatedLaunchStream {
    request_id: String,
    events: Vec<LaunchEvent>,
    exit_seq: u64,
}

impl ValidatedLaunchStream {
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub fn events(&self) -> &[LaunchEvent] {
        &self.events
    }

    pub fn event_count(&self) -> usize {
        self.events.len()
    }

    pub fn exit_seq(&self) -> u64 {
        self.exit_seq
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LaunchStreamError {
    #[error("launch stream is not UTF-8: {0}")]
    InvalidUtf8(String),
    #[error("launch stream contains a blank line at {line}")]
    BlankLine { line: usize },
    #[error("launch stream line {line} is malformed JSON: {message}")]
    MalformedLine { line: usize, message: String },
    #[error("launch stream line {line} has unknown event kind: {kind}")]
    UnknownEventKind { line: usize, kind: String },
    #[error("launch stream line {line} violates the {kind} event schema: {source}")]
    Schema {
        line: usize,
        kind: String,
        source: SchemaValidationError,
    },
    #[error("launch stream line {line} cannot be represented as a typed event: {message}")]
    Dto { line: usize, message: String },
    #[error("launch stream line {line} has contract {actual}, expected {expected}")]
    MismatchedContract {
        line: usize,
        expected: String,
        actual: String,
    },
    #[error("launch stream line {line} has request ID {actual}, expected {expected}")]
    MismatchedRequestId {
        line: usize,
        expected: String,
        actual: String,
    },
    #[error("launch stream line {line} has sequence {actual}, expected {expected}")]
    InvalidSequence {
        line: usize,
        expected: u64,
        actual: u64,
    },
    #[error("launch stream line {line} contains invalid base64 payload")]
    InvalidBase64 { line: usize },
    #[error("launch stream contains a second exit event at line {line}")]
    DuplicateExit { line: usize },
    #[error("launch stream contains {kind} after exit at line {line}")]
    EventAfterExit { line: usize, kind: String },
    #[error("launch stream has no final exit event")]
    MissingFinalExit,
}

pub fn validate_launch_ndjson(
    bytes: &[u8],
    expected_request_id: &str,
) -> Result<ValidatedLaunchStream, LaunchStreamError> {
    let input = std::str::from_utf8(bytes)
        .map_err(|error| LaunchStreamError::InvalidUtf8(error.to_string()))?;
    let registry = SchemaRegistry::new();
    let mut events = Vec::new();
    let mut expected_seq = 1;
    let mut exit_seq = None;

    for (index, line) in input.split_terminator('\n').enumerate() {
        let line_number = index + 1;
        if line.trim().is_empty() {
            return Err(LaunchStreamError::BlankLine { line: line_number });
        }

        let event: Value =
            serde_json::from_str(line).map_err(|error| LaunchStreamError::MalformedLine {
                line: line_number,
                message: error.to_string(),
            })?;
        let kind = required_string(&event, "kind").unwrap_or("<missing>");
        if registry.schema_for_launch_event(kind).is_none() {
            return Err(LaunchStreamError::UnknownEventKind {
                line: line_number,
                kind: kind.to_owned(),
            });
        }
        if let Some(contract) = required_string(&event, "contract") {
            if contract != CONTRACT_VERSION {
                return Err(LaunchStreamError::MismatchedContract {
                    line: line_number,
                    expected: CONTRACT_VERSION.to_owned(),
                    actual: contract.to_owned(),
                });
            }
        }
        if let Some(request_id) = required_string(&event, "request_id") {
            if request_id != expected_request_id {
                return Err(LaunchStreamError::MismatchedRequestId {
                    line: line_number,
                    expected: expected_request_id.to_owned(),
                    actual: request_id.to_owned(),
                });
            }
        }
        registry
            .validate_launch_event(kind, &event)
            .map_err(|source| LaunchStreamError::Schema {
                line: line_number,
                kind: kind.to_owned(),
                source,
            })?;

        if exit_seq.is_some() {
            return Err(if kind == "exit" {
                LaunchStreamError::DuplicateExit { line: line_number }
            } else {
                LaunchStreamError::EventAfterExit {
                    line: line_number,
                    kind: kind.to_owned(),
                }
            });
        }
        let seq = event["seq"].as_u64().expect("schema requires integer seq");
        if seq != expected_seq {
            return Err(LaunchStreamError::InvalidSequence {
                line: line_number,
                expected: expected_seq,
                actual: seq,
            });
        }

        if matches!(kind, "stdout" | "stderr") {
            let payload = required_string(&event, "data_base64")
                .expect("stdout and stderr schemas require data_base64");
            base64::engine::general_purpose::STANDARD
                .decode(payload)
                .map_err(|_| LaunchStreamError::InvalidBase64 { line: line_number })?;
        }
        if kind == "exit" {
            exit_seq = Some(seq);
        }

        let typed = serde_json::from_value(event).map_err(|error| LaunchStreamError::Dto {
            line: line_number,
            message: error.to_string(),
        })?;
        events.push(typed);
        expected_seq += 1;
    }

    let exit_seq = exit_seq.ok_or(LaunchStreamError::MissingFinalExit)?;
    Ok(ValidatedLaunchStream {
        request_id: expected_request_id.to_owned(),
        events,
        exit_seq,
    })
}

fn required_string<'a>(value: &'a Value, field: &str) -> Option<&'a str> {
    value.get(field).and_then(Value::as_str)
}
