pub use crate::operations::OPERATION_SCHEMAS as SUBCOMMAND_SCHEMAS;
use crate::operations::{Admitted, RequestOperation, ResponseOperation};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

pub const SCHEMA_DRAFT_2020_12: &str = "https://json-schema.org/draft/2020-12/schema";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaFile {
    pub filename: &'static str,
    pub contents: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubcommandSchema {
    pub subcommand: &'static str,
    pub schema_file: &'static str,
    pub request_def: &'static str,
    pub response_def: Option<&'static str>,
    pub error_response_def: Option<&'static str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LaunchEventSchema {
    pub kind: &'static str,
    pub schema_file: &'static str,
    pub event_def: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaValidationError {
    #[error("unknown schema file: {0}")]
    UnknownSchemaFile(String),
    #[error("unknown subcommand: {0}")]
    UnknownSubcommand(String),
    #[error("unknown launch event kind: {0}")]
    UnknownLaunchEventKind(String),
    #[error("subcommand has no single response envelope: {0}")]
    MissingResponseEnvelope(String),
    #[error("schema parse failed for {schema_file}: {message}")]
    SchemaParse {
        schema_file: &'static str,
        message: String,
    },
    #[error("schema compile failed for {schema_file}#{definition}: {message}")]
    SchemaCompile {
        schema_file: &'static str,
        definition: &'static str,
        message: String,
    },
    #[error("schema validation failed for {schema_file}#{definition}: {errors:?}")]
    Validation {
        schema_file: &'static str,
        definition: &'static str,
        errors: Vec<String>,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum ContractAdmissionError {
    #[error("wire payload is not valid JSON: {message}")]
    Json { message: String },
    #[error(transparent)]
    Schema(#[from] SchemaValidationError),
    #[error("schema-admitted payload does not match its DTO: {message}")]
    Dto { message: String },
    #[error("DTO cannot be represented as wire JSON: {message}")]
    Serialization { message: String },
}

#[derive(Debug, Clone)]
pub struct SchemaRegistry {
    files: BTreeMap<&'static str, &'static str>,
    subcommands: BTreeMap<&'static str, SubcommandSchema>,
    launch_events: BTreeMap<&'static str, LaunchEventSchema>,
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self {
            files: SCHEMA_FILES
                .iter()
                .map(|file| (file.filename, file.contents))
                .collect(),
            subcommands: SUBCOMMAND_SCHEMAS
                .iter()
                .map(|schema| (schema.subcommand, *schema))
                .collect(),
            launch_events: LAUNCH_EVENT_SCHEMAS
                .iter()
                .map(|schema| (schema.kind, *schema))
                .collect(),
        }
    }

    pub fn schema_by_file(&self, filename: &str) -> Option<&'static str> {
        let normalized = normalize_schema_filename(filename);
        self.files.get(normalized.as_str()).copied()
    }

    pub fn schema_for_subcommand(&self, subcommand: &str) -> Option<SubcommandSchema> {
        self.subcommands.get(subcommand).copied()
    }

    pub fn schema_for_launch_event(&self, kind: &str) -> Option<LaunchEventSchema> {
        self.launch_events.get(kind).copied()
    }

    pub fn validate_request(
        &self,
        subcommand: &str,
        instance: &Value,
    ) -> Result<(), SchemaValidationError> {
        let target = self
            .schema_for_subcommand(subcommand)
            .ok_or_else(|| SchemaValidationError::UnknownSubcommand(subcommand.to_owned()))?;
        self.validate_definition(target.schema_file, target.request_def, instance)
    }

    pub fn validate_response(
        &self,
        subcommand: &str,
        instance: &Value,
    ) -> Result<(), SchemaValidationError> {
        let target = self
            .schema_for_subcommand(subcommand)
            .ok_or_else(|| SchemaValidationError::UnknownSubcommand(subcommand.to_owned()))?;
        let definition = target
            .response_def
            .ok_or_else(|| SchemaValidationError::MissingResponseEnvelope(subcommand.to_owned()))?;
        self.validate_definition(target.schema_file, definition, instance)
    }

    pub fn validate_error_response(
        &self,
        subcommand: &str,
        instance: &Value,
    ) -> Result<(), SchemaValidationError> {
        let target = self
            .schema_for_subcommand(subcommand)
            .ok_or_else(|| SchemaValidationError::UnknownSubcommand(subcommand.to_owned()))?;
        let definition = target
            .error_response_def
            .ok_or_else(|| SchemaValidationError::MissingResponseEnvelope(subcommand.to_owned()))?;
        self.validate_definition(target.schema_file, definition, instance)
    }

    pub fn validate_launch_event(
        &self,
        kind: &str,
        instance: &Value,
    ) -> Result<(), SchemaValidationError> {
        let target = self
            .schema_for_launch_event(kind)
            .ok_or_else(|| SchemaValidationError::UnknownLaunchEventKind(kind.to_owned()))?;
        self.validate_definition(target.schema_file, target.event_def, instance)
    }

    pub fn decode_request<O>(
        &self,
        bytes: &[u8],
    ) -> Result<Admitted<O::Request, O>, ContractAdmissionError>
    where
        O: RequestOperation,
    {
        let instance = parse_wire_json(bytes)?;
        self.validate_request(O::SUBCOMMAND, &instance)?;
        deserialize_admitted(instance).map(Admitted::new)
    }

    pub fn decode_response<O>(
        &self,
        bytes: &[u8],
    ) -> Result<Admitted<O::Response, O>, ContractAdmissionError>
    where
        O: ResponseOperation,
    {
        let instance = parse_wire_json(bytes)?;
        self.validate_response(O::SUBCOMMAND, &instance)?;
        deserialize_admitted(instance).map(Admitted::new)
    }

    pub fn decode_error_response<O>(
        &self,
        bytes: &[u8],
    ) -> Result<Admitted<O::ErrorResponse, O>, ContractAdmissionError>
    where
        O: ResponseOperation,
    {
        let instance = parse_wire_json(bytes)?;
        self.validate_error_response(O::SUBCOMMAND, &instance)?;
        deserialize_admitted(instance).map(Admitted::new)
    }

    pub fn encode_request<O>(&self, value: &O::Request) -> Result<Vec<u8>, ContractAdmissionError>
    where
        O: RequestOperation,
    {
        let instance = serialize_dto(value)?;
        self.validate_request(O::SUBCOMMAND, &instance)?;
        encode_wire_json(&instance)
    }

    pub fn encode_response<O>(&self, value: &O::Response) -> Result<Vec<u8>, ContractAdmissionError>
    where
        O: ResponseOperation,
    {
        let instance = serialize_dto(value)?;
        self.validate_response(O::SUBCOMMAND, &instance)?;
        encode_wire_json(&instance)
    }

    pub fn encode_error_response<O>(
        &self,
        value: &O::ErrorResponse,
    ) -> Result<Vec<u8>, ContractAdmissionError>
    where
        O: ResponseOperation,
    {
        let instance = serialize_dto(value)?;
        self.validate_error_response(O::SUBCOMMAND, &instance)?;
        encode_wire_json(&instance)
    }

    fn validate_definition(
        &self,
        schema_file: &'static str,
        definition: &'static str,
        instance: &Value,
    ) -> Result<(), SchemaValidationError> {
        let contents = self
            .schema_by_file(schema_file)
            .ok_or_else(|| SchemaValidationError::UnknownSchemaFile(schema_file.to_owned()))?;
        let mut schema = parse_schema(schema_file, contents)?;
        let common = parse_schema("common.schema.json", COMMON_SCHEMA)?;
        merge_common_defs_and_rewrite_refs(&mut schema, &common);

        let defs = schema
            .get("$defs")
            .cloned()
            .unwrap_or_else(|| Value::Object(Map::new()));
        let wrapper = json!({
            "$schema": SCHEMA_DRAFT_2020_12,
            "$defs": defs,
            "$ref": format!("#/$defs/{definition}")
        });
        let validator = jsonschema::validator_for(&wrapper).map_err(|error| {
            SchemaValidationError::SchemaCompile {
                schema_file,
                definition,
                message: error.to_string(),
            }
        })?;
        let mut errors = validator
            .iter_errors(instance)
            .map(|error| error.to_string())
            .collect::<Vec<_>>();
        errors.sort();

        if errors.is_empty() {
            Ok(())
        } else {
            Err(SchemaValidationError::Validation {
                schema_file,
                definition,
                errors,
            })
        }
    }
}

pub fn schema_by_file(filename: &str) -> Option<&'static str> {
    let normalized = normalize_schema_filename(filename);
    SCHEMA_FILES
        .iter()
        .find(|file| file.filename == normalized)
        .map(|file| file.contents)
}

pub fn schema_for_subcommand(subcommand: &str) -> Option<SubcommandSchema> {
    SchemaRegistry::new().schema_for_subcommand(subcommand)
}

pub fn schema_for_launch_event(kind: &str) -> Option<LaunchEventSchema> {
    SchemaRegistry::new().schema_for_launch_event(kind)
}

pub fn validate_request(subcommand: &str, instance: &Value) -> Result<(), SchemaValidationError> {
    SchemaRegistry::new().validate_request(subcommand, instance)
}

pub fn validate_response(subcommand: &str, instance: &Value) -> Result<(), SchemaValidationError> {
    SchemaRegistry::new().validate_response(subcommand, instance)
}

pub fn validate_error_response(
    subcommand: &str,
    instance: &Value,
) -> Result<(), SchemaValidationError> {
    SchemaRegistry::new().validate_error_response(subcommand, instance)
}

pub fn validate_launch_event(kind: &str, instance: &Value) -> Result<(), SchemaValidationError> {
    SchemaRegistry::new().validate_launch_event(kind, instance)
}

pub fn decode_request<O>(bytes: &[u8]) -> Result<Admitted<O::Request, O>, ContractAdmissionError>
where
    O: RequestOperation,
{
    SchemaRegistry::new().decode_request::<O>(bytes)
}

pub fn decode_response<O>(bytes: &[u8]) -> Result<Admitted<O::Response, O>, ContractAdmissionError>
where
    O: ResponseOperation,
{
    SchemaRegistry::new().decode_response::<O>(bytes)
}

pub fn decode_error_response<O>(
    bytes: &[u8],
) -> Result<Admitted<O::ErrorResponse, O>, ContractAdmissionError>
where
    O: ResponseOperation,
{
    SchemaRegistry::new().decode_error_response::<O>(bytes)
}

pub fn encode_request<O>(value: &O::Request) -> Result<Vec<u8>, ContractAdmissionError>
where
    O: RequestOperation,
{
    SchemaRegistry::new().encode_request::<O>(value)
}

pub fn encode_response<O>(value: &O::Response) -> Result<Vec<u8>, ContractAdmissionError>
where
    O: ResponseOperation,
{
    SchemaRegistry::new().encode_response::<O>(value)
}

pub fn encode_error_response<O>(value: &O::ErrorResponse) -> Result<Vec<u8>, ContractAdmissionError>
where
    O: ResponseOperation,
{
    SchemaRegistry::new().encode_error_response::<O>(value)
}

fn parse_wire_json(bytes: &[u8]) -> Result<Value, ContractAdmissionError> {
    serde_json::from_slice(bytes).map_err(|error| ContractAdmissionError::Json {
        message: error.to_string(),
    })
}

fn deserialize_admitted<T>(instance: Value) -> Result<T, ContractAdmissionError>
where
    T: DeserializeOwned,
{
    serde_json::from_value(instance).map_err(|error| ContractAdmissionError::Dto {
        message: error.to_string(),
    })
}

fn serialize_dto<T>(value: &T) -> Result<Value, ContractAdmissionError>
where
    T: Serialize,
{
    serde_json::to_value(value).map_err(|error| ContractAdmissionError::Serialization {
        message: error.to_string(),
    })
}

fn encode_wire_json(instance: &Value) -> Result<Vec<u8>, ContractAdmissionError> {
    serde_json::to_vec(instance).map_err(|error| ContractAdmissionError::Serialization {
        message: error.to_string(),
    })
}

macro_rules! event_row {
    ($kind:literal, $definition:literal) => {
        LaunchEventSchema {
            kind: $kind,
            schema_file: "launch.schema.json",
            event_def: $definition,
        }
    };
}

const COMMON_SCHEMA: &str = include_str!("../contract/v1/common.schema.json");

pub const SCHEMA_FILES: &[SchemaFile] = &[
    SchemaFile {
        filename: "common.schema.json",
        contents: COMMON_SCHEMA,
    },
    SchemaFile {
        filename: "describe.schema.json",
        contents: include_str!("../contract/v1/describe.schema.json"),
    },
    SchemaFile {
        filename: "schema.schema.json",
        contents: include_str!("../contract/v1/schema.schema.json"),
    },
    SchemaFile {
        filename: "settings.schema.json",
        contents: include_str!("../contract/v1/settings.schema.json"),
    },
    SchemaFile {
        filename: "launch.schema.json",
        contents: include_str!("../contract/v1/launch.schema.json"),
    },
    SchemaFile {
        filename: "policy.schema.json",
        contents: include_str!("../contract/v1/policy.schema.json"),
    },
    SchemaFile {
        filename: "quota.schema.json",
        contents: include_str!("../contract/v1/quota.schema.json"),
    },
    SchemaFile {
        filename: "terminal.schema.json",
        contents: include_str!("../contract/v1/terminal.schema.json"),
    },
    SchemaFile {
        filename: "session.schema.json",
        contents: include_str!("../contract/v1/session.schema.json"),
    },
    SchemaFile {
        filename: "rotation.schema.json",
        contents: include_str!("../contract/v1/rotation.schema.json"),
    },
    SchemaFile {
        filename: "discovery.schema.json",
        contents: include_str!("../contract/v1/discovery.schema.json"),
    },
    SchemaFile {
        filename: "setup.schema.json",
        contents: include_str!("../contract/v1/setup.schema.json"),
    },
    SchemaFile {
        filename: "migration.schema.json",
        contents: include_str!("../contract/v1/migration.schema.json"),
    },
];

pub const LAUNCH_EVENT_SCHEMAS: &[LaunchEventSchema] = &[
    event_row!("stdout", "LaunchStdoutEvent"),
    event_row!("stderr", "LaunchStderrEvent"),
    event_row!("marker", "LaunchMarkerEvent"),
    event_row!("heartbeat", "LaunchHeartbeatEvent"),
    event_row!("exit", "LaunchExitEvent"),
];

fn parse_schema(
    schema_file: &'static str,
    contents: &'static str,
) -> Result<Value, SchemaValidationError> {
    serde_json::from_str(contents).map_err(|err| SchemaValidationError::SchemaParse {
        schema_file,
        message: err.to_string(),
    })
}

fn normalize_schema_filename(filename: &str) -> String {
    let filename = filename.strip_prefix("contract/v1/").unwrap_or(filename);
    if filename.ends_with(".schema.json") {
        filename.to_owned()
    } else {
        format!("{filename}.schema.json")
    }
}

fn merge_common_defs_and_rewrite_refs(schema: &mut Value, common: &Value) {
    rewrite_common_refs(schema);

    let Some(common_defs) = common.get("$defs").and_then(Value::as_object) else {
        return;
    };
    let schema_defs = schema
        .as_object_mut()
        .expect("schema root must be a JSON object")
        .entry("$defs")
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .expect("schema $defs must be a JSON object");
    for (name, definition) in common_defs {
        schema_defs
            .entry(name.clone())
            .or_insert_with(|| definition.clone());
    }
}

fn rewrite_common_refs(value: &mut Value) {
    match value {
        Value::Object(object) => {
            let reference = object
                .get("$ref")
                .and_then(Value::as_str)
                .map(str::to_owned);
            if let Some(reference) = reference {
                if let Some(definition) = reference.strip_prefix("common.schema.json#/$defs/") {
                    object.insert(
                        "$ref".to_owned(),
                        Value::String(format!("#/$defs/{definition}")),
                    );
                }
            }
            for child in object.values_mut() {
                rewrite_common_refs(child);
            }
        }
        Value::Array(values) => {
            for child in values {
                rewrite_common_refs(child);
            }
        }
        _ => {}
    }
}
