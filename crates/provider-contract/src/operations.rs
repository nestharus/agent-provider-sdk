use crate::generated as dto;
use crate::schemas::SubcommandSchema;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;

mod sealed {
    pub trait Sealed {}
}

pub trait RequestOperation: sealed::Sealed {
    type Request: DeserializeOwned + Serialize;
    const SCHEMA: SubcommandSchema;
    const SUBCOMMAND: &'static str = Self::SCHEMA.subcommand;
}

pub trait ResponseOperation: RequestOperation {
    type Response: DeserializeOwned + Serialize;
    type ErrorResponse: DeserializeOwned + Serialize;
}

#[derive(Debug, Clone, PartialEq)]
pub struct Admitted<T, Operation> {
    value: T,
    operation: PhantomData<fn() -> Operation>,
}

impl<T, Operation> Admitted<T, Operation> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            value,
            operation: PhantomData,
        }
    }

    pub fn value(&self) -> &T {
        &self.value
    }

    pub fn into_inner(self) -> T {
        self.value
    }
}

macro_rules! operation {
    (
        $name:ident,
        $subcommand:literal,
        $schema_file:literal,
        $stem:literal,
        $request:ty,
        $response:ty,
        $error:ty
    ) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct $name;

        impl sealed::Sealed for $name {}

        impl RequestOperation for $name {
            type Request = $request;
            const SCHEMA: SubcommandSchema = SubcommandSchema {
                subcommand: $subcommand,
                schema_file: $schema_file,
                request_def: concat!($stem, "Request"),
                response_def: Some(concat!($stem, "Response")),
                error_response_def: Some(concat!($stem, "ErrorResponse")),
            };
        }

        impl ResponseOperation for $name {
            type Response = $response;
            type ErrorResponse = $error;
        }
    };
}

macro_rules! request_only_operation {
    ($name:ident, $subcommand:literal, $schema_file:literal, $request_def:literal, $request:ty) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct $name;

        impl sealed::Sealed for $name {}

        impl RequestOperation for $name {
            type Request = $request;
            const SCHEMA: SubcommandSchema = SubcommandSchema {
                subcommand: $subcommand,
                schema_file: $schema_file,
                request_def: $request_def,
                response_def: None,
                error_response_def: None,
            };
        }
    };
}

macro_rules! declare_operations {
    (
        response {
            $(
                $name:ident => (
                    $subcommand:literal,
                    $schema_file:literal,
                    $stem:literal,
                    $request:ty,
                    $response:ty,
                    $error:ty
                );
            )+
        }
        request_only {
            $(
                $request_name:ident => (
                    $request_subcommand:literal,
                    $request_schema_file:literal,
                    $request_def:literal,
                    $request_type:ty
                );
            )+
        }
    ) => {
        $(
            operation!(
                $name,
                $subcommand,
                $schema_file,
                $stem,
                $request,
                $response,
                $error
            );
        )+
        $(
            request_only_operation!(
                $request_name,
                $request_subcommand,
                $request_schema_file,
                $request_def,
                $request_type
            );
        )+

        pub const OPERATION_SCHEMAS: &[SubcommandSchema] = &[
            $(<$name as RequestOperation>::SCHEMA,)+
            $(<$request_name as RequestOperation>::SCHEMA,)+
        ];
    };
}

declare_operations! {
    response {
        Describe => ("describe", "describe.schema.json", "Describe", dto::DescribeRequest, dto::DescribeResponse, dto::DescribeErrorResponse);
        Schema => ("schema", "schema.schema.json", "Schema", dto::SchemaRequest, dto::SchemaResponse, dto::SchemaErrorResponse);
        SettingsList => ("settings.list", "settings.schema.json", "SettingsList", dto::SettingsListRequest, dto::SettingsListResponse, dto::SettingsListErrorResponse);
        SettingsGet => ("settings.get", "settings.schema.json", "SettingsGet", dto::SettingsGetRequest, dto::SettingsGetResponse, dto::SettingsGetErrorResponse);
        SettingsCreate => ("settings.create", "settings.schema.json", "SettingsCreate", dto::SettingsCreateRequest, dto::SettingsCreateResponse, dto::SettingsCreateErrorResponse);
        SettingsUpdate => ("settings.update", "settings.schema.json", "SettingsUpdate", dto::SettingsUpdateRequest, dto::SettingsUpdateResponse, dto::SettingsUpdateErrorResponse);
        SettingsDelete => ("settings.delete", "settings.schema.json", "SettingsDelete", dto::SettingsDeleteRequest, dto::SettingsDeleteResponse, dto::SettingsDeleteErrorResponse);
        SettingsValidate => ("settings.validate", "settings.schema.json", "SettingsValidate", dto::SettingsValidateRequest, dto::SettingsValidateResponse, dto::SettingsValidateErrorResponse);
        SettingsMigrate => ("settings.migrate", "settings.schema.json", "SettingsMigrate", dto::SettingsMigrateRequest, dto::SettingsMigrateResponse, dto::SettingsMigrateErrorResponse);
        PolicyEvaluate => ("policy.evaluate", "policy.schema.json", "PolicyEvaluate", dto::PolicyEvaluateRequest, dto::PolicyEvaluateResponse, dto::PolicyEvaluateErrorResponse);
        TerminalClassify => ("terminal.classify", "terminal.schema.json", "TerminalClassify", dto::TerminalClassifyRequest, dto::TerminalClassifyResponse, dto::TerminalClassifyErrorResponse);
        QuotaSource => ("quota.source", "quota.schema.json", "QuotaSource", dto::QuotaSourceRequest, dto::QuotaSourceResponse, dto::QuotaSourceErrorResponse);
        QuotaProbe => ("quota.probe", "quota.schema.json", "QuotaProbe", dto::QuotaProbeRequest, dto::QuotaProbeResponse, dto::QuotaProbeErrorResponse);
        QuotaRefreshAuth => ("quota.refresh_auth", "quota.schema.json", "QuotaRefreshAuth", dto::QuotaRefreshAuthRequest, dto::QuotaRefreshAuthResponse, dto::QuotaRefreshAuthErrorResponse);
        SessionEnumerate => ("session.enumerate", "session.schema.json", "SessionEnumerate", dto::SessionEnumerateRequest, dto::SessionEnumerateResponse, dto::SessionEnumerateErrorResponse);
        SessionLocateTranscript => ("session.locate_transcript", "session.schema.json", "SessionLocateTranscript", dto::SessionLocateTranscriptRequest, dto::SessionLocateTranscriptResponse, dto::SessionLocateTranscriptErrorResponse);
        SessionReadTurns => ("session.read_turns", "session.schema.json", "SessionReadTurns", dto::SessionReadTurnsRequest, dto::SessionReadTurnsResponse, dto::SessionReadTurnsErrorResponse);
        SessionCapture => ("session.capture", "session.schema.json", "SessionCapture", dto::SessionCaptureRequest, dto::SessionCaptureResponse, dto::SessionCaptureErrorResponse);
        SessionExport => ("session.export", "session.schema.json", "SessionExport", dto::SessionExportRequest, dto::SessionExportResponse, dto::SessionExportErrorResponse);
        SessionReplace => ("session.replace", "session.schema.json", "SessionReplace", dto::SessionReplaceRequest, dto::SessionReplaceResponse, dto::SessionReplaceErrorResponse);
        RotationAssess => ("rotation.assess", "rotation.schema.json", "RotationAssess", dto::RotationAssessRequest, dto::RotationAssessResponse, dto::RotationAssessErrorResponse);
        RotationMaterialize => ("rotation.materialize", "rotation.schema.json", "RotationMaterialize", dto::RotationMaterializeRequest, dto::RotationMaterializeResponse, dto::RotationMaterializeErrorResponse);
        DiscoveryModels => ("discovery.models", "discovery.schema.json", "DiscoveryModels", dto::DiscoveryModelsRequest, dto::DiscoveryModelsResponse, dto::DiscoveryModelsErrorResponse);
        DiscoveryAccounts => ("discovery.accounts", "discovery.schema.json", "DiscoveryAccounts", dto::DiscoveryAccountsRequest, dto::DiscoveryAccountsResponse, dto::DiscoveryAccountsErrorResponse);
        SetupDetect => ("setup.detect", "setup.schema.json", "SetupDetect", dto::SetupDetectRequest, dto::SetupDetectResponse, dto::SetupDetectErrorResponse);
        SetupInstallPlan => ("setup.install_plan", "setup.schema.json", "SetupInstallPlan", dto::SetupInstallPlanRequest, dto::SetupInstallPlanResponse, dto::SetupInstallPlanErrorResponse);
        SetupSyncPlan => ("setup.sync_plan", "setup.schema.json", "SetupSyncPlan", dto::SetupSyncPlanRequest, dto::SetupSyncPlanResponse, dto::SetupSyncPlanErrorResponse);
        SetupBrainTurn => ("setup_brain.turn", "setup.schema.json", "SetupBrainTurn", dto::SetupBrainTurnRequest, dto::SetupBrainTurnResponse, dto::SetupBrainTurnErrorResponse);
        MigrationPlan => ("migration.plan", "migration.schema.json", "MigrationPlan", dto::MigrationPlanRequest, dto::MigrationPlanResponse, dto::MigrationPlanErrorResponse);
        MigrationApply => ("migration.apply", "migration.schema.json", "MigrationApply", dto::MigrationApplyRequest, dto::MigrationApplyResponse, dto::MigrationApplyErrorResponse);
    }
    request_only {
        Launch => ("launch", "launch.schema.json", "LaunchRequest", dto::LaunchRequest);
    }
}
