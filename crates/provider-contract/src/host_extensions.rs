//! Canonical vocabulary for the three host-selected extensions already defined
//! in the base provider/v1 schemas. DTOs remain in [`crate::generated`]; wire
//! admission remains in [`crate::schemas`] and [`crate::launch_stream`].
//!
//! Offers come from the request's `host.env`, never the process environment.
//! Use each [`crate::negotiation::VersionFamily`] with the versions actually
//! offered/supported. These definitions do not advertise capabilities, choose
//! host policy, attest submission, or verify request-to-stream accounting.

/// The exact request-local opt-in value; other strings do not offer support.
pub use crate::negotiation::SELECTED as OPT_IN_VALUE;

/// Exact-prompt submission attestation vocabulary (not host trust policy).
pub mod prompt_acceptance {
    use crate::negotiation::VersionFamily;

    pub const PROTOCOL: &str = "oulipoly.prompt_acceptance/v1";
    pub const MARKER_NAME: &str = "oulipoly.prompt_accepted/v1";
    pub const SELECTOR: &str = "OULIPOLY_HOST_PROMPT_ACCEPTANCE_V1";
    pub const CAPABILITY: &str = "prompt_acceptance_v1";
    /// Versions defined here, not a provider's support declaration.
    pub const SUPPORTED_VERSIONS: &[u32] = &[1];
    pub const FAMILY: VersionFamily = VersionFamily {
        selector_prefix: "OULIPOLY_HOST_PROMPT_ACCEPTANCE_V",
        capability_prefix: "prompt_acceptance_v",
    };
}

/// Launch-output delivery request and completion-summary vocabulary.
pub mod launch_output {
    use crate::negotiation::VersionFamily;

    pub const PROTOCOL: &str = "oulipoly.launch_output/v1";
    pub const MARKER_NAME: &str = "oulipoly.launch_output_complete/v1";
    pub const SELECTOR: &str = "OULIPOLY_HOST_LAUNCH_OUTPUT_V1";
    pub const CAPABILITY: &str = "launch_output_v1";
    /// Versions defined here, not a provider's support declaration.
    pub const SUPPORTED_VERSIONS: &[u32] = &[1];
    pub const FAMILY: VersionFamily = VersionFamily {
        selector_prefix: "OULIPOLY_HOST_LAUNCH_OUTPUT_V",
        capability_prefix: "launch_output_v",
    };
}

/// Bounded `session.read_turns` vocabulary; reader authority stays with the host.
pub mod session_turn_pages {
    use crate::negotiation::VersionFamily;

    pub const PROTOCOL: &str = "oulipoly.session_turn_pages/v1";
    pub const SELECTOR: &str = "OULIPOLY_HOST_SESSION_TURN_PAGES_V1";
    pub const CAPABILITY: &str = "session_turn_pages_v1";
    /// Versions defined here, not a provider's support declaration.
    pub const SUPPORTED_VERSIONS: &[u32] = &[1];
    pub const FAMILY: VersionFamily = VersionFamily {
        selector_prefix: "OULIPOLY_HOST_SESSION_TURN_PAGES_V",
        capability_prefix: "session_turn_pages_v",
    };
}
