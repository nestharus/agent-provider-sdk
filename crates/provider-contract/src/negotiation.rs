//! Common supported-version selection for provider/v1 and its host-selected
//! extensions.
//!
//! Compatibility is decided by declared supported wire versions and capability
//! agreement, never by executable, banner, package or source identity.
//!
//! * **Contract versions.** [`select_contract_version`] picks one
//!   `oulipoly.provider/vN` both sides support: the provider's preferred
//!   version when the host supports it, otherwise the highest common one.
//!   Versions the host does not know are ignored rather than refused, so a
//!   provider that also advertises a newer version stays usable through a
//!   version the host still supports. Describe admits future advertisements,
//!   while selected v1 payloads remain strict. The preferred version must be
//!   declared; both schema admission and this chooser enforce that invariant.
//! * **Extension versions.** A host offers each version of an extension
//!   family it supports with its own `host.env` selector
//!   (`<SELECTOR_PREFIX><n>=1`). A provider advertises the
//!   `<capability_prefix><n>: true` describe capability only for offered
//!   versions it supports ([`VersionFamily::advertised`]). The
//!   host uses the highest offered version the provider advertised
//!   ([`VersionFamily::select`]); no common version is an explicit refusal.

use serde_json::{Map, Value};
use std::collections::BTreeMap;

/// No version is supported by both sides.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("no common supported version (host {host:?}, peer {peer:?})")]
pub struct NoCommonVersion {
    pub host: Vec<String>,
    pub peer: Vec<String>,
}

/// The provider/v1 contract family prefix.
pub const CONTRACT_PREFIX: &str = "oulipoly.provider/v";

/// The numeric version of `oulipoly.provider/vN`, if well formed.
pub fn contract_version_number(contract: &str) -> Option<u32> {
    let digits = contract.strip_prefix(CONTRACT_PREFIX)?;
    if digits.is_empty() || digits.starts_with('0') || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// Selects the contract version for a provider whose describe result
/// advertised `advertised` and `preferred`.
pub fn select_contract_version(
    host_supported: &[&str],
    advertised: &[String],
    preferred: &str,
) -> Result<String, NoCommonVersion> {
    let common = |version: &str| {
        contract_version_number(version).is_some()
            && host_supported.contains(&version)
            && advertised.iter().any(|offered| offered == version)
    };
    if !advertised.iter().any(|version| version == preferred) {
        return Err(NoCommonVersion {
            host: host_supported.iter().map(|v| (*v).to_owned()).collect(),
            peer: advertised.to_vec(),
        });
    }
    if common(preferred) {
        return Ok(preferred.to_owned());
    }
    advertised
        .iter()
        .filter(|version| common(version))
        .max_by_key(|version| contract_version_number(version))
        .cloned()
        .ok_or_else(|| NoCommonVersion {
            host: host_supported.iter().map(|v| (*v).to_owned()).collect(),
            peer: advertised.to_vec(),
        })
}

/// One host-selected extension family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionFamily {
    /// `host.env` selector prefix; version `n` is selected by `<prefix><n>=1`.
    pub selector_prefix: &'static str,
    /// Describe capability prefix; version `n` is `<prefix><n>`.
    pub capability_prefix: &'static str,
}

/// The exact selector value.
pub const SELECTED: &str = "1";

impl VersionFamily {
    pub fn selector(&self, version: u32) -> String {
        format!("{}{version}", self.selector_prefix)
    }

    pub fn capability(&self, version: u32) -> String {
        format!("{}{version}", self.capability_prefix)
    }

    /// Host side: the `host.env` entries offering every supported version.
    pub fn host_selectors(&self, host_supported: &[u32]) -> BTreeMap<String, String> {
        host_supported
            .iter()
            .map(|version| (self.selector(*version), SELECTED.to_owned()))
            .collect()
    }

    /// Provider side: versions the request's own `host.env` selected with the
    /// exact value `1`. The ambient process environment never selects.
    pub fn selected_by_host(&self, env: Option<&BTreeMap<String, String>>) -> Vec<u32> {
        let mut versions: Vec<u32> = env
            .into_iter()
            .flatten()
            .filter(|(_, value)| value.as_str() == SELECTED)
            .filter_map(|(key, _)| {
                let digits = key.strip_prefix(self.selector_prefix)?;
                (!digits.starts_with('0') && digits.bytes().all(|b| b.is_ascii_digit()))
                    .then(|| digits.parse().ok())
                    .flatten()
            })
            .collect();
        versions.sort_unstable();
        versions.dedup();
        versions
    }

    /// Provider side: the versions to advertise, ascending: those the host
    /// offered that the provider supports.
    pub fn advertised(
        &self,
        provider_supported: &[u32],
        env: Option<&BTreeMap<String, String>>,
    ) -> Vec<u32> {
        self.selected_by_host(env)
            .into_iter()
            .filter(|version| provider_supported.contains(version))
            .collect()
    }

    /// Provider side: sets `capabilities[<prefix><n>] = true` for every
    /// advertised version and nothing else.
    pub fn advertise_into(
        &self,
        capabilities: &mut Map<String, Value>,
        provider_supported: &[u32],
        env: Option<&BTreeMap<String, String>>,
    ) {
        for version in self.advertised(provider_supported, env) {
            capabilities.insert(self.capability(version), Value::Bool(true));
        }
    }

    /// Host side: the highest version the host supports whose capability the
    /// provider advertised `true`. Unknown and newer capability keys are
    /// ignored; `false`, absent and non-boolean values do not advertise.
    pub fn select(
        &self,
        host_supported: &[u32],
        capabilities: &Map<String, Value>,
    ) -> Result<u32, NoCommonVersion> {
        let advertised: Vec<u32> = capabilities
            .iter()
            .filter(|(_, value)| value.as_bool() == Some(true))
            .filter_map(|(key, _)| {
                let digits = key.strip_prefix(self.capability_prefix)?;
                (!digits.starts_with('0') && digits.bytes().all(|b| b.is_ascii_digit()))
                    .then(|| digits.parse::<u32>().ok())
                    .flatten()
            })
            .collect();
        advertised
            .iter()
            .copied()
            .filter(|version| host_supported.contains(version))
            .max()
            .ok_or_else(|| NoCommonVersion {
                host: host_supported.iter().map(u32::to_string).collect(),
                peer: advertised.iter().map(u32::to_string).collect(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_contract_versions_have_no_number() {
        assert_eq!(contract_version_number("oulipoly.provider/v1"), Some(1));
        assert_eq!(contract_version_number("oulipoly.provider/v01"), None);
        assert_eq!(contract_version_number("oulipoly.provider/v"), None);
        assert_eq!(contract_version_number("other/v1"), None);
    }
}
