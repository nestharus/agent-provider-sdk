//! `user_observation` cursors: authenticated, source-backed tokens with no
//! per-page storage and no canonical staging admission.
use super::{io_error, stale, Binding, Budgets, Cursor, Span, Stamp};
use crate::encoding::{decode_base64, encode_base64, sha256_hex};
use agent_provider_contract::generated::{ErrorCategory, ErrorObject};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;

// The binding is hashed rather than embedded, which keeps account paths and
// host identifiers out of tokens and bounds token size.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObservationCursor {
    binding: String,
    kind: String,
    budgets: Budgets,
    stamp: Stamp,
    head: String,
    snapshot: String,
    offset: u64,
    page: u64,
    sequence: u64,
    partial_record: Option<Span>,
    anchor: Option<Span>,
}

fn binding_digest(binding: &Binding) -> Result<String, ErrorObject> {
    Ok(sha256_hex(
        &serde_json::to_vec(binding).map_err(|_| io_error())?,
    ))
}

// HMAC-SHA256 (RFC 2104) with a fixed 32-byte key.
fn authenticate(key: &[u8; 32], bytes: &[u8]) -> [u8; 32] {
    let mut inner_pad = [0x36; 64];
    let mut outer_pad = [0x5c; 64];
    for i in 0..key.len() {
        inner_pad[i] ^= key[i];
        outer_pad[i] ^= key[i];
    }
    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update(bytes);
    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner.finalize());
    outer.finalize().into()
}

pub(super) fn token(prefix: &str, key: &[u8; 32], cursor: &Cursor) -> Result<String, ErrorObject> {
    let compact = ObservationCursor {
        binding: binding_digest(&cursor.binding)?,
        kind: cursor.kind.clone(),
        budgets: cursor.budgets.clone(),
        stamp: cursor.stamp.clone(),
        head: cursor.head.clone(),
        snapshot: cursor.snapshot.clone(),
        offset: cursor.offset,
        page: cursor.page,
        sequence: cursor.sequence,
        partial_record: cursor.partial_record.clone(),
        anchor: cursor.anchor.clone(),
    };
    let mut bytes = serde_json::to_vec(&compact).map_err(|_| io_error())?;
    let mac = authenticate(key, &bytes);
    bytes.extend_from_slice(&mac);
    Ok(format!("{prefix}{}", encode_base64(&bytes)))
}

pub(super) fn load(
    prefix: &str,
    key: &[u8; 32],
    value: &str,
    binding: &Binding,
) -> Result<Cursor, ErrorObject> {
    let encoded = value.strip_prefix(prefix).ok_or_else(stale)?;
    let bytes = decode_base64(encoded).map_err(|_| stale())?;
    // Reject alternate encodings as well as truncation.
    if bytes.len() < 32 || encode_base64(&bytes) != encoded {
        return Err(stale());
    }
    let (payload, supplied) = bytes.split_at(bytes.len() - 32);
    let expected = authenticate(key, payload);
    let difference = supplied
        .iter()
        .zip(expected)
        .fold(0u8, |diff, (a, b)| diff | (a ^ b));
    if difference != 0 {
        return Err(stale());
    }
    let compact: ObservationCursor = serde_json::from_slice(payload).map_err(|_| stale())?;
    if compact.binding != binding_digest(binding)? || compact.offset > compact.stamp.len {
        return Err(stale());
    }
    Ok(Cursor {
        canonical_format: None,
        verification: None,
        kind: compact.kind,
        binding: binding.clone(),
        budgets: compact.budgets,
        stamp: compact.stamp,
        head: compact.head,
        snapshot: compact.snapshot,
        offset: compact.offset,
        page: compact.page,
        sequence: compact.sequence,
        partial_record: compact.partial_record,
        anchor: compact.anchor,
    })
}

// One reserved preparation slot, never used to sign tokens. Every cooperating
// initializer holds the authority directory's inode lock until publication is
// durable.
const PREPARATION: &str = "key.preparing";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PublicationPoint {
    Created,
    Written,
    CandidateSynced,
    Published,
    FinalSynced,
    DirectorySynced,
}

/// The scope's observation signing key, created once. `issued` is true when
/// the request presents an observation token: missing authority for an issued
/// token never authorizes initialization.
pub(super) fn key(paging_root: &Path, issued: bool) -> Result<[u8; 32], ErrorObject> {
    key_with_observer(paging_root, issued, |_| Ok(()))
}

pub(super) fn key_with_observer(
    paging_root: &Path,
    issued: bool,
    mut observe: impl FnMut(PublicationPoint) -> std::io::Result<()>,
) -> Result<[u8; 32], ErrorObject> {
    // A sibling of the canonical pool, never inside its admitted objects.
    let root = paging_root
        .parent()
        .ok_or_else(io_error)?
        .join("observation-auth-v1");
    match fs::symlink_metadata(&root) {
        Ok(metadata) if !metadata.file_type().is_dir() => return Err(io_error()),
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(io_error()),
        _ => {}
    }
    crate::durable_fs::create_private_directories(&root).map_err(|_| io_error())?;
    let mut options = fs::OpenOptions::new();
    options.read(true);
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY);
    }
    let lock = options.open(&root).map_err(|_| io_error())?;
    fs2::FileExt::lock_exclusive(&lock).map_err(|_| io_error())?;
    let path = root.join("key");
    let published = match fs::symlink_metadata(&path) {
        Ok(_) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(_) => return Err(io_error()),
    };
    if !published {
        if issued {
            return Err(stale());
        }
        prepare_and_publish(&root, &mut observe).map_err(|_| io_error())?;
    }
    // Existing final authority is immutable. A malformed final may be damaged
    // issued authority, not demonstrably unissued preparation: never repair it.
    let mut file = open_private_key_file(&path).map_err(|_| malformed_key())?;
    if file.metadata().map_err(|_| io_error())?.len() != 32 {
        return Err(malformed_key());
    }
    let mut bytes = [0u8; 32];
    file.read_exact(&mut bytes).map_err(|_| io_error())?;
    // Also finish durability after an interruption that followed the rename:
    // no signing key leaves here before file and directory sync succeed.
    file.sync_all().map_err(|_| io_error())?;
    observe(PublicationPoint::FinalSynced).map_err(|_| io_error())?;
    lock.sync_all().map_err(|_| io_error())?;
    observe(PublicationPoint::DirectorySynced).map_err(|_| io_error())?;
    Ok(bytes)
}

fn malformed_key() -> ErrorObject {
    super::error(
        ErrorCategory::Failed,
        "session_turn_page_io",
        "Published observation authentication key is malformed or unsafe; refusing replacement of potentially issued authority",
    )
}

fn open_private_key_file(path: &Path) -> std::io::Result<File> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let metadata = file.metadata()?;
    // SAFETY: geteuid has no preconditions and cannot fail.
    let euid = unsafe { libc::geteuid() };
    if !metadata.is_file()
        || metadata.uid() != euid
        || metadata.nlink() != 1
        || metadata.mode() & 0o077 != 0
    {
        return Err(std::io::Error::other("unsafe key file"));
    }
    Ok(file)
}

fn prepare_and_publish(
    root: &Path,
    observe: &mut impl FnMut(PublicationPoint) -> std::io::Result<()>,
) -> std::io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let preparation = root.join(PREPARATION);
    // Under the lock, absent final authority proves this slot never issued a
    // token, so even a complete candidate is discarded. Only this one bounded
    // private regular file is recovered: no scans or wildcard cleanup.
    match fs::symlink_metadata(&preparation) {
        Ok(_) => {
            let file = open_private_key_file(&preparation)?;
            if file.metadata()?.len() > 32 {
                return Err(std::io::Error::other("oversized key preparation"));
            }
            fs::remove_file(&preparation)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let mut bytes = [0u8; 32];
    File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&preparation)?;
    observe(PublicationPoint::Created)?;
    file.write_all(&bytes)?;
    observe(PublicationPoint::Written)?;
    file.sync_all()?;
    observe(PublicationPoint::CandidateSynced)?;
    // Same-directory rename publishes only a complete synced file.
    fs::rename(&preparation, root.join("key"))?;
    observe(PublicationPoint::Published)?;
    Ok(())
}

#[cfg(test)]
pub(super) fn authenticate_for_test(key: &[u8; 32], bytes: &[u8]) -> [u8; 32] {
    authenticate(key, bytes)
}

#[cfg(test)]
pub(super) const PREPARATION_NAME: &str = PREPARATION;
