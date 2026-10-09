//! Canonical staging: one admitted, durable pool of retained record prefixes
//! and cursor packs under one scope directory.
//!
//! The scope directory's inode is the cross-process lock; aliases to the same
//! directory share it and nothing ever unlinks it. Admission is recomputed from
//! the filesystem under that lock, so interrupted temporary writes and
//! published-but-unreferenced objects stay charged rather than collected.
use super::{io_error, stale, Cursor, Span, MAX_RECORD_BYTES};
use crate::encoding::sha256_hex;
use agent_provider_contract::generated::{ErrorCategory, ErrorObject};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const MAX_CURSOR_BYTES: usize = 32768;
// Digest, space, cursor JSON and newline.
const MAX_FRAME_BYTES: usize = 64 + 1 + MAX_CURSOR_BYTES + 1;

/// Byte and object ceilings of one canonical staging pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StagingLimits {
    pub bytes: u64,
    pub objects: u64,
}

impl StagingLimits {
    pub const DEFAULT: Self = Self {
        bytes: 512 * 1024 * 1024,
        objects: 512 * 1024 * 1024 / 4096,
    };
}

fn storage_limit() -> ErrorObject {
    super::error(
        ErrorCategory::Unsupported,
        "session_turn_staging_capacity_exceeded",
        "Paging staging capacity exhausted; checkpoint retained; operator intervention required",
    )
}

/// Conservative allocation quantum plus a per-inode allowance: logical content
/// and metadata both consume budget, so small files cannot evade accounting.
pub(super) fn charged_bytes(len: u64) -> u64 {
    (len.saturating_add(4095) / 4096 * 4096).saturating_add(4096)
}

/// Holds the scope lock. While held, it numerically reserves the one pending
/// write before allocation; no other cooperating writer can admit until
/// publication or failure releases it.
pub(super) struct Admission {
    _lock: File,
    pub(super) bytes: u64,
    pub(super) objects: u64,
}

impl Admission {
    pub(super) fn acquire(root: &Path) -> Result<Self, ErrorObject> {
        let lock = File::open(root).map_err(|_| io_error())?;
        fs2::FileExt::lock_exclusive(&lock).map_err(|_| io_error())?;
        Ok(Self {
            _lock: lock,
            bytes: 0,
            objects: 0,
        })
    }

    pub(super) fn reserve(
        &mut self,
        root: &Path,
        bytes: usize,
        limits: StagingLimits,
    ) -> Result<(), ErrorObject> {
        self.reserve_growth(root, charged_bytes(bytes as u64), 1, limits)
    }

    pub(super) fn reserve_growth(
        &mut self,
        root: &Path,
        bytes: u64,
        objects: u64,
        limits: StagingLimits,
    ) -> Result<(), ErrorObject> {
        let (mut retained_bytes, mut retained_objects) = (0u64, 0u64);
        for entry in std::fs::read_dir(root).map_err(|_| io_error())? {
            let entry = entry.map_err(|_| io_error())?;
            let metadata = std::fs::symlink_metadata(entry.path()).map_err(|_| io_error())?;
            if !metadata.is_file() {
                return Err(storage_limit());
            }
            retained_bytes = retained_bytes.saturating_add(charged_bytes(metadata.len()));
            retained_objects = retained_objects.saturating_add(1);
            if retained_bytes > limits.bytes || retained_objects > limits.objects {
                return Err(storage_limit());
            }
        }
        self.bytes = retained_bytes.saturating_add(bytes);
        self.objects = retained_objects.saturating_add(objects);
        if self.bytes > limits.bytes || self.objects > limits.objects {
            return Err(storage_limit());
        }
        Ok(())
    }
}

fn partial_path(root: &Path, digest: &str) -> PathBuf {
    root.join(format!("record-{digest}.part"))
}

/// Publishes an unfinished record prefix once, by content digest. An existing
/// identical prefix is reused before reserving, even at or above capacity.
pub(super) fn stage_partial(
    root: &Path,
    admission: &mut Admission,
    limits: StagingLimits,
    start: u64,
    bytes: &[u8],
) -> Result<Span, ErrorObject> {
    let sha256 = sha256_hex(bytes);
    let path = partial_path(root, &sha256);
    if path.exists() {
        let existing =
            crate::durable_fs::read_file_bounded(&path, bytes.len()).map_err(|_| stale())?;
        if existing != bytes {
            return Err(stale());
        }
        return Ok(Span { start, sha256 });
    }
    admission.reserve(root, bytes.len(), limits)?;
    let mut temp = tempfile::NamedTempFile::new_in(root).map_err(|_| io_error())?;
    temp.write_all(bytes).map_err(|_| io_error())?;
    temp.as_file().sync_all().map_err(|_| io_error())?;
    // Atomic rename keeps one reserved object throughout publication,
    // including interruption. The scope lock excludes competing writers.
    temp.persist(&path).map_err(|_| io_error())?;
    crate::durable_fs::sync_directory(root).map_err(|_| io_error())?;
    Ok(Span { start, sha256 })
}

pub(super) fn partial_bytes(root: &Path, span: &Span, len: u64) -> Result<Vec<u8>, ErrorObject> {
    if len >= MAX_RECORD_BYTES as u64 {
        return Err(stale());
    }
    crate::durable_fs::read_file_bounded(&partial_path(root, &span.sha256), len as usize)
        .map_err(|_| stale())
}

fn serialize(cursor: &Cursor) -> Result<Vec<u8>, ErrorObject> {
    let bytes = serde_json::to_vec(cursor).map_err(|_| io_error())?;
    if bytes.len() > MAX_CURSOR_BYTES || bytes.contains(&b'\n') {
        return Err(io_error());
    }
    Ok(bytes)
}

pub(super) fn token(prefix: &str, cursor: &Cursor) -> Result<String, ErrorObject> {
    Ok(format!("{prefix}{}", sha256_hex(&serialize(cursor)?)))
}

pub(super) fn load(root: &Path, prefix: &str, value: &str) -> Result<Cursor, ErrorObject> {
    let digest = value
        .strip_prefix(prefix)
        .filter(|digest| super::is_digest(digest))
        .ok_or_else(stale)?;
    let bytes = scan_pack(&pack_path(root, digest), digest)
        .map_err(|_| stale())?
        .0
        .ok_or_else(stale)?;
    if sha256_hex(&bytes) != digest {
        return Err(stale());
    }
    serde_json::from_slice(&bytes).map_err(|_| stale())
}

/// Appends the cursor's frame to its bucket pack unless already present.
/// Only an incomplete final frame is reclaimable: no issued token can name it.
/// Complete frames, referenced or not, never expire.
pub(super) fn persist(
    root: &Path,
    admission: &mut Admission,
    limits: StagingLimits,
    cursor: &Cursor,
) -> Result<(), ErrorObject> {
    let bytes = serialize(cursor)?;
    let digest = sha256_hex(&bytes);
    let path = pack_path(root, &digest);
    let (existing, valid_end, physical_len) = scan_pack(&path, &digest).map_err(|_| io_error())?;
    if let Some(existing) = existing {
        if existing != bytes {
            return Err(stale());
        }
        // A writer may have died after completing the frame but before
        // syncing it; dedup completes publication before returning.
        return sync_pack(&path, root).map_err(|_| io_error());
    }
    let mut frame = Vec::with_capacity(bytes.len() + 66);
    frame.extend_from_slice(digest.as_bytes());
    frame.push(b' ');
    frame.extend_from_slice(&bytes);
    frame.push(b'\n');
    let exists = path.exists();
    let old_charge = if exists {
        charged_bytes(physical_len)
    } else {
        0
    };
    let new_charge = charged_bytes(valid_end + frame.len() as u64);
    admission.reserve_growth(
        root,
        new_charge.saturating_sub(old_charge),
        u64::from(!exists),
        limits,
    )?;
    let mut options = std::fs::OpenOptions::new();
    options.create(true).read(true).write(true);
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&path).map_err(|_| io_error())?;
    if physical_len != valid_end {
        file.set_len(valid_end).map_err(|_| io_error())?;
        file.sync_all().map_err(|_| io_error())?;
    }
    file.seek(SeekFrom::Start(valid_end))
        .map_err(|_| io_error())?;
    file.write_all(&frame).map_err(|_| io_error())?;
    sync_pack(&path, root).map_err(|_| io_error())
}

/// Fixed hash buckets bound cursor inode growth to 256 without a mutable index.
pub(super) fn pack_path(root: &Path, digest: &str) -> PathBuf {
    root.join(format!("cursors-{}.pack", &digest[..2]))
}

fn sync_pack(path: &Path, root: &Path) -> std::io::Result<()> {
    File::open(path)?.sync_all()?;
    crate::durable_fs::sync_directory(root)
}

/// Returns the matching frame's content, the end of complete verified frames,
/// and the physical length. An interrupted append has no newline; streaming
/// never loads a whole pack.
pub(super) fn scan_pack(path: &Path, digest: &str) -> std::io::Result<(Option<Vec<u8>>, u64, u64)> {
    use std::io::{Error, ErrorKind};
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok((None, 0, 0)),
        Err(e) => return Err(e),
    };
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > StagingLimits::DEFAULT.bytes {
        return Err(Error::other("invalid cursor pack"));
    }
    let mut reader = BufReader::new(file);
    let mut end = 0;
    let mut found = None;
    loop {
        let mut frame = Vec::new();
        (&mut reader)
            .take(MAX_FRAME_BYTES as u64 + 1)
            .read_until(b'\n', &mut frame)?;
        if frame.is_empty() {
            break;
        }
        if frame.len() > MAX_FRAME_BYTES {
            return Err(Error::other("oversized cursor frame"));
        }
        if frame.last() != Some(&b'\n') {
            break;
        }
        if frame.len() < 67 || frame[64] != b' ' {
            return Err(Error::other("invalid cursor frame"));
        }
        let bytes = &frame[65..frame.len() - 1];
        let hash = sha256_hex(bytes);
        if hash.as_bytes() != &frame[..64] {
            return Err(Error::other("corrupt cursor frame"));
        }
        if hash == digest {
            found = Some(bytes.to_vec());
        }
        end += frame.len() as u64;
    }
    Ok((found, end, metadata.len()))
}
