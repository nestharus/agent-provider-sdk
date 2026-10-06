//! Request custody for one-shot launches.
//!
//! A provider keys each launch by provider instance and request ID, holds an
//! exclusive per-request lock while it runs, and records a durable phase:
//! `prepared` before spawn, `running` with the native process-group actor once
//! spawned, and `complete` with the exit code and the sealed journal's length
//! and SHA-256. A retry with identical inputs replays a complete journal
//! byte-for-byte after verifying it against the durable receipt; a retry after
//! an interrupted launch first discharges the recorded actor and then requires
//! reconciliation rather than starting another native turn. Which inputs form
//! the request digest, and how outcomes map to provider failures, remain
//! provider decisions.

use crate::encoding::sha256_hex;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Upper bound for a durable launch state record.
pub const LAUNCH_STATE_MAX_BYTES: usize = 64 * 1024;
pub const PHASE_PREPARED: &str = "prepared";
pub const PHASE_RUNNING: &str = "running";
pub const PHASE_COMPLETE: &str = "complete";

#[derive(Debug)]
pub enum CustodyError {
    /// Another holder owns the exclusive lock.
    Busy,
    /// The durable state record cannot be decoded.
    InvalidState,
    /// The state record could not be serialized to its temporary file.
    StateWrite(String),
    /// A complete state record names a journal that does not exist.
    JournalMissing,
    /// The journal's length or digest differs from its durable receipt.
    JournalMismatch,
    /// The journal byte count overflowed.
    JournalOverflow,
    Io(io::Error),
}

impl fmt::Display for CustodyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Busy => formatter.write_str("custody lock is held"),
            Self::InvalidState => formatter.write_str("invalid launch state"),
            Self::StateWrite(message) => write!(formatter, "launch state write failed: {message}"),
            Self::JournalMissing => formatter.write_str("completed launch journal is missing"),
            Self::JournalMismatch => {
                formatter.write_str("completed launch journal does not match its durable receipt")
            }
            Self::JournalOverflow => formatter.write_str("launch journal byte count overflowed"),
            Self::Io(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for CustodyError {}

impl From<io::Error> for CustodyError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Durable launch state record. Field names are the on-disk format.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct LaunchState {
    pub digest: String,
    pub phase: String,
    pub actor_id: Option<u32>,
    pub incarnation: Option<String>,
    pub exit_code: Option<i32>,
    pub journal_sha256: Option<String>,
    pub journal_len: Option<u64>,
}

impl LaunchState {
    pub fn prepared(digest: String) -> Self {
        Self {
            digest,
            phase: PHASE_PREPARED.into(),
            ..Self::default()
        }
    }

    pub fn is_complete(&self) -> bool {
        self.phase == PHASE_COMPLETE
    }
}

/// Stable custody key for one provider instance and request ID. An absent
/// instance is keyed as JSON `null`, distinct from any instance string.
pub fn request_key(provider_instance_id: Option<&str>, request_id: &str) -> String {
    sha256_hex(&serde_json::to_vec(&json!([provider_instance_id, request_id])).unwrap())
}

/// Opens (creating if needed) and exclusively locks `path` without blocking.
/// Any lock failure reports [`CustodyError::Busy`].
pub fn try_lock_exclusive(path: &Path) -> Result<File, CustodyError> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    file.try_lock_exclusive().map_err(|_| CustodyError::Busy)?;
    Ok(file)
}

/// Exclusive custody of one request's durable state and journal under a
/// provider-chosen state root. The lock is released when this value drops.
pub struct RequestCustody {
    _lock: File,
    root: PathBuf,
    key: String,
}

impl RequestCustody {
    /// Locks `<root>/<key>.lock`. The root must already exist.
    pub fn acquire(root: &Path, key: &str) -> Result<Self, CustodyError> {
        let lock = try_lock_exclusive(&root.join(format!("{key}.lock")))?;
        Ok(Self {
            _lock: lock,
            root: root.to_path_buf(),
            key: key.to_string(),
        })
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    /// Path of a provider-owned sibling file, `<root>/<key>.<extension>`.
    pub fn sibling(&self, extension: &str) -> PathBuf {
        self.root.join(format!("{}.{extension}", self.key))
    }

    pub fn state_path(&self) -> PathBuf {
        self.sibling("json")
    }

    pub fn journal_path(&self) -> PathBuf {
        self.sibling("jsonl")
    }

    /// Reads the prior state record, if one was published.
    pub fn load_state(&self) -> Result<Option<LaunchState>, CustodyError> {
        let path = self.state_path();
        if !path.is_file() {
            return Ok(None);
        }
        let bytes = crate::durable_fs::read_file_bounded(&path, LAUNCH_STATE_MAX_BYTES)?;
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|_| CustodyError::InvalidState)
    }

    /// Atomically replaces the state record and synchronizes its directory.
    pub fn write_state(&self, state: &LaunchState) -> Result<(), CustodyError> {
        let path = self.state_path();
        let mut temporary = tempfile::NamedTempFile::new_in(&self.root)?;
        serde_json::to_writer(&mut temporary, state)
            .map_err(|error| CustodyError::StateWrite(error.to_string()))?;
        temporary.as_file().sync_all()?;
        temporary.persist(&path).map_err(|error| error.error)?;
        File::open(&self.root).and_then(|root| root.sync_all())?;
        Ok(())
    }

    /// Creates the journal; an existing journal is an error.
    pub fn create_journal(&self) -> io::Result<Journal> {
        Journal::create_new(&self.journal_path())
    }

    /// Verifies a complete journal against `state` and copies it to `writer`.
    pub fn replay<W: Write>(
        &self,
        state: &LaunchState,
        writer: &mut W,
    ) -> Result<(), CustodyError> {
        replay_journal(&self.journal_path(), state, writer)
    }
}

/// Copies a journal to `writer` only after its length and SHA-256 match the
/// durable receipt in `state`.
pub fn replay_journal<W: Write>(
    path: &Path,
    state: &LaunchState,
    writer: &mut W,
) -> Result<(), CustodyError> {
    let mut file = File::open(path).map_err(|_| CustodyError::JournalMissing)?;
    if Some(file.metadata()?.len()) != state.journal_len {
        return Err(CustodyError::JournalMismatch);
    }
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    if state.journal_sha256.as_deref() != Some(format!("{:x}", hash.finalize()).as_str()) {
        return Err(CustodyError::JournalMismatch);
    }
    file.seek(SeekFrom::Start(0))?;
    io::copy(&mut file, writer)?;
    writer.flush()?;
    Ok(())
}

/// Append-only launch journal that tracks its length and SHA-256.
pub struct Journal {
    file: File,
    sha256: Sha256,
    len: u64,
}

/// Length and digest of a sealed journal, recorded in the complete state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalReceipt {
    pub sha256: String,
    pub len: u64,
}

impl Journal {
    pub fn create_new(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().create_new(true).write(true).open(path)?;
        Ok(Self {
            file,
            sha256: Sha256::new(),
            len: 0,
        })
    }

    /// Appends `bytes`. The length is checked before anything is written.
    pub fn append(&mut self, bytes: &[u8]) -> Result<(), CustodyError> {
        self.len = self
            .len
            .checked_add(bytes.len() as u64)
            .ok_or(CustodyError::JournalOverflow)?;
        self.file.write_all(bytes)?;
        self.sha256.update(bytes);
        Ok(())
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Synchronizes the journal and returns its receipt.
    pub fn seal(&mut self) -> io::Result<JournalReceipt> {
        self.file.sync_all()?;
        Ok(JournalReceipt {
            sha256: format!("{:x}", self.sha256.clone().finalize()),
            len: self.len,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn complete(root: &Path, key: &str, bytes: &[u8]) -> (RequestCustody, LaunchState) {
        let custody = RequestCustody::acquire(root, key).unwrap();
        let mut journal = custody.create_journal().unwrap();
        journal.append(bytes).unwrap();
        let receipt = journal.seal().unwrap();
        let state = LaunchState {
            phase: PHASE_COMPLETE.into(),
            exit_code: Some(0),
            journal_sha256: Some(receipt.sha256),
            journal_len: Some(receipt.len),
            ..LaunchState::prepared("digest".into())
        };
        custody.write_state(&state).unwrap();
        (custody, state)
    }

    #[test]
    fn state_record_keeps_its_durable_field_names() {
        let state = LaunchState {
            actor_id: Some(7),
            incarnation: Some("linux:boot:1".into()),
            ..LaunchState::prepared("abc".into())
        };
        assert_eq!(
            serde_json::to_value(&state).unwrap(),
            json!({"digest":"abc","phase":"prepared","actor_id":7,
                "incarnation":"linux:boot:1","exit_code":null,
                "journal_sha256":null,"journal_len":null})
        );
    }

    #[test]
    fn request_key_is_the_digest_of_instance_and_request() {
        assert_eq!(
            request_key(Some("instance"), "request"),
            sha256_hex(br#"["instance","request"]"#)
        );
        assert_eq!(
            request_key(None, "request"),
            sha256_hex(br#"[null,"request"]"#)
        );
        assert_ne!(
            request_key(None, "request"),
            request_key(Some(""), "request")
        );
    }

    #[test]
    fn a_second_holder_is_busy_until_the_first_releases() {
        let root = tempfile::tempdir().unwrap();
        let first = RequestCustody::acquire(root.path(), "key").unwrap();
        assert!(matches!(
            RequestCustody::acquire(root.path(), "key"),
            Err(CustodyError::Busy)
        ));
        drop(first);
        RequestCustody::acquire(root.path(), "key").unwrap();
    }

    #[test]
    fn complete_journal_replays_exact_bytes() {
        let root = tempfile::tempdir().unwrap();
        let bytes = b"{\"seq\":1}\n{\"seq\":2}\n";
        let (custody, _) = complete(root.path(), "key", bytes);
        let state = custody.load_state().unwrap().expect("published state");
        assert!(state.is_complete());
        let mut replayed = Vec::new();
        custody.replay(&state, &mut replayed).unwrap();
        assert_eq!(replayed, bytes);
    }

    #[test]
    fn replay_refuses_a_journal_that_differs_from_its_receipt() {
        let root = tempfile::tempdir().unwrap();
        let (custody, state) = complete(root.path(), "key", b"original\n");
        std::fs::write(custody.journal_path(), b"tampered\n").unwrap();
        let mut replayed = Vec::new();
        assert!(matches!(
            custody.replay(&state, &mut replayed),
            Err(CustodyError::JournalMismatch)
        ));
        std::fs::write(custody.journal_path(), b"longer than before\n").unwrap();
        assert!(matches!(
            custody.replay(&state, &mut replayed),
            Err(CustodyError::JournalMismatch)
        ));
        std::fs::remove_file(custody.journal_path()).unwrap();
        assert!(matches!(
            custody.replay(&state, &mut replayed),
            Err(CustodyError::JournalMissing)
        ));
        assert!(replayed.is_empty(), "no unverified byte is replayed");
    }

    #[test]
    fn undecodable_state_is_invalid_rather_than_absent() {
        let root = tempfile::tempdir().unwrap();
        let custody = RequestCustody::acquire(root.path(), "key").unwrap();
        assert!(custody.load_state().unwrap().is_none());
        std::fs::write(custody.state_path(), b"not json").unwrap();
        assert!(matches!(
            custody.load_state(),
            Err(CustodyError::InvalidState)
        ));
    }

    #[test]
    fn journal_is_never_reopened_over_existing_custody() {
        let root = tempfile::tempdir().unwrap();
        let (custody, _) = complete(root.path(), "key", b"x\n");
        assert_eq!(
            custody.create_journal().err().map(|error| error.kind()),
            Some(io::ErrorKind::AlreadyExists)
        );
    }
}
