//! Durable provider-owned directory publication and bounded file reads.
//!
//! Directory creation publishes each missing link in order and synchronizes it
//! and its containing parent. Existing targets synchronize themselves only;
//! their creators own publication of pre-existing ancestor links. A failure may
//! leave visible directories. Existence, a later successful retry or sync is
//! not a receipt for failed earlier publication or proof of host-crash durability.
//! Fresh per-call roots avoid reuse of failed lineage. Deliberate recovery must
//! name its roots/lineage and establish the required guarantee at its consumer;
//! this helper supplies no general recovery machinery or host-created-link receipt.

use sha2::{Digest, Sha256};
use std::fs;
use std::io::{Error, ErrorKind, Read};
use std::path::{Path, PathBuf};

/// Creates missing directories and synchronizes each new directory and the
/// parent containing its link before proceeding. An existing target is synced
/// without syncing its ancestors. See the module's creator/recovery obligations.
pub fn create_directories(path: &Path) -> std::io::Result<()> {
    create_directory_chain(path, false)
}

/// Like [`create_directories`], setting newly created directories and the target
/// to mode 0700 on Unix before synchronizing them. Existing ancestors keep their
/// permissions and remain their creator's publication responsibility.
pub fn create_private_directories(path: &Path) -> std::io::Result<()> {
    create_directory_chain(path, true)
}

pub fn prepare_best_effort_private_directory(path: &Path) -> std::io::Result<()> {
    prepare_best_effort_private_directory_with_sync(path, sync_directory)
}

pub fn is_executable_file(path: &Path) -> std::io::Result<bool> {
    let metadata = fs::metadata(path)?;
    if !metadata.is_file() {
        return Ok(false);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        Ok(metadata.permissions().mode() & 0o111 != 0)
    }
    #[cfg(not(unix))]
    {
        Ok(true)
    }
}

pub fn read_file_bounded(path: &Path, maximum_bytes: usize) -> std::io::Result<Vec<u8>> {
    read_file_bounded_or(path, maximum_bytes, maximum_bytes, |_| false).map(|(bytes, _)| bytes)
}

pub fn sha256_file_bounded(path: &Path, maximum_bytes: usize) -> std::io::Result<(String, usize)> {
    let metadata = fs::metadata(path)?;
    if metadata.len() > maximum_bytes as u64 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!("file exceeds supported {maximum_bytes}-byte bound"),
        ));
    }
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut observed_bytes = 0_usize;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        observed_bytes = observed_bytes.saturating_add(read);
        if observed_bytes > maximum_bytes {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("file exceeds supported {maximum_bytes}-byte bound"),
            ));
        }
        hasher.update(&buffer[..read]);
    }
    let digest = hasher.finalize();
    Ok((
        digest.iter().map(|byte| format!("{byte:02x}")).collect(),
        observed_bytes,
    ))
}

pub fn read_file_bounded_or(
    path: &Path,
    maximum_bytes: usize,
    maximum_oversized_bytes: usize,
    allow_oversized: impl FnOnce(&[u8]) -> bool,
) -> std::io::Result<(Vec<u8>, bool)> {
    if maximum_oversized_bytes < maximum_bytes {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "oversized file bound is smaller than the steady-state bound",
        ));
    }
    let mut file = fs::File::open(path)?;
    let mut bytes = Vec::new();
    (&mut file)
        .take(maximum_bytes.saturating_add(1) as u64)
        .read_to_end(&mut bytes)?;
    let oversized = bytes.len() > maximum_bytes;
    if oversized && !allow_oversized(&bytes) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!("file exceeds supported {maximum_bytes}-byte bound"),
        ));
    }
    if oversized {
        let remaining_with_sentinel = maximum_oversized_bytes
            .saturating_sub(bytes.len())
            .saturating_add(1);
        (&mut file)
            .take(remaining_with_sentinel as u64)
            .read_to_end(&mut bytes)?;
        if bytes.len() > maximum_oversized_bytes {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "file exceeds supported {maximum_oversized_bytes}-byte compatibility bound"
                ),
            ));
        }
    }
    sync_directory(
        path.parent()
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "file has no parent directory"))?,
    )?;
    Ok((bytes, oversized))
}

fn create_directory_chain(path: &Path, private: bool) -> std::io::Result<()> {
    create_directory_chain_with_sync(path, private, sync_directory)
}

fn create_directory_chain_with_sync<F>(
    path: &Path,
    private: bool,
    mut sync: F,
) -> std::io::Result<()>
where
    F: FnMut(&Path) -> std::io::Result<()>,
{
    let mut missing = Vec::<PathBuf>::new();
    let mut ancestor = path;
    loop {
        match fs::metadata(ancestor) {
            Ok(metadata) if metadata.is_dir() => break,
            Ok(_) => {
                return Err(Error::new(
                    ErrorKind::NotADirectory,
                    "directory ancestor is not a directory",
                ));
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {
                missing.push(ancestor.to_path_buf());
                ancestor = ancestor.parent().ok_or_else(|| {
                    Error::new(
                        ErrorKind::InvalidInput,
                        "directory has no existing ancestor",
                    )
                })?;
            }
            Err(error) => return Err(error),
        }
    }
    if missing.is_empty() {
        if private {
            set_private_directory_permissions(path)?;
        }
        return sync(path);
    }
    for directory in missing.into_iter().rev() {
        match fs::create_dir(&directory) {
            Ok(()) => {}
            Err(error)
                if error.kind() == ErrorKind::AlreadyExists
                    && fs::metadata(&directory).is_ok_and(|metadata| metadata.is_dir()) => {}
            Err(error) => return Err(error),
        }
        if private {
            set_private_directory_permissions(&directory)?;
        }
        sync(&directory)?;
        sync(
            directory
                .parent()
                .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "directory has no parent"))?,
        )?;
    }
    Ok(())
}

fn prepare_best_effort_private_directory_with_sync<F>(
    path: &Path,
    mut sync: F,
) -> std::io::Result<()>
where
    F: FnMut(&Path) -> std::io::Result<()>,
{
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => {
            set_private_directory_permissions(path)?;
            sync(path)
        }
        Ok(_) => Err(Error::new(
            ErrorKind::NotADirectory,
            "best-effort evidence root is not a directory",
        )),
        Err(error) if error.kind() == ErrorKind::NotFound => {
            create_directory_chain_with_sync(path, true, sync)
        }
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
/// Syncs this directory, reporting the operation and path on failure. The error
/// keeps its kind; the underlying I/O error (including errno) is in its source.
pub fn sync_directory(path: &Path) -> std::io::Result<()> {
    let directory = fs::File::open(path)
        .map_err(|error| directory_operation_error("open directory for sync", path, error))?;
    directory
        .sync_all()
        .map_err(|error| directory_operation_error("sync directory", path, error))
}

#[cfg(unix)]
#[derive(Debug)]
struct DirectoryOperationError {
    operation: &'static str,
    path: PathBuf,
    cause: Error,
}

#[cfg(unix)]
impl std::fmt::Display for DirectoryOperationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} '{}': {}",
            self.operation,
            self.path.display(),
            self.cause
        )
    }
}

#[cfg(unix)]
impl std::error::Error for DirectoryOperationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

#[cfg(unix)]
fn directory_operation_error(operation: &'static str, path: &Path, cause: Error) -> Error {
    Error::new(
        cause.kind(),
        DirectoryOperationError {
            operation,
            path: path.to_path_buf(),
            cause,
        },
    )
}

#[cfg(not(unix))]
pub fn sync_directory(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_private_directory_permissions(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn set_private_directory_permissions(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_links_sync_their_objects_and_containing_parents_only() {
        let temporary = tempfile::tempdir().unwrap();
        let first = temporary.path().join("first");
        let target = first.join("target");
        let mut syncs = Vec::new();
        create_directory_chain_with_sync(&target, true, |directory| {
            syncs.push(directory.to_path_buf());
            Ok(())
        })
        .unwrap();
        assert_eq!(
            syncs,
            vec![first.clone(), temporary.path().to_path_buf(), target, first]
        );
    }

    #[cfg(target_os = "linux")]
    struct RestoreReadPermission(PathBuf);

    #[cfg(target_os = "linux")]
    impl Drop for RestoreReadPermission {
        fn drop(&mut self) {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&self.0, fs::Permissions::from_mode(0o700)).unwrap();
        }
    }

    #[cfg(target_os = "linux")]
    fn deny_directory_read(path: &Path) -> RestoreReadPermission {
        use std::os::unix::fs::PermissionsExt;
        assert_ne!(
            unsafe { libc::geteuid() },
            0,
            "DAC control requires nonroot"
        );
        fs::set_permissions(path, fs::Permissions::from_mode(0o300)).unwrap();
        assert_eq!(
            fs::File::open(path).unwrap_err().kind(),
            ErrorKind::PermissionDenied
        );
        RestoreReadPermission(path.to_path_buf())
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn unrelated_search_only_ancestor_allows_new_and_existing_targets() {
        use std::os::unix::fs::PermissionsExt;
        let temporary = tempfile::tempdir().unwrap();
        let ancestor = temporary.path().join("administrative");
        let root = ancestor.join("provider");
        fs::create_dir_all(&root).unwrap();
        let _restore = deny_directory_read(&ancestor);
        for (name, prepare) in [
            (
                "public",
                create_directories as fn(&Path) -> std::io::Result<()>,
            ),
            ("private", create_private_directories),
            ("evidence", prepare_best_effort_private_directory),
        ] {
            let target = root.join(name);
            prepare(&target).unwrap();
            prepare(&target).unwrap();
            assert!(target.is_dir());
        }
        assert_eq!(
            fs::metadata(&ancestor).unwrap().permissions().mode() & 0o777,
            0o300
        );
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn unreadable_required_parent_fails_after_link_and_needs_explicit_repair() {
        let temporary = tempfile::tempdir().unwrap();
        let parent = temporary.path().join("parent");
        fs::create_dir(&parent).unwrap();
        let restore = deny_directory_read(&parent);
        let first = parent.join("first");
        let target = first.join("target");
        let error = create_private_directories(&target).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::PermissionDenied);
        assert!(first.is_dir());
        assert!(!target.exists());
        assert!(error.to_string().contains("open directory for sync"));
        assert!(error.to_string().contains(&parent.display().to_string()));
        let detail = error
            .get_ref()
            .unwrap()
            .downcast_ref::<DirectoryOperationError>()
            .unwrap();
        assert_eq!(detail.cause.raw_os_error(), Some(libc::EACCES));
        // Seeing the partial link cannot certify its earlier publication.
        // Its creator repairs the failed containing-parent sync explicitly.
        drop(restore);
        sync_directory(&parent).unwrap();
        create_private_directories(&target).unwrap();
        assert!(target.is_dir());
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn unreadable_existing_target_still_fails_its_own_sync() {
        let temporary = tempfile::tempdir().unwrap();
        let target = temporary.path().join("target");
        fs::create_dir(&target).unwrap();
        let _restore = deny_directory_read(&target);
        let error = create_directories(&target).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::PermissionDenied);
        assert!(error.to_string().contains(&target.display().to_string()));
    }

    #[test]
    fn parent_sync_failure_stops_creation_and_requires_creator_repair_before_retry() {
        let temporary = tempfile::tempdir().expect("create durable directory test root");
        let first_directory = temporary.path().join("first");
        let target = first_directory.join("target");
        let mut failed_parent_sync = false;
        let first = create_directory_chain_with_sync(&target, false, |directory| {
            if directory == temporary.path() && !failed_parent_sync {
                failed_parent_sync = true;
                return Err(Error::other("injected parent sync failure"));
            }
            Ok(())
        });
        assert_eq!(
            first.unwrap_err().to_string(),
            "injected parent sync failure"
        );
        assert!(first_directory.is_dir());
        assert!(!target.exists());

        // Repair the failed publication explicitly; a retry cannot distinguish
        // this partial link from an ancestor published by a different creator.
        sync_directory(temporary.path()).expect("repair the failed containing-parent sync");
        let mut retry_syncs = Vec::new();
        create_directory_chain_with_sync(&target, false, |directory| {
            retry_syncs.push(directory.to_path_buf());
            Ok(())
        })
        .expect("retry after repairing prior publication");

        assert!(target.is_dir());
        assert_eq!(retry_syncs, vec![target, first_directory]);
    }

    #[test]
    fn existing_material_directory_syncs_only_itself_and_propagates_failure() {
        let temporary = tempfile::tempdir().expect("create material directory test root");
        let target = temporary.path().join("existing");
        fs::create_dir(&target).expect("create existing material directory");

        let mut syncs = Vec::new();
        create_directory_chain_with_sync(&target, true, |directory| {
            syncs.push(directory.to_path_buf());
            Ok(())
        })
        .expect("prepare existing material directory");

        assert_eq!(syncs, vec![target.clone()]);
        let error = create_directory_chain_with_sync(&target, false, |_| {
            Err(Error::from_raw_os_error(libc::EIO))
        })
        .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EIO));
    }

    #[test]
    fn best_effort_existing_directory_sync_cost_is_independent_of_lineage_depth() {
        let temporary = tempfile::tempdir().expect("create best-effort directory test root");
        let target = (0..64).fold(temporary.path().to_path_buf(), |path, index| {
            path.join(format!("depth-{index}"))
        });
        fs::create_dir_all(&target).expect("create deep existing directory lineage");

        let mut syncs = Vec::new();
        prepare_best_effort_private_directory_with_sync(&target, |directory| {
            syncs.push(directory.to_path_buf());
            Ok(())
        })
        .expect("prepare existing best-effort directory");

        assert_eq!(syncs, vec![target]);
    }

    #[test]
    fn bounded_read_rejects_oversized_files_without_loading_the_tail() {
        let temporary = tempfile::tempdir().expect("create bounded-read root");
        let path = temporary.path().join("bounded");
        fs::write(&path, vec![b'x'; 33]).expect("write oversized fixture");
        let error = read_file_bounded(&path, 32).expect_err("oversized file must fail");
        assert_eq!(error.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn bounded_streaming_digest_matches_the_in_memory_identity() {
        let temporary = tempfile::tempdir().expect("create bounded-digest root");
        let path = temporary.path().join("executable");
        let bytes = vec![b'x'; 96 * 1024 + 17];
        fs::write(&path, &bytes).expect("write digest fixture");
        let (digest, observed_bytes) =
            sha256_file_bounded(&path, bytes.len()).expect("digest boundary fixture");
        assert_eq!(digest, crate::encoding::sha256_hex(&bytes));
        assert_eq!(observed_bytes, bytes.len());
        assert_eq!(
            sha256_file_bounded(&path, bytes.len() - 1)
                .expect_err("oversized digest input must fail")
                .kind(),
            ErrorKind::InvalidData
        );
    }

    #[test]
    fn compatibility_read_stops_at_its_distinct_upper_bound() {
        let temporary = tempfile::tempdir().expect("create compatibility-read root");
        let path = temporary.path().join("bounded-compatibility");
        fs::write(&path, vec![b'x'; 65]).expect("write oversized compatibility fixture");
        let error = read_file_bounded_or(&path, 32, 64, |_| true)
            .expect_err("compatibility read above its upper bound must fail");
        assert_eq!(error.kind(), ErrorKind::InvalidData);

        fs::write(&path, vec![b'x'; 64]).expect("write boundary compatibility fixture");
        let (bytes, compatibility) = read_file_bounded_or(&path, 32, 64, |_| true)
            .expect("compatibility read at the upper bound");
        assert_eq!(bytes.len(), 64);
        assert!(compatibility);
    }
}
