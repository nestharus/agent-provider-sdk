//! Staging admission, cursor-pack publication and observation-key publication
//! controls. Subprocess fixtures re-execute this test binary and exit at a
//! production boundary without destructors, leaving real interrupted state.
use super::observation::{self, PublicationPoint};
use super::staging::{self, charged_bytes, pack_path, Admission, StagingLimits};
use super::*;
use std::fs;
use std::io::Write;

const PREFIX: &str = "unit-stp1-";

fn limits(bytes: u64, objects: u64) -> StagingLimits {
    StagingLimits { bytes, objects }
}

fn files(root: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let mut out = Vec::new();
    if root.exists() {
        for entry in fs::read_dir(root).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                out.extend(files(&path));
            } else {
                out.push((path.clone(), fs::read(path).unwrap()));
            }
        }
    }
    out.sort();
    out
}

fn seed_cursor() -> Cursor {
    Cursor {
        kind: "resume".into(),
        binding: Binding {
            provider: "p".into(),
            account: "/a".into(),
            settings: "s".into(),
            session: "s".into(),
            projection: "canonical_ingest".into(),
            nonce: None,
        },
        budgets: Budgets {
            turns: 8,
            response: 4096,
            source: 512,
            inline: 100,
        },
        stamp: Stamp {
            device: 1,
            inode: 2,
            len: 1000,
            modified: 1,
        },
        head: "h".repeat(64),
        snapshot: "s".repeat(64),
        offset: 1000,
        page: 0,
        sequence: 0,
        partial_record: None,
        anchor: None,
    }
}

fn digest(cursor: &Cursor) -> String {
    staging::token(PREFIX, cursor).unwrap()[PREFIX.len()..].to_owned()
}

fn round_trip(root: &Path, cursor: &Cursor) -> String {
    let token = staging::token(PREFIX, cursor).unwrap();
    staging::token(PREFIX, &staging::load(root, PREFIX, &token).unwrap()).unwrap()
}

#[test]
fn numeric_reservation_orphan_charging_dedup_and_boundaries() {
    let root = tempfile::tempdir().unwrap();
    let root = root.path();
    let mut guard = Admission::acquire(root).unwrap();
    guard.reserve(root, 6, limits(16384, 2)).unwrap();
    assert_eq!((guard.bytes, guard.objects), (8192, 1));
    // A reservation dies with its writer; the retained orphan is charged.
    fs::write(root.join("interrupted-temp"), b"1234").unwrap();
    drop(guard);
    let mut guard = Admission::acquire(root).unwrap();
    let first = staging::stage_partial(root, &mut guard, limits(16384, 2), 0, b"abcdef").unwrap();
    assert_eq!((guard.bytes, guard.objects), (16384, 2));
    let before = files(root);
    staging::stage_partial(root, &mut guard, limits(16384, 2), 0, b"abcdef").unwrap();
    assert_eq!(files(root), before);
    assert_eq!(
        staging::stage_partial(root, &mut guard, limits(16384, 2), 0, b"z")
            .unwrap_err()
            .code,
        "session_turn_staging_capacity_exceeded"
    );
    // Dedup still works above the limit.
    assert_eq!(
        staging::stage_partial(root, &mut guard, limits(1, 1), 0, b"abcdef")
            .unwrap()
            .sha256,
        first.sha256
    );
    assert!(staging::stage_partial(root, &mut guard, limits(16383, 9), 0, b"y").is_err());
    assert!(staging::stage_partial(root, &mut guard, limits(100000, 2), 0, b"y").is_err());
    assert_eq!(files(root), before);
}

#[test]
fn concurrent_independent_writers_cannot_double_admit() {
    let root = tempfile::tempdir().unwrap();
    let barrier = std::sync::Barrier::new(8);
    let results: Vec<_> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..8u8)
            .map(|i| {
                let (root, barrier) = (root.path(), &barrier);
                scope.spawn(move || {
                    barrier.wait();
                    let mut guard = Admission::acquire(root).unwrap();
                    staging::stage_partial(root, &mut guard, limits(16384, 2), 0, &[b'a' + i; 6])
                        .map(|_| ())
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 2);
    assert!(results
        .iter()
        .filter_map(|r| r.as_ref().err())
        .all(|e| e.code == "session_turn_staging_capacity_exceeded"));
    let retained = files(root.path());
    assert_eq!(retained.len(), 2);
    assert_eq!(retained.iter().map(|(_, b)| b.len()).sum::<usize>(), 12);
}

#[test]
#[ignore = "subprocess fixture only; the parent supplies an isolated root"]
fn abrupt_writer_fixture() {
    let root = PathBuf::from(std::env::var("PAGES_ORPHAN_ROOT").unwrap());
    let mut guard = Admission::acquire(&root).unwrap();
    guard.reserve(&root, 6, limits(16384, 2)).unwrap();
    let mut temp = tempfile::NamedTempFile::new_in(&root).unwrap();
    temp.write_all(b"abc").unwrap();
    temp.as_file().sync_all().unwrap();
    // Process exit leaves a real partial temporary and releases the lock.
    std::process::exit(0);
}

#[test]
fn process_exit_leaves_a_charged_orphan_and_releases_the_reservation() {
    let root = tempfile::tempdir().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "session_pages::tests::abrupt_writer_fixture",
            "--ignored",
        ])
        .env("PAGES_ORPHAN_ROOT", root.path())
        .status()
        .unwrap();
    assert!(status.success());
    let orphan = files(root.path());
    assert_eq!(orphan.len(), 1);
    assert_eq!(orphan[0].1, b"abc");
    let mut guard = Admission::acquire(root.path()).unwrap();
    staging::stage_partial(root.path(), &mut guard, limits(16384, 2), 0, b"123456").unwrap();
    assert_eq!((guard.bytes, guard.objects), (16384, 2));
    assert!(staging::stage_partial(root.path(), &mut guard, limits(16384, 2), 0, b"x").is_err());
    assert!(files(root.path()).contains(&orphan[0]));
}

fn same_bucket_pair() -> (Cursor, Cursor) {
    let first = seed_cursor();
    let bucket = digest(&first)[..2].to_owned();
    for i in 1..10000 {
        let mut second = first.clone();
        second.sequence = i;
        if digest(&second)[..2] == bucket {
            return (first, second);
        }
    }
    panic!("synthetic same-bucket search exhausted");
}

#[test]
fn interrupted_pack_append_at_every_byte_boundary_preserves_issued_frames() {
    let root = tempfile::tempdir().unwrap();
    let root = root.path();
    let mut guard = Admission::acquire(root).unwrap();
    let (first, second) = same_bucket_pair();
    staging::persist(root, &mut guard, StagingLimits::DEFAULT, &first).unwrap();
    let path = pack_path(root, &digest(&first));
    let published = fs::read(&path).unwrap();
    let frame = format!(
        "{} {}\n",
        digest(&second),
        serde_json::to_string(&second).unwrap()
    );
    for split in 0..=frame.len() {
        let mut bytes = published.clone();
        bytes.extend_from_slice(&frame.as_bytes()[..split]);
        fs::write(&path, bytes).unwrap();
        assert_eq!(
            round_trip(root, &first),
            staging::token(PREFIX, &first).unwrap()
        );
        staging::persist(root, &mut guard, StagingLimits::DEFAULT, &second).unwrap();
        assert_eq!(
            round_trip(root, &second),
            staging::token(PREFIX, &second).unwrap()
        );
        let complete = fs::read(&path).unwrap();
        assert_eq!(complete, [published.as_slice(), frame.as_bytes()].concat());
        // Dedup of a published frame needs no admission.
        staging::persist(root, &mut guard, limits(0, 0), &second).unwrap();
        assert_eq!(fs::read(&path).unwrap(), complete);
    }
    let mut corrupted = published.clone();
    corrupted[65] ^= 1;
    fs::write(&path, &corrupted).unwrap();
    assert!(staging::persist(root, &mut guard, StagingLimits::DEFAULT, &second).is_err());
    assert!(staging::load(root, PREFIX, &staging::token(PREFIX, &first).unwrap()).is_err());
    assert_eq!(fs::read(&path).unwrap(), corrupted);
}

#[test]
fn pack_growth_is_charged_and_new_packs_respect_the_object_cap() {
    let root = tempfile::tempdir().unwrap();
    let root = root.path();
    let mut guard = Admission::acquire(root).unwrap();
    let (first, second) = same_bucket_pair();
    staging::persist(root, &mut guard, limits(8192, 1), &first).unwrap();
    staging::persist(root, &mut guard, limits(8192, 1), &second).unwrap();
    let retained = files(root);
    assert_eq!(retained.len(), 1);
    let path = pack_path(root, &digest(&first));
    let before = fs::metadata(&path).unwrap().len();
    assert_eq!(charged_bytes(before), 8192);
    let mut enlarged = second.clone();
    enlarged.snapshot = "x".repeat(5000);
    let enlarged = (1..10000)
        .find_map(|sequence| {
            enlarged.sequence = sequence;
            (pack_path(root, &digest(&enlarged)) == path).then(|| enlarged.clone())
        })
        .unwrap();
    let after = before + 66 + serde_json::to_vec(&enlarged).unwrap().len() as u64;
    let error = staging::persist(root, &mut guard, limits(8192, 1), &enlarged).unwrap_err();
    assert_eq!(error.code, "session_turn_staging_capacity_exceeded");
    assert!(!error.retryable);
    assert_eq!(files(root), retained);
    staging::persist(root, &mut guard, limits(charged_bytes(after), 1), &enlarged).unwrap();
    assert_eq!(fs::metadata(&path).unwrap().len(), after);

    let other = (1..10000)
        .find_map(|sequence| {
            let mut cursor = first.clone();
            cursor.sequence = sequence;
            (pack_path(root, &digest(&cursor)) != path).then_some(cursor)
        })
        .unwrap();
    let retained = files(root);
    let error = staging::persist(root, &mut guard, limits(1 << 20, 1), &other).unwrap_err();
    assert_eq!(error.code, "session_turn_staging_capacity_exceeded");
    assert_eq!(files(root), retained);
    // A non-file object in the scope is never admitted around.
    fs::create_dir(root.join("unexpected-directory")).unwrap();
    assert!(staging::persist(root, &mut guard, StagingLimits::DEFAULT, &other).is_err());
    fs::remove_dir(root.join("unexpected-directory")).unwrap();
    staging::persist(root, &mut guard, limits(1 << 20, 2), &other).unwrap();
    for cursor in [&first, &second, &enlarged, &other] {
        assert_eq!(
            round_trip(root, cursor),
            staging::token(PREFIX, cursor).unwrap()
        );
    }
}

#[test]
fn sparse_retained_exhaustion_refuses_new_frames_without_mutation() {
    let root = tempfile::tempdir().unwrap();
    let root = root.path();
    let mut guard = Admission::acquire(root).unwrap();
    let first = seed_cursor();
    staging::persist(root, &mut guard, StagingLimits::DEFAULT, &first).unwrap();
    let orphan = File::create(root.join("retained-orphan")).unwrap();
    orphan.set_len(StagingLimits::DEFAULT.bytes).unwrap();
    let mut second = first.clone();
    second.sequence = 123;
    let error = staging::persist(root, &mut guard, StagingLimits::DEFAULT, &second).unwrap_err();
    assert_eq!(error.code, "session_turn_staging_capacity_exceeded");
    staging::persist(root, &mut guard, StagingLimits::DEFAULT, &first).unwrap();
    assert_eq!(fs::read_dir(root).unwrap().count(), 2);
}

#[test]
#[ignore = "subprocess fixture only; the parent supplies an isolated root"]
fn pack_writer_fixture() {
    let root = PathBuf::from(std::env::var("PAGES_PACK_ROOT").unwrap());
    for i in 0..32 {
        let mut cursor = seed_cursor();
        cursor.sequence = i;
        let mut guard = Admission::acquire(&root).unwrap();
        staging::persist(&root, &mut guard, StagingLimits::DEFAULT, &cursor).unwrap();
        assert_eq!(
            round_trip(&root, &cursor),
            staging::token(PREFIX, &cursor).unwrap()
        );
    }
}

#[test]
fn independent_processes_share_the_scope_lock_through_an_alias() {
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("store");
    fs::create_dir(&directory).unwrap();
    let alias = root.path().join("alias");
    std::os::unix::fs::symlink(&directory, &alias).unwrap();
    let mut children: Vec<_> = (0..4)
        .map(|i| {
            std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "session_pages::tests::pack_writer_fixture",
                    "--ignored",
                ])
                .env(
                    "PAGES_PACK_ROOT",
                    if i % 2 == 0 { &directory } else { &alias },
                )
                .spawn()
                .unwrap()
        })
        .collect();
    for child in &mut children {
        assert!(child.wait().unwrap().success());
    }
    let frames: usize = files(&directory)
        .iter()
        .map(|(_, bytes)| bytes.iter().filter(|b| **b == b'\n').count())
        .sum();
    assert_eq!(frames, 32);
}

fn authority(root: &Path) -> PathBuf {
    root.join("observation-auth-v1")
}

fn paging(root: &Path) -> PathBuf {
    root.join("session-pages-v1")
}

fn key_child(root: &Path, mode: &str) -> std::process::Command {
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "session_pages::tests::key_publication_fixture",
            "--ignored",
        ])
        .env("PAGES_KEY_ROOT", root)
        .env("PAGES_KEY_MODE", mode);
    command
}

#[test]
#[ignore = "subprocess fixture only; the parent supplies an isolated root"]
fn key_publication_fixture() {
    let root = PathBuf::from(std::env::var_os("PAGES_KEY_ROOT").unwrap());
    let mode = std::env::var("PAGES_KEY_MODE").unwrap();
    if mode == "concurrent" {
        let id = std::env::var("PAGES_KEY_ID").unwrap();
        fs::write(root.join(format!("ready-{id}")), b"").unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !root.join("go").exists() {
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let bytes = observation::key(&paging(&root), false).unwrap();
        fs::write(root.join(format!("result-{id}")), sha256_hex(&bytes)).unwrap();
        return;
    }
    let result = observation::key_with_observer(&paging(&root), false, |point| {
        if mode == "partial" && point == PublicationPoint::Created {
            fs::OpenOptions::new()
                .write(true)
                .open(authority(&root).join(observation::PREPARATION_NAME))
                .unwrap()
                .write_all(&[1u8; 16])
                .unwrap();
            std::process::exit(86);
        }
        if format!("{point:?}") == mode {
            std::process::exit(86);
        }
        Ok(())
    });
    panic!("interruption boundary was not reached: {result:?}");
}

#[test]
fn interrupted_first_key_publication_recovers_without_changing_published_identity() {
    for (boundary, published, candidate_len) in [
        ("Created", false, 0),
        ("partial", false, 16),
        ("Written", false, 32),
        ("CandidateSynced", false, 32),
        ("Published", true, 0),
        ("FinalSynced", true, 0),
        ("DirectorySynced", true, 0),
    ] {
        let root = tempfile::tempdir().unwrap();
        let status = key_child(root.path(), boundary).status().unwrap();
        assert_eq!(status.code(), Some(86), "{boundary}");
        let final_path = authority(root.path()).join("key");
        assert_eq!(final_path.exists(), published, "{boundary}");
        let published_digest = published.then(|| sha256_hex(&fs::read(&final_path).unwrap()));
        if !published {
            let preparation = authority(root.path()).join(observation::PREPARATION_NAME);
            assert_eq!(fs::metadata(preparation).unwrap().len(), candidate_len);
        }
        let bytes = observation::key(&paging(root.path()), false).unwrap();
        if let Some(digest) = published_digest {
            assert_eq!(sha256_hex(&bytes), digest, "{boundary}");
        }
        assert_eq!(fs::read_dir(authority(root.path())).unwrap().count(), 1);
        assert_eq!(
            bytes,
            observation::key(&paging(root.path()), false).unwrap()
        );
    }
}

#[test]
fn concurrent_first_creators_in_separate_processes_return_one_key() {
    let root = tempfile::tempdir().unwrap();
    assert_eq!(
        key_child(root.path(), "partial").status().unwrap().code(),
        Some(86)
    );
    let mut children: Vec<_> = (0..8)
        .map(|id| {
            key_child(root.path(), "concurrent")
                .env("PAGES_KEY_ID", id.to_string())
                .spawn()
                .unwrap()
        })
        .collect();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !(0..8).all(|id| root.path().join(format!("ready-{id}")).exists()) {
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    fs::write(root.path().join("go"), b"").unwrap();
    for child in &mut children {
        assert!(child.wait().unwrap().success());
    }
    let digest = sha256_hex(&observation::key(&paging(root.path()), false).unwrap());
    for id in 0..8 {
        assert_eq!(
            fs::read_to_string(root.path().join(format!("result-{id}"))).unwrap(),
            digest
        );
    }
    assert_eq!(fs::read_dir(authority(root.path())).unwrap().count(), 1);
}

#[test]
fn key_publication_errors_never_return_authority_and_a_visible_final_is_resynced() {
    for failed in [
        PublicationPoint::CandidateSynced,
        PublicationPoint::Published,
        PublicationPoint::FinalSynced,
        PublicationPoint::DirectorySynced,
    ] {
        let root = tempfile::tempdir().unwrap();
        let result = observation::key_with_observer(&paging(root.path()), false, |point| {
            if point == failed {
                Err(std::io::Error::other("injected publication failure"))
            } else {
                Ok(())
            }
        });
        assert!(result.is_err());
        let path = authority(root.path()).join("key");
        let prior = path.exists().then(|| sha256_hex(&fs::read(&path).unwrap()));
        let mut points = Vec::new();
        let bytes = observation::key_with_observer(&paging(root.path()), false, |point| {
            points.push(point);
            Ok(())
        })
        .unwrap();
        assert!(points.ends_with(&[
            PublicationPoint::FinalSynced,
            PublicationPoint::DirectorySynced
        ]));
        if let Some(prior) = prior {
            assert_eq!(sha256_hex(&bytes), prior);
            assert_eq!(
                points,
                [
                    PublicationPoint::FinalSynced,
                    PublicationPoint::DirectorySynced
                ]
            );
        }
    }
}

#[test]
fn malformed_published_key_and_missing_issued_authority_are_never_repaired() {
    for len in [0, 16, 33] {
        let root = tempfile::tempdir().unwrap();
        observation::key(&paging(root.path()), false).unwrap();
        let path = authority(root.path()).join("key");
        fs::write(&path, vec![1u8; len]).unwrap();
        let error = observation::key(&paging(root.path()), false).unwrap_err();
        assert_eq!(error.code, "session_turn_page_io");
        assert!(error.message.contains("refusing replacement"));
        assert_eq!(fs::read(&path).unwrap(), vec![1u8; len]);
    }
    let root = tempfile::tempdir().unwrap();
    assert_eq!(
        key_child(root.path(), "CandidateSynced")
            .status()
            .unwrap()
            .code(),
        Some(86)
    );
    let preparation = authority(root.path()).join(observation::PREPARATION_NAME);
    let original = fs::read(&preparation).unwrap();
    assert_eq!(
        observation::key(&paging(root.path()), true)
            .unwrap_err()
            .code,
        "session_turn_page_token_stale"
    );
    assert_eq!(fs::read(&preparation).unwrap(), original);
    assert!(!authority(root.path()).join("key").exists());
}

#[test]
fn unsafe_key_paths_and_residue_are_refused_without_cleanup() {
    use std::os::unix::fs::{symlink, PermissionsExt};
    for name in ["key", observation::PREPARATION_NAME] {
        for kind in ["symlink", "directory", "oversized", "hardlink", "public"] {
            let root = tempfile::tempdir().unwrap();
            crate::durable_fs::create_private_directories(&authority(root.path())).unwrap();
            let outside = root.path().join("unrelated");
            fs::write(&outside, [2u8; 32]).unwrap();
            fs::set_permissions(&outside, fs::Permissions::from_mode(0o600)).unwrap();
            let path = authority(root.path()).join(name);
            match kind {
                "symlink" => symlink(&outside, &path).unwrap(),
                "directory" => fs::create_dir(&path).unwrap(),
                "hardlink" => fs::hard_link(&outside, &path).unwrap(),
                _ => {
                    fs::write(&path, vec![3u8; if kind == "oversized" { 33 } else { 32 }]).unwrap();
                    let mode = if kind == "public" { 0o644 } else { 0o600 };
                    fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
                }
            }
            assert!(
                observation::key(&paging(root.path()), false).is_err(),
                "{name}/{kind}"
            );
            assert!(fs::symlink_metadata(&path).is_ok());
            assert_eq!(fs::read(&outside).unwrap(), [2u8; 32]);
            assert_eq!(fs::read_dir(authority(root.path())).unwrap().count(), 1);
        }
    }
    let root = tempfile::tempdir().unwrap();
    let outside = root.path().join("unrelated-dir");
    fs::create_dir(&outside).unwrap();
    fs::set_permissions(&outside, fs::Permissions::from_mode(0o755)).unwrap();
    symlink(&outside, authority(root.path())).unwrap();
    assert!(observation::key(&paging(root.path()), false).is_err());
    assert_eq!(fs::read_dir(&outside).unwrap().count(), 0);
}

#[test]
fn hmac_matches_rfc4231_case_one() {
    let mut key = [0u8; 32];
    key[..20].fill(0x0b);
    let mac = observation::authenticate_for_test(&key, b"Hi There");
    let hex: String = mac.iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(
        hex,
        "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
    );
}

#[test]
fn anchors_cover_at_most_the_window_and_never_cross_the_retained_record() {
    let bytes = b"0123456789".repeat(20);
    let span = anchor(100, &bytes, 300, None).unwrap();
    assert_eq!(span.start, 300 - ANCHOR_BYTES);
    assert_eq!(
        span.sha256,
        sha256_hex(&bytes[(200 - ANCHOR_BYTES as usize)..])
    );
    assert_eq!(anchor(100, &bytes, 130, None).unwrap().start, 100);
    assert_eq!(anchor(100, &bytes, 300, Some(290)).unwrap().start, 290);
    assert!(anchor(100, &bytes, 100, None).is_none());
}
