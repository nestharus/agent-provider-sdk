use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

#[test]
fn every_schema_matches_the_recorded_import_digest() {
    let schema_dir = contract_v1_dir();
    let upstream =
        fs::read_to_string(schema_dir.join("UPSTREAM.md")).expect("read contract provenance");
    let recorded = recorded_schema_digests(&upstream);
    assert_eq!(recorded.len(), 13, "all provider/v1 schemas are recorded");

    let actual = fs::read_dir(&schema_dir)
        .expect("read contract directory")
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let name = entry.file_name().into_string().ok()?;
            name.ends_with(".schema.json").then_some(name)
        })
        .collect::<Vec<_>>();
    assert_eq!(actual.len(), recorded.len(), "no unrecorded schema exists");

    for (name, expected) in recorded {
        let bytes = fs::read(schema_dir.join(&name)).expect("read pinned schema");
        let observed = format!("{:x}", Sha256::digest(bytes));
        assert_eq!(observed, expected, "{name} differs from provenance");
    }
}

fn contract_v1_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("contract/v1")
}

fn recorded_schema_digests(upstream: &str) -> BTreeMap<String, String> {
    upstream
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let digest = fields.next()?;
            let filename = fields.next()?;
            (digest.len() == 64 && filename.ends_with(".schema.json"))
                .then(|| (filename.to_owned(), digest.to_owned()))
        })
        .collect()
}
