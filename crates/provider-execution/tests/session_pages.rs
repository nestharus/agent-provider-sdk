//! The common paging engine driven by a fake adapter that supplies only native
//! facts. Its native format, paths, identifiers and turn ids deliberately differ
//! from any real provider's, and every turn fact it reports is a known fake
//! fact. These controls exercise engine mechanics; they are not native,
//! provider-pair or restart-hardware qualification.
use agent_provider_contract::generated::{ErrorCategory, ErrorObject, RequestEnvelope};
use agent_provider_execution::encoding::sha256_hex;
use agent_provider_execution::session_pages::{
    read_turns, read_turns_with_limits, NativeFact, NativeRecord, NativeTurn, PageAdapter,
    PageFact, SnapshotFacts, SourceFailure, StagingLimits,
};
use serde_json::{json, Value};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

const ID: &str = "0123abcd-0000-4000-8000-00000000beef";
const NONCE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const MAX_RECORD: usize = 8 * 1024 * 1024;

fn fake_error(category: ErrorCategory, code: &str) -> ErrorObject {
    ErrorObject {
        code: code.into(),
        category,
        message: format!("fake adapter: {code}"),
        retryable: false,
        details: None,
        diagnostics: Vec::new(),
    }
}

/// A fake native store: `<HOME>/.fakeprov/<settings>/store/<session>.log`,
/// a `{"fake":"head","sid":..}` first record, and message records carrying a
/// native `uid` and explicit `parent`/`side`/`fold` facts. A missing fact key
/// is an unavailable fact, never a default.
struct Fake {
    source_final: NativeFact<bool>,
}

const FAKE: Fake = Fake {
    source_final: NativeFact::Known(false),
};

fn fact<T>(value: &Value, read: impl Fn(&Value) -> Option<T>) -> NativeFact<T> {
    match read(value) {
        Some(value) => NativeFact::Known(value),
        None => NativeFact::Unavailable,
    }
}

impl PageAdapter for Fake {
    fn namespace(&self) -> &str {
        "fake-notes"
    }

    fn default_data_root(&self, request: &RequestEnvelope) -> Result<PathBuf, ErrorObject> {
        request
            .host
            .env
            .get("HOME")
            .map(|home| Path::new(home).join(".fake-state"))
            .ok_or_else(|| fake_error(ErrorCategory::InvalidSettings, "fake_home_missing"))
    }

    fn account(
        &self,
        request: &RequestEnvelope,
        settings_id: &str,
    ) -> Result<PathBuf, ErrorObject> {
        let home = request
            .host
            .env
            .get("HOME")
            .ok_or_else(|| fake_error(ErrorCategory::InvalidSettings, "fake_home_missing"))?;
        let account = Path::new(home).join(".fakeprov").join(settings_id);
        if !account.is_dir() {
            return Err(fake_error(
                ErrorCategory::InvalidSettings,
                "fake_account_missing",
            ));
        }
        Ok(account)
    }

    fn validate_session_id(&self, session_id: &str) -> bool {
        (8..=64).contains(&session_id.len())
            && session_id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b) || b == b'-')
    }

    fn source_candidates(
        &self,
        account: &Path,
        session_id: &str,
        bound: bool,
        _: &RequestEnvelope,
    ) -> Result<Vec<PathBuf>, ErrorObject> {
        let store = account.join("store");
        let named = store.join(format!("{session_id}.log"));
        if !bound && named.is_file() {
            return Ok(vec![named]);
        }
        let mut all: Vec<_> = fs::read_dir(&store)
            .map_err(|_| fake_error(ErrorCategory::Failed, "fake_source_io"))?
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| path.extension().is_some_and(|ext| ext == "log"))
            .collect();
        all.sort();
        Ok(all)
    }

    fn first_record_session(
        &self,
        record: &[u8],
        _: &RequestEnvelope,
    ) -> Result<String, ErrorObject> {
        let value: Value = serde_json::from_slice(record)
            .map_err(|_| fake_error(ErrorCategory::Failed, "fake_source_invalid"))?;
        match (&value["fake"], value["sid"].as_str()) {
            (Value::String(kind), Some(sid)) if kind == "head" => Ok(sid.to_owned()),
            _ => Err(fake_error(ErrorCategory::Failed, "fake_source_invalid")),
        }
    }

    fn source_error(&self, failure: SourceFailure, _: &RequestEnvelope) -> ErrorObject {
        match failure {
            SourceFailure::NotFound => {
                fake_error(ErrorCategory::Unavailable, "fake_session_missing")
            }
            SourceFailure::Ambiguous => {
                fake_error(ErrorCategory::Conflict, "fake_session_ambiguous")
            }
            SourceFailure::Io => fake_error(ErrorCategory::Failed, "fake_source_io"),
        }
    }

    fn project(
        &self,
        record: &NativeRecord<'_>,
        _: &RequestEnvelope,
    ) -> Result<Option<NativeTurn>, ErrorObject> {
        if record.bytes.iter().all(u8::is_ascii_whitespace) {
            return Ok(None);
        }
        let value: Value = serde_json::from_slice(record.bytes)
            .map_err(|_| fake_error(ErrorCategory::InvalidRequest, "fake_record_invalid"))?;
        if value["fake"] != "msg" {
            return Ok(None);
        }
        let role = match value["who"].as_str() {
            Some("user") => "user",
            Some("agent") => "assistant",
            _ => return Ok(None),
        };
        let parts = value["parts"]
            .as_array()
            .ok_or_else(|| fake_error(ErrorCategory::InvalidRequest, "fake_record_invalid"))?;
        let text: Vec<String> = parts
            .iter()
            .filter_map(|part| part.as_str().map(str::to_owned))
            .collect();
        let observable = text.len() == parts.len() && !text.is_empty();
        if record.projection
            == &agent_provider_contract::generated::SessionTurnProjection::UserObservation
            && !observable
        {
            return Ok(None);
        }
        Ok(Some(NativeTurn {
            turn_id: value["uid"]
                .as_str()
                .ok_or_else(|| fake_error(ErrorCategory::InvalidRequest, "fake_record_invalid"))?
                .to_owned(),
            timestamp: value["at"].as_str().unwrap_or_default().to_owned(),
            role: role.into(),
            text,
            parent_turn_id: fact(&value, |v| match v.get("parent")? {
                Value::Null => Some(None),
                Value::String(id) => Some(Some(id.clone())),
                _ => None,
            }),
            is_sidechain: fact(&value, |v| v.get("side")?.as_bool()),
            is_compaction_boundary: fact(&value, |v| v.get("fold")?.as_bool()),
        }))
    }

    fn source_final(&self, _: &SnapshotFacts<'_>) -> NativeFact<bool> {
        self.source_final.clone()
    }

    fn unavailable_fact(&self, fact: PageFact, _: &RequestEnvelope) -> ErrorObject {
        let mut error = fake_error(ErrorCategory::Unsupported, "fake_fact_unavailable");
        error.message = format!("fake adapter cannot supply {fact:?}");
        error
    }
}

fn message(uid: &str, who: &str, text: &str) -> Value {
    json!({"fake":"msg","uid":uid,"who":who,"at":"2026-10-08T00:00:01Z","parts":[text],
        "parent":null,"side":false,"fold":false})
}

fn padding(bytes: usize) -> String {
    let mut value = json!({"fake":"note","pad":""});
    let overhead = value.to_string().len() + 1;
    value["pad"] = json!("x".repeat(bytes - overhead));
    let line = format!("{value}\n");
    assert_eq!(line.len(), bytes);
    line
}

struct Fixture {
    root: TempDir,
    path: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let store = root.path().join(".fakeprov/main/store");
        fs::create_dir_all(&store).unwrap();
        let path = store.join(format!("{ID}.log"));
        fs::write(&path, format!("{}\n", json!({"fake":"head","sid":ID}))).unwrap();
        Self { root, path }
    }

    fn header_len(&self) -> usize {
        fs::read(&self.path)
            .unwrap()
            .iter()
            .position(|b| *b == b'\n')
            .unwrap()
            + 1
    }

    fn append_value(&self, value: &Value) {
        self.append_raw(format!("{value}\n").as_bytes());
    }

    fn append(&self, uid: &str, who: &str, text: &str) {
        self.append_value(&message(uid, who, text));
    }

    fn append_raw(&self, bytes: &[u8]) {
        fs::OpenOptions::new()
            .append(true)
            .open(&self.path)
            .unwrap()
            .write_all(bytes)
            .unwrap();
    }

    fn state(&self) -> PathBuf {
        self.root
            .path()
            .join("state/provider-state/fake-notes/session-pages-v1")
    }

    fn params(&self) -> Value {
        json!({"settings_id":"main","session_id":ID,"read_protocol":"oulipoly.session_turn_pages/v1",
            "turn_projection":"canonical_ingest","start_mode":"beginning","after_token":null,
            "snapshot_id":null,"page_token":null,"max_turns":1,"max_response_bytes":4096,
            "max_source_bytes":1048576,"max_inline_body_bytes":65536})
    }

    fn observation_params(&self) -> Value {
        let mut p = self.params();
        p["turn_projection"] = json!("user_observation");
        p["expected_delivery_nonce"] = json!(NONCE);
        p["max_source_bytes"] = json!(512);
        p["max_response_bytes"] = json!(524_288);
        p
    }

    fn envelope(&self, p: &Value) -> Value {
        json!({"contract":"oulipoly.provider/v1","request_id":"page-test",
            "provider_instance_id":"fake-provider","host":{"app":"test",
            "data_root":self.root.path().join("state"),"env":{"HOME":self.root.path()}},"params":p})
    }

    fn request(&self, p: &Value) -> RequestEnvelope {
        serde_json::from_value(self.envelope(p)).unwrap()
    }

    fn try_read_with(
        &self,
        request: &RequestEnvelope,
        limits: StagingLimits,
    ) -> Result<Value, ErrorObject> {
        let page = serde_json::to_value(read_turns_with_limits(&FAKE, request, limits)?).unwrap();
        check_page(&page, request);
        Ok(page)
    }

    fn try_read(&self, p: &Value) -> Result<Value, ErrorObject> {
        self.try_read_with(&self.request(p), StagingLimits::DEFAULT)
    }

    fn read(&self, p: &Value) -> Value {
        self.try_read(p).unwrap()
    }

    fn code(&self, p: &Value) -> String {
        self.try_read(p).unwrap_err().code
    }
}

fn continuation(p: &Value, page: &Value) -> Value {
    let mut next = p.clone();
    next["start_mode"] = json!("continuation");
    next["after_token"] = Value::Null;
    next["snapshot_id"] = page["snapshot_id"].clone();
    next["page_token"] = page["next_page_token"].clone();
    next
}

fn resume(p: &Value, page: &Value) -> Value {
    let mut next = p.clone();
    next["start_mode"] = json!("beginning");
    next["after_token"] = page["resume_token"].clone();
    next["snapshot_id"] = Value::Null;
    next["page_token"] = Value::Null;
    next
}

/// Invariants every successful page must hold against its own request: the
/// framed response fits, and native reads obey the projection's accounting.
fn check_page(page: &Value, request: &RequestEnvelope) {
    let framed = json!({"contract":"oulipoly.provider/v1","request_id":request.request_id,
        "ok":true,"result":page});
    agent_provider_contract::schemas::validate_response("session.read_turns", &framed).unwrap();
    let response = serde_json::to_vec(&framed).unwrap().len() + 1;
    assert!(response as u64 <= request.params["max_response_bytes"].as_u64().unwrap());
    let quantum = request.params["max_source_bytes"].as_u64().unwrap();
    let total = page["source_bytes_examined"].as_u64().unwrap();
    if page["turn_projection"] == "user_observation" {
        let (forward, reconstruction, metadata) = declaration(page);
        assert!(forward + metadata <= quantum);
        assert!(reconstruction < MAX_RECORD as u64);
        assert_eq!(forward + reconstruction + metadata, total);
    } else {
        assert_eq!(page["warnings"], json!([]));
        assert!(total <= quantum);
    }
}

fn declaration(page: &Value) -> (u64, u64, u64) {
    let warnings = page["warnings"].as_array().unwrap();
    assert_eq!(warnings.len(), 1);
    let fields: Vec<u64> = warnings[0]
        .as_str()
        .unwrap()
        .strip_prefix("codex_observation_io_v1:forward=")
        .unwrap()
        .split([';', '='])
        .filter_map(|field| field.parse().ok())
        .collect();
    assert_eq!(fields.len(), 3);
    (fields[0], fields[1], fields[2])
}

fn drain(f: &Fixture, p: &Value, limit: usize) -> (Vec<Value>, Value) {
    let mut request = p.clone();
    let mut turns = Vec::new();
    for _ in 0..limit {
        let page = f.read(&request);
        assert_eq!(page, f.read(&request), "replayed page differs");
        turns.extend(page["turns"].as_array().unwrap().iter().cloned());
        if page["snapshot_complete"] == true {
            return (turns, page);
        }
        assert!(page["scan_progress"] == true || page["page_turn_count"].as_u64().unwrap() > 0);
        request = continuation(p, &page);
    }
    panic!("bounded drain did not reach snapshot end");
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

#[test]
fn both_projections_page_replay_and_complete_with_native_turn_ids() {
    let f = Fixture::new();
    f.append("u-1", "user", "first");
    f.append("a-1", "agent", "second");
    f.append_value(&json!({"fake":"msg","uid":"t-1","who":"tool","parts":["hidden"]}));
    let original = fs::read(&f.path).unwrap();
    let p = f.params();
    let first = f.read(&p);
    assert_eq!(first, f.read(&p));
    assert_eq!(first["page_turn_count"], 1);
    assert_eq!(first["snapshot_complete"], false);
    assert_eq!(first["turns"][0]["turn_id"], "u-1");
    let second = f.read(&continuation(&p, &first));
    assert_eq!(second["page_index"], 1);
    assert_eq!(second["page_start_sequence"], 1);
    assert_eq!(second["turns"][0]["role"], "assistant");
    assert_eq!(second["turns"][0]["turn_id"], "a-1");
    assert_eq!(second["turns"][0]["snapshot_sequence"], 1);
    assert_eq!(second["snapshot_complete"], true);
    assert_eq!(second["source_final"], false);
    assert!(second["resume_token"]
        .as_str()
        .unwrap()
        .starts_with("fake-notes-stp1-"));

    let mut o = f.observation_params();
    o["max_source_bytes"] = json!(1_048_576);
    o["max_turns"] = json!(8);
    let observed = f.read(&o);
    assert_eq!(observed["page_turn_count"], 1);
    assert_eq!(observed["turns"][0]["role"], "user");
    assert_eq!(observed["snapshot_complete"], true);
    assert!(observed["resume_token"]
        .as_str()
        .unwrap()
        .starts_with("fake-notes-obs1-"));
    assert_eq!(fs::read(&f.path).unwrap(), original);
}

#[test]
fn cursors_bind_provider_account_projection_nonce_budgets_and_snapshot() {
    let f = Fixture::new();
    f.append("u-1", "user", "first");
    f.append("u-2", "user", "second");
    let other = f.root.path().join(".fakeprov/other/store");
    fs::create_dir_all(&other).unwrap();
    fs::copy(&f.path, other.join(format!("{ID}.log"))).unwrap();
    for observation in [false, true] {
        let mut p = if observation {
            f.observation_params()
        } else {
            f.params()
        };
        p["max_source_bytes"] = json!(1_048_576);
        p["max_response_bytes"] = json!(4096);
        let first = f.read(&p);
        let next = continuation(&p, &first);
        assert_eq!(f.read(&next)["turns"][0]["turn_id"], "u-2");
        let mut cases = Vec::new();
        let mut changed = next.clone();
        changed["max_turns"] = json!(2);
        cases.push(changed);
        let mut changed = next.clone();
        changed["settings_id"] = json!("other");
        cases.push(changed);
        let mut changed = next.clone();
        changed["snapshot_id"] = json!("f".repeat(64));
        cases.push(changed);
        let mut changed = next.clone();
        if observation {
            changed["expected_delivery_nonce"] = json!("b".repeat(64));
            cases.push(changed.clone());
            changed["turn_projection"] = json!("canonical_ingest");
            changed
                .as_object_mut()
                .unwrap()
                .remove("expected_delivery_nonce");
        } else {
            changed["turn_projection"] = json!("user_observation");
            changed["expected_delivery_nonce"] = json!(NONCE);
        }
        cases.push(changed);
        for case in cases {
            assert_eq!(f.code(&case), "session_turn_page_token_stale", "{case}");
        }
        let mut provider = f.request(&next);
        provider.provider_instance_id = Some("other-provider".into());
        assert_eq!(
            read_turns(&FAKE, &provider).unwrap_err().code,
            "session_turn_page_token_stale"
        );
    }
    let mut canonical_tail = f.params();
    canonical_tail["start_mode"] = json!("tail");
    canonical_tail["after_token"] = Value::Null;
    assert_eq!(f.code(&canonical_tail), "invalid_session_read_turns_params");
    let mut bad_id = f.params();
    bad_id["session_id"] = json!("UPPER_case");
    assert_eq!(f.code(&bad_id), "invalid_session_read_turns_params");
}

/// The host's chunk serialization: `type`, then `text`.
fn host_body(text: &str) -> Vec<u8> {
    format!("[{{\"type\":\"text\",\"text\":{}}}]", json!(text)).into_bytes()
}

#[test]
fn body_serialization_digests_normalization_and_oversize_omission() {
    let f = Fixture::new();
    f.append("u-1", "user", " one\r\ntwo ");
    let page = f.read(&f.params());
    let turn = &page["turns"][0];
    let host = host_body(" one\r\ntwo ");
    assert_eq!(turn["body_state"], "inline");
    assert_eq!(turn["body_bytes"], host.len());
    assert_eq!(turn["body_sha256"], sha256_hex(&host));
    assert_eq!(turn["canonical_text_sha256"], sha256_hex(b"one\ntwo"));

    let f = Fixture::new();
    f.append("u-1", "user", &"x".repeat(12000));
    let mut p = f.params();
    p["max_inline_body_bytes"] = json!(10);
    p["max_response_bytes"] = json!(1800);
    let page = f.read(&p);
    let body = host_body(&"x".repeat(12000));
    assert_eq!(page["turns"][0]["body_state"], "omitted_oversize");
    assert!(page["turns"][0]["body"].is_null());
    assert_eq!(page["turns"][0]["body_sha256"], sha256_hex(&body));
    // A body under the inline limit is still omitted when the response cannot
    // hold it; its digests remain.
    p["max_inline_body_bytes"] = json!(65536);
    let page = f.read(&p);
    assert_eq!(page["turns"][0]["body_state"], "omitted_oversize");
    assert_eq!(page["turns"][0]["body_bytes"], body.len());

    let f = Fixture::new();
    f.append_value(
        &json!({"fake":"msg","uid":"u-1","who":"user","at":"t","parts":[],
        "parent":null,"side":false,"fold":false}),
    );
    let page = f.read(&f.params());
    assert_eq!(page["turns"][0]["body_state"], "absent");
    assert!(page["turns"][0]["canonical_text_sha256"].is_null());
}

#[test]
fn known_fake_facts_pass_through_and_unavailable_facts_refuse_without_effects() {
    let f = Fixture::new();
    f.append_value(
        &json!({"fake":"msg","uid":"u-1","who":"user","at":"t","parts":["a"],
        "parent":"u-0","side":true,"fold":true}),
    );
    f.append_value(
        &json!({"fake":"msg","uid":"u-2","who":"user","at":"t","parts":["b"],
        "parent":null,"fold":false}),
    );
    let p = f.params();
    let first = f.read(&p);
    let turn = &first["turns"][0];
    assert_eq!(turn["parent_turn_id"], "u-0");
    assert_eq!(turn["is_sidechain"], true);
    assert_eq!(turn["is_compaction_boundary"], true);
    let before = files(f.root.path());
    let next = continuation(&p, &first);
    let error = f.try_read(&next).unwrap_err();
    assert_eq!(error.code, "fake_fact_unavailable");
    assert!(error.message.contains("IsSidechain"));
    assert_eq!(files(f.root.path()), before);
    // Never skipped past: the refusal repeats and no later turn is reached.
    f.append("u-3", "user", "later");
    assert_eq!(f.code(&next), "fake_fact_unavailable");
    let mut wide = p.clone();
    wide["max_turns"] = json!(8);
    assert_eq!(f.code(&wide), "fake_fact_unavailable");

    let unknown_final = Fake {
        source_final: NativeFact::Unavailable,
    };
    let g = Fixture::new();
    g.append("u-1", "user", "a");
    let error = read_turns(&unknown_final, &g.request(&g.params())).unwrap_err();
    assert_eq!(error.code, "fake_fact_unavailable");
    assert!(error.message.contains("SourceFinal"));
    assert!(files(&g.state()).is_empty());
    let known_final = Fake {
        source_final: NativeFact::Known(true),
    };
    let page = read_turns(&known_final, &g.request(&g.params())).unwrap();
    assert!(page.source_final);
}

#[test]
fn tail_anchor_then_append_returns_only_new_user_and_strips_matching_marker() {
    let f = Fixture::new();
    f.append("u-old", "user", "old");
    let mut p = f.observation_params();
    p["start_mode"] = json!("tail");
    let tail = f.read(&p);
    assert_eq!(tail["snapshot_complete"], true);
    assert_eq!(tail["scan_progress"], false);
    assert_eq!(tail["turns"], json!([]));
    f.append("a-1", "agent", "skip");
    f.append(
        "u-new",
        "user",
        &format!("new task\n[OULIPOLY-DELIVERY {NONCE}]"),
    );
    f.append(
        "u-other",
        "user",
        &format!("other\n[OULIPOLY-DELIVERY {}]", "b".repeat(64)),
    );
    let mut next = resume(&p, &tail);
    next["max_turns"] = json!(8);
    next["max_source_bytes"] = json!(4096);
    let page = f.read(&next);
    assert_eq!(page["page_start_sequence"], 0);
    assert_eq!(page["page_turn_count"], 2);
    assert_eq!(page["turns"][0]["turn_id"], "u-new");
    assert_eq!(page["turns"][0]["body"][0]["text"], "new task");
    assert_eq!(
        page["turns"][0]["canonical_text_sha256"],
        sha256_hex(b"new task")
    );
    // A different nonce's marker is user text, not a delivery marker.
    assert!(page["turns"][1]["body"][0]["text"]
        .as_str()
        .unwrap()
        .ends_with(&format!("[OULIPOLY-DELIVERY {}]", "b".repeat(64))));
}

#[test]
fn unfinished_final_record_is_retained_and_completed_by_append_in_both_projections() {
    for observation in [false, true] {
        let f = Fixture::new();
        f.append("u-old", "user", "old");
        let record = format!("{}\n", message("u-new", "user", &"z".repeat(1200)));
        let split = record.len() - 5;
        f.append_raw(&record.as_bytes()[..split]);
        let mut p = if observation {
            f.observation_params()
        } else {
            f.params()
        };
        p["max_turns"] = json!(10);
        p["max_source_bytes"] = json!(512);
        let (turns, end) = drain(&f, &p, 16);
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0]["turn_id"], "u-old");
        let still = f.read(&resume(&p, &end));
        assert_eq!(still["snapshot_complete"], true);
        assert_eq!(still["turns"], json!([]));
        if observation {
            // The whole retained prefix is re-read from native source and
            // declared as reconstruction; nothing new is read forward.
            assert_eq!(
                declaration(&still),
                (0, split as u64, f.header_len() as u64)
            );
        }
        f.append_raw(&record.as_bytes()[split..]);
        let (turns, _) = drain(&f, &resume(&p, &end), 8);
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0]["turn_id"], "u-new");
        assert_eq!(
            turns[0]["canonical_text_sha256"],
            sha256_hex("z".repeat(1200).as_bytes())
        );
        let staged = files(&f.state())
            .iter()
            .filter(|(path, _)| path.extension().is_some_and(|ext| ext == "part"))
            .count();
        assert_eq!(staged > 0, !observation);
    }
}

#[test]
fn small_source_budgets_advance_over_nonturn_records_with_scan_progress() {
    let f = Fixture::new();
    for i in 0..8 {
        f.append_value(&json!({"fake":"note","n":i,"pad":"y".repeat(100)}));
    }
    f.append("u-1", "user", "visible");
    for observation in [false, true] {
        let mut p = if observation {
            f.observation_params()
        } else {
            f.params()
        };
        p["max_source_bytes"] = json!(300);
        let mut request = p.clone();
        let (mut seen, mut scanned, mut complete) = (0, false, false);
        for _ in 0..40 {
            let page = f.read(&request);
            assert!(page["source_bytes_examined"].as_u64().unwrap() <= 300 || observation);
            seen += page["page_turn_count"].as_u64().unwrap();
            scanned |= page["scan_progress"] == true;
            if page["snapshot_complete"] == true {
                complete = true;
                break;
            }
            request = continuation(&p, &page);
        }
        assert!(complete && scanned);
        assert_eq!(seen, 1);
    }
}

/// Rewrites the native bytes immediately before `boundary` in place (same
/// inode), then appends, so length grows and only content can reveal it.
fn rewrite_before(f: &Fixture, boundary: usize, append: &[u8]) {
    let mut bytes = fs::read(&f.path).unwrap();
    let index = (boundary - 10..boundary - 1)
        .find(|&i| bytes[i].is_ascii_alphanumeric())
        .unwrap();
    bytes[index] = if bytes[index] == b'q' { b'r' } else { b'q' };
    bytes.extend_from_slice(append);
    fs::OpenOptions::new()
        .write(true)
        .open(&f.path)
        .unwrap()
        .write_all(&bytes)
        .unwrap();
}

#[test]
fn source_replacement_truncation_forgery_and_rewrites_are_stale_while_appends_continue() {
    for observation in [false, true] {
        for mutation in [
            "append",
            "replace",
            "truncate",
            "forge",
            "head",
            "head_shift",
            "boundary",
            "partial",
            "missing",
        ] {
            let f = Fixture::new();
            f.append("u-1", "user", &"a".repeat(200));
            f.append("u-2", "user", &"b".repeat(200));
            f.append("u-3", "user", &"c".repeat(1200));
            let mut p = if observation {
                f.observation_params()
            } else {
                f.params()
            };
            p["max_turns"] = json!(1);
            p["max_source_bytes"] = json!(if mutation == "partial" { 900 } else { 4096 });
            let first = f.read(&p);
            assert_eq!(first["turns"][0]["turn_id"], "u-1");
            let mut next = continuation(&p, &first);
            let u1 = message("u-1", "user", &"a".repeat(200)).to_string().len() + 1;
            let mut boundary = f.header_len() + u1;
            if mutation == "partial" {
                // The second page returns u-2 and retains an unfinished u-3
                // prefix ending where its 900-byte quantum ended.
                let second = f.read(&next);
                assert_eq!(second["turns"][0]["turn_id"], "u-2");
                next = continuation(&p, &second);
                // Canonical spends 64 bytes of that quantum re-checking the
                // first page's boundary; observation declares it separately.
                boundary = u1 + 900 - if observation { 0 } else { 64 };
                if observation {
                    assert!(declaration(&f.read(&next)).1 > 64);
                } else {
                    assert!(files(&f.state())
                        .iter()
                        .any(|(path, _)| path.extension().is_some_and(|ext| ext == "part")));
                }
            }
            match mutation {
                "append" => f.append("u-4", "user", "appended"),
                "replace" => {
                    let copy = f.path.with_extension("copy");
                    fs::copy(&f.path, &copy).unwrap();
                    fs::rename(&copy, &f.path).unwrap();
                }
                "truncate" => fs::OpenOptions::new()
                    .write(true)
                    .open(&f.path)
                    .unwrap()
                    .set_len(f.header_len() as u64 + 10)
                    .unwrap(),
                "forge" => {
                    let token = next["page_token"].as_str().unwrap();
                    let mut bytes = token.as_bytes().to_vec();
                    let at = bytes.len() - 5;
                    bytes[at] = if bytes[at] == b'A' { b'B' } else { b'A' };
                    next["page_token"] = json!(String::from_utf8(bytes).unwrap());
                }
                "head" | "head_shift" => {
                    // Same session and inode; the first record is rewritten.
                    // "head" keeps its length, so every later offset and the
                    // boundary bytes are unchanged and only the first-record
                    // digest can reveal it; "head_shift" also moves offsets.
                    let source = fs::read_to_string(&f.path).unwrap();
                    let rest = &source[f.header_len()..];
                    let head = if mutation == "head" {
                        format!("{{\"sid\":\"{ID}\",\"fake\":\"head\"}}")
                    } else {
                        json!({"fake":"head","sid":ID,"migrated":true}).to_string()
                    };
                    assert_eq!(head.len() + 1 == f.header_len(), mutation == "head");
                    let mut file = fs::OpenOptions::new().write(true).open(&f.path).unwrap();
                    file.write_all(format!("{head}\n{rest}").as_bytes())
                        .unwrap();
                    if mutation == "head" {
                        f.append_raw(b"\n");
                    }
                }
                "boundary" | "partial" => rewrite_before(&f, boundary, b"\n"),
                "missing" => fs::remove_file(&f.path).unwrap(),
                _ => unreachable!(),
            }
            let result = f.try_read(&next);
            if mutation == "append" {
                let page = result.unwrap();
                assert_eq!(page["turns"][0]["turn_id"], "u-2");
                let (turns, end) = drain(&f, &continuation(&p, &page), 16);
                assert_eq!(turns.len(), 1, "snapshot excludes the append");
                assert_eq!(f.read(&resume(&p, &end))["turns"][0]["turn_id"], "u-4");
            } else {
                assert_eq!(
                    result.unwrap_err().code,
                    "session_turn_page_token_stale",
                    "observation={observation} mutation={mutation}"
                );
            }
        }
    }
}

#[test]
fn rewritten_boundary_is_stale_on_resume_after_a_completed_snapshot() {
    for observation in [false, true] {
        let f = Fixture::new();
        f.append("u-1", "user", "first");
        let mut p = if observation {
            f.observation_params()
        } else {
            f.params()
        };
        p["max_turns"] = json!(8);
        let end = f.read(&p);
        assert_eq!(end["snapshot_complete"], true);
        let boundary = fs::metadata(&f.path).unwrap().len() as usize;
        rewrite_before(
            &f,
            boundary,
            format!("{}\n", message("u-2", "user", "x")).as_bytes(),
        );
        assert_eq!(
            f.code(&resume(&p, &end)),
            "session_turn_page_token_stale",
            "observation={observation}"
        );
        // A clean append after the same boundary resumes normally.
        let g = Fixture::new();
        g.append("u-1", "user", "first");
        let end = g.read(&p);
        g.append("u-2", "user", "second");
        let page = g.read(&resume(&p, &end));
        assert_eq!(page["turns"][0]["turn_id"], "u-2");
        if observation {
            // The boundary check re-reads at most 64 already-examined bytes.
            assert!(declaration(&page).1 <= 64 && declaration(&page).1 > 0);
        }
    }
}

#[test]
fn record_ceiling_refuses_deterministically_and_never_skips() {
    for (size, refused) in [(MAX_RECORD, false), (MAX_RECORD + 1, true)] {
        let f = Fixture::new();
        f.append_raw(padding(size).as_bytes());
        f.append("u-after", "user", "after");
        let p = f.params();
        let mut request = p.clone();
        let mut outcome = None;
        for _ in 0..16 {
            match f.try_read(&request) {
                Ok(page) => {
                    if page["page_turn_count"] == 1 {
                        assert_eq!(page["turns"][0]["turn_id"], "u-after");
                        outcome = Some(false);
                        break;
                    }
                    assert_eq!(page["turns"], json!([]));
                    assert_eq!(page["snapshot_complete"], false);
                    request = continuation(&p, &page);
                }
                Err(error) => {
                    assert_eq!(error.code, "session_turn_record_ceiling_exceeded");
                    assert!(!error.retryable);
                    assert_eq!(f.code(&request), error.code);
                    outcome = Some(true);
                    break;
                }
            }
        }
        assert_eq!(outcome, Some(refused), "record size {size}");
    }
}

#[test]
fn malformed_complete_record_is_an_adapter_error_not_a_skip() {
    let f = Fixture::new();
    f.append_raw(b"{not json}\n");
    f.append("u-1", "user", "must not be reached");
    for p in [f.params(), f.observation_params()] {
        assert_eq!(f.code(&p), "fake_record_invalid");
    }
    let g = Fixture::new();
    g.append_value(
        &json!({"fake":"msg","uid":"u-1","who":"user","at":"","parts":["x"],
        "parent":null,"side":false,"fold":false}),
    );
    assert_eq!(g.code(&g.params()), "invalid_session_read_turns_params");
}

#[test]
fn exhausted_canonical_staging_preserves_checkpoint_while_observation_bypasses_it() {
    let f = Fixture::new();
    f.append_raw(padding(900).as_bytes());
    f.append("u-1", "user", "after");
    let mut p = f.params();
    p["max_source_bytes"] = json!(512);
    let tight = StagingLimits {
        bytes: 16384,
        objects: 2,
    };
    let request = f.request(&p);
    let first = f.try_read_with(&request, tight).unwrap();
    assert_eq!(first["snapshot_complete"], false);
    assert_eq!(first["scan_progress"], true);
    let before = files(&f.state());
    assert_eq!(before.len(), 2);
    assert_eq!(f.try_read_with(&request, tight).unwrap(), first);
    let next = f.request(&continuation(&p, &first));
    assert_eq!(
        f.try_read_with(&next, tight).unwrap_err().code,
        "session_turn_staging_capacity_exceeded"
    );
    assert_eq!(files(&f.state()), before);
    // Replay of the published page needs no new admission.
    let none = StagingLimits {
        bytes: 0,
        objects: 0,
    };
    assert_eq!(f.try_read_with(&request, none).unwrap(), first);
    // Observation reads the same source with no staging admission at all.
    let mut o = f.observation_params();
    o["max_turns"] = json!(4);
    let mut observed = o.clone();
    let mut seen = Vec::new();
    for _ in 0..12 {
        let page = f.try_read_with(&f.request(&observed), none).unwrap();
        seen.extend(page["turns"].as_array().unwrap().iter().cloned());
        if page["snapshot_complete"] == true {
            break;
        }
        observed = continuation(&o, &page);
    }
    assert_eq!(seen.len(), 1);
    assert_eq!(files(&f.state()), before);
}

#[test]
fn failed_cursor_publication_leaves_only_an_admitted_recoverable_prefix() {
    let f = Fixture::new();
    f.append_raw(padding(900).as_bytes());
    f.append("u-1", "user", "after");
    let mut p = f.params();
    p["max_source_bytes"] = json!(512);
    let request = f.request(&p);
    let one = StagingLimits {
        bytes: 16384,
        objects: 1,
    };
    assert_eq!(
        f.try_read_with(&request, one).unwrap_err().code,
        "session_turn_staging_capacity_exceeded"
    );
    let orphan = files(&f.state());
    assert_eq!(orphan.len(), 1);
    assert!(orphan[0].0.extension().is_some_and(|ext| ext == "part"));
    let two = StagingLimits {
        bytes: 16384,
        objects: 2,
    };
    let first = f.try_read_with(&request, two).unwrap();
    let state = files(&f.state());
    assert_eq!(state.len(), 2);
    assert!(state.contains(&orphan[0]));
    let none = StagingLimits {
        bytes: 0,
        objects: 0,
    };
    assert_eq!(f.try_read_with(&request, none).unwrap(), first);
    assert_eq!(files(&f.state()), state);
}

#[test]
fn concurrent_canonical_requests_deduplicate_cursor_and_prefix_at_exact_object_limit() {
    let f = Fixture::new();
    f.append_raw(padding(900).as_bytes());
    f.append("u-1", "user", "after");
    let mut p = f.params();
    p["max_source_bytes"] = json!(512);
    let request = f.request(&p);
    let barrier = std::sync::Barrier::new(6);
    let pages: Vec<Value> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..6)
            .map(|_| {
                scope.spawn(|| {
                    barrier.wait();
                    let limits = StagingLimits {
                        bytes: 16384,
                        objects: 2,
                    };
                    serde_json::to_value(read_turns_with_limits(&FAKE, &request, limits).unwrap())
                        .unwrap()
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    assert!(pages.iter().all(|page| page == &pages[0]));
    assert_eq!(files(&f.state()).len(), 2);
}

#[test]
#[ignore = "subprocess fixture only; the parent supplies isolated request/result paths"]
fn observation_page_subprocess_fixture() {
    let request: RequestEnvelope =
        serde_json::from_slice(&fs::read(std::env::var("PAGE_REQUEST_PATH").unwrap()).unwrap())
            .unwrap();
    let page = read_turns(&FAKE, &request).unwrap();
    fs::write(
        std::env::var("PAGE_RESULT_PATH").unwrap(),
        serde_json::to_vec(&page).unwrap(),
    )
    .unwrap();
}

fn subprocess(f: &Fixture, p: &Value) -> Value {
    let input = f.root.path().join("request.json");
    let output = f.root.path().join("result.json");
    fs::write(&input, serde_json::to_vec(&f.envelope(p)).unwrap()).unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "observation_page_subprocess_fixture",
            "--ignored",
        ])
        .env("PAGE_REQUEST_PATH", &input)
        .env("PAGE_RESULT_PATH", &output)
        .status()
        .unwrap();
    assert!(status.success());
    serde_json::from_slice(&fs::read(output).unwrap()).unwrap()
}

#[test]
fn observation_multiquantum_record_declares_exact_reconstruction_across_process_restart() {
    let f = Fixture::new();
    let mut text = String::new();
    for i in 0u32..200 {
        text.push_str(&sha256_hex(&i.to_le_bytes()));
    }
    f.append(
        "u-big",
        "user",
        &format!("{text}\n[OULIPOLY-DELIVERY {NONCE}]"),
    );
    let header = f.header_len();
    let record = fs::read(&f.path).unwrap().len() - header;
    let p = f.observation_params();
    let quantum = 512 - header;
    let mut request = p.clone();
    let mut last = None;
    for index in 0..64 {
        let page = if index == 0 {
            subprocess(&f, &request)
        } else {
            f.read(&request)
        };
        assert_eq!(page, f.read(&request));
        assert_eq!(page["page_index"], index);
        let (forward, reconstruction, metadata) = declaration(&page);
        assert_eq!(metadata as usize, header);
        // Each page re-reads exactly the retained prefix and reads forward a
        // full quantum (or the remainder), so the record crosses many pages
        // without staging.
        assert_eq!(reconstruction as usize, index * quantum);
        assert_eq!(forward as usize, (record - index * quantum).min(quantum));
        if page["snapshot_complete"] == true {
            last = Some((request.clone(), page));
            break;
        }
        assert_eq!(page["turns"], json!([]));
        request = continuation(&p, &page);
    }
    let (last_request, last) = last.expect("bounded progress reaches the record end");
    assert_eq!(last["turns"][0]["body"][0]["text"], text);
    let restarted = subprocess(&f, &last_request);
    assert_eq!(
        serde_json::to_vec(&restarted).unwrap(),
        serde_json::to_vec(&last).unwrap()
    );
    assert!(files(&f.state()).is_empty());
    let authority = f.state().parent().unwrap().join("observation-auth-v1");
    assert_eq!(fs::read_dir(&authority).unwrap().count(), 1);
    assert_eq!(fs::metadata(authority.join("key")).unwrap().len(), 32);
}

#[test]
fn missing_observation_authority_for_an_issued_token_is_stale_and_never_reinitialized() {
    let f = Fixture::new();
    f.append("u-1", "user", &"x".repeat(1200));
    let p = f.observation_params();
    let first = f.read(&p);
    let key = f.state().parent().unwrap().join("observation-auth-v1/key");
    fs::remove_file(&key).unwrap();
    assert_eq!(
        f.code(&continuation(&p, &first)),
        "session_turn_page_token_stale"
    );
    assert!(!key.exists());
    // A fresh beginning (no issued token) may create new authority, after
    // which the old token is no longer authentic.
    f.read(&p);
    assert!(key.exists());
    assert_eq!(
        f.code(&continuation(&p, &first)),
        "session_turn_page_token_stale"
    );
}

#[test]
fn continuation_opens_only_the_bound_source_and_charges_exact_metadata() {
    for observation in [false, true] {
        let f = Fixture::new();
        f.append("u-1", "user", &"x".repeat(1800));
        let mut p = if observation {
            f.observation_params()
        } else {
            f.params()
        };
        p["max_source_bytes"] = json!(512);
        let first = f.read(&p);
        let request = continuation(&p, &first);
        let before = f.read(&request);
        let unrelated = f.path.parent().unwrap().join("zzzz.log");
        fs::write(
            &unrelated,
            format!(
                "{}\n",
                json!({"fake":"head","sid":"ffff0000","pad":"z".repeat(1000)})
            ),
        )
        .unwrap();
        assert_eq!(before, f.read(&request));
        if observation {
            assert_eq!(declaration(&before).2 as usize, f.header_len());
        } else {
            assert_eq!(before["source_bytes_examined"], 512);
        }
        // A fresh lookup that must scan every candidate is refused once an
        // unrelated header exceeds the remaining budget.
        fs::rename(&f.path, f.path.with_file_name("aaaa.log")).unwrap();
        assert_eq!(f.code(&p), "session_turn_page_budget_too_small");
    }
}

#[test]
fn metadata_only_page_falls_back_to_the_preceding_complete_boundary() {
    let f = Fixture::new();
    f.append("u-1", "user", &"x".repeat(128));
    let boundary = fs::metadata(&f.path).unwrap().len() as usize;
    f.append("u-2", "user", &"y".repeat(1200));
    let mut p = f.observation_params();
    p["max_inline_body_bytes"] = json!(0);
    p["max_turns"] = json!(4);
    p["max_source_bytes"] = json!(boundary);
    let (mut low, mut high) = (1023, 4096);
    while low + 1 < high {
        let budget = (low + high) / 2;
        p["max_response_bytes"] = json!(budget);
        if f.try_read(&p).is_ok() {
            high = budget;
        } else {
            low = budget;
        }
    }
    // Room for decimal growth, not for the retained-prefix token fields.
    p["max_response_bytes"] = json!(high + 32);
    let control = f.read(&p);
    assert_eq!(control["page_turn_count"], 1);
    p["max_source_bytes"] = json!(boundary + 80);
    let page = f.read(&p);
    assert_eq!(page["turns"], control["turns"]);
    assert_eq!(page["snapshot_complete"], false);
    let (forward, reconstruction, _) = declaration(&page);
    assert_eq!(
        (forward as usize, reconstruction),
        (boundary + 80 - f.header_len(), 0)
    );
    let next = f.read(&continuation(&p, &page));
    // The unretained suffix was charged but not checkpointed.
    assert_eq!(
        declaration(&next).1,
        64.min((boundary - f.header_len()) as u64)
    );
    let (turns, _) = drain(&f, &continuation(&p, &page), 16);
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0]["turn_id"], "u-2");
}

#[test]
fn common_engine_source_carries_no_provider_native_constants() {
    let sources = [
        include_str!("../src/session_pages.rs"),
        include_str!("../src/session_pages/staging.rs"),
        include_str!("../src/session_pages/observation.rs"),
    ];
    for source in sources {
        for line in source.lines() {
            let lower = line.to_ascii_lowercase();
            if lower.contains("codex_observation_io_v1") {
                continue;
            }
            for native in [
                "codex",
                "claude",
                "rollout",
                "session_meta",
                ":byte:",
                ".jsonl",
            ] {
                assert!(!lower.contains(native), "{native}: {line}");
            }
        }
    }
}
