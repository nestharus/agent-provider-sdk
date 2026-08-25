use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub const REPORT_SCHEMA: &str = "agent-provider.memory/v1";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryKb {
    pub rss: u64,
    pub pss: u64,
    pub private_clean: u64,
    pub private_dirty: u64,
    pub shared_clean: u64,
    pub shared_dirty: u64,
    pub swap: u64,
    pub swap_pss: u64,
}

impl MemoryKb {
    fn add_assign(&mut self, other: Self) {
        self.rss = self.rss.saturating_add(other.rss);
        self.pss = self.pss.saturating_add(other.pss);
        self.private_clean = self.private_clean.saturating_add(other.private_clean);
        self.private_dirty = self.private_dirty.saturating_add(other.private_dirty);
        self.shared_clean = self.shared_clean.saturating_add(other.shared_clean);
        self.shared_dirty = self.shared_dirty.saturating_add(other.shared_dirty);
        self.swap = self.swap.saturating_add(other.swap);
        self.swap_pss = self.swap_pss.saturating_add(other.swap_pss);
    }

    fn max_assign(&mut self, other: Self) {
        self.rss = self.rss.max(other.rss);
        self.pss = self.pss.max(other.pss);
        self.private_clean = self.private_clean.max(other.private_clean);
        self.private_dirty = self.private_dirty.max(other.private_dirty);
        self.shared_clean = self.shared_clean.max(other.shared_clean);
        self.shared_dirty = self.shared_dirty.max(other.shared_dirty);
        self.swap = self.swap.max(other.swap);
        self.swap_pss = self.swap_pss.max(other.swap_pss);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessRole {
    Runner,
    ProviderBridge,
    Terminal,
    LanguageServer,
    McpServer,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProcessKey {
    pub pid: u32,
    pub start_time_ticks: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessSample {
    pub pid: u32,
    pub parent_pid: u32,
    pub start_time_ticks: u64,
    pub name: String,
    pub executable: Option<PathBuf>,
    pub role: ProcessRole,
    pub memory_kb: MemoryKb,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeSample {
    pub captured_unix_ms: u128,
    pub root: ProcessKey,
    pub process_count: usize,
    pub unavailable_processes: usize,
    pub total_kb: MemoryKb,
    pub processes: Vec<ProcessSample>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutableIdentity {
    pub path: PathBuf,
    pub size_bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandIdentity {
    pub executable: ExecutableIdentity,
    pub argument_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExitSummary {
    pub success: bool,
    pub code: Option<i32>,
    pub signal: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeakSummary {
    pub memory_kb: MemoryKb,
    pub process_count: usize,
    pub unavailable_processes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryReportConfig {
    pub mode: String,
    pub interval_ms: u64,
    pub retained_sample_limit: usize,
    pub label: Option<String>,
    pub command: Option<CommandIdentity>,
    pub identities: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryReport {
    pub schema: String,
    pub label: Option<String>,
    pub mode: String,
    pub root: ProcessKey,
    pub interval_ms: u64,
    pub retained_sample_limit: usize,
    pub dropped_samples: u64,
    pub started_unix_ms: u128,
    pub finished_unix_ms: u128,
    pub command: Option<CommandIdentity>,
    pub identities: BTreeMap<String, String>,
    pub exit: Option<ExitSummary>,
    pub peak: PeakSummary,
    pub samples: VecDeque<TreeSample>,
}

impl MemoryReport {
    pub fn new(root: ProcessKey, config: MemoryReportConfig) -> Self {
        let started_unix_ms = unix_ms();
        Self {
            schema: REPORT_SCHEMA.to_owned(),
            label: config.label,
            mode: config.mode,
            root,
            interval_ms: config.interval_ms,
            retained_sample_limit: config.retained_sample_limit.max(1),
            dropped_samples: 0,
            started_unix_ms,
            finished_unix_ms: started_unix_ms,
            command: config.command,
            identities: config.identities,
            exit: None,
            peak: PeakSummary {
                memory_kb: MemoryKb::default(),
                process_count: 0,
                unavailable_processes: 0,
            },
            samples: VecDeque::new(),
        }
    }

    pub fn push(&mut self, sample: TreeSample) {
        self.peak.memory_kb.max_assign(sample.total_kb);
        self.peak.process_count = self.peak.process_count.max(sample.process_count);
        self.peak.unavailable_processes = self
            .peak
            .unavailable_processes
            .max(sample.unavailable_processes);
        self.finished_unix_ms = sample.captured_unix_ms;
        if self.samples.len() == self.retained_sample_limit {
            self.samples.pop_front();
            self.dropped_samples = self.dropped_samples.saturating_add(1);
        }
        self.samples.push_back(sample);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProcError {
    #[error("process {pid} no longer exists")]
    ProcessGone { pid: u32 },
    #[error(
        "process identity changed for pid {pid}: expected start_time_ticks={expected}, observed={observed}"
    )]
    PidReused {
        pid: u32,
        expected: u64,
        observed: u64,
    },
    #[error("malformed {path}: {reason}")]
    Malformed { path: PathBuf, reason: String },
    #[error("failed to read {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

#[derive(Debug, Clone)]
struct ProcStat {
    pid: u32,
    parent_pid: u32,
    start_time_ticks: u64,
    name: String,
}

#[derive(Debug, Clone)]
pub struct ProcReader {
    root: PathBuf,
}

impl Default for ProcReader {
    fn default() -> Self {
        Self::new("/proc")
    }
}

impl ProcReader {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn process_key(&self, pid: u32) -> Result<ProcessKey, ProcError> {
        let stat = self.read_stat(pid)?;
        Ok(ProcessKey {
            pid,
            start_time_ticks: stat.start_time_ticks,
        })
    }

    pub fn sample_tree(&self, root: ProcessKey) -> Result<TreeSample, ProcError> {
        let root_stat = self.read_stat(root.pid)?;
        if root_stat.start_time_ticks != root.start_time_ticks {
            return Err(ProcError::PidReused {
                pid: root.pid,
                expected: root.start_time_ticks,
                observed: root_stat.start_time_ticks,
            });
        }

        let mut stats = HashMap::new();
        stats.insert(root.pid, root_stat);
        let entries = fs::read_dir(&self.root).map_err(|source| ProcError::Io {
            path: self.root.clone(),
            source,
        })?;
        for entry in entries.flatten() {
            let Some(pid) = numeric_pid(&entry.file_name()) else {
                continue;
            };
            if pid == root.pid {
                continue;
            }
            if let Ok(stat) = self.read_stat(pid) {
                stats.insert(pid, stat);
            }
        }

        let mut included = BTreeSet::new();
        included.insert(root.pid);
        loop {
            let before = included.len();
            for stat in stats.values() {
                if included.contains(&stat.parent_pid) {
                    included.insert(stat.pid);
                }
            }
            if included.len() == before {
                break;
            }
        }

        let mut unavailable_processes = 0usize;
        let mut processes = Vec::new();
        let mut total_kb = MemoryKb::default();
        for pid in included {
            let Some(stat) = stats.get(&pid) else {
                unavailable_processes += 1;
                continue;
            };
            let memory_kb = match self.read_smaps_rollup(pid) {
                Ok(memory) => memory,
                Err(ProcError::ProcessGone { .. }) | Err(ProcError::Io { .. }) => {
                    unavailable_processes += 1;
                    continue;
                }
                Err(error) => return Err(error),
            };
            total_kb.add_assign(memory_kb);
            let executable = fs::read_link(self.pid_path(pid).join("exe")).ok();
            let command_line = fs::read(self.pid_path(pid).join("cmdline")).unwrap_or_default();
            let role = classify_process(&stat.name, &command_line);
            processes.push(ProcessSample {
                pid,
                parent_pid: stat.parent_pid,
                start_time_ticks: stat.start_time_ticks,
                name: stat.name.clone(),
                executable,
                role,
                memory_kb,
            });
        }

        Ok(TreeSample {
            captured_unix_ms: unix_ms(),
            root,
            process_count: processes.len(),
            unavailable_processes,
            total_kb,
            processes,
        })
    }

    fn read_stat(&self, pid: u32) -> Result<ProcStat, ProcError> {
        let path = self.pid_path(pid).join("stat");
        let value = fs::read_to_string(&path).map_err(|source| {
            if source.kind() == io::ErrorKind::NotFound {
                ProcError::ProcessGone { pid }
            } else {
                ProcError::Io {
                    path: path.clone(),
                    source,
                }
            }
        })?;
        parse_stat(&path, &value)
    }

    fn read_smaps_rollup(&self, pid: u32) -> Result<MemoryKb, ProcError> {
        let path = self.pid_path(pid).join("smaps_rollup");
        let value = fs::read_to_string(&path).map_err(|source| {
            if source.kind() == io::ErrorKind::NotFound {
                ProcError::ProcessGone { pid }
            } else {
                ProcError::Io {
                    path: path.clone(),
                    source,
                }
            }
        })?;
        parse_smaps_rollup(&path, &value)
    }

    fn pid_path(&self, pid: u32) -> PathBuf {
        self.root.join(pid.to_string())
    }
}

pub fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

pub fn executable_identity(command: &OsStr) -> io::Result<ExecutableIdentity> {
    let path = resolve_executable(command)?;
    let metadata = fs::metadata(&path)?;
    let mut file = File::open(&path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(ExecutableIdentity {
        path,
        size_bytes: metadata.len(),
        sha256: format!("{:x}", hasher.finalize()),
    })
}

fn resolve_executable(command: &OsStr) -> io::Result<PathBuf> {
    let command_path = Path::new(command);
    if command_path.components().count() > 1 {
        return fs::canonicalize(command_path);
    }
    let Some(path) = std::env::var_os("PATH") else {
        return Err(io::Error::new(io::ErrorKind::NotFound, "PATH is unset"));
    };
    for directory in std::env::split_paths(&path) {
        let candidate = directory.join(command);
        if candidate.is_file() {
            return fs::canonicalize(candidate);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!("executable {:?} was not found in PATH", command),
    ))
}

fn numeric_pid(name: &OsStr) -> Option<u32> {
    name.to_str()?.parse().ok()
}

fn parse_stat(path: &Path, value: &str) -> Result<ProcStat, ProcError> {
    let open = value
        .find('(')
        .ok_or_else(|| malformed(path, "missing opening parenthesis"))?;
    let close = value
        .rfind(')')
        .ok_or_else(|| malformed(path, "missing closing parenthesis"))?;
    if close <= open {
        return Err(malformed(path, "invalid command field"));
    }
    let pid = value[..open]
        .trim()
        .parse::<u32>()
        .map_err(|_| malformed(path, "invalid pid"))?;
    let name = value[open + 1..close].to_owned();
    let fields: Vec<&str> = value[close + 1..].split_whitespace().collect();
    if fields.len() < 20 {
        return Err(malformed(path, "not enough stat fields"));
    }
    let parent_pid = fields[1]
        .parse::<u32>()
        .map_err(|_| malformed(path, "invalid parent pid"))?;
    let start_time_ticks = fields[19]
        .parse::<u64>()
        .map_err(|_| malformed(path, "invalid start time"))?;
    Ok(ProcStat {
        pid,
        parent_pid,
        start_time_ticks,
        name,
    })
}

fn parse_smaps_rollup(path: &Path, value: &str) -> Result<MemoryKb, ProcError> {
    let mut memory = MemoryKb::default();
    let mut saw_rss = false;
    let mut saw_pss = false;
    for line in value.lines() {
        let Some((key, rest)) = line.split_once(':') else {
            continue;
        };
        let Some(raw) = rest.split_whitespace().next() else {
            continue;
        };
        let parsed = raw
            .parse::<u64>()
            .map_err(|_| malformed(path, &format!("invalid {key} value")))?;
        match key {
            "Rss" => {
                memory.rss = parsed;
                saw_rss = true;
            }
            "Pss" => {
                memory.pss = parsed;
                saw_pss = true;
            }
            "Private_Clean" => memory.private_clean = parsed,
            "Private_Dirty" => memory.private_dirty = parsed,
            "Shared_Clean" => memory.shared_clean = parsed,
            "Shared_Dirty" => memory.shared_dirty = parsed,
            "Swap" => memory.swap = parsed,
            "SwapPss" => memory.swap_pss = parsed,
            _ => {}
        }
    }
    if !saw_rss || !saw_pss {
        return Err(malformed(path, "missing Rss or Pss"));
    }
    Ok(memory)
}

fn malformed(path: &Path, reason: &str) -> ProcError {
    ProcError::Malformed {
        path: path.to_path_buf(),
        reason: reason.to_owned(),
    }
}

fn classify_process(name: &str, command_line: &[u8]) -> ProcessRole {
    let mut haystack = name.to_ascii_lowercase();
    haystack.push(' ');
    haystack.push_str(&String::from_utf8_lossy(command_line).to_ascii_lowercase());
    if haystack.contains("language-server")
        || haystack.contains("langserver")
        || haystack.contains("pyright")
        || haystack.contains("rust-analyzer")
    {
        ProcessRole::LanguageServer
    } else if haystack.contains("mcp") {
        ProcessRole::McpServer
    } else if haystack.contains("agent-runner-opencode")
        || haystack.contains("agent-runner-codex")
        || haystack.contains("agent-runner-claude")
        || haystack.contains("agent-runner-pi")
    {
        ProcessRole::ProviderBridge
    } else if haystack.contains("opencode")
        || haystack.contains("claude")
        || haystack.contains("codex")
        || haystack.contains("pi-coding-agent")
    {
        ProcessRole::Terminal
    } else if haystack.contains("oulipoly-agent-runner") || haystack.contains("agents") {
        ProcessRole::Runner
    } else {
        ProcessRole::Other
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn stat(pid: u32, name: &str, parent: u32, start: u64) -> String {
        let mut fields = vec!["S".to_owned(), parent.to_string()];
        fields.extend((5..=21).map(|_| "0".to_owned()));
        fields.push(start.to_string());
        format!("{pid} ({name}) {}\n", fields.join(" "))
    }

    fn smaps(rss: u64, pss: u64, private: u64, shared: u64) -> String {
        format!(
            "0000-1000 r--p 00000000 00:00 0 [rollup]\nRss: {rss} kB\nPss: {pss} kB\nPrivate_Clean: 0 kB\nPrivate_Dirty: {private} kB\nShared_Clean: {shared} kB\nShared_Dirty: 0 kB\nSwap: 3 kB\nSwapPss: 2 kB\n"
        )
    }

    fn process(root: &Path, pid: u32, name: &str, parent: u32, start: u64, memory: &str) {
        let directory = root.join(pid.to_string());
        fs::create_dir_all(&directory).unwrap();
        fs::write(directory.join("stat"), stat(pid, name, parent, start)).unwrap();
        fs::write(directory.join("smaps_rollup"), memory).unwrap();
        fs::write(directory.join("cmdline"), name.as_bytes()).unwrap();
    }

    #[test]
    fn samples_descendants_across_process_groups_and_sums_pss() {
        let temp = TempDir::new().unwrap();
        process(temp.path(), 100, "agents", 1, 10, &smaps(100, 80, 60, 40));
        process(
            temp.path(),
            101,
            "opencode",
            100,
            11,
            &smaps(200, 150, 130, 70),
        );
        process(
            temp.path(),
            102,
            "pyright-langserver",
            101,
            12,
            &smaps(90, 70, 50, 40),
        );
        process(
            temp.path(),
            999,
            "unrelated",
            1,
            99,
            &smaps(900, 800, 700, 200),
        );

        let sample = ProcReader::new(temp.path())
            .sample_tree(ProcessKey {
                pid: 100,
                start_time_ticks: 10,
            })
            .unwrap();

        assert_eq!(sample.process_count, 3);
        assert_eq!(sample.total_kb.rss, 390);
        assert_eq!(sample.total_kb.pss, 300);
        assert_eq!(sample.total_kb.private_dirty, 240);
        assert_eq!(sample.total_kb.shared_clean, 150);
        assert!(sample
            .processes
            .iter()
            .any(|p| p.role == ProcessRole::LanguageServer));
    }

    #[test]
    fn rejects_pid_reuse() {
        let temp = TempDir::new().unwrap();
        process(temp.path(), 100, "agents", 1, 44, &smaps(1, 1, 1, 0));
        let error = ProcReader::new(temp.path())
            .sample_tree(ProcessKey {
                pid: 100,
                start_time_ticks: 43,
            })
            .unwrap_err();
        assert!(matches!(error, ProcError::PidReused { .. }));
    }

    #[test]
    fn counts_child_that_exits_during_sampling_as_unavailable() {
        let temp = TempDir::new().unwrap();
        process(temp.path(), 100, "agents", 1, 10, &smaps(1, 1, 1, 0));
        let child = temp.path().join("101");
        fs::create_dir_all(&child).unwrap();
        fs::write(child.join("stat"), stat(101, "short-lived", 100, 11)).unwrap();

        let sample = ProcReader::new(temp.path())
            .sample_tree(ProcessKey {
                pid: 100,
                start_time_ticks: 10,
            })
            .unwrap();
        assert_eq!(sample.process_count, 1);
        assert_eq!(sample.unavailable_processes, 1);
    }

    #[test]
    fn rejects_malformed_root_stat_and_smaps() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("100");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("stat"), "not a stat record").unwrap();
        assert!(matches!(
            ProcReader::new(temp.path()).process_key(100),
            Err(ProcError::Malformed { .. })
        ));

        fs::write(root.join("stat"), stat(100, "agents", 1, 10)).unwrap();
        fs::write(root.join("smaps_rollup"), "Rss: nope kB\nPss: 1 kB\n").unwrap();
        assert!(matches!(
            ProcReader::new(temp.path()).sample_tree(ProcessKey {
                pid: 100,
                start_time_ticks: 10
            }),
            Err(ProcError::Malformed { .. })
        ));
    }

    #[test]
    fn peak_tracks_each_metric_without_double_counting_shared_pages() {
        let root = ProcessKey {
            pid: 1,
            start_time_ticks: 2,
        };
        let mut report = MemoryReport::new(
            root,
            MemoryReportConfig {
                mode: "attach".to_owned(),
                interval_ms: 100,
                retained_sample_limit: 4,
                label: None,
                command: None,
                identities: BTreeMap::new(),
            },
        );
        report.push(TreeSample {
            captured_unix_ms: 2,
            root,
            process_count: 2,
            unavailable_processes: 0,
            total_kb: MemoryKb {
                rss: 300,
                pss: 180,
                shared_clean: 120,
                ..MemoryKb::default()
            },
            processes: vec![],
        });
        assert_eq!(report.peak.memory_kb.rss, 300);
        assert_eq!(report.peak.memory_kb.pss, 180);
        assert_ne!(report.peak.memory_kb.rss, report.peak.memory_kb.pss);
    }

    #[test]
    fn report_retention_is_bounded_without_losing_peak() {
        let root = ProcessKey {
            pid: 1,
            start_time_ticks: 2,
        };
        let mut report = MemoryReport::new(
            root,
            MemoryReportConfig {
                mode: "attach".to_owned(),
                interval_ms: 10,
                retained_sample_limit: 2,
                label: None,
                command: None,
                identities: BTreeMap::new(),
            },
        );
        for pss in [100, 900, 200] {
            report.push(TreeSample {
                captured_unix_ms: pss as u128,
                root,
                process_count: 1,
                unavailable_processes: 0,
                total_kb: MemoryKb {
                    pss,
                    ..MemoryKb::default()
                },
                processes: vec![],
            });
        }
        assert_eq!(report.samples.len(), 2);
        assert_eq!(report.dropped_samples, 1);
        assert_eq!(report.peak.memory_kb.pss, 900);
        assert_eq!(report.samples.front().unwrap().total_kb.pss, 900);
    }
}
