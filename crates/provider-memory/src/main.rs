use agent_provider_memory::{
    executable_identity, unix_ms, CommandIdentity, ExitSummary, MemoryReport, MemoryReportConfig,
    ProcError, ProcReader,
};
use anyhow::{bail, Context, Result};
use clap::{Args, Parser, Subcommand};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::path::PathBuf;
use std::process::{Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Parser)]
#[command(name = "provider-memory")]
#[command(about = "Measure complete Agent Runner provider process trees on Linux")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Capture one JSON sample for an existing process tree.
    Snapshot(SnapshotArgs),
    /// Attach to an existing process tree and sample it for a bounded duration.
    Attach(AttachArgs),
    /// Launch a command and write a time-series report after it exits.
    Run(RunArgs),
}

#[derive(Debug, Args)]
struct SnapshotArgs {
    #[arg(long)]
    root_pid: u32,
    #[arg(long, default_value = "/proc", hide = true)]
    proc_root: PathBuf,
}

#[derive(Debug, Args)]
struct AttachArgs {
    #[arg(long)]
    root_pid: u32,
    #[arg(long)]
    duration_ms: u64,
    #[arg(long, default_value_t = 100)]
    interval_ms: u64,
    /// Bound retained samples in memory; peak values still cover dropped samples.
    #[arg(long, default_value_t = 4096)]
    max_samples: usize,
    #[arg(long)]
    label: Option<String>,
    #[arg(long = "identity", value_parser = parse_identity)]
    identities: Vec<(String, String)>,
    #[arg(long, default_value = "/proc", hide = true)]
    proc_root: PathBuf,
}

#[derive(Debug, Args)]
struct RunArgs {
    /// JSON report path. Child stdout/stderr remain attached to the caller.
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value_t = 100)]
    interval_ms: u64,
    /// Bound retained samples in memory; peak values still cover dropped samples.
    #[arg(long, default_value_t = 4096)]
    max_samples: usize,
    #[arg(long)]
    label: Option<String>,
    /// Non-secret benchmark identity, such as terminal_version=1.18.23.
    #[arg(long = "identity", value_parser = parse_identity)]
    identities: Vec<(String, String)>,
    #[arg(required = true, trailing_var_arg = true, allow_hyphen_values = true)]
    command: Vec<OsString>,
}

fn main() -> Result<()> {
    if !cfg!(target_os = "linux") {
        bail!("provider-memory currently requires Linux /proc with smaps_rollup support");
    }
    match Cli::parse().command {
        Commands::Snapshot(args) => snapshot(args),
        Commands::Attach(args) => attach(args),
        Commands::Run(args) => run(args),
    }
}

fn snapshot(args: SnapshotArgs) -> Result<()> {
    let reader = ProcReader::new(args.proc_root);
    let key = reader.process_key(args.root_pid)?;
    let sample = reader.sample_tree(key)?;
    serde_json::to_writer_pretty(std::io::stdout().lock(), &sample)?;
    println!();
    Ok(())
}

fn attach(args: AttachArgs) -> Result<()> {
    validate_sampling(args.interval_ms, args.max_samples)?;
    let reader = ProcReader::new(args.proc_root);
    let key = reader.process_key(args.root_pid)?;
    let mut report = MemoryReport::new(
        key,
        MemoryReportConfig {
            mode: "attach".to_owned(),
            interval_ms: args.interval_ms,
            retained_sample_limit: args.max_samples,
            label: args.label,
            command: None,
            identities: identities(args.identities)?,
        },
    );
    let deadline = Instant::now()
        .checked_add(Duration::from_millis(args.duration_ms))
        .context("duration-ms is too large for this platform")?;
    loop {
        match reader.sample_tree(key) {
            Ok(sample) => report.push(sample),
            Err(ProcError::ProcessGone { .. }) => break,
            Err(error) => return Err(error.into()),
        }
        if Instant::now() >= deadline {
            break;
        }
        thread::sleep(Duration::from_millis(args.interval_ms));
    }
    report.finished_unix_ms = unix_ms();
    serde_json::to_writer_pretty(std::io::stdout().lock(), &report)?;
    println!();
    Ok(())
}

fn run(args: RunArgs) -> Result<()> {
    validate_sampling(args.interval_ms, args.max_samples)?;
    let executable = args
        .command
        .first()
        .context("run requires a command after --")?;
    let command_identity = CommandIdentity {
        executable: executable_identity(executable)
            .with_context(|| format!("failed to identify executable {executable:?}"))?,
        argument_count: args.command.len().saturating_sub(1),
    };
    let mut child = Command::new(executable)
        .args(&args.command[1..])
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| format!("failed to launch {executable:?}"))?;
    let reader = ProcReader::default();
    let root = reader
        .process_key(child.id())
        .context("launched command exited before its process identity could be captured")?;
    let mut report = MemoryReport::new(
        root,
        MemoryReportConfig {
            mode: "run".to_owned(),
            interval_ms: args.interval_ms,
            retained_sample_limit: args.max_samples,
            label: args.label,
            command: Some(command_identity),
            identities: identities(args.identities)?,
        },
    );
    let status = loop {
        match reader.sample_tree(root) {
            Ok(sample) => report.push(sample),
            Err(ProcError::ProcessGone { .. }) => {}
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(error.into());
            }
        }
        if let Some(status) = child.try_wait()? {
            break status;
        }
        thread::sleep(Duration::from_millis(args.interval_ms));
    };
    report.exit = Some(exit_summary(status));
    report.finished_unix_ms = unix_ms();
    write_report(&args.output, &report)?;
    if !status.success() {
        bail!(
            "measured command failed with {}; report written to {}",
            status,
            args.output.display()
        );
    }
    Ok(())
}

fn validate_sampling(interval_ms: u64, max_samples: usize) -> Result<()> {
    if interval_ms == 0 {
        bail!("interval-ms must be greater than zero");
    }
    if max_samples == 0 {
        bail!("max-samples must be greater than zero");
    }
    Ok(())
}

fn parse_identity(value: &str) -> Result<(String, String), String> {
    let Some((key, value)) = value.split_once('=') else {
        return Err("identity must use KEY=VALUE".to_owned());
    };
    if key.trim().is_empty() || value.trim().is_empty() {
        return Err("identity key and value must be non-empty".to_owned());
    }
    Ok((key.trim().to_owned(), value.trim().to_owned()))
}

fn identities(values: Vec<(String, String)>) -> Result<BTreeMap<String, String>> {
    let mut identities = BTreeMap::new();
    for (key, value) in values {
        if identities.insert(key.clone(), value).is_some() {
            bail!("identity {key:?} was supplied more than once");
        }
    }
    Ok(identities)
}

fn write_report(path: &PathBuf, report: &MemoryReport) -> Result<()> {
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    fs::create_dir_all(parent)
        .with_context(|| format!("failed to create report directory {}", parent.display()))?;
    let temporary = path.with_extension(format!("{}.tmp", std::process::id()));
    let bytes = serde_json::to_vec_pretty(report)?;
    fs::write(&temporary, bytes)
        .with_context(|| format!("failed to write temporary report {}", temporary.display()))?;
    fs::rename(&temporary, path).with_context(|| {
        format!(
            "failed to publish report from {} to {}",
            temporary.display(),
            path.display()
        )
    })?;
    Ok(())
}

fn exit_summary(status: ExitStatus) -> ExitSummary {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        ExitSummary {
            success: status.success(),
            code: status.code(),
            signal: status.signal(),
        }
    }
    #[cfg(not(unix))]
    {
        ExitSummary {
            success: status.success(),
            code: status.code(),
            signal: None,
        }
    }
}
