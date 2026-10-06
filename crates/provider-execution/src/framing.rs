//! `oulipoly.provider/v1` launch-event framing.
//!
//! Each event receives the contract discriminator, the request ID, a sequence
//! number starting at one, and a timestamp, and is written as one NDJSON line:
//! first to the custody journal, then to the host. Which native events become
//! launch events, and which markers a provider emits, are adapter decisions.

use crate::custody::{CustodyError, Journal, JournalReceipt};
use crate::encoding::{encode_base64, now_unix_ms};
use serde_json::{json, Value};
use std::fmt;
use std::io::{self, Write};

#[derive(Debug)]
pub enum FramingError {
    /// The journal byte count overflowed.
    Overflow,
    /// Journal or host delivery failed.
    Io(io::Error),
}

impl fmt::Display for FramingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overflow => formatter.write_str("launch journal byte count overflowed"),
            Self::Io(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for FramingError {}

impl From<io::Error> for FramingError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<CustodyError> for FramingError {
    fn from(error: CustodyError) -> Self {
        match error {
            CustodyError::JournalOverflow => Self::Overflow,
            CustodyError::Io(error) => Self::Io(error),
            other => Self::Io(io::Error::other(other.to_string())),
        }
    }
}

/// Frames launch events into a journal and a host writer.
pub struct LaunchEventWriter<'a, W: Write> {
    writer: &'a mut W,
    journal: Journal,
    contract: &'a str,
    request_id: &'a str,
    seq: u64,
}

impl<'a, W: Write> LaunchEventWriter<'a, W> {
    pub fn new(
        writer: &'a mut W,
        journal: Journal,
        contract: &'a str,
        request_id: &'a str,
    ) -> Self {
        Self {
            writer,
            journal,
            contract,
            request_id,
            seq: 0,
        }
    }

    /// Frames and delivers one event. Delivery is flushed before returning.
    pub fn event(&mut self, mut event: Value) -> Result<(), FramingError> {
        self.seq += 1;
        event["contract"] = json!(self.contract);
        event["request_id"] = json!(self.request_id);
        event["seq"] = json!(self.seq);
        event["time_unix_ms"] = json!(now_unix_ms());
        let mut bytes = serde_json::to_vec(&event).unwrap();
        bytes.push(b'\n');
        self.journal.append(&bytes)?;
        self.writer.write_all(&bytes)?;
        self.writer.flush()?;
        Ok(())
    }

    pub fn marker(&mut self, name: &str, value: Value) -> Result<(), FramingError> {
        self.event(json!({"kind":"marker", "name":name, "value":value}))
    }

    /// Emits a `stdout` or `stderr` data event carrying `bytes`.
    pub fn data(&mut self, kind: &str, bytes: &[u8]) -> Result<(), FramingError> {
        self.event(json!({"kind":kind,"data_base64":encode_base64(bytes)}))
    }

    pub fn heartbeat(&mut self) -> Result<(), FramingError> {
        self.event(json!({"kind":"heartbeat"}))
    }

    /// Last sequence number written.
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Synchronizes the journal and returns its receipt.
    pub fn seal(&mut self) -> io::Result<JournalReceipt> {
        self.journal.seal()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::custody::replay_journal;
    use crate::custody::LaunchState;

    #[test]
    fn events_are_correlated_sequenced_and_journaled_before_delivery() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("journal.jsonl");
        let mut delivered = Vec::new();
        let receipt = {
            let journal = Journal::create_new(&path).unwrap();
            let mut writer =
                LaunchEventWriter::new(&mut delivered, journal, "oulipoly.provider/v1", "req-1");
            writer.marker("example", json!(true)).unwrap();
            writer.data("stdout", b"hi\n").unwrap();
            writer.heartbeat().unwrap();
            assert_eq!(writer.seq(), 3);
            writer.seal().unwrap()
        };
        let lines: Vec<Value> = delivered
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();
        assert_eq!(lines.len(), 3);
        for (index, line) in lines.iter().enumerate() {
            assert_eq!(line["contract"], "oulipoly.provider/v1");
            assert_eq!(line["request_id"], "req-1");
            assert_eq!(line["seq"], index as u64 + 1);
            assert!(line["time_unix_ms"].is_u64());
        }
        assert_eq!(lines[1]["kind"], "stdout");
        assert_eq!(lines[1]["data_base64"], "aGkK");
        assert_eq!(std::fs::read(&path).unwrap(), delivered);
        let state = LaunchState {
            journal_sha256: Some(receipt.sha256),
            journal_len: Some(receipt.len),
            ..LaunchState::default()
        };
        let mut replayed = Vec::new();
        replay_journal(&path, &state, &mut replayed).unwrap();
        assert_eq!(replayed, delivered);
    }

    struct FailingHost;
    impl Write for FailingHost {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::Error::from(io::ErrorKind::TimedOut))
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn host_delivery_failure_is_reported_after_journaling() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("journal.jsonl");
        let mut host = FailingHost;
        let journal = Journal::create_new(&path).unwrap();
        let mut writer = LaunchEventWriter::new(&mut host, journal, "c", "r");
        match writer.heartbeat() {
            Err(FramingError::Io(error)) => assert_eq!(error.kind(), io::ErrorKind::TimedOut),
            other => panic!("unexpected {other:?}"),
        }
        assert!(!std::fs::read(&path).unwrap().is_empty());
    }
}
