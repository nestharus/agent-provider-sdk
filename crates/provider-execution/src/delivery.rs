//! Bounded launch-output delivery to the host.
//!
//! No background writer owns a native child or a borrowed output stream. A
//! write that cannot make progress within the stall limit fails, and a failed
//! write stays failed, so dispatch cannot append a response to a partial JSON
//! event or wait on the same pipe again while unwinding launch custody.
//!
//! A provider binary should wrap its stdout with [`BoundedOutput::stdout`]
//! before running a launch; writing launch events to a plain blocking stdout
//! does not carry this guarantee.
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, RawFd};
use std::os::unix::fs::{FileTypeExt, OpenOptionsExt};
use std::time::{Duration, Instant};

/// Default time a single write may make no progress before delivery fails.
pub const OUTPUT_STALL_LIMIT: Duration = Duration::from_secs(2);

/// Non-blocking, stall-bounded writer over a private duplicate of a host
/// output descriptor.
pub struct BoundedOutput {
    output: File,
    socket: bool,
    stall_limit: Duration,
    failure: Option<io::ErrorKind>,
}

impl BoundedOutput {
    /// Wraps this process's stdout with the default stall limit.
    pub fn stdout() -> io::Result<Self> {
        let stdout = unsafe { BorrowedFd::borrow_raw(libc::STDOUT_FILENO) };
        Self::duplicate(stdout, OUTPUT_STALL_LIMIT)
    }

    /// Wraps a duplicate of `output`. The inherited descriptor's file status
    /// flags are never changed: a FIFO is reopened as an independent
    /// non-blocking open file description, and admission fails if that cannot
    /// be isolated.
    pub fn duplicate(output: BorrowedFd<'_>, stall_limit: Duration) -> io::Result<Self> {
        let fd = unsafe { libc::fcntl(output.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3) };
        if fd == -1 {
            return Err(io::Error::last_os_error());
        }
        let output = unsafe { File::from_raw_fd(fd) };
        let kind = output.metadata()?.file_type();
        let output = if kind.is_fifo() {
            private_pipe(output)?
        } else {
            output
        };
        Ok(Self {
            output,
            socket: kind.is_socket(),
            stall_limit,
            failure: None,
        })
    }

    /// Descriptor actually written; distinct from the inherited one.
    pub fn as_raw_fd(&self) -> RawFd {
        self.output.as_raw_fd()
    }

    /// The first delivery failure, which every later write and flush repeats.
    pub fn failure(&self) -> Option<io::ErrorKind> {
        self.failure
    }

    fn fail(&mut self, kind: io::ErrorKind) -> io::Error {
        self.failure = Some(kind);
        io::Error::new(kind, "host launch output delivery failed")
    }

    fn write_ready(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let started = Instant::now();
        loop {
            if started.elapsed() >= self.stall_limit {
                return Err(self.fail(io::ErrorKind::TimedOut));
            }
            let count = self.try_write(bytes);
            if count >= 0 {
                return Ok(count as usize);
            }
            let kind = io::Error::last_os_error().kind();
            match kind {
                io::ErrorKind::Interrupted => continue,
                io::ErrorKind::WouldBlock => self.wait_ready()?,
                _ => return Err(self.fail(kind)),
            }
        }
    }

    fn try_write(&self, bytes: &[u8]) -> isize {
        if self.socket {
            return unsafe {
                libc::send(
                    self.output.as_raw_fd(),
                    bytes.as_ptr().cast(),
                    bytes.len(),
                    libc::MSG_DONTWAIT,
                )
            };
        }
        unsafe { libc::write(self.output.as_raw_fd(), bytes.as_ptr().cast(), bytes.len()) }
    }

    fn wait_ready(&mut self) -> io::Result<()> {
        let mut fd = libc::pollfd {
            fd: self.output.as_raw_fd(),
            events: libc::POLLOUT,
            revents: 0,
        };
        let result = unsafe { libc::poll(&mut fd, 1, 20) };
        if result >= 0 {
            return Ok(());
        }
        let kind = io::Error::last_os_error().kind();
        if kind == io::ErrorKind::Interrupted {
            return Ok(());
        }
        Err(self.fail(kind))
    }
}

impl Write for BoundedOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if let Some(kind) = self.failure {
            return Err(self.fail(kind));
        }
        self.write_ready(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(kind) = self.failure {
            return Err(self.fail(kind));
        }
        // Writes go directly to the descriptor; there is no userspace buffer.
        Ok(())
    }
}

// Reopening a FIFO obtains an independent open file description on Linux. Do
// not set O_NONBLOCK on inherited stdout: its flags may be shared with a host
// or another invocation. Fail admission on fd filesystems that cannot isolate
// those flags, rather than silently falling back to a blocking pipe.
fn private_pipe(inherited: File) -> io::Result<File> {
    let fd = inherited.as_raw_fd();
    let original = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if original == -1 {
        return Err(io::Error::last_os_error());
    }
    let output = OpenOptions::new()
        .write(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(format!("/dev/fd/{fd}"))?;
    let observed = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if observed != original {
        unsafe {
            libc::fcntl(fd, libc::F_SETFL, original);
        }
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "cannot isolate host output pipe flags",
        ));
    }
    let flags = unsafe { libc::fcntl(output.as_raw_fd(), libc::F_GETFL) };
    if flags == -1 {
        return Err(io::Error::last_os_error());
    }
    if flags & libc::O_NONBLOCK == 0 {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "host output pipe is not nonblocking",
        ));
    }
    Ok(output)
}
