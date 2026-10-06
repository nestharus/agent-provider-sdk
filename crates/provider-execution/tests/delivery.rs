#![cfg(target_os = "linux")]

use agent_provider_execution::delivery::BoundedOutput;
use std::io::{ErrorKind, Read, Write};
use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

const STALL: Duration = Duration::from_millis(200);

fn pipe() -> (std::fs::File, OwnedFd) {
    let mut fds = [0; 2];
    assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
    unsafe {
        (
            std::fs::File::from_raw_fd(fds[0]),
            OwnedFd::from_raw_fd(fds[1]),
        )
    }
}

fn flags(fd: &impl AsRawFd) -> i32 {
    unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) }
}

fn assert_stall_fails_and_stays_failed(output: &mut BoundedOutput) {
    let chunk = vec![b'x'; 64 * 1024];
    let started = Instant::now();
    let error = loop {
        match output.write(&chunk) {
            Ok(count) => assert!(
                count > 0 && started.elapsed() < Duration::from_secs(10),
                "unread output never stalled"
            ),
            Err(error) => break error,
        }
    };
    assert_eq!(error.kind(), ErrorKind::TimedOut);
    assert!(started.elapsed() >= STALL);
    assert!(started.elapsed() < STALL + Duration::from_secs(5));
    assert_eq!(output.failure(), Some(ErrorKind::TimedOut));
    assert_eq!(output.write(b"y").unwrap_err().kind(), ErrorKind::TimedOut);
    assert_eq!(output.flush().unwrap_err().kind(), ErrorKind::TimedOut);
}

#[test]
fn unread_pipe_fails_within_the_stall_limit_without_changing_inherited_flags() {
    let (_reader, writer) = pipe();
    let inherited = flags(&writer);
    let mut output = BoundedOutput::duplicate(writer.as_fd(), STALL).unwrap();
    assert_ne!(output.as_raw_fd(), writer.as_raw_fd());
    assert_eq!(flags(&writer), inherited, "inherited pipe flags changed");
    assert_eq!(inherited & libc::O_NONBLOCK, 0);
    assert_stall_fails_and_stays_failed(&mut output);
}

#[test]
fn unread_socket_fails_within_the_stall_limit() {
    let (_reader, writer) = UnixStream::pair().unwrap();
    let inherited = flags(&writer);
    let mut output = BoundedOutput::duplicate(writer.as_fd(), STALL).unwrap();
    assert_eq!(flags(&writer), inherited);
    assert_stall_fails_and_stays_failed(&mut output);
}

#[test]
fn a_draining_host_receives_every_byte() {
    let (mut reader, writer) = pipe();
    let mut output = BoundedOutput::duplicate(writer.as_fd(), STALL).unwrap();
    drop(writer);
    let expected: Vec<u8> = (0..1024 * 1024).map(|index| (index % 251) as u8).collect();
    let drain = std::thread::spawn(move || {
        let mut received = Vec::new();
        reader.read_to_end(&mut received).unwrap();
        received
    });
    output.write_all(&expected).unwrap();
    output.flush().unwrap();
    drop(output);
    assert_eq!(drain.join().unwrap(), expected);
}

#[test]
fn closed_reader_fails_delivery() {
    let (reader, writer) = pipe();
    let mut output = BoundedOutput::duplicate(writer.as_fd(), STALL).unwrap();
    drop(reader);
    unsafe { libc::signal(libc::SIGPIPE, libc::SIG_IGN) };
    assert_eq!(
        output.write(b"x").unwrap_err().kind(),
        ErrorKind::BrokenPipe
    );
    assert_eq!(
        output.write(b"x").unwrap_err().kind(),
        ErrorKind::BrokenPipe
    );
}

#[test]
fn regular_file_output_is_written_directly() {
    let root = tempfile::tempdir().unwrap();
    let file = std::fs::File::create(root.path().join("out")).unwrap();
    let mut output = BoundedOutput::duplicate(file.as_fd(), STALL).unwrap();
    output.write_all(b"event\n").unwrap();
    drop(output);
    assert_eq!(std::fs::read(root.path().join("out")).unwrap(), b"event\n");
}
