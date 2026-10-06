//! Run potentially hazardous recovery inputs only in a disposable session.
//! Even without input validation, kill(0, ...) cannot reach the test runner.
#[cfg(target_os = "linux")]
fn main() {
    use agent_provider_execution::process::{terminate_process_group_actor, ProcessGroupActor};
    use std::io;
    use std::os::unix::process::CommandExt;
    use std::process::Command;
    use std::sync::atomic::{AtomicBool, Ordering};

    static SIGNALLED: AtomicBool = AtomicBool::new(false);
    extern "C" fn record_signal(_: i32) {
        SIGNALLED.store(true, Ordering::Relaxed);
    }

    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("--invalid-actor") {
        // Verify isolation before calling the public recovery primitive.
        let pid = unsafe { libc::getpid() };
        assert_eq!(unsafe { libc::getpgrp() }, pid);
        assert_eq!(unsafe { libc::getsid(0) }, pid);
        let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
        action.sa_sigaction = record_signal as *const () as usize;
        unsafe {
            assert_eq!(libc::sigemptyset(&mut action.sa_mask), 0);
            assert_eq!(
                libc::sigaction(libc::SIGTERM, &action, std::ptr::null_mut()),
                0
            );
        }
        let actor = ProcessGroupActor {
            process_group_id: args[2].parse().unwrap(),
            incarnation: "invalid-durable-record".into(),
        };
        let error = terminate_process_group_actor(&actor).expect_err("invalid actor must fail");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(!SIGNALLED.load(Ordering::Relaxed), "recovery sent SIGTERM");
        println!(
            "invalid actor {} rejected without SIGTERM",
            actor.process_group_id
        );
        return;
    }

    for id in [0, i32::MAX as u32 + 1, u32::MAX] {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command.arg("--invalid-actor").arg(id.to_string());
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let output = command.output().expect("spawn isolated recovery control");
        assert!(
            output.status.success(),
            "invalid actor {id}: status {}; stdout {}; stderr {}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        print!("{}", String::from_utf8_lossy(&output.stdout));
    }
    println!("test invalid_durable_actors_fail_without_signalling ... ok (3 inputs)");
}

#[cfg(not(target_os = "linux"))]
fn main() {}
