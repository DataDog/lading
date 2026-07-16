//! Antithesis `serial_driver_` for the `rig-runs-lading-cleanly` property,
//! catalog Category H, P0 baseline control.
//!
//! This is the control that makes every other verdict interpretable. With no
//! faults, a lading run must reach a clean, productive completion. If it does
//! not, a green board means "possibly nothing ran", not "lading works". So the
//! check is deliberately boring. It asserts the happy path end to end:
//!
//! 1. a lading process exits 0,
//! 2. its capture file parses and is non-empty, and
//! 3. the probe's received-byte total rose during the run, meaning bytes
//!    reached the wire and the oracle caught them.
//!
//! All three are read here in the harness and combined into one client-side
//! `Sometimes(clean_run_completed)`. `Sometimes` because it need only become
//! true at least once under no-fault conditions. A faulted timeline may
//! legitimately never reach it. The run is wrapped in an `ANTITHESIS_STOP_FAULTS`
//! quiet window so the clean path is reachable and a failure points at the rig,
//! not at injected chaos.
//!
//! Like the divide driver, this spawns the real instrumented `lading` binary,
//! because the always-on compose SUT never exits and so cannot supply an exit
//! code. The assertion lives here, not in lading's source.

// Keep the instrumentation crate linked. It provides the sancov coverage
// runtime the `antithesis` build's rustflags reference, reached through that
// runtime rather than any path we call directly.
#[cfg(feature = "antithesis")]
use antithesis_instrumentation as _;

use std::io::Read;
use std::net::{TcpStream, ToSocketAddrs};
use std::path::Path;
use std::process::Command;
use std::thread::sleep;
use std::time::{Duration, Instant};

use antithesis_sdk::prelude::*;
use antithesis_sdk::serde_json::{json, Value};

/// The instrumented `lading` binary, copied into the client image by
/// `antithesis/Dockerfile`.
const LADING_BIN: &str = "/usr/bin/lading";
/// The probe's **dedicated baseline** ingest port. The always-on compose SUT
/// drives the shared ingest port, 4000. Aiming this run at a separate port
/// keeps its received-byte total free of that background traffic, so the
/// measured delta reflects only this run.
const BASELINE_INGEST_PORT: u16 = 4002;
/// The probe's dedicated baseline ingest port, where the spawned run sends.
const PROBE_INGEST_ADDR: &str = "lading-probe:4002";
/// The probe's report address. Connecting reads back per-port received totals.
const PROBE_REPORT_ADDR: &str = "lading-probe:4001";
/// How long the spawned lading run generates. Long enough to cross a capture
/// flush interval and put bytes on the wire. Short so re-runs stay cheap.
const RUN_SECONDS: u32 = 5;
/// Hard ceiling on the spawned run so a shutdown hang cannot wedge this command.
const RUN_TIMEOUT: Duration = Duration::from_secs(60);
/// Fault-quiet window requested around the run, sized to at least the run's
/// hard ceiling plus margin for the two probe reads, so faults cannot resume
/// mid-run.
const QUIET_SECONDS: u32 = 90;

fn main() {
    antithesis_init();

    // Ask Antithesis to pause faults for the run so the clean path is reachable
    // and any failure is attributable to the rig, not injected chaos. A no-op
    // outside an Antithesis environment, for example local runs or
    // `snouty validate`.
    request_fault_quiet(QUIET_SECONDS);

    let probe_before = read_probe_total();

    let pid = std::process::id();
    let config_path = std::env::temp_dir().join(format!("baseline-{pid}.yaml"));
    let capture_path = std::env::temp_dir().join(format!("baseline-{pid}.capture"));

    if let Err(e) = std::fs::write(&config_path, lading_config()) {
        eprintln!("failed writing lading config {}: {e}", config_path.display());
        std::process::exit(1);
    }

    let exit_ok = match run_lading(&config_path, &capture_path) {
        RunOutcome::Exited(status) => status.code() == Some(0),
        RunOutcome::TimedOut => false,
        RunOutcome::SpawnFailed(e) => {
            // Could not launch the SUT at all, a broken image rather than a
            // lading behavior. Fail loudly so the harness gets fixed.
            eprintln!("failed to spawn {LADING_BIN}: {e}");
            let _ = std::fs::remove_file(&config_path);
            std::process::exit(1);
        }
    };

    let capture_records = count_capture_records(&capture_path);
    let capture_ok = capture_records > 0;

    let probe_after = read_probe_total();
    let probe_delta = match (probe_before, probe_after) {
        (Some(before), Some(after)) => Some(after.saturating_sub(before)),
        _ => None,
    };
    let probe_ok = probe_delta.is_some_and(|d| d > 0);

    let _ = std::fs::remove_file(&config_path);
    let _ = std::fs::remove_file(&capture_path);

    let clean = exit_ok && capture_ok && probe_ok;

    // The baseline: a no-fault run exercised lading end to end. Client side,
    // so a failure is attributed to the harness.
    assert_sometimes!(
        clean,
        "rig runs lading cleanly under no faults",
        &json!({
            "exit_zero": exit_ok,
            "capture_records": capture_records,
            "probe_bytes_before": probe_before,
            "probe_bytes_after": probe_after,
            "probe_bytes_delta": probe_delta,
        })
    );
}

/// Best-effort request for a fault-quiet window via the `ANTITHESIS_STOP_FAULTS`
/// binary Antithesis injects. Silent no-op when the variable is unset.
fn request_fault_quiet(seconds: u32) {
    if let Ok(bin) = std::env::var("ANTITHESIS_STOP_FAULTS") {
        if !bin.is_empty() {
            let _ = Command::new(bin).arg(seconds.to_string()).status();
        }
    }
}

/// A minimal lading config: one TCP generator aimed at the probe at a healthy
/// rate, no blackhole, telemetry supplied on the command line. The rate
/// comfortably exceeds the block size at `parallel_connections = 1`, so the run
/// delivers rather than starving. That starvation case belongs to
/// `divide-preserves-aggregate-rate`.
fn lading_config() -> String {
    format!(
        "generator:\n  - tcp:\n      \
         seed: [2, 3, 5, 7, 11, 13, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131, 137]\n      \
         addr: \"{addr}\"\n      \
         variant: \"syslog5424\"\n      \
         bytes_per_second: \"1 MiB\"\n      \
         maximum_block_size: \"32 KiB\"\n      \
         maximum_prebuild_cache_size_bytes: \"16 MiB\"\n      \
         parallel_connections: 1\n",
        addr = PROBE_INGEST_ADDR,
    )
}

/// Result of one bounded `lading` run.
enum RunOutcome {
    Exited(std::process::ExitStatus),
    TimedOut,
    SpawnFailed(std::io::Error),
}

/// Launch `lading` in no-target mode against the probe, writing a JSONL capture,
/// and wait for it bounded by [`RUN_TIMEOUT`]. Polls rather than blocking so a
/// hung child cannot wedge this command.
fn run_lading(config_path: &Path, capture_path: &Path) -> RunOutcome {
    let mut child = match Command::new(LADING_BIN)
        .arg("--no-target")
        .arg("--config-path")
        .arg(config_path)
        .arg("--capture-path")
        .arg(capture_path)
        .arg("--capture-format")
        .arg("jsonl")
        .arg("--experiment-duration-seconds")
        .arg(RUN_SECONDS.to_string())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return RunOutcome::SpawnFailed(e),
    };

    let deadline = Instant::now() + RUN_TIMEOUT;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return RunOutcome::Exited(status),
            Ok(None) => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return RunOutcome::TimedOut;
                }
                sleep(Duration::from_millis(250));
            }
            Err(e) => {
                let _ = child.kill();
                let _ = child.wait();
                return RunOutcome::SpawnFailed(e);
            }
        }
    }
}

/// Count parseable, non-empty JSONL records in the capture file. Zero when the
/// file is missing, empty, or unreadable. Any of those fails the baseline.
fn count_capture_records(capture_path: &Path) -> usize {
    let Ok(contents) = std::fs::read_to_string(capture_path) else {
        return 0;
    };
    contents
        .lines()
        .filter(|line| !line.trim().is_empty())
        .filter(|line| antithesis_sdk::serde_json::from_str::<Value>(line).is_ok())
        .count()
}

/// Read the probe's received-byte total **for the dedicated baseline port**
/// from its report port. The report is one `"<port> <bytes>"` line per ingest
/// port. We return the bytes for [`BASELINE_INGEST_PORT`]. `None` when the
/// probe is unreachable, the reply does not parse, or the baseline port is
/// absent. Any of those fails the baseline rather than passing on background
/// traffic.
fn read_probe_total() -> Option<u64> {
    let addr = PROBE_REPORT_ADDR.to_socket_addrs().ok()?.next()?;
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    let mut buf = String::new();
    stream.read_to_string(&mut buf).ok()?;
    buf.lines().find_map(|line| {
        let (port, bytes) = line.split_once(' ')?;
        (port.trim().parse::<u16>().ok()? == BASELINE_INGEST_PORT)
            .then(|| bytes.trim().parse::<u64>().ok())?
    })
}
