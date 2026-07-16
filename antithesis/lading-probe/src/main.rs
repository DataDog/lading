//! lading-probe: purpose-built target/oracle for the lading Antithesis rig.
//!
//! It binds one or more TCP ingest ports and drains whatever lading's generator
//! sends, counting every received byte **per ingest port** with `+=`, the
//! accounting the retired `ducks` got wrong. A separate TCP report port
//! exposes those per-port totals so a workload driver can read ground truth.
//! On connect, the probe writes one `"<port> <bytes>"` line per ingest port
//! and closes.
//!
//! Per-port counting matters because the rig runs more than one generator at a
//! time. The always-on compose SUT drives one ingest port continuously, so a
//! driver that spawned its own run and read a single shared total could not
//! tell its own delivery apart from that background traffic. A driver aims its
//! run at a dedicated port and reads only that port's total.
//!
//! This is the minimal oracle the baseline property `rig-runs-lading-cleanly`
//! needs. Per-protocol counters, per-arrival timestamps, and adversarial
//! receiver behaviors extend this as later properties require them.

// Keep the instrumentation crate linked. It is reached through the coverage
// runtime, not any path we call directly.
#[cfg(feature = "antithesis")]
use antithesis_instrumentation as _;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

/// One ingest port and its running received-byte total.
#[derive(Clone)]
struct Ingest {
    port: u16,
    received: Arc<AtomicU64>,
}

/// Read a `u16` port from an environment variable, falling back to `default`.
fn env_port(key: &str, default: u16) -> u16 {
    match std::env::var(key) {
        Ok(raw) => raw.trim().parse().unwrap_or(default),
        Err(_) => default,
    }
}

/// Drain a single accepted ingest connection until the peer closes it, adding
/// every received byte to this port's total. We only consume so the sender is
/// never backpressured by an unread socket.
fn drain(mut stream: TcpStream, received: &AtomicU64) {
    let mut buf = [0u8; 64 * 1024];
    loop {
        match stream.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                received.fetch_add(n as u64, Ordering::Relaxed);
            }
        }
    }
}

/// Accept loop for one ingest port. Each connection drains on its own thread.
fn ingest_listener(listener: TcpListener, received: Arc<AtomicU64>) {
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let received = Arc::clone(&received);
                thread::spawn(move || drain(stream, &received));
            }
            Err(e) => eprintln!("lading-probe ingest accept error: {e}"),
        }
    }
}

/// Serve a single report connection. Write one `"<port> <bytes>"` line per
/// ingest port, then drop the connection.
fn serve_report(mut stream: TcpStream, ingests: &[Ingest]) {
    let mut body = String::new();
    for ingest in ingests {
        let total = ingest.received.load(Ordering::Relaxed);
        body.push_str(&format!("{} {}\n", ingest.port, total));
    }
    // Best-effort. A driver that hangs up early is not the probe's problem.
    let _ = stream.write_all(body.as_bytes());
    let _ = stream.flush();
}

/// Accept loop for the report port. Runs on its own thread so ingest stays hot.
fn report_listener(listener: TcpListener, ingests: Vec<Ingest>) {
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let ingests = ingests.clone();
                thread::spawn(move || serve_report(stream, &ingests));
            }
            Err(e) => eprintln!("lading-probe report accept error: {e}"),
        }
    }
}

fn main() {
    // Initialize the Antithesis SDK as early as possible. This is a no-op
    // without the `antithesis` feature.
    #[cfg(feature = "antithesis")]
    antithesis_sdk::antithesis_init();

    // Two ingest ports: the shared/SUT port the always-on compose generator
    // drives, and a dedicated port a driver aims a one-off run at so its
    // delivery is measurable free of that background traffic.
    let shared_port = env_port("PROBE_TCP_PORT", 4000);
    let baseline_port = env_port("PROBE_BASELINE_PORT", 4002);
    let report_port = env_port("PROBE_REPORT_PORT", 4001);

    let ingests = vec![
        Ingest {
            port: shared_port,
            received: Arc::new(AtomicU64::new(0)),
        },
        Ingest {
            port: baseline_port,
            received: Arc::new(AtomicU64::new(0)),
        },
    ];

    // Bind every ingest port and the report port up front so a bind failure is
    // loud at startup rather than a silently missing counter.
    let ingest_listeners: Vec<(TcpListener, Ingest)> = ingests
        .iter()
        .map(|ingest| {
            let listener = TcpListener::bind(("0.0.0.0", ingest.port)).unwrap_or_else(|e| {
                panic!("lading-probe failed to bind ingest 0.0.0.0:{}: {e}", ingest.port)
            });
            (listener, ingest.clone())
        })
        .collect();
    let report = TcpListener::bind(("0.0.0.0", report_port))
        .unwrap_or_else(|e| panic!("lading-probe failed to bind report 0.0.0.0:{report_port}: {e}"));
    println!(
        "lading-probe listening: ingest {shared_port} (shared), {baseline_port} (baseline), report {report_port}"
    );

    // Bootstrap Antithesis property. Proves the SDK path is wired into the
    // probe. Inline constant literal so assertion cataloging can discover it.
    #[cfg(feature = "antithesis")]
    antithesis_sdk::assert_reachable!("lading-probe startup path executed");

    thread::spawn(move || report_listener(report, ingests));

    // Run every ingest port but the last on its own thread. Keep the last on
    // the main thread so the process stays alive on the accept loop.
    let mut listeners = ingest_listeners.into_iter();
    let last = listeners.next_back();
    for (listener, ingest) in listeners {
        thread::spawn(move || ingest_listener(listener, ingest.received));
    }
    if let Some((listener, ingest)) = last {
        ingest_listener(listener, ingest.received);
    }
}
