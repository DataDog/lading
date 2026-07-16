//! workload-client: the Antithesis test driver for the lading rig.
//!
//! This is the **setup skeleton only**. Docker Compose orders this container to
//! start after `lading` and `lading-probe` report healthy, so by the time we
//! run the system is up. We then emit the `setup_complete` lifecycle event,
//! which tells Antithesis it may begin running test commands, and idle forever
//! so the container stays alive to host them.
//!
//! The real driver logic is deferred to `antithesis-workload`: selecting
//! scenarios, drawing SDK randomness, triggering faults, reading the capture
//! file and the probe's report, and emitting the property-catalog assertions.
//! Its test commands live under `/opt/antithesis/test/v1/`, baked into this
//! image from `antithesis/test/v1/`, so adding commands means rebuilding.

// Keep the instrumentation crate linked. It is reached through the coverage
// runtime, not any path we call directly.
#[cfg(feature = "antithesis")]
use antithesis_instrumentation as _;

use std::io::Write;
use std::time::Duration;

/// Emit the `setup_complete` event to `$ANTITHESIS_OUTPUT_DIR/sdk.jsonl`.
///
/// This mirrors `antithesis/setup-complete.sh` and works whether or not the SDK
/// is linked, so local `snouty validate` reliably observes it. Only emit once
/// the system under test is actually ready for testing.
fn emit_setup_complete() {
    let Ok(dir) = std::env::var("ANTITHESIS_OUTPUT_DIR") else {
        println!("ANTITHESIS_OUTPUT_DIR is unset; not emitting setup_complete");
        return;
    };
    let path = format!("{dir}/sdk.jsonl");
    let record = r#"{"antithesis_setup":{"status":"complete","details":{"message":"lading rig ready"}}}"#;
    match std::fs::OpenOptions::new().create(true).append(true).open(&path) {
        Ok(mut f) => {
            if let Err(e) = writeln!(f, "{record}") {
                eprintln!("workload-client failed writing setup_complete to {path}: {e}");
            } else {
                println!("workload-client emitted setup_complete to {path}");
            }
        }
        Err(e) => eprintln!("workload-client failed opening {path}: {e}"),
    }
}

fn main() {
    // Initialize the Antithesis SDK as early as possible. This is a no-op
    // without the `antithesis` feature.
    #[cfg(feature = "antithesis")]
    antithesis_sdk::antithesis_init();

    // Bootstrap Antithesis property. Proves the SDK path is wired into the
    // client. Inline constant literal so assertion cataloging can discover it.
    #[cfg(feature = "antithesis")]
    antithesis_sdk::assert_reachable!("workload-client startup path executed");

    emit_setup_complete();

    // Idle so the container stays up for Antithesis test commands.
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}
