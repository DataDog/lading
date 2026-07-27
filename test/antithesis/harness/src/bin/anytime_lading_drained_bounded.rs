//! Antithesis `anytime_` command: assert lading drains promptly on graceful
//! (experiment-timer) shutdown even when its generator points at an unreachable
//! destination.
//!
//! The lading (system under test) container runs lading under a wall-clock
//! `timeout` watchdog and writes lading's exit code to a sentinel file on the
//! shared volume. This checker runs in the (never-faulted) workload container
//! and fires whenever Antithesis chooses. It reads that exit code:
//!   * rc == 0   -> lading drained within the watchdog bound (the good outcome).
//!   * rc == 124 -> the `timeout` watchdog killed lading before it drained. That
//!     would indicate the shutdown drain hung rather than completing promptly,
//!     for example if a generator stuck against the unreachable destination
//!     blocked the drain instead of being abandoned at runtime teardown.
//!   * any other rc -> some other failure, e.g. a lading crash.
//!
//! It always exits 0; findings are reported as named assertions.

use std::fs;

/// File the lading entrypoint writes lading's exit code into. Overridable via
/// `LADING_EXIT_PATH`.
const DEFAULT_EXIT_PATH: &str = "/shared/lading_exit";

fn main() {
    lading_antithesis::init();

    let path =
        std::env::var("LADING_EXIT_PATH").unwrap_or_else(|_| DEFAULT_EXIT_PATH.to_string());

    let Ok(content) = fs::read_to_string(&path) else {
        // The exit-code sentinel is not present yet this tick: lading has not
        // finished (or been killed) yet. Nothing to check.
        return;
    };

    let trimmed = content.trim();
    let Ok(rc) = trimmed.parse::<i32>() else {
        // A non-integer sentinel means the entrypoint has not written a
        // well-formed exit code yet; wait for a later tick.
        return;
    };

    // Non-vacuity floor: prove the checker actually observed a recorded exit at
    // least once across the run, so the always-invariant below is not passing
    // vacuously on an absent file.
    lading_antithesis::sometimes!(
        !trimmed.is_empty(),
        "lading recorded a bounded exit",
        { "rc": rc }
    );

    // The invariant under test: on graceful (experiment-timer) shutdown with an
    // unreachable destination, lading drains within the watchdog bound
    // (rc == 0). A watchdog kill (rc == 124) would mean the shutdown drain hung
    // rather than completing promptly.
    lading_antithesis::always!(
        rc == 0,
        "lading drains within bound on graceful shutdown with an unreachable destination",
        { "rc": rc }
    );

    lading_antithesis::reachable!(
        "bounded-drain checker validated a recorded lading exit",
        { "rc": rc }
    );
}
