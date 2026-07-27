//! Antithesis `anytime_` command: assert lading's capture is crash-consistent,
//! for every capture format lading can emit (JSONL, Parquet, or Multi = both).
//!
//! node faults hard-kill lading (SIGKILL), so the whole lading container dies and
//! only artifacts on the shared `capture` volume survive. This checker runs in the
//! (never-faulted) workload container and fires whenever Antithesis chooses,
//! including right after a kill/restart. It scans the capture directory and
//! validates each file by its format:
//!   * JSONL: a valid parseable prefix (a torn final line -- the interrupted
//!     write -- is tolerated; a broken earlier line or a broken invariant is not).
//!   * Parquet: footer-terminated, so a hard kill leaves it unreadable -- that is
//!     expected, not corruption; but a *readable* Parquet must be consistent.
//!   * Multi: both of the above, on `<base>.jsonl` and `<base>.parquet`.
//!
//! It always exits 0; findings are reported as named assertions.

use std::fs;

/// Directory lading writes capture files into. Overridable via `CAPTURE_DIR`.
const DEFAULT_CAPTURE_DIR: &str = "/capture";
/// Non-vacuity floor: prove the checker saw a substantive capture at least once
/// across the run, so the always-invariants are not passing on empty files.
const MIN_RECORDS: usize = 10;

fn main() {
    lading_antithesis::init();

    let dir = std::env::var("CAPTURE_DIR").unwrap_or_else(|_| DEFAULT_CAPTURE_DIR.to_string());
    let Ok(entries) = fs::read_dir(&dir) else {
        // Capture directory not present yet this tick; nothing to check.
        return;
    };

    let mut checked_any = false;
    for entry in entries.flatten() {
        let path = entry.path();
        match path.extension().and_then(|e| e.to_str()) {
            Some("jsonl") => {
                let Ok(content) = fs::read_to_string(&path) else {
                    continue;
                };
                let r = harness::capture::check_consistency(&content);
                checked_any = true;
                lading_antithesis::always!(
                    r.torn_before_final == 0,
                    "jsonl capture has no torn record before the final line",
                    { "parsed": r.parsed, "torn_before_final": r.torn_before_final }
                );
                lading_antithesis::always!(
                    r.invariants_hold,
                    "jsonl capture fetch_index and per-series time stay monotonic",
                    { "fetch_index_errors": r.fetch_index_errors, "per_series_errors": r.per_series_errors }
                );
                lading_antithesis::sometimes!(
                    r.parsed >= MIN_RECORDS,
                    "jsonl capture accumulated records across the run",
                    { "parsed": r.parsed }
                );
            }
            Some("parquet") => {
                let r = harness::capture::check_parquet(&path);
                checked_any = true;
                // A readable Parquet must be internally consistent. An unreadable
                // one is the expected result of a hard kill (footer only on clean
                // close), so it is not a violation.
                lading_antithesis::always!(
                    !r.readable || r.invariants_hold,
                    "readable parquet capture is internally consistent",
                    { "readable": r.readable, "records": r.records }
                );
                lading_antithesis::sometimes!(
                    r.readable && r.records >= MIN_RECORDS,
                    "parquet capture finalized and readable across the run",
                    { "readable": r.readable, "records": r.records }
                );
            }
            _ => {}
        }
    }

    if checked_any {
        lading_antithesis::reachable!("capture consistency checker validated a capture file");
    }
}
