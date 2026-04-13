//! Blackhole capture parsing.
//!
//! Reads the blackhole capture JSONL file, parses each payload as a DD logs
//! agent JSON array, and extracts the `message` field from each entry.

use std::{
    hash::{Hash, Hasher},
    io::{BufRead, BufReader},
    path::Path,
};

use serde::Deserialize;

use crate::Error;
use crate::context::OutputLine;

/// A parsed blackhole capture record.
#[derive(Debug, Deserialize)]
pub struct BlackholeEvent {
    /// Milliseconds since lading epoch.
    pub relative_ms: u64,
    /// Size of the compressed HTTP body.
    pub compressed_bytes: u64,
    /// The decoded payload (DD logs agent JSON).
    pub payload: String,
}

/// A single log entry in the DD logs agent JSON payload.
#[derive(Debug, Deserialize)]
struct LogEntry {
    message: String,
    // Other fields exist (status, timestamp, hostname, service, ddsource,
    // ddtags) but are not needed for matching.
}

/// Hash a message string for matching against input lines.
fn hash_message(msg: &str) -> u64 {
    let mut hasher = rustc_hash::FxHasher::default();
    msg.as_bytes().hash(&mut hasher);
    hasher.finish()
}

/// Parse the blackhole capture JSONL and extract output lines.
///
/// Returns `(output_lines, all_blackhole_events)`.
///
/// # Errors
///
/// Returns an error if the file cannot be read or parsed.
pub fn parse(
    blackhole_capture_path: &Path,
) -> Result<(Vec<OutputLine>, Vec<BlackholeEvent>), Error> {
    let file = std::fs::File::open(blackhole_capture_path)?;
    let reader = BufReader::new(file);

    let mut output_lines: Vec<OutputLine> = Vec::new();
    let mut events: Vec<BlackholeEvent> = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        let event: BlackholeEvent = serde_json::from_str(&line)?;

        // Parse the payload as a JSON array of log entries.
        // The agent may send a single object or an array; handle both.
        if let Ok(entries) = serde_json::from_str::<Vec<LogEntry>>(&event.payload) {
            for entry in entries {
                let h = hash_message(&entry.message);
                output_lines.push(OutputLine {
                    hash: h,
                    message: entry.message,
                    relative_ms: event.relative_ms,
                });
            }
        }
        // If the payload doesn't parse as a log array, skip it silently.
        // This handles non-log payloads (e.g., from the exerciser scripts).

        events.push(event);
    }

    Ok((output_lines, events))
}
