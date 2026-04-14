//! Blackhole capture parsing.
//!
//! Reads the blackhole capture JSONL file, parses each payload as a DD logs
//! agent JSON array, and extracts the `message` field from each entry.

use std::{
    io::{BufRead, BufReader},
    path::Path,
};

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::Error;
use crate::context::{ContentHash, OutputLine};

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
}

/// SHA-256 hash of a string's bytes.
fn sha256(data: &[u8]) -> ContentHash {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
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

        if let Ok(entries) = serde_json::from_str::<Vec<LogEntry>>(&event.payload) {
            for entry in entries {
                let h = sha256(entry.message.as_bytes());
                output_lines.push(OutputLine {
                    hash: h,
                    message: entry.message,
                    relative_ms: event.relative_ms,
                });
            }
        }

        events.push(event);
    }

    Ok((output_lines, events))
}
