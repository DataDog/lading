//! Truncation flag propagation scenario.
//!
//! Tests whether the agent correctly propagates the framer's `IsTruncated`
//! flag through the `CombiningAggregator`'s `bucket.flush()` code path.
//!
//! # The Bug
//!
//! `bucket.flush()` only checks `b.contentLen >= b.maxContentSize` to decide
//! on adding the truncation marker — it never reads `msg.ParsingExtra.IsTruncated`
//! from upstream. Compare to `bucket.emitSingle()` which checks both.
//!
//! # The Exploit
//!
//! Without independent config knobs for the framer limit and the aggregator
//! limit (they're both tied to `max_message_size_bytes`), we exploit the
//! parser's byte-count reduction:
//!
//! 1. Framer reads raw bytes from file, cuts at `max_message_size_bytes`,
//!    marks `IsTruncated=true`
//! 2. The `encodedtext` parser decodes UTF-16-LE to UTF-8, halving the byte
//!    count for ASCII content
//! 3. Aggregator sees content well under `maxContentSize`, doesn't add
//!    marker, doesn't add tag
//!
//! The upstream `IsTruncated` flag is silently dropped.
//!
//! # Setup
//!
//! - `max_message_size_bytes: 1024` — both framer and aggregator limit
//! - One UTF-16-LE encoded ASCII line of ~700 chars (~1400 raw bytes)
//! - No leading `{` (skip `JSONDetector`), no timestamp shape
//!   (skip `TimestampDetector`) — labels as `aggregate`
//! - Aggregate label on empty bucket: `bucket.add` then `flushToCollected`
//!   calls `bucket.flush()` with `lineCount=1`
//!
//! # Variants
//!
//! - **Main test**: `auto_multi_line_detection: true` (Path C
//!   `CombiningAggregator`). Bug manifests. Output should be missing marker
//!   and `truncated:single_line` tag.
//! - **Control test**: `auto_multi_line_detection: false` (Path A
//!   `SingleLineHandler`). The marker and tag should both be present.

use crate::config::LogSourceConfig;
use crate::orchestrator::Action;
use crate::property::{self, Property};

/// Maximum message size for the test, used as both framer `contentLenLimit`
/// and aggregator `maxContentSize`.
pub const MAX_MESSAGE_BYTES: usize = 1024;

/// Encode an ASCII string as UTF-16-LE bytes (no BOM) plus the UTF-16-LE
/// newline terminator (`\n\0`).
#[must_use]
pub fn encode_utf16_le_line(s: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(s.len() * 2 + 2);
    for c in s.chars() {
        let mut buf = [0u16; 2];
        for unit in c.encode_utf16(&mut buf) {
            bytes.push((*unit & 0xff) as u8);
            bytes.push((*unit >> 8) as u8);
        }
    }
    // Newline in UTF-16-LE: 0x0A 0x00
    bytes.push(0x0A);
    bytes.push(0x00);
    bytes
}

/// Build the test input: a long plain-text ASCII line, UTF-16-LE encoded.
///
/// Length chosen so:
/// - Raw bytes exceed `MAX_MESSAGE_BYTES` (framer cuts and marks truncated)
/// - Post-parse UTF-8 byte count is under `MAX_MESSAGE_BYTES` (aggregator
///   doesn't see overflow)
///
/// Content avoids leading `{` (no JSON detection) and avoids timestamp
/// patterns (no timestamp detection) → labeled `aggregate`.
#[must_use]
pub fn build_test_line() -> String {
    // 700 ASCII chars → 1400 raw bytes UTF-16-LE (well over 1024)
    // → 700 bytes UTF-8 after parse (well under 1024)
    let prefix = "plain text line with no leading brace and no timestamp ";
    let filler = "x".repeat(700 - prefix.len());
    format!("{prefix}{filler}")
}

/// Build the action sequence: write one UTF-16-LE encoded line.
#[must_use]
pub fn build_actions() -> Vec<Action> {
    let line = build_test_line();
    let bytes = encode_utf16_le_line(&line);
    vec![Action::WriteRawBytes(bytes)]
}

/// Properties to assert if the bug is fixed (truncation flag propagated correctly).
///
/// On the buggy agent, these will FAIL — demonstrating the divergence.
#[must_use]
pub fn build_properties() -> Vec<Box<dyn Property>> {
    vec![
        Box::new(property::OutputHasTruncationMarker),
        Box::new(property::OutputHasTruncationTag {
            reason: "single_line".to_string(),
        }),
    ]
}

/// Log source config for the main test (Path C `CombiningAggregator`).
#[must_use]
pub fn log_source_config_main() -> LogSourceConfig {
    LogSourceConfig::Utf16LeEncoded {
        auto_multi_line: true,
    }
}

/// Log source config for the control test (Path A `SingleLineHandler`).
#[must_use]
pub fn log_source_config_control() -> LogSourceConfig {
    LogSourceConfig::Utf16LeEncoded {
        auto_multi_line: false,
    }
}
