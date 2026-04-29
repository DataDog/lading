//! Log format generation with embedded UUID markers.
//!
//! Each [`LogFormat`] variant produces format-valid log lines with a UUID
//! embedded in a position that survives the agent's processing (truncation,
//! multiline aggregation, etc.).
//!
//! These are lightweight implementations that do NOT depend on `lading_payload`.
//! They produce content sufficient to trigger the agent's format-specific
//! behavior (JSON detection, timestamp detection, syslog parsing) without
//! needing full spec compliance.

use proptest::prelude::*;

/// Marker prefix used to identify proptest-generated log lines.
pub const PROPTEST_MARKER: &str = "PROPTEST";

/// Marker prefix for continuation lines in multiline scenarios.
pub const CONTINUATION_MARKER: &str = "CONT";

/// Supported log formats for test generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    /// Plain text: `[PROPTEST:<uuid>] <content>`
    PlainText,
    /// JSON object: `{"proptest_id":"<uuid>","message":"<content>","level":"info",...}`
    Json,
    /// RFC 5424 syslog: `<134>1 <timestamp> host app 1234 - - [PROPTEST:<uuid>] <content>`
    Syslog5424,
    /// Apache Combined Log: `192.168.1.1 - - [timestamp] "GET /<uuid>/path HTTP/1.1" 200 <len>`
    ApacheCommon,
    /// Timestamp-prefixed: `2024-01-15T10:30:00.000Z [PROPTEST:<uuid>] <content>`
    ///
    /// Primary format for multiline testing as it triggers the agent's datetime
    /// detection.
    TimestampPrefixed,
}

impl LogFormat {
    /// Format a single log line with the given UUID and content.
    #[must_use]
    pub fn format_line(&self, id: &str, content: &str) -> String {
        match self {
            Self::PlainText => {
                format!("[{PROPTEST_MARKER}:{id}] {content}")
            }
            Self::Json => {
                // Escape content for JSON safety.
                let escaped = content.replace('\\', "\\\\").replace('"', "\\\"");
                format!(
                    r#"{{"proptest_id":"{id}","message":"{escaped}","level":"info","logger":"proptest"}}"#
                )
            }
            Self::Syslog5424 => {
                format!(
                    "<134>1 2024-01-15T10:30:00.000000Z proptest.host proptest-app 1234 ID001 - [{PROPTEST_MARKER}:{id}] {content}"
                )
            }
            Self::ApacheCommon => {
                let len = content.len();
                format!(
                    "192.168.1.42 - proptest [15/Jan/2024:10:30:00 +0000] \"GET /{id}/{} HTTP/1.1\" 200 {len}",
                    content.replace('"', "%22").replace(' ', "%20"),
                )
            }
            Self::TimestampPrefixed => {
                format!("2024-01-15T10:30:00.000Z [{PROPTEST_MARKER}:{id}] {content}")
            }
        }
    }

    /// Format a continuation line for multiline scenarios.
    ///
    /// Continuation lines are indented and lack the leading structure (no
    /// timestamp, no syslog header, etc.) so the agent aggregates them with
    /// the preceding header line.
    #[must_use]
    pub fn format_continuation(&self, header_id: &str, seq: usize, content: &str) -> String {
        match self {
            Self::Json => {
                // For JSON multiline, continuation is just more content on the next line
                // that doesn't start with `{`. The agent's JSON aggregator will merge
                // incomplete JSON objects.
                format!("    [{CONTINUATION_MARKER}:{header_id}:{seq}] {content}")
            }
            _ => {
                // For all other formats, a continuation line starts with whitespace
                // (no timestamp/structure prefix), signaling it belongs to the
                // previous entry.
                format!("    [{CONTINUATION_MARKER}:{header_id}:{seq}] {content}")
            }
        }
    }

    /// Extract the proptest UUID from an agent output message.
    ///
    /// The agent may have modified the line (truncation, aggregation), but the
    /// UUID marker should still be present if the line was delivered.
    #[must_use]
    pub fn extract_id(message: &str) -> Option<&str> {
        // Look for [PROPTEST:<uuid>] pattern
        if let Some(start) = message.find(&format!("[{PROPTEST_MARKER}:")) {
            let after_prefix = start + PROPTEST_MARKER.len() + 2; // skip "[PROPTEST:"
            if let Some(end) = message[after_prefix..].find(']') {
                return Some(&message[after_prefix..after_prefix + end]);
            }
        }
        // Look for proptest_id in JSON
        if let Some(start) = message.find("\"proptest_id\":\"") {
            let after_prefix = start + "\"proptest_id\":\"".len();
            if let Some(end) = message[after_prefix..].find('"') {
                return Some(&message[after_prefix..after_prefix + end]);
            }
        }
        // Look for UUID in Apache path: GET /<uuid>/
        if let Some(start) = message.find("GET /") {
            let after_prefix = start + "GET /".len();
            if let Some(end) = message[after_prefix..].find('/') {
                let candidate = &message[after_prefix..after_prefix + end];
                if candidate.len() == 36 && candidate.contains('-') {
                    return Some(candidate);
                }
            }
        }
        None
    }

    /// Extract continuation markers from an agent output message.
    ///
    /// Returns a list of `(header_id, sequence_number)` pairs found.
    #[must_use]
    pub fn extract_continuations(message: &str) -> Vec<(&str, usize)> {
        let mut results = Vec::new();
        let marker = format!("[{CONTINUATION_MARKER}:");
        let mut search_from = 0;
        while let Some(start) = message[search_from..].find(&marker) {
            let abs_start = search_from + start + marker.len();
            if let Some(end) = message[abs_start..].find(']') {
                let inner = &message[abs_start..abs_start + end];
                if let Some(colon) = inner.find(':') {
                    let header_id = &inner[..colon];
                    if let Ok(seq) = inner[colon + 1..].parse::<usize>() {
                        results.push((header_id, seq));
                    }
                }
            }
            search_from = abs_start;
        }
        results
    }
}

/// Proptest strategy that generates a [`LogFormat`] variant.
pub fn log_format_strategy() -> impl Strategy<Value = LogFormat> {
    prop_oneof![
        Just(LogFormat::PlainText),
        Just(LogFormat::Json),
        Just(LogFormat::Syslog5424),
        Just(LogFormat::ApacheCommon),
        Just(LogFormat::TimestampPrefixed),
    ]
}

/// Proptest strategy for log formats suitable for multiline testing.
///
/// Only includes formats where the agent's multiline detection can
/// distinguish header lines from continuation lines.
pub fn multiline_format_strategy() -> impl Strategy<Value = LogFormat> {
    prop_oneof![
        Just(LogFormat::TimestampPrefixed),
        Just(LogFormat::Json),
        Just(LogFormat::PlainText),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_text_roundtrip() {
        let id = "550e8400-e29b-41d4-a716-446655440000";
        let line = LogFormat::PlainText.format_line(id, "hello world");
        assert_eq!(
            line,
            "[PROPTEST:550e8400-e29b-41d4-a716-446655440000] hello world"
        );
        assert_eq!(LogFormat::extract_id(&line), Some(id));
    }

    #[test]
    fn json_roundtrip() {
        let id = "550e8400-e29b-41d4-a716-446655440000";
        let line = LogFormat::Json.format_line(id, "hello world");
        assert!(line.starts_with('{'));
        assert!(line.contains("proptest_id"));
        assert_eq!(LogFormat::extract_id(&line), Some(id));
    }

    #[test]
    fn syslog_roundtrip() {
        let id = "550e8400-e29b-41d4-a716-446655440000";
        let line = LogFormat::Syslog5424.format_line(id, "test message");
        assert!(line.starts_with("<134>"));
        assert_eq!(LogFormat::extract_id(&line), Some(id));
    }

    #[test]
    fn apache_roundtrip() {
        let id = "550e8400-e29b-41d4-a716-446655440000";
        let line = LogFormat::ApacheCommon.format_line(id, "test");
        assert!(line.starts_with("192.168.1.42"));
        assert_eq!(LogFormat::extract_id(&line), Some(id));
    }

    #[test]
    fn timestamp_prefixed_roundtrip() {
        let id = "550e8400-e29b-41d4-a716-446655440000";
        let line = LogFormat::TimestampPrefixed.format_line(id, "test message");
        assert!(line.starts_with("2024-"));
        assert_eq!(LogFormat::extract_id(&line), Some(id));
    }

    #[test]
    fn continuation_extraction() {
        let header_id = "abc123";
        let line = LogFormat::PlainText.format_continuation(header_id, 2, "continuation text");
        let continuations = LogFormat::extract_continuations(&line);
        assert_eq!(continuations.len(), 1);
        assert_eq!(continuations[0], ("abc123", 2));
    }

    #[test]
    fn extract_id_from_truncated_line() {
        // UUID is near the start, so it should survive truncation
        let id = "550e8400-e29b-41d4-a716-446655440000";
        let line = LogFormat::PlainText.format_line(id, "very long content");
        // Simulate truncation: take only the first 80 chars
        let truncated = &line[..line.len().min(80)];
        assert_eq!(LogFormat::extract_id(truncated), Some(id));
    }
}
