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
    ///
    /// Uses a fixed timestamp. For multiline scenarios where each header needs
    /// a distinct timestamp, use [`format_multiline_header`] instead.
    #[must_use]
    pub fn format_line(&self, id: &str, content: &str) -> String {
        self.format_line_with_index(id, 0, content)
    }

    /// Format a log line with a timestamp that varies by `entry_index`.
    ///
    /// For formats with timestamps (`TimestampPrefixed`, `Syslog5424`,
    /// `ApacheCommon`), the seconds field increments by `entry_index` so the
    /// agent's auto multiline detector sees each header as a distinct
    /// `startGroup` event.
    #[must_use]
    pub fn format_line_with_index(&self, id: &str, entry_index: usize, content: &str) -> String {
        let secs = entry_index % 60;
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
                    "<134>1 2024-01-15T10:30:{secs:02}.000000Z proptest.host proptest-app 1234 ID001 - [{PROPTEST_MARKER}:{id}] {content}"
                )
            }
            Self::ApacheCommon => {
                let len = content.len();
                format!(
                    "192.168.1.42 - proptest [15/Jan/2024:10:30:{secs:02} +0000] \"GET /{id}/{} HTTP/1.1\" 200 {len}",
                    content.replace('"', "%22").replace(' ', "%20"),
                )
            }
            Self::TimestampPrefixed => {
                format!("2024-01-15T10:30:{secs:02}.000Z [{PROPTEST_MARKER}:{id}] {content}")
            }
        }
    }

    /// Format a continuation line for multiline scenarios.
    ///
    /// Continuation lines lack any leading structure (no timestamp, no syslog
    /// header, no JSON opening brace) so the agent's auto multiline detection
    /// labels them `aggregate` and appends them to the current buffer.
    ///
    /// No artificial indentation — the agent doesn't use whitespace for
    /// detection, and we don't want to make aggregation easier than it would
    /// be with real-world input.
    #[must_use]
    pub fn format_continuation(&self, header_id: &str, seq: usize, content: &str) -> String {
        // All formats use the same continuation shape: just the marker + content.
        // No indentation, no format-specific structure.
        let _ = self; // format doesn't affect continuation lines
        format!("[{CONTINUATION_MARKER}:{header_id}:{seq}] {content}")
    }

    /// Extract the first proptest UUID from an agent output message.
    ///
    /// The agent may have modified the line (truncation, aggregation), but the
    /// UUID marker should still be present if the line was delivered.
    #[must_use]
    pub fn extract_id(message: &str) -> Option<&str> {
        Self::extract_all_ids(message).into_iter().next()
    }

    /// Extract all proptest UUIDs from an agent output message.
    ///
    /// An aggregated message may contain multiple `[PROPTEST:<uuid>]` markers
    /// (e.g., when a plain text line is merged into a preceding entry's
    /// buffer). This returns all of them.
    #[must_use]
    pub fn extract_all_ids(message: &str) -> Vec<&str> {
        let mut ids = Vec::new();
        let marker = format!("[{PROPTEST_MARKER}:");

        // Find all [PROPTEST:<uuid>] patterns
        let mut search_from = 0;
        while let Some(start) = message[search_from..].find(&marker) {
            let abs_start = search_from + start + marker.len();
            if let Some(end) = message[abs_start..].find(']') {
                ids.push(&message[abs_start..abs_start + end]);
            }
            search_from = abs_start;
        }

        // Find all proptest_id in JSON
        let json_marker = "\"proptest_id\":\"";
        search_from = 0;
        while let Some(start) = message[search_from..].find(json_marker) {
            let abs_start = search_from + start + json_marker.len();
            if let Some(end) = message[abs_start..].find('"') {
                let id = &message[abs_start..abs_start + end];
                if !ids.contains(&id) {
                    ids.push(id);
                }
            }
            search_from = abs_start;
        }

        // Find UUID in Apache path: GET /<uuid>/
        if let Some(start) = message.find("GET /") {
            let after_prefix = start + "GET /".len();
            if let Some(end) = message[after_prefix..].find('/') {
                let candidate = &message[after_prefix..after_prefix + end];
                if candidate.len() == 36 && candidate.contains('-') && !ids.contains(&candidate) {
                    ids.push(candidate);
                }
            }
        }

        ids
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

// --- JSON structure generation for JSON multiline testing ---

/// The shape of a JSON entry for multiline testing.
///
/// Each variant generates structurally different valid JSON to exercise
/// different code paths in the agent's JSON aggregator.
#[derive(Debug, Clone, Copy)]
pub enum JsonStructure {
    /// Flat object: `{"proptest_id":"uuid","f_0":"v","f_1":"v",...}`
    Flat {
        /// Number of additional fields beyond `proptest_id`.
        field_count: usize,
    },
    /// Nested object: `{"proptest_id":"uuid","data":{"f_0":"v",...}}`
    Nested {
        /// Number of fields inside the nested `data` object.
        field_count: usize,
    },
    /// Object containing an array of strings: `{"proptest_id":"uuid","items":["v0","v1",...]}`
    WithArray {
        /// Number of items in the array.
        item_count: usize,
    },
    /// Top-level array: `[{"proptest_id":"uuid","f_0":"v"}]`
    ///
    /// **Known agent limitation**: the agent's JSON aggregator does not support
    /// top-level arrays. The incremental validator returns `Invalid` when it
    /// sees `[` at `objCount == 0`, causing the buffer to flush unchanged.
    /// This variant is included to detect if/when the agent adds array support.
    TopLevelArray,
    /// Deeply nested objects: `{"proptest_id":"uuid","l0":{"l1":{"l2":"val"}}}`
    /// Tests brace depth tracking at 3+ levels.
    DeepNested {
        /// Nesting depth (2 = two levels of inner objects).
        depth: usize,
    },
    /// Array of objects inside an object:
    /// `{"proptest_id":"uuid","items":[{"k0":"v0"},{"k1":"v1"}]}`
    /// Tests mixed `[` and `{` bracket/brace tracking.
    MixedNesting {
        /// Number of objects in the array.
        obj_count: usize,
    },
    /// Object with an empty nested object: `{"proptest_id":"uuid","data":{}}`
    EmptyObject,
    /// Object with an empty array: `{"proptest_id":"uuid","items":[]}`
    EmptyArray,
    /// Object with escaped characters in string values.
    /// Tests JSON escape sequences that could confuse line-level parsing.
    EscapedStrings,
    /// Object with Unicode values (CJK, emoji, accented chars).
    UnicodeValues,
    /// Object with non-string value types (numbers, booleans, null).
    MixedValueTypes,
}

impl JsonStructure {
    /// Render the complete JSON value as a single string.
    ///
    /// # Panics
    ///
    /// Panics if JSON serialization fails (indicates a bug in generation).
    #[must_use]
    pub fn render_complete(&self, id: &str) -> String {
        match self {
            Self::Flat { field_count } => {
                let extra: Vec<String> = (0..*field_count)
                    .map(|i| format!(r#""f_{i}":"value_{i}""#))
                    .collect();
                if extra.is_empty() {
                    format!(r#"{{"proptest_id":"{id}"}}"#)
                } else {
                    format!(r#"{{"proptest_id":"{id}",{}}}"#, extra.join(","))
                }
            }
            Self::Nested { field_count } => {
                let inner: Vec<String> = (0..*field_count)
                    .map(|i| format!(r#""f_{i}":"value_{i}""#))
                    .collect();
                format!(
                    r#"{{"proptest_id":"{id}","data":{{{}}}}}"#,
                    inner.join(",")
                )
            }
            Self::WithArray { item_count } => {
                let items: Vec<String> = (0..*item_count)
                    .map(|i| format!(r#""item_{i}""#))
                    .collect();
                format!(
                    r#"{{"proptest_id":"{id}","items":[{}]}}"#,
                    items.join(",")
                )
            }
            Self::TopLevelArray => {
                format!(r#"[{{"proptest_id":"{id}","f_0":"value_0"}}]"#)
            }
            Self::DeepNested { depth } => {
                // Build from inside out: innermost has a value field
                let mut json = r#""leaf":"deep_value""#.to_string();
                for level in (0..*depth).rev() {
                    json = format!(r#""l_{level}":{{{json}}}"#);
                }
                format!(r#"{{"proptest_id":"{id}",{json}}}"#)
            }
            Self::MixedNesting { obj_count } => {
                let objects: Vec<String> = (0..*obj_count)
                    .map(|i| format!(r#"{{"k_{i}":"v_{i}"}}"#))
                    .collect();
                format!(
                    r#"{{"proptest_id":"{id}","items":[{}]}}"#,
                    objects.join(",")
                )
            }
            Self::EmptyObject => {
                format!(r#"{{"proptest_id":"{id}","data":{{}}}}"#)
            }
            Self::EmptyArray => {
                format!(r#"{{"proptest_id":"{id}","items":[]}}"#)
            }
            Self::EscapedStrings => {
                // Use serde_json to ensure valid escaping
                let obj = serde_json::json!({
                    "proptest_id": id,
                    "msg": "line1\nline2\ttab",
                    "path": "C:\\foo\\bar",
                    "quote": "he said \"hello\""
                });
                serde_json::to_string(&obj).expect("serde_json serialization cannot fail")
            }
            Self::UnicodeValues => {
                let obj = serde_json::json!({
                    "proptest_id": id,
                    "cjk": "テスト",
                    "emoji": "🔥",
                    "accented": "café"
                });
                serde_json::to_string(&obj).expect("serde_json serialization cannot fail")
            }
            Self::MixedValueTypes => {
                format!(
                    r#"{{"proptest_id":"{id}","count":42,"ratio":3.14,"active":true,"data":null}}"#
                )
            }
        }
    }

    /// Split the JSON into multiple lines for multiline file tailing.
    ///
    /// Splits after commas and opening braces — the same places a
    /// pretty-printer would insert line breaks.
    #[must_use]
    #[expect(clippy::too_many_lines)]
    pub fn render_lines(&self, id: &str) -> Vec<String> {
        match self {
            Self::Flat { field_count } => {
                let mut lines = Vec::with_capacity(field_count + 1);
                // First line: opening brace + proptest_id field + trailing comma
                if *field_count > 0 {
                    lines.push(format!(r#"{{"proptest_id":"{id}","#));
                    for i in 0..*field_count {
                        if i == field_count - 1 {
                            // Last field: include closing brace
                            lines.push(format!(r#""f_{i}":"value_{i}"}}"#));
                        } else {
                            lines.push(format!(r#""f_{i}":"value_{i}","#));
                        }
                    }
                } else {
                    lines.push(format!(r#"{{"proptest_id":"{id}"}}"#));
                }
                lines
            }
            Self::Nested { field_count } => {
                let mut lines = Vec::new();
                lines.push(format!(r#"{{"proptest_id":"{id}","#));
                if *field_count > 0 {
                    lines.push(r#""data":{"#.to_string());
                    for i in 0..*field_count {
                        if i == field_count - 1 {
                            lines.push(format!(r#""f_{i}":"value_{i}"}}}}"#));
                        } else {
                            lines.push(format!(r#""f_{i}":"value_{i}","#));
                        }
                    }
                } else {
                    lines.push(r#""data":{}}"#.to_string());
                }
                lines
            }
            Self::WithArray { item_count } => {
                let mut lines = Vec::new();
                lines.push(format!(r#"{{"proptest_id":"{id}","#));
                let items: Vec<String> = (0..*item_count)
                    .map(|i| format!(r#""item_{i}""#))
                    .collect();
                lines.push(format!(r#""items":[{}]}}"#, items.join(",")));
                lines
            }
            Self::TopLevelArray => {
                vec![
                    "[".to_string(),
                    format!(r#"{{"proptest_id":"{id}","f_0":"value_0"}}"#),
                    "]".to_string(),
                ]
            }
            Self::DeepNested { depth } => {
                let mut lines = Vec::new();
                lines.push(format!(r#"{{"proptest_id":"{id}","#));
                // One line per nesting level opening
                for level in 0..*depth {
                    lines.push(format!(r#""l_{level}":{{"#));
                }
                // Innermost value + all closing braces
                let closing: String = "}".repeat(depth + 1);
                lines.push(format!(r#""leaf":"deep_value"{closing}"#));
                lines
            }
            Self::MixedNesting { obj_count } => {
                let mut lines = Vec::new();
                lines.push(format!(r#"{{"proptest_id":"{id}","#));
                lines.push(r#""items":["#.to_string());
                for i in 0..*obj_count {
                    let comma = if i < obj_count - 1 { "," } else { "" };
                    lines.push(format!(r#"{{"k_{i}":"v_{i}"}}{comma}"#));
                }
                lines.push("]}".to_string());
                lines
            }
            Self::EmptyObject => {
                vec![
                    format!(r#"{{"proptest_id":"{id}","#),
                    r#""data":{}}"#.to_string(),
                ]
            }
            Self::EmptyArray => {
                vec![
                    format!(r#"{{"proptest_id":"{id}","#),
                    r#""items":[]}"#.to_string(),
                ]
            }
            Self::EscapedStrings => {
                // Get the complete valid JSON, then split it into lines
                // by breaking after known field boundaries.
                let complete = self.render_complete(id);
                split_json_after_fields(&complete, 1)
            }
            Self::UnicodeValues => {
                let complete = self.render_complete(id);
                split_json_after_fields(&complete, 1)
            }
            Self::MixedValueTypes => {
                vec![
                    format!(r#"{{"proptest_id":"{id}","#),
                    r#""count":42,"#.to_string(),
                    r#""ratio":3.14,"#.to_string(),
                    r#""active":true,"#.to_string(),
                    r#""data":null}"#.to_string(),
                ]
            }
        }
    }

    /// Build the expected JSON value for property assertion.
    ///
    /// Returns the parsed JSON that the agent should produce after
    /// compaction (for objects) or pass-through (for arrays).
    ///
    /// # Panics
    ///
    /// Panics if the generated JSON is not valid (indicates a bug in
    /// the generation logic).
    #[must_use]
    pub fn expected_json(&self, id: &str) -> serde_json::Value {
        let complete = self.render_complete(id);
        serde_json::from_str(&complete).expect("generated JSON must be valid")
    }
}

/// Split a JSON string into multiple lines by breaking after commas that
/// follow complete string values. Splits after the first `n_first_fields`
/// comma-separated fields on the first line, then puts the rest on a second line.
fn split_json_after_fields(json: &str, n_first_fields: usize) -> Vec<String> {
    // Find the Nth comma at the top level (not inside strings)
    let mut commas_found = 0;
    let mut in_string = false;
    let mut escape_next = false;

    for (i, ch) in json.char_indices() {
        if escape_next {
            escape_next = false;
            continue;
        }
        if ch == '\\' && in_string {
            escape_next = true;
            continue;
        }
        if ch == '"' {
            in_string = !in_string;
            continue;
        }
        if !in_string && ch == ',' {
            commas_found += 1;
            if commas_found == n_first_fields {
                // Split here: everything up to and including this comma on line 1
                return vec![
                    json[..=i].to_string(),
                    json[i + 1..].to_string(),
                ];
            }
        }
    }

    // Couldn't split — return as single line
    vec![json.to_string()]
}

/// Proptest strategy that generates a [`JsonStructure`] variant.
///
/// Generates a mix of all 11 structural variants to maximize the chance
/// of finding edge cases in the agent's JSON processing.
pub fn json_structure_strategy() -> impl Strategy<Value = JsonStructure> {
    prop_oneof![
        // Parameterized variants
        (2_usize..8).prop_map(|field_count| JsonStructure::Flat { field_count }),
        (1_usize..5).prop_map(|field_count| JsonStructure::Nested { field_count }),
        (1_usize..6).prop_map(|item_count| JsonStructure::WithArray { item_count }),
        (2_usize..5).prop_map(|depth| JsonStructure::DeepNested { depth }),
        (1_usize..4).prop_map(|obj_count| JsonStructure::MixedNesting { obj_count }),
        // Fixed variants
        Just(JsonStructure::TopLevelArray),
        Just(JsonStructure::EmptyObject),
        Just(JsonStructure::EmptyArray),
        Just(JsonStructure::EscapedStrings),
        Just(JsonStructure::UnicodeValues),
        Just(JsonStructure::MixedValueTypes),
    ]
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
/// Only includes formats with timestamps that the agent's auto multiline
/// detector recognizes as `startGroup` signals. `PlainText` (no timestamp)
/// and `Json` (complete objects get `noAggregate`) are excluded.
pub fn multiline_format_strategy() -> impl Strategy<Value = LogFormat> {
    prop_oneof![
        Just(LogFormat::TimestampPrefixed),
        Just(LogFormat::Syslog5424),
        Just(LogFormat::ApacheCommon),
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
    fn extract_continuations_from_joined_output() {
        // The agent joins aggregated lines with literal \n (0x5C 0x6E),
        // not actual newlines. Verify extraction works with this format.
        let header_id = "abc123";
        let joined = format!(
            "2024-01-15T10:30:00.000Z [PROPTEST:{header_id}] header content\\n    [CONT:{header_id}:0] cont 0\\n    [CONT:{header_id}:1] cont 1"
        );
        let continuations = LogFormat::extract_continuations(&joined);
        assert_eq!(continuations.len(), 2);
        assert_eq!(continuations[0], ("abc123", 0));
        assert_eq!(continuations[1], ("abc123", 1));
    }

    #[test]
    fn multiline_header_varies_timestamp() {
        let id = "test-uuid";
        let line0 = LogFormat::TimestampPrefixed.format_line_with_index(id, 0, "msg");
        let line1 = LogFormat::TimestampPrefixed.format_line_with_index(id, 1, "msg");
        let line2 = LogFormat::TimestampPrefixed.format_line_with_index(id, 2, "msg");
        assert!(line0.contains("10:30:00"));
        assert!(line1.contains("10:30:01"));
        assert!(line2.contains("10:30:02"));
        // All should still have extractable IDs
        assert_eq!(LogFormat::extract_id(&line0), Some(id));
        assert_eq!(LogFormat::extract_id(&line1), Some(id));
    }

    #[test]
    fn syslog_multiline_header_varies_timestamp() {
        let id = "test-uuid";
        let line0 = LogFormat::Syslog5424.format_line_with_index(id, 0, "msg");
        let line3 = LogFormat::Syslog5424.format_line_with_index(id, 3, "msg");
        assert!(line0.contains("10:30:00.000000Z"));
        assert!(line3.contains("10:30:03.000000Z"));
    }

    #[test]
    fn apache_multiline_header_varies_timestamp() {
        let id = "test-uuid";
        let line0 = LogFormat::ApacheCommon.format_line_with_index(id, 0, "msg");
        let line5 = LogFormat::ApacheCommon.format_line_with_index(id, 5, "msg");
        assert!(line0.contains("10:30:00 +0000"));
        assert!(line5.contains("10:30:05 +0000"));
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
