//! Mixed format multiline scenario.
//!
//! Interleaves all timestamp formats (`TimestampPrefixed`, `Syslog5424`,
//! `ApacheCommon`), all JSON structures, and plain text lines in the same
//! file. Tests interactions between the agent's detection chain (JSON
//! detector → timestamp detector → default aggregate) and aggregation
//! systems when formats change rapidly.

use proptest::prelude::*;

use crate::config::LogSourceConfig;
use crate::log_format::{self, LogFormat};
use crate::log_gen::{self, LogBatch};
use crate::property::{self, Property};
use crate::scenario::Scenario;

/// A single entry in the mixed sequence.
#[derive(Debug, Clone, Copy)]
pub enum MixedEntry {
    /// Timestamp header with N continuations, using a specific format.
    Timestamp {
        /// The timestamp format for the header line.
        format: LogFormat,
        /// Number of continuation lines.
        continuations: usize,
    },
    /// Single timestamp line, no continuations.
    TimestampOnly {
        /// The timestamp format.
        format: LogFormat,
    },
    /// Plain text line with no format structure.
    /// Defaults to `aggregate` — will merge with whatever is currently buffered.
    PlainText,
    /// Flat JSON object split across lines.
    JsonFlat {
        /// Number of fields beyond `proptest_id`.
        field_count: usize,
    },
    /// Nested JSON object split across lines.
    JsonNested {
        /// Number of fields in the nested object.
        field_count: usize,
    },
    /// JSON with a timestamp embedded as a string value.
    JsonWithTimestampValue,
    /// Complete single-line JSON.
    JsonSingleLine,
    /// JSON with mixed value types (numbers, booleans, null).
    JsonMixedTypes,
    /// Deeply nested JSON object.
    JsonDeepNested {
        /// Nesting depth.
        depth: usize,
    },
}

/// Parameters for mixed multiline testing.
#[derive(Debug, Clone)]
pub struct MixedMultilineParams {
    /// The sequence of entries to generate.
    pub entries: Vec<MixedEntry>,
}

/// Mixed multiline scenario.
#[derive(Debug, Copy, Clone)]
pub struct MixedMultilineScenario;

/// Strategy for a timestamp format (only formats with detectable timestamps).
fn timestamp_format_strategy() -> impl Strategy<Value = LogFormat> {
    prop_oneof![
        Just(LogFormat::TimestampPrefixed),
        Just(LogFormat::Syslog5424),
        Just(LogFormat::ApacheCommon),
    ]
}

/// Strategy that generates a single [`MixedEntry`].
fn mixed_entry_strategy() -> impl Strategy<Value = MixedEntry> {
    prop_oneof![
        // Timestamp entries with varying formats and continuations
        (timestamp_format_strategy(), 1_usize..4)
            .prop_map(|(format, continuations)| MixedEntry::Timestamp { format, continuations }),
        timestamp_format_strategy()
            .prop_map(|format| MixedEntry::TimestampOnly { format }),
        // Plain text — no structure, tests default aggregate behavior
        Just(MixedEntry::PlainText),
        // JSON variants
        (2_usize..6).prop_map(|field_count| MixedEntry::JsonFlat { field_count }),
        (1_usize..4).prop_map(|field_count| MixedEntry::JsonNested { field_count }),
        Just(MixedEntry::JsonWithTimestampValue),
        Just(MixedEntry::JsonSingleLine),
        Just(MixedEntry::JsonMixedTypes),
        (2_usize..4).prop_map(|depth| MixedEntry::JsonDeepNested { depth }),
    ]
}

impl Scenario for MixedMultilineScenario {
    type Params = MixedMultilineParams;

    fn strategy() -> BoxedStrategy<Self::Params> {
        proptest::collection::vec(mixed_entry_strategy(), 5..15)
            .prop_map(|entries| MixedMultilineParams { entries })
            .boxed()
    }

    fn log_source_config(_params: &Self::Params) -> LogSourceConfig {
        // JsonMultiline enables auto_multi_line_detection + JSON detection +
        // JSON aggregation. Datetime detection defaults to true, so both
        // aggregation systems are active.
        LogSourceConfig::JsonMultiline
    }

    fn log_format(_params: &Self::Params) -> LogFormat {
        LogFormat::PlainText
    }

    fn generate_input(params: &Self::Params) -> LogBatch {
        let mut lines = Vec::new();
        let mut expected_continuations = Vec::new();
        let mut expected_json: Vec<(String, serde_json::Value)> = Vec::new();
        let mut timestamp_index: usize = 0;

        for entry in &params.entries {
            let id = uuid::Uuid::new_v4().to_string();

            match entry {
                MixedEntry::Timestamp { format, continuations } => {
                    let header = format
                        .format_line_with_index(&id, timestamp_index, "mixed entry header");
                    timestamp_index += 1;
                    lines.push(log_gen::LogLine {
                        id: id.clone(),
                        content: header,
                    });

                    expected_continuations.push((id.clone(), *continuations));

                    for seq in 0..*continuations {
                        let cont = format
                            .format_continuation(&id, seq, &format!("continuation {seq}"));
                        lines.push(log_gen::LogLine {
                            id: format!("{id}:cont:{seq}"),
                            content: cont,
                        });
                    }
                }

                MixedEntry::TimestampOnly { format } => {
                    let header = format
                        .format_line_with_index(&id, timestamp_index, "standalone entry");
                    timestamp_index += 1;
                    lines.push(log_gen::LogLine {
                        id: id.clone(),
                        content: header,
                    });
                    expected_continuations.push((id, 0));
                }

                MixedEntry::PlainText => {
                    // A line with no timestamp or JSON structure.
                    // The agent labels this `aggregate` and appends it to
                    // whatever is currently buffered.
                    let content = format!("[PROPTEST:{id}] plain text with no format structure");
                    lines.push(log_gen::LogLine { id, content });
                    // We don't add to expected_continuations or expected_json.
                    // AllLinesDelivered will check this UUID appears somewhere.
                }

                MixedEntry::JsonFlat { field_count } => {
                    let structure = log_format::JsonStructure::Flat {
                        field_count: *field_count,
                    };
                    push_json_entry(&mut lines, &mut expected_json, &id, &structure);
                }

                MixedEntry::JsonNested { field_count } => {
                    let structure = log_format::JsonStructure::Nested {
                        field_count: *field_count,
                    };
                    push_json_entry(&mut lines, &mut expected_json, &id, &structure);
                }

                MixedEntry::JsonWithTimestampValue => {
                    let obj = serde_json::json!({
                        "proptest_id": id,
                        "timestamp": "2024-01-15T10:30:00.000Z",
                        "msg": "json with embedded timestamp"
                    });
                    let complete = serde_json::to_string(&obj)
                        .expect("serde_json serialization cannot fail");
                    expected_json.push((id.clone(), obj));
                    push_split_json(&mut lines, &id, &complete);
                }

                MixedEntry::JsonSingleLine => {
                    let obj = serde_json::json!({
                        "proptest_id": id,
                        "msg": "single line json"
                    });
                    let complete = serde_json::to_string(&obj)
                        .expect("serde_json serialization cannot fail");
                    expected_json.push((id.clone(), obj));
                    lines.push(log_gen::LogLine { id, content: complete });
                }

                MixedEntry::JsonMixedTypes => {
                    let structure = log_format::JsonStructure::MixedValueTypes;
                    push_json_entry(&mut lines, &mut expected_json, &id, &structure);
                }

                MixedEntry::JsonDeepNested { depth } => {
                    let structure = log_format::JsonStructure::DeepNested { depth: *depth };
                    push_json_entry(&mut lines, &mut expected_json, &id, &structure);
                }
            }
        }

        LogBatch {
            lines,
            format: LogFormat::PlainText,
            expected_continuations,
            expected_json: Some(expected_json),
        }
    }

    fn properties(_params: &Self::Params) -> Vec<Box<dyn Property>> {
        vec![
            Box::new(property::AllLinesDelivered),
            Box::new(property::MultilineAggregated),
            Box::new(property::JsonIntegrity),
        ]
    }
}

/// Helper: push a JSON structure's lines and expected value.
fn push_json_entry(
    lines: &mut Vec<log_gen::LogLine>,
    expected_json: &mut Vec<(String, serde_json::Value)>,
    id: &str,
    structure: &log_format::JsonStructure,
) {
    expected_json.push((id.to_string(), structure.expected_json(id)));
    for (i, line) in structure.render_lines(id).into_iter().enumerate() {
        lines.push(log_gen::LogLine {
            id: if i == 0 {
                id.to_string()
            } else {
                format!("{id}:json_line:{i}")
            },
            content: line,
        });
    }
}

/// Helper: split a JSON string after the first comma and push as lines.
fn push_split_json(
    lines: &mut Vec<log_gen::LogLine>,
    id: &str,
    complete: &str,
) {
    let split_point = complete.find(',').unwrap_or(complete.len());
    if split_point < complete.len() - 1 {
        lines.push(log_gen::LogLine {
            id: id.to_string(),
            content: complete[..=split_point].to_string(),
        });
        lines.push(log_gen::LogLine {
            id: format!("{id}:json_line:1"),
            content: complete[split_point + 1..].to_string(),
        });
    } else {
        lines.push(log_gen::LogLine {
            id: id.to_string(),
            content: complete.to_string(),
        });
    }
}
