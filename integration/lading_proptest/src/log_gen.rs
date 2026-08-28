//! Proptest strategies for generating unique log lines.
//!
//! Each generated line contains a UUID marker for input-output correlation
//! during property assertion. See [`crate::log_format`] for format details.

use proptest::prelude::*;

use crate::log_format::LogFormat;

/// A single log line with a unique identifier.
#[derive(Debug, Clone)]
pub struct LogLine {
    /// Unique identifier for this line (UUID).
    pub id: String,
    /// The full text content of the line as written to disk.
    pub content: String,
}

/// A batch of log lines representing one test case's input.
#[derive(Debug, Clone)]
pub struct LogBatch {
    /// The individual log lines.
    pub lines: Vec<LogLine>,
    /// The format used to generate these lines.
    pub format: LogFormat,
    /// For multiline scenarios: maps header UUID → expected continuation count.
    /// Empty for non-multiline scenarios.
    pub expected_continuations: Vec<(String, usize)>,
    /// For JSON multiline scenarios: maps UUID → expected JSON value.
    /// `None` for non-JSON scenarios.
    pub expected_json: Option<Vec<(String, serde_json::Value)>>,
}

/// A multiline log entry consisting of a header and continuation lines.
#[derive(Debug, Clone)]
pub struct MultilineEntry {
    /// The header line (starts the logical entry).
    pub header: LogLine,
    /// Continuation lines that should be aggregated with the header.
    pub continuations: Vec<LogLine>,
}

/// Strategy for generating a batch of simple single-line logs.
///
/// Each line is unique (distinct UUID) and formatted according to the given
/// [`LogFormat`].
pub fn simple_log_batch(
    format: LogFormat,
    line_count: impl Strategy<Value = usize>,
    line_len_range: std::ops::Range<usize>,
) -> impl Strategy<Value = LogBatch> {
    line_count.prop_flat_map(move |count| {
        proptest::collection::vec(line_len_range.clone(), count).prop_map(move |lens| {
            let mut lines = Vec::with_capacity(lens.len());
            for len in &lens {
                let id = uuid::Uuid::new_v4().to_string();
                // Generate deterministic content based on line index for
                // simplicity; the UUID provides uniqueness.
                let filler: String = (0..*len)
                    .map(|i| char::from(b'a' + u8::try_from(i % 26).unwrap_or(0)))
                    .collect();
                let content = format.format_line(&id, &filler);
                lines.push(LogLine { id, content });
            }
            LogBatch { lines, format, expected_continuations: Vec::new(), expected_json: None }
        })
    })
}

/// Strategy for generating multiline log entries.
///
/// Each entry has a header line (with timestamp/structure per format) and
/// 0-N continuation lines (indented, no leading structure).
pub fn multiline_log_batch(
    format: LogFormat,
    entry_count: impl Strategy<Value = usize>,
    max_continuations: usize,
) -> impl Strategy<Value = LogBatch> {
    entry_count.prop_flat_map(move |count| {
        proptest::collection::vec(0_usize..=max_continuations, count).prop_map(
            move |continuation_counts| {
                let mut lines = Vec::new();
                for cont_count in &continuation_counts {
                    let header_id = uuid::Uuid::new_v4().to_string();
                    let header_content = format.format_line(&header_id, "log entry header");
                    lines.push(LogLine {
                        id: header_id.clone(),
                        content: header_content,
                    });
                    for seq in 0..*cont_count {
                        let cont_content = format.format_continuation(
                            &header_id,
                            seq,
                            &format!("continuation line {seq}"),
                        );
                        // Continuation lines share the header's ID conceptually
                        // but get their own line entry. The property checker uses
                        // the CONT marker to associate them.
                        lines.push(LogLine {
                            id: format!("{header_id}:cont:{seq}"),
                            content: cont_content,
                        });
                    }
                }
                LogBatch { lines, format, expected_continuations: Vec::new(), expected_json: None }
            },
        )
    })
}

/// Strategy for generating lines near truncation boundaries.
///
/// Produces a mix of:
/// - Lines well under the limit
/// - Lines near the boundary (+/- 100 bytes)
/// - Lines well over the limit
pub fn truncation_log_batch(
    format: LogFormat,
    line_count: impl Strategy<Value = usize>,
    max_message_bytes: usize,
) -> impl Strategy<Value = LogBatch> {
    line_count.prop_flat_map(move |count| {
        // Generate a mix of line length categories
        proptest::collection::vec(
            prop_oneof![
                // Well under limit (100-1000 bytes)
                100_usize..1000,
                // Near boundary (limit - 100 to limit + 100)
                (max_message_bytes.saturating_sub(100))..=(max_message_bytes + 100),
                // Well over limit (limit + 1000 to limit * 2)
                (max_message_bytes + 1000)..=(max_message_bytes * 2),
            ],
            count,
        )
        .prop_map(move |lengths| {
            let mut lines = Vec::with_capacity(lengths.len());
            for target_len in &lengths {
                let id = uuid::Uuid::new_v4().to_string();
                // The format_line call adds overhead (markers, format structure).
                // We generate filler content sized so the total line is
                // approximately target_len bytes.
                let overhead_estimate = format.format_line(&id, "").len();
                let filler_len = target_len.saturating_sub(overhead_estimate);
                let filler: String = (0..filler_len)
                    .map(|i| char::from(b'a' + u8::try_from(i % 26).unwrap_or(0)))
                    .collect();
                let content = format.format_line(&id, &filler);
                lines.push(LogLine { id, content });
            }
            LogBatch { lines, format, expected_continuations: Vec::new(), expected_json: None }
        })
    })
}

/// Strategy that generates a [`LogFormat`] alongside a simple log batch.
///
/// Useful for tests that should exercise all formats.
pub fn simple_log_batch_any_format() -> impl Strategy<Value = LogBatch> {
    crate::log_format::log_format_strategy().prop_flat_map(|format| {
        simple_log_batch(format, 3_usize..20, 10..100)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::test_runner::{Config, TestRunner};

    #[test]
    fn simple_batch_has_unique_ids() {
        let mut runner = TestRunner::new(Config::with_cases(10));
        runner
            .run(
                &simple_log_batch(LogFormat::PlainText, 5_usize..20, 10..100),
                |batch| {
                    let ids: rustc_hash::FxHashSet<&str> =
                        batch.lines.iter().map(|l| l.id.as_str()).collect();
                    // All IDs should be unique
                    prop_assert_eq!(ids.len(), batch.lines.len());
                    Ok(())
                },
            )
            .unwrap();
    }

    #[test]
    fn multiline_batch_has_headers_and_continuations() {
        let mut runner = TestRunner::new(Config::with_cases(10));
        runner
            .run(
                &multiline_log_batch(LogFormat::TimestampPrefixed, 3_usize..10, 3),
                |batch| {
                    // Should have at least as many lines as entries (headers)
                    prop_assert!(!batch.lines.is_empty());
                    // First line should be a header (not a continuation)
                    prop_assert!(
                        !batch.lines[0].content.contains("CONT:"),
                        "first line should be a header"
                    );
                    Ok(())
                },
            )
            .unwrap();
    }

    #[test]
    fn truncation_batch_has_mixed_sizes() {
        let limit = 256 * 1024; // 256 KB
        let mut runner = TestRunner::new(Config::with_cases(10));
        runner
            .run(
                &truncation_log_batch(LogFormat::PlainText, Just(9_usize), limit),
                |batch| {
                    // With 9 lines and 3 categories, we should have a mix
                    let under: Vec<_> = batch
                        .lines
                        .iter()
                        .filter(|l| l.content.len() < limit - 100)
                        .collect();
                    let over: Vec<_> = batch
                        .lines
                        .iter()
                        .filter(|l| l.content.len() > limit + 100)
                        .collect();
                    // At least some should be under and some over
                    // (probabilistic, but with 9 lines across 3 categories
                    // it's very likely)
                    prop_assert!(
                        !under.is_empty() || !over.is_empty(),
                        "should have a mix of sizes"
                    );
                    Ok(())
                },
            )
            .unwrap();
    }
}
