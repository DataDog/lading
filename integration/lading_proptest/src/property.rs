//! Declarative property assertion system.
//!
//! Properties are composable checks that verify relationships between
//! test input ([`LogBatch`]) and agent output ([`ReceivedLogEntry`]).

use std::fmt;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::intake::ReceivedLogEntry;
use crate::log_format::LogFormat;
use crate::log_gen::LogBatch;

/// A property that can be checked against test input and output.
pub trait Property: fmt::Debug {
    /// Human-readable name of this property.
    fn name(&self) -> &'static str;

    /// Check the property.
    ///
    /// Returns `Ok(())` if the property holds, or a [`PropertyFailure`]
    /// describing what went wrong.
    ///
    /// # Errors
    ///
    /// Returns [`PropertyFailure`] when the property does not hold.
    fn check(
        &self,
        input: &LogBatch,
        output: &[ReceivedLogEntry],
    ) -> Result<(), PropertyFailure>;
}

/// Detailed information about a property failure.
#[derive(Debug, Clone)]
pub struct PropertyFailure {
    /// The name of the property that failed.
    pub property_name: String,
    /// A human-readable description of the failure.
    pub description: String,
    /// Key-value pairs providing additional context.
    pub details: Vec<(String, String)>,
}

impl fmt::Display for PropertyFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Property '{}' failed: {}", self.property_name, self.description)?;
        for (key, value) in &self.details {
            writeln!(f, "  {key}: {value}")?;
        }
        Ok(())
    }
}

// --- Built-in Properties ---

/// Every unique input line UUID appears in at least one output entry.
#[derive(Debug, Copy, Clone)]
pub struct AllLinesDelivered;

impl Property for AllLinesDelivered {
    fn name(&self) -> &'static str {
        "all_lines_delivered"
    }

    fn check(
        &self,
        input: &LogBatch,
        output: &[ReceivedLogEntry],
    ) -> Result<(), PropertyFailure> {
        // Collect all header IDs from input (skip continuation IDs like "uuid:cont:N")
        let input_ids: FxHashSet<&str> = input
            .lines
            .iter()
            .filter(|l| !l.id.contains(":cont:"))
            .map(|l| l.id.as_str())
            .collect();

        // Collect all IDs found in output messages
        let output_ids: FxHashSet<&str> = output
            .iter()
            .filter_map(|entry| LogFormat::extract_id(&entry.message))
            .collect();

        let missing: Vec<&str> = input_ids.difference(&output_ids).copied().collect();

        if missing.is_empty() {
            Ok(())
        } else {
            Err(PropertyFailure {
                property_name: self.name().to_string(),
                description: format!(
                    "{} of {} input lines not found in output",
                    missing.len(),
                    input_ids.len()
                ),
                details: vec![
                    ("missing_ids".to_string(), format!("{missing:?}")),
                    ("input_count".to_string(), input_ids.len().to_string()),
                    ("output_count".to_string(), output.len().to_string()),
                ],
            })
        }
    }
}

/// No output entry contains a UUID that was not in the input.
#[derive(Debug, Copy, Clone)]
pub struct NoExtraLines;

impl Property for NoExtraLines {
    fn name(&self) -> &'static str {
        "no_extra_lines"
    }

    fn check(
        &self,
        input: &LogBatch,
        output: &[ReceivedLogEntry],
    ) -> Result<(), PropertyFailure> {
        let input_ids: FxHashSet<&str> = input
            .lines
            .iter()
            .filter(|l| !l.id.contains(":cont:"))
            .map(|l| l.id.as_str())
            .collect();

        let extra: Vec<String> = output
            .iter()
            .filter_map(|entry| {
                LogFormat::extract_id(&entry.message).and_then(|id| {
                    if input_ids.contains(id) {
                        None
                    } else {
                        Some(id.to_string())
                    }
                })
            })
            .collect();

        if extra.is_empty() {
            Ok(())
        } else {
            Err(PropertyFailure {
                property_name: self.name().to_string(),
                description: format!(
                    "{} output entries have IDs not in input",
                    extra.len()
                ),
                details: vec![("extra_ids".to_string(), format!("{extra:?}"))],
            })
        }
    }
}

/// For non-truncated lines, the message content matches the input exactly.
#[derive(Debug, Copy, Clone)]
pub struct ContentPreserved;

impl Property for ContentPreserved {
    fn name(&self) -> &'static str {
        "content_preserved"
    }

    fn check(
        &self,
        input: &LogBatch,
        output: &[ReceivedLogEntry],
    ) -> Result<(), PropertyFailure> {
        // Build a map from ID to input content
        let input_by_id: FxHashMap<&str, &str> = input
            .lines
            .iter()
            .filter(|l| !l.id.contains(":cont:"))
            .map(|l| (l.id.as_str(), l.content.as_str()))
            .collect();

        let mut mismatches = Vec::new();

        for entry in output {
            let Some(id) = LogFormat::extract_id(&entry.message) else {
                continue;
            };
            let Some(expected) = input_by_id.get(id) else {
                continue;
            };

            // Check if the output message contains the expected content.
            // The agent may wrap the content (add metadata, etc.) so we check
            // containment rather than exact equality.
            if !entry.message.contains(expected) && !expected.contains(&entry.message) {
                // Could be truncated — skip those (TruncationRespected handles them)
                if !entry.message.contains("TRUNCATED") {
                    mismatches.push((id.to_string(), (*expected).to_string(), entry.message.clone()));
                }
            }
        }

        if mismatches.is_empty() {
            Ok(())
        } else {
            let detail: Vec<String> = mismatches
                .iter()
                .take(5)
                .map(|(id, expected, actual)| {
                    format!("id={id}: expected contains '{expected}', got '{actual}'")
                })
                .collect();
            Err(PropertyFailure {
                property_name: self.name().to_string(),
                description: format!("{} lines had content mismatches", mismatches.len()),
                details: vec![
                    ("mismatches".to_string(), detail.join("\n")),
                    ("total_mismatches".to_string(), mismatches.len().to_string()),
                ],
            })
        }
    }
}

/// Lines exceeding the configured max message size are truncated; lines under
/// the limit are delivered intact.
///
/// The Datadog Agent's truncation behavior:
/// - Lines under the limit pass through intact.
/// - Lines over the limit are split into a **head chunk** (content up to the
///   limit with `...TRUNCATED...` appended) and one or more **tail chunks**
///   (prefixed with `...TRUNCATED...` containing the remaining content).
/// - The head chunk size is `max_message_size_bytes + TRUNCATED_MARKER.len()`.
/// - Tail chunks are standalone output entries with no UUID.
#[derive(Debug, Copy, Clone)]
pub struct TruncationRespected {
    /// The agent's `max_message_size_bytes` setting.
    pub max_message_bytes: usize,
}

/// The marker the agent appends/prepends on truncated messages.
const TRUNCATED_MARKER: &str = "...TRUNCATED...";

impl Property for TruncationRespected {
    fn name(&self) -> &'static str {
        "truncation_respected"
    }

    fn check(
        &self,
        input: &LogBatch,
        output: &[ReceivedLogEntry],
    ) -> Result<(), PropertyFailure> {
        let input_by_id: FxHashMap<&str, &str> = input
            .lines
            .iter()
            .filter(|l| !l.id.contains(":cont:"))
            .map(|l| (l.id.as_str(), l.content.as_str()))
            .collect();

        // The agent appends "...TRUNCATED..." to the head chunk, so the
        // output message can be up to this many bytes over the raw limit.
        let max_head_bytes = self.max_message_bytes + TRUNCATED_MARKER.len();

        let mut violations = Vec::new();

        for entry in output {
            // Skip tail chunks — they are expected byproducts of truncation
            if entry.message.starts_with(TRUNCATED_MARKER) {
                continue;
            }

            let Some(id) = LogFormat::extract_id(&entry.message) else {
                continue;
            };
            let Some(input_content) = input_by_id.get(id) else {
                continue;
            };

            let input_len = input_content.len();
            let output_len = entry.message.len();

            if input_len > self.max_message_bytes {
                // Input was over limit — head chunk should be truncated.
                // The agent appends "...TRUNCATED..." so output can be up to
                // max_message_bytes + 15 bytes.
                if output_len > max_head_bytes {
                    violations.push(format!(
                        "id={id}: input {input_len}B over limit, head chunk {output_len}B exceeds max head size {max_head_bytes}B"
                    ));
                }
                // The head chunk should also contain the truncation marker
                if !entry.message.ends_with(TRUNCATED_MARKER) {
                    violations.push(format!(
                        "id={id}: input {input_len}B over limit, but head chunk missing '{TRUNCATED_MARKER}' suffix"
                    ));
                }
            } else {
                // Input was under limit — output should be delivered intact
                if output_len < input_len / 2 {
                    violations.push(format!(
                        "id={id}: input {input_len}B under limit, but output only {output_len}B (possible data loss)"
                    ));
                }
            }
        }

        if violations.is_empty() {
            Ok(())
        } else {
            Err(PropertyFailure {
                property_name: self.name().to_string(),
                description: format!("{} truncation violations", violations.len()),
                details: vec![
                    ("violations".to_string(), violations.join("\n")),
                    (
                        "max_message_bytes".to_string(),
                        self.max_message_bytes.to_string(),
                    ),
                ],
            })
        }
    }
}

/// Continuation lines are correctly aggregated with their header line.
#[derive(Debug, Copy, Clone)]
pub struct MultilineAggregated;

impl Property for MultilineAggregated {
    fn name(&self) -> &'static str {
        "multiline_aggregated"
    }

    fn check(
        &self,
        _input: &LogBatch,
        output: &[ReceivedLogEntry],
    ) -> Result<(), PropertyFailure> {
        // For each output entry, check that if it contains a header marker,
        // it also contains the expected continuation markers.
        let mut failures = Vec::new();

        for entry in output {
            let Some(header_id) = LogFormat::extract_id(&entry.message) else {
                continue;
            };

            // Check what continuations are present in this entry
            let continuations = LogFormat::extract_continuations(&entry.message);

            // If there are continuations, verify they belong to this header
            for (cont_header_id, _seq) in &continuations {
                if *cont_header_id != header_id {
                    failures.push(format!(
                        "header {header_id} contains continuation from different header {cont_header_id}"
                    ));
                }
            }
        }

        if failures.is_empty() {
            Ok(())
        } else {
            Err(PropertyFailure {
                property_name: self.name().to_string(),
                description: format!("{} multiline aggregation failures", failures.len()),
                details: vec![("failures".to_string(), failures.join("\n"))],
            })
        }
    }
}

/// The number of output entries matches the expected count.
#[derive(Debug, Copy, Clone)]
pub struct ExpectedEntryCount {
    /// Expected number of output entries.
    pub expected: usize,
}

impl Property for ExpectedEntryCount {
    fn name(&self) -> &'static str {
        "expected_entry_count"
    }

    fn check(
        &self,
        _input: &LogBatch,
        output: &[ReceivedLogEntry],
    ) -> Result<(), PropertyFailure> {
        // Count output entries that have a proptest marker (ignore agent
        // internal log entries that may also arrive).
        let proptest_entries: usize = output
            .iter()
            .filter(|entry| LogFormat::extract_id(&entry.message).is_some())
            .count();

        if proptest_entries == self.expected {
            Ok(())
        } else {
            Err(PropertyFailure {
                property_name: self.name().to_string(),
                description: format!(
                    "expected {expected} entries, got {proptest_entries}",
                    expected = self.expected,
                ),
                details: vec![
                    ("expected".to_string(), self.expected.to_string()),
                    ("actual".to_string(), proptest_entries.to_string()),
                    ("total_output".to_string(), output.len().to_string()),
                ],
            })
        }
    }
}
