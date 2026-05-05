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
        // Collect all header IDs from input. Skip internal line fragment IDs:
        // - "uuid:cont:N" — timestamp multiline continuation lines
        // - "uuid:json_line:N" — JSON multiline continuation lines
        let input_ids: FxHashSet<&str> = input
            .lines
            .iter()
            .filter(|l| !l.id.contains(":cont:") && !l.id.contains(":json_line:"))
            .map(|l| l.id.as_str())
            .collect();

        // Collect all IDs found in output messages. Use extract_all_ids
        // because aggregated messages may contain multiple UUIDs (e.g., a
        // plain text line merged into a preceding entry's buffer).
        let output_ids: FxHashSet<&str> = output
            .iter()
            .flat_map(|entry| LogFormat::extract_all_ids(&entry.message))
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
            .filter(|l| !l.id.contains(":cont:") && !l.id.contains(":json_line:"))
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
///
/// Verifies:
/// - All expected continuations for each header are present in its output entry
/// - Continuations are in ascending sequence order
/// - No continuations from a different header appear in an entry
///
/// Uses `LogBatch::expected_continuations` to know what to expect. If that
/// field is empty (non-multiline scenarios), this property passes trivially.
#[derive(Debug, Copy, Clone)]
pub struct MultilineAggregated;

impl Property for MultilineAggregated {
    fn name(&self) -> &'static str {
        "multiline_aggregated"
    }

    fn check(
        &self,
        input: &LogBatch,
        output: &[ReceivedLogEntry],
    ) -> Result<(), PropertyFailure> {
        // If no continuation metadata, pass trivially (non-multiline scenario)
        if input.expected_continuations.is_empty() {
            return Ok(());
        }

        // Build expected: header_id → continuation count
        let expected: FxHashMap<&str, usize> = input
            .expected_continuations
            .iter()
            .map(|(id, count)| (id.as_str(), *count))
            .collect();

        let mut failures = Vec::new();

        for entry in output {
            let Some(header_id) = LogFormat::extract_id(&entry.message) else {
                continue;
            };

            let Some(&expected_count) = expected.get(header_id) else {
                continue;
            };

            // Extract continuations found in this output entry
            let continuations = LogFormat::extract_continuations(&entry.message);

            // Check no cross-contamination
            for (cont_header_id, _seq) in &continuations {
                if *cont_header_id != header_id {
                    failures.push(format!(
                        "header {header_id} contains continuation from different header {cont_header_id}"
                    ));
                }
            }

            // Check all expected continuations are present
            let found_seqs: Vec<usize> = continuations
                .iter()
                .filter(|(hid, _)| *hid == header_id)
                .map(|(_, seq)| *seq)
                .collect();

            for expected_seq in 0..expected_count {
                if !found_seqs.contains(&expected_seq) {
                    failures.push(format!(
                        "header {header_id} missing continuation seq {expected_seq} (expected {expected_count} continuations, found {found_seqs:?})"
                    ));
                }
            }

            // Check ordering (continuations should appear in ascending order)
            for window in found_seqs.windows(2) {
                if window[0] >= window[1] {
                    failures.push(format!(
                        "header {header_id} has out-of-order continuations: seq {} before seq {}",
                        window[0], window[1]
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

/// JSON data integrity: every input JSON entry arrives in the output with
/// all fields preserved.
///
/// Parses output messages as JSON and verifies that the `proptest_id` field
/// matches and all expected fields are present with correct values. This
/// property does not encode knowledge of which JSON structures the agent
/// supports — it discovers gaps mechanically.
#[derive(Debug, Copy, Clone)]
pub struct JsonIntegrity;

impl Property for JsonIntegrity {
    fn name(&self) -> &'static str {
        "json_integrity"
    }

    fn check(
        &self,
        input: &LogBatch,
        output: &[ReceivedLogEntry],
    ) -> Result<(), PropertyFailure> {
        let Some(expected_entries) = &input.expected_json else {
            // No JSON metadata — pass trivially
            return Ok(());
        };

        let mut failures = Vec::new();

        for (expected_id, expected_json) in expected_entries {
            // Find the output entry containing this UUID
            let matching_output = output.iter().find(|entry| {
                entry.message.contains(expected_id)
            });

            let Some(output_entry) = matching_output else {
                failures.push(format!(
                    "id={expected_id}: not found in any output entry"
                ));
                continue;
            };

            // Try to parse the output message as JSON
            let parsed: Result<serde_json::Value, _> =
                serde_json::from_str(&output_entry.message);

            match parsed {
                Ok(output_json) => {
                    // Verify the proptest_id field
                    if let Some(id_val) = output_json.get("proptest_id")
                        && id_val.as_str() != Some(expected_id.as_str())
                    {
                        failures.push(format!(
                            "id={expected_id}: proptest_id mismatch, got {id_val}"
                        ));
                    }

                    // Verify all expected fields are present
                    check_json_fields(
                        expected_id,
                        expected_json,
                        &output_json,
                        "",
                        &mut failures,
                    );
                }
                Err(e) => {
                    failures.push(format!(
                        "id={expected_id}: output is not valid JSON: {e} (message: {})",
                        &output_entry.message[..output_entry.message.len().min(200)]
                    ));
                }
            }
        }

        if failures.is_empty() {
            Ok(())
        } else {
            Err(PropertyFailure {
                property_name: self.name().to_string(),
                description: format!("{} JSON integrity failures", failures.len()),
                details: vec![("failures".to_string(), failures.join("\n"))],
            })
        }
    }
}

/// Recursively verify that all fields in `expected` are present in `actual`.
fn check_json_fields(
    id: &str,
    expected: &serde_json::Value,
    actual: &serde_json::Value,
    path: &str,
    failures: &mut Vec<String>,
) {
    match (expected, actual) {
        (serde_json::Value::Object(exp_map), serde_json::Value::Object(act_map)) => {
            for (key, exp_val) in exp_map {
                let field_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                match act_map.get(key) {
                    Some(act_val) => {
                        check_json_fields(id, exp_val, act_val, &field_path, failures);
                    }
                    None => {
                        failures.push(format!(
                            "id={id}: missing field '{field_path}'"
                        ));
                    }
                }
            }
        }
        (serde_json::Value::Array(exp_arr), serde_json::Value::Array(act_arr)) => {
            if exp_arr.len() != act_arr.len() {
                failures.push(format!(
                    "id={id}: array at '{path}' has {} elements, expected {}",
                    act_arr.len(),
                    exp_arr.len()
                ));
            }
            for (i, (exp_item, act_item)) in
                exp_arr.iter().zip(act_arr.iter()).enumerate()
            {
                let item_path = format!("{path}[{i}]");
                check_json_fields(id, exp_item, act_item, &item_path, failures);
            }
        }
        (exp, act) => {
            if exp != act {
                failures.push(format!(
                    "id={id}: field '{path}' expected {exp}, got {act}"
                ));
            }
        }
    }
}

// --- Adaptive Sampling Properties ---

/// Model-based adaptive sampling check.
///
/// Simulates the agent's credit-based rate limiter as a simple state machine:
/// - Each `WriteLines` action consumes 1 credit per line (if available)
/// - Each `Sleep` action refills credits at `rate_limit` per second
/// - Compares the model's prediction of which IDs should be delivered
///   against what actually appeared in the output
///
/// Also verifies the `adaptive_sampler_sampled_count` tag appears when
/// the model predicts drops occurred.
///
/// The model is intentionally simpler than the agent's implementation —
/// no tokenizer, no pattern table, no hot-path optimization. It just
/// tracks credits. This tests that the real system adheres to the model.
#[derive(Debug, Clone)]
pub struct AdaptiveSamplingModel {
    /// The action sequence that was executed, with IDs for each write step.
    pub actions: Vec<SamplingAction>,
    /// Configured burst size (initial credits and cap).
    pub burst_size: f64,
    /// Credits refilled per second.
    pub rate_limit: f64,
    /// Allowed deviation per check point (absorbs timing jitter).
    /// Goal is to eventually get this to 0.
    pub tolerance: usize,
}

/// A step in the sampling action sequence, annotated with line IDs.
#[derive(Debug, Clone)]
pub enum SamplingAction {
    /// Lines written to the log file, with their IDs.
    Write(Vec<String>),
    /// Sleep duration in seconds.
    Sleep(u64),
}

impl Property for AdaptiveSamplingModel {
    fn name(&self) -> &'static str {
        "adaptive_sampling_model"
    }

    fn check(
        &self,
        _input: &LogBatch,
        output: &[ReceivedLogEntry],
    ) -> Result<(), PropertyFailure> {
        // Simulate the credit model
        let mut credits = self.burst_size;
        let mut expected_delivered: Vec<String> = Vec::new();
        let mut expected_dropped: usize = 0;

        for action in &self.actions {
            match action {
                SamplingAction::Write(ids) => {
                    for id in ids {
                        if credits >= 1.0 {
                            expected_delivered.push(id.clone());
                            credits -= 1.0;
                        } else {
                            expected_dropped += 1;
                        }
                    }
                }
                SamplingAction::Sleep(seconds) => {
                    #[expect(clippy::cast_precision_loss)]
                    let refill = *seconds as f64 * self.rate_limit;
                    credits += refill;
                    if credits > self.burst_size {
                        credits = self.burst_size;
                    }
                }
            }
        }

        // Collect all IDs found in output
        let output_ids: FxHashSet<&str> = output
            .iter()
            .flat_map(|entry| LogFormat::extract_all_ids(&entry.message))
            .collect();

        let mut failures = Vec::new();

        // Check: model-predicted deliveries vs actual
        let model_delivered_count = expected_delivered.len();
        let actual_delivered_count = expected_delivered
            .iter()
            .filter(|id| output_ids.contains(id.as_str()))
            .count();

        let min_expected = model_delivered_count.saturating_sub(self.tolerance);
        let max_expected = model_delivered_count + self.tolerance;
        if actual_delivered_count < min_expected || actual_delivered_count > max_expected {
            // Report which specific IDs the model expected but didn't find
            let missing: Vec<&str> = expected_delivered
                .iter()
                .filter(|id| !output_ids.contains(id.as_str()))
                .map(String::as_str)
                .take(10)
                .collect();
            failures.push(format!(
                "model predicted {model_delivered_count} delivered, got {actual_delivered_count} (±{}). missing: {missing:?}",
                self.tolerance,
            ));
        }

        // Check: total output should be close to model prediction
        // (some output entries may be agent-internal without our IDs)
        let total_proptest_output: usize = output
            .iter()
            .filter(|e| LogFormat::extract_id(&e.message).is_some())
            .count();
        if total_proptest_output < min_expected || total_proptest_output > max_expected {
            failures.push(format!(
                "total proptest output: {total_proptest_output}, model predicted: {model_delivered_count} (±{})",
                self.tolerance,
            ));
        }

        // Check: if the model predicted any drops, the sampled count tag
        // should appear on at least one output entry
        if expected_dropped > 0 {
            let has_sampled_tag = output.iter().any(|entry| {
                entry
                    .ddtags
                    .as_deref()
                    .unwrap_or("")
                    .contains("adaptive_sampler_sampled_count:")
            });
            if !has_sampled_tag {
                failures.push(format!(
                    "model predicted {expected_dropped} drops, but no output has adaptive_sampler_sampled_count tag"
                ));
            }
        }

        if failures.is_empty() {
            Ok(())
        } else {
            Err(PropertyFailure {
                property_name: self.name().to_string(),
                description: format!("{} model violations", failures.len()),
                details: vec![
                    ("failures".to_string(), failures.join("\n")),
                    ("model_delivered".to_string(), model_delivered_count.to_string()),
                    ("model_dropped".to_string(), expected_dropped.to_string()),
                    ("actual_output".to_string(), total_proptest_output.to_string()),
                    ("credits_remaining".to_string(), format!("{credits:.1}")),
                ],
            })
        }
    }
}
