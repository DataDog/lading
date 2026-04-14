//! Fabrication check: output lines that don't match any input line.

use super::{Check, CheckResult, input_line_hashes};
use crate::context::{AnalysisContext, ReconstructedInput};

/// Checks that at most `max_count` output lines have no matching input line.
pub(crate) struct Fabrication {
    pub(crate) max_count: u64,
}

impl Check for Fabrication {
    fn name(&self) -> &str {
        "fabrication"
    }

    fn check(&self, ctx: &AnalysisContext) -> CheckResult {
        let input_hashes = input_line_hashes(&ctx.input);

        if matches!(&ctx.input, ReconstructedInput::Raw(_)) {
            return CheckResult {
                name: self.name().into(),
                passed: false,
                summary: "raw mode: line-level checks require newline_delimited reconstruction".into(),
                details: vec![],
            };
        }

        let mut fabricated_count: u64 = 0;
        let mut examples: Vec<String> = Vec::new();

        for ol in &ctx.output_lines {
            if !input_hashes.contains(&ol.hash) {
                fabricated_count += 1;
                if examples.len() < 5 {
                    let preview: String = ol.message.chars().take(120).collect();
                    examples.push(format!(
                        "  at {ms}ms: \"{preview}...\"",
                        ms = ol.relative_ms,
                    ));
                }
            }
        }

        let passed = fabricated_count <= self.max_count;

        let mut details = vec![format!(
            "{fabricated_count} fabricated lines out of {} total output lines",
            ctx.output_lines.len()
        )];
        if !examples.is_empty() {
            details.push("examples:".into());
            details.extend(examples);
        }

        CheckResult {
            name: self.name().into(),
            passed,
            summary: format!(
                "{fabricated_count} fabricated (max {})",
                self.max_count
            ),
            details,
        }
    }
}
