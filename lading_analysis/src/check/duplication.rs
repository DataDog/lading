//! Duplication check: output lines that match the same input more than once.

use rustc_hash::FxHashMap;

use super::{Check, CheckResult, input_line_hashes};
use crate::context::{AnalysisContext, ContentHash, ReconstructedInput};

/// Checks that duplicated output lines stay below `max_ratio` of total output.
pub(crate) struct Duplication {
    pub(crate) max_ratio: f64,
}

impl Check for Duplication {
    fn name(&self) -> &str {
        "duplication"
    }

    fn check(&self, ctx: &AnalysisContext) -> CheckResult {
        if matches!(&ctx.input, ReconstructedInput::Raw(_)) {
            return CheckResult {
                name: self.name().into(),
                passed: false,
                summary: "raw mode: line-level checks require newline_delimited reconstruction".into(),
                details: vec![],
            };
        }

        if ctx.output_lines.is_empty() {
            return CheckResult {
                name: self.name().into(),
                passed: true,
                summary: "no output lines to check".into(),
                details: vec![],
            };
        }

        let input_hashes = input_line_hashes(&ctx.input);

        let mut output_counts: FxHashMap<ContentHash, u64> = FxHashMap::default();
        for ol in &ctx.output_lines {
            *output_counts.entry(ol.hash).or_default() += 1;
        }

        let mut total_matched: u64 = 0;
        let mut duplicate_count: u64 = 0;
        let mut worst_dupes: Vec<(ContentHash, u64)> = Vec::new();

        for (&hash, &count) in &output_counts {
            if input_hashes.contains(&hash) {
                total_matched += count;
                if count > 1 {
                    duplicate_count += count - 1;
                    worst_dupes.push((hash, count));
                }
            }
        }

        let ratio = if total_matched > 0 {
            duplicate_count as f64 / total_matched as f64
        } else {
            0.0
        };
        let passed = ratio <= self.max_ratio;

        let mut details = vec![format!(
            "{duplicate_count} duplicate lines out of {total_matched} matched output lines ({ratio:.4})"
        )];

        worst_dupes.sort_by(|a, b| b.1.cmp(&a.1));
        worst_dupes.truncate(5);
        if !worst_dupes.is_empty() {
            details.push("most duplicated:".into());
            for (hash, count) in &worst_dupes {
                let short_hash: String = hash.iter().take(8).map(|b| format!("{b:02x}")).collect();
                details.push(format!("  hash {short_hash}...: {count} occurrences"));
            }
        }

        CheckResult {
            name: self.name().into(),
            passed,
            summary: format!(
                "duplication ratio {ratio:.4} (max {:.4})",
                self.max_ratio
            ),
            details,
        }
    }
}
