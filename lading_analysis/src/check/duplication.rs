//! Duplication check: output lines that match the same input more than once.

use rustc_hash::FxHashMap;

use super::{Check, CheckResult};
use crate::context::AnalysisContext;

/// Checks that duplicated output lines stay below `max_ratio` of total output.
pub(crate) struct Duplication {
    pub(crate) max_ratio: f64,
}

impl Check for Duplication {
    fn name(&self) -> &str {
        "duplication"
    }

    fn check(&self, ctx: &AnalysisContext) -> CheckResult {
        if ctx.output_lines.is_empty() {
            return CheckResult {
                name: self.name().into(),
                passed: true,
                summary: "no output lines to check".into(),
                details: vec![],
            };
        }

        // Count how many times each hash appears in the output
        let mut output_counts: FxHashMap<u64, u64> = FxHashMap::default();
        for ol in &ctx.output_lines {
            *output_counts.entry(ol.hash).or_default() += 1;
        }

        // Only consider hashes that also exist in input (fabricated lines are
        // handled by the fabrication check)
        let mut total_matched: u64 = 0;
        let mut duplicate_count: u64 = 0;
        let mut worst_dupes: Vec<(u64, u64)> = Vec::new(); // (hash, count)

        for (&hash, &count) in &output_counts {
            if ctx.input_lines.contains_key(&hash) {
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

        // Show the worst offenders
        worst_dupes.sort_by(|a, b| b.1.cmp(&a.1));
        worst_dupes.truncate(5);
        if !worst_dupes.is_empty() {
            details.push("most duplicated:".into());
            for (hash, count) in &worst_dupes {
                details.push(format!("  hash {hash:#018x}: {count} occurrences"));
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
