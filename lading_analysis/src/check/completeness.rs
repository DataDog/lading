//! Completeness check: what fraction of input lines appeared in the output.

use rustc_hash::FxHashSet;

use super::{Check, CheckResult};
use crate::context::AnalysisContext;

/// Checks that at least `min_ratio` of unique input line hashes appear in the
/// output.
pub(crate) struct Completeness {
    pub(crate) min_ratio: f64,
}

impl Check for Completeness {
    fn name(&self) -> &str {
        "completeness"
    }

    fn check(&self, ctx: &AnalysisContext) -> CheckResult {
        if ctx.input_lines.is_empty() {
            return CheckResult {
                name: self.name().into(),
                passed: true,
                summary: "no input lines to check".into(),
                details: vec![],
            };
        }

        let output_hashes: FxHashSet<u64> = ctx.output_lines.iter().map(|ol| ol.hash).collect();

        let total_input = ctx.input_lines.len() as u64;
        let matched = ctx
            .input_lines
            .keys()
            .filter(|h| output_hashes.contains(h))
            .count() as u64;

        let ratio = matched as f64 / total_input as f64;
        let passed = ratio >= self.min_ratio;

        let mut details = vec![format!(
            "matched {matched}/{total_input} unique input lines ({ratio:.4})"
        )];

        // Per-group breakdown
        let mut group_total: rustc_hash::FxHashMap<u16, u64> = rustc_hash::FxHashMap::default();
        let mut group_matched: rustc_hash::FxHashMap<u16, u64> = rustc_hash::FxHashMap::default();
        for il in ctx.input_lines.values() {
            *group_total.entry(il.group_id).or_default() += 1;
            if output_hashes.contains(&il.hash) {
                *group_matched.entry(il.group_id).or_default() += 1;
            }
        }
        let mut groups: Vec<u16> = group_total.keys().copied().collect();
        groups.sort_unstable();
        for gid in groups {
            let t = group_total.get(&gid).copied().unwrap_or(0);
            let m = group_matched.get(&gid).copied().unwrap_or(0);
            let r = if t > 0 { m as f64 / t as f64 } else { 1.0 };
            details.push(format!("  group {gid}: {m}/{t} ({r:.4})"));
        }

        CheckResult {
            name: self.name().into(),
            passed,
            summary: format!("completeness {ratio:.4} (threshold {:.4})", self.min_ratio),
            details,
        }
    }
}
