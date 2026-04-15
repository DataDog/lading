//! Latency distribution check: measures per-line latency from FUSE read to
//! blackhole receipt.

use rustc_hash::FxHashMap;

use super::{Check, CheckResult};
use crate::context::{AnalysisContext, ContentHash};

/// Measures per-line latency and reports the distribution. Optionally fails if
/// p99 exceeds a configured threshold.
pub(crate) struct Latency {
    pub(crate) max_p99_ms: Option<u64>,
}

/// Compute a percentile from a sorted slice.
fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64) * p / 100.0).ceil() as usize;
    sorted[idx.saturating_sub(1).min(sorted.len() - 1)]
}

impl Check for Latency {
    fn name(&self) -> &str {
        "latency"
    }

    fn check(&self, ctx: &AnalysisContext) -> CheckResult {
        // Build lookup from hash -> last contributing read timestamp
        let mut input_read_ms: FxHashMap<ContentHash, u64> = FxHashMap::default();
        for line in &ctx.lines {
            if let Some(last) = line.contributions.last() {
                input_read_ms.insert(line.hash, last.relative_ms);
            }
        }

        // Compute per-line latency
        let mut latencies: Vec<u64> = Vec::new();
        let mut unmatched: u64 = 0;

        for ol in &ctx.output_lines {
            if let Some(&read_ms) = input_read_ms.get(&ol.hash) {
                let latency = ol.relative_ms.saturating_sub(read_ms);
                latencies.push(latency);
            } else {
                unmatched += 1;
            }
        }

        if latencies.is_empty() {
            return CheckResult {
                name: self.name().into(),
                passed: true,
                summary: "no matched lines to measure latency".into(),
                details: vec![format!("unmatched output lines: {unmatched}")],
            };
        }

        latencies.sort_unstable();

        let min = latencies[0];
        let max = latencies[latencies.len() - 1];
        let p50 = percentile(&latencies, 50.0);
        let p95 = percentile(&latencies, 95.0);
        let p99 = percentile(&latencies, 99.0);

        let passed = match self.max_p99_ms {
            Some(threshold) => p99 <= threshold,
            None => true,
        };

        let threshold_str = match self.max_p99_ms {
            Some(t) => format!(" (max {t}ms)"),
            None => String::new(),
        };

        let summary = format!("p50={p50}ms p95={p95}ms p99={p99}ms{threshold_str}");

        let mut details = vec![
            format!(
                "min={min}ms max={max}ms matched={} unmatched={unmatched}",
                latencies.len()
            ),
        ];

        // Histogram in 1-second buckets
        let max_bucket = (max / 1000) + 1;
        let mut buckets: Vec<u64> = vec![0; max_bucket as usize];
        for &lat in &latencies {
            let bucket = (lat / 1000) as usize;
            if bucket < buckets.len() {
                buckets[bucket] += 1;
            }
        }
        let bucket_strs: Vec<String> = buckets
            .iter()
            .enumerate()
            .filter(|&(_, count)| *count > 0)
            .map(|(i, count)| format!("{}-{}s: {count}", i, i + 1))
            .collect();
        details.push(format!("distribution: [{}]", bucket_strs.join(", ")));

        CheckResult {
            name: self.name().into(),
            passed,
            summary,
            details,
        }
    }
}
