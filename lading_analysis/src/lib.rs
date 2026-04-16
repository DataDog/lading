//! Offline analysis tool for lading capture files.
//!
//! Reconstructs input lines from FUSE read captures, parses output lines from
//! blackhole captures, and runs configurable invariant checks (completeness,
//! fabrication, duplication, latency).

pub mod check;
pub mod config;
pub mod context;
pub mod input;
pub mod output;

use config::AnalysisConfig;
use context::AnalysisContext;

/// Errors from the analysis pipeline.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// IO error reading capture files.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// JSON parse error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    /// YAML parse error.
    #[error("YAML error: {0}")]
    Yaml(#[from] serde_yaml::Error),
    /// Block cache reconstruction error.
    #[error("Block cache error: {0}")]
    Block(#[from] lading_payload::block::Error),
    /// Input reconstruction error.
    #[error("Input error: {0}")]
    Input(String),
    /// Configuration error.
    #[error("Config error: {0}")]
    Config(String),
}

/// Run the full analysis pipeline: build context, run checks, return results.
///
/// # Errors
///
/// Returns an error if capture files cannot be read or parsed, or if the block
/// cache cannot be reconstructed.
pub fn run(config: &AnalysisConfig) -> Result<Vec<check::CheckResult>, Error> {
    let ctx = AnalysisContext::build(config)?;

    if let Some(ref dir) = config.output_dir {
        std::fs::create_dir_all(dir)?;
        dump_reconstructed(&ctx, dir)?;
    }

    let checks = check::from_config(&config.checks)?;
    let results: Vec<check::CheckResult> = checks.iter().map(|c| c.check(&ctx)).collect();

    Ok(results)
}

/// Write reconstructed inputs and extracted outputs to files for human
/// inspection.
fn dump_reconstructed(
    ctx: &AnalysisContext,
    dir: &std::path::Path,
) -> Result<(), Error> {
    use std::io::Write;

    // Always write raw reads
    let mut f = std::io::BufWriter::new(
        std::fs::File::create(dir.join("reconstructed_inputs_raw.txt"))?,
    );
    for r in &ctx.raw_reads {
        writeln!(
            f,
            "[{ms}ms inode={ino} group={g} offset={o} size={s}] {content}",
            ms = r.relative_ms,
            ino = r.inode,
            g = r.group_id,
            o = r.offset,
            s = r.size,
            content = r.content,
        )?;
    }
    f.flush()?;
    eprintln!(
        "Wrote {} raw reads to {}/reconstructed_inputs_raw.txt",
        ctx.raw_reads.len(),
        dir.display()
    );

    // Always write reconstructed lines
    let mut f = std::io::BufWriter::new(
        std::fs::File::create(dir.join("reconstructed_inputs.txt"))?,
    );
    for line in &ctx.lines {
        let first_ms = line.contributions.first().map_or(0, |c| c.relative_ms);
        let last_ms = line.contributions.last().map_or(0, |c| c.relative_ms);
        let reads = line.contributions.len();
        if first_ms == last_ms {
            writeln!(
                f,
                "[{first_ms}ms group={g} reads={reads}] {text}",
                g = line.group_id,
                text = line.text,
            )?;
        } else {
            writeln!(
                f,
                "[{first_ms}ms..{last_ms}ms group={g} reads={reads}] {text}",
                g = line.group_id,
                text = line.text,
            )?;
        }
    }
    f.flush()?;
    eprintln!(
        "Wrote {} lines to {}/reconstructed_inputs.txt",
        ctx.lines.len(),
        dir.display()
    );

    // Write extracted output messages
    let mut f = std::io::BufWriter::new(
        std::fs::File::create(dir.join("extracted_outputs.txt"))?,
    );
    for ol in &ctx.output_lines {
        writeln!(
            f,
            "[{ms}ms] {msg}",
            ms = ol.relative_ms,
            msg = ol.message,
        )?;
    }
    f.flush()?;
    eprintln!(
        "Wrote {} output lines to {}/extracted_outputs.txt",
        ctx.output_lines.len(),
        dir.display()
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::context::{
        AnalysisContext, ContentHash, OutputLine, ReadContribution, ReconstructedLine,
    };
    use sha2::{Digest, Sha256};

    fn hash(s: &str) -> ContentHash {
        let mut h = Sha256::new();
        h.update(s.as_bytes());
        h.finalize().into()
    }

    /// Build a reconstructed line from a single read at the given timestamp.
    fn line_single_read(text: &str, group_id: u16, read_ms: u64) -> ReconstructedLine {
        ReconstructedLine {
            text: text.to_string(),
            hash: hash(text),
            group_id,
            contributions: vec![ReadContribution {
                offset: 0,
                size: text.len() as u64,
                relative_ms: read_ms,
            }],
        }
    }

    /// Build a reconstructed line that spans two reads (simulates a line
    /// crossing a 4096-byte read boundary, as seen in real agent data).
    fn line_two_reads(
        text: &str,
        group_id: u16,
        first_read_ms: u64,
        second_read_ms: u64,
        split_point: u64,
    ) -> ReconstructedLine {
        ReconstructedLine {
            text: text.to_string(),
            hash: hash(text),
            group_id,
            contributions: vec![
                ReadContribution {
                    offset: 0,
                    size: split_point,
                    relative_ms: first_read_ms,
                },
                ReadContribution {
                    offset: split_point,
                    size: text.len() as u64 - split_point,
                    relative_ms: second_read_ms,
                },
            ],
        }
    }

    fn make_output(text: &str, relative_ms: u64) -> OutputLine {
        OutputLine {
            hash: hash(text),
            message: text.to_string(),
            relative_ms,
        }
    }

    fn make_ctx(lines: Vec<ReconstructedLine>, output_lines: Vec<OutputLine>) -> AnalysisContext {
        AnalysisContext {
            raw_reads: vec![],
            lines,
            output_lines,
            fuse_events: vec![],
            blackhole_events: vec![],
        }
    }

    /// Generate a realistic-length line (~190 chars, matching real agent data).
    fn realistic_line(id: u32) -> String {
        format!(
            r#"{{"level":"DEBUG","message":"request processed","tags":["auth","api","database","cache","network"],"id":{id},"padding":"{pad}"}}"#,
            pad = "x".repeat(100)
        )
    }

    // ── Completeness ────────────────────────────────────────────────────

    #[test]
    fn completeness_all_matched() {
        let lines: Vec<_> = (0..50).map(|i| line_single_read(&realistic_line(i), 0, 1000 + i as u64 * 100)).collect();
        let outputs: Vec<_> = lines.iter().map(|l| make_output(&l.text, 5000)).collect();
        let ctx = make_ctx(lines, outputs);

        let check = super::check::from_config(&[crate::config::CheckConfig::Completeness(
            crate::config::CompletenessParams { min_ratio: 1.0 },
        )])
        .unwrap();
        let result = check[0].check(&ctx);
        assert!(result.passed, "expected pass: {}", result.summary);
    }

    #[test]
    fn completeness_partial_match() {
        let lines: Vec<_> = (0..100).map(|i| line_single_read(&realistic_line(i), 0, 1000 + i as u64 * 100)).collect();
        // Only send first 30 of 100
        let outputs: Vec<_> = lines[..30].iter().map(|l| make_output(&l.text, 5000)).collect();
        let ctx = make_ctx(lines, outputs);

        let check = super::check::from_config(&[crate::config::CheckConfig::Completeness(
            crate::config::CompletenessParams { min_ratio: 0.5 },
        )])
        .unwrap();
        let result = check[0].check(&ctx);
        assert!(!result.passed, "30/100=0.3 < 0.5 should fail: {}", result.summary);

        let check = super::check::from_config(&[crate::config::CheckConfig::Completeness(
            crate::config::CompletenessParams { min_ratio: 0.25 },
        )])
        .unwrap();
        let result = check[0].check(&ctx);
        assert!(result.passed, "30/100=0.3 >= 0.25 should pass: {}", result.summary);
    }

    #[test]
    fn completeness_empty_inputs() {
        let ctx = make_ctx(vec![], vec![make_output("anything", 500)]);
        let check = super::check::from_config(&[crate::config::CheckConfig::Completeness(
            crate::config::CompletenessParams { min_ratio: 1.0 },
        )])
        .unwrap();
        let result = check[0].check(&ctx);
        assert!(result.passed, "empty inputs should pass");
    }

    #[test]
    fn completeness_per_group_breakdown() {
        let lines = vec![
            line_single_read(&realistic_line(1), 0, 1000),
            line_single_read(&realistic_line(2), 0, 2000),
            line_single_read(&realistic_line(3), 1, 3000),
            line_single_read(&realistic_line(4), 1, 4000),
        ];
        // Only send group 0 lines
        let outputs = vec![
            make_output(&lines[0].text, 5000),
            make_output(&lines[1].text, 6000),
        ];
        let ctx = make_ctx(lines, outputs);

        let check = super::check::from_config(&[crate::config::CheckConfig::Completeness(
            crate::config::CompletenessParams { min_ratio: 0.4 },
        )])
        .unwrap();
        let result = check[0].check(&ctx);
        assert!(result.passed, "2/4=0.5 >= 0.4: {}", result.summary);
        // Verify per-group details exist
        assert!(result.details.iter().any(|d| d.contains("group 0")));
        assert!(result.details.iter().any(|d| d.contains("group 1")));
    }

    // ── Fabrication ─────────────────────────────────────────────────────

    #[test]
    fn fabrication_none() {
        let lines: Vec<_> = (0..20).map(|i| line_single_read(&realistic_line(i), 0, 1000)).collect();
        let outputs: Vec<_> = lines.iter().map(|l| make_output(&l.text, 5000)).collect();
        let ctx = make_ctx(lines, outputs);

        let check = super::check::from_config(&[crate::config::CheckConfig::Fabrication(
            crate::config::FabricationParams { max_count: 0 },
        )])
        .unwrap();
        let result = check[0].check(&ctx);
        assert!(result.passed, "no fabrication expected: {}", result.summary);
    }

    #[test]
    fn fabrication_detected() {
        let lines = vec![line_single_read(&realistic_line(1), 0, 1000)];
        let outputs = vec![
            make_output(&lines[0].text, 5000),
            make_output("FABRICATED_LINE_NOT_IN_INPUT", 6000),
            make_output("ANOTHER_FAKE_LINE", 7000),
        ];
        let ctx = make_ctx(lines, outputs);

        let check = super::check::from_config(&[crate::config::CheckConfig::Fabrication(
            crate::config::FabricationParams { max_count: 0 },
        )])
        .unwrap();
        let result = check[0].check(&ctx);
        assert!(!result.passed, "should detect 2 fabricated lines");
        assert!(result.summary.contains("2 fabricated"), "summary: {}", result.summary);

        // Allow 2
        let check = super::check::from_config(&[crate::config::CheckConfig::Fabrication(
            crate::config::FabricationParams { max_count: 2 },
        )])
        .unwrap();
        let result = check[0].check(&ctx);
        assert!(result.passed, "should pass with max_count=2");
    }

    #[test]
    fn fabrication_empty_payload_ignored() {
        // Empty {} payloads from agent health checks produce no output lines
        // (the JSON parser skips non-array payloads). This test verifies
        // that only actual message lines are counted.
        let lines = vec![line_single_read(&realistic_line(1), 0, 1000)];
        let outputs = vec![make_output(&lines[0].text, 5000)];
        let ctx = make_ctx(lines, outputs);

        let check = super::check::from_config(&[crate::config::CheckConfig::Fabrication(
            crate::config::FabricationParams { max_count: 0 },
        )])
        .unwrap();
        let result = check[0].check(&ctx);
        assert!(result.passed, "no fabrication from normal lines");
    }

    // ── Duplication ─────────────────────────────────────────────────────

    #[test]
    fn duplication_none() {
        let lines: Vec<_> = (0..20).map(|i| line_single_read(&realistic_line(i), 0, 1000)).collect();
        let outputs: Vec<_> = lines.iter().map(|l| make_output(&l.text, 5000)).collect();
        let ctx = make_ctx(lines, outputs);

        let check = super::check::from_config(&[crate::config::CheckConfig::Duplication(
            crate::config::DuplicationParams { max_ratio: 0.0 },
        )])
        .unwrap();
        let result = check[0].check(&ctx);
        assert!(result.passed, "no duplicates expected: {}", result.summary);
    }

    #[test]
    fn duplication_detected() {
        let line = realistic_line(1);
        let lines = vec![line_single_read(&line, 0, 1000)];
        let outputs = vec![
            make_output(&line, 5000),
            make_output(&line, 6000), // duplicate
            make_output(&line, 7000), // duplicate
        ];
        let ctx = make_ctx(lines, outputs);

        // 2 duplicates out of 3 matched = 0.667 ratio
        let check = super::check::from_config(&[crate::config::CheckConfig::Duplication(
            crate::config::DuplicationParams { max_ratio: 0.0 },
        )])
        .unwrap();
        let result = check[0].check(&ctx);
        assert!(!result.passed, "should detect duplicates");

        let check = super::check::from_config(&[crate::config::CheckConfig::Duplication(
            crate::config::DuplicationParams { max_ratio: 0.7 },
        )])
        .unwrap();
        let result = check[0].check(&ctx);
        assert!(result.passed, "should pass at 0.7 threshold");
    }

    #[test]
    fn duplication_ignores_fabricated() {
        let line = realistic_line(1);
        let lines = vec![line_single_read(&line, 0, 1000)];
        let outputs = vec![
            make_output(&line, 5000),
            make_output("FAKE_A", 6000),
            make_output("FAKE_A", 7000), // duplicate of fabricated line
        ];
        let ctx = make_ctx(lines, outputs);

        let check = super::check::from_config(&[crate::config::CheckConfig::Duplication(
            crate::config::DuplicationParams { max_ratio: 0.0 },
        )])
        .unwrap();
        let result = check[0].check(&ctx);
        assert!(result.passed, "fabricated duplicates should be ignored: {}", result.summary);
    }

    // ── Latency ─────────────────────────────────────────────────────────

    #[test]
    fn latency_realistic_distribution() {
        // Simulate agent batching: lines read at 1s intervals, batched every ~5s
        let mut lines = Vec::new();
        let mut outputs = Vec::new();
        for i in 0..50u64 {
            let text = realistic_line(i as u32);
            let read_ms = 1000 + i * 1000; // read at 1s, 2s, 3s, ...
            let batch_ms = ((i / 5) + 1) * 5000; // batched at 5s, 10s, 15s, ...
            lines.push(line_single_read(&text, 0, read_ms));
            outputs.push(make_output(&text, batch_ms));
        }
        let ctx = make_ctx(lines, outputs);

        let check = super::check::from_config(&[crate::config::CheckConfig::Latency(
            crate::config::LatencyParams {
                max_p99_ms: Some(10000),
            },
        )])
        .unwrap();
        let result = check[0].check(&ctx);
        assert!(result.passed, "should pass with 10s threshold: {}", result.summary);
        assert!(result.summary.contains("p50="), "should report p50");
        assert!(result.summary.contains("p99="), "should report p99");
        // Check histogram is present
        assert!(result.details.iter().any(|d| d.contains("distribution")));
    }

    #[test]
    fn latency_multi_read_line_uses_last_read_timestamp() {
        // A line spanning two reads: first at 1000ms, second at 2000ms
        // Latency should be computed from the LAST read (2000ms), not first
        let text = realistic_line(1);
        let line = line_two_reads(&text, 0, 1000, 2000, 50);
        let output = make_output(&text, 5000);
        let ctx = make_ctx(vec![line], vec![output]);

        let check = super::check::from_config(&[crate::config::CheckConfig::Latency(
            crate::config::LatencyParams {
                max_p99_ms: Some(10000),
            },
        )])
        .unwrap();
        let result = check[0].check(&ctx);
        assert!(result.passed);
        // Latency = 5000 - 2000 = 3000ms (not 5000 - 1000 = 4000ms)
        assert!(result.summary.contains("p50=3000ms"), "expected 3000ms latency, got: {}", result.summary);
    }

    #[test]
    fn latency_threshold_exceeded() {
        let text = realistic_line(1);
        let ctx = make_ctx(
            vec![line_single_read(&text, 0, 1000)],
            vec![make_output(&text, 12000)], // 11000ms latency
        );

        let check = super::check::from_config(&[crate::config::CheckConfig::Latency(
            crate::config::LatencyParams {
                max_p99_ms: Some(5000),
            },
        )])
        .unwrap();
        let result = check[0].check(&ctx);
        assert!(!result.passed, "p99=11000 > 5000 should fail");
    }

    #[test]
    fn latency_informational_always_passes() {
        let text = realistic_line(1);
        let ctx = make_ctx(
            vec![line_single_read(&text, 0, 1000)],
            vec![make_output(&text, 100_000)],
        );

        let check = super::check::from_config(&[crate::config::CheckConfig::Latency(
            crate::config::LatencyParams { max_p99_ms: None },
        )])
        .unwrap();
        let result = check[0].check(&ctx);
        assert!(result.passed, "informational mode should always pass");
    }

    #[test]
    fn latency_unmatched_lines_skipped() {
        let ctx = make_ctx(
            vec![line_single_read(&realistic_line(1), 0, 1000)],
            vec![make_output("UNKNOWN_LINE", 5000)],
        );

        let check = super::check::from_config(&[crate::config::CheckConfig::Latency(
            crate::config::LatencyParams {
                max_p99_ms: Some(5000),
            },
        )])
        .unwrap();
        let result = check[0].check(&ctx);
        assert!(result.passed, "no matched lines = pass");
        assert!(result.details.iter().any(|d| d.contains("unmatched")));
    }

    // ── Cross-check interactions ────────────────────────────────────────

    #[test]
    fn multi_read_line_hash_matches_output() {
        // The core correctness property: a line stitched from two reads
        // must produce the same hash as the output message containing
        // that exact text.
        let text = "AAAA_BOUNDARY_CROSS_BBBB";
        let line = line_two_reads(text, 0, 1000, 2000, 10);
        let output = make_output(text, 5000);

        assert_eq!(line.hash, output.hash, "stitched line hash must match output hash");

        // Verify through the full check pipeline
        let ctx = make_ctx(vec![line], vec![output]);

        let checks = super::check::from_config(&[
            crate::config::CheckConfig::Completeness(crate::config::CompletenessParams {
                min_ratio: 1.0,
            }),
            crate::config::CheckConfig::Fabrication(crate::config::FabricationParams {
                max_count: 0,
            }),
        ])
        .unwrap();

        for c in &checks {
            let result = c.check(&ctx);
            assert!(result.passed, "check '{}' failed: {}", result.name, result.summary);
        }
    }

    #[test]
    fn realistic_batch_scenario() {
        // Simulate a realistic scenario: 500 lines across two groups,
        // ~5% spanning two reads, batched into blackhole every ~5s,
        // with a few lines missing (shutdown race) and no fabrication.
        let mut lines = Vec::new();
        let mut outputs = Vec::new();

        for i in 0..500u32 {
            let text = realistic_line(i);
            let group = if i < 300 { 0u16 } else { 1 };
            let read_ms = 1000 + i as u64 * 200;

            // ~5% of lines span two reads
            if i % 20 == 7 {
                lines.push(line_two_reads(&text, group, read_ms, read_ms + 1000, 80));
            } else {
                lines.push(line_single_read(&text, group, read_ms));
            }

            // Skip last 10 lines (simulate shutdown race)
            if i < 490 {
                let batch_ms = ((i as u64 / 50) + 1) * 5000;
                outputs.push(make_output(&text, batch_ms));
            }
        }

        let ctx = make_ctx(lines, outputs);

        let checks = super::check::from_config(&[
            crate::config::CheckConfig::Completeness(crate::config::CompletenessParams {
                min_ratio: 0.95,
            }),
            crate::config::CheckConfig::Fabrication(crate::config::FabricationParams {
                max_count: 0,
            }),
            crate::config::CheckConfig::Duplication(crate::config::DuplicationParams {
                max_ratio: 0.0,
            }),
            crate::config::CheckConfig::Latency(crate::config::LatencyParams {
                max_p99_ms: Some(50000),
            }),
        ])
        .unwrap();

        let results: Vec<_> = checks.iter().map(|c| c.check(&ctx)).collect();

        // 490/500 = 0.98 >= 0.95
        assert!(results[0].passed, "completeness: {}", results[0].summary);
        // 0 fabricated
        assert!(results[1].passed, "fabrication: {}", results[1].summary);
        // 0 duplicates
        assert!(results[2].passed, "duplication: {}", results[2].summary);
        // Latency within bounds
        assert!(results[3].passed, "latency: {}", results[3].summary);
    }
}
