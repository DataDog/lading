//! Offline analysis tool for lading capture files.
//!
//! Reconstructs input lines from FUSE read captures, parses output lines from
//! blackhole captures, and runs configurable invariant checks (completeness,
//! fabrication, duplication).

pub mod check;
pub mod config;
pub mod context;
pub mod input;
pub mod output;

use config::AnalysisConfig;
use context::{AnalysisContext, ReconstructedInput};

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

    match &ctx.input {
        ReconstructedInput::Raw(reads) => {
            let mut f = std::io::BufWriter::new(
                std::fs::File::create(dir.join("reconstructed_inputs_raw.txt"))?,
            );
            for r in reads {
                let short_hash: String = r.hash.iter().take(8).map(|b| format!("{b:02x}")).collect();
                writeln!(
                    f,
                    "[{ms}ms inode={ino} group={g} offset={o} size={s}] sha256={h}...",
                    ms = r.relative_ms,
                    ino = r.inode,
                    g = r.group_id,
                    o = r.offset,
                    s = r.size,
                    h = short_hash,
                )?;
            }
            f.flush()?;
            eprintln!(
                "Wrote {} raw reads to {}/reconstructed_inputs_raw.txt",
                reads.len(),
                dir.display()
            );
        }
        ReconstructedInput::NewlineDelimited(lines) => {
            let mut f = std::io::BufWriter::new(
                std::fs::File::create(dir.join("reconstructed_inputs.txt"))?,
            );
            for line in lines {
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
                lines.len(),
                dir.display()
            );
        }
    }

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
