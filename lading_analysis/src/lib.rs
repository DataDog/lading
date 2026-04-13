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

    // Write reconstructed data for human inspection if output_dir is set
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

    // Sort input lines by first_seen_ms for readable ordering
    let mut inputs: Vec<_> = ctx.input_lines.values().collect();
    inputs.sort_by_key(|il| il.first_seen_ms);

    let mut f = std::io::BufWriter::new(std::fs::File::create(dir.join("reconstructed_inputs.txt"))?);
    for il in &inputs {
        writeln!(
            f,
            "[{ms}ms group={g} count={c}] {text}",
            ms = il.first_seen_ms,
            g = il.group_id,
            c = il.count,
            text = il.text,
        )?;
    }
    f.flush()?;

    // Write extracted output messages in order of receipt
    let mut f = std::io::BufWriter::new(std::fs::File::create(dir.join("extracted_outputs.txt"))?);
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
        "Wrote {} input lines to {}/reconstructed_inputs.txt",
        inputs.len(),
        dir.display()
    );
    eprintln!(
        "Wrote {} output lines to {}/extracted_outputs.txt",
        ctx.output_lines.len(),
        dir.display()
    );

    Ok(())
}
