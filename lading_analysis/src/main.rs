//! CLI for the lading offline analysis tool.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

/// Offline analysis of lading capture files.
///
/// Reconstructs input lines from FUSE read captures, parses output from
/// blackhole captures, and runs configurable invariant checks.
#[derive(Parser, Debug)]
#[command(name = "lading-analysis")]
struct Cli {
    /// Path to the analysis config YAML.
    #[arg(long)]
    config: PathBuf,

    /// Output results as JSON (for CI integration).
    #[arg(long, default_value_t = false)]
    json: bool,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    let config_contents = match std::fs::read_to_string(&cli.config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: cannot read config {}: {e}", cli.config.display());
            return ExitCode::FAILURE;
        }
    };

    let config: lading_analysis::config::AnalysisConfig = match serde_yaml::from_str(&config_contents) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: invalid config: {e}");
            return ExitCode::FAILURE;
        }
    };

    let results = match lading_analysis::run(&config) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: analysis failed: {e}");
            return ExitCode::FAILURE;
        }
    };

    let all_passed = results.iter().all(|r| r.passed);

    if cli.json {
        print_json(&results);
    } else {
        print_human(&results);
    }

    if all_passed {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn print_human(results: &[lading_analysis::check::CheckResult]) {
    for r in results {
        let status = if r.passed { "PASS" } else { "FAIL" };
        println!("[{status}] {}: {}", r.name, r.summary);
        for detail in &r.details {
            println!("  {detail}");
        }
        println!();
    }
}

fn print_json(results: &[lading_analysis::check::CheckResult]) {
    // Simple JSON output without pulling in serde for CheckResult
    print!("[");
    for (i, r) in results.iter().enumerate() {
        if i > 0 {
            print!(",");
        }
        // Escape the summary for JSON
        let summary = r.summary.replace('\\', "\\\\").replace('"', "\\\"");
        print!(
            "{{\"name\":\"{}\",\"passed\":{},\"summary\":\"{}\"}}",
            r.name, r.passed, summary
        );
    }
    println!("]");
}
