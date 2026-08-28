//! Failure diagnostics formatting.
//!
//! When a property fails, format the failure with enough context for debugging:
//! input lines, output entries, property details, and temp directory path.

use std::fmt::Write;

use crate::log_format::LogFormat;
use crate::orchestrator::TestCaseResult;

/// Format a test case result for display when a property has failed.
#[must_use]
pub fn format_failure(result: &TestCaseResult) -> String {
    let mut out = String::new();

    out.push_str("=== PROPERTY TEST FAILURE ===\n\n");

    for prop_result in &result.property_results {
        if let Err(failure) = prop_result {
            let _ = writeln!(out, "FAILED: {failure}");
        }
    }

    let _ = write!(
        out,
        "\n--- Input ({} lines, format: {:?}) ---\n",
        result.input.lines.len(),
        result.input.format,
    );
    let max_show = 20;
    for (i, line) in result.input.lines.iter().take(max_show).enumerate() {
        if line.content.len() > 120 {
            let _ = writeln!(out, "  [{i}] {}...", &line.content[..120]);
        } else {
            let _ = writeln!(out, "  [{i}] {}", line.content);
        }
    }
    if result.input.lines.len() > max_show {
        let _ = writeln!(
            out,
            "  ... and {} more lines",
            result.input.lines.len() - max_show
        );
    }

    let _ = write!(
        out,
        "\n--- Output ({} entries) ---\n",
        result.output.len(),
    );
    for (i, entry) in result.output.iter().take(max_show).enumerate() {
        let id = LogFormat::extract_id(&entry.message).unwrap_or("???");
        if entry.message.len() > 120 {
            let _ = writeln!(out, "  [{i}] id={id} msg={}...", &entry.message[..120]);
        } else {
            let _ = writeln!(out, "  [{i}] id={id} msg={}", entry.message);
        }
    }
    if result.output.len() > max_show {
        let _ = writeln!(
            out,
            "  ... and {} more entries",
            result.output.len() - max_show
        );
    }

    let _ = write!(
        out,
        "\n--- Debug ---\nTemp dir (preserved): {}\n",
        result.temp_dir().display(),
    );

    out
}
