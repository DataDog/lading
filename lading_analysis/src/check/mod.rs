//! Invariant check framework.

mod completeness;
mod duplication;
mod fabrication;
mod latency;

use rustc_hash::FxHashSet;

use crate::Error;
use crate::config::CheckConfig;
use crate::context::{AnalysisContext, ContentHash};

/// Result of running a single check.
#[derive(Debug)]
pub struct CheckResult {
    /// Name of the check.
    pub name: String,
    /// Whether the check passed.
    pub passed: bool,
    /// One-line summary.
    pub summary: String,
    /// Detailed breakdown.
    pub details: Vec<String>,
}

/// Trait for all invariant checks.
pub trait Check {
    /// Human-readable name of this check.
    fn name(&self) -> &str;
    /// Run the check against the analysis context.
    fn check(&self, ctx: &AnalysisContext) -> CheckResult;
}

/// Extract the set of content hashes from reconstructed input lines.
pub(crate) fn input_line_hashes(ctx: &AnalysisContext) -> FxHashSet<ContentHash> {
    ctx.lines.iter().map(|l| l.hash).collect()
}

/// Build check instances from the config entries.
///
/// # Errors
///
/// Returns an error if a check config is invalid.
pub fn from_config(configs: &[CheckConfig]) -> Result<Vec<Box<dyn Check>>, Error> {
    let mut checks: Vec<Box<dyn Check>> = Vec::with_capacity(configs.len());
    for cfg in configs {
        match cfg {
            CheckConfig::Completeness(params) => {
                checks.push(Box::new(completeness::Completeness {
                    min_ratio: params.min_ratio,
                }));
            }
            CheckConfig::Fabrication(params) => {
                checks.push(Box::new(fabrication::Fabrication {
                    max_count: params.max_count,
                }));
            }
            CheckConfig::Duplication(params) => {
                checks.push(Box::new(duplication::Duplication {
                    max_ratio: params.max_ratio,
                }));
            }
            CheckConfig::Latency(params) => {
                checks.push(Box::new(latency::Latency {
                    max_p99_ms: params.max_p99_ms,
                }));
            }
        }
    }
    Ok(checks)
}
