//! Invariant check framework.
//!
//! Each check implements the [`Check`] trait and is constructed from its YAML
//! config parameters. The [`from_config`] function maps config entries to check
//! instances.

mod completeness;
mod duplication;
mod fabrication;

use crate::Error;
use crate::config::CheckConfig;
use crate::context::AnalysisContext;

/// Result of running a single check.
#[derive(Debug)]
pub struct CheckResult {
    /// Name of the check.
    pub name: String,
    /// Whether the check passed.
    pub passed: bool,
    /// One-line summary.
    pub summary: String,
    /// Detailed breakdown (per-group stats, example failures, etc.).
    pub details: Vec<String>,
}

/// Trait for all invariant checks.
pub trait Check {
    /// Human-readable name of this check.
    fn name(&self) -> &str;
    /// Run the check against the analysis context.
    fn check(&self, ctx: &AnalysisContext) -> CheckResult;
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
        }
    }
    Ok(checks)
}
