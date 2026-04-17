//! YAML configuration for the analysis tool.

use serde::Deserialize;
use std::path::PathBuf;

/// Top-level analysis configuration.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnalysisConfig {
    /// Paths to input files.
    pub inputs: Inputs,
    /// Optional directory to write reconstructed data for human inspection.
    #[serde(default)]
    pub output_dir: Option<PathBuf>,
    /// Checks to run.
    #[serde(with = "serde_yaml::with::singleton_map_recursive")]
    pub checks: Vec<CheckConfig>,
}

/// Paths to capture files and the lading config used for the experiment.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Inputs {
    /// Path to the FUSE read capture JSONL file.
    pub fuse_capture: PathBuf,
    /// Path to the blackhole capture JSONL file.
    pub blackhole_capture: PathBuf,
    /// Path to the lading config YAML (needed for block cache reconstruction).
    pub lading_config: PathBuf,
}

/// A single check configuration entry.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckConfig {
    /// Completeness check: fraction of input lines present in output.
    Completeness(CompletenessParams),
    /// Fabrication check: output lines not matching any input.
    Fabrication(FabricationParams),
    /// Duplication check: output lines matching the same input more than once.
    Duplication(DuplicationParams),
    /// Latency distribution check: measures per-line latency from FUSE read to blackhole receipt.
    Latency(LatencyParams),
    /// Truncation check: verifies the downstream system correctly truncated
    /// oversized lines emitted by the `truncation_test` payload variant.
    Truncation(TruncationParams),
}

/// Parameters for the completeness check.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletenessParams {
    /// Minimum fraction of input lines that must appear in output (0.0 to 1.0).
    pub min_ratio: f64,
}

/// Parameters for the fabrication check.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FabricationParams {
    /// Maximum number of output lines allowed that don't match any input.
    pub max_count: u64,
}

/// Parameters for the duplication check.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DuplicationParams {
    /// Maximum fraction of output lines that are duplicates (0.0 to 1.0).
    pub max_ratio: f64,
}

/// Parameters for the latency distribution check.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LatencyParams {
    /// If set, fail when the p99 latency exceeds this threshold (milliseconds).
    /// If omitted, the check is informational (always passes).
    #[serde(default)]
    pub max_p99_ms: Option<u64>,
}

/// Parameters for the truncation check.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TruncationParams {
    /// The downstream system's configured max message size, in bytes. Lines
    /// longer than this are expected to be split into multiple output messages
    /// with `...TRUNCATED...` markers. Default is 900_000 (DD logs agent default).
    #[serde(default = "default_max_message_size")]
    pub max_message_size: u64,
}

fn default_max_message_size() -> u64 {
    900_000
}
