//! The analysis context: parsed and reconstructed data from both capture files.

use rustc_hash::FxHashMap;

use crate::Error;
use crate::config::AnalysisConfig;
use crate::input::{self, FuseEvent};
use crate::output::{self, BlackholeEvent};

/// A line reconstructed from FUSE read data.
#[derive(Debug, Clone)]
pub struct InputLine {
    /// Content hash of the line.
    pub hash: u64,
    /// The raw text of the line.
    pub text: String,
    /// Number of times this exact line appeared across all reads.
    pub count: u64,
    /// Group ID of the file this line was first seen in.
    pub group_id: u16,
    /// Relative ms of the first read containing this line.
    pub first_seen_ms: u64,
}

/// A line extracted from a blackhole payload.
#[derive(Debug, Clone)]
pub struct OutputLine {
    /// Content hash of the message.
    pub hash: u64,
    /// The raw message text.
    pub message: String,
    /// Relative ms when the blackhole received this payload.
    pub relative_ms: u64,
}

/// All data needed by checks: reconstructed inputs, parsed outputs, and raw
/// events for future timing/ordering checks.
#[derive(Debug)]
pub struct AnalysisContext {
    /// Input lines keyed by content hash.
    pub input_lines: FxHashMap<u64, InputLine>,
    /// All output lines in order of receipt.
    pub output_lines: Vec<OutputLine>,
    /// Raw FUSE events (for future timing/ordering checks).
    pub fuse_events: Vec<FuseEvent>,
    /// Raw blackhole events (for future timing checks).
    pub blackhole_events: Vec<BlackholeEvent>,
}

impl AnalysisContext {
    /// Build the context from capture files and lading config.
    ///
    /// # Errors
    ///
    /// Returns an error if files cannot be read, parsed, or if the block cache
    /// cannot be reconstructed.
    pub fn build(config: &AnalysisConfig) -> Result<Self, Error> {
        let (input_lines, fuse_events) = input::reconstruct(
            &config.inputs.fuse_capture,
            &config.inputs.lading_config,
        )?;

        let (output_lines, blackhole_events) =
            output::parse(&config.inputs.blackhole_capture)?;

        Ok(Self {
            input_lines,
            output_lines,
            fuse_events,
            blackhole_events,
        })
    }
}
