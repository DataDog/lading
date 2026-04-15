//! The analysis context: parsed and reconstructed data from both capture files.

use crate::Error;
use crate::config::AnalysisConfig;
use crate::input::{self, FuseEvent};
use crate::output::{self, BlackholeEvent};

/// SHA-256 hash type used for content matching.
pub type ContentHash = [u8; 32];

/// A read that contributed bytes to a reconstructed line.
#[derive(Debug, Clone, Copy)]
pub struct ReadContribution {
    /// Byte offset within the file.
    pub offset: u64,
    /// Number of bytes from this read.
    pub size: u64,
    /// Milliseconds since lading epoch when this read occurred.
    pub relative_ms: u64,
}

/// A single FUSE read with its raw bytes hash and timestamp (raw mode).
#[derive(Debug, Clone)]
pub struct RawRead {
    /// Inode of the file.
    pub inode: usize,
    /// Group ID of the file.
    pub group_id: u16,
    /// Byte offset within the file.
    pub offset: u64,
    /// Number of bytes read.
    pub size: u64,
    /// Milliseconds since lading epoch.
    pub relative_ms: u64,
    /// The actual bytes returned by this read.
    pub content: String,
}

/// A line reconstructed from one or more reads (newline_delimited mode).
#[derive(Debug, Clone)]
pub struct ReconstructedLine {
    /// The raw text of the line (without trailing newline).
    pub text: String,
    /// SHA-256 hash of the line bytes (without trailing newline).
    pub hash: ContentHash,
    /// Group ID of the file this line came from.
    pub group_id: u16,
    /// The reads that contributed bytes to this line.
    pub contributions: Vec<ReadContribution>,
}

/// A line extracted from a blackhole payload.
#[derive(Debug, Clone)]
pub struct OutputLine {
    /// SHA-256 hash of the message.
    pub hash: ContentHash,
    /// The raw message text.
    pub message: String,
    /// Relative ms when the blackhole received this payload.
    pub relative_ms: u64,
}

/// All data needed by checks. Always contains both raw reads and reconstructed
/// lines — raw for human inspection, lines for correctness checks.
#[derive(Debug)]
pub struct AnalysisContext {
    /// One entry per FUSE read, with raw byte hash and exact timestamp.
    pub raw_reads: Vec<RawRead>,
    /// Lines reconstructed across read boundaries, for correctness checks.
    pub lines: Vec<ReconstructedLine>,
    /// All output lines in order of receipt.
    pub output_lines: Vec<OutputLine>,
    /// Raw FUSE events (for future checks).
    pub fuse_events: Vec<FuseEvent>,
    /// Raw blackhole events (for future checks).
    pub blackhole_events: Vec<BlackholeEvent>,
}

impl AnalysisContext {
    /// Build the context from capture files and lading config.
    ///
    /// # Errors
    ///
    /// Returns an error if files cannot be read or parsed, or if the block
    /// cache cannot be reconstructed.
    pub fn build(config: &AnalysisConfig) -> Result<Self, Error> {
        let (raw_reads, lines, fuse_events) = input::reconstruct(
            &config.inputs.fuse_capture,
            &config.inputs.lading_config,
        )?;

        let (output_lines, blackhole_events) =
            output::parse(&config.inputs.blackhole_capture)?;

        Ok(Self {
            raw_reads,
            lines,
            output_lines,
            fuse_events,
            blackhole_events,
        })
    }
}
