//! Output format abstraction for capture files
//!
//! This module provides a trait-based abstraction for capture output
//! formats. Only JSONL is supported now.

use crate::line;

pub mod jsonl;

/// Format operation errors
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// IO errors during write operations
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// JSON serialization errors (for JSONL format)
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Trait for output format implementations
///
/// Implementations of this trait handle the serialization and writing of
/// metrics to a specific file format. The capture manager's state machine uses
/// this trait to remain agnostic to the output format.
pub trait OutputFormat {
    /// Write a single metric line to the output
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or writing fails.
    fn write_metric(&mut self, line: &line::Line) -> Result<(), Error>;

    /// Flush any buffered data to disk
    ///
    /// # Errors
    ///
    /// Returns an error if flushing fails.
    fn flush(&mut self) -> Result<(), Error>;
}
