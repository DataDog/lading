//! Output format abstraction for capture files
//!
//! This module provides a trait-based abstraction for capture output
//! formats.

use crate::line;

pub mod jsonl;
pub mod multi;
pub mod parquet;

/// Format operation errors
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// JSONL format errors
    #[error("JSONL format error: {0}")]
    Jsonl(#[from] jsonl::Error),
    /// Multi format errors
    #[error("Multi format error: {0}")]
    Multi(#[from] multi::Error),
    /// Parquet format errors
    #[error("Parquet format error: {0}")]
    Parquet(#[from] parquet::Error),
    /// IO errors during write operations
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
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

    /// Close and finalize the output format
    ///
    /// This method must be called to properly finalize the output file. For
    /// formats like Parquet, this writes critical metadata (file footer). For
    /// simpler formats like JSONL, this ensures all buffered data is written.
    ///
    /// Consumes the format as it can no longer be used after closing.
    ///
    /// # Errors
    ///
    /// Returns an error if closing fails.
    fn close(self) -> Result<(), Error>;
}
