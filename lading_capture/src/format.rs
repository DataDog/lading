//! Format detection for capture files
//!
//! This module provides utilities to detect the format of capture files by
//! examining their magic bytes and file extensions.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

/// The format of a capture file
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureFormat {
    /// JSON Lines forma, potentially zstd-compressed
    Jsonl {
        /// Whether the file is zstd-compressed
        compressed: bool,
    },
    /// Apache Parquet columnar format
    Parquet,
}

/// Errors during format detection
#[derive(thiserror::Error, Debug)]
pub enum DetectionError {
    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Detects the format of a capture file.
///
/// Detection strategy:
/// 1. Check for Parquet magic bytes ("PAR1" at beginning and end)
/// 2. Fall back to extension-based detection
/// 3. Default to JSONL if no conclusive match
///
/// # Errors
///
/// Returns an error if the file cannot be read.
pub fn detect_format<P: AsRef<Path>>(path: P) -> Result<CaptureFormat, DetectionError> {
    let path = path.as_ref();

    // Check for parquet magic bytes
    if let Ok(mut file) = File::open(path) {
        let mut header_magic = [0u8; 4];

        // PAR1 at the head
        if file.read_exact(&mut header_magic).is_ok() && &header_magic == b"PAR1" {
            // PAR1 at the footer
            if file.seek(SeekFrom::End(-4)).is_ok() {
                let mut footer_magic = [0u8; 4];
                if file.read_exact(&mut footer_magic).is_ok() && &footer_magic == b"PAR1" {
                    return Ok(CaptureFormat::Parquet);
                }
            } else {
                // Edge case: small file, header match is enough
                return Ok(CaptureFormat::Parquet);
            }
        }
    }

    // Fall back to extension-based detection
    if let Some(ext) = path.extension() {
        if ext == "parquet" {
            return Ok(CaptureFormat::Parquet);
        }
        if ext == "zstd" {
            return Ok(CaptureFormat::Jsonl { compressed: true });
        }
    }

    // Default to uncompressed JSONL
    Ok(CaptureFormat::Jsonl { compressed: false })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn detects_parquet_by_magic_bytes() {
        let mut file = NamedTempFile::new().expect("create temp file");

        // Write minimal parquet structure with magic bytes
        file.write_all(b"PAR1").expect("write header");
        file.write_all(&[0u8; 100]).expect("write body");
        file.write_all(b"PAR1").expect("write footer");
        file.flush().expect("flush");

        let format = detect_format(file.path()).expect("detect format");
        assert_eq!(format, CaptureFormat::Parquet);
    }

    #[test]
    fn detects_parquet_by_extension() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir.path().join("test.parquet");
        std::fs::write(&path, b"not really parquet").expect("write file");

        let format = detect_format(&path).expect("detect format");
        assert_eq!(format, CaptureFormat::Parquet);
    }

    #[test]
    fn detects_zstd_jsonl_by_extension() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir.path().join("test.zstd");
        std::fs::write(&path, b"{}").expect("write file");

        let format = detect_format(&path).expect("detect format");
        assert_eq!(format, CaptureFormat::Jsonl { compressed: true });
    }

    #[test]
    fn defaults_to_uncompressed_jsonl() {
        let mut file = NamedTempFile::new().expect("create temp file");
        file.write_all(b"{}\n").expect("write");
        file.flush().expect("flush");

        let format = detect_format(file.path()).expect("detect format");
        assert_eq!(format, CaptureFormat::Jsonl { compressed: false });
    }
}
