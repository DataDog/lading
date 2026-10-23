//! Run the frozen reader without sharing Cargo dependency resolution with the writer.

#[path = "reference_v1/src/model.rs"]
mod model;

pub(crate) use model::Record;

use std::{env, fs, io, process::Command};

/// Decode through the independent reader that `ci/test` builds before the suite.
///
/// # Errors
///
/// Return an error when the reader is unavailable or cannot decode the capture.
pub(crate) fn read(data: &[u8]) -> io::Result<Vec<Record>> {
    let binary = env::var_os("LADING_CAPTURE_REFERENCE_V1").ok_or_else(|| {
        io::Error::other("run ci/test to build the frozen capture reference reader")
    })?;
    let input = tempfile::NamedTempFile::new()?;
    fs::write(input.path(), data)?;
    let output = Command::new(binary).arg(input.path()).output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "reference reader failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    serde_json::from_slice(&output.stdout).map_err(io::Error::other)
}
