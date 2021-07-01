//! The `file_gen` library
//!
//! This crate is primarily intended to back the `file_gen` executable and is
//! not considered useful otherwise.

#![deny(missing_docs)]
#![deny(clippy::all)]
#![deny(clippy::pedantic)]
#![deny(clippy::cargo)]
#![allow(clippy::multiple_crate_versions)]

use metrics::{register_counter, register_gauge, Unit};

mod payload;
//pub mod buffer;
pub mod config;
mod file;

pub use file::Log;

/// Initialize this program's metrics subsystem
pub fn init_metrics(mut targets: Vec<String>) {
    for target in targets.drain(..) {
        register_gauge!("maximum_bytes_per_file",
                        Unit::Bytes,
                        "soft maximum size of the file, in bytes",
                        "target" => target.clone());
        register_gauge!("maximum_line_size_bytes",
                        Unit::Bytes,
                        "maximum line size of the file, in bytes",
                        "target" => target.clone());
        register_counter!("bytes_written",
                          Unit::Bytes,
                          "total bytes written to the file",
                          "target" => target.clone());
        register_counter!("lines_written",
                          Unit::Count,
                          "total lines written to the file",
                          "target" => target.clone());
        register_gauge!("current_target_size_bytes",
                          Unit::Bytes,
                          "current size in bytes of the target file",
                          "target" => target.clone());
        register_counter!("file_rotated",
                          Unit::Count,
                          "number of times the underlying file has been rotated",
                          "target" => target.clone());
        register_counter!("unable_to_write_to_target",
                          Unit::Count,
                          "number of times the underlying file failed to be written to",
                          "target" => target.clone());
        register_gauge!("file_rotation_slop",
                        Unit::Bytes,
                        "total number of bytes over the soft maximum at rotation",
                        "target" => target);
    }
}
