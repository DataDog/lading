//! This module controls configuration parsing from the end user, providing a
//! convenience mechanism for the rest of the program. Crashes are most likely
//! to originate from this code, intentionally.
use byte_unit::{Byte, ByteUnit};
use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::TryFromIntError;
use std::path::PathBuf;
use std::str;

/// Main configuration struct for this program
#[derive(Debug, Deserialize)]
pub struct Config {
    /// Total number of worker threads to use in this program
    pub worker_threads: u16,
    /// Address and port for prometheus exporter
    pub prometheus_addr: SocketAddr,
    /// The [`LogTargetTemplate`] instances and their base name
    pub targets: HashMap<String, LogTargetTemplate>,
}

/// Variant of the [`LogTarget`]
///
/// This variant controls what kind of line text is created by this program.
#[derive(Debug, Deserialize, Clone)]
pub enum Variant {
    /// Generates Datadog Logs JSON messages
    DatadogLog,
    /// Generates a limited subset of FoundationDB logs
    FoundationDb,
    /// Generates a static, user supplied data
    Static {
        /// Defines the file path to read static variant data from. Content is
        /// assumed to be line-oriented but no other claim is made on the file.
        static_path: PathBuf,
    },
    /// Generates a line of printable ascii characters
    Ascii,
    /// Generates a json encoded line
    Json,
}

/// The template from which to create [`LogTarget`] instances
#[derive(Debug, Deserialize)]
pub struct LogTargetTemplate {
    /// The seed for random operations against this target
    pub seed: [u8; 32],
    /// The path template for [`LogTarget`]. "%NNN%" will be replaced in the
    /// template with the duplicate number.
    pub path_template: String,
    /// Total number of duplicates to make from this template.
    pub duplicates: u8,
    /// Sets the [`Variant`] of this template.
    pub variant: Variant,
    /// Sets the **soft** maximum bytes to be written into the `LogTarget`. This
    /// limit is soft, meaning a burst may go beyond this limit by no more than
    /// `maximum_token_burst`.
    ///
    /// After this limit is breached the target is closed and deleted. A new
    /// target with the same name is created to be written to.
    maximum_bytes_per_file: Byte,
    /// Defines the number of bytes that are added into the `LogTarget`'s rate
    /// limiting mechanism per second. This sets the maximum bytes that can be
    /// written _continuously_ per second from this target. Higher bursts are
    /// possible as the internal governor accumulates, up to
    /// `maximum_bytes_burst`.
    bytes_per_second: Byte,
    /// The block sizes for messages to this target
    pub block_sizes: Option<Vec<byte_unit::Byte>>,
    /// Defines the maximum internal cache of this log target. file_gen will
    /// pre-build its outputs up to the byte capacity specified here.
    maximum_prebuild_cache_size_bytes: Byte,
}

/// The [`LogTarget`] is generated by [`LogTargetTemplate`]
#[derive(Debug)]
pub struct LogTarget {
    /// The seed for random operations against this target
    pub seed: [u8; 32],
    /// The path for this target
    pub path: PathBuf,
    /// Sets the [`Variant`] of this target
    pub variant: Variant,
    /// Sets the **soft** maximum bytes to be written into the `LogTarget`. This
    /// limit is soft, meaning a burst may go beyond this limit by no more than
    /// `maximum_token_burst`.
    ///
    /// After this limit is breached the target is closed and deleted. A new
    /// target with the same name is created to be written to.
    pub maximum_bytes_per_file: Byte,
    /// Defines the number of bytes that are added into the `LogTarget`'s rate
    /// limiting mechanism per second. This sets the maximum bytes that can be
    /// written _continuously_ per second from this target. Higher bursts are
    /// possible as the internal governor accumulates, up to
    /// `maximum_bytes_burst`.
    pub bytes_per_second: Byte,
    /// The block sizes for messages to this target
    pub block_sizes: Vec<usize>,
    /// The maximum size in bytes that the prebuild cache may be.
    pub maximum_prebuild_cache_size_bytes: Byte,
}

/// Configuration errors
#[derive(Debug)]
pub enum Error {
    /// User provided a string that could not be decoded into an integer
    TryFromInt(TryFromIntError),
}

impl From<TryFromIntError> for Error {
    fn from(error: TryFromIntError) -> Self {
        Error::TryFromInt(error)
    }
}

impl LogTargetTemplate {
    /// Strike a new `LogTarget` from this template
    ///
    /// # Panics
    ///
    /// Function will panic if user configuration contains values that can't be
    /// converted to u32 instances.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn strike(&self, duplicate: u8) -> LogTarget {
        let duplicate = format!("{}", duplicate);
        let full_path = self.path_template.replace("%NNN%", &duplicate);
        let path = PathBuf::from(full_path);
        let block_sizes: Vec<usize> = self
            .block_sizes
            .clone()
            .unwrap_or_else(|| {
                vec![
                    Byte::from_unit(1_f64, ByteUnit::MB).unwrap(),
                    Byte::from_unit(2_f64, ByteUnit::MB).unwrap(),
                    Byte::from_unit(4_f64, ByteUnit::MB).unwrap(),
                    Byte::from_unit(8_f64, ByteUnit::MB).unwrap(),
                    Byte::from_unit(16_f64, ByteUnit::MB).unwrap(),
                    Byte::from_unit(32_f64, ByteUnit::MB).unwrap(),
                ]
            })
            .iter()
            .map(|sz| sz.get_bytes() as usize)
            .collect();

        LogTarget {
            path,
            block_sizes,
            seed: self.seed,
            variant: self.variant.clone(),
            maximum_bytes_per_file: self.maximum_bytes_per_file,
            bytes_per_second: self.bytes_per_second,
            maximum_prebuild_cache_size_bytes: self.maximum_prebuild_cache_size_bytes,
        }
    }
}
