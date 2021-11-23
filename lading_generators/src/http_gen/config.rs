//! This module controls configuration parsing from the end user, providing a
//! convenience mechanism for the rest of the program. Crashes are most likely
//! to originate from this code, intentionally.
use hyper::{HeaderMap, Uri};
use serde::Deserialize;
use std::{collections::HashMap, net::SocketAddr, path::PathBuf};

/// Main configuration struct for this program
#[derive(Debug, Deserialize)]
pub struct Config {
    /// Total number of worker threads to use in this program
    pub worker_threads: u16,
    /// Address and port for prometheus exporter
    pub prometheus_addr: SocketAddr,
    /// The [`Target`] instances and their base name
    pub targets: HashMap<String, Target>,
}

/// The HTTP method to be used in requests
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Method {
    /// Make HTTP Post requests
    Post {
        /// The payload generator to use for this target
        variant: Variant,
        /// The maximum size in bytes of the cache of prebuilt messages
        maximum_prebuild_cache_size_bytes: byte_unit::Byte,
    },
}

/// Variant of the [`TargetTemplate`]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Variant {
    /// Generates Splunk HEC messages
    SplunkHec,
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

/// The [`Target`] instance from which to derive workers
#[derive(Debug, Deserialize)]
pub struct Target {
    /// The URI for the target, must be a valid URI
    #[serde(with = "http_serde::uri")]
    pub target_uri: Uri,
    /// The method to use against the URI
    pub method: Method,
    /// Headers to include in the request
    #[serde(with = "http_serde::header_map")]
    pub headers: HeaderMap,
    /// The bytes per second to send or receive from the target
    pub bytes_per_second: byte_unit::Byte,
    /// The block sizes for messages to this target
    pub block_sizes: Option<Vec<byte_unit::Byte>>,
    /// The total number of parallel connections to maintain
    pub parallel_connections: u16,
}
