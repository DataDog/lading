//! This module controls configuration parsing from the end user, providing a
//! convenience mechanism for the rest of the program. Crashes are most likely
//! to originate from this code, intentionally.
use serde::Deserialize;
use std::{collections::HashMap, net::SocketAddr};

/// Main configuration struct for this program
#[derive(Debug, Deserialize)]
pub struct Config {
    /// Total number of worker thread to use in this program
    pub worker_threads: u16,
    /// Address and port for prometheus exporter
    pub prometheus_addr: SocketAddr,
    /// The [`Target`] instances and their base name
    pub targets: HashMap<String, Target>,
}

/// The throughput configuration
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Throughput {
    /// The producer should run as fast as possible.
    Unlimited,
    /// The producer is limited to sending a certain number of bytes every second.
    BytesPerSecond {
        /// Number of bytes.
        amount: byte_unit::Byte,
    },
    /// The producer is limited to sending a certain number of messages every second.
    MessagesPerSecond {
        /// Number of messages.
        amount: u32,
    },
}

/// Variant of the [`TargetTemplate`]
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Variant {
    /// Generates Datadog Logs JSON messages
    DatadogLog,
    /// Generates a limited subset of FoundationDB logs
    FoundationDb,
    /// Generates a line of printable ascii characters
    Ascii,
    /// Generates a json encoded line
    Json,
}

/// The [`Target`] instance from which to derive workers
#[derive(Clone, Debug, Deserialize)]
pub struct Target {
    /// Bootstrap server for Kafka.  Used identically like the flag of the same
    /// name present on Kafka CLI tooling.
    pub bootstrap_server: String,
    /// Topic to produce to.
    pub topic: String,
    /// The payload generator to use for this target
    pub variant: Variant,
    /// The throughput configuration
    pub throughput: Throughput,
    /// The maximum size in bytes of the cache of prebuilt messages
    pub maximum_prebuild_cache_size_bytes: byte_unit::Byte,
    /// The block sizes for messages to this target
    pub block_sizes: Option<Vec<byte_unit::Byte>>,
    /// Map of rdkafka=-specific overrides to apply to the producer
    pub producer_config: Option<HashMap<String, String>>,
}
