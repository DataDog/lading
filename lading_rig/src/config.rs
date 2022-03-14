//! This module controls configuration parsing from the end user, providing a
//! convenience mechanism for the rest of the program. Crashes are most likely
//! to originate from this code, intentionally.
use serde::Deserialize;
use std::{collections::HashMap, net::SocketAddr, path::PathBuf};

use crate::generator;

/// Main configuration struct for this program
#[derive(Debug, Deserialize)]
pub struct Config {
    /// Total number of worker threads to use in this program
    pub worker_threads: u16,
    /// The time, in seconds, to run target in-rig
    // TODO later it would be interesting to set goals and let rig figure out
    // how long to run for, have a maximum duration cap instead
    pub experiment_duration: u32,
    /// The method by which to express telemetry
    pub telemetry: Telemetry,
    /// The generator to apply to the target in-rig
    pub generator: generator::Config,
    /// The program being targetted by this rig
    pub target: Target,
    /// The blackhole to supply for the target
    pub blackhole: Option<Blackhole>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum Telemetry {
    Prometheus {
        /// Address and port for prometheus exporter
        prometheus_addr: SocketAddr,
        /// Additional labels to include in every metric
        global_labels: HashMap<String, String>,
    },
    Log {
        /// Location on disk to write captures
        path: PathBuf,
        /// Additional labels to include in every metric
        global_labels: HashMap<String, String>,
    },
}

#[derive(Debug, Deserialize)]
pub struct Target {
    pub command: String,
    pub arguments: Vec<String>,
    pub environment_variables: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlackholeVariant {
    Tcp,
}

#[derive(Debug, Deserialize)]
pub struct Blackhole {
    /// address -- IP plus port -- to bind to
    pub binding_addr: SocketAddr,
}
