//! This module controls configuration parsing from the end user, providing a
//! convenience mechanism for the rest of the program. Crashes are most likely
//! to originate from this code, intentionally.
use serde::Deserialize;
use std::{collections::HashMap, net::SocketAddr, path::PathBuf};

use crate::{blackhole, generator, target};

/// Main configuration struct for this program
#[derive(Debug, Deserialize)]
pub struct Config {
    /// The time, in seconds, to run target in-rig
    // TODO later it would be interesting to set goals and let rig figure out
    // how long to run for, have a maximum duration cap instead
    pub experiment_duration: u32,
    /// The method by which to express telemetry
    #[serde(default)]
    pub telemetry: Telemetry,
    /// The generator to apply to the target in-rig
    pub generator: generator::Config,
    /// The program being targetted by this rig
    pub target: target::Config,
    /// The blackhole to supply for the target
    pub blackhole: Option<blackhole::Config>,
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

impl Default for Telemetry {
    fn default() -> Self {
        Self::Prometheus {
            prometheus_addr: "0.0.0.0:9000".parse().unwrap(),
            global_labels: HashMap::default(),
        }
    }
}
