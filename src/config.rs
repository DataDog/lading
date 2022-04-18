//! This module controls configuration parsing from the end user, providing a
//! convenience mechanism for the rest of the program. Crashes are most likely
//! to originate from this code, intentionally.
use std::{collections::HashMap, net::SocketAddr, path::PathBuf};

use serde::Deserialize;

use crate::{blackhole, generator, inspector, target};

/// Main configuration struct for this program
#[derive(Debug, Deserialize)]
pub struct Config {
    /// The method by which to express telemetry
    #[serde(default)]
    pub telemetry: Telemetry,
    /// The generator to apply to the target in-rig
    pub generator: generator::Config,
    /// The program being targetted by this rig
    #[serde(skip_deserializing)]
    pub target: Option<target::Config>,
    /// The blackhole to supply for the target
    pub blackhole: Option<blackhole::Config>,
    /// The target inspector sub-program
    pub inspector: Option<inspector::Config>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
/// Defines the manner of lading's telemetry.
pub enum Telemetry {
    /// In prometheus mode lading will emit its internal telemetry for scraping
    /// at a prometheus poll endpoint.
    Prometheus {
        /// Address and port for prometheus exporter
        prometheus_addr: SocketAddr,
        /// Additional labels to include in every metric
        global_labels: HashMap<String, String>,
    },
    /// In log mode lading will emit its internal telemetry to a structured log
    /// file, the "capture" file.
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
