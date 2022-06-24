//! This module controls configuration parsing from the end user, providing a
//! convenience mechanism for the rest of the program. Crashes are most likely
//! to originate from this code, intentionally.
use std::{collections::HashMap, net::SocketAddr, path::PathBuf};

use serde::Deserialize;

use crate::{blackhole, generator, inspector, observer, target};

/// Generator configuration for this program.
///
/// We have many uses that exist prior to the introduction of multiple
/// generators, meaning many configs that _assume_ there is only one generator
/// and that they do not exist in an array. In order to avoid breaking those
/// configs we support this goofy structure. A deprecation cycle here is in
/// order someday.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Generator {
    /// Load in only one generator
    One(Box<generator::Config>),
    /// Load in one or more generators
    Many(Vec<generator::Config>),
}

/// Blackhole configuration for this program.
///
/// We have many uses that exist prior to the introduction of multiple
/// blackholes, meaning many configs that _assume_ there is only one bloackhole
/// and that they do not exist in an array. In order to avoid breaking those
/// configs we support this goofy structure. A deprecation cycle here is in
/// order someday.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Blackhole {
    /// Load in only one generator
    One(Box<blackhole::Config>),
    /// Load in one or more generators
    Many(Vec<blackhole::Config>),
}

/// Main configuration struct for this program
#[derive(Debug, Deserialize)]
pub struct Config {
    /// The method by which to express telemetry
    #[serde(default)]
    pub telemetry: Telemetry,
    /// The generator to apply to the target in-rig
    pub generator: Generator,
    /// The observer that watches the target
    #[serde(skip_deserializing)]
    pub observer: observer::Config,
    /// The program being targetted by this rig
    #[serde(skip_deserializing)]
    pub target: Option<target::Config>,
    /// The blackhole to supply for the target
    pub blackhole: Option<Blackhole>,
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
