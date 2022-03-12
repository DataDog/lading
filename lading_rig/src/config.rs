//! This module controls configuration parsing from the end user, providing a
//! convenience mechanism for the rest of the program. Crashes are most likely
//! to originate from this code, intentionally.
use serde::Deserialize;
use std::{collections::HashMap, net::SocketAddr};

use crate::generator;

/// Main configuration struct for this program
#[derive(Debug, Deserialize)]
pub struct Config {
    /// Total number of worker threads to use in this program
    pub worker_threads: u16,
    /// Address and port for prometheus exporter
    pub prometheus_addr: SocketAddr,
    /// The generator to apply to the target in-rig
    pub generator: generator::Config,
    /// The program being targetted by this rig
    pub target: Target,
    /// The blackhole to supply for the target
    pub blackhole: Option<Blackhole>,
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
