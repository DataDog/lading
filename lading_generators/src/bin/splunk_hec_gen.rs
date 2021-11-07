use std::{collections::HashMap, fs::read_to_string, net::SocketAddr, path::Path};

use argh::FromArgs;
use http::Uri;
use serde::Deserialize;

fn default_config_path() -> String {
    "/etc/lading/splunk_hec_gen.toml".to_string()
}

#[derive(FromArgs, Debug)]
/// splunk_hec_gen options
struct Opts {
    /// config
    #[argh(option, default = "default_config_path()")]
    config_path: String,
}

#[derive(Deserialize, Debug)]
struct Config {
    /// Total number of worker threads to use in this program
    pub worker_threads: u16,
    /// Address and port for prometheus exporter
    pub prometheus_addr: SocketAddr,
    /// The [`Target`] instances and their base name
    pub targets: HashMap<String, Target>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum Encoding {
    Raw,
    Json,
}

#[derive(Deserialize, Debug)]
struct AckConfig {
    /// The number of channels to create
    pub channels: usize,
    /// The time between queries to /services/collector/ack
    pub ack_query_interval: usize,
    /// The time an ackId can remain pending before assuming data was dropped
    pub ack_timeout: usize,
}

#[derive(Deserialize, Debug)]
struct Target {
    /// The URI for the target, must be a valid URI
    #[serde(with = "http_serde::uri")]
    pub target_uri: Uri,
    /// Encode data as string (services/collector/raw endpoint) or JSON (services/collector/event endpoint)
    pub encoding: Encoding,
    /// Indexer acknowledgements behavior, if enabled
    pub acknowledgements: Option<AckConfig>,
    /// The bytes per second to send or receive from the target
    pub bytes_per_second: byte_unit::Byte,
    /// The block sizes for messages to this target
    pub block_sizes: Option<Vec<byte_unit::Byte>>, // q: what is a block size?
    /// The total number of parallel connections to maintain
    pub parallel_connections: u16, // q: how does this combine with worker threads?
}

fn get_config() -> Config {
    let opts = argh::from_env::<Opts>(); // q: turbofish vs annotating variable?
    let contents = read_to_string(&opts.config_path)
        .expect(&format!("Failed to read config at {}", opts.config_path));
    toml::from_str::<Config>(&contents).expect("Configuration missing required settings")
}

fn main() {
    // read the config from a toml file
    let config = get_config();
    println!("toml: {:?}", config);

    // spin up the data generation system
    // respect the worker threads configuration
    // respect the prometheus addr configuration (record and send metrics here)

    // send data to Splunk, ack, loop
}
