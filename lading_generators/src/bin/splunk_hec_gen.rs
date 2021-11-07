use std::{collections::HashMap, fs::read_to_string, net::SocketAddr, path::Path};

use argh::FromArgs;
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
    pub targets: HashMap<String, ()>,
}

fn main() {
    // read the config from a toml file
    println!("arguments from environment {:?}", argh::from_env::<Opts>());
    let opts = argh::from_env::<Opts>(); // turbofish vs annotating variable?
    let toml: Config = toml::from_str(
        &read_to_string(&opts.config_path)
            .expect(&format!("Failed to read config at {}", opts.config_path)),
    )
    .expect("Configuration missing required settings");
    println!("the config as toml {:?}", toml);

    // spin up the data generation system

    // send data to Splunk, ack, loop
}
