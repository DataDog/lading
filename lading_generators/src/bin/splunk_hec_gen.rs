use std::{collections::HashMap, fs::read_to_string, net::SocketAddr};

use argh::FromArgs;
use futures::stream::{FuturesUnordered, StreamExt};
use lading_generators::splunk_hec_gen::{
    config::{Config, Target},
    Worker,
};
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::runtime::Builder;

fn default_config_path() -> String {
    "/etc/lading/splunk_hec_gen.toml".to_string()
}

#[derive(FromArgs, Debug)]
/// splunk_hec_gen options
struct Opts {
    /// path on disk to the configuration file
    #[argh(option, default = "default_config_path()")]
    config_path: String,
}
fn get_config() -> Config {
    let opts = argh::from_env::<Opts>();
    let contents = read_to_string(&opts.config_path).unwrap();
    serde_yaml::from_str::<Config>(&contents).unwrap()
}

async fn run(addr: SocketAddr, targets: HashMap<String, Target>) {
    let _: () = PrometheusBuilder::new()
        .listen_address(addr)
        .install()
        .unwrap();

    let mut workers = FuturesUnordered::new();

    targets
        .into_iter()
        .map(|(name, target)| Worker::new(name, target).unwrap())
        .for_each(|worker| workers.push(worker.spin()));

    loop {
        if let Some(res) = workers.next().await {
            res.unwrap();
        }
    }
}

fn main() {
    let config = get_config();
    let runtime = Builder::new_multi_thread()
        .worker_threads(config.worker_threads as usize)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(run(config.prometheus_addr, config.targets));
}
