use argh::FromArgs;
use futures::stream::{FuturesUnordered, StreamExt};
use lading_generators::http_gen::config::{Config, Target};
use lading_generators::http_gen::Worker;
use metrics_exporter_prometheus::PrometheusBuilder;
use std::collections::HashMap;
use std::io::Read;
use std::net::SocketAddr;
use tokio::runtime::Builder;

#[derive(FromArgs)]
/// `http_gen` options
struct Opts {
    /// path on disk to the configuration file
    #[argh(option)]
    config_path: String,
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

fn get_config() -> Config {
    let ops: Opts = argh::from_env();
    let mut file: std::fs::File = std::fs::OpenOptions::new()
        .read(true)
        .open(ops.config_path)
        .unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    toml::from_str(&contents).unwrap()
}

fn main() {
    let config: Config = get_config();
    let runtime = Builder::new_multi_thread()
        .worker_threads(config.worker_threads as usize)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(run(config.prometheus_addr, config.targets));
}
