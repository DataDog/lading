use argh::FromArgs;
use futures::stream::{FuturesUnordered, StreamExt};
use lading_generators::file_gen::config::{Config, LogTargetTemplate};
use lading_generators::file_gen::Log;
use metrics_exporter_prometheus::PrometheusBuilder;
use rayon::prelude::*;
use std::collections::HashMap;
use std::io::Read;
use std::net::SocketAddr;
use std::{fs, mem};
use tokio::runtime::Builder;

#[derive(FromArgs)]
/// `file_gen` options
struct Opts {
    /// path on disk to the configuration file
    #[argh(option)]
    config_path: String,
}

async fn run(addr: SocketAddr, targets: HashMap<String, LogTargetTemplate>) {
    // Set up the `metrics` integration. All metrics are exported from
    // 0.0.0.0:9000 in prometheus format.
    let _: () = PrometheusBuilder::new()
        .listen_address(addr)
        .install()
        .unwrap();

    let mut workers = FuturesUnordered::new();

    targets.into_par_iter().for_each(|(name, template)| {
        (0..template.duplicates)
            .into_par_iter()
            .map(|duplicate| {
                let tgt_name = format!("{}[{}]", name.clone(), duplicate);
                let tgt = template.strike(duplicate);
                Log::new(tgt_name, tgt).unwrap()
            })
            .for_each(|log| workers.push(log.spin()));
    });

    loop {
        if let Some(res) = workers.next().await {
            res.unwrap();
        }
    }
}

fn get_config() -> Config {
    let ops: Opts = argh::from_env();
    let mut file: fs::File = fs::OpenOptions::new()
        .read(true)
        .open(ops.config_path)
        .unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    toml::from_str(&contents).unwrap()
}

fn main() {
    assert!(mem::size_of::<usize>() >= mem::size_of::<u64>());

    let config: Config = get_config();
    let runtime = Builder::new_multi_thread()
        .worker_threads(config.worker_threads as usize)
        .enable_io()
        .build()
        .unwrap();
    runtime.block_on(run(config.prometheus_addr, config.targets));
}
