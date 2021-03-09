use argh::FromArgs;
use file_gen::config::{Config, LogTargetTemplate};
use file_gen::Log;
use futures::stream::{FuturesUnordered, StreamExt};
use metrics_exporter_prometheus::PrometheusBuilder;
use rand::{Rng, SeedableRng};
use rand_xoshiro::SplitMix64;
use std::collections::HashMap;
use std::io::Read;
use std::{fs, mem};
use tokio::runtime::Builder;

#[derive(FromArgs)]
/// `file_gen` options
struct Opts {
    /// path on disk to the configuration file for `file_gen`
    #[argh(option)]
    config_path: String,
}

async fn run<R>(rng: R, targets: HashMap<String, LogTargetTemplate>)
where
    R: Rng + Sized + Clone,
{
    // Set up the `metrics` integration. All metrics are exported from
    // 0.0.0.0:9000 in prometheus format.
    file_gen::init_metrics(targets.keys().cloned().collect());
    let _: () = PrometheusBuilder::new().install().unwrap();

    let mut workers = FuturesUnordered::new();

    for (name, template) in targets {
        let iter = template.iter().unwrap();
        for (cur, tgt) in iter.enumerate() {
            let name = format!("{}[{}]", name.clone(), cur);
            let log = Log::new(rng.clone(), name, tgt).await.unwrap();
            workers.push(log.spin());
        }
    }

    while workers.next().await.is_some() {}
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

    // The rng of this program is not meant to be cryptographical good just fast
    // and repeatable. This will be cloned into every file worker. So, it's the
    // root rng. If any other rng is used as the source _other_ than this one
    // the determinism of this program is lost.
    let rng = SplitMix64::from_seed(config.random_seed.to_be_bytes());

    let runtime = Builder::new_multi_thread()
        .worker_threads(config.worker_threads as usize)
        .enable_io()
        .build()
        .unwrap();
    runtime.block_on(run(rng, config.targets));
}
