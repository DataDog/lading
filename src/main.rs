use argh::FromArgs;
use fastrand::Rng;
use file_gen::config::{Config, LogTarget};
use file_gen::Log;
use futures::stream::{FuturesUnordered, StreamExt};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::collections::HashMap;
use std::fs;
use std::io::Read;
use tokio::runtime::Builder;
use tracing::{debug, dispatcher, instrument};
use tracing_subscriber::fmt;

#[derive(FromArgs)]
/// `file_gen` options
struct Opts {
    /// path on disk to the configuration file for `file_gen`
    #[argh(option)]
    config_path: String,
}

#[instrument]
async fn run(rng: Rng, targets: HashMap<String, LogTarget>) {
    // Set up the `metrics` integration. All metrics are exported from
    // 0.0.0.0:9000 in prometheus format.
    file_gen::init_metrics();
    let _: () = PrometheusBuilder::new().install().unwrap();

    // Set up the `tracing` integration. All traces are emitted on stdout.
    let subscriber = fmt::SubscriberBuilder::default().finish();
    let dispatch = dispatcher::Dispatch::new(subscriber);
    dispatcher::set_global_default(dispatch).unwrap();

    let mut workers = FuturesUnordered::new();

    for (_, tgt) in targets {
        let log = Log::new(rng.clone(), tgt).await.unwrap();
        workers.push(log.spin());
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
    let config: Config = get_config();
    debug!("CONFIG: {:?}", config);

    // The rng of this program is not meant to be cryptographical good just fast
    // and repeatable. This will be cloned into every file worker. So, it's the
    // root rng. If any other rng is used as the source _other_ than this one
    // the determinism of this program is lost.
    let rng: Rng = Rng::with_seed(config.random_seed);

    let runtime = Builder::new_current_thread().enable_io().build().unwrap();
    runtime.block_on(run(rng, config.targets));
}
