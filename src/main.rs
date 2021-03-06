use argh::FromArgs;
use fastrand::Rng;
use file_gen::config::{Config, LogTarget};
use file_gen::Log;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::runtime::Builder;
use tokio::{spawn, time};
use tracing::{dispatcher, info, instrument};
use tracing_subscriber::fmt;

#[derive(FromArgs)]
/// `file_gen` options
struct Opts {
    /// path on disk to the configuration file for `file_gen`
    #[argh(option)]
    config_path: String,
}

#[instrument]
async fn stats(global_bytes: Arc<AtomicU64>) {
    let mut interval = time::interval(time::Duration::from_secs(1));

    loop {
        interval.tick().await;
        info!("global_bytes: {}", global_bytes.swap(0, Ordering::Relaxed));
    }
}

#[instrument]
async fn run(rng: Rng, targets: HashMap<String, LogTarget>) {
    let mut workers = FuturesUnordered::new();

    let global_bytes = Arc::new(AtomicU64::new(0));

    for (_, tgt) in targets {
        let log = Log::new(rng.clone(), tgt, global_bytes.clone())
            .await
            .unwrap();
        workers.push(log.spin());
    }

    spawn(stats(global_bytes));
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
    let subscriber = fmt::SubscriberBuilder::default().finish();
    let dispatch = dispatcher::Dispatch::new(subscriber);
    dispatcher::set_global_default(dispatch).unwrap();

    let config: Config = get_config();
    info!("CONFIG: {:?}", config);

    let rng: Rng = Rng::with_seed(config.random_seed);

    let runtime = Builder::new_current_thread().enable_time().build().unwrap();

    runtime.block_on(run(rng, config.targets));
}
