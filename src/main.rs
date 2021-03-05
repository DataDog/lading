use argh::FromArgs;
use fastrand::Rng;
use futures::stream::{FuturesUnordered, StreamExt};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::Path;
use tokio::runtime::Builder;
use tokio::time;

#[derive(FromArgs)]
/// `file_gen` options
struct Opts {
    /// path on disk to the configuration file for `file_gen`
    #[argh(option)]
    config_path: String,
}

#[derive(Debug, Deserialize)]
struct Config {
    random_seed: u64,
    root: String,
    targets: HashMap<String, LogTarget>,
}

#[derive(Debug, Deserialize)]
struct LogTarget {
    path: String,
}

async fn file_writer<P: AsRef<Path>>(rng: Rng, path: P) {
    if let Some(p) = path.as_ref().to_str() {
        let mut interval = time::interval(time::Duration::from_millis(1_000));

        loop {
            interval.tick().await;
            println!("[{}]{} goes brrrr...", rng.u8(0..u8::MAX), p);
        }
    }
}

async fn run(rng: Rng) {
    let mut workers = FuturesUnordered::new();

    workers.push(file_writer(rng.clone(), Path::new("./foo/bar.txt")));

    while let Some(_) = workers.next().await {}
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
    println!("{:?}", config);

    let rng: Rng = Rng::with_seed(config.random_seed);

    let runtime = Builder::new_current_thread().enable_time().build().unwrap();

    runtime.block_on(run(rng));
}
