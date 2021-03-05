use argh::FromArgs;
use fastrand::Rng;
use futures::stream::{FuturesUnordered, StreamExt};
use governor::state::direct::InsufficientCapacity;
use governor::{Quota, RateLimiter};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::num::NonZeroU32;
use std::path::PathBuf;
use tokio::runtime::Builder;

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
    path: PathBuf,
    /// Sets the **soft** maximum bytes to be written into the `LogTarget`. This
    /// limit is soft, meaning a burst may go beyond this limit by no more than
    /// `maximum_token_burst`.
    ///
    /// After this limit is breached the target is closed and deleted. A new
    /// target with the same name is created to be written to.
    maximum_bytes_per: NonZeroU32,
    /// Defines the number of bytes that are added into the `LogTarget`'s rate
    /// limiting mechanism per second. This sets the maximum bytes that can be
    /// written _continuously_ per second from this target. Higher bursts are
    /// possible as the internal governor accumulates, up to
    /// `maximum_bytes_burst`.
    bytes_per_second: NonZeroU32,
    maximum_bytes_burst: NonZeroU32,
}

enum Error {
    Governor(InsufficientCapacity),
}

impl From<InsufficientCapacity> for Error {
    fn from(error: InsufficientCapacity) -> Self {
        Error::Governor(error)
    }
}

async fn file_writer(rng: Rng, target: LogTarget) -> Result<(), Error> {
    let gov = RateLimiter::direct(
        Quota::per_second(target.bytes_per_second).allow_burst(target.maximum_bytes_burst),
    );

    if let Some(p) = target.path.to_str() {
        loop {
            match rng.u32(0..target.maximum_bytes_burst.get()) {
                0 => {
                    // TODO flush the file
                }
                bytes => {
                    let bytes = NonZeroU32::new(bytes).unwrap();
                    gov.until_n_ready(bytes).await?;
                    println!("[{}]{} goes brrrr...", bytes, p);
                }
            }
        }
    }
    Ok(())
}

async fn run(rng: Rng, targets: HashMap<String, LogTarget>) {
    let mut workers = FuturesUnordered::new();

    for (_, v) in targets {
        workers.push(file_writer(rng.clone(), v));
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
    println!("{:?}", config);

    let rng: Rng = Rng::with_seed(config.random_seed);

    let runtime = Builder::new_current_thread().build().unwrap();

    runtime.block_on(run(rng, config.targets));
}
