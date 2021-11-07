use std::{collections::HashMap, fs::read_to_string, net::SocketAddr, num::NonZeroU32};

use argh::FromArgs;
use byte_unit::{Byte, ByteUnit};
use futures::stream::{FuturesUnordered, StreamExt};
use governor::{
    clock,
    state::{self, direct},
    Quota, RateLimiter,
};
use http::Uri;
use lading_common::{
    block::{chunk_bytes, construct_block_cache, Block},
    payload,
};
use metrics_exporter_prometheus::PrometheusBuilder;
use serde::Deserialize;
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
    /// The maximum size in bytes of the cache of prebuilt messages
    pub maximum_prebuild_cache_size_bytes: byte_unit::Byte,
    /// The bytes per second to send or receive from the target
    pub bytes_per_second: byte_unit::Byte,
    /// The block sizes for messages to this target
    pub block_sizes: Option<Vec<byte_unit::Byte>>, // q: what is a block size?
    /// The total number of parallel connections to maintain
    pub parallel_connections: u16, // q: how does this combine with worker threads?
}

struct Worker {
    uri: Uri,
    name: String,
    parallel_connections: u16,
    rate_limiter: RateLimiter<direct::NotKeyed, state::InMemoryState, clock::QuantaClock>,
    block_cache: Vec<Block>,
    metric_labels: Vec<(String, String)>,
}

#[derive(Debug)]
pub enum Error {}

impl Worker {
    fn new(name: String, target: Target) -> Result<Self, Error> {
        let mut rng = rand::thread_rng();
        let block_sizes: Vec<usize> = target
            .block_sizes
            .unwrap_or_else(|| {
                vec![
                    Byte::from_unit(1.0 / 8.0, ByteUnit::MB).unwrap(),
                    Byte::from_unit(1.0 / 4.0, ByteUnit::MB).unwrap(),
                    Byte::from_unit(1.0 / 2.0, ByteUnit::MB).unwrap(),
                    Byte::from_unit(1_f64, ByteUnit::MB).unwrap(),
                    Byte::from_unit(2_f64, ByteUnit::MB).unwrap(),
                    Byte::from_unit(4_f64, ByteUnit::MB).unwrap(),
                ]
            })
            .iter()
            .map(|sz| sz.get_bytes() as usize)
            .collect();
        let bytes_per_second = NonZeroU32::new(target.bytes_per_second.get_bytes() as u32).unwrap();
        let rate_limiter = RateLimiter::direct(Quota::per_second(bytes_per_second));
        let labels = vec![
            ("name".to_string(), name.clone()),
            ("target".to_string(), target.target_uri.to_string()),
        ];
        let block_chunks = chunk_bytes(
            &mut rng,
            target.maximum_prebuild_cache_size_bytes.get_bytes() as usize,
            &block_sizes,
        );
        let block_cache =
            construct_block_cache(&payload::SplunkHec::default(), &block_chunks, &labels);

        Ok(Self {
            parallel_connections: target.parallel_connections,
            uri: target.target_uri,
            block_cache,
            name,
            rate_limiter,
            metric_labels: labels,
        })
    }

    async fn spin(self) -> Result<(), Error> {
        // send data to Splunk, ack, loop
        println!("hello world");
        Ok(())
    }
}

fn get_config() -> Config {
    let opts = argh::from_env::<Opts>(); // q: turbofish vs annotating variable?
    let contents = read_to_string(&opts.config_path)
        .expect(&format!("Failed to read config at {}", opts.config_path));
    toml::from_str::<Config>(&contents).expect("Configuration missing required settings")
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
