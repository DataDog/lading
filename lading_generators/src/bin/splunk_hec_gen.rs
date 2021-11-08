use std::{
    collections::{HashMap, HashSet},
    fs::read_to_string,
    net::SocketAddr,
    num::NonZeroU32,
    sync::Arc,
    time::Duration,
};

use argh::FromArgs;
use byte_unit::{Byte, ByteUnit};
use futures::stream::{self, FuturesUnordered, StreamExt};
use governor::{
    clock,
    state::{self, direct},
    Quota, RateLimiter,
};
use http::{
    header::{AUTHORIZATION, CONTENT_LENGTH},
    Method, Request, Response, Uri,
};
use hyper::{client::HttpConnector, Body, Client};
use lading_common::{
    block::{chunk_bytes, construct_block_cache, Block},
    payload::{self, SplunkHecAckQuery},
};
use metrics::{counter, gauge};
use metrics_exporter_prometheus::PrometheusBuilder;
use serde::Deserialize;
use tokio::{runtime::Builder, sync::RwLock};

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
struct AckConfig {
    /// The time between queries to /services/collector/ack
    pub ack_query_interval: u64,
    /// The time an ackId can remain pending before assuming data was dropped
    pub ack_timeout: u64,
}

#[derive(Deserialize, Debug)]
struct Target {
    /// The URI for the target, must be a valid URI
    #[serde(with = "http_serde::uri")]
    pub target_uri: Uri,
    /// HEC authentication token
    pub token: String,
    /// Indexer acknowledgements behavior, if enabled
    pub acknowledgements: Option<AckConfig>,
    /// The maximum size in bytes of the cache of prebuilt messages
    pub maximum_prebuild_cache_size_bytes: byte_unit::Byte,
    /// The bytes per second to send or receive from the target
    pub bytes_per_second: byte_unit::Byte,
    /// The block sizes for messages to this target
    pub block_sizes: Option<Vec<byte_unit::Byte>>, // q: what is a block representing?
    /// The total number of parallel connections to maintain
    pub parallel_connections: u16, // q: how does this combine with worker threads?
}

struct Worker {
    uri: Uri,
    token: String,
    name: String,
    parallel_connections: u16,
    rate_limiter: RateLimiter<direct::NotKeyed, state::InMemoryState, clock::QuantaClock>,
    block_cache: Vec<Block>,
    metric_labels: Vec<(String, String)>,
    ack_config: Option<AckConfig>,
}

#[derive(Debug)]
pub enum Error {
    InvalidHECPath,
}

/// Derive the intended encoding based on the URI path
/// https://docs.splunk.com/Documentation/Splunk/latest/Data/FormateventsforHTTPEventCollector#Event_data
fn get_encoding_from_uri(uri: &Uri) -> Result<payload::Encoding, Error> {
    let endpoint = uri.path_and_query().expect("Missing HEC path");
    let path = endpoint.path();
    if path == "/services/collector/event" || path == "/services/collector/event/1.0" {
        Ok(payload::Encoding::Json)
    } else if path == "/services/collector/raw" || path == "/services/collector/raw/1.0" {
        Ok(payload::Encoding::Text)
    } else {
        Err(Error::InvalidHECPath)
    }
}

type ChannelIdToAckIds = HashMap<String, Arc<RwLock<HashSet<u64>>>>;

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
        let encoding = get_encoding_from_uri(&target.target_uri)?;
        let block_chunks = chunk_bytes(
            &mut rng,
            target.maximum_prebuild_cache_size_bytes.get_bytes() as usize,
            &block_sizes,
        );
        let block_cache =
            construct_block_cache(&payload::SplunkHec::new(encoding), &block_chunks, &labels);

        Ok(Self {
            parallel_connections: target.parallel_connections,
            uri: target.target_uri,
            token: target.token,
            block_cache,
            name,
            rate_limiter,
            metric_labels: labels,
            ack_config: target.acknowledgements,
        })
    }

    async fn spin(self) -> Result<(), Error> {
        let client: Client<HttpConnector, Body> = Client::builder()
            .pool_max_idle_per_host(self.parallel_connections as usize)
            .retry_canceled_requests(false)
            .set_host(false)
            .build_http();

        let rate_limiter = Arc::new(self.rate_limiter);
        let uri = self.uri;
        let token = self.token;
        let labels = self.metric_labels;

        let mut channel_ids = Vec::new();
        for i in 0..self.parallel_connections {
            channel_ids.push(format!(
                "{}-1111-1111-1111-111111111111",
                10000000 as u32 + i as u32
            ));
        }

        let pending_ack_ids = if let Some(ack_config) = self.ack_config {
            let mut pending_ack_ids: ChannelIdToAckIds = HashMap::new();
            let ack_client: Client<HttpConnector, Body> = Client::builder()
                .retry_canceled_requests(false)
                .set_host(false)
                .build_http();
            for channel_id in channel_ids.clone() {
                let ack_ids = Arc::new(RwLock::new(HashSet::new()));
                pending_ack_ids.insert(channel_id.clone(), ack_ids.clone());
                spawn_ack_service(
                    uri.clone(),
                    token.clone(),
                    channel_id.clone(),
                    ack_client.clone(),
                    &ack_config,
                    ack_ids.clone(),
                );
            }
            Some(Arc::new(pending_ack_ids))
        } else {
            None
        };

        let mut channel_ids_cycle = channel_ids.iter().cycle();
        gauge!(
            "maximum_requests",
            f64::from(self.parallel_connections),
            &labels
        );
        stream::iter(self.block_cache.iter().cycle())
            .for_each_concurrent(self.parallel_connections as usize, |blk| {
                let client = client.clone();
                let rate_limiter = Arc::clone(&rate_limiter);
                let labels = labels.clone();
                let uri = uri.clone();
                let pending_ack_ids = pending_ack_ids.clone();

                let total_bytes = blk.total_bytes;
                let body = Body::from(blk.bytes.clone());
                let block_length = blk.bytes.len();
                let channel_id = channel_ids_cycle.next().unwrap();

                let request: Request<Body> = Request::builder()
                    .method(Method::POST)
                    .uri(uri)
                    .header(AUTHORIZATION, format!("Splunk {}", token))
                    .header(CONTENT_LENGTH, block_length)
                    .header("x-splunk-request-channel", channel_id)
                    .body(body)
                    .unwrap();

                async move {
                    rate_limiter.until_n_ready(total_bytes).await.unwrap();
                    counter!("requests_sent", 1, &labels);
                    match client.request(request).await {
                        Ok(response) => {
                            counter!("bytes_written", block_length as u64, &labels);
                            let status = response.status();
                            let mut status_labels = labels.clone();
                            status_labels
                                .push(("status_code".to_string(), status.as_u16().to_string()));
                            counter!("request_ok", 1, &status_labels);
                            if let Some(pending_acks) = pending_ack_ids {
                                let body = String::from_utf8(
                                    hyper::body::to_bytes(response.into_body())
                                        .await
                                        .unwrap()
                                        .to_vec(),
                                )
                                .unwrap();
                                let hec_ack_response =
                                    serde_json::from_str::<HecAckResponse>(body.as_str()).unwrap();
                                match pending_acks.get(channel_id) {
                                    Some(channel_ack_ids_lock) => {
                                        let mut channel_ack_ids =
                                            channel_ack_ids_lock.write().await;
                                        channel_ack_ids.insert(hec_ack_response.ackId);
                                    }
                                    None => todo!(),
                                }
                            }
                        }
                        Err(err) => {
                            let mut error_labels = labels.clone();
                            error_labels.push(("error".to_string(), err.to_string()));
                            error_labels.push(("body_size".to_string(), block_length.to_string()));
                            counter!("request_failure", 1, &error_labels);
                        }
                    }
                }
            })
            .await;
        Ok(())
    }
}

fn spawn_ack_service(
    uri: Uri,
    token: String,
    channel_id: String,
    client: Client<HttpConnector, Body>,
    ack_config: &AckConfig,
    ack_ids_lock: Arc<RwLock<HashSet<u64>>>,
) {
    let ack_uri = Uri::builder()
        .authority(uri.authority().unwrap().to_string())
        .scheme("http")
        .path_and_query("/services/collector/ack")
        .build()
        .unwrap();
    let mut interval = tokio::time::interval(Duration::from_secs(ack_config.ack_query_interval));
    tokio::spawn(async move {
        loop {
            interval.tick().await;
            let ack_ids = ack_ids_lock.read().await;
            let ack_ids = ack_ids.iter().map(|ack_id| *ack_id).collect::<Vec<u64>>();
            if ack_ids.len() > 0 {
                let body = Body::from(SplunkHecAckQuery::new_ack_body(ack_ids));
                let request: Request<Body> = Request::builder()
                    .method(Method::POST)
                    .uri(ack_uri.clone())
                    .header(AUTHORIZATION, format!("Splunk {}", token))
                    .header("x-splunk-request-channel", channel_id.clone())
                    .body(body)
                    .unwrap();
                match client.request(request).await {
                    Ok(response) => {
                        let status = response.status();
                        let body = serde_json::json!(String::from_utf8(
                            hyper::body::to_bytes(response.into_body())
                                .await
                                .unwrap()
                                .to_vec(),
                        )
                        .unwrap());
                        println!("queried for ack: {:?}", body);
                    }
                    Err(err) => {
                        println!("query failed");
                    }
                }
            }
        }
    });
}

#[derive(Deserialize, Debug)]
struct HecAckResponse {
    text: String,
    code: u8,
    ackId: u64,
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
