//! The gRPC generator.
//!
//! ## Metrics
//!
//! `requests_sent`: Total number of requests sent
//! `request_ok`: Successful requests
//! `request_failure`: Failed requests
//! `bytes_written`: Total bytes written
//! `response_bytes`: Total bytes received
//! `bytes_per_second`: Configured rate to send data
//! `data_points_transmitted`: Total data points transmitted (for OpenTelemetry metrics)
//!
//! Additional metrics may be emitted by this generator's [throttle].
//!

use std::{convert::TryFrom, num::NonZeroU16, num::NonZeroU32, sync::Arc, time::Duration};

use byte_unit::Byte;
use bytes::{Buf, BufMut, Bytes};
use http::{
    Uri,
    uri::{self, PathAndQuery},
};
use metrics::counter;
use once_cell::sync::OnceCell;
use rand::SeedableRng;
use rand::rngs::StdRng;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tonic::{
    Request, Response, Status, client,
    codec::{DecodeBuf, Decoder, EncodeBuf, Encoder},
    transport,
};
use tracing::{debug, error, info};

use lading_payload::block;

use super::General;
use crate::generator::common::{
    BlockThrottle, ConcurrencyStrategy, MetricsBuilder, ThrottleConfig, ThrottleConversionError,
    create_throttle,
};

static CONNECTION_SEMAPHORE: OnceCell<Semaphore> = OnceCell::new();

/// Errors produced by [`Grpc`]
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// The remote RPC endpoint returned an error.
    #[error("RPC endpoint error: {0}")]
    Rpc(Box<tonic::Status>),
    /// gRPC transport error
    #[error("gRPC transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    /// Creation of payload blocks failed.
    #[error("Block creation error: {0}")]
    Block(#[from] block::Error),
    /// IO error
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Byte error
    #[error("Bytes must not be negative: {0}")]
    Byte(#[from] byte_unit::ParseError),
    /// Zero value
    #[error("Value provided must not be zero")]
    Zero,
    /// Throttle configuration error
    #[error("Throttle configuration error: {0}")]
    ThrottleConversion(#[from] ThrottleConversionError),
    /// Error connecting to gRPC server
    #[error("Failed to connect to gRPC endpoint {endpoint}: {source}")]
    ConnectionFailed {
        /// gRPC endpoint
        endpoint: String,
        /// Underlying transport error
        #[source]
        source: Box<tonic::transport::Error>,
    },
    /// Error making RPC request
    #[error("Failed to make RPC request to {endpoint}{path}: {source}")]
    RpcRequestFailed {
        /// gRPC endpoint
        endpoint: String,
        /// RPC path
        path: String,
        /// Underlying RPC error
        #[source]
        source: Box<tonic::Status>,
    },
}

impl From<tonic::Status> for Error {
    fn from(status: tonic::Status) -> Self {
        Error::Rpc(Box::new(status))
    }
}

/// Config for [`Grpc`]
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The gRPC URI. Looks like `http://<host>/<service path>/<endpoint>`
    pub target_uri: String,
    /// The seed for random operations against this target
    pub seed: [u8; 32],
    /// The payload variant. This should be protobuf encoded for typical gRPC
    /// endpoints.
    pub variant: lading_payload::Config,
    /// The bytes per second to send or receive from the target
    pub bytes_per_second: Option<Byte>,
    /// The maximum size in bytes of the cache of prebuilt messages
    pub maximum_prebuild_cache_size_bytes: Byte,
    /// The maximum size in bytes of the largest block in the prebuild cache.
    #[serde(default = "lading_payload::block::default_maximum_block_size")]
    pub maximum_block_size: Byte,
    /// Whether to use a fixed or streaming block cache
    #[serde(default = "lading_payload::block::default_cache_method")]
    pub block_cache_method: block::CacheMethod,
    /// The total number of parallel connections to maintain
    pub parallel_connections: u16,
    /// The load throttle configuration
    pub throttle: Option<ThrottleConfig>,
}

/// No-op tonic codec. Sends raw bytes and returns the number of bytes received.
#[derive(Debug, Clone, Default, Copy)]
pub struct NoopCodec;

impl tonic::codec::Codec for NoopCodec {
    type Encode = Bytes;
    type Decode = usize;

    type Encoder = Self;
    type Decoder = CountingDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        Self
    }

    fn decoder(&mut self) -> Self::Decoder {
        CountingDecoder
    }
}

impl Encoder for NoopCodec {
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, buf: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        buf.put(item);
        Ok(())
    }
}

/// This decoder returns the number of bytes received
#[derive(Debug, Clone, Default, Copy)]
pub struct CountingDecoder;
impl Decoder for CountingDecoder {
    type Item = usize;
    type Error = Status;

    fn decode(&mut self, buf: &mut DecodeBuf<'_>) -> Result<Option<usize>, Self::Error> {
        let response_bytes = buf.remaining();

        // Consume the provided response buffer. If this isn't done, tonic will
        // throw an unexpected EOF error while processing the response.
        buf.advance(response_bytes);

        Ok(Some(response_bytes))
    }
}

/// The gRPC generator.
///
/// This generator is able to connect to targets via gRPC.
#[derive(Debug)]
pub struct Grpc {
    target_uri: Uri,
    rpc_path: PathAndQuery,
    concurrency: ConcurrencyStrategy,
    shutdown: lading_signal::Watcher,
    throttle: BlockThrottle,
    block_cache: Arc<block::Cache>,
    metric_labels: Vec<(String, String)>,
}

impl Grpc {
    /// Create a new [`Grpc`] instance.
    ///
    /// # Errors
    ///
    /// Creation will fail if the underlying governor capacity exceeds u32.
    ///
    /// # Panics
    ///
    /// Function will panic if user has passed zero values for any byte
    /// values. Sharp corners.
    #[expect(clippy::cast_possible_truncation)]
    pub fn new(
        general: General,
        config: Config,
        shutdown: lading_signal::Watcher,
    ) -> Result<Self, Error> {
        let mut rng = StdRng::from_seed(config.seed);
        let labels = MetricsBuilder::new("grpc").with_id(general.id).build();

        let throttle = create_throttle(config.throttle.as_ref(), config.bytes_per_second.as_ref())?;

        let maximum_prebuild_cache_size_bytes =
            NonZeroU32::new(config.maximum_prebuild_cache_size_bytes.as_u128() as u32)
                .ok_or(Error::Zero)?;
        let block_cache = match config.block_cache_method {
            block::CacheMethod::Fixed => block::Cache::fixed_with_max_overhead(
                &mut rng,
                maximum_prebuild_cache_size_bytes,
                config.maximum_block_size.as_u128(),
                &config.variant,
                // NOTE we bound payload generation to have overhead only
                // equivalent to the prebuild cache size,
                // `maximum_prebuild_cache_size_bytes`. This means on systems with plentiful
                // memory we're under generating entropy, on systems with
                // minimal memory we're over-generating.
                //
                // `lading::get_available_memory` suggests we can learn to
                // divvy this up in the future.
                maximum_prebuild_cache_size_bytes.get() as usize,
            )?,
        };

        let concurrency =
            ConcurrencyStrategy::new(NonZeroU16::new(config.parallel_connections), false);

        CONNECTION_SEMAPHORE
            .set(Semaphore::new(concurrency.connection_count() as usize))
            .expect("failed to set semaphore");

        let target_uri =
            uri::Uri::try_from(config.target_uri).expect("target_uri must be valid");
        let rpc_path = target_uri
            .path_and_query()
            .cloned()
            .expect("target_uri should have an RPC path");
        Ok(Self {
            target_uri,
            rpc_path,
            concurrency,
            shutdown,
            block_cache: Arc::new(block_cache),
            throttle,
            metric_labels: labels,
        })
    }

    /// Establish a connection with the configured RPC server
    async fn connect(&self) -> Result<client::Grpc<transport::Channel>, Error> {
        let mut parts = self.target_uri.clone().into_parts();
        parts.path_and_query = Some(PathAndQuery::from_static(""));
        let uri = Uri::from_parts(parts).expect("failed to convert parts into uri");

        let endpoint = transport::Endpoint::new(uri)?;
        let endpoint = endpoint.connect_timeout(Duration::from_secs(1));
        let conn = endpoint.connect().await?;
        let conn = client::Grpc::new(conn);

        debug!("gRPC generator connected");

        Ok(conn)
    }

    /// Send one RPC request
    async fn req(
        client: &mut client::Grpc<transport::Channel>,
        rpc_path: http::uri::PathAndQuery,
        request: Bytes,
    ) -> Result<Response<usize>, tonic::Status> {
        client.ready().await.map_err(|e| {
            tonic::Status::new(tonic::Code::Unknown, format!("Service was not ready: {e}"))
        })?;
        let res = client
            .unary(Request::new(request), rpc_path, NoopCodec)
            .await?;

        Ok(res)
    }

    /// Run to completion or until a shutdown signal is received.
    ///
    /// # Errors
    ///
    /// Function will return an error when the RPC connection cannot be
    /// established.
    ///
    /// # Panics
    ///
    /// Function will panic if underlying byte capacity is not available.
    pub async fn spin(mut self) -> Result<(), Error> {
        let client = loop {
            match self.connect().await {
                Ok(c) => break c,
                Err(source) => {
                    debug!(
                        "Failed to connect to gRPC endpoint {endpoint} (will retry): {source}",
                        endpoint = self.target_uri
                    );
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        };

        let mut handle = self.block_cache.handle();
        let rpc_path = self.rpc_path;
        let labels = self.metric_labels;

        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);
        loop {
            tokio::select! {
                result = self.throttle.wait_for_block(&self.block_cache, &handle) => {
                    let _ = result;
                    let block = self.block_cache.advance(&mut handle);
                    let block_bytes = Bytes::copy_from_slice(&block.bytes);
                    let block_length = block.bytes.len();
                    let data_points = block.metadata.data_points;

                    let mut task_client = client.clone();
                    let task_rpc_path = rpc_path.clone();
                    let task_labels = labels.clone();
                    let target_uri = self.target_uri.clone();

                    let permit = CONNECTION_SEMAPHORE
                        .get()
                        .expect("connection semaphore not initialized")
                        .acquire()
                        .await
                        .expect("connection semaphore closed");
                    tokio::spawn(async move {
                        counter!("requests_sent", &task_labels).increment(1);
                        let res = Self::req(
                            &mut task_client,
                            task_rpc_path.clone(),
                            block_bytes,
                        )
                        .await;

                        match res {
                            Ok(res) => {
                                counter!("bytes_written", &task_labels)
                                    .increment(block_length as u64);
                                if let Some(dp) = data_points {
                                    counter!("data_points_transmitted", &task_labels)
                                        .increment(dp);
                                }
                                counter!("request_ok", &task_labels).increment(1);
                                counter!("response_bytes", &task_labels)
                                    .increment(res.into_inner() as u64);
                            }
                            Err(source) => {
                                error!(
                                    "Failed to make RPC request to {endpoint}{path}: {source}",
                                    endpoint = target_uri,
                                    path = task_rpc_path
                                );
                                let mut error_labels = task_labels.clone();
                                error_labels
                                    .push(("error".to_string(), source.to_string()));
                                counter!("request_failure", &error_labels).increment(1);
                            }
                        }
                        drop(permit);
                    });
                },
                () = &mut shutdown_wait => {
                    info!("shutdown signal received");
                    // Acquire all permits to ensure in-flight tasks complete
                    // before returning.
                    let _semaphore = CONNECTION_SEMAPHORE
                        .get()
                        .expect("connection semaphore not initialized")
                        .acquire_many(u32::from(self.concurrency.connection_count()))
                        .await
                        .expect("connection semaphore closed");
                    return Ok(());
                },
            }
        }
    }
}
