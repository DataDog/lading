//! The gRPC generator

use std::{
    convert::TryFrom,
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

use bytes::{Buf, BufMut, Bytes};
use http::{uri::PathAndQuery, Uri};
use metrics::{counter, gauge, register_counter};
use rand::rngs::StdRng;
use rand::SeedableRng;
use serde::Deserialize;
use tonic::{
    codec::{DecodeBuf, Decoder, EncodeBuf, Encoder},
    Request, Response, Status,
};
use tracing::{debug, info};

use crate::{
    block::{self, chunk_bytes, construct_block_cache, Block},
    payload,
    signals::Shutdown,
    throttle::Throttle,
};

/// Errors produced by [`Grpc`]
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// The remote RPC endpoint returned an error.
    #[error("RPC endpoint error: {0}")]
    Rpc(#[from] tonic::Status),
    /// gRPC transport error
    #[error("gRPC transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    /// Creation of payload blocks failed.
    #[error("Block creation error: {0}")]
    Block(#[from] block::Error),
}

/// Config for [`Grpc`]
#[derive(Debug, Deserialize, PartialEq)]
pub struct Config {
    /// The gRPC URI. Looks like http://host/service.path/endpoint
    pub target_uri: String,
    /// The seed for random operations against this target
    pub seed: [u8; 32],
    /// The payload variant. This should be protobuf encoded for typical gRPC
    /// endpoints.
    pub variant: payload::Config,
    /// The bytes per second to send or receive from the target
    pub bytes_per_second: byte_unit::Byte,
    /// The block sizes for messages to this target
    pub block_sizes: Option<Vec<byte_unit::Byte>>,
    /// The maximum size in bytes of the cache of prebuilt messages
    pub maximum_prebuild_cache_size_bytes: byte_unit::Byte,
    /// The total number of parallel connections to maintain
    pub parallel_connections: u16,
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
    config: Config,
    target_uri: Uri,
    rpc_path: PathAndQuery,
    shutdown: Shutdown,
    throttle: Throttle,
    block_cache: Vec<Block>,
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
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(config: Config, shutdown: Shutdown) -> Result<Self, Error> {
        use byte_unit::{Byte, ByteUnit};

        let mut rng = StdRng::from_seed(config.seed);
        let block_sizes: Vec<NonZeroUsize> = config
            .block_sizes
            .clone()
            .unwrap_or_else(|| {
                vec![
                    Byte::from_unit(1.0 / 32.0, ByteUnit::MB).unwrap(),
                    Byte::from_unit(1.0 / 16.0, ByteUnit::MB).unwrap(),
                    Byte::from_unit(1.0 / 8.0, ByteUnit::MB).unwrap(),
                    Byte::from_unit(1.0 / 4.0, ByteUnit::MB).unwrap(),
                    Byte::from_unit(1.0 / 2.0, ByteUnit::MB).unwrap(),
                    Byte::from_unit(1_f64, ByteUnit::MB).unwrap(),
                    Byte::from_unit(2_f64, ByteUnit::MB).unwrap(),
                    Byte::from_unit(4_f64, ByteUnit::MB).unwrap(),
                ]
            })
            .iter()
            .map(|sz| NonZeroUsize::new(sz.get_bytes() as usize).expect("bytes must be non-zero"))
            .collect();
        let labels = vec![
            ("component".to_string(), "generator".to_string()),
            ("component_name".to_string(), "grpc".to_string()),
        ];

        let bytes_per_second = NonZeroU32::new(config.bytes_per_second.get_bytes() as u32).unwrap();
        gauge!(
            "bytes_per_second",
            f64::from(bytes_per_second.get()),
            &labels
        );

        let block_chunks = chunk_bytes(
            &mut rng,
            NonZeroUsize::new(config.maximum_prebuild_cache_size_bytes.get_bytes() as usize)
                .expect("bytes must be non-zero"),
            &block_sizes,
        )?;
        let block_cache = construct_block_cache(&mut rng, &config.variant, &block_chunks, &labels);

        let target_uri =
            http::uri::Uri::try_from(config.target_uri.clone()).expect("target_uri must be valid");
        let rpc_path = target_uri
            .path_and_query()
            .cloned()
            .expect("target_uri should have an RPC path");

        Ok(Self {
            target_uri,
            rpc_path,
            config,
            shutdown,
            block_cache,
            throttle: Throttle::new(bytes_per_second),
            metric_labels: labels,
        })
    }

    /// Establish a connection with the configured RPC server
    async fn connect(&self) -> Result<tonic::client::Grpc<tonic::transport::Channel>, Error> {
        let mut parts = self.target_uri.clone().into_parts();
        parts.path_and_query = Some(PathAndQuery::from_static(""));
        let uri = Uri::from_parts(parts).unwrap();

        let endpoint = tonic::transport::Endpoint::new(uri)?;
        let endpoint = endpoint.concurrency_limit(self.config.parallel_connections as usize);
        let endpoint = endpoint.connect_timeout(Duration::from_secs(1));
        let conn = endpoint.connect().await?;
        let conn = tonic::client::Grpc::new(conn);

        debug!("gRPC generator connected");

        Ok(conn)
    }

    /// Send one RPC request
    async fn req(
        client: &mut tonic::client::Grpc<tonic::transport::Channel>,
        rpc_path: http::uri::PathAndQuery,
        request: Bytes,
    ) -> Result<Response<usize>, tonic::Status> {
        client.ready().await.map_err(|e| {
            tonic::Status::new(tonic::Code::Unknown, format!("Service was not ready: {e}"))
        })?;
        let res = client
            .unary(Request::new(request), rpc_path, NoopCodec::default())
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
        let mut client = loop {
            match self.connect().await {
                Ok(c) => break c,
                Err(e) => debug!("Failed to connect gRPC generator (will retry): {}", e),
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        };

        let mut blocks = self.block_cache.iter().cycle().peekable();
        let rpc_path = self.rpc_path;

        let requests_sent = register_counter!("requests_sent", &self.metric_labels);
        let bytes_written = register_counter!("bytes_written", &self.metric_labels);
        let request_ok = register_counter!("request_ok", &self.metric_labels);
        let response_bytes = register_counter!("response_bytes", &self.metric_labels);

        loop {
            let blk = blocks.peek().unwrap();
            let total_bytes = blk.total_bytes;

            tokio::select! {
                _ = self.throttle.wait_for(total_bytes) => {
                    let block_length = blk.bytes.len();
                    requests_sent.increment(1);
                    let blk = blocks.next().unwrap(); // actually advance through the blocks
                    let res = Self::req(
                        &mut client,
                        rpc_path.clone(),
                        Bytes::copy_from_slice(&blk.bytes),
                    )
                    .await;

                    match res {
                        Ok(res) => {
                            bytes_written.increment(block_length as u64);
                            request_ok.increment(1);
                            response_bytes.increment(res.into_inner() as u64);
                        }
                        Err(err) => {
                            let mut error_labels = self.metric_labels.clone();
                            error_labels.push(("error".to_string(), err.to_string()));
                            counter!("request_failure", 1, &error_labels);
                        }
                    }
                },
                _ = self.shutdown.recv() => {
                    info!("shutdown signal received");
                    break;
                },
            }
        }

        Ok(())
    }
}
