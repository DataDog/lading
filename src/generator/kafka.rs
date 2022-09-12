//! The Kafka generator.

use std::{
    collections::HashMap,
    convert::TryInto,
    num::{NonZeroU32, NonZeroUsize},
};

use byte_unit::{Byte, ByteUnit};
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use governor::{
    clock, state,
    state::direct::{self, InsufficientCapacity},
    Quota, RateLimiter,
};
use metrics::{counter, increment_counter};
use rand::{prelude::StdRng, SeedableRng};
use rdkafka::{
    config::FromClientConfig,
    error::KafkaError,
    producer::{FutureProducer, FutureRecord},
    types::RDKafkaErrorCode,
    ClientConfig,
};
use serde::Deserialize;
use tracing::info;

use crate::{
    block::{self, chunk_bytes, construct_block_cache, Block},
    payload,
    signals::Shutdown,
};

/// Configuration for generator throughput.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Throughput {
    /// The producer should run as fast as possible.
    Unlimited,
    /// The producer is limited to sending a certain number of bytes every
    /// second.
    BytesPerSecond {
        /// Number of bytes.
        amount: byte_unit::Byte,
    },
    /// The producer is limited to sending a certain number of messages every
    /// second.
    MessagesPerSecond {
        /// Number of messages.
        amount: u32,
    },
}

#[derive(Clone, Debug, Deserialize)]
/// Configuration for [`Kafka`]
pub struct Config {
    /// The seed for random operations against this target
    pub seed: [u8; 32],
    /// Bootstrap server for Kafka.  Used identically like the flag of the same
    /// name present on Kafka CLI tooling.
    pub bootstrap_server: String,
    /// Topic to produce to.
    pub topic: String,
    /// The payload generator to use for this target
    pub variant: payload::Config,
    /// The throughput configuration
    pub throughput: Throughput,
    /// The maximum size in bytes of the cache of prebuilt messages
    pub maximum_prebuild_cache_size_bytes: byte_unit::Byte,
    /// The block sizes for messages to this target
    pub block_sizes: Option<Vec<byte_unit::Byte>>,
    /// Map of rdkafka=-specific overrides to apply to the producer
    pub producer_config: Option<HashMap<String, String>>,
}

#[derive(Debug)]
/// Errors produced by [`Kafka`]
pub enum Error {
    /// Rate limiter has insuficient capacity for payload. Indicates a serious
    /// bug.
    Governor(InsufficientCapacity),
    /// Wrapper for [`std::io::Error`]
    Io(::std::io::Error),
    /// Creation of payload blocks failed.
    Block(block::Error),
    /// Wrapper around [`rdkafka::error::KafkaError`].
    Kafka(rdkafka::error::KafkaError),
}

impl From<block::Error> for Error {
    fn from(error: block::Error) -> Self {
        Error::Block(error)
    }
}

impl From<rdkafka::error::KafkaError> for Error {
    fn from(error: rdkafka::error::KafkaError) -> Self {
        Error::Kafka(error)
    }
}

impl From<InsufficientCapacity> for Error {
    fn from(error: InsufficientCapacity) -> Self {
        Error::Governor(error)
    }
}

impl From<::std::io::Error> for Error {
    fn from(error: ::std::io::Error) -> Self {
        Error::Io(error)
    }
}

#[derive(Debug)]
/// The Kafka generator.
pub struct Kafka {
    block_cache: Vec<Block>,
    labels: Vec<(String, String)>,
    bootstrap_server: String,
    topic: String,
    producer_config: Option<HashMap<String, String>>,
    throughput: Throughput,
    shutdown: Shutdown,
}

impl Kafka {
    /// Create a new [`Kafka`] instance
    ///
    /// # Errors
    ///
    /// Creation will fail if the underlying governor capacity exceeds u32.
    ///
    /// # Panics
    ///
    /// Function will panic if user has passed non-zero values for any byte
    /// values. Sharp corners.
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(config: Config, shutdown: Shutdown) -> Result<Self, Error> {
        let labels = vec![];

        let block_sizes: Vec<NonZeroUsize> = config
            .block_sizes
            .clone()
            .unwrap_or_else(|| {
                vec![
                    Byte::from_unit(1.0 / 8.0, ByteUnit::MB).unwrap(),
                    Byte::from_unit(1.0 / 16.0, ByteUnit::MB).unwrap(),
                    Byte::from_unit(1.0 / 32.0, ByteUnit::MB).unwrap(),
                    Byte::from_unit(1.0 / 64.0, ByteUnit::MB).unwrap(),
                    Byte::from_unit(1.0 / 128.0, ByteUnit::MB).unwrap(),
                    Byte::from_unit(1.0 / 256.0, ByteUnit::MB).unwrap(),
                    Byte::from_unit(1.0 / 512.0, ByteUnit::MB).unwrap(),
                    Byte::from_unit(1.0 / 1024.0, ByteUnit::MB).unwrap(),
                ]
            })
            .iter()
            .map(|sz| NonZeroUsize::new(sz.get_bytes() as usize).expect("bytes must be non-zero"))
            .collect();
        let block_cache = generate_block_cache(
            config.maximum_prebuild_cache_size_bytes,
            &config.variant,
            config.seed,
            &block_sizes,
            &labels,
        )?;

        Ok(Self {
            block_cache,
            labels,
            bootstrap_server: config.bootstrap_server,
            producer_config: config.producer_config,
            throughput: config.throughput,
            topic: config.topic,
            shutdown,
        })
    }

    /// Run [`Kafka`] to completion or until a shutdown signal is received.
    ///
    /// # Errors
    ///
    /// TODO
    ///
    /// # Panics
    ///
    /// Function will panic if it is unable to produce messages to the target
    /// Kafka cluster.
    pub async fn spin(mut self) -> Result<(), Error> {
        // Configure our Kafka producer.
        let bootstrap_server = self.bootstrap_server;
        let topic = self.topic;
        let labels = self.labels;

        let mut client_config = ClientConfig::new();
        let mut config_values = self.producer_config.unwrap_or_default();
        config_values.insert("bootstrap.servers".to_string(), bootstrap_server);
        for (k, v) in config_values.drain() {
            client_config.set(k, v);
        }

        let producer = FutureProducer::from_config(&client_config)?;

        // Configure our rate limiter.
        let limit_by_bytes = matches!(self.throughput, Throughput::BytesPerSecond { .. });
        let rate_limiter = get_rate_limiter(self.throughput);

        let mut in_flight = FuturesUnordered::new();

        // Now produce our messages.
        let mut blocks = self.block_cache.iter().cycle();
        loop {
            while let Some(Some(result)) = in_flight.next().now_or_never() {
                match result {
                    Ok(block_size) => {
                        increment_counter!("request_ok", &labels);
                        counter!("bytes_written", block_size, &labels);
                    }
                    Err(..) => {
                        counter!("request_failure", 1, &labels);
                    }
                }
            }

            let block = blocks.next().expect("should never be empty");
            let block_size = block.total_bytes;
            let limiter_n = if limit_by_bytes { block_size.get() } else { 1 };
            let limiter_n = NonZeroU32::new(limiter_n).expect("should never be zero");

            tokio::select! {
                _ = rate_limiter.until_n_ready(limiter_n) => {
                    let mut record = Some(
                        FutureRecord::to(topic.as_ref())
                            .payload(&block.bytes)
                            .key(&()),
                    );

                    loop {
                        match producer.send_result(record.take().unwrap()) {
                            Ok(fut) => {
                                counter!("requests_sent", 1, &labels);
                                in_flight
                                    .push(async move { fut.await.map(|_| u64::from(block_size.get())) });
                                break;
                            }
                            Err((e, old_record)) => {
                                if e == KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull) {
                                    record = Some(old_record);
                                    tokio::task::yield_now().await;
                                }
                            }
                        }
                    }
                }
                _ = self.shutdown.recv() => {
                    info!("shutdown signal received");
                    return Ok(());
                },
            }
        }
    }
}

fn generate_block_cache(
    cache_size: byte_unit::Byte,
    variant: &payload::Config,
    seed: [u8; 32],
    block_sizes: &[NonZeroUsize],
    #[allow(clippy::ptr_arg)] labels: &Vec<(String, String)>,
) -> Result<Vec<Block>, Error> {
    let mut rng = StdRng::from_seed(seed);

    let total_size = NonZeroUsize::new(cache_size.get_bytes().try_into().unwrap_or(usize::MAX))
        .expect("bytes must be non-zero");
    let chunks = chunk_bytes(&mut rng, total_size, block_sizes)?;

    let blocks = construct_block_cache(&mut rng, variant, &chunks, labels);
    Ok(blocks)
}

fn get_rate_limiter(
    throughput: Throughput,
) -> RateLimiter<direct::NotKeyed, state::InMemoryState, clock::QuantaClock> {
    match throughput {
        Throughput::Unlimited => {
            let amount = NonZeroU32::new(u32::MAX).expect("amount should not be zero");
            RateLimiter::direct(Quota::per_second(amount))
        }
        Throughput::BytesPerSecond { amount } => {
            let amount = if amount.get_bytes() == 0 {
                1
            } else {
                amount.get_bytes().try_into().unwrap_or(u32::MAX)
            };
            let amount = NonZeroU32::new(amount).expect("amount should not be zero");
            RateLimiter::direct(Quota::per_second(amount))
        }
        Throughput::MessagesPerSecond { amount } => {
            let amount = if amount == 0 { 1 } else { amount as u32 };
            let amount = NonZeroU32::new(amount).expect("amount should not be zero");

            RateLimiter::direct(Quota::per_second(amount))
        }
    }
}
