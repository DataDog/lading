use crate::kafka_gen::config::Throughput;
use crate::kafka_gen::config::{Target, Variant};
use byte_unit::{Byte, ByteUnit};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use governor::state::direct::{self, InsufficientCapacity};
use governor::{clock, state, Quota, RateLimiter};
use lading_common::block::{self, chunk_bytes, construct_block_cache, Block};
use lading_common::payload;
use metrics::{counter, increment_counter};
use rand::prelude::StdRng;
use rand::SeedableRng;
use rdkafka::config::FromClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::ClientConfig;
use std::convert::TryInto;
use std::num::NonZeroU32;

#[derive(Debug)]
pub enum Error {
    Governor(InsufficientCapacity),
    Io(::std::io::Error),
    Block(block::Error),
    Kafka(rdkafka::error::KafkaError),
    MpscClosed,
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

/// The [`Worker`] defines a task that emits variant lines to an HTTP server
/// controlling throughput.
#[derive(Debug)]
pub struct Worker {
    name: String,
    target: Target,
    block_cache: Vec<Block>,
    labels: Vec<(String, String)>,
}

impl Worker {
    /// Create a new [`Worker`] instance
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
    pub fn new(name: String, target: Target) -> Result<Self, Error> {
        let labels = vec![
            ("name".to_string(), name.clone()),
            ("server".to_string(), target.bootstrap_server.to_string()),
        ];

        let block_sizes: Vec<usize> = target
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
            .map(|sz| sz.get_bytes() as usize)
            .collect();
        let block_cache = generate_block_cache(
            target.maximum_prebuild_cache_size_bytes,
            target.variant,
            target.seed,
            &block_sizes,
            &labels,
        );

        Ok(Self {
            name,
            target,
            block_cache,
            labels,
        })
    }

    /// Enter the main loop of this [`Worker`]
    ///
    /// # Errors
    ///
    /// TODO
    ///
    /// # Panics
    ///
    /// Function will panic if it is unable to produce messages to the target Kafka cluster.
    pub async fn spin(self) -> Result<(), Error> {
        // Configure our Kafka producer.
        let bootstrap_server = self.target.bootstrap_server;
        let topic = self.target.topic;
        let labels = self.labels;

        let mut client_config = ClientConfig::new();
        let mut config_values = self.target.producer_config.unwrap_or_default();
        config_values.insert("bootstrap.servers".to_string(), bootstrap_server);
        for (k, v) in config_values.drain() {
            client_config.set(k, v);
        }

        let producer = FutureProducer::from_config(&client_config)?;

        // Configure our rate limiter.
        let limit_by_bytes = matches!(self.target.throughput, Throughput::BytesPerSecond { .. });
        let rate_limiter = get_rate_limiter(self.target.throughput);

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

            let _ = rate_limiter.until_n_ready(limiter_n).await;
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
    }
}

fn generate_block_cache(
    cache_size: byte_unit::Byte,
    variant: Variant,
    seed: [u8; 32],
    block_sizes: &[usize],
    #[allow(clippy::ptr_arg)] labels: &Vec<(String, String)>,
) -> Vec<Block> {
    let mut rng = StdRng::from_seed(seed);

    let total_size = cache_size.get_bytes().try_into().unwrap_or(usize::MAX);
    let chunks = chunk_bytes(&mut rng, total_size, block_sizes);

    match variant {
        Variant::Ascii => {
            construct_block_cache(&mut rng, &payload::Ascii::default(), &chunks, labels)
        }
        Variant::DatadogLog => {
            construct_block_cache(&mut rng, &payload::DatadogLog::default(), &chunks, labels)
        }
        Variant::Json => {
            construct_block_cache(&mut rng, &payload::Json::default(), &chunks, labels)
        }
        Variant::FoundationDb => {
            construct_block_cache(&mut rng, &payload::FoundationDb::default(), &chunks, labels)
        }
    }
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
