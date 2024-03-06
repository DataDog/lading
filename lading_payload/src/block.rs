//! Construct byte blocks for use in generators.
//!
//! The method that lading uses to maintain speed over its target is to avoid
//! runtime generation where possible _or_ to generate into a queue and consume
//! from that, decoupling the create/send operations. This module is the
//! mechanism by which 'blocks' -- that is, byte blobs of a predetermined size
//! -- are created.
use std::{collections::VecDeque, num::NonZeroU32};

use bytes::{buf::Writer, BufMut, Bytes, BytesMut};
use rand::{prelude::SliceRandom, rngs::StdRng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{self, error::SendError, Sender};
use tracing::{error, info, span, warn, Level};

const MAX_CHUNKS: usize = 16_384;

/// Error for `Cache::spin`
#[derive(Debug, thiserror::Error)]
pub enum SpinError {
    /// See [`SendError`]
    #[error(transparent)]
    Send(#[from] SendError<Block>),
    /// Provided configuration had validation errors
    #[error("Provided configuration was not valid: {0}")]
    InvalidConfig(String),
    /// Static payload creation error
    #[error(transparent)]
    Static(#[from] crate::statik::Error),
    /// rng slice is Empty
    #[error("RNG slice is empty")]
    EmptyRng,
    /// Error for crate deserialization
    #[error("Deserialization error: {0}")]
    Deserialize(#[from] crate::Error),
    /// Error for constructing the block cache
    #[error(transparent)]
    ConstructBlockCache(#[from] ConstructBlockCacheError),
    /// Zero value
    #[error("Value provided must not be zero")]
    Zero,
}

/// Error for [`Cache`]
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// See [`ChunkError`]
    #[error("Chunk error: {0}")]
    Chunk(#[from] ChunkError),
    /// See [`ConstructBlockCacheError`]
    #[error(transparent)]
    Construct(#[from] ConstructBlockCacheError),
    /// Provided configuration had validation errors
    #[error("Provided configuration was not valid: {0}")]
    InvalidConfig(String),
    /// Static payload creation error
    #[error(transparent)]
    Static(#[from] crate::statik::Error),
    /// Error for crate deserialization
    #[error("Deserialization error: {0}")]
    Deserialize(#[from] crate::Error),
    /// See [`SpinError`]
    #[error(transparent)]
    Spin(#[from] SpinError),
}

/// Errors for the construction of chunks
#[derive(Debug, thiserror::Error, Clone, Copy)]
pub enum ChunkError {
    /// The slice of byte sizes given to [`chunk_bytes`] was empty.
    #[error("The slice of byte sizes given was empty.")]
    EmptyBlockBytes,
    /// The `total_bytes` parameter is insufficient.
    #[error("Insufficient total bytes.")]
    InsufficientTotalBytes,
}

/// The fixed-size byte blob
#[derive(Debug, Clone)]
pub struct Block {
    /// The total number of bytes in this block.
    pub total_bytes: NonZeroU32,
    /// The bytes of this block.
    pub bytes: Bytes,
}

/// Errors for the construction of the block cache
#[derive(Debug, thiserror::Error, Clone, Copy)]
pub enum ConstructBlockCacheError {
    /// All blocks sizes were insufficient
    #[error("Insufficient block sizes.")]
    InsufficientBlockSizes,
}

#[cfg(feature = "arbitrary")]
impl<'a> arbitrary::Arbitrary<'a> for Block {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let total_bytes = u32::arbitrary(u)?;
        let bytes = u.bytes(total_bytes as usize).map(Bytes::copy_from_slice)?;
        Ok(Self {
            total_bytes: NonZeroU32::new(total_bytes).expect("total_bytes must be non-zero"),
            bytes,
        })
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone, Copy)]
#[serde(deny_unknown_fields)]
/// The method for which caching will be configure
pub enum CacheMethod {
    /// Create a single fixed size block cache and rotate through it
    Fixed,
    /// Maintain a fixed sized block cache buffer and stream from it
    Streaming,
}

/// The default cache method.
#[must_use]
pub fn default_cache_method() -> CacheMethod {
    CacheMethod::Fixed
}

#[derive(Debug)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
/// A mechanism for streaming byte blobs, 'blocks'
///
/// The `Cache` is a mechanism to allow generators to request 'blocks' without
/// needing to be aware of the origin or generation mechanism of these
/// blocks. We support a single mode of operation where all blocks are computed
/// ahead-of-time and stored in the `Cache`. Callers are responsible for timing
/// et al.
///
/// We expect to expand the different modes of `Cache` operation in the future.
pub enum Cache {
    /// A fixed size cache of blocks. Blocks are looped over in a round-robin
    /// fashion.
    Fixed {
        /// The current index into `blocks`
        idx: usize,
        /// The store of blocks.
        blocks: Vec<Block>,
    },
    /// A streaming cache of blocks. Blocks are generated on the fly and
    /// streamed through a queue.
    Stream {
        /// The seed used to construct the `Cache`.
        seed: [u8; 32],
        /// The total number of bytes that will be generated.
        total_bytes: u32,
        /// The sizes of the blocks that will be generated.
        block_chunks: Vec<u32>,
        /// The payload that will be generated.
        payload: crate::Config,
    },
}

impl Cache {
    /// Construct a streaming `Cache`.
    ///
    /// This constructor makes an internal pool of `Block` instances up to
    /// `total_bytes`, each of which are roughly the size of one of the
    /// `block_byte_sizes`. Internally, `Blocks` are replaced as they are spun out.
    ///
    /// # Errors
    ///
    /// Function will return an error if `block_byte_sizes` is empty or if a member
    /// of `block_byte_sizes` is large than `total_bytes`.
    pub fn stream(
        seed: [u8; 32],
        total_bytes: NonZeroU32,
        block_byte_sizes: &[NonZeroU32],
        payload: crate::Config,
    ) -> Result<Self, Error> {
        let mut block_chunks: [u32; MAX_CHUNKS] = [0; MAX_CHUNKS];
        let total_chunks = chunk_bytes(total_bytes, block_byte_sizes, &mut block_chunks)?;
        Ok(Self::Stream {
            seed,
            total_bytes: total_bytes.get(),
            block_chunks: block_chunks[..total_chunks].to_vec(),
            payload,
        })
    }

    /// Construct a `Cache` of fixed size.
    ///
    /// This constructor makes an internal pool of `Block` instances up to
    /// `total_bytes`, each of which are roughly the size of one of the
    /// `block_byte_sizes`. Internally, `Blocks` are looped over in a
    /// round-robin during peeking, iteration.
    ///
    /// # Errors
    ///
    /// Function will return an error if `block_byte_sizes` is empty or if a member
    /// of `block_byte_sizes` is large than `total_bytes`.
    #[allow(clippy::too_many_lines)]
    pub fn fixed<R>(
        mut rng: &mut R,
        total_bytes: NonZeroU32,
        block_byte_sizes: &[NonZeroU32],
        payload: &crate::Config,
    ) -> Result<Self, Error>
    where
        R: Rng + ?Sized,
    {
        let mut block_chunks: [u32; MAX_CHUNKS] = [0; MAX_CHUNKS];
        let total_chunks = chunk_bytes(total_bytes, block_byte_sizes, &mut block_chunks)?;
        let block_chunks = block_chunks[..total_chunks].to_vec();
        let blocks = match payload {
            crate::Config::TraceAgent(enc) => {
                let ta = match enc {
                    crate::Encoding::Json => crate::TraceAgent::json(&mut rng),
                    crate::Encoding::MsgPack => crate::TraceAgent::msg_pack(&mut rng),
                };

                let span = span!(
                    Level::INFO,
                    "fixed",
                    max_chunks = total_chunks,
                    payload = "trace-agent"
                );
                let _guard = span.enter();

                construct_block_cache_inner(&mut rng, &ta, &block_chunks)?
            }
            crate::Config::Syslog5424 => {
                let span = span!(
                    Level::INFO,
                    "fixed",
                    max_chunks = total_chunks,
                    payload = "syslog5424"
                );
                let _guard = span.enter();

                construct_block_cache_inner(&mut rng, &crate::Syslog5424::default(), &block_chunks)?
            }
            crate::Config::DogStatsD(conf) => {
                match conf.valid() {
                    Ok(()) => (),
                    Err(e) => {
                        warn!("Invalid DogStatsD configuration: {}", e);
                        return Err(Error::InvalidConfig(e));
                    }
                }
                let serializer = crate::DogStatsD::new(*conf, &mut rng)?;

                let span = span!(
                    Level::INFO,
                    "fixed",
                    max_chunks = total_chunks,
                    payload = "dogstatsd"
                );
                let _guard = span.enter();

                construct_block_cache_inner(&mut rng, &serializer, &block_chunks)?
            }
            crate::Config::Fluent => {
                let pyld = crate::Fluent::new(&mut rng);
                let span = span!(
                    Level::INFO,
                    "fixed",
                    max_chunks = total_chunks,
                    payload = "fluent"
                );
                let _guard = span.enter();
                construct_block_cache_inner(&mut rng, &pyld, &block_chunks)?
            }
            crate::Config::SplunkHec { encoding } => {
                let span = span!(
                    Level::INFO,
                    "fixed",
                    max_chunks = total_chunks,
                    payload = "splunkHec"
                );
                let _guard = span.enter();
                construct_block_cache_inner(
                    &mut rng,
                    &crate::SplunkHec::new(*encoding),
                    &block_chunks,
                )?
            }
            crate::Config::ApacheCommon => {
                let pyld = crate::ApacheCommon::new(&mut rng);
                let span = span!(
                    Level::INFO,
                    "fixed",
                    max_chunks = total_chunks,
                    payload = "apache-common"
                );
                let _guard = span.enter();
                construct_block_cache_inner(&mut rng, &pyld, &block_chunks)?
            }
            crate::Config::Ascii => {
                let pyld = crate::Ascii::new(&mut rng);
                let span = span!(
                    Level::INFO,
                    "fixed",
                    max_chunks = total_chunks,
                    payload = "ascii"
                );
                let _guard = span.enter();
                construct_block_cache_inner(&mut rng, &pyld, &block_chunks)?
            }
            crate::Config::DatadogLog => {
                let serializer = crate::DatadogLog::new(&mut rng);
                let span = span!(
                    Level::INFO,
                    "fixed",
                    max_chunks = total_chunks,
                    payload = "datadog-log"
                );
                let _guard = span.enter();
                construct_block_cache_inner(&mut rng, &serializer, &block_chunks)?
            }
            crate::Config::Json => {
                let span = span!(
                    Level::INFO,
                    "fixed",
                    max_chunks = total_chunks,
                    payload = "json"
                );
                let _guard = span.enter();
                construct_block_cache_inner(&mut rng, &crate::Json, &block_chunks)?
            }
            crate::Config::Static { ref static_path } => {
                let span = span!(
                    Level::INFO,
                    "fixed",
                    max_chunks = total_chunks,
                    payload = "static"
                );
                let _guard = span.enter();
                construct_block_cache_inner(
                    &mut rng,
                    &crate::Static::new(static_path)?,
                    &block_chunks,
                )?
            }
            crate::Config::OpentelemetryTraces => {
                let pyld = crate::OpentelemetryTraces::new(&mut rng);
                let span = span!(
                    Level::INFO,
                    "fixed",
                    max_chunks = total_chunks,
                    payload = "otel-traces"
                );
                let _guard = span.enter();
                construct_block_cache_inner(rng, &pyld, &block_chunks)?
            }
            crate::Config::OpentelemetryLogs => {
                let pyld = crate::OpentelemetryLogs::new(&mut rng);
                let span = span!(
                    Level::INFO,
                    "fixed",
                    max_chunks = total_chunks,
                    payload = "otel-logs"
                );
                let _guard = span.enter();
                construct_block_cache_inner(rng, &pyld, &block_chunks)?
            }
            crate::Config::OpentelemetryMetrics => {
                let pyld = crate::OpentelemetryMetrics::new(&mut rng);
                let span = span!(
                    Level::INFO,
                    "fixed",
                    max_chunks = total_chunks,
                    payload = "otel-metrics"
                );
                let _guard = span.enter();
                construct_block_cache_inner(rng, &pyld, &block_chunks)?
            }
        };
        Ok(Self::Fixed { idx: 0, blocks })
    }

    /// Run `Cache` forward on the user-provided mpsc sender.
    ///
    /// This is a blocking function that pushes `Block` instances into the
    /// user-provided mpsc `Sender<Block>`. The user is required to set an
    /// appropriate size on the channel. This function will never exit.
    ///
    /// # Errors
    ///
    /// Function will return an error if the user-provided mpsc `Sender<Block>`
    /// is closed.
    #[allow(clippy::needless_pass_by_value)]
    pub fn spin(self, snd: Sender<Block>) -> Result<(), SpinError> {
        match self {
            Self::Fixed { mut idx, blocks } => loop {
                snd.blocking_send(blocks[idx].clone())?;
                idx = (idx + 1) % blocks.len();
            },
            Cache::Stream {
                seed,
                total_bytes,
                block_chunks,
                payload,
            } => stream_inner(seed, total_bytes, &block_chunks, &payload, snd),
        }
    }
}

#[allow(clippy::needless_pass_by_value)]
#[inline]
fn stream_inner(
    seed: [u8; 32],
    total_bytes: u32,
    block_chunks: &[u32],
    payload: &crate::Config,
    snd: Sender<Block>,
) -> Result<(), SpinError> {
    let mut rng = StdRng::from_seed(seed);

    match payload {
        crate::Config::TraceAgent(enc) => {
            let ta = match enc {
                crate::Encoding::Json => crate::TraceAgent::json(&mut rng),
                crate::Encoding::MsgPack => crate::TraceAgent::msg_pack(&mut rng),
            };

            stream_block_inner(&mut rng, total_bytes, &ta, block_chunks, &snd)
        }
        crate::Config::Syslog5424 => {
            let pyld = crate::Syslog5424::default();
            stream_block_inner(&mut rng, total_bytes, &pyld, block_chunks, &snd)
        }
        crate::Config::DogStatsD(conf) => {
            match conf.valid() {
                Ok(()) => (),
                Err(e) => {
                    warn!("Invalid DogStatsD configuration: {}", e);
                    return Err(SpinError::InvalidConfig(e));
                }
            }
            let pyld = crate::DogStatsD::new(*conf, &mut rng)?;

            stream_block_inner(&mut rng, total_bytes, &pyld, block_chunks, &snd)
        }
        crate::Config::Fluent => {
            let pyld = crate::Fluent::new(&mut rng);
            stream_block_inner(&mut rng, total_bytes, &pyld, block_chunks, &snd)
        }
        crate::Config::SplunkHec { encoding } => {
            let pyld = crate::SplunkHec::new(*encoding);
            stream_block_inner(&mut rng, total_bytes, &pyld, block_chunks, &snd)
        }
        crate::Config::ApacheCommon => {
            let pyld = crate::ApacheCommon::new(&mut rng);
            stream_block_inner(&mut rng, total_bytes, &pyld, block_chunks, &snd)
        }
        crate::Config::Ascii => {
            let pyld = crate::Ascii::new(&mut rng);
            stream_block_inner(&mut rng, total_bytes, &pyld, block_chunks, &snd)
        }
        crate::Config::DatadogLog => {
            let pyld = crate::DatadogLog::new(&mut rng);
            stream_block_inner(&mut rng, total_bytes, &pyld, block_chunks, &snd)
        }
        crate::Config::Json => {
            let pyld = crate::Json;
            stream_block_inner(&mut rng, total_bytes, &pyld, block_chunks, &snd)
        }
        crate::Config::Static { ref static_path } => {
            let pyld = crate::Static::new(static_path)?;
            stream_block_inner(&mut rng, total_bytes, &pyld, block_chunks, &snd)
        }
        crate::Config::OpentelemetryTraces => {
            let pyld = crate::OpentelemetryTraces::new(&mut rng);
            stream_block_inner(&mut rng, total_bytes, &pyld, block_chunks, &snd)
        }
        crate::Config::OpentelemetryLogs => {
            let pyld = crate::OpentelemetryLogs::new(&mut rng);
            stream_block_inner(&mut rng, total_bytes, &pyld, block_chunks, &snd)
        }
        crate::Config::OpentelemetryMetrics => {
            let pyld = crate::OpentelemetryMetrics::new(&mut rng);
            stream_block_inner(&mut rng, total_bytes, &pyld, block_chunks, &snd)
        }
    }
}

/// Construct a vec of block sizes that fit into `total_bytes`.
///
/// We partition `total_bytes` by `block_byte_sizes` via a round-robin method.
/// Our goal is to terminate, more or less partition the space.
///
/// # Errors
///
/// Function will return an error if `block_byte_sizes` is empty or if a member
/// of `block_byte_sizes` is large than `total_bytes`.
fn chunk_bytes<const N: usize>(
    total_bytes: NonZeroU32,
    block_byte_sizes: &[NonZeroU32],
    chunks: &mut [u32; N],
) -> Result<usize, Error> {
    if block_byte_sizes.is_empty() {
        return Err(ChunkError::EmptyBlockBytes.into());
    }
    for bb in block_byte_sizes {
        if *bb > total_bytes {
            return Err(ChunkError::InsufficientTotalBytes.into());
        }
    }

    let mut total_chunks = 0;
    let mut bytes_remaining = total_bytes.get();

    let mut iter = block_byte_sizes.iter().cycle();
    while bytes_remaining > 0 {
        if total_chunks >= N {
            break;
        }
        let mut attempt_block = |block| {
            if block <= bytes_remaining {
                chunks[total_chunks] = block;
                total_chunks += 1;
                bytes_remaining -= block;
                return true;
            }
            false
        };

        // SAFETY: By construction, the iterator will never terminate.
        let block_size = iter
            .next()
            .expect("failed to get block size, should not fail")
            .get();

        if !attempt_block(block_size) {
            // Block size did not fit into the remaining bytes.
            // Try each block size one more time.
            let mut found_fitting_block = false;
            for block_size in block_byte_sizes {
                if attempt_block(block_size.get()) {
                    found_fitting_block = true;
                    break;
                }
            }
            if !found_fitting_block {
                #[cfg(not(kani))]
                {
                    warn!("Failed to fill the remaining {bytes_remaining} bytes after attempting each block size again.");
                }
                break;
            }
        }
    }

    #[cfg(not(kani))]
    {
        let computed_chunk_capacity = total_bytes.get() - bytes_remaining;
        let total_chunk_capacity = byte_unit::Byte::from_bytes(computed_chunk_capacity.into());
        let total_chunk_capacity_str = total_chunk_capacity.get_appropriate_unit(false).to_string();

        if bytes_remaining > 0 {
            let total_requested_bytes = byte_unit::Byte::from_bytes(total_bytes.get().into());
            let total_requested_bytes_str = total_requested_bytes
                .get_appropriate_unit(false)
                .to_string();

            let bytes_remaining = byte_unit::Byte::from_bytes(bytes_remaining.into());
            let bytes_remaining_str = bytes_remaining.get_appropriate_unit(false).to_string();
            let extra_advice = if total_chunks == N {
                "Max capacity of chunks was hit, consider making block sizes larger to fit more data."
            } else {
                "Current block sizes pack inefficiently, consider adding/changing the block sizes to better pack."
            };
            warn!("Failed to construct chunks adding up to {total_requested_bytes_str}. Chunks created have total capacity of {total_chunk_capacity_str}. {bytes_remaining_str} unfulfilled. {extra_advice}");
        }
        info!("Allocated {total_chunks} chunks with total capacity of {total_chunk_capacity_str}.");
    }

    Ok(total_chunks)
}

/// Construct a new block cache of form defined by `serializer`.
///
/// A "block cache" is a pre-made vec of serialized arbitrary instances of the
/// data implied by `serializer`. Considering that it's not cheap, necessarily,
/// to construct and serialize arbitrary data on the fly we want to do it ahead
/// of time. We vary the size of blocks -- via `block_chunks` -- to allow the
/// user to express a range of block sizes they wish to see.
///
/// # Panics
///
/// Function will panic if the `serializer` signals an error. In the future we
/// would like to propagate this error to the caller.
#[inline]
#[tracing::instrument(skip(rng, serializer, block_chunks))]
fn construct_block_cache_inner<R, S>(
    mut rng: &mut R,
    serializer: &S,
    block_chunks: &[u32],
) -> Result<Vec<Block>, SpinError>
where
    S: crate::Serialize,
    R: Rng + ?Sized,
{
    let mut block_cache: Vec<Block> = Vec::with_capacity(block_chunks.len());
    let mut block_iter = block_chunks.iter().peekable();
    let mut construct_block_failures = 0;
    let max_construct_block_failures: usize = block_chunks.len() * 5;
    while let Some(peeked_block) = block_iter.peek() {
        assert!(construct_block_failures <= max_construct_block_failures, "Block construction failed {construct_block_failures} times consecutively while trying to construct block of size {peeked_block}. Failing the program. Check payload generation parameters.");
        // Consume the block only if a block is successfully constructed
        // otherwise retry the block size until it succeeds.
        block_iter.next_if(|block_size| {
            match construct_block(&mut rng, serializer, **block_size) {
                Ok(Some(block)) => {
                    construct_block_failures = 0; // reset failure counter on success
                    block_cache.push(block);
                    true
                }
                Ok(None) | Err(_) => {
                    construct_block_failures += 1;
                    false
                }
            }
        });
    }
    if block_cache.is_empty() {
        error!("Empty block cache, unable to construct blocks!");
        Err(SpinError::ConstructBlockCache(
            ConstructBlockCacheError::InsufficientBlockSizes,
        ))
    } else {
        // "Capacity" here refers to how much data the block_chunks can hold.
        // Note that this may not match the end-user's requested pregen-cache-size.
        // This function is only concerned with filling up the chunks requested.
        let capacity_sum = block_chunks.iter().sum::<u32>();
        let filled_sum = block_cache.iter().map(|b| b.total_bytes.get()).sum::<u32>();

        if filled_sum == capacity_sum {
            info!(
                num_blocks = block_cache.len(),
                filled_byte_sum = filled_sum,
                capacity_sum = capacity_sum,
                "Block cache constructed."
            );
        } else {
            let filled_sum_str = byte_unit::Byte::from_bytes(filled_sum.into())
                .get_appropriate_unit(false)
                .to_string();
            let capacity_sum_str = byte_unit::Byte::from_bytes(capacity_sum.into())
                .get_appropriate_unit(false)
                .to_string();
            warn!(
                "Filled {filled_sum_str} only. Could not fill up to the chunk capacity of {capacity_sum_str}."
            );
        }
        Ok(block_cache)
    }
}

#[inline]
fn stream_block_inner<R, S>(
    mut rng: &mut R,
    total_bytes: u32,
    serializer: &S,
    block_chunks: &[u32],
    snd: &Sender<Block>,
) -> Result<(), SpinError>
where
    S: crate::Serialize,
    R: Rng + ?Sized,
{
    let total_bytes: u64 = u64::from(total_bytes);
    let mut accum_bytes: u64 = 0;
    let mut cache: VecDeque<Block> = VecDeque::new();

    loop {
        // Attempt to read from the cache first, being sure to subtract the
        // bytes we send out.
        if let Some(block) = cache.pop_front() {
            accum_bytes -= u64::from(block.total_bytes.get());
            snd.blocking_send(block)?;
        }
        // There are no blocks in the cache. In order to minimize latency we
        // push blocks into the sender until such time as it's full. When that
        // happens we overflow into the cache until such time as that's full.
        'refill: loop {
            let block_size = block_chunks.choose(&mut rng).ok_or(SpinError::EmptyRng)?;
            if let Some(block) = construct_block(&mut rng, serializer, *block_size)? {
                match snd.try_reserve() {
                    Ok(permit) => permit.send(block),
                    Err(err) => match err {
                        mpsc::error::TrySendError::Full(()) => {
                            if accum_bytes < total_bytes {
                                accum_bytes += u64::from(block.total_bytes.get());
                                cache.push_back(block);
                                break 'refill;
                            }
                        }
                        mpsc::error::TrySendError::Closed(()) => return Ok(()),
                    },
                }
            }
        }
    }
}

/// Construct a new block
///
/// # Panics
///
/// Function will panic if the `serializer` signals an error. In the future we
/// would like to propagate this error to the caller.
#[inline]
fn construct_block<R, S>(
    mut rng: &mut R,
    serializer: &S,
    chunk_size: u32,
) -> Result<Option<Block>, SpinError>
where
    S: crate::Serialize,
    R: Rng + ?Sized,
{
    let mut block: Writer<BytesMut> = BytesMut::with_capacity(chunk_size as usize).writer();
    serializer.to_bytes(&mut rng, chunk_size as usize, &mut block)?;
    let bytes: Bytes = block.into_inner().freeze();
    if bytes.is_empty() {
        // Blocks may be empty, especially when the amount of bytes
        // requested for the block are relatively low. This is a quirk of
        // our use of randomness. We do not have the ability to tell that
        // library that we would like such and such number of bytes
        // approximately from an instance. This does mean that we sometimes
        // waste computation because the size of the block given cannot be
        // serialized into.
        Ok(None)
    } else {
        let total_bytes = NonZeroU32::new(
            bytes
                .len()
                .try_into()
                .expect("failed to get length of bytes"),
        )
        .ok_or(SpinError::Zero)?;
        Ok(Some(Block { total_bytes, bytes }))
    }
}

/// Get the block sizes from the configuration.
/// If none are present, then return the defaults.
///
/// # Panics
/// - Panics if a block size is not representable as a 32bit integer
/// - Panics if a block size is zero
pub fn get_blocks(
    config_block_sizes: &Option<Vec<byte_unit::Byte>>,
    block_size_limit: Option<byte_unit::Byte>,
) -> Vec<NonZeroU32> {
    let block_sizes = match config_block_sizes {
        Some(ref sizes) => {
            info!("Generator using user-specified block sizes: {:?}", sizes);
            sizes
                .iter()
                .map(|sz| {
                    NonZeroU32::new(
                        u32::try_from(sz.get_bytes())
                            .expect("Block size not representable as 32bit integer"),
                    )
                    .expect("bytes must be non-zero")
                })
                .collect()
        }
        None => default_blocks(),
    };
    if let Some(block_size_limit) = block_size_limit {
        let limit = NonZeroU32::new(
            u32::try_from(block_size_limit.get_bytes())
                .expect("Block size not representable as 32bit integer"),
        )
        .expect("block size limit must be non-zero");
        block_sizes.into_iter().filter(|sz| sz <= &limit).collect()
    } else {
        block_sizes
    }
}

#[must_use]
/// The default block sizes.
///
/// Panics are not possible in practice due to these being static values.
/// # Panics
/// - Panics if a block size is not representable as a 32bit integer
/// - Panics if a block size is zero
pub fn default_blocks() -> Vec<NonZeroU32> {
    [
        byte_unit::Byte::from_unit(1.0 / 32.0, byte_unit::ByteUnit::MB).expect("valid bytes"),
        byte_unit::Byte::from_unit(1.0 / 16.0, byte_unit::ByteUnit::MB).expect("valid bytes"),
        byte_unit::Byte::from_unit(1.0 / 4.0, byte_unit::ByteUnit::MB).expect("valid bytes"),
        byte_unit::Byte::from_unit(1.0 / 2.0, byte_unit::ByteUnit::MB).expect("valid bytes"),
        byte_unit::Byte::from_unit(1.0, byte_unit::ByteUnit::MB).expect("valid bytes"),
        byte_unit::Byte::from_unit(2.0, byte_unit::ByteUnit::MB).expect("valid bytes"),
        byte_unit::Byte::from_unit(4.0, byte_unit::ByteUnit::MB).expect("valid bytes"),
    ]
    .iter()
    .map(|sz| {
        NonZeroU32::new(
            u32::try_from(sz.get_bytes()).expect("Block size not representable as 32bit integer"),
        )
        .expect("Block size is non-zero")
    })
    .collect()
}

#[cfg(test)]
mod test {
    use std::num::NonZeroU32;

    use proptest::prelude::*;

    use crate::block::{chunk_bytes, get_blocks, MAX_CHUNKS};

    #[test]
    fn construct_block_cache_inner_fills_blocks() {
        use crate::block::construct_block_cache_inner;
        use rand::{rngs::SmallRng, SeedableRng};

        let mut rng = SmallRng::seed_from_u64(0);
        let block_chunks = [100, 100, 100, 200, 300];
        let serializer = crate::Json;
        let block_cache = construct_block_cache_inner(&mut rng, &serializer, &block_chunks)
            .expect("construct_block_cache_inner should not fail");

        assert_eq!(block_cache.len(), block_chunks.len());
    }
    proptest! {
        #[test]
        fn construct_block_cache_inner_fills_all_blocks(seed: u64, num_chunks in 1..=4096usize) {
            use crate::block::construct_block_cache_inner;
            use rand::{rngs::SmallRng, SeedableRng};

            let mut rng = SmallRng::seed_from_u64(seed);
            let block_chunks = vec![100; num_chunks];
            let serializer = crate::Json;
            let block_cache = construct_block_cache_inner(&mut rng, &serializer, &block_chunks)
                .expect("construct_block_cache_inner should not fail");

            assert_eq!(block_cache.len(), block_chunks.len());
        }
    }
    macro_rules! nz_u32 {
        ($value:expr) => {
            NonZeroU32::new($value).expect(concat!($value, " is non-zero"))
        };
    }

    /// This test ensures that `chunk_bytes` will re-use block sizes if it helps reach
    /// the desired total bytes.
    #[test]
    fn chunk_bytes_fills_using_repeated_blocks_if_needed() {
        // 10 bytes total, [3 1 9] block sizes, 10 chunks
        const NUM_CHUNKS: usize = 10;
        let total_bytes = nz_u32!(10);
        let block_byte_sizes = [nz_u32!(3), nz_u32!(1), nz_u32!(9)];
        let mut block_chunks: [u32; NUM_CHUNKS] = [0; NUM_CHUNKS];

        let res = chunk_bytes(total_bytes, &block_byte_sizes, &mut block_chunks)
            .expect("chunk_bytes should not fail");
        let populated_block_chunks = &block_chunks[0..res];
        let block_chunk_sum = populated_block_chunks.iter().sum::<u32>();
        assert_eq!(total_bytes.get(), block_chunk_sum);

        // 35 bytes total, [ 3 1 9 ] block sizes
        let total_bytes = nz_u32!(35);
        let block_byte_sizes = [nz_u32!(3), nz_u32!(1), nz_u32!(9)];
        let mut block_chunks: [u32; MAX_CHUNKS] = [0; MAX_CHUNKS];

        let res = chunk_bytes(total_bytes, &block_byte_sizes, &mut block_chunks)
            .expect("chunk_bytes should not fail");
        let populated_block_chunks = &block_chunks[0..res];
        let block_chunk_sum = populated_block_chunks.iter().sum::<u32>();
        assert_eq!(total_bytes.get(), block_chunk_sum);
    }

    proptest! {
        // This test does not pass! (hence the #[ignore])
        // It is trivial to find a combination of total_bytes and `block_byte_sizes` where
        // its not possible to reach the max amount. Consider total_bytes=5 and block_sizes=[4]

        // proptest todo - figure out how to express "block_byte_sizes members must be less than total_bytes"
        // until then, the strategy is to set the max block size to the min total_bytes
        #[ignore]
        #[test]
        fn chunk_bytes_yields_total_expected_bytes( total_bytes in 1_000..=100_000_000u32, block_byte_sizes in proptest::collection::vec(any::<NonZeroU32>(), 1..1_000)) {
            let total_bytes = NonZeroU32::new(total_bytes).expect("total_bytes is non-zero");
            let mut block_chunks: [u32; MAX_CHUNKS] = [0; MAX_CHUNKS];

            let res = chunk_bytes(total_bytes, &block_byte_sizes, &mut block_chunks)
                .expect("chunk_bytes should not fail");
            let populated_block_chunks = &block_chunks[0..res];
            let block_chunk_sum = populated_block_chunks.iter().sum::<u32>();
            let requested_bytes = total_bytes.get();
            assert_eq!(requested_bytes, block_chunk_sum, "filled {res} chunks (max allowed is {MAX_CHUNKS}), but couldn't satisfy request for {requested_bytes} bytes");
        }
    }

    #[test]
    fn get_blocks_works() {
        let config_block_sizes = Some(vec![
            byte_unit::Byte::from_unit(1.0, byte_unit::ByteUnit::MB).expect("valid bytes"),
            byte_unit::Byte::from_unit(2.0, byte_unit::ByteUnit::MB).expect("valid bytes"),
        ]);
        let block_size_limit =
            Some(byte_unit::Byte::from_unit(3.0, byte_unit::ByteUnit::MB).expect("valid bytes"));

        let result = get_blocks(&config_block_sizes, block_size_limit);
        assert_eq!(result.len(), 2);

        let block_size_limit =
            Some(byte_unit::Byte::from_unit(1.0, byte_unit::ByteUnit::MB).expect("valid bytes"));
        let result = get_blocks(&config_block_sizes, block_size_limit);
        assert_eq!(result.len(), 1);

        let block_size_limit =
            Some(byte_unit::Byte::from_unit(0.1, byte_unit::ByteUnit::MB).expect("valid bytes"));
        let result = get_blocks(&config_block_sizes, block_size_limit);
        assert_eq!(result.len(), 0);

        let result = get_blocks(&config_block_sizes, None);
        assert_eq!(result.len(), 2);
    }
}

#[cfg(kani)]
mod verification {
    use crate::block::chunk_bytes;
    use std::num::NonZeroU32;

    /// Function `chunk_bytes` will always fail with an error if the passed
    /// `block_byte_sizes` is empty.
    #[kani::proof]
    #[kani::unwind(11)]
    fn chunk_bytes_empty_sizes_error() {
        let total_bytes: NonZeroU32 = kani::any();
        let block_byte_sizes = [];
        let mut block_chunks: [u32; 10] = [0; 10];

        let res = chunk_bytes(total_bytes, &block_byte_sizes, &mut block_chunks);
        kani::assert(
            res.is_err(),
            "chunk_bytes must always fail if block_byte_sizes is empty.",
        );
    }

    /// Function `chunk_bytes` should not fail if no member of block sizes is
    /// large than `total_bytes`.
    #[kani::proof]
    #[kani::unwind(11)]
    fn chunk_bytes_sizes_under_under_check() {
        let total_bytes: NonZeroU32 = kani::any_where(|x: &NonZeroU32| x.get() < 64);
        let mut block_chunks: [u32; 10] = [0; 10];

        let under: NonZeroU32 = kani::any_where(|x| *x < total_bytes);

        kani::assert(
            chunk_bytes(total_bytes, &[under, under], &mut block_chunks).is_ok(),
            "chunk_bytes should not fail when all sizes are under limit",
        );
    }

    /// Function `chunk_bytes` should not fail if no member of block sizes is
    /// large than `total_bytes`.
    #[kani::proof]
    #[kani::unwind(11)]
    fn chunk_bytes_sizes_under_equal_check() {
        let total_bytes: NonZeroU32 = kani::any();
        let mut block_chunks: [u32; 10] = [0; 10];

        let under: NonZeroU32 = kani::any_where(|x| *x < total_bytes);
        let equal = total_bytes;

        kani::assert(
            chunk_bytes(total_bytes, &[under, equal], &mut block_chunks).is_ok(),
            "chunk_bytes should not fail when all sizes are under or equal to limit",
        );
    }

    /// Function `chunk_bytes` will fail if any member of `block_byte_sizes` is
    /// larger than `total_bytes`.
    #[kani::proof]
    #[kani::unwind(11)]
    fn chunk_bytes_sizes_under_equal_over_check() {
        let total_bytes: NonZeroU32 = kani::any();
        let mut block_chunks: [u32; 10] = [0; 10];

        let under: NonZeroU32 = kani::any_where(|x| *x < total_bytes);
        let equal = total_bytes;
        let over: NonZeroU32 = kani::any_where(|x| *x > total_bytes);

        kani::assert(
            chunk_bytes(total_bytes, &[under, equal, over], &mut block_chunks).is_err(),
            "chunk_bytes should fail when not all sizes are under the limit",
        );
    }

    /// Function `chunk_bytes` does not fail to return some chunks.
    #[kani::proof]
    #[kani::unwind(11)]
    fn chunk_bytes_never_chunk_empty() {
        let total_bytes: NonZeroU32 = kani::any();
        let byte_sizes: [NonZeroU32; 5] = [
            kani::any_where(|x| *x < total_bytes),
            kani::any_where(|x| *x < total_bytes),
            kani::any_where(|x| *x < total_bytes),
            kani::any_where(|x| *x < total_bytes),
            kani::any_where(|x| *x < total_bytes),
        ];
        let mut block_chunks: [u32; 10] = [0; 10];

        let chunks = chunk_bytes(total_bytes, &byte_sizes, &mut block_chunks)
            .expect("chunk_bytes should never fail");
        kani::assert(
            chunks > 0,
            "chunk_bytes should never return an empty vec of chunks",
        );
    }

    /// Function `chunk_bytes` does not return a chunk that is not present in
    /// the byte sizes.
    #[kani::proof]
    #[kani::unwind(11)]
    fn chunk_bytes_always_present() {
        let total_bytes: NonZeroU32 = kani::any();
        let byte_sizes: [NonZeroU32; 5] = [
            kani::any_where(|x| *x < total_bytes),
            kani::any_where(|x| *x < total_bytes),
            kani::any_where(|x| *x < total_bytes),
            kani::any_where(|x| *x < total_bytes),
            kani::any_where(|x| *x < total_bytes),
        ];
        let mut block_chunks: [u32; 10] = [0; 10];

        let chunks = chunk_bytes(total_bytes, &byte_sizes, &mut block_chunks)
            .expect("chunk_bytes should never fail");
        for chunk in &block_chunks[0..chunks] {
            kani::assert(
                byte_sizes.contains(&NonZeroU32::new(*chunk).expect("chunk must be non-zero")),
                "chunk_bytes should never return a chunk that is not present in the byte sizes",
            );
        }
    }

    /// Function `chunk_bytes` does not populate values above the returned
    /// index, that is, they all remain zero.
    #[kani::proof]
    #[kani::unwind(11)]
    fn chunk_bytes_never_populate_above_index() {
        let total_bytes: NonZeroU32 = kani::any();
        let byte_sizes: [NonZeroU32; 5] = [
            kani::any_where(|x| *x < total_bytes),
            kani::any_where(|x| *x < total_bytes),
            kani::any_where(|x| *x < total_bytes),
            kani::any_where(|x| *x < total_bytes),
            kani::any_where(|x| *x < total_bytes),
        ];
        let mut block_chunks: [u32; 10] = [0; 10];

        let chunks = chunk_bytes(total_bytes, &byte_sizes, &mut block_chunks)
            .expect("chunk_bytes should never fail");
        for chunk in &block_chunks[chunks..] {
            kani::assert(
                *chunk == 0,
                "chunk_bytes should never populate values above the returned index",
            );
        }
    }
}
