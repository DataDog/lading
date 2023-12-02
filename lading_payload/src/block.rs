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
use serde::Deserialize;
use tokio::sync::mpsc::{self, error::SendError, Sender};
use tracing::{error, info, span, Level};

use crate::dogstatsd;

const MAX_CHUNKS: usize = 4096;

/// Error for `Cache::spin`
#[derive(Debug, thiserror::Error)]
pub enum SpinError {
    /// See [`SendError`]
    #[error(transparent)]
    Send(#[from] SendError<Block>),
    /// Provided configuration had validation errors
    #[error("Provided configuration was not valid.")]
    InvalidConfig,
    /// DogStatsD creation error
    #[error(transparent)]
    DogStatsD(#[from] dogstatsd::Error),
    /// Static payload creation error
    #[error(transparent)]
    Static(#[from] crate::statik::Error),
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
    #[error("Provided configuration was not valid.")]
    InvalidConfig,
    /// DogStatsD creation error
    #[error(transparent)]
    DogStatsD(#[from] dogstatsd::Error),
    /// Static payload creation error
    #[error(transparent)]
    Static(#[from] crate::statik::Error),
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
            total_bytes: NonZeroU32::new(total_bytes).unwrap(),
            bytes,
        })
    }
}

#[derive(Debug, Deserialize, PartialEq, Clone, Copy)]
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
            crate::Config::DogStatsD(
                conf @ crate::dogstatsd::Config {
                    contexts,
                    name_length,
                    tag_key_length,
                    tag_value_length,
                    tags_per_msg,
                    // TODO -- Validate user input for multivalue_pack_probability.
                    multivalue_pack_probability,
                    multivalue_count,
                    sampling_range,
                    sampling_probability,
                    kind_weights,
                    metric_weights,
                    value,
                    service_check_names,
                },
            ) => {
                if !conf.valid() {
                    return Err(Error::InvalidConfig);
                }
                let serializer = crate::DogStatsD::new(
                    *contexts,
                    *service_check_names,
                    *name_length,
                    *tag_key_length,
                    *tag_value_length,
                    *tags_per_msg,
                    *multivalue_count,
                    *multivalue_pack_probability,
                    *sampling_range,
                    *sampling_probability,
                    *kind_weights,
                    *metric_weights,
                    *value,
                    &mut rng,
                )?;

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
        crate::Config::DogStatsD(
            conf @ crate::dogstatsd::Config {
                contexts,
                service_check_names,
                name_length,
                tag_key_length,
                tag_value_length,
                tags_per_msg,
                // TODO -- Validate user input for multivalue_pack_probability.
                multivalue_pack_probability,
                multivalue_count,
                sampling_range: sampling,
                sampling_probability,
                kind_weights,
                metric_weights,
                value,
            },
        ) => {
            if !conf.valid() {
                return Err(SpinError::InvalidConfig);
            }
            let pyld = crate::DogStatsD::new(
                *contexts,
                *service_check_names,
                *name_length,
                *tag_key_length,
                *tag_value_length,
                *tags_per_msg,
                *multivalue_count,
                *multivalue_pack_probability,
                *sampling,
                *sampling_probability,
                *kind_weights,
                *metric_weights,
                *value,
                &mut rng,
            )?;

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
/// Our goal is to terminate, more or less partition the space and do so without
/// biasing toward any given byte size.
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
        // SAFETY: By construction, the iterator will never terminate.
        let block_size = iter.next().unwrap().get();

        // Determine if the block_size fits in the remaining bytes. If it does,
        // great. If not, we break out of the round-robin.
        if block_size <= bytes_remaining {
            chunks[total_chunks] = block_size;
            total_chunks += 1;
            bytes_remaining -= block_size;
        } else {
            break;
        }
    }
    // It's possible at this point that there are still bytes remaining. If
    // there are, we loop over the block byte sizes one last time just in case
    // there's space left we can occupy.
    //
    // This could theoretically be packed a little tighter if we sorted
    // `block_byte_sizes` into descending order. I want to avoid additional
    // allocation in this function and so this is not done.
    if bytes_remaining > 0 {
        for block_size in block_byte_sizes {
            if total_chunks >= N {
                break;
            }
            let block_size = block_size.get();
            if block_size <= bytes_remaining {
                chunks[total_chunks] = block_size;
                total_chunks += 1;
                bytes_remaining -= block_size;
            }
        }
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
) -> Result<Vec<Block>, ConstructBlockCacheError>
where
    S: crate::Serialize,
    R: Rng + ?Sized,
{
    let mut block_cache: Vec<Block> = Vec::with_capacity(block_chunks.len());
    for block_size in block_chunks {
        if let Some(block) = construct_block(&mut rng, serializer, *block_size) {
            block_cache.push(block);
        }
    }
    if block_cache.is_empty() {
        error!("Empty block cache, unable to construct blocks!");
        Err(ConstructBlockCacheError::InsufficientBlockSizes)
    } else {
        info!(size = block_cache.len(), "Block cache constructed.");
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
            let block_size = block_chunks.choose(&mut rng).unwrap();
            if let Some(block) = construct_block(&mut rng, serializer, *block_size) {
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
fn construct_block<R, S>(mut rng: &mut R, serializer: &S, chunk_size: u32) -> Option<Block>
where
    S: crate::Serialize,
    R: Rng + ?Sized,
{
    let mut block: Writer<BytesMut> = BytesMut::with_capacity(chunk_size as usize).writer();
    serializer
        .to_bytes(&mut rng, chunk_size as usize, &mut block)
        .unwrap();
    let bytes: Bytes = block.into_inner().freeze();
    if bytes.is_empty() {
        // Blocks may be empty, especially when the amount of bytes
        // requested for the block are relatively low. This is a quirk of
        // our use of randomness. We do not have the ability to tell that
        // library that we would like such and such number of bytes
        // approximately from an instance. This does mean that we sometimes
        // waste computation because the size of the block given cannot be
        // serialized into.
        None
    } else {
        let total_bytes = NonZeroU32::new(bytes.len().try_into().unwrap()).unwrap();
        Some(Block { total_bytes, bytes })
    }
}

#[cfg(kani)]
mod verification {
    use crate::block::chunk_bytes;
    use std::num::NonZeroU32;

    /// Function `chunk_bytes` will always fail with an error if the passed
    /// `block_byte_sizes` is empty.
    #[kani::proof]
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

        let chunks = chunk_bytes(total_bytes, &byte_sizes, &mut block_chunks).unwrap();
        kani::assert(
            chunks > 0,
            "chunk_bytes should never return an empty vec of chunks",
        );
    }

    /// Function `chunk_bytes` does not return a chunk that is not present in
    /// the byte sizes.
    #[kani::proof]
    #[kani::unwind(15)]
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

        let chunks = chunk_bytes(total_bytes, &byte_sizes, &mut block_chunks).unwrap();
        for chunk in &block_chunks[0..chunks] {
            kani::assert(
                byte_sizes.contains(&NonZeroU32::new(*chunk).unwrap()),
                "chunk_bytes should never return a chunk that is not present in the byte sizes",
            );
        }
    }

    /// Function `chunk_bytes` does not populate values above the returned
    /// index, that is, they all remain zero.
    #[kani::proof]
    #[kani::unwind(15)]
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

        let chunks = chunk_bytes(total_bytes, &byte_sizes, &mut block_chunks).unwrap();
        for chunk in &block_chunks[chunks..] {
            kani::assert(
                *chunk == 0,
                "chunk_bytes should never populate values above the returned index",
            );
        }
    }
}
