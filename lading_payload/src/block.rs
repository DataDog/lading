//! Construct byte blocks for use in generators.
//!
//! The method that lading uses to maintain speed over its target is to avoid
//! runtime generation where possible _or_ to generate into a queue and consume
//! from that, decoupling the create/send operations. This module is the
//! mechanism by which 'blocks' -- that is, byte blobs of a predetermined size
//! -- are created.
use std::num::NonZeroU32;

use byte_unit::{Byte, Unit};
use bytes::{BufMut, Bytes, BytesMut, buf::Writer};
use rand::{Rng, RngExt};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tracing::{Level, debug, error, info, span, warn};

use crate::templated_json::TemplatedJson;

/// Error for block construction
#[derive(Debug, thiserror::Error)]
pub enum SpinError {
    /// Provided configuration had validation errors
    #[error("Provided configuration was not valid: {0}")]
    InvalidConfig(String),
    /// Static payload creation error
    #[error(transparent)]
    Static(#[from] crate::statik::Error),
    /// `StaticChunks` payload creation error
    #[error(transparent)]
    StaticChunks(#[from] crate::static_chunks::Error),
    /// rng slice is Empty
    #[error("RNG slice is empty")]
    EmptyRng,
    /// Error for crate deserialization
    #[error("Deserialization error: {0}")]
    Deserialize(#[from] crate::Error),
    /// Error for constructing the block cache
    #[error(transparent)]
    ConstructBlockCache(#[from] ConstructBlockCacheError),
    /// Serializer returned and empty block
    #[error("Serializer returned an empty block")]
    EmptyBlock,
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
    /// `StaticChunks` payload creation error
    #[error(transparent)]
    StaticChunks(#[from] crate::static_chunks::Error),
    /// Static timestamp-grouped payload creation error
    #[error(transparent)]
    StaticTimestamped(#[from] crate::static_timestamped::Error),
    /// Error for crate deserialization
    #[error("Deserialization error: {0}")]
    Deserialize(#[from] crate::Error),
    /// User provided maximum block size is too large.
    #[error("User provided maximum block size is too large.")]
    MaximumBlock,
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
    /// Optional metadata for the block
    pub metadata: BlockMetadata,
}

/// Metadata associated with a Block
#[derive(Debug, Clone, Default, Copy)]
pub struct BlockMetadata {
    /// Number of data points in this block
    pub data_points: Option<u64>,
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
            metadata: BlockMetadata::default(),
        })
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone, Copy)]
#[serde(deny_unknown_fields)]
/// The method for which caching will be configure
pub enum CacheMethod {
    /// Create a single fixed size block cache and rotate through it
    Fixed,
}

/// The default cache method.
#[must_use]
pub fn default_cache_method() -> CacheMethod {
    CacheMethod::Fixed
}

/// The default block maximum size.
///
/// # Panics
///
/// This function will panic if the byte unit conversion fails, which should never happen
/// with the hardcoded value of 1 MiB.
#[must_use]
pub fn default_maximum_block_size() -> Byte {
    Byte::from_u64_with_unit(1, Unit::MiB).expect("catastrophic programming bug")
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
        /// The amount of data stored in one cycle, or all blocks
        total_cycle_size: u64,
    },
}

/// An opaque handle for iterating through blocks in a Cache.
///
/// Each independent consumer should create its own Handle by calling
/// `Cache::handle()`. Handles maintain their own position in the cache
/// and advance independently.
#[derive(Debug)]
#[allow(missing_copy_implementations)] // intentionally not Copy to force callers to call `handle`.
pub struct Handle {
    idx: usize,
}

impl Cache {
    /// Construct a `Cache` of fixed size.
    ///
    /// This constructor makes an internal pool of `Block` instances up to
    /// `total_bytes`, each of which are no larger than
    /// `maximum_block_bytes`. The `payload` may or may not have internal
    /// overhead, capped at `payload_overhead_allowance_bytes`.
    ///
    /// # Errors
    ///
    /// Function will return an error if `maximum_block_bytes` is greater than
    /// `u32::MAX` or if it is larger than `total_bytes`.
    #[expect(clippy::too_many_lines)]
    #[expect(clippy::cast_possible_truncation)]
    pub fn fixed_with_max_overhead<R>(
        mut rng: &mut R,
        total_bytes: NonZeroU32,
        maximum_block_bytes: u128,
        payload: &crate::Config,
        payload_overhead_allowance_bytes: usize,
    ) -> Result<Self, Error>
    where
        R: Rng + ?Sized,
    {
        let maximum_block_bytes = if (maximum_block_bytes > u128::from(u32::MAX))
            || (maximum_block_bytes > u128::from(total_bytes.get()))
        {
            return Err(Error::MaximumBlock);
        } else {
            maximum_block_bytes as u32
        };

        let blocks = match payload {
            crate::Config::TemplatedJson { template_path } => {
                let mut serializer = TemplatedJson::from_path(template_path)?;
                let span = span!(Level::INFO, "fixed", payload = "templated-json");
                let _guard = span.enter();
                construct_block_cache_inner(
                    &mut rng,
                    &mut serializer,
                    maximum_block_bytes,
                    total_bytes.get(),
                )?
            }
            crate::Config::TraceAgent(config) => {
                use crate::trace_agent::{self, v04};

                let mut ta = match config {
                    trace_agent::Config::V04(v04_config) => {
                        v04::V04::with_config(*v04_config, &mut rng)?
                    }
                };

                let span = span!(Level::INFO, "fixed", payload = "trace-agent");
                let _guard = span.enter();

                construct_block_cache_inner(
                    &mut rng,
                    &mut ta,
                    maximum_block_bytes,
                    total_bytes.get(),
                )?
            }
            crate::Config::Syslog5424 => {
                let span = span!(Level::INFO, "fixed", payload = "syslog5424");
                let _guard = span.enter();

                let mut syslog = crate::Syslog5424::default();
                construct_block_cache_inner(
                    &mut rng,
                    &mut syslog,
                    maximum_block_bytes,
                    total_bytes.get(),
                )?
            }
            crate::Config::DogStatsD(conf) => {
                match conf.valid() {
                    Ok(()) => (),
                    Err(e) => {
                        warn!("Invalid DogStatsD configuration: {}", e);
                        return Err(Error::InvalidConfig(e));
                    }
                }
                let mut serializer = crate::DogStatsD::new(conf, &mut rng)?;

                let span = span!(Level::INFO, "fixed", payload = "dogstatsd");
                let _guard = span.enter();

                construct_block_cache_inner(
                    &mut rng,
                    &mut serializer,
                    maximum_block_bytes,
                    total_bytes.get(),
                )?
            }
            crate::Config::Fluent => {
                let mut pyld = crate::Fluent::new(&mut rng);
                let span = span!(Level::INFO, "fixed", payload = "fluent");
                let _guard = span.enter();
                construct_block_cache_inner(
                    &mut rng,
                    &mut pyld,
                    maximum_block_bytes,
                    total_bytes.get(),
                )?
            }
            crate::Config::SplunkHec { encoding } => {
                let span = span!(Level::INFO, "fixed", payload = "splunkHec");
                let _guard = span.enter();
                let mut splunk_hec = crate::SplunkHec::new(*encoding);
                construct_block_cache_inner(
                    &mut rng,
                    &mut splunk_hec,
                    maximum_block_bytes,
                    total_bytes.get(),
                )?
            }
            crate::Config::ApacheCommon => {
                let mut pyld = crate::ApacheCommon::new(&mut rng);
                let span = span!(Level::INFO, "fixed", payload = "apache-common");
                let _guard = span.enter();
                construct_block_cache_inner(
                    &mut rng,
                    &mut pyld,
                    maximum_block_bytes,
                    total_bytes.get(),
                )?
            }
            crate::Config::Ascii => {
                let mut pyld = crate::Ascii::new(&mut rng);
                let span = span!(Level::INFO, "fixed", payload = "ascii");
                let _guard = span.enter();
                construct_block_cache_inner(
                    &mut rng,
                    &mut pyld,
                    maximum_block_bytes,
                    total_bytes.get(),
                )?
            }
            crate::Config::DatadogLog => {
                let mut serializer = crate::DatadogLog::new(&mut rng);
                let span = span!(Level::INFO, "fixed", payload = "datadog-log");
                let _guard = span.enter();
                construct_block_cache_inner(
                    &mut rng,
                    &mut serializer,
                    maximum_block_bytes,
                    total_bytes.get(),
                )?
            }
            crate::Config::Json => {
                let span = span!(Level::INFO, "fixed", payload = "json");
                let _guard = span.enter();
                let mut json = crate::Json;
                construct_block_cache_inner(
                    &mut rng,
                    &mut json,
                    maximum_block_bytes,
                    total_bytes.get(),
                )?
            }
            crate::Config::Static { static_path } => {
                let span = span!(Level::INFO, "fixed", payload = "static");
                let _guard = span.enter();
                let mut static_serializer = crate::Static::new(static_path)?;
                construct_block_cache_inner(
                    &mut rng,
                    &mut static_serializer,
                    maximum_block_bytes,
                    total_bytes.get(),
                )?
            }
            crate::Config::StaticChunks { static_path } => {
                let span = span!(Level::INFO, "fixed", payload = "static-chunks");
                let _guard = span.enter();
                let mut serializer = crate::StaticChunks::new(static_path)?;
                construct_block_cache_inner(
                    &mut rng,
                    &mut serializer,
                    maximum_block_bytes,
                    total_bytes.get(),
                )?
            }
            crate::Config::StaticTimestamped {
                static_path,
                timestamp_format,
                emit_placeholder,
                start_line_index,
            } => {
                let span = span!(Level::INFO, "fixed", payload = "static-timestamped");
                let _guard = span.enter();
                let mut serializer = crate::StaticTimestamped::new(
                    static_path,
                    timestamp_format,
                    *emit_placeholder,
                    *start_line_index,
                )?;
                construct_block_cache_inner(
                    &mut rng,
                    &mut serializer,
                    maximum_block_bytes,
                    total_bytes.get(),
                )?
            }
            crate::Config::OpentelemetryTraces(config) => {
                let mut pyld = crate::OpentelemetryTraces::with_config(config, &mut rng)?;
                let span = span!(Level::INFO, "fixed", payload = "otel-traces");
                let _guard = span.enter();
                construct_block_cache_inner(rng, &mut pyld, maximum_block_bytes, total_bytes.get())?
            }
            crate::Config::OpentelemetryLogs(config) => {
                match config.valid() {
                    Ok(()) => (),
                    Err(e) => {
                        warn!("Invalid OpentelemetryLogs configuration: {e}");
                        return Err(Error::InvalidConfig(e));
                    }
                }
                let mut pyld = crate::OpentelemetryLogs::new(
                    *config,
                    payload_overhead_allowance_bytes,
                    &mut rng,
                )?;
                let span = span!(Level::INFO, "fixed", payload = "otel-logs");
                let _guard = span.enter();
                construct_block_cache_inner(rng, &mut pyld, maximum_block_bytes, total_bytes.get())?
            }
            crate::Config::OpentelemetryMetrics(config) => {
                let mut pyld = crate::OpentelemetryMetrics::new(
                    *config,
                    payload_overhead_allowance_bytes,
                    &mut rng,
                )?;
                let span = span!(Level::INFO, "fixed", payload = "otel-metrics");
                let _guard = span.enter();

                construct_block_cache_inner(rng, &mut pyld, maximum_block_bytes, total_bytes.get())?
            }
        };

        let total_cycle_size = blocks
            .iter()
            .map(|block| u64::from(block.total_bytes.get()))
            .sum();

        Ok(Self::Fixed {
            idx: 0,
            blocks,
            total_cycle_size,
        })
    }

    /// Create a new handle for iterating through blocks.
    #[must_use]
    #[inline]
    pub fn handle(&self) -> Handle {
        Handle { idx: 0 }
    }

    /// Number of blocks in the cache.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Fixed { blocks, .. } => blocks.len(),
        }
    }

    /// Whether the cache is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the total size of the cache in bytes.
    #[must_use]
    #[inline]
    pub fn total_size(&self) -> u64 {
        match self {
            Self::Fixed {
                total_cycle_size, ..
            } => *total_cycle_size,
        }
    }

    /// Get the total bytes of the next block without advancing.
    #[must_use]
    #[inline]
    pub fn peek_next_size(&self, handle: &Handle) -> NonZeroU32 {
        match self {
            Self::Fixed { blocks, .. } => blocks[handle.idx].total_bytes,
        }
    }

    /// Get metadata of the next block without advancing.
    #[must_use]
    #[inline]
    pub fn peek_next_metadata(&self, handle: &Handle) -> BlockMetadata {
        match self {
            Self::Fixed { blocks, .. } => blocks[handle.idx].metadata,
        }
    }

    /// Advance the handle and return a reference to the current block.
    ///
    /// This advances the handle to the next block in the cache and returns a
    /// reference to the block corresponding to `Handle` internal position.
    #[inline]
    pub fn advance<'a>(&'a self, handle: &mut Handle) -> &'a Block {
        match self {
            Self::Fixed { blocks, .. } => {
                let block = &blocks[handle.idx];
                handle.idx = (handle.idx + 1) % blocks.len();
                block
            }
        }
    }

    /// Read data starting from a given offset and up to the specified size.
    ///
    /// # Panics
    ///
    /// Function will panic if reads are larger than machine word bytes wide.
    pub fn read_at(&self, offset: u64, size: usize) -> Bytes {
        let mut data = BytesMut::with_capacity(size);

        let (blocks, total_cycle_size) = match self {
            Cache::Fixed {
                blocks,
                total_cycle_size,
                ..
            } => (
                blocks,
                usize::try_from(*total_cycle_size)
                    .expect("cycle size larger than machine word bytes"),
            ),
        };

        let mut remaining = size;
        let mut current_offset =
            usize::try_from(offset).expect("offset larger than machine word bytes");

        while remaining > 0 {
            // The plan is this. We treat the blocks as one infinite cycle. We
            // map our offset into the domain of the blocks, then seek forward
            // until we find the block we need to start reading from. Then we
            // read into `data`.

            let offset_within_cycle = current_offset % total_cycle_size;
            let mut block_start = 0;
            for block in blocks {
                let block_size = block.total_bytes.get() as usize;
                if offset_within_cycle < block_start + block_size {
                    // Offset is within this block. Begin reading into `data`.
                    let block_offset = offset_within_cycle - block_start;
                    let bytes_in_block = (block_size - block_offset).min(remaining);

                    data.extend_from_slice(
                        &block.bytes[block_offset..block_offset + bytes_in_block],
                    );

                    remaining -= bytes_in_block;
                    current_offset += bytes_in_block;
                    break;
                }
                block_start += block_size;
            }

            // If we couldn't find a block this suggests something seriously
            // wacky has happened.
            if remaining > 0 && block_start >= total_cycle_size {
                error!("Offset exceeds total cycle size");
                break;
            }
        }

        data.freeze()
    }
}

/// Construct a new block cache of form defined by `serializer`.
///
/// A "block cache" is a pre-made vec of serialized arbitrary instances of the
/// data implied by `serializer`. Considering that it's not cheap, necessarily,
/// to construct and serialize arbitrary data on the fly we want to do it ahead
/// of time. We vary the size of blocks -- via `block_chunks` -- to allow the
/// user to express a range of block sizes they wish to see.
///
/// This function works by randomly probing the block size search space. This
/// has the benefit of making the payload generators conceptually simple with
/// the downside of wasting -- potentially -- `crate::Serializer::to_bytes`
/// calls when the passed block size cannot be satisfied.
///
/// # Panics
///
/// Function will panic if the `serializer` signals an error. In the future we
/// would like to propagate this error to the caller.
#[inline]
#[tracing::instrument(skip_all)]
#[expect(clippy::cast_possible_truncation)]
#[expect(clippy::cast_sign_loss)]
fn construct_block_cache_inner<R, S>(
    mut rng: &mut R,
    serializer: &mut S,
    max_block_size: u32,
    total_bytes: u32,
) -> Result<Vec<Block>, SpinError>
where
    S: crate::Serialize,
    R: Rng + ?Sized,
{
    let mut min_block_size = 0;
    let mut min_actual_block_size = u32::MAX;
    let mut max_actual_block_size = 0;
    let mut rejected_block_sizes = 0;
    let mut success_block_sizes = 0;

    info!(
        ?max_block_size,
        ?total_bytes,
        "Constructing requested block cache"
    );
    let mut block_cache: Vec<Block> = Vec::with_capacity(128);
    let mut bytes_remaining = total_bytes;

    // A serializer returns `EmptyBlock` when it cannot fit even one item into
    // the requested chunk. If `max_block_size` is below the payload's minimum
    // serializable size, *every* attempt fails, `min_block_size` decays to
    // zero, the `bytes_remaining < min_block_size` break never fires, and this
    // loop spins forever building nothing. This is a fuzz-found hang, for
    // example a trace-agent v04 config whose single trace exceeds
    // `max_block_size`. The serializer draws a random item each attempt, so a
    // single rejection does not prove the config impossible. After a run of
    // rejected random picks, force attempts at `max_block_size`, the largest
    // permitted block and likeliest to fit. Give up only once a long run of
    // forced attempts all reject too, meaning no item can ever fit and
    // construction genuinely cannot proceed. A tight but constructable config
    // keeps succeeding within the budget and never trips this. Only an
    // impossible config exhausts it.
    const REJECTS_BEFORE_MAX_PROBE: u32 = 16;
    const MAX_PROBE_BUDGET: u32 = 1024;
    let mut consecutive_rejections: u32 = 0;

    let start = Instant::now();
    let mut next_minute = 1;

    // Build out the blocks.
    //
    // Our strategy here is to keep track of the minimal viable size of a block
    // -- `min_block_size` -- as the "floor" for block sizes. Because the
    // serialization format varies we can't know what the floor actually is
    // until runtime. We take the user-provided blocks and the total byte
    // objective and iterate over these, choosing random block sizes between the
    // discovered floor and the maximum user-provided block size.
    while bytes_remaining > 0 {
        // block_size is random in [min_block_size, max_block_size). After a
        // long run of rejections, force `max_block_size`, the largest
        // permitted block and likeliest to serialize.
        let probing_max = consecutive_rejections >= REJECTS_BEFORE_MAX_PROBE;
        let block_size = if probing_max {
            max_block_size
        } else {
            rng.random_range(min_block_size..max_block_size)
        };

        match construct_block(&mut rng, serializer, block_size) {
            Ok(block) => {
                success_block_sizes += 1;
                consecutive_rejections = 0;

                let total_bytes = block.total_bytes.get();
                max_actual_block_size = max_actual_block_size.max(total_bytes);
                min_actual_block_size = min_actual_block_size.min(total_bytes);
                bytes_remaining = bytes_remaining.saturating_sub(total_bytes);
                block_cache.push(block);
            }
            Err(SpinError::EmptyBlock) => {
                debug!(?block_size, "rejected block");
                rejected_block_sizes += 1;
                consecutive_rejections += 1;
                if consecutive_rejections >= REJECTS_BEFORE_MAX_PROBE + MAX_PROBE_BUDGET {
                    // A long run of forced `max_block_size` attempts all
                    // rejected. No item fits even the largest permitted block,
                    // so max_block_size is below the payload's minimum
                    // serializable size and construction cannot proceed.
                    error!(
                        ?max_block_size,
                        rejected_block_sizes,
                        "Unable to construct any block; max_block_size is below the payload's minimum serializable size"
                    );
                    return Err(SpinError::ConstructBlockCache(
                        ConstructBlockCacheError::InsufficientBlockSizes,
                    ));
                }
                if !probing_max {
                    // It might be that `block_size` could not be constructed
                    // because the size is too small or we just caught a bad
                    // break. We do know that there's some true minimum viable
                    // size out there for each serialization format and user
                    // configuration, but we can only guess at it. To avoid
                    // racing _too_ far off the minimum viable size we scale the
                    // block size by -75% -- an arbitrary figure -- and set that
                    // as the new minimum block size.
                    min_block_size = (f64::from(block_size) * 0.25) as u32;
                }
            }
            Err(e) => {
                error!("Unexpected error during block construction: {e}");
                return Err(e);
            }
        }

        let elapsed_secs = start.elapsed().as_secs();
        let elapsed_minutes = elapsed_secs / 60;
        if elapsed_minutes >= next_minute {
            info!(
                "Progress: {} bytes remaining, elapsed time: {:?}",
                bytes_remaining,
                start.elapsed()
            );
            next_minute += 1;
        }

        if bytes_remaining < min_block_size {
            break;
        }
    }

    // Instrument the results of the block construction.
    if block_cache.is_empty() {
        error!("Empty block cache, unable to construct blocks!");
        Err(SpinError::ConstructBlockCache(
            ConstructBlockCacheError::InsufficientBlockSizes,
        ))
    } else {
        let filled_sum = block_cache.iter().map(|b| b.total_bytes.get()).sum::<u32>();

        let filled_sum_str = Byte::from_u64(filled_sum.into())
            .get_appropriate_unit(byte_unit::UnitType::Binary)
            .to_string();
        let capacity_sum_str = Byte::from_u64(total_bytes.into())
            .get_appropriate_unit(byte_unit::UnitType::Binary)
            .to_string();
        let max_actual_block_str = Byte::from_u64(max_actual_block_size.into())
            .get_appropriate_unit(byte_unit::UnitType::Binary)
            .to_string();
        let min_actual_block_str = Byte::from_u64(min_actual_block_size.into())
            .get_appropriate_unit(byte_unit::UnitType::Binary)
            .to_string();

        let total_data_points: u64 = block_cache
            .iter()
            .filter_map(|b| b.metadata.data_points)
            .sum();

        if total_data_points > 0 {
            info!(
                "Filled {filled_sum_str} of requested {capacity_sum_str}. Discovered minimum block size of {min_actual_block_str}, maximum: {max_actual_block_str}. Total success blocks: {success_block_sizes}. Total rejected blocks: {rejected_block_sizes}. Total data points: {total_data_points}."
            );
        } else {
            info!(
                "Filled {filled_sum_str} of requested {capacity_sum_str}. Discovered minimum block size of {min_actual_block_str}, maximum: {max_actual_block_str}. Total success blocks: {success_block_sizes}. Total rejected blocks: {rejected_block_sizes}."
            );
        }

        Ok(block_cache)
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
    serializer: &mut S,
    chunk_size: u32,
) -> Result<Block, SpinError>
where
    S: crate::Serialize,
    R: Rng + ?Sized,
{
    let mut block: Writer<BytesMut> = BytesMut::with_capacity(chunk_size as usize).writer();
    serializer.to_bytes(&mut rng, chunk_size as usize, &mut block)?;
    let inner = block.into_inner();
    // When the actual block data usage is under half of its allocated capacity (chunk_size),
    // shrink its buffer to the actual size to avoid holding onto excess capacity.
    // This ensures that generators with lots of small blocks respect the total cache size, and
    // no block cache will hold more than 2x the total cache size in allocated buffers.
    let bytes: Bytes = if inner.len() < inner.capacity() / 2 {
        Bytes::copy_from_slice(&inner)
    } else {
        inner.freeze()
    };
    if bytes.is_empty() {
        // Blocks should not be empty and if they are empty this is an
        // error. Caller may choose to handle this however they wish, often it
        // means that the specific request could not be satisfied for a given
        // serializer.
        Err(SpinError::EmptyBlock)
    } else {
        let total_bytes = NonZeroU32::new(
            bytes
                .len()
                .try_into()
                .expect("failed to get length of bytes"),
        )
        .ok_or(SpinError::Zero)?;

        let mut metadata = BlockMetadata::default();
        if let Some(data_points) = serializer.data_points_generated() {
            metadata.data_points = Some(data_points);
        }

        Ok(Block {
            total_bytes,
            bytes,
            metadata,
        })
    }
}

#[cfg(test)]
mod hang_regression {
    use super::Cache;
    use crate::trace_agent;
    use rand::{SeedableRng, rngs::SmallRng};
    use std::num::NonZeroU32;

    /// Regression: a tight but constructable config, `max_block_size` just above
    /// the payload's minimum serializable size, must build a cache for every
    /// seed. The first hang fix capped construction at a fixed run of consecutive
    /// rejections, so an unlucky opening run of small random picks returned
    /// `InsufficientBlockSizes` on a config that can build, empirically ~17% of
    /// these seeds. Failing fast must key off "even the maximum block cannot be
    /// built", not a raw rejection count.
    #[test]
    fn tight_but_constructable_config_never_fails() {
        let payload = crate::Config::TraceAgent(trace_agent::Config::V04(
            trace_agent::v04::Config::default(),
        ));
        // A couple of blocks is enough. The spurious failure struck on the
        // opening run of rejections while the cache was still empty.
        let total_bytes = NonZeroU32::new(30_000).expect("nonzero");
        // 12288 is the smallest max that can serialize one v04 trace, so most
        // random picks below it are rejected and the opening reject run is long.
        let max_block_size = 12_288;
        for seed in 0..64u64 {
            let mut rng = SmallRng::seed_from_u64(seed);
            let result = Cache::fixed_with_max_overhead(
                &mut rng,
                total_bytes,
                max_block_size,
                &payload,
                total_bytes.get() as usize,
            );
            assert!(
                result.is_ok(),
                "seed {seed}: a constructable config must not fail construction"
            );
        }
    }

    /// Regression for a fuzz-found hang. When `max_block_size` is below the
    /// payload's minimum serializable size, no trace fits a block. Previously
    /// `to_bytes` emitted a one-byte empty msgpack array that `construct_block`
    /// accepted, so cache construction "progressed" one byte at a time,
    /// regenerating an expensive trace per byte, effectively forever for a large
    /// `total_bytes`. Construction must instead fail fast.
    #[test]
    fn tiny_max_block_size_fails_fast_instead_of_hanging() {
        let mut rng = SmallRng::seed_from_u64(0);
        let payload = crate::Config::TraceAgent(trace_agent::Config::V04(
            trace_agent::v04::Config::default(),
        ));
        let total_bytes = NonZeroU32::new(1_000_000).expect("nonzero");
        // Eight bytes cannot hold even one serialized span.
        let result = Cache::fixed_with_max_overhead(
            &mut rng,
            total_bytes,
            8,
            &payload,
            total_bytes.get() as usize,
        );
        assert!(
            result.is_err(),
            "construction must fail fast when no trace fits a block, not hang"
        );
    }
}
