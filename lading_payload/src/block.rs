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
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tracing::{Level, debug, error, info, span, warn};

/// Read current process RSS in bytes from /proc/self/statm (Linux only).
/// Returns 0 on non-Linux or if reading fails.
#[cfg(target_os = "linux")]
fn get_rss_bytes() -> u64 {
    use std::fs;
    let page_size = 4096u64; // Typical page size
    fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map(|pages| pages * page_size)
        .unwrap_or(0)
}

#[cfg(not(target_os = "linux"))]
fn get_rss_bytes() -> u64 {
    0
}

fn log_rss(label: &str) {
    let rss = get_rss_bytes();
    if rss > 0 {
        let rss_str = Byte::from_u64(rss)
            .get_appropriate_unit(byte_unit::UnitType::Binary)
            .to_string();
        info!("[MEMORY] {}: RSS = {}", label, rss_str);
    }
}

/// Error for block construction
#[derive(Debug, thiserror::Error)]
pub enum SpinError {
    /// Provided configuration had validation errors
    #[error("Provided configuration was not valid: {0}")]
    InvalidConfig(String),
    /// Static payload creation error
    #[error(transparent)]
    Static(#[from] crate::statik::Error),
    /// Static line-rate payload creation error
    #[error(transparent)]
    StaticLinesPerSecond(#[from] crate::statik_line_rate::Error),
    /// Static second-grouped payload creation error
    #[error(transparent)]
    StaticSecond(#[from] crate::statik_second::Error),
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
    /// Static line-rate payload creation error
    #[error(transparent)]
    StaticLinesPerSecond(#[from] crate::statik_line_rate::Error),
    /// Static second-grouped payload creation error
    #[error(transparent)]
    StaticSecond(#[from] crate::statik_second::Error),
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

impl Block {
    /// Estimate the actual heap memory used by this block.
    /// This includes the Block struct, Bytes overhead, and payload.
    pub fn estimated_heap_bytes(&self) -> usize {
        // Block struct size (stack/inline)
        let block_struct_size = std::mem::size_of::<Block>();
        // Bytes payload (actual data)
        let payload_size = self.bytes.len();
        // Bytes has internal Arc overhead (~32 bytes on 64-bit)
        let bytes_overhead = 32;
        // jemalloc has ~16 bytes overhead per allocation
        let alloc_overhead = 16;
        block_struct_size + payload_size + bytes_overhead + alloc_overhead
    }
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
#[derive(Debug, Clone)]
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
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::cast_possible_truncation)]
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
        let payload_name = format!("{:?}", payload).chars().take(50).collect::<String>();
        log_rss(&format!("Before cache construction for {}", payload_name));

        let maximum_block_bytes = if (maximum_block_bytes > u128::from(u32::MAX))
            || (maximum_block_bytes > u128::from(total_bytes.get()))
        {
            return Err(Error::MaximumBlock);
        } else {
            maximum_block_bytes as u32
        };

        let blocks = match payload {
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
                let mut serializer = crate::DogStatsD::new(*conf, &mut rng)?;

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
            crate::Config::StaticLinesPerSecond {
                static_path,
                lines_per_second,
            } => {
                let span = span!(Level::INFO, "fixed", payload = "static-lines-per-second");
                let _guard = span.enter();
                let mut serializer =
                    crate::StaticLinesPerSecond::new(static_path, *lines_per_second)?;
                construct_block_cache_inner(
                    &mut rng,
                    &mut serializer,
                    maximum_block_bytes,
                    total_bytes.get(),
                )?
            }
            crate::Config::StaticSecond {
                static_path,
                timestamp_format,
                emit_placeholder,
                start_line_index,
            } => {
                let span = span!(Level::INFO, "fixed", payload = "static-second");
                let _guard = span.enter();
                let mut serializer = crate::StaticSecond::new(
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
                let mut pyld = crate::OpentelemetryTraces::with_config(*config, &mut rng)?;
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

        // Log cache stats and memory after construction
        let block_count = blocks.len();
        let cache_size_str = Byte::from_u64(total_cycle_size)
            .get_appropriate_unit(byte_unit::UnitType::Binary)
            .to_string();
        
        // Estimate actual heap usage
        let estimated_heap: usize = blocks.iter().map(|b| b.estimated_heap_bytes()).sum();
        let vec_capacity_bytes = blocks.capacity() * std::mem::size_of::<Block>();
        let total_estimated = estimated_heap + vec_capacity_bytes;
        let estimated_str = Byte::from_u64(total_estimated as u64)
            .get_appropriate_unit(byte_unit::UnitType::Binary)
            .to_string();
        
        info!(
            "[CACHE] Constructed {} blocks, payload size: {}, estimated heap: {}, vec capacity: {} blocks ({} bytes)",
            block_count,
            cache_size_str,
            estimated_str,
            blocks.capacity(),
            vec_capacity_bytes
        );
        log_rss(&format!("After cache construction ({} blocks)", block_count));

        Ok(Self::Fixed {
            idx: 0,
            blocks,
            total_cycle_size,
        })
    }

    /// Create a new handle for iterating through blocks.
    #[must_use]
    pub fn handle(&self) -> Handle {
        Handle { idx: 0 }
    }

    /// Get the total size of the cache in bytes.
    #[must_use]
    pub fn total_size(&self) -> u64 {
        match self {
            Self::Fixed {
                total_cycle_size, ..
            } => *total_cycle_size,
        }
    }

    /// Get the total bytes of the next block without advancing.
    #[must_use]
    pub fn peek_next_size(&self, handle: &Handle) -> NonZeroU32 {
        match self {
            Self::Fixed { blocks, .. } => blocks[handle.idx].total_bytes,
        }
    }

    /// Get the number of blocks in the cache.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Fixed { blocks, .. } => blocks.len(),
        }
    }

    /// Returns true if the cache has no blocks.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get metadata of the next block without advancing.
    #[must_use]
    pub fn peek_next_metadata(&self, handle: &Handle) -> BlockMetadata {
        match self {
            Self::Fixed { blocks, .. } => blocks[handle.idx].metadata,
        }
    }

    /// Advance the handle and return a reference to the current block.
    ///
    /// This advances the handle to the next block in the cache and returns a
    /// reference to the block corresponding to `Handle` internal position.
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
#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::cast_sign_loss)]
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

    // Estimate block count: use conservative 4KB average for small-block payloads
    // (like static_second), but cap at 1/4 max_block_size for large-block payloads.
    // This reduces Vec reallocations during cache construction.
    let conservative_avg = 4096u32; // 4 KB - typical for static_second
    let optimistic_avg = max_block_size / 4;
    let avg_block_estimate = conservative_avg.min(optimistic_avg).max(1);
    let estimated_blocks = (total_bytes / avg_block_estimate).max(128) as usize;
    info!(
        ?max_block_size,
        ?total_bytes,
        ?estimated_blocks,
        ?avg_block_estimate,
        "Constructing requested block cache"
    );
    let mut block_cache: Vec<Block> = Vec::with_capacity(estimated_blocks);
    let mut bytes_remaining = total_bytes;

    let start = Instant::now();
    let mut next_minute = 1;
    let mut next_rss_log_blocks = 1000; // Log RSS every 1000 blocks

    log_rss("Start of cache construction loop");

    // Build out the blocks.
    //
    // Our strategy here is to keep track of the minimal viable size of a block
    // -- `min_block_size` -- as the "floor" for block sizes. Because the
    // serialization format varies we can't know what the floor actually is
    // until runtime. We take the user-provided blocks and the total byte
    // objective and iterate over these, choosing random block sizes between the
    // discovered floor and the maximum user-provided block size.
    while bytes_remaining > 0 {
        // A block_size is always in the range [min_block_size,
        // max_block_size).
        let block_size = rng.random_range(min_block_size..max_block_size);

        match construct_block(&mut rng, serializer, block_size) {
            Ok(block) => {
                success_block_sizes += 1;

                let total_bytes = block.total_bytes.get();
                max_actual_block_size = max_actual_block_size.max(total_bytes);
                min_actual_block_size = min_actual_block_size.min(total_bytes);
                bytes_remaining = bytes_remaining.saturating_sub(total_bytes);
                block_cache.push(block);

                // Periodic RSS logging during construction
                if success_block_sizes == next_rss_log_blocks {
                    log_rss(&format!(
                        "After {} blocks, {} remaining, vec cap={}",
                        success_block_sizes, bytes_remaining, block_cache.capacity()
                    ));
                    next_rss_log_blocks *= 2; // Log at 1000, 2000, 4000, 8000...
                }
            }
            Err(SpinError::EmptyBlock) => {
                debug!(?block_size, "rejected block");
                rejected_block_sizes += 1;
                // It might be that `block_size` could not be constructed
                // because the size is too small or we just caught a bad
                // break. We do know that there's some true minimum viable size
                // out there for each serialization format and user
                // configuration, but we can only guess at it. To avoid racing
                // _too_ far off the minimum viable size we scale the block size
                // by -75% -- an arbitrary figure -- and set that as the new
                // minimum block size.
                min_block_size = (f64::from(block_size) * 0.25) as u32;
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

        log_rss(&format!(
            "End of cache loop: {} blocks, filled={}, vec_cap={}",
            block_cache.len(),
            filled_sum_str,
            block_cache.capacity()
        ));

        // Release excess Vec capacity to reduce memory footprint
        let old_cap = block_cache.capacity();
        block_cache.shrink_to_fit();
        if block_cache.capacity() < old_cap {
            info!(
                "Shrunk block_cache Vec from {} to {} capacity (saved {} bytes)",
                old_cap,
                block_cache.capacity(),
                (old_cap - block_cache.capacity()) * std::mem::size_of::<Block>()
            );
            log_rss("After shrink_to_fit");
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
    // Shrink allocation to actual size to avoid holding onto excess capacity.
    // This is critical for small-block payloads like static_second where
    // requested chunk_size (up to 5MB) >> actual data (often <10KB).
    let bytes: Bytes = if inner.len() < inner.capacity() / 2 {
        // Copy to right-sized allocation when we'd waste >50% capacity
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
