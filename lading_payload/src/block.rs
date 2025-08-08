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
use tokio::{
    sync::mpsc::{Sender, error::SendError},
    time::Instant,
};
use tracing::{Level, debug, error, info, span, warn};

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

impl Cache {
    /// Construct a `Cache` of fixed size.
    ///
    /// This constructor makes an internal pool of `Block` instances up to
    /// `total_bytes`, each of which are no larger than `maximum_block_bytes`.
    ///
    /// # Errors
    ///
    /// Function will return an error if `maximum_block_bytes` is greater than
    /// `u32::MAX` or if it is larger than `total_bytes`.
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::cast_possible_truncation)]
    pub fn fixed<R>(
        mut rng: &mut R,
        total_bytes: NonZeroU32,
        maximum_block_bytes: u128,
        payload: &crate::Config,
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
            crate::Config::TraceAgent(enc) => {
                let mut ta = match enc {
                    crate::Encoding::Json => crate::TraceAgent::json(&mut rng),
                    crate::Encoding::MsgPack => crate::TraceAgent::msg_pack(&mut rng),
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
            crate::Config::OpentelemetryTraces => {
                let mut pyld = crate::OpentelemetryTraces::new(&mut rng);
                let span = span!(Level::INFO, "fixed", payload = "otel-traces");
                let _guard = span.enter();
                construct_block_cache_inner(rng, &mut pyld, maximum_block_bytes, total_bytes.get())?
            }
            crate::Config::OpentelemetryLogs => {
                let mut pyld = crate::OpentelemetryLogs::new(&mut rng);
                let span = span!(Level::INFO, "fixed", payload = "otel-logs");
                let _guard = span.enter();
                construct_block_cache_inner(rng, &mut pyld, maximum_block_bytes, total_bytes.get())?
            }
            crate::Config::OpentelemetryMetrics(config) => {
                let mut pyld = crate::OpentelemetryMetrics::new(*config, &mut rng)?;
                let span = span!(Level::INFO, "fixed", payload = "otel-metrics");
                let _guard = span.enter();

                construct_block_cache_inner(rng, &mut pyld, maximum_block_bytes, total_bytes.get())?
            }
            crate::Config::NetFlowV5(config) => {
                match config.valid() {
                    Ok(()) => (),
                    Err(e) => {
                        warn!("Invalid NetFlowV5 configuration: {}", e);
                        return Err(Error::InvalidConfig(e));
                    }
                }
                let mut pyld = crate::NetFlowV5::new(*config, &mut rng)?;
                let span = span!(Level::INFO, "fixed", payload = "netflow-v5");
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
            Self::Fixed {
                mut idx, blocks, ..
            } => loop {
                snd.blocking_send(blocks[idx].clone())?;
                idx = (idx + 1) % blocks.len();
            },
        }
    }

    /// Peek at the next `Block` from the `Cache`.
    ///
    /// This is a block function that returns a reference to the next `Block`
    /// instance although the cache is not advanced by this call. Callers must
    /// call [`Self::next_block`] or this cache will not advance.
    #[must_use]
    pub fn peek_next(&self) -> &Block {
        match self {
            Self::Fixed { idx, blocks, .. } => &blocks[*idx],
        }
    }

    /// Return a `Block` from the `Cache`
    ///
    /// This is a blocking function that returns a single `Block` instance as
    /// soon as one is ready, blocking the caller until one is available.
    pub fn next_block(&mut self) -> &Block {
        match self {
            Self::Fixed { idx, blocks, .. } => {
                let block = &blocks[*idx];
                *idx = (*idx + 1) % blocks.len();
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

    info!(
        ?max_block_size,
        ?total_bytes,
        "Constructing requested block cache"
    );
    let mut block_cache: Vec<Block> = Vec::with_capacity(128);
    let mut bytes_remaining = total_bytes;

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
        // Handle edge case where min equals max
        let block_size = if min_block_size == max_block_size {
            min_block_size
        } else if min_block_size > max_block_size {
            error!("Cannot generate blocks: min_block_size ({}) > max_block_size ({})", min_block_size, max_block_size);
            return Err(SpinError::EmptyBlock.into());
        } else {
            // A block_size is in the range [min_block_size, max_block_size)
            rng.random_range(min_block_size..max_block_size)
        };

        match construct_block(&mut rng, serializer, block_size) {
            Ok(block) => {
                success_block_sizes += 1;

                let total_bytes = block.total_bytes.get();
                max_actual_block_size = max_actual_block_size.max(total_bytes);
                min_actual_block_size = min_actual_block_size.min(total_bytes);
                bytes_remaining = bytes_remaining.saturating_sub(total_bytes);
                block_cache.push(block);
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
                println!(
                    //"NetFlow DEBUG Block size {block_size} was rejected, setting new minimum block size."
                );
                let proposed_min = (f64::from(block_size) * 0.25) as u32;
                // Ensure min_block_size always increases by at least some amount to avoid infinite loops
                min_block_size = min_block_size
                    .max(proposed_min)
                    .max(min_block_size + 64)
                    .min(max_block_size);
                //println!("NetFlow DEBUG New minimum block size is {min_block_size}.");
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
    let bytes: Bytes = block.into_inner().freeze();
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
