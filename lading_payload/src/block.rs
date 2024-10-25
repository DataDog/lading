//! Construct byte blocks for use in generators.
//!
//! The method that lading uses to maintain speed over its target is to avoid
//! runtime generation where possible _or_ to generate into a queue and consume
//! from that, decoupling the create/send operations. This module is the
//! mechanism by which 'blocks' -- that is, byte blobs of a predetermined size
//! -- are created.
use std::num::NonZeroU32;

use byte_unit::{Byte, ByteUnit};
use bytes::{buf::Writer, BufMut, Bytes, BytesMut};
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::mpsc::{error::SendError, Sender},
    time::Instant,
};
use tracing::{error, info, span, warn, Level};

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
/// Function will only panic if there is a serious programming mistake.
#[must_use]
pub fn default_maximum_block_size() -> Byte {
    Byte::from_unit(1f64, ByteUnit::MiB).expect("should not fail")
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
        let maximum_block_bytes = if (maximum_block_bytes > u32::MAX.into())
            || (maximum_block_bytes > total_bytes.get().into())
        {
            return Err(Error::MaximumBlock);
        } else {
            maximum_block_bytes as u32
        };

        let blocks = match payload {
            crate::Config::TraceAgent(enc) => {
                let ta = match enc {
                    crate::Encoding::Json => crate::TraceAgent::json(&mut rng),
                    crate::Encoding::MsgPack => crate::TraceAgent::msg_pack(&mut rng),
                };

                let span = span!(Level::INFO, "fixed", payload = "trace-agent");
                let _guard = span.enter();

                construct_block_cache_inner(&mut rng, &ta, maximum_block_bytes, total_bytes.get())?
            }
            crate::Config::Syslog5424 => {
                let span = span!(Level::INFO, "fixed", payload = "syslog5424");
                let _guard = span.enter();

                construct_block_cache_inner(
                    &mut rng,
                    &crate::Syslog5424::default(),
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
                let serializer = crate::DogStatsD::new(*conf, &mut rng)?;

                let span = span!(Level::INFO, "fixed", payload = "dogstatsd");
                let _guard = span.enter();

                construct_block_cache_inner(
                    &mut rng,
                    &serializer,
                    maximum_block_bytes,
                    total_bytes.get(),
                )?
            }
            crate::Config::Fluent => {
                let pyld = crate::Fluent::new(&mut rng);
                let span = span!(Level::INFO, "fixed", payload = "fluent");
                let _guard = span.enter();
                construct_block_cache_inner(
                    &mut rng,
                    &pyld,
                    maximum_block_bytes,
                    total_bytes.get(),
                )?
            }
            crate::Config::SplunkHec { encoding } => {
                let span = span!(Level::INFO, "fixed", payload = "splunkHec");
                let _guard = span.enter();
                construct_block_cache_inner(
                    &mut rng,
                    &crate::SplunkHec::new(*encoding),
                    maximum_block_bytes,
                    total_bytes.get(),
                )?
            }
            crate::Config::ApacheCommon => {
                let pyld = crate::ApacheCommon::new(&mut rng);
                let span = span!(Level::INFO, "fixed", payload = "apache-common");
                let _guard = span.enter();
                construct_block_cache_inner(
                    &mut rng,
                    &pyld,
                    maximum_block_bytes,
                    total_bytes.get(),
                )?
            }
            crate::Config::Ascii => {
                let pyld = crate::Ascii::new(&mut rng);
                let span = span!(Level::INFO, "fixed", payload = "ascii");
                let _guard = span.enter();
                construct_block_cache_inner(
                    &mut rng,
                    &pyld,
                    maximum_block_bytes,
                    total_bytes.get(),
                )?
            }
            crate::Config::DatadogLog => {
                let serializer = crate::DatadogLog::new(&mut rng);
                let span = span!(Level::INFO, "fixed", payload = "datadog-log");
                let _guard = span.enter();
                construct_block_cache_inner(
                    &mut rng,
                    &serializer,
                    maximum_block_bytes,
                    total_bytes.get(),
                )?
            }
            crate::Config::Json => {
                let span = span!(Level::INFO, "fixed", payload = "json");
                let _guard = span.enter();
                construct_block_cache_inner(
                    &mut rng,
                    &crate::Json,
                    maximum_block_bytes,
                    total_bytes.get(),
                )?
            }
            crate::Config::Static { ref static_path } => {
                let span = span!(Level::INFO, "fixed", payload = "static");
                let _guard = span.enter();
                construct_block_cache_inner(
                    &mut rng,
                    &crate::Static::new(static_path)?,
                    maximum_block_bytes,
                    total_bytes.get(),
                )?
            }
            crate::Config::OpentelemetryTraces => {
                let pyld = crate::OpentelemetryTraces::new(&mut rng);
                let span = span!(Level::INFO, "fixed", payload = "otel-traces");
                let _guard = span.enter();
                construct_block_cache_inner(rng, &pyld, maximum_block_bytes, total_bytes.get())?
            }
            crate::Config::OpentelemetryLogs => {
                let pyld = crate::OpentelemetryLogs::new(&mut rng);
                let span = span!(Level::INFO, "fixed", payload = "otel-logs");
                let _guard = span.enter();
                construct_block_cache_inner(rng, &pyld, maximum_block_bytes, total_bytes.get())?
            }
            crate::Config::OpentelemetryMetrics => {
                let pyld = crate::OpentelemetryMetrics::new(&mut rng);
                let span = span!(Level::INFO, "fixed", payload = "otel-metrics");
                let _guard = span.enter();
                construct_block_cache_inner(rng, &pyld, maximum_block_bytes, total_bytes.get())?
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
        }
    }

    /// Return a `Block` from the `Cache`
    ///
    /// This is a blocking function that returns a single `Block` instance as
    /// soon as one is ready, blocking the caller until one is available.
    pub fn next_block(&mut self) -> &Block {
        match self {
            Self::Fixed {
                ref mut idx,
                blocks,
            } => {
                let block = &blocks[*idx];
                *idx = (*idx + 1) % blocks.len();
                block
            }
        }
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
    serializer: &S,
    max_block_size: u32,
    total_bytes: u32,
) -> Result<Vec<Block>, SpinError>
where
    S: crate::Serialize,
    R: Rng + ?Sized,
{
    let mut block_cache: Vec<Block> = Vec::with_capacity(128);
    let mut bytes_remaining = total_bytes;
    let mut min_block_size = 0;
    info!(
        ?max_block_size,
        ?total_bytes,
        "Constructing requested block cache"
    );

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
        // A block_size is always in the range [min_block_size,
        // max_block_size).
        let block_size = rng.gen_range(min_block_size..max_block_size);

        match construct_block(&mut rng, serializer, block_size) {
            Ok(block) => {
                bytes_remaining = bytes_remaining.saturating_sub(block.total_bytes.get());
                block_cache.push(block);
            }
            Err(SpinError::EmptyBlock) => {
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

        let filled_sum_str = byte_unit::Byte::from_bytes(filled_sum.into())
            .get_appropriate_unit(false)
            .to_string();
        let capacity_sum_str = byte_unit::Byte::from_bytes(total_bytes.into())
            .get_appropriate_unit(false)
            .to_string();
        let min_block_str = byte_unit::Byte::from_bytes(min_block_size.into())
            .get_appropriate_unit(false)
            .to_string();
        info!("Filled {filled_sum_str} of requested {capacity_sum_str}. Discovered minimum block size of {min_block_str}");

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
    serializer: &S,
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
        Ok(Block { total_bytes, bytes })
    }
}
