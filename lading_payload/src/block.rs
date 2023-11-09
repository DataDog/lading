//! Construct byte blocks for use in generators.
//!
//! The method that lading uses to maintain speed over its target is to avoid
//! runtime generation where possible _or_ to generate into a queue and consume
//! from that, decoupling the create/send operations. This module is the
//! mechanism by which 'blocks' -- that is, byte blobs of a predetermined size
//! -- are created.
use std::{
    collections::VecDeque,
    num::{NonZeroU32, NonZeroUsize},
};

use bytes::{buf::Writer, BufMut, Bytes, BytesMut};
use rand::{prelude::SliceRandom, rngs::StdRng, Rng, SeedableRng};
use serde::Deserialize;
use tokio::sync::mpsc::{self, error::SendError, Sender};

/// Error for `Cache::spin`
#[derive(Debug, thiserror::Error)]
pub enum SpinError {
    /// See [`SendError`]
    #[error(transparent)]
    Send(#[from] SendError<Block>),
}

/// Error for [`Cache`]
#[derive(Debug, thiserror::Error, Clone, Copy)]
pub enum Error {
    /// See [`ChunkError`]
    #[error("Chunk error: {0}")]
    Chunk(#[from] ChunkError),
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

#[derive(Debug, Deserialize, PartialEq, Clone, Copy)]
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
        total_bytes: usize,
        /// The sizes of the blocks that will be generated.
        block_chunks: Vec<usize>,
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
        total_bytes: NonZeroUsize,
        block_byte_sizes: &[NonZeroUsize],
        payload: crate::Config,
    ) -> Result<Self, Error> {
        let mut rng = StdRng::from_seed(seed);

        let block_chunks = chunk_bytes(&mut rng, total_bytes, block_byte_sizes)?;
        Ok(Self::Stream {
            seed,
            total_bytes: total_bytes.get(),
            block_chunks,
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
    pub fn fixed<R>(
        mut rng: &mut R,
        total_bytes: NonZeroUsize,
        block_byte_sizes: &[NonZeroUsize],
        payload: &crate::Config,
    ) -> Result<Self, Error>
    where
        R: Rng + ?Sized,
    {
        let block_chunks = chunk_bytes(&mut rng, total_bytes, block_byte_sizes)?;
        let blocks = match payload {
            crate::Config::TraceAgent(enc) => {
                let ta = match enc {
                    crate::Encoding::Json => crate::TraceAgent::json(&mut rng),
                    crate::Encoding::MsgPack => crate::TraceAgent::msg_pack(&mut rng),
                };

                construct_block_cache_inner(&mut rng, &ta, &block_chunks)
            }
            crate::Config::Syslog5424 => {
                construct_block_cache_inner(&mut rng, &crate::Syslog5424::default(), &block_chunks)
            }
            crate::Config::DogStatsD(crate::dogstatsd::Config {
                contexts,
                name_length,
                tag_key_length,
                tag_value_length,
                tags_per_msg,
                // TODO -- Validate user input for multivalue_pack_probability.
                multivalue_pack_probability,
                multivalue_count,
                kind_weights,
                metric_weights,
                value,
            }) => {
                let serializer = crate::DogStatsD::new(
                    *contexts,
                    *name_length,
                    *tag_key_length,
                    *tag_value_length,
                    *tags_per_msg,
                    *multivalue_count,
                    *multivalue_pack_probability,
                    *kind_weights,
                    *metric_weights,
                    *value,
                    &mut rng,
                );

                construct_block_cache_inner(&mut rng, &serializer, &block_chunks)
            }
            crate::Config::Fluent => {
                let pyld = crate::Fluent::new(&mut rng);
                construct_block_cache_inner(&mut rng, &pyld, &block_chunks)
            }
            crate::Config::SplunkHec { encoding } => construct_block_cache_inner(
                &mut rng,
                &crate::SplunkHec::new(*encoding),
                &block_chunks,
            ),
            crate::Config::ApacheCommon => {
                let pyld = crate::ApacheCommon::new(&mut rng);
                construct_block_cache_inner(&mut rng, &pyld, &block_chunks)
            }
            crate::Config::Ascii => {
                let pyld = crate::Ascii::new(&mut rng);
                construct_block_cache_inner(&mut rng, &pyld, &block_chunks)
            }
            crate::Config::DatadogLog => {
                let serializer = crate::DatadogLog::new(&mut rng);
                construct_block_cache_inner(&mut rng, &serializer, &block_chunks)
            }
            crate::Config::Json => {
                construct_block_cache_inner(&mut rng, &crate::Json, &block_chunks)
            }
            crate::Config::Static { ref static_path } => construct_block_cache_inner(
                &mut rng,
                &crate::Static::new(static_path),
                &block_chunks,
            ),
            crate::Config::OpentelemetryTraces => {
                let pyld = crate::OpentelemetryTraces::new(&mut rng);
                construct_block_cache_inner(rng, &pyld, &block_chunks)
            }
            crate::Config::OpentelemetryLogs => {
                let pyld = crate::OpentelemetryLogs::new(&mut rng);
                construct_block_cache_inner(rng, &pyld, &block_chunks)
            }
            crate::Config::OpentelemetryMetrics => {
                let pyld = crate::OpentelemetryMetrics::new(&mut rng);
                construct_block_cache_inner(rng, &pyld, &block_chunks)
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
    total_bytes: usize,
    block_chunks: &[usize],
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
        crate::Config::DogStatsD(crate::dogstatsd::Config {
            contexts,
            name_length,
            tag_key_length,
            tag_value_length,
            tags_per_msg,
            // TODO -- Validate user input for multivalue_pack_probability.
            multivalue_pack_probability,
            multivalue_count,
            kind_weights,
            metric_weights,
            value,
        }) => {
            let pyld = crate::DogStatsD::new(
                *contexts,
                *name_length,
                *tag_key_length,
                *tag_value_length,
                *tags_per_msg,
                *multivalue_count,
                *multivalue_pack_probability,
                *kind_weights,
                *metric_weights,
                *value,
                &mut rng,
            );

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
            let pyld = crate::Static::new(static_path);
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
/// When calling [`construct_block_cache`] it's necessary to supply a
/// `block_chunks` argument, defining the block sizes that will be used when
/// serializing. Callers _generally_ will want to hit a certain total bytes
/// number of blocks and getting `total_bytes` parceled
/// up correctly is not necessarily straightforward. This utility method does
/// the computation in cases where it would otherwise be annoying. From the
/// allowable block sizes -- defined by `block_byte_sizes` -- a random member is
/// chosen and is deducted from the total bytes remaining. This process
/// continues until the total bytes remaining falls below the smallest block
/// size. It's possible that a user could supply just the right parameters to
/// make this loop infinitely. A more clever algorithm would be great.
///
/// # Errors
///
/// Function will return an error if `block_byte_sizes` is empty or if a member
/// of `block_byte_sizes` is large than `total_bytes`.
fn chunk_bytes<R>(
    rng: &mut R,
    total_bytes: NonZeroUsize,
    block_byte_sizes: &[NonZeroUsize],
) -> Result<Vec<usize>, Error>
where
    R: Rng + Sized,
{
    if block_byte_sizes.is_empty() {
        return Err(ChunkError::EmptyBlockBytes.into());
    }
    for bb in block_byte_sizes {
        if *bb > total_bytes {
            return Err(ChunkError::InsufficientTotalBytes.into());
        }
    }

    let mut chunks = Vec::new();
    let mut bytes_remaining = total_bytes.get();
    let minimum = block_byte_sizes.iter().min().unwrap().get();
    let maximum = block_byte_sizes.iter().max().unwrap().get();

    while bytes_remaining > minimum {
        let bytes_max = std::cmp::min(maximum, bytes_remaining);
        let block_bytes = block_byte_sizes.choose(rng).unwrap().get();
        if block_bytes > bytes_max {
            continue;
        }
        chunks.push(block_bytes);
        bytes_remaining = bytes_remaining.saturating_sub(block_bytes);
    }
    Ok(chunks)
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
fn construct_block_cache_inner<R, S>(
    mut rng: &mut R,
    serializer: &S,
    block_chunks: &[usize],
) -> Vec<Block>
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
    assert!(!block_cache.is_empty());
    block_cache
}

#[inline]
fn stream_block_inner<R, S>(
    mut rng: &mut R,
    total_bytes: usize,
    serializer: &S,
    block_chunks: &[usize],
    snd: &Sender<Block>,
) -> Result<(), SpinError>
where
    S: crate::Serialize,
    R: Rng + ?Sized,
{
    let total_bytes: u64 = total_bytes as u64;
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
                        mpsc::error::TrySendError::Full(_) => {
                            if accum_bytes < total_bytes {
                                accum_bytes += u64::from(block.total_bytes.get());
                                cache.push_back(block);
                                break 'refill;
                            }
                        }
                        mpsc::error::TrySendError::Closed(_) => return Ok(()),
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
fn construct_block<R, S>(mut rng: &mut R, serializer: &S, chunk_size: usize) -> Option<Block>
where
    S: crate::Serialize,
    R: Rng + ?Sized,
{
    let mut block: Writer<BytesMut> = BytesMut::with_capacity(chunk_size).writer();
    serializer
        .to_bytes(&mut rng, chunk_size, &mut block)
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

#[cfg(test)]
mod test {
    use std::num::NonZeroUsize;

    use proptest::{collection, prelude::*};
    use rand::{rngs::SmallRng, SeedableRng};

    use crate::block::{chunk_bytes, ChunkError, Error};

    /// Construct our `block_bytes_sizes` vector and the `total_bytes` value. We are
    /// careful to never generate an empty vector nor a `total_bytes` that is less
    /// than any value in `block_bytes_sizes`.
    fn total_bytes_and_block_bytes() -> impl Strategy<Value = (NonZeroUsize, Vec<NonZeroUsize>)> {
        (1..usize::MAX).prop_flat_map(|total_bytes| {
            (
                Just(NonZeroUsize::new(total_bytes).unwrap()),
                collection::vec(
                    (1..total_bytes).prop_map(|i| NonZeroUsize::new(i).unwrap()),
                    1..1_000,
                ),
            )
        })
    }

    // No chunk is a returned set of chunks should ever be 0.
    proptest! {
        #[test]
        fn chunk_never_size_zero(seed: u64, (total_bytes, block_bytes_sizes) in total_bytes_and_block_bytes()) {
            let mut rng = SmallRng::seed_from_u64(seed);
            let chunks = chunk_bytes(&mut rng, total_bytes, &block_bytes_sizes).unwrap();

            for chunk in chunks {
                prop_assert!(chunk > 0);
            }
        }
    }

    // The vec of chunks must not be empty.
    proptest! {
        #[test]
        fn chunks_never_empty(seed: u64, (total_bytes, block_bytes_sizes) in total_bytes_and_block_bytes()) {
            let mut rng = SmallRng::seed_from_u64(seed);
            let chunks = chunk_bytes(&mut rng, total_bytes, &block_bytes_sizes).unwrap();
            prop_assert!(!chunks.is_empty());
        }
    }

    // Passing an empty block_byte_sizes always triggers an error condition.
    proptest! {
        #[test]
        fn chunks_empty_trigger_error(seed: u64, total_bytes in (1..usize::MAX).prop_map(|i| NonZeroUsize::new(i).unwrap())) {
            let mut rng = SmallRng::seed_from_u64(seed);
            match chunk_bytes(&mut rng, total_bytes, &[]) {
                Err(Error::Chunk(ChunkError::EmptyBlockBytes)) => assert!(true),
                _ => assert!(false),
            }
        }
    }
}
