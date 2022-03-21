use crate::payload::{self, Serialize};
use metrics::gauge;
use rand::{prelude::SliceRandom, Rng};
use std::{convert::TryInto, num::NonZeroU32};

#[derive(Debug)]
pub enum Error {
    Payload(payload::Error),
    Empty,
}

impl From<payload::Error> for Error {
    fn from(error: payload::Error) -> Self {
        Error::Payload(error)
    }
}

#[derive(Debug)]
pub struct Block {
    pub total_bytes: NonZeroU32,
    pub lines: u64,
    pub bytes: Vec<u8>,
}

#[inline]
fn total_newlines(input: &[u8]) -> u64 {
    bytecount::count(input, b'\n') as u64
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
/// # Panics
///
/// Function will panic if `block_bytes_sizes` is empty.
pub fn chunk_bytes<R>(rng: &mut R, total_bytes: usize, block_byte_sizes: &[usize]) -> Vec<usize>
where
    R: Rng + Sized,
{
    assert!(!block_byte_sizes.is_empty());

    let mut chunks = Vec::new();
    let mut bytes_remaining = total_bytes;
    let minimum = *block_byte_sizes.iter().min().unwrap();
    let maximum = *block_byte_sizes.iter().max().unwrap();

    while bytes_remaining > minimum {
        let bytes_max = std::cmp::min(maximum, bytes_remaining);
        let block_bytes = block_byte_sizes.choose(rng).unwrap();
        if *block_bytes > bytes_max {
            continue;
        }
        chunks.push(*block_bytes);
        bytes_remaining = bytes_remaining.saturating_sub(*block_bytes);
    }
    chunks
}

#[allow(clippy::ptr_arg)]
#[allow(clippy::cast_precision_loss)]
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
/// Function will panic if the `serializer` signals an error. In the futures we
/// would like to propagate this error to the caller.
pub fn construct_block_cache<R, S>(
    mut rng: R,
    serializer: &S,
    block_chunks: &[usize],
    labels: &Vec<(String, String)>,
) -> Vec<Block>
where
    S: Serialize,
    R: Rng,
{
    let mut block_cache: Vec<Block> = Vec::with_capacity(block_chunks.len());
    for block_size in block_chunks {
        let mut block: Vec<u8> = Vec::with_capacity(*block_size);
        serializer
            .to_bytes(&mut rng, *block_size, &mut block)
            .unwrap();
        block.shrink_to_fit();
        // For unknown reasons this fails. Will need to start property testing
        // this library.
        // assert!(!block.is_empty());
        if block.is_empty() {
            continue;
        }
        let total_bytes = NonZeroU32::new(block.len().try_into().unwrap()).unwrap();
        let newlines = total_newlines(&block);
        block_cache.push(Block {
            total_bytes,
            lines: newlines,
            bytes: block,
        });
    }
    assert!(!block_cache.is_empty());
    gauge!("block_construction_complete", 1.0, labels);
    block_cache
}
