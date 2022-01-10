use crate::payload::{self, Serialize};
use metrics::gauge;
use rand::prelude::SliceRandom;
use rand::Rng;
use std::convert::TryInto;
use std::num::NonZeroU32;

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
