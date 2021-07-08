use crate::payload::{self, Serialize};
use metrics::gauge;
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

#[allow(clippy::ptr_arg)]
#[allow(clippy::cast_precision_loss)]
pub fn construct_block_cache<S>(
    serializer: &S,
    block_chunks: &[usize],
    labels: &Vec<(String, String)>,
) -> Vec<Block>
where
    S: Serialize,
{
    let mut block_cache: Vec<Block> = Vec::with_capacity(block_chunks.len());
    for block_size in block_chunks {
        let mut block: Vec<u8> = Vec::with_capacity(*block_size);
        serializer.to_bytes(*block_size, &mut block).unwrap();
        block.shrink_to_fit();
        assert!(!block.is_empty());
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
