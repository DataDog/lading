#![no_main]

use arbitrary;
use libfuzzer_sys::fuzz_target;
use rand::Rng;
use rand::{rngs::SmallRng, SeedableRng};
use std::num::{NonZeroU32, NonZeroU8};

use lading_payload::block::Cache;

#[derive(arbitrary::Arbitrary, Debug)]
struct Input {
    seed: [u8; 32],
    total_bytes: NonZeroU8,
    block_bytes_sizes: [NonZeroU8; 8],
}

fuzz_target!(|input: Input| {
    for byte_size in &input.block_bytes_sizes {
        if byte_size > &input.total_bytes {
            return;
        }
    }

    let total_bytes = NonZeroU32::new(input.total_bytes.get() as u32).unwrap();
    let block_bytes_sizes = input
        .block_bytes_sizes
        .map(|x| NonZeroU32::new(x.get() as u32).unwrap());

    let mut rng = SmallRng::from_seed(input.seed);
    let res = Cache::fixed(
        &mut rng,
        total_bytes,
        &block_bytes_sizes,
        &lading_payload::Config::Fluent,
    );
    res.unwrap();
});
