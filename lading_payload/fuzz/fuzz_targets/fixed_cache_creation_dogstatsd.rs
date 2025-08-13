#![no_main]

use arbitrary;
use libfuzzer_sys::fuzz_target;
use rand::{rngs::SmallRng, SeedableRng};
use std::num::NonZeroU32;

use lading_payload::block::Cache;

#[derive(arbitrary::Arbitrary, Debug)]
struct Input {
    seed: [u8; 32],
    total_bytes: NonZeroU32,
    block_bytes_sizes: [NonZeroU32; 32],
    config: lading_payload::dogstatsd::Config,
}

fuzz_target!(|input: Input| {
    // Limit total bytes to 100MB to prevent OOM
    if input.total_bytes.get() > 100_000_000 {
        return;
    }

    for byte_size in &input.block_bytes_sizes {
        if byte_size > &input.total_bytes || byte_size.get() > 10_000_000 {
            return;
        }
    }

    let mut rng = SmallRng::from_seed(input.seed);
    let max_block_size = input
        .block_bytes_sizes
        .iter()
        .filter(|&size| size.get() <= 10_000_000)
        .max()
        .copied()
        .unwrap_or(NonZeroU32::new(1).unwrap());
    let _res = Cache::fixed(
        &mut rng,
        input.total_bytes,
        u128::from(max_block_size.get()),
        &lading_payload::Config::DogStatsD(input.config),
    );
});
