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
}

fuzz_target!(|input: Input| {
    // Limit total bytes to 100MB to prevent OOM
    if input.total_bytes.get() > 100_000_000 {
        return;
    }

    let max_block_size = input.block_bytes_sizes
        .iter()
        .map(|s| s.get())
        .filter(|&s| s <= 10_000_000)
        .max()
        .unwrap_or(1);
    
    if max_block_size > input.total_bytes.get() {
        return;
    }

    let mut rng = SmallRng::from_seed(input.seed);
    let _res = Cache::fixed(
        &mut rng,
        input.total_bytes,
        u128::from(max_block_size),
        &lading_payload::Config::Json,
    );
});
