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
    payload: lading_payload::Config,
}

fuzz_target!(|input: Input| {
    for byte_size in &input.block_bytes_sizes {
        if byte_size > &input.total_bytes {
            return;
        }
    }

    let mut rng = SmallRng::from_seed(input.seed);
    let _res = Cache::fixed(
        &mut rng,
        input.total_bytes,
        &input.block_bytes_sizes,
        &input.payload,
    );
});
