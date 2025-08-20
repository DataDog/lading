#![no_main]

use arbitrary;
use libfuzzer_sys::fuzz_target;
use rand::{SeedableRng, rngs::SmallRng};
use std::num::NonZeroU32;

use lading_payload::block::Cache;

#[derive(arbitrary::Arbitrary, Debug)]
struct Input {
    seed: [u8; 32],
    total_bytes: NonZeroU32,
    max_block_size: NonZeroU32,
    encoding: lading_payload::Encoding,
}

const MAX_TOTAL_BYTES: u32 = 10 * 1024 * 1024;  // 10 MiB
const MAX_BLOCK_SIZE: u32 = 1 * 1024 * 1024;    // 1 MiB

fuzz_target!(|input: Input| {
    lading_fuzz::debug_input(&input);
    
    if input.total_bytes.get() > MAX_TOTAL_BYTES {
        return;
    }
    
    if input.max_block_size.get() > MAX_BLOCK_SIZE {
        return;
    }
    
    if input.max_block_size.get() > input.total_bytes.get() {
        return;
    }

    let mut rng = SmallRng::from_seed(input.seed);
    let payload = lading_payload::Config::TraceAgent(input.encoding);
    
    let mut cache = match Cache::fixed(
        &mut rng,
        input.total_bytes,
        u128::from(input.max_block_size.get()),
        &payload,
    ) {
        Ok(c) => c,
        Err(_) => return,
    };
    
    // Call next_block 10 times to exercise the cache rotation
    for _ in 0..10 {
        let _block = cache.next_block();
    }
});