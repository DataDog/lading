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
    config: lading_payload::opentelemetry::metric::Config,
}

const MAX_TOTAL_BYTES: u32 = 10 * 1024 * 1024;  // 10 MiB
const MAX_BLOCK_SIZE: u32 = 1 * 1024 * 1024;    // 1 MiB
const MAX_CONTEXTS: u32 = 5_000;

fuzz_target!(|input: Input| {
    if std::env::var("FUZZ_DEBUG").is_ok() {
        eprintln!("=== FUZZ INPUT DEBUG ===");
        eprintln!("{:#?}", input);
        eprintln!("========================");
    }
    
    if input.total_bytes.get() > MAX_TOTAL_BYTES {
        return;
    }
    
    if input.max_block_size.get() > MAX_BLOCK_SIZE {
        return;
    }
    
    if input.max_block_size.get() > input.total_bytes.get() {
        return;
    }

    if input.config.valid().is_err() {
        return;
    }
    
    let max_contexts = match input.config.contexts.total_contexts {
        lading_payload::common::config::ConfRange::Constant(n) => n,
        lading_payload::common::config::ConfRange::Inclusive { max, .. } => max,
    };
    if max_contexts > MAX_CONTEXTS {
        return;
    }

    let mut rng = SmallRng::from_seed(input.seed);
    let payload = lading_payload::Config::OpentelemetryMetrics(input.config);
    
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