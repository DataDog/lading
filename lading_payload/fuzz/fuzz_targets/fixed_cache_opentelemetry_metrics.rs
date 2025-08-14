#![no_main]

use arbitrary;
use libfuzzer_sys::fuzz_target;
use rand::{SeedableRng, rngs::SmallRng};
use std::num::NonZeroU32;

use lading_payload::Serialize;
use lading_payload::opentelemetry::metric;

#[derive(arbitrary::Arbitrary, Debug)]
struct Input {
    seed: [u8; 32],
    budget_bytes: NonZeroU32,
    config: metric::Config,
}

const MAX_CONTEXTS: u32 = 5_000;
const MAX_BUDGET: usize = 1 * 1024 * 1024; // MiB

fuzz_target!(|input: Input| {
    // Validate inputs, skipping if too large to fit into memory or just plain invalid.
    if input.config.valid().is_err() {
        return;
    }
    let total_contexts = match input.config.contexts.total_contexts {
        lading_payload::common::config::ConfRange::Constant(n) => n,
        lading_payload::common::config::ConfRange::Inclusive { max, .. } => max,
    };
    if total_contexts > MAX_CONTEXTS {
        return;
    }

    let budget = input.budget_bytes.get() as usize;
    if budget > MAX_BUDGET {
        return;
    }
    if budget < metric::SMALLEST_PROTOBUF {
        return;
    }

    // The actual fuzz bit. We make a new OpentelemetryMetrics instance then
    // generate `budget` bytes into a vec. If the vec ends up being larger than
    // the budget, failure.
    let mut rng = SmallRng::from_seed(input.seed);
    let mut metrics = metric::OpentelemetryMetrics::new(input.config, &mut rng)
        .expect("failed to create metrics generator");

    let mut bytes = Vec::with_capacity(budget);
    metrics
        .to_bytes(&mut rng, budget, &mut bytes)
        .expect("failed to convert to bytes");
    assert!(
        bytes.len() <= budget,
        "max len: {budget}, actual: {}",
        bytes.len()
    );
});
