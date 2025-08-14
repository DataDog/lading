#![no_main]

use arbitrary;
use libfuzzer_sys::fuzz_target;
use rand::{SeedableRng, rngs::SmallRng};
use std::num::NonZeroU16;

use lading_payload::Serialize;
use lading_payload::common::config::ConfRange;
use lading_payload::opentelemetry::metric;

#[derive(arbitrary::Arbitrary, Debug)]
struct Input {
    seed: [u8; 32],
    budget_bytes: NonZeroU16,
}

const MAX_BUDGET: usize = 1 * 1024; // KiB

// This fuzz test is specifically for use in debugging PR #1447. It is otherwise
// improved on by fixed_cache_opentelemetry_metrics and should be removed once
// PR #1447 is closed.

fuzz_target!(|input: Input| {
    let config = metric::Config {
        contexts: metric::Contexts {
            total_contexts: ConfRange::Constant(10),
            attributes_per_resource: ConfRange::Constant(5),
            scopes_per_resource: ConfRange::Constant(2),
            attributes_per_scope: ConfRange::Constant(3),
            metrics_per_scope: ConfRange::Constant(4),
            attributes_per_metric: ConfRange::Constant(2),
        },
        ..Default::default()
    };

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
    let mut metrics = metric::OpentelemetryMetrics::new(config, &mut rng)
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
