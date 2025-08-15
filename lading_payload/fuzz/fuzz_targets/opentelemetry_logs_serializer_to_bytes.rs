#![no_main]

use arbitrary;
use libfuzzer_sys::fuzz_target;
use rand::{SeedableRng, rngs::SmallRng};
use std::num::NonZeroU32;

use lading_payload::Serialize;

#[derive(arbitrary::Arbitrary, Debug)]
struct Input {
    seed: [u8; 32],
    budget_bytes: NonZeroU32,
    config: lading_payload::opentelemetry::log::Config,
}

const MAX_BUDGET: usize = 1 * 1024 * 1024; // 1 MiB
const MAX_CONTEXTS: u32 = 5_000;

fuzz_target!(|input: Input| {
    let budget = input.budget_bytes.get() as usize;
    if budget > MAX_BUDGET {
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
    let mut bytes = Vec::with_capacity(budget);

    let mut serializer = match lading_payload::opentelemetry::log::OpentelemetryLogs::new(input.config, &mut rng) {
        Ok(s) => s,
        Err(_) => return,
    };
    
    if serializer.to_bytes(&mut rng, budget, &mut bytes).is_ok() {
        assert!(bytes.len() <= budget);
    }
});