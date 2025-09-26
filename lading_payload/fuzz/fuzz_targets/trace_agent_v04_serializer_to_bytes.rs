#![no_main]

use arbitrary;
use libfuzzer_sys::fuzz_target;
use rand::{SeedableRng, rngs::SmallRng};
use std::num::NonZeroU32;

use lading_payload::{Serialize, trace_agent::v04::{Config, V04}};

#[derive(arbitrary::Arbitrary, Debug)]
struct Input {
    seed: [u8; 32],
    budget_bytes: NonZeroU32,
    config: Config,
}

const MAX_BUDGET: usize = 1 * 1024 * 1024; // 1 MiB

fuzz_target!(|input: Input| {
    lading_fuzz::debug_input(&input);

    let budget = input.budget_bytes.get() as usize;
    if budget > MAX_BUDGET {
        return;
    }

    if input.config.valid().is_err() {
        return;
    }

    let mut rng = SmallRng::from_seed(input.seed);
    let mut bytes = Vec::with_capacity(budget);

    let mut serializer = match V04::with_config(input.config, &mut rng) {
        Ok(s) => s,
        Err(_) => return,
    };

    if serializer.to_bytes(&mut rng, budget, &mut bytes).is_ok() {
        assert!(bytes.len() <= budget);
    }
});
