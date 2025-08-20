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
    encoding: lading_payload::Encoding,
}

const MAX_BUDGET: usize = 1 * 1024 * 1024; // 1 MiB

fuzz_target!(|input: Input| {
    lading_fuzz::debug_input(&input);
    
    let budget = input.budget_bytes.get() as usize;
    if budget > MAX_BUDGET {
        return;
    }

    let mut rng = SmallRng::from_seed(input.seed);
    let mut bytes = Vec::with_capacity(budget);

    let mut serializer = match input.encoding {
        lading_payload::Encoding::Json => lading_payload::TraceAgent::json(&mut rng),
        lading_payload::Encoding::MsgPack => lading_payload::TraceAgent::msg_pack(&mut rng),
    };
    
    if serializer.to_bytes(&mut rng, budget, &mut bytes).is_ok() {
        assert!(bytes.len() <= budget);
    }
});