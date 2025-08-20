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
    config: lading_payload::dogstatsd::Config,
}

const MAX_BUDGET: usize = 1 * 1024 * 1024; // 1 MiB
const MAX_CONTEXTS: u32 = 5_000;
const MAX_TAG_LENGTH: u16 = 200;
const MAX_TAGS_PER_MSG: u8 = 50;
const MAX_SERVICE_CHECK_NAMES: u16 = 1_000;
const MAX_NAME_LENGTH: u16 = 200;

fuzz_target!(|input: Input| {
    lading_fuzz::debug_input(&input);

    let budget = input.budget_bytes.get() as usize;
    if budget > MAX_BUDGET {
        return;
    }

    if input.config.valid().is_err() {
        return;
    }

    let max_contexts = match input.config.contexts {
        lading_payload::common::config::ConfRange::Constant(n) => n,
        lading_payload::common::config::ConfRange::Inclusive { max, .. } => max,
    };
    if max_contexts > MAX_CONTEXTS {
        return;
    }

    let max_tag_length = match input.config.tag_length {
        lading_payload::common::config::ConfRange::Constant(n) => n,
        lading_payload::common::config::ConfRange::Inclusive { max, .. } => max,
    };
    if max_tag_length > MAX_TAG_LENGTH {
        return;
    }

    let max_tags_per_msg = match input.config.tags_per_msg {
        lading_payload::common::config::ConfRange::Constant(n) => n,
        lading_payload::common::config::ConfRange::Inclusive { max, .. } => max,
    };
    if max_tags_per_msg > MAX_TAGS_PER_MSG {
        return;
    }

    let max_service_check_names = match input.config.service_check_names {
        lading_payload::common::config::ConfRange::Constant(n) => n,
        lading_payload::common::config::ConfRange::Inclusive { max, .. } => max,
    };
    if max_service_check_names > MAX_SERVICE_CHECK_NAMES {
        return;
    }

    let max_name_length = match input.config.name_length {
        lading_payload::common::config::ConfRange::Constant(n) => n,
        lading_payload::common::config::ConfRange::Inclusive { max, .. } => max,
    };
    if max_name_length > MAX_NAME_LENGTH {
        return;
    }

    let mut rng = SmallRng::from_seed(input.seed);
    let mut bytes = Vec::with_capacity(budget);

    let mut serializer = match lading_payload::DogStatsD::new(input.config, &mut rng) {
        Ok(s) => s,
        Err(_) => return,
    };

    if serializer.to_bytes(&mut rng, budget, &mut bytes).is_ok() {
        assert!(bytes.len() <= budget);
    }
});
