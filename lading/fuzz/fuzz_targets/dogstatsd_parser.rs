#![no_main]

use lading::blackhold::dogstatsd;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    lading_fuzz::debug_input(&data);

    for _metric in dogstatsd::Parser::new(data) {
        // Iterate until the end, no panics
    }
});
