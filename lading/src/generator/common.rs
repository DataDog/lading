//! Common types for generators

use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;

/// Generator-specific throttle configuration with clearer field names
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum BytesThrottleConfig {
    /// A throttle that allows the generator to produce as fast as possible
    AllOut,
    /// A throttle that attempts stable load
    Stable {
        /// The bytes per second rate limit
        bytes_per_second: NonZeroU32,
    },
    /// A throttle that linearly increases load over time
    Linear {
        /// The initial bytes per second
        initial_bytes_per_second: u32,
        /// The maximum bytes per second
        maximum_bytes_per_second: NonZeroU32,
        /// The rate of change per second
        rate_of_change: u32,
    },
}

impl From<BytesThrottleConfig> for lading_throttle::Config {
    fn from(config: BytesThrottleConfig) -> Self {
        match config {
            BytesThrottleConfig::AllOut => lading_throttle::Config::AllOut,
            BytesThrottleConfig::Stable { bytes_per_second } => lading_throttle::Config::Stable {
                maximum_capacity: bytes_per_second,
            },
            BytesThrottleConfig::Linear {
                initial_bytes_per_second,
                maximum_bytes_per_second,
                rate_of_change,
            } => lading_throttle::Config::Linear {
                initial_capacity: initial_bytes_per_second,
                maximum_capacity: maximum_bytes_per_second,
                rate_of_change,
            },
        }
    }
}
