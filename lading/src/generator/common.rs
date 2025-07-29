//! Common types for generators

use byte_unit::Byte;
use serde::{Deserialize, Serialize};
use std::{convert::TryFrom, num::NonZeroU32};

/// Generator-specific throttle configuration with clearer field names
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum BytesThrottleConfig {
    /// A throttle that allows the generator to produce as fast as possible
    AllOut,
    /// A throttle that attempts stable load
    Stable {
        /// The bytes per second rate limit (e.g., "1MB", "512KiB")
        bytes_per_second: Byte,
        /// The timeout in milliseconds for IO operations. Default is 0.
        #[serde(default)]
        timeout_millis: u64,
    },
    /// A throttle that linearly increases load over time
    Linear {
        /// The initial bytes per second (e.g., "100KB")
        initial_bytes_per_second: Byte,
        /// The maximum bytes per second (e.g., "10MB")
        maximum_bytes_per_second: Byte,
        /// The rate of change in bytes per second per second
        rate_of_change: Byte,
    },
}

/// Error converting `BytesThrottleConfig` to internal throttle config
#[derive(Debug, thiserror::Error, Clone, Copy)]
pub enum ThrottleConversionError {
    /// Value exceeds u32 capacity
    #[error("Throttle value {0} exceeds maximum supported value")]
    ValueTooLarge(Byte),
    /// Value is zero
    #[error("Throttle value must not be zero")]
    Zero,
}

impl TryFrom<BytesThrottleConfig> for lading_throttle::Config {
    type Error = ThrottleConversionError;

    #[allow(clippy::cast_possible_truncation)]
    fn try_from(config: BytesThrottleConfig) -> Result<Self, Self::Error> {
        match config {
            BytesThrottleConfig::AllOut => Ok(lading_throttle::Config::AllOut),
            BytesThrottleConfig::Stable {
                bytes_per_second,
                timeout_millis,
            } => {
                let value = bytes_per_second.as_u128();
                if value > u128::from(u32::MAX) {
                    return Err(ThrottleConversionError::ValueTooLarge(bytes_per_second));
                }
                let value = value as u32;
                let value = NonZeroU32::new(value).ok_or(ThrottleConversionError::Zero)?;
                Ok(lading_throttle::Config::Stable {
                    maximum_capacity: value,
                    timeout_micros: timeout_millis.saturating_mul(1000),
                })
            }
            BytesThrottleConfig::Linear {
                initial_bytes_per_second,
                maximum_bytes_per_second,
                rate_of_change,
            } => {
                let initial = initial_bytes_per_second.as_u128();
                let maximum = maximum_bytes_per_second.as_u128();
                let rate = rate_of_change.as_u128();

                if initial > u128::from(u32::MAX) {
                    return Err(ThrottleConversionError::ValueTooLarge(
                        initial_bytes_per_second,
                    ));
                }
                if maximum > u128::from(u32::MAX) {
                    return Err(ThrottleConversionError::ValueTooLarge(
                        maximum_bytes_per_second,
                    ));
                }
                if rate > u128::from(u32::MAX) {
                    return Err(ThrottleConversionError::ValueTooLarge(rate_of_change));
                }

                let initial = initial as u32;
                let maximum = maximum as u32;
                let rate = rate as u32;

                let maximum = NonZeroU32::new(maximum).ok_or(ThrottleConversionError::Zero)?;

                Ok(lading_throttle::Config::Linear {
                    initial_capacity: initial,
                    maximum_capacity: maximum,
                    rate_of_change: rate,
                })
            }
        }
    }
}
