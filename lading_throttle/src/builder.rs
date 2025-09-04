//! Throttle builder for unified configuration
//!
//! This module provides a builder pattern for creating throttles with
//! consistent configuration handling across all generators.

use crate::{Config, Throttle};
use byte_unit::Byte;
use serde::{Deserialize, Serialize};
use std::num::{NonZeroU16, NonZeroU32};

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

impl TryFrom<BytesThrottleConfig> for Config {
    type Error = ThrottleConversionError;

    #[allow(clippy::cast_possible_truncation)]
    fn try_from(config: BytesThrottleConfig) -> Result<Self, Self::Error> {
        match config {
            BytesThrottleConfig::AllOut => Ok(Config::AllOut),
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
                Ok(Config::Stable {
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

                Ok(Config::Linear {
                    initial_capacity: initial,
                    maximum_capacity: maximum,
                    rate_of_change: rate,
                })
            }
        }
    }
}

/// Unified throttle configuration builder for generators
///
/// This builder handles the common pattern of configuring throttles from either
/// `bytes_per_second` or a `throttle` configuration, ensuring exactly one is provided.
#[derive(Debug, Default)]
pub struct ThrottleBuilder<'a> {
    bytes_per_second: Option<&'a Byte>,
    throttle_config: Option<&'a BytesThrottleConfig>,
    parallel_connections: Option<NonZeroU16>,
}

impl<'a> ThrottleBuilder<'a> {
    /// Create a new throttle builder
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set bytes per second configuration
    #[must_use]
    pub fn bytes_per_second(mut self, bps: Option<&'a Byte>) -> Self {
        self.bytes_per_second = bps;
        self
    }

    /// Set throttle configuration
    #[must_use]
    pub fn throttle_config(mut self, config: Option<&'a BytesThrottleConfig>) -> Self {
        self.throttle_config = config;
        self
    }

    /// Set the number of parallel connections that will share this throttle
    #[must_use]
    pub fn parallel_connections(mut self, connections: Option<NonZeroU16>) -> Self {
        self.parallel_connections = connections;
        self
    }

    /// Build the throttle
    ///
    /// If `parallel_connections` > 1, the throttle capacity will be divided evenly
    /// among the connections.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Both `bytes_per_second` and `throttle_config` are provided
    /// - Neither configuration is provided
    /// - The configuration values are invalid (zero or too large)
    #[allow(clippy::cast_possible_truncation)]
    pub fn build(self) -> Result<Throttle, ThrottleBuilderError> {
        let divisor = match self.parallel_connections {
            Some(n) => u32::from(n.get()),
            None => 1,
        };

        let config = match (self.bytes_per_second, self.throttle_config) {
            (Some(bps), None) => {
                let total_bps = bps.as_u128() as u32;
                let divided_bps = total_bps / divisor;
                let bytes_per_second =
                    NonZeroU32::new(divided_bps).ok_or(ThrottleBuilderError::Zero)?;
                Config::Stable {
                    maximum_capacity: bytes_per_second,
                    timeout_micros: 0,
                }
            }
            (None, Some(throttle_config)) => {
                let divided_config = if divisor == 1 {
                    *throttle_config
                } else {
                    match *throttle_config {
                        BytesThrottleConfig::AllOut => BytesThrottleConfig::AllOut,
                        BytesThrottleConfig::Stable {
                            bytes_per_second,
                            timeout_millis,
                        } => {
                            let total = bytes_per_second.as_u128() as u32;
                            BytesThrottleConfig::Stable {
                                bytes_per_second: Byte::from_u64(u64::from(total / divisor)),
                                timeout_millis,
                            }
                        }
                        BytesThrottleConfig::Linear {
                            initial_bytes_per_second,
                            maximum_bytes_per_second,
                            rate_of_change,
                        } => {
                            let initial_total = initial_bytes_per_second.as_u128() as u32;
                            let max_total = maximum_bytes_per_second.as_u128() as u32;
                            let rate_total = rate_of_change.as_u128() as u32;

                            BytesThrottleConfig::Linear {
                                initial_bytes_per_second: Byte::from_u64(u64::from(
                                    initial_total / divisor,
                                )),
                                maximum_bytes_per_second: Byte::from_u64(u64::from(
                                    max_total / divisor,
                                )),
                                rate_of_change: Byte::from_u64(u64::from(rate_total / divisor)),
                            }
                        }
                    }
                };
                divided_config
                    .try_into()
                    .map_err(ThrottleBuilderError::Conversion)?
            }
            (Some(_), Some(_)) => return Err(ThrottleBuilderError::ConflictingConfig),
            (None, None) => return Err(ThrottleBuilderError::NoConfig),
        };

        Ok(Throttle::new_with_config(config))
    }
}

/// Errors that can occur when building a throttle
#[derive(Debug, thiserror::Error, Clone, Copy)]
pub enum ThrottleBuilderError {
    /// Both `bytes_per_second` and throttle configurations provided
    #[error("Both bytes_per_second and throttle configurations provided. Use only one.")]
    ConflictingConfig,
    /// No throttle configuration provided
    #[error("Either bytes_per_second or throttle configuration must be provided")]
    NoConfig,
    /// Value is zero
    #[error("Throttle value must not be zero")]
    Zero,
    /// Throttle conversion error
    #[error("Throttle configuration error: {0}")]
    Conversion(#[from] ThrottleConversionError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn throttle_builder_division_works(
            bytes in 1_u32..=1_000_000_u32,
            worker_count in 1_u16..=100_u16
        ) {
            let bytes = Byte::from_u64(u64::from(bytes));
            let result = ThrottleBuilder::new()
                .bytes_per_second(Some(&bytes))
                .parallel_connections(NonZeroU16::new(worker_count))
                .build();

            assert!(result.is_ok());
        }

        #[test]
        fn throttle_builder_rejects_dual_config(
            bytes in 1_u32..=1_000_000_u32,
            timeout_millis in 1..=10_000_u64,
        ) {
            let bytes = Byte::from_u64(u64::from(bytes));
            let throttle_config = BytesThrottleConfig::Stable {
                bytes_per_second: bytes,
                timeout_millis,
            };

            let result = ThrottleBuilder::new()
                .bytes_per_second(Some(&bytes))
                .throttle_config(Some(&throttle_config))
                .build();

            assert!(matches!(result, Err(ThrottleBuilderError::ConflictingConfig)));
        }
    }

    #[test]
    fn throttle_builder_requires_config() {
        let result = ThrottleBuilder::new().build();
        assert!(matches!(result, Err(ThrottleBuilderError::NoConfig)));
    }
}
