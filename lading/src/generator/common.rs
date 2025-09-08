//! Common types for generators

use byte_unit::Byte;
use lading_throttle::Throttle;
use serde::{Deserialize, Serialize};
use std::{
    convert::TryFrom,
    num::{NonZeroU16, NonZeroU32},
};
use tracing::error;

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

/// Unified throttle configuration builder for generators
///
/// This builder handles the common pattern of configuring throttles from either
/// `bytes_per_second` or a `throttle` configuration, ensuring exactly one is provided.
pub(super) struct ThrottleBuilder<'a> {
    bytes_per_second: Option<&'a Byte>,
    throttle_config: Option<&'a BytesThrottleConfig>,
    parallel_connections: Option<NonZeroU16>,
}

impl<'a> ThrottleBuilder<'a> {
    /// Create a new throttle builder
    pub(super) fn new() -> Self {
        Self {
            bytes_per_second: None,
            throttle_config: None,
            parallel_connections: None,
        }
    }

    /// Set bytes per second configuration
    pub(super) fn bytes_per_second(mut self, bps: Option<&'a Byte>) -> Self {
        self.bytes_per_second = bps;
        self
    }

    /// Set throttle configuration
    pub(super) fn throttle_config(mut self, config: Option<&'a BytesThrottleConfig>) -> Self {
        self.throttle_config = config;
        self
    }

    /// Set the number of parallel connections that will share this throttle
    pub(super) fn parallel_connections(mut self, connections: Option<NonZeroU16>) -> Self {
        self.parallel_connections = connections;
        self
    }

    /// Determine if the builder will produce a valid throttle
    ///
    /// Returns 'true' if the builder configuration will produce a valid
    /// throttle, false otherwise.
    #[allow(clippy::cast_possible_truncation)]
    fn valid(&self) -> bool {
        let divisor = match self.parallel_connections {
            Some(n) => u32::from(n.get()),
            None => 1,
        };

        match (self.bytes_per_second, self.throttle_config) {
            (Some(bps), None) => {
                let bytes_per_second = bps.as_u128();
                if bytes_per_second > u128::from(u32::MAX) {
                    error!("bytes_per_second greater than max throttle capacity");
                    return false;
                }
                (bytes_per_second as u32) >= divisor
            }
            (None, Some(throttle_config)) => match *throttle_config {
                BytesThrottleConfig::AllOut => true,
                BytesThrottleConfig::Stable {
                    bytes_per_second, ..
                } => {
                    if bytes_per_second.as_u128() > u128::from(u32::MAX) {
                        error!("bytes_per_second greater than max throttle capacity");
                        return false;
                    }
                    (bytes_per_second.as_u128() as u32) >= divisor
                }
                BytesThrottleConfig::Linear {
                    initial_bytes_per_second,
                    maximum_bytes_per_second,
                    rate_of_change,
                } => {
                    let initial = initial_bytes_per_second.as_u128();
                    let maximum = maximum_bytes_per_second.as_u128();
                    let rate = rate_of_change.as_u128();

                    if initial > u128::from(u32::MAX)
                        || maximum > u128::from(u32::MAX)
                        || rate > u128::from(u32::MAX)
                    {
                        error!("One or more throttle parameter greater than max throttle capacity");
                        return false;
                    }

                    let max_total = maximum as u32;
                    max_total > 0 && max_total >= divisor
                }
            },
            // Both configs provided or no config provided
            (Some(_), Some(_)) | (None, None) => false,
        }
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
    /// - The configuration values are invalid
    #[allow(clippy::cast_possible_truncation)]
    pub(super) fn build(self) -> Result<Throttle, ThrottleBuilderError> {
        assert!(self.valid());

        let divisor = match self.parallel_connections {
            Some(n) => u32::from(n.get()),
            None => 1,
        };

        let config = match (self.bytes_per_second, self.throttle_config) {
            (Some(bps), None) => {
                let total_bps = bps.as_u128() as u32;
                if total_bps < divisor {
                    return Err(ThrottleBuilderError::InsufficientBytesForWorkers {
                        bytes_per_second: total_bps,
                        worker_count: divisor,
                    });
                }
                let divided_bps = total_bps / divisor;
                let bytes_per_second =
                    NonZeroU32::new(divided_bps).ok_or(ThrottleBuilderError::Zero)?;
                lading_throttle::Config::Stable {
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
                            if total < divisor {
                                return Err(ThrottleBuilderError::InsufficientBytesForWorkers {
                                    bytes_per_second: total,
                                    worker_count: divisor,
                                });
                            }
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
                            if max_total < divisor {
                                return Err(ThrottleBuilderError::InsufficientBytesForWorkers {
                                    bytes_per_second: max_total,
                                    worker_count: divisor,
                                });
                            }
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
    /// Insufficient bytes for worker count
    #[error(
        "Bytes per second ({bytes_per_second}) is less than worker count ({worker_count}). Each worker needs at least 1 byte per second."
    )]
    InsufficientBytesForWorkers {
        bytes_per_second: u32,
        worker_count: u32,
    },
    /// Throttle conversion error
    #[error("Throttle configuration error: {0}")]
    Conversion(#[from] ThrottleConversionError),
}

/// Concurrency management strategies for generators
///
/// This enum represents the two main concurrency patterns used across generators:
/// - Pooled: Multiple concurrent requests with semaphore limiting (HTTP/Splunk HEC pattern)
/// - Workers: Multiple persistent worker tasks (TCP/UDP/Unix pattern)
#[derive(Debug)]
pub(super) enum ConcurrencyStrategy {
    /// Pool of connections with semaphore limiting concurrent requests
    Pooled {
        /// Number of concurrent connections
        max_connections: NonZeroU16,
    },
    /// Multiple worker tasks that run independently
    Workers {
        /// Number of worker tasks
        count: NonZeroU16,
    },
}

impl ConcurrencyStrategy {
    /// Create a new concurrency strategy
    ///
    /// # Arguments
    /// * `connections` - Number of parallel connections (defaults to 1 if None)
    /// * `use_workers` - If true, use Workers strategy; otherwise use Pooled
    pub(super) fn new(connections: Option<NonZeroU16>, use_workers: bool) -> Self {
        let connections = connections.unwrap_or(NonZeroU16::MIN);
        if use_workers {
            Self::Workers { count: connections }
        } else {
            Self::Pooled {
                max_connections: connections,
            }
        }
    }

    /// Get the number of parallel connections for this strategy
    pub(super) fn connection_count(&self) -> u16 {
        match self {
            Self::Pooled { max_connections } => max_connections.get(),
            Self::Workers { count } => count.get(),
        }
    }
}

/// Builder for consistent metric labels across generators
pub(super) struct MetricsBuilder {
    labels: Vec<(String, String)>,
}

impl MetricsBuilder {
    /// Create a new metrics builder with standard component labels
    pub(super) fn new(component_name: &str) -> Self {
        Self {
            labels: vec![
                ("component".to_string(), "generator".to_string()),
                ("component_name".to_string(), component_name.to_string()),
            ],
        }
    }

    /// Add an ID label if provided
    pub(super) fn with_id(mut self, id: Option<String>) -> Self {
        if let Some(id) = id {
            self.labels.push(("id".to_string(), id));
        }
        self
    }

    /// Build the final label vector
    pub(super) fn build(self) -> Vec<(String, String)> {
        self.labels
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BytesThrottleConfig, ConcurrencyStrategy, MetricsBuilder, NonZeroU16, ThrottleBuilder,
    };
    use byte_unit::Byte;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn throttle_builder_validate(
            bytes: u32,
            worker_count in 1_u16..=100_u16
        ) {
            let bytes = Byte::from_u64(u64::from(bytes));
            let builder = ThrottleBuilder::new()
                .bytes_per_second(Some(&bytes))
                .parallel_connections(NonZeroU16::new(worker_count));
            prop_assume!(builder.valid());
            prop_assert!(builder.build().is_ok());
        }

        #[test]
        fn throttle_builder_valid_and_builds(
            bytes: u32,
            worker_count in 1_u16..=u16::MAX
        ) {
            let bytes = Byte::from_u64(u64::from(bytes));
            let worker_count = NonZeroU16::new(worker_count);
            let builder = ThrottleBuilder::new()
                .bytes_per_second(Some(&bytes))
                .parallel_connections(worker_count);
            if builder.valid() {
                prop_assert!(builder.build().is_ok());
            } else {
                prop_assert!(builder.build().is_err());
            }
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

            let builder = ThrottleBuilder::new()
                .bytes_per_second(Some(&bytes))
                .throttle_config(Some(&throttle_config));

            // This configuration should be invalid (dual config)
            prop_assert!(!builder.valid());
        }

        #[test]
        fn throttle_builder_rejects_no_config(
            worker_count in prop::option::of(1_u16..=100_u16)
        ) {
            let builder = ThrottleBuilder::new()
                .parallel_connections(worker_count.and_then(NonZeroU16::new));

            // No configuration provided should be invalid
            prop_assert!(!builder.valid());
        }

        #[test]
        fn concurrency_strategy_connection_count(
            connections in 1_u16..=100_u16,
            use_workers in any::<bool>()
        ) {
            let strategy = ConcurrencyStrategy::new(NonZeroU16::new(connections), use_workers);
            let count = strategy.connection_count();
            prop_assert_eq!(count, connections);
        }

        #[test]
        fn concurrency_strategy_workers_when_requested(connections in 1_u16..=100_u16) {
            let strategy = ConcurrencyStrategy::new(NonZeroU16::new(connections), true);
            match strategy {
                ConcurrencyStrategy::Workers { count } => prop_assert_eq!(count.get(), connections),
                _ => prop_assert!(false, "Expected Workers variant"),
            }
        }

        #[test]
        fn concurrency_strategy_pooled_when_not_workers(connections in 1_u16..=100_u16) {
            let strategy = ConcurrencyStrategy::new(NonZeroU16::new(connections), false);
            match strategy {
                ConcurrencyStrategy::Pooled { max_connections } => prop_assert_eq!(max_connections.get(), connections),
                _ => prop_assert!(false, "Expected Pooled variant"),
            }
        }

        #[test]
        fn metrics_builder_always_has_base_labels(
            component_name in "[a-z]{3,10}"
        ) {
            let labels = MetricsBuilder::new(&component_name).build();

            prop_assert!(labels.len() >= 2);
            prop_assert!(labels.contains(&("component".to_string(), "generator".to_string())));
            prop_assert!(labels.contains(&("component_name".to_string(), component_name)));
        }

        #[test]
        fn metrics_builder_id_label_optional(
            component_name in "[a-z]{3,10}",
            id in prop::option::of("[a-z0-9]{5,15}")
        ) {
            let labels = MetricsBuilder::new(&component_name)
                .with_id(id.clone())
                .build();

            if let Some(id_val) = id {
                prop_assert!(labels.contains(&("id".to_string(), id_val)));
                prop_assert_eq!(labels.len(), 3);
            } else {
                prop_assert_eq!(labels.len(), 2);
            }
        }
    }

    #[test]
    fn concurrency_strategy_defaults_to_one() {
        let workers = ConcurrencyStrategy::new(None, true);
        assert_eq!(workers.connection_count(), 1);

        let pooled = ConcurrencyStrategy::new(None, false);
        assert_eq!(pooled.connection_count(), 1);
    }
}
