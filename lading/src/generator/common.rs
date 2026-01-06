//! Common types for generators

use byte_unit::Byte;
use serde::{Deserialize, Serialize};
use std::num::{NonZeroU16, NonZeroU32};

/// Generator-specific throttle configuration with field names that are specific
/// to byte-oriented generators.
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
    /// Conflicting configuration provided
    #[error("Cannot specify both throttle config and bytes_per_second")]
    ConflictingConfig,
}

/// Create a throttle from optional config and `bytes_per_second` fallback
///
/// This function implements the standard throttle creation logic for
/// byte-oriented generators. It handles the interaction between the new
/// `BytesThrottleConfig` and the legacy `bytes_per_second` field.
///
/// # Decision Logic
///
/// | `BytesThrottleConfig` | `bytes_per_second` | Result |
/// |---------------------|------------------|--------|
/// | Some(config)        | Some(bps)        | Error - Conflicting configuration |
/// | Some(config)        | None             | Use `BytesThrottleConfig` |
/// | None                | Some(bps)        | Create Stable throttle with `timeout_micros`: 0 |
/// | None                | None             | `AllOut` throttle (no rate limiting) |
///
/// # Errors
///
/// Returns an error if:
/// - Both config and `bytes_per_second` are provided (conflicting configuration)
/// - The `bytes_per_second` value exceeds `u32::MAX`
/// - The `bytes_per_second` value is zero
pub(super) fn create_throttle(
    config: Option<&BytesThrottleConfig>,
    bytes_per_second: Option<&byte_unit::Byte>,
) -> Result<lading_throttle::Throttle, ThrottleConversionError> {
    let throttle_config = match (config, bytes_per_second) {
        (Some(_), Some(_)) => {
            return Err(ThrottleConversionError::ConflictingConfig);
        }
        (Some(tc), None) => tc.try_into()?,
        (None, Some(bps)) => {
            let bps_value = bps.as_u128();
            if bps_value > u128::from(u32::MAX) {
                return Err(ThrottleConversionError::ValueTooLarge(*bps));
            }
            #[allow(clippy::cast_possible_truncation)]
            let bps_u32 = NonZeroU32::new(bps_value as u32).ok_or(ThrottleConversionError::Zero)?;
            lading_throttle::Config::Stable {
                maximum_capacity: bps_u32,
                timeout_micros: 0,
            }
        }
        (None, None) => lading_throttle::Config::AllOut,
    };
    Ok(lading_throttle::Throttle::new_with_config(throttle_config))
}

impl TryFrom<&BytesThrottleConfig> for lading_throttle::Config {
    type Error = ThrottleConversionError;

    #[allow(clippy::cast_possible_truncation)]
    fn try_from(config: &BytesThrottleConfig) -> Result<Self, Self::Error> {
        match config {
            BytesThrottleConfig::AllOut => Ok(lading_throttle::Config::AllOut),
            BytesThrottleConfig::Stable {
                bytes_per_second,
                timeout_millis,
            } => {
                let value = bytes_per_second.as_u128();
                if value > u128::from(u32::MAX) {
                    return Err(ThrottleConversionError::ValueTooLarge(*bytes_per_second));
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
                        *initial_bytes_per_second,
                    ));
                }
                if maximum > u128::from(u32::MAX) {
                    return Err(ThrottleConversionError::ValueTooLarge(
                        *maximum_bytes_per_second,
                    ));
                }
                if rate > u128::from(u32::MAX) {
                    return Err(ThrottleConversionError::ValueTooLarge(*rate_of_change));
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

/// Concurrency management strategies for generators
///
/// This enum represents the two main concurrency patterns used across
/// generators:
///
/// - Pooled: Multiple concurrent requests with semaphore limiting (HTTP/Splunk
///   HEC pattern)
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
    use super::{ConcurrencyStrategy, MetricsBuilder, NonZeroU16};
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn concurrency_strategy_connection_count(
            connections in 1_u16..=100_u16,
            use_workers in any::<bool>()
        ) {
            let strategy = ConcurrencyStrategy::new(NonZeroU16::new(connections), use_workers);
            let count = strategy.connection_count();
            assert_eq!(count, connections);
        }

        #[test]
        fn concurrency_strategy_workers_when_requested(connections in 1_u16..=100_u16) {
            let strategy = ConcurrencyStrategy::new(NonZeroU16::new(connections), true);
            assert!(matches!(strategy, ConcurrencyStrategy::Workers { count } if count.get() == connections));
        }

        #[test]
        fn concurrency_strategy_pooled_when_not_workers(connections in 1_u16..=100_u16) {
            let strategy = ConcurrencyStrategy::new(NonZeroU16::new(connections), false);
            assert!(matches!(strategy, ConcurrencyStrategy::Pooled { max_connections } if max_connections.get() == connections));
        }

        #[test]
        fn metrics_builder_always_has_base_labels(
            component_name in "[a-z]{3,10}"
        ) {
            let labels = MetricsBuilder::new(&component_name).build();

            assert!(labels.len() >= 2);
            assert!(labels.contains(&("component".to_string(), "generator".to_string())));
            assert!(labels.contains(&("component_name".to_string(), component_name)));
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
                assert!(labels.contains(&("id".to_string(), id_val)));
                assert_eq!(labels.len(), 3);
            } else {
                assert_eq!(labels.len(), 2);
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

    mod throttle_config_parsing {
        use crate::generator::common::BytesThrottleConfig;
        use serde_yaml::with::singleton_map_recursive;

        /// Helper to deserialize ThrottleConfig using singleton_map_recursive
        /// (matches how the main config deserializes it)
        fn parse_throttle_config(yaml: &str) -> BytesThrottleConfig {
            let value: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
            singleton_map_recursive::deserialize(value).unwrap()
        }

        #[test]
        fn parse_all_out() {
            let yaml = r#"all_out"#;
            let config = parse_throttle_config(yaml);
            assert!(matches!(config, BytesThrottleConfig::AllOut));
        }

        #[test]
        fn parse_stable_bytes_per_second() {
            let yaml = r#"
                stable:
                    bytes_per_second: "10 MiB"
                    timeout_millis: 100
            "#;
            let config = parse_throttle_config(yaml);
            assert!(matches!(config, BytesThrottleConfig::Stable { .. }));
            if let BytesThrottleConfig::Stable {
                bytes_per_second,
                timeout_millis,
            } = config
            {
                assert_eq!(timeout_millis, 100);
                assert_eq!(bytes_per_second.as_u64(), 10 * 1024 * 1024);
            }
        }

        #[test]
        fn parse_linear_bytes_per_second() {
            let yaml = r#"
                linear:
                    initial_bytes_per_second: "10 MiB"
                    maximum_bytes_per_second: "100 MiB"
                    rate_of_change: "1 MiB"
            "#;
            let config = parse_throttle_config(yaml);
            assert!(matches!(config, BytesThrottleConfig::Linear { .. }));
            if let BytesThrottleConfig::Linear {
                initial_bytes_per_second,
                maximum_bytes_per_second,
                rate_of_change,
            } = config
            {
                assert_eq!(initial_bytes_per_second.as_u64(), 10 * 1024 * 1024);
                assert_eq!(maximum_bytes_per_second.as_u64(), 100 * 1024 * 1024);
                assert_eq!(rate_of_change.as_u64(), 1 * 1024 * 1024);
            }
        }
    }
}
