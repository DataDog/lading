//! Common types for generators

use byte_unit::Byte;
use lading_throttle::Throttle;
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

/// Unified throttle configuration builder for generators
///
/// This builder handles the common pattern of configuring throttles from either
/// `bytes_per_second` or a `throttle` configuration, ensuring exactly one is provided.
pub(super) struct ThrottleBuilder<'a> {
    bytes_per_second: Option<&'a Byte>,
    throttle_config: Option<&'a BytesThrottleConfig>,
}

impl<'a> ThrottleBuilder<'a> {
    /// Create a new throttle builder
    pub(super) fn new() -> Self {
        Self {
            bytes_per_second: None,
            throttle_config: None,
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

    /// Build the throttle
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Both `bytes_per_second` and `throttle_config` are provided
    /// - Neither configuration is provided
    /// - The configuration values are invalid (zero or too large)
    #[allow(clippy::cast_possible_truncation)]
    pub(super) fn build(self) -> Result<Throttle, ThrottleBuilderError> {
        let config = match (self.bytes_per_second, self.throttle_config) {
            (Some(bps), None) => {
                let bytes_per_second =
                    NonZeroU32::new(bps.as_u128() as u32).ok_or(ThrottleBuilderError::Zero)?;
                lading_throttle::Config::Stable {
                    maximum_capacity: bytes_per_second,
                }
            }
            (None, Some(throttle_config)) => (*throttle_config)
                .try_into()
                .map_err(ThrottleBuilderError::Conversion)?,
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

/// Concurrency management strategies for generators
///
/// This enum represents the three main concurrency patterns used across generators:
/// - Single: One connection with reconnect on failure (original TCP/UDP pattern)
/// - Pooled: Multiple concurrent requests with semaphore limiting (HTTP/Splunk HEC pattern)  
/// - Workers: Multiple persistent worker tasks (Unix stream/datagram pattern)
#[derive(Debug)]
pub(super) enum ConcurrencyStrategy {
    /// Single connection with automatic reconnection on failure
    Single,
    /// Pool of connections with semaphore limiting concurrent requests
    Pooled {
        /// Maximum number of concurrent connections
        max_connections: u16,
    },
    /// Multiple worker tasks that run independently
    Workers {
        /// Number of worker tasks
        count: u16,
    },
}

impl ConcurrencyStrategy {
    /// Create a new concurrency strategy based on the connection count
    ///
    /// # Arguments
    /// * `connections` - Number of parallel connections (1 = Single, >1 = strategy-specific)
    /// * `use_workers` - If true and connections > 1, use Workers strategy; otherwise use Pooled
    pub(super) fn new(connections: u16, use_workers: bool) -> Self {
        match connections {
            0 | 1 => Self::Single,
            n if use_workers => Self::Workers { count: n },
            n => Self::Pooled { max_connections: n },
        }
    }

    /// Get the number of parallel connections for this strategy
    pub(super) fn connection_count(&self) -> u16 {
        match self {
            Self::Single => 1,
            Self::Pooled {
                max_connections, ..
            } => *max_connections,
            Self::Workers { count } => *count,
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

    /// Add a custom label
    #[allow(dead_code)]
    pub(super) fn with_label(mut self, key: String, value: String) -> Self {
        self.labels.push((key, value));
        self
    }

    /// Build the final label vector
    pub(super) fn build(self) -> Vec<(String, String)> {
        self.labels
    }
}
