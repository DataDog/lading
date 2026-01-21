//! Common types for generators

use byte_unit::Byte;
use serde::{Deserialize, Deserializer, Serialize};
use std::num::{NonZeroU16, NonZeroU32};

/// Unified rate specification as a "one of" - either byte-based or block-based.
#[derive(Debug, Serialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum RateSpec {
    /// Byte-based rate specification
    Bytes {
        /// Bytes per second
        bytes_per_second: Byte,
    },
    /// Block-based rate specification
    Blocks {
        /// Blocks per second
        blocks_per_second: NonZeroU32,
    },
}

// Custom deserialize implementation to support both legacy (direct Byte) and new (RateSpec struct) formats
impl<'de> Deserialize<'de> for RateSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum RateSpecHelper {
            // New format with explicit fields
            Bytes { bytes_per_second: Byte },
            Blocks { blocks_per_second: NonZeroU32 },
            // Legacy format: direct Byte value
            LegacyByte(Byte),
        }

        let helper = RateSpecHelper::deserialize(deserializer)?;
        match helper {
            RateSpecHelper::Bytes { bytes_per_second } => Ok(RateSpec::Bytes { bytes_per_second }),
            RateSpecHelper::Blocks { blocks_per_second } => {
                Ok(RateSpec::Blocks { blocks_per_second })
            }
            RateSpecHelper::LegacyByte(bytes) => Ok(RateSpec::Bytes {
                bytes_per_second: bytes,
            }),
        }
    }
}

impl RateSpec {
    fn resolve(&self) -> Result<(ThrottleMode, NonZeroU32), ThrottleConversionError> {
        match self {
            RateSpec::Bytes { bytes_per_second } => {
                let val = bytes_per_second.as_u128();
                let val = u32::try_from(val)
                    .map_err(|_| ThrottleConversionError::ValueTooLarge(*bytes_per_second))?;
                NonZeroU32::new(val)
                    .map(|n| (ThrottleMode::Bytes, n))
                    .ok_or(ThrottleConversionError::Zero)
            }
            RateSpec::Blocks { blocks_per_second } => {
                Ok((ThrottleMode::Blocks, *blocks_per_second))
            }
        }
    }
}

/// Generator-specific throttle configuration unified for bytes or blocks.
///
/// Note: We intentionally do not use `#[serde(deny_unknown_fields)]` here because the
/// `Stable` variant uses `#[serde(flatten)]` on the `rate` field. Serde's `flatten`
/// attribute is incompatible with `deny_unknown_fields` - the flattened struct consumes
/// fields dynamically, making it impossible for serde to determine what constitutes an
/// "unknown" field during deserialization.
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum ThrottleConfig {
    /// A throttle that allows the generator to produce as fast as possible
    AllOut,
    /// A throttle that attempts stable load
    Stable {
        /// Rate specification (bytes or blocks).
        #[serde(flatten)]
        rate: RateSpec,
        /// The timeout in milliseconds for IO operations. Default is 0.
        #[serde(default)]
        timeout_millis: u64,
    },
    /// A throttle that linearly increases load over time
    Linear {
        /// The initial rate (bytes or blocks per second)
        #[serde(alias = "initial_bytes_per_second")]
        initial: RateSpec,
        /// The maximum rate (bytes or blocks per second)
        #[serde(alias = "maximum_bytes_per_second")]
        maximum: RateSpec,
        /// The rate of change (must be in same units as initial and maximum)
        rate_of_change: RateSpec,
    },
}

/// Error converting `ThrottleConfig` to internal throttle config
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
    /// Missing rate specification
    #[error("Rate must be specified for the selected throttle mode")]
    MissingRate,
    /// Mixed throttle modes in a linear profile
    #[error("All rate specs in a linear throttle must use the same mode")]
    MixedModes,
}

/// Indicates how a throttle should interpret its token units.
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub(super) enum ThrottleMode {
    /// Throttle tokens represent bytes.
    Bytes,
    /// Throttle tokens represent block counts.
    Blocks,
}

/// Wrapper around a throttle and how its tokens should be interpreted.
#[derive(Debug)]
pub(super) struct BlockThrottle {
    /// Underlying throttle instance.
    inner: lading_throttle::Throttle,
    /// Token interpretation mode.
    pub mode: ThrottleMode,
}

impl BlockThrottle {
    /// Wait for capacity for a block, interpreting tokens according to `mode`.
    pub(super) async fn wait_for_block(
        &mut self,
        block_cache: &lading_payload::block::Cache,
        handle: &lading_payload::block::Handle,
    ) -> Result<(), lading_throttle::Error> {
        let tokens: NonZeroU32 = match self.mode {
            ThrottleMode::Bytes => block_cache.peek_next_size(handle),
            ThrottleMode::Blocks => NonZeroU32::new(1).expect("non-zero"),
        };
        self.inner.wait_for(tokens).await
    }

    /// Divide the underlying throttle capacity by `n`, preserving mode.
    pub(super) fn divide(self, n: NonZeroU32) -> Result<Self, lading_throttle::Error> {
        let throttle = self.inner.divide(n)?;
        Ok(Self {
            inner: throttle,
            mode: self.mode,
        })
    }

    /// Get the maximum capacity of the underlying throttle in bytes
    pub(super) fn maximum_capacity_bytes(&self, maximum_block_size: u32) -> usize {
        match self.mode {
            ThrottleMode::Bytes => self.inner.maximum_capacity() as usize,
            ThrottleMode::Blocks => self
                .inner
                .maximum_capacity()
                .saturating_mul(maximum_block_size) as usize,
        }
    }
}

/// Create a throttle from config plus optional legacy bytes-per-second fallback.
///
/// Returns a [`BlockThrottle`] that carries both the throttle and its mode
/// (bytes vs blocks).
pub(super) fn create_throttle(
    config: Option<&ThrottleConfig>,
    legacy_bytes_per_second: Option<&byte_unit::Byte>,
) -> Result<BlockThrottle, ThrottleConversionError> {
    let config_with_fallback = match (config, legacy_bytes_per_second) {
        (Some(_), Some(_)) => {
            return Err(ThrottleConversionError::ConflictingConfig);
        }
        (Some(config), None) => *config,
        (None, Some(bytes_per_second)) => ThrottleConfig::Stable {
            rate: RateSpec::Bytes {
                bytes_per_second: *bytes_per_second,
            },
            timeout_millis: 0,
        },
        (None, None) => ThrottleConfig::AllOut,
    };

    let (throttle, mode) = match config_with_fallback {
        ThrottleConfig::AllOut => (
            lading_throttle::Throttle::new_with_config(lading_throttle::Config::AllOut),
            ThrottleMode::Bytes,
        ),
        ThrottleConfig::Stable {
            rate,
            timeout_millis,
        } => {
            let (resolved_mode, cap) = rate.resolve()?;
            (
                lading_throttle::Throttle::new_with_config(lading_throttle::Config::Stable {
                    maximum_capacity: cap,
                    timeout_micros: timeout_millis.saturating_mul(1000),
                }),
                resolved_mode,
            )
        }
        ThrottleConfig::Linear {
            initial,
            maximum,
            rate_of_change,
        } => {
            let (m1, init) = initial.resolve()?;
            let (m2, max) = maximum.resolve()?;
            let (m3, rate) = rate_of_change.resolve()?;
            if m1 != m2 || m1 != m3 {
                return Err(ThrottleConversionError::MixedModes);
            }
            (
                lading_throttle::Throttle::new_with_config(lading_throttle::Config::Linear {
                    initial_capacity: init.get(),
                    maximum_capacity: max,
                    rate_of_change: rate.get(),
                }),
                m1,
            )
        }
    };

    Ok(BlockThrottle {
        inner: throttle,
        mode,
    })
}

impl TryFrom<&ThrottleConfig> for lading_throttle::Config {
    type Error = ThrottleConversionError;

    #[allow(clippy::cast_possible_truncation)]
    fn try_from(config: &ThrottleConfig) -> Result<Self, Self::Error> {
        match config {
            ThrottleConfig::AllOut => Ok(lading_throttle::Config::AllOut),
            ThrottleConfig::Stable {
                rate,
                timeout_millis,
            } => {
                let (mode, cap) = rate.resolve()?;
                if mode != ThrottleMode::Bytes {
                    return Err(ThrottleConversionError::MixedModes);
                }
                Ok(lading_throttle::Config::Stable {
                    maximum_capacity: cap,
                    timeout_micros: timeout_millis.saturating_mul(1000),
                })
            }
            ThrottleConfig::Linear {
                initial,
                maximum,
                rate_of_change,
            } => {
                let (m1, init) = initial.resolve()?;
                let (m2, max) = maximum.resolve()?;
                let (m3, rate) = rate_of_change.resolve()?;
                if m1 != m2 || m1 != m3 || m1 != ThrottleMode::Bytes {
                    return Err(ThrottleConversionError::MixedModes);
                }
                Ok(lading_throttle::Config::Linear {
                    initial_capacity: init.get(),
                    maximum_capacity: max,
                    rate_of_change: rate.get(),
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
#[derive(Debug, Clone, Copy)]
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
    pub(super) fn connection_count(self) -> u16 {
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

    mod rate_spec_parsing {
        use crate::generator::common::RateSpec;

        #[test]
        fn parse_bytes_new_format() {
            let yaml = r#"
                bytes_per_second: "10 MiB"
            "#;
            let rate: RateSpec = serde_yaml::from_str(yaml).unwrap();
            assert!(matches!(rate, RateSpec::Bytes { .. }));
            if let RateSpec::Bytes { bytes_per_second } = rate {
                assert_eq!(bytes_per_second.as_u64(), 10 * 1024 * 1024);
            }
        }

        #[test]
        fn parse_blocks_new_format() {
            let yaml = r#"
                blocks_per_second: 1000
            "#;
            let rate: RateSpec = serde_yaml::from_str(yaml).unwrap();
            assert!(matches!(rate, RateSpec::Blocks { .. }));
            if let RateSpec::Blocks { blocks_per_second } = rate {
                assert_eq!(blocks_per_second.get(), 1000);
            }
        }

        #[test]
        fn parse_legacy_direct_byte_value() {
            let yaml = r#""500 KiB""#;
            let rate: RateSpec = serde_yaml::from_str(yaml).unwrap();
            assert!(matches!(rate, RateSpec::Bytes { .. }));
            if let RateSpec::Bytes { bytes_per_second } = rate {
                assert_eq!(bytes_per_second.as_u64(), 500 * 1024);
            }
        }

        #[test]
        fn parse_legacy_direct_byte_value_no_quotes() {
            let yaml = r#"5 MiB"#;
            let rate: RateSpec = serde_yaml::from_str(yaml).unwrap();
            assert!(matches!(rate, RateSpec::Bytes { .. }));
            if let RateSpec::Bytes { bytes_per_second } = rate {
                assert_eq!(bytes_per_second.as_u64(), 5 * 1024 * 1024);
            }
        }
    }

    mod throttle_config_parsing {
        use crate::generator::common::{RateSpec, ThrottleConfig};
        use serde_yaml::with::singleton_map_recursive;

        /// Helper to deserialize ThrottleConfig using singleton_map_recursive
        /// (matches how the main config deserializes it)
        fn parse_throttle_config(yaml: &str) -> ThrottleConfig {
            let value: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
            singleton_map_recursive::deserialize(value).unwrap()
        }

        #[test]
        fn parse_all_out() {
            let yaml = r#"all_out"#;
            let config = parse_throttle_config(yaml);
            assert!(matches!(config, ThrottleConfig::AllOut));
        }

        #[test]
        fn parse_stable_bytes_per_second() {
            let yaml = r#"
                stable:
                    bytes_per_second: "10 MiB"
                    timeout_millis: 100
            "#;
            let config = parse_throttle_config(yaml);
            assert!(matches!(config, ThrottleConfig::Stable { .. }));
            if let ThrottleConfig::Stable {
                rate,
                timeout_millis,
            } = config
            {
                assert_eq!(timeout_millis, 100);
                assert!(matches!(rate, RateSpec::Bytes { .. }));
                if let RateSpec::Bytes { bytes_per_second } = rate {
                    assert_eq!(bytes_per_second.as_u64(), 10 * 1024 * 1024);
                }
            }
        }

        #[test]
        fn parse_stable_blocks_per_second() {
            let yaml = r#"
                stable:
                    blocks_per_second: 1000
                    timeout_millis: 0
            "#;
            let config = parse_throttle_config(yaml);
            assert!(matches!(config, ThrottleConfig::Stable { .. }));
            if let ThrottleConfig::Stable {
                rate,
                timeout_millis,
            } = config
            {
                assert_eq!(timeout_millis, 0);
                assert!(matches!(rate, RateSpec::Blocks { .. }));
                if let RateSpec::Blocks { blocks_per_second } = rate {
                    assert_eq!(blocks_per_second.get(), 1000);
                }
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
            assert!(matches!(config, ThrottleConfig::Linear { .. }));
            if let ThrottleConfig::Linear {
                initial,
                maximum,
                rate_of_change,
            } = config
            {
                assert!(matches!(initial, RateSpec::Bytes { .. }));
                assert!(matches!(maximum, RateSpec::Bytes { .. }));
                assert!(matches!(rate_of_change, RateSpec::Bytes { .. }));

                if let RateSpec::Bytes { bytes_per_second } = initial {
                    assert_eq!(bytes_per_second.as_u64(), 10 * 1024 * 1024);
                }
                if let RateSpec::Bytes { bytes_per_second } = maximum {
                    assert_eq!(bytes_per_second.as_u64(), 100 * 1024 * 1024);
                }
                if let RateSpec::Bytes { bytes_per_second } = rate_of_change {
                    assert_eq!(bytes_per_second.as_u64(), 1 * 1024 * 1024);
                }
            }
        }

        #[test]
        fn parse_linear_new_format() {
            let yaml = r#"
                linear:
                    initial:
                        bytes_per_second: "1 MiB"
                    maximum:
                        bytes_per_second: "50 MiB"
                    rate_of_change:
                        bytes_per_second: "500 KiB"
            "#;
            let config = parse_throttle_config(yaml);
            assert!(matches!(config, ThrottleConfig::Linear { .. }));
            if let ThrottleConfig::Linear {
                initial,
                maximum,
                rate_of_change,
            } = config
            {
                assert!(matches!(initial, RateSpec::Bytes { .. }));
                assert!(matches!(maximum, RateSpec::Bytes { .. }));
                assert!(matches!(rate_of_change, RateSpec::Bytes { .. }));

                if let RateSpec::Bytes { bytes_per_second } = initial {
                    assert_eq!(bytes_per_second.as_u64(), 1 * 1024 * 1024);
                }
                if let RateSpec::Bytes { bytes_per_second } = maximum {
                    assert_eq!(bytes_per_second.as_u64(), 50 * 1024 * 1024);
                }
                if let RateSpec::Bytes { bytes_per_second } = rate_of_change {
                    assert_eq!(bytes_per_second.as_u64(), 500 * 1024);
                }
            }
        }

        #[test]
        fn parse_linear_new_format_flattened_bytes() {
            let yaml = r#"
                linear:
                    initial: "1 MiB"
                    maximum: "50 MiB"
                    rate_of_change: "500 KiB"
            "#;
            let config = parse_throttle_config(yaml);
            assert!(matches!(config, ThrottleConfig::Linear { .. }));
            if let ThrottleConfig::Linear {
                initial,
                maximum,
                rate_of_change,
            } = config
            {
                assert!(matches!(initial, RateSpec::Bytes { .. }));
                assert!(matches!(maximum, RateSpec::Bytes { .. }));
                assert!(matches!(rate_of_change, RateSpec::Bytes { .. }));

                if let RateSpec::Bytes { bytes_per_second } = initial {
                    assert_eq!(bytes_per_second.as_u64(), 1 * 1024 * 1024);
                }
                if let RateSpec::Bytes { bytes_per_second } = maximum {
                    assert_eq!(bytes_per_second.as_u64(), 50 * 1024 * 1024);
                }
                if let RateSpec::Bytes { bytes_per_second } = rate_of_change {
                    assert_eq!(bytes_per_second.as_u64(), 500 * 1024);
                }
            }
        }

        #[test]
        fn parse_linear_new_format_blocks() {
            let yaml = r#"
            linear:
                initial:
                    blocks_per_second: 100
                maximum:
                    blocks_per_second: 5000
                rate_of_change:
                    blocks_per_second: 50
                "#;
            let config = parse_throttle_config(yaml);
            assert!(matches!(config, ThrottleConfig::Linear { .. }));
            if let ThrottleConfig::Linear {
                initial,
                maximum,
                rate_of_change,
            } = config
            {
                assert!(matches!(initial, RateSpec::Blocks { .. }));
                assert!(matches!(maximum, RateSpec::Blocks { .. }));
                assert!(matches!(rate_of_change, RateSpec::Blocks { .. }));

                if let RateSpec::Blocks { blocks_per_second } = initial {
                    assert_eq!(blocks_per_second.get(), 100);
                }
                if let RateSpec::Blocks { blocks_per_second } = maximum {
                    assert_eq!(blocks_per_second.get(), 5000);
                }
                if let RateSpec::Blocks { blocks_per_second } = rate_of_change {
                    assert_eq!(blocks_per_second.get(), 50);
                }
            }
        }
    }

    mod rate_spec_resolve {
        use crate::generator::common::{RateSpec, ThrottleConversionError, ThrottleMode};
        use std::num::NonZeroU32;

        #[test]
        fn resolve_bytes_rate() {
            let rate = RateSpec::Bytes {
                bytes_per_second: byte_unit::Byte::from_u64(1024 * 1024),
            };
            let result = rate.resolve();
            assert!(result.is_ok());
            let (mode, capacity) = result.unwrap();
            assert_eq!(mode, ThrottleMode::Bytes);
            assert_eq!(capacity.get(), 1024 * 1024);
        }

        #[test]
        fn resolve_blocks_rate() {
            let rate = RateSpec::Blocks {
                blocks_per_second: NonZeroU32::new(1000).unwrap(),
            };
            let result = rate.resolve();
            assert!(result.is_ok());
            let (mode, capacity) = result.unwrap();
            assert_eq!(mode, ThrottleMode::Blocks);
            assert_eq!(capacity.get(), 1000);
        }

        #[test]
        fn resolve_zero_bytes_fails() {
            let rate = RateSpec::Bytes {
                bytes_per_second: byte_unit::Byte::from_u64(0),
            };
            let result = rate.resolve();
            assert!(result.is_err());
            assert!(matches!(result.unwrap_err(), ThrottleConversionError::Zero));
        }
    }

    mod create_throttle_tests {
        use crate::generator::common::{
            RateSpec, ThrottleConfig, ThrottleConversionError, ThrottleMode, create_throttle,
        };
        use std::num::NonZeroU32;
        use std::str::FromStr;

        #[test]
        fn create_throttle_with_stable_bytes() {
            let config = ThrottleConfig::Stable {
                rate: RateSpec::Bytes {
                    bytes_per_second: byte_unit::Byte::from_str("10 MiB").unwrap(),
                },
                timeout_millis: 100,
            };
            let result = create_throttle(Some(&config), None);
            assert!(result.is_ok());
            let throttle = result.unwrap();
            assert_eq!(throttle.mode, ThrottleMode::Bytes);
        }

        #[test]
        fn create_throttle_with_stable_blocks() {
            let config = ThrottleConfig::Stable {
                rate: RateSpec::Blocks {
                    blocks_per_second: NonZeroU32::new(1000).unwrap(),
                },
                timeout_millis: 0,
            };
            let result = create_throttle(Some(&config), None);
            assert!(result.is_ok());
            let throttle = result.unwrap();
            assert_eq!(throttle.mode, ThrottleMode::Blocks);
        }

        #[test]
        fn create_throttle_with_legacy_bytes_per_second() {
            let legacy = byte_unit::Byte::from_str("5 MiB").unwrap();
            let result = create_throttle(None, Some(&legacy));
            assert!(result.is_ok());
            let throttle = result.unwrap();
            assert_eq!(throttle.mode, ThrottleMode::Bytes);
        }

        #[test]
        fn create_throttle_conflicting_config() {
            let config = ThrottleConfig::Stable {
                rate: RateSpec::Bytes {
                    bytes_per_second: byte_unit::Byte::from_str("10 MiB").unwrap(),
                },
                timeout_millis: 100,
            };
            let legacy = byte_unit::Byte::from_str("5 MiB").unwrap();
            let result = create_throttle(Some(&config), Some(&legacy));
            assert!(result.is_err());
            assert!(matches!(
                result.unwrap_err(),
                ThrottleConversionError::ConflictingConfig
            ));
        }

        #[test]
        fn create_throttle_linear_mixed_modes_fails() {
            let config = ThrottleConfig::Linear {
                initial: RateSpec::Bytes {
                    bytes_per_second: byte_unit::Byte::from_str("1 MiB").unwrap(),
                },
                maximum: RateSpec::Blocks {
                    blocks_per_second: NonZeroU32::new(1000).unwrap(),
                },
                rate_of_change: RateSpec::Bytes {
                    bytes_per_second: byte_unit::Byte::from_str("100 KiB").unwrap(),
                },
            };
            let result = create_throttle(Some(&config), None);
            assert!(result.is_err());
            assert!(matches!(
                result.unwrap_err(),
                ThrottleConversionError::MixedModes
            ));
        }

        #[test]
        fn create_throttle_all_out() {
            let config = ThrottleConfig::AllOut;
            let result = create_throttle(Some(&config), None);
            assert!(result.is_ok());
            let throttle = result.unwrap();
            assert_eq!(throttle.mode, ThrottleMode::Bytes);
        }

        #[test]
        fn create_throttle_none_defaults_to_all_out() {
            let result = create_throttle(None, None);
            assert!(result.is_ok());
            let throttle = result.unwrap();
            assert_eq!(throttle.mode, ThrottleMode::Bytes);
        }
    }
}
