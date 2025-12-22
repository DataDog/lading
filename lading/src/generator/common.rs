//! Common types for generators

use byte_unit::Byte;
use serde::{Deserialize, Serialize};
use std::num::{NonZeroU16, NonZeroU32};

/// Unified rate specification; defaults to bytes when `mode` is unset.
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone, Copy, Default)]
#[serde(deny_unknown_fields)]
pub struct RateSpec {
    /// Throttle mode; defaults to bytes when absent.
    #[serde(default)]
    pub mode: Option<ThrottleMode>,
    /// Bytes per second (bytes mode only).
    #[serde(default)]
    pub bytes_per_second: Option<Byte>,
    /// Blocks per second (blocks mode only).
    #[serde(default)]
    pub blocks_per_second: Option<NonZeroU32>,
}

impl RateSpec {
    fn resolve(&self) -> Result<(ThrottleMode, NonZeroU32), ThrottleConversionError> {
        let mode = self.mode.unwrap_or(ThrottleMode::Bytes);
        match mode {
            ThrottleMode::Bytes => {
                let bps = self
                    .bytes_per_second
                    .ok_or(ThrottleConversionError::MissingRate)?;
                let val = bps.as_u128();
                let val =
                    u32::try_from(val).map_err(|_| ThrottleConversionError::ValueTooLarge(bps))?;
                NonZeroU32::new(val)
                    .map(|n| (ThrottleMode::Bytes, n))
                    .ok_or(ThrottleConversionError::Zero)
            }
            ThrottleMode::Blocks => self
                .blocks_per_second
                .map(|n| (ThrottleMode::Blocks, n))
                .ok_or(ThrottleConversionError::MissingRate),
        }
    }
}

/// Generator-specific throttle configuration unified for bytes or blocks.
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum ThrottleConfig {
    /// A throttle that allows the generator to produce as fast as possible
    AllOut,
    /// A throttle that attempts stable load
    Stable {
        /// Rate specification (bytes or blocks). Defaults to bytes when mode is unset.
        #[serde(default)]
        rate: RateSpec,
        /// The timeout in milliseconds for IO operations. Default is 0.
        #[serde(default)]
        timeout_millis: u64,
    },
    /// A throttle that linearly increases load over time
    Linear {
        /// The initial rate (bytes or blocks per second)
        initial: RateSpec,
        /// The maximum rate (bytes or blocks per second)
        maximum: RateSpec,
        /// The rate of change per second (bytes or blocks per second)
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
pub enum ThrottleMode {
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

    /// Get the maximum capacity of the underlying throttle
    pub(super) fn maximum_capacity(&self) -> u32 {
        self.inner.maximum_capacity()
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
    let fallback = legacy_bytes_per_second.map(|bps| ThrottleConfig::Stable {
        rate: RateSpec {
            mode: Some(ThrottleMode::Bytes),
            bytes_per_second: Some(*bps),
            blocks_per_second: None,
        },
        timeout_millis: 0,
    });

    let cfg = config
        .copied()
        .or(fallback)
        .unwrap_or(ThrottleConfig::AllOut);
    let throttle = match cfg {
        ThrottleConfig::AllOut => {
            lading_throttle::Throttle::new_with_config(lading_throttle::Config::AllOut)
        }
        ThrottleConfig::Stable {
            rate,
            timeout_millis,
        } => {
            let (_mode, cap) = rate.resolve()?;
            lading_throttle::Throttle::new_with_config(lading_throttle::Config::Stable {
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
            if m1 != m2 || m1 != m3 {
                return Err(ThrottleConversionError::MixedModes);
            }
            lading_throttle::Throttle::new_with_config(lading_throttle::Config::Linear {
                initial_capacity: init.get(),
                maximum_capacity: max,
                rate_of_change: rate.get(),
            })
        }
    };

    let mode = match cfg {
        ThrottleConfig::AllOut => ThrottleMode::Bytes,
        ThrottleConfig::Stable { rate, .. } => rate.resolve()?.0,
        ThrottleConfig::Linear {
            initial,
            maximum,
            rate_of_change,
        } => {
            let (m1, _) = initial.resolve()?;
            let (m2, _) = maximum.resolve()?;
            let (m3, _) = rate_of_change.resolve()?;
            if m1 != m2 || m1 != m3 {
                return Err(ThrottleConversionError::MixedModes);
            }
            m1
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
}
