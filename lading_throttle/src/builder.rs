//! Throttle builder for convenient configuration

use crate::{Config, Throttle};
use std::num::NonZeroU32;

/// Builder for creating throttles
///
/// This builder provides a fluent API for creating throttles with
/// various configurations.
#[derive(Debug)]
pub struct Builder {
    config: BuilderConfig,
}

#[derive(Debug)]
enum BuilderConfig {
    Stable {
        operations_per_second: NonZeroU32,
        divisor: Option<NonZeroU32>,
    },
}

impl Builder {
    /// Create a new builder for a stable throttle
    ///
    /// # Arguments
    /// * `operations_per_second` - Total operations per second
    #[must_use]
    pub fn stable(operations_per_second: NonZeroU32) -> Self {
        Self {
            config: BuilderConfig::Stable {
                operations_per_second,
                divisor: None,
            },
        }
    }

    /// Set the divisor for parallel operations
    ///
    /// When set, the `operations_per_second` will be divided evenly
    /// among this many parallel workers.
    #[must_use]
    pub fn with_divisor(mut self, divisor: NonZeroU32) -> Self {
        match &mut self.config {
            BuilderConfig::Stable { divisor: d, .. } => *d = Some(divisor),
        }
        self
    }

    /// Build the throttle
    #[must_use]
    pub fn build(self) -> Throttle {
        match self.config {
            BuilderConfig::Stable {
                operations_per_second,
                divisor,
            } => {
                let ops = if let Some(divisor) = divisor {
                    let total = operations_per_second.get();
                    let divided = total / divisor.get();
                    NonZeroU32::new(divided).unwrap_or(NonZeroU32::MIN)
                } else {
                    operations_per_second
                };

                let config = Config::Stable {
                    maximum_capacity: ops,
                    timeout_micros: 0,
                };

                Throttle::new_with_config(config)
            }
        }
    }
}
