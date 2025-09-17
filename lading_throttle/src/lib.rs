//! The lading throttle mechanism
//!
//! This library supports throttling mechanisms for the rest of the lading
//! project.

#![deny(clippy::all)]
#![deny(clippy::cargo)]
#![deny(clippy::perf)]
#![deny(clippy::suspicious)]
#![deny(clippy::complexity)]
#![deny(clippy::unnecessary_to_owned)]
#![deny(clippy::manual_memcpy)]
#![deny(clippy::float_cmp)]
#![deny(clippy::large_stack_arrays)]
#![deny(clippy::large_futures)]
#![deny(clippy::rc_buffer)]
#![deny(clippy::redundant_allocation)]
#![deny(unused_extern_crates)]
#![deny(unused_allocation)]
#![deny(unused_assignments)]
#![deny(unused_comparisons)]
#![deny(unreachable_pub)]
#![deny(missing_docs)]
#![deny(missing_copy_implementations)]
#![deny(missing_debug_implementations)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::multiple_crate_versions)]

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;
use tokio::time::{self, Duration, Instant};

pub mod linear;
pub mod stable;

// An 'interval' is the period in which all counters reset. The throttle makes
// no claims on units, but consider if a user intends to produce 1Mb/s the
// 'interval' is one second and each tick corresponds to one microsecond. Each
// microsecond accumulates 1 byte.
const INTERVAL_TICKS: u64 = 1_000_000;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
#[serde(deny_unknown_fields)]
/// Configuration of the lading throttle mechanism.
#[serde(rename_all = "snake_case")]
pub enum Config {
    /// A throttle that allows the user to produce as fast as possible.
    AllOut,
    /// A throttle that attempts stable load
    Stable {
        /// The maximum capacity of the throttle, beyond which no interval will
        /// grow greater.
        maximum_capacity: NonZeroU32,
        /// The timeout in microseconds for rolled capacity. When 0 (default),
        /// no capacity rolls over between intervals.
        #[serde(default)]
        timeout_micros: u64,
    },
    /// A throttle that linearly increases load from `initial_capacity` to
    /// `maximum_capacity` with an increase of `rate_of_change` per tick.
    Linear {
        /// The initial capacity of the linear throttle, the lowest capacity at
        /// interval tick 0.
        initial_capacity: u32,
        /// The maximum capacity of the throttle, beyond which no interval will
        /// grow greater.
        maximum_capacity: NonZeroU32,
        /// The rate of change per interval tick.
        rate_of_change: u32,
    },
}

/// Errors produced by [`Throttle`].
#[derive(thiserror::Error, Debug, Clone, Copy)]
pub enum Error {
    /// Stable
    #[error(transparent)]
    Stable(#[from] stable::Error),
    /// Linear
    #[error(transparent)]
    Linear(#[from] linear::Error),
    /// Division would result in zero capacity
    #[error("Division would result in zero capacity")]
    DivisionByZero,
}

#[async_trait]
/// The `Clock` used for every throttle
pub trait Clock {
    /// The number of ticks elapsed since last queried
    fn ticks_elapsed(&self) -> u64;
    /// Wait for `ticks` amount of time
    async fn wait(&self, ticks: u64);
}

#[derive(Debug, Clone, Copy)]
/// A clock that operates with respect to real-clock time.
pub struct RealClock {
    start: Instant,
}

impl Default for RealClock {
    fn default() -> Self {
        Self {
            start: Instant::now(),
        }
    }
}

#[async_trait]
impl Clock for RealClock {
    /// Return the number of ticks since `Clock` was created.
    ///
    /// # Panics
    ///
    /// Function will panic if the number of ticks elapsed is greater than `u64::MAX`.
    #[allow(clippy::cast_possible_truncation)]
    fn ticks_elapsed(&self) -> u64 {
        let now = Instant::now();
        let ticks_since: u128 = now.duration_since(self.start).as_micros();
        assert!(
            ticks_since <= u128::from(u64::MAX),
            "584,554 years elapsed since last call!"
        );
        ticks_since as u64
    }

    async fn wait(&self, ticks: u64) {
        time::sleep(Duration::from_micros(ticks)).await;
    }
}

/// The throttle mechanism
#[derive(Debug)]
pub enum Throttle<C = RealClock> {
    /// Load that comes from this variant is stable with respect to the clock
    Stable(stable::Stable<C>),
    /// Load that comes from this variant is stable with respect to the clock
    Linear(linear::Linear<C>),
    /// Load that comes from this variant is as fast as possible with respect to
    /// the clock
    AllOut,
}

impl Throttle<RealClock> {
    /// Create a new instance of `Throttle` with a real-time clock
    #[must_use]
    pub fn new_with_config(config: Config) -> Self {
        match config {
            Config::Stable {
                maximum_capacity,
                timeout_micros,
            } => Throttle::Stable(stable::Stable::with_clock(
                maximum_capacity,
                timeout_micros,
                RealClock::default(),
            )),
            Config::Linear {
                initial_capacity,
                maximum_capacity,
                rate_of_change,
            } => Throttle::Linear(linear::Linear::with_clock(
                initial_capacity,
                maximum_capacity,
                rate_of_change,
                RealClock::default(),
            )),
            Config::AllOut => Throttle::AllOut,
        }
    }
}

impl Throttle<RealClock> {
    /// Create a new throttle with capacity divided by the divisor
    ///
    /// This is useful when distributing a throttle across multiple workers.
    /// For `AllOut` throttles, returns self as they have unlimited capacity.
    ///
    /// # Errors
    ///
    /// Returns an error if the division would result in zero capacity.
    #[allow(clippy::cast_possible_truncation)]
    pub fn divide(self, divisor: NonZeroU32) -> Result<Self, Error> {
        let divisor = divisor.get();

        match self {
            Throttle::AllOut => Ok(Throttle::AllOut),
            Throttle::Stable(inner) => {
                let capacity = inner.maximum_capacity();
                let divided = capacity / divisor;
                let divided = NonZeroU32::new(divided).ok_or(Error::DivisionByZero)?;
                Ok(Throttle::new_with_config(Config::Stable {
                    maximum_capacity: divided,
                    timeout_micros: inner.timeout_micros(),
                }))
            }
            Throttle::Linear(inner) => {
                let initial = inner.initial_capacity();
                let max_capacity = inner.maximum_capacity();
                let rate = inner.rate_of_change();
                // Divide capacities by divisor so that N workers sum to original capacity
                let divided_initial = initial / divisor;
                let divided_max = max_capacity / divisor;
                let divided_max = NonZeroU32::new(divided_max).ok_or(Error::DivisionByZero)?;
                // Rate of change should be preserved - each worker grows at the same rate
                // This way all workers reach their max capacity at the same time as the original
                Ok(Throttle::new_with_config(Config::Linear {
                    initial_capacity: divided_initial,
                    maximum_capacity: divided_max,
                    rate_of_change: rate,
                }))
            }
        }
    }
}

impl<C> Throttle<C>
where
    C: Clock + Sync + Send,
{
    /// Wait for a single unit of capacity to be available, equivalent to
    /// `wait_for` of 1.
    ///
    /// # Errors
    ///
    /// See documentation in `Error`
    #[inline]
    pub async fn wait(&mut self) -> Result<(), Error> {
        match self {
            Throttle::Stable(inner) => inner.wait().await?,
            Throttle::Linear(inner) => inner.wait().await?,
            Throttle::AllOut => (),
        }

        Ok(())
    }

    /// Wait for `request` capacity to be available in the throttle
    ///
    /// # Errors
    ///
    /// See documentation in `Error`
    #[inline]
    pub async fn wait_for(&mut self, request: NonZeroU32) -> Result<(), Error> {
        match self {
            Throttle::Stable(inner) => inner.wait_for(request).await?,
            Throttle::Linear(inner) => inner.wait_for(request).await?,
            Throttle::AllOut => (),
        }

        Ok(())
    }

    /// Get the maximum capacity of this throttle in bytes. For `AllOut`
    /// throttles, returns 100MiB. This is a guess, big but not too big.
    pub fn maximum_capacity(&self) -> u32 {
        match self {
            Throttle::Stable(inner) => inner.maximum_capacity(),
            Throttle::Linear(inner) => inner.maximum_capacity(),
            Throttle::AllOut => 100 * 1024 * 1024, // 100MiB, a guess, big but not too big
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn divide_preserves_throttle_type_allout(divisor in 1u32..=100u32) {
            let divisor = NonZeroU32::new(divisor).expect("test divisor");
            let throttle = Throttle::<RealClock>::new_with_config(Config::AllOut);
            let divided = throttle.divide(divisor);
            assert!(divided.is_ok());
            assert!(matches!(divided.unwrap(), Throttle::AllOut));
        }

        #[test]
        fn divide_preserves_throttle_type_stable(
            capacity in 100u32..=1_000_000u32,
            timeout in 0u64..=10_000u64,
            divisor in 1u32..=100u32
        ) {
            let capacity = NonZeroU32::new(capacity).expect("test capacity");
            let divisor = NonZeroU32::new(divisor).expect("test divisor");

            let throttle = Throttle::<RealClock>::new_with_config(Config::Stable {
                maximum_capacity: capacity,
                timeout_micros: timeout,
            });

            let divided = throttle.divide(divisor);
            assert!(divided.is_ok());
            assert!(matches!(divided.unwrap(), Throttle::Stable(_)));
        }

        #[test]
        fn divide_preserves_throttle_type_linear(
            initial in 0u32..=100u32,
            max_capacity in 100u32..=1_000_000u32,
            rate in 1u32..=100u32,
            divisor in 1u32..=100u32
        ) {
            let max_capacity = NonZeroU32::new(max_capacity).expect("test capacity");
            let divisor = NonZeroU32::new(divisor).expect("test divisor");

            let throttle = Throttle::<RealClock>::new_with_config(Config::Linear {
                initial_capacity: initial,
                maximum_capacity: max_capacity,
                rate_of_change: rate,
            });

            let divided = throttle.divide(divisor);
            assert!(divided.is_ok());
            assert!(matches!(divided.unwrap(), Throttle::Linear(_)));
        }

        #[test]
        fn divide_stable_preserves_properties(
            capacity in 100u32..=1_000_000u32,
            timeout in 0u64..=10_000u64,
            divisor in 1u32..=100u32
        ) {
            let capacity = NonZeroU32::new(capacity).expect("test capacity");
            let divisor = NonZeroU32::new(divisor).expect("test divisor");

            let throttle = Throttle::<RealClock>::new_with_config(Config::Stable {
                maximum_capacity: capacity,
                timeout_micros: timeout,
            });

            let divided = throttle.divide(divisor);
            assert!(divided.is_ok());

            let divided_throttle = divided.unwrap();
            let expected_capacity = capacity.get() / divisor.get();
            assert_eq!(divided_throttle.maximum_capacity(), expected_capacity);

            // Verify timeout is preserved
            match divided_throttle {
                Throttle::Stable(inner) => {
                    assert_eq!(inner.timeout_micros(), timeout);
                }
                _ => panic!("Expected Stable throttle"),
            }
        }

        #[test]
        fn divide_linear_preserves_additive_capacity(
            initial in 0u32..=500u32,
            max_capacity in 500u32..=100_000u32,
            rate in 1u32..=100u32,
            divisor in 1u32..=10u32
        ) {
            let max_capacity = NonZeroU32::new(max_capacity).expect("test capacity");
            let divisor_nz = NonZeroU32::new(divisor).expect("test divisor");

            let throttle = Throttle::<RealClock>::new_with_config(Config::Linear {
                initial_capacity: initial,
                maximum_capacity: max_capacity,
                rate_of_change: rate,
            });

            let divided = throttle.divide(divisor_nz);
            assert!(divided.is_ok());

            let divided_throttle = divided.unwrap();

            // Verify it's still Linear with divided capacities but preserved rate
            match divided_throttle {
                Throttle::Linear(inner) => {
                    assert_eq!(inner.initial_capacity(), initial / divisor);
                    assert_eq!(inner.maximum_capacity(), max_capacity.get() / divisor);
                    assert_eq!(inner.rate_of_change(), rate); // Rate preserved, not divided
                }
                _ => panic!("Expected Linear throttle after division"),
            }
        }

        #[test]
        fn divide_returns_error_when_result_is_zero(
            capacity in 1u32..=10u32,
            divisor in 100u32..=1000u32
        ) {
            let capacity = NonZeroU32::new(capacity).expect("test capacity");
            let divisor = NonZeroU32::new(divisor).expect("test divisor");

            let throttle = Throttle::<RealClock>::new_with_config(Config::Stable {
                maximum_capacity: capacity,
                timeout_micros: 0,
            });

            let divided = throttle.divide(divisor);
            assert!(divided.is_err());
            assert!(matches!(divided.unwrap_err(), Error::DivisionByZero));
        }
    }

    #[test]
    fn divide_stable_preserves_timeout() {
        let timeout_micros = 5000;
        let throttle = Throttle::<RealClock>::new_with_config(Config::Stable {
            maximum_capacity: NonZeroU32::new(1000).expect("test capacity"),
            timeout_micros,
        });

        let divided = throttle
            .divide(NonZeroU32::new(2).expect("divisor"))
            .expect("divide");

        // Verify the divided throttle is Stable with preserved timeout
        match divided {
            Throttle::Stable(inner) => {
                assert_eq!(inner.timeout_micros(), timeout_micros);
                assert_eq!(inner.maximum_capacity(), 500);
            }
            _ => panic!("Expected Stable throttle after division"),
        }
    }

    #[test]
    fn divide_linear_preserves_rate_divides_capacity() {
        let initial = 100;
        let rate = 10;
        let throttle = Throttle::<RealClock>::new_with_config(Config::Linear {
            initial_capacity: initial,
            maximum_capacity: NonZeroU32::new(1000).expect("test capacity"),
            rate_of_change: rate,
        });

        let divisor = 2;
        let divided = throttle
            .divide(NonZeroU32::new(divisor).expect("divisor"))
            .expect("divide");

        // Verify the divided throttle has divided capacities but preserved rate
        match divided {
            Throttle::Linear(inner) => {
                assert_eq!(inner.initial_capacity(), initial / divisor);
                assert_eq!(inner.maximum_capacity(), 1000 / divisor);
                assert_eq!(inner.rate_of_change(), rate); // Rate is preserved
            }
            _ => panic!("Expected Linear throttle after division"),
        }
    }

    #[test]
    fn divide_ensures_additive_property() {
        // Test that N divided throttles sum to the original capacity. This
        // doesn't _quite_ work out if the division is not equal, so we make
        // sure the division is equal here.
        let original_capacity = 1000;
        let divisor = 4;

        let throttle = Throttle::<RealClock>::new_with_config(Config::Stable {
            maximum_capacity: NonZeroU32::new(original_capacity).expect("capacity"),
            timeout_micros: 100,
        });

        let divided = throttle
            .divide(NonZeroU32::new(divisor).expect("divisor"))
            .expect("divide");

        // Each divided throttle should have 1/N of the capacity
        let divided_capacity = divided.maximum_capacity();
        assert_eq!(divided_capacity * divisor, original_capacity);
    }
}
