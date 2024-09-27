//! The lading throttle mechanism
//!
//! This library supports throttling mechanisms for the rest of the lading
//! project.

#![deny(clippy::all)]
#![deny(clippy::cargo)]
#![deny(clippy::pedantic)]
#![deny(clippy::print_stdout)]
#![deny(clippy::print_stderr)]
#![deny(clippy::dbg_macro)]
#![deny(clippy::unwrap_used)]
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

pub mod stable;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
#[serde(deny_unknown_fields)]
/// Configuration of this generator.
#[serde(rename_all = "snake_case")]
pub enum Config {
    /// A throttle that allows the user to produce as fast as possible.
    AllOut,
    /// A throttle that attempts stable load
    Stable,
}

impl Default for Config {
    fn default() -> Self {
        Self::Stable
    }
}

/// Errors produced by [`Throttle`].
#[derive(thiserror::Error, Debug, Clone, Copy)]
pub enum Error {
    /// Stable
    #[error(transparent)]
    Stable(#[from] stable::Error),
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
    /// Function will panic if the number of ticks elapsed is greater than u64::MAX.
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
    /// Load that comes from this variant is as fast as possible with respect to
    /// the clock
    AllOut,
}

impl Throttle<RealClock> {
    /// Create a new instance of `Throttle` with a real-time clock
    #[must_use]
    pub fn new_with_config(config: Config, maximum_capacity: NonZeroU32) -> Self {
        match config {
            Config::Stable => Throttle::Stable(stable::Stable::with_clock(
                maximum_capacity,
                RealClock::default(),
            )),
            Config::AllOut => Throttle::AllOut,
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
            Throttle::AllOut => (),
        }

        Ok(())
    }
}
