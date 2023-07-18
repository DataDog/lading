//! A throttle for input into the target program.
//!
//! ## Metrics
//!
//! The [predictive] and [stable] throttles emit metrics. See those modules'
//! documentation for details. The all-out throttle does not emit any metrics.
//!

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;
use tokio::time::{self, Duration, Instant};

pub mod predictive;
pub mod stable;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
/// Configuration of this generator.
#[serde(rename_all = "snake_case")]
pub enum Config {
    /// A throttle that predicts target capability
    Predictive,
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
pub(crate) enum Error {
    #[error(transparent)]
    Predictive(#[from] predictive::Error),
    #[error(transparent)]
    Stable(#[from] stable::Error),
}

#[async_trait]
pub(crate) trait Clock {
    fn ticks_elapsed(&self) -> u64;
    async fn wait(&self, ticks: u64);
}

#[derive(Debug, Clone, Copy)]
/// A clock that operates with respect to real-clock time.
pub(crate) struct RealClock {
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

#[derive(Debug)]
pub(crate) enum Throttle<C = RealClock> {
    Predictive(predictive::Predictive<C>),
    Stable(stable::Stable<C>),
    AllOut,
}

impl Throttle<RealClock> {
    pub(crate) fn new_with_config(
        config: Config,
        maximum_capacity: NonZeroU32,
        labels: Vec<(String, String)>,
    ) -> Self {
        match config {
            Config::Predictive => Throttle::Predictive(predictive::Predictive::with_clock(
                maximum_capacity,
                RealClock::default(),
                labels,
            )),
            Config::Stable => Throttle::Stable(stable::Stable::with_clock(
                maximum_capacity,
                RealClock::default(),
                labels,
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
    #[inline]
    pub(crate) async fn wait(&mut self) -> Result<(), Error> {
        match self {
            Throttle::Predictive(inner) => inner.wait().await?,
            Throttle::Stable(inner) => inner.wait().await?,
            Throttle::AllOut => (),
        }

        Ok(())
    }

    /// Wait for `request` capacity to be available in the throttle
    #[inline]
    pub(crate) async fn wait_for(&mut self, request: NonZeroU32) -> Result<(), Error> {
        match self {
            Throttle::Predictive(inner) => inner.wait_for(request).await?,
            Throttle::Stable(inner) => inner.wait_for(request).await?,
            Throttle::AllOut => (),
        }

        Ok(())
    }
}
