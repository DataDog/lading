//! A throttle for input into the target program.

use async_trait::async_trait;
use serde::Deserialize;
use std::num::NonZeroU32;
use tokio::time::{self, Duration, Instant};

mod predictive;

#[derive(Debug, Deserialize, PartialEq, Clone, Copy)]
/// Configuration of this generator.
#[serde(rename_all = "snake_case")]
pub enum Config {
    /// Create a throttle that predicts target capability
    Predictive,
}

impl Default for Config {
    fn default() -> Self {
        Self::Predictive
    }
}

/// Errors produced by [`Throttle`].
#[derive(thiserror::Error, Debug, Clone, Copy)]
pub(crate) enum Error {
    #[error(transparent)]
    Predictive(#[from] predictive::Error),
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
}

impl Throttle<RealClock> {
    pub(crate) fn new(maximum_capacity: NonZeroU32) -> Self {
        Throttle::new_with_config(Config::Predictive, maximum_capacity)
    }

    pub(crate) fn new_with_config(config: Config, maximum_capacity: NonZeroU32) -> Self {
        match config {
            Config::Predictive => Throttle::Predictive(predictive::Predictive::with_clock(
                maximum_capacity,
                RealClock::default(),
            )),
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
        }

        Ok(())
    }

    /// Wait for `request` capacity to be available in the throttle
    #[inline]
    pub(crate) async fn wait_for(&mut self, request: NonZeroU32) -> Result<(), Error> {
        match self {
            Throttle::Predictive(inner) => inner.wait_for(request).await?,
        }

        Ok(())
    }
}
