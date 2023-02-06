//! A throttle for input into the target program.

use std::num::NonZeroU32;

use governor::{
    clock,
    state::{self, direct},
    Quota, RateLimiter,
};
use tracing::info;

use crate::target;

/// Errors produced by [`Throttle`].
#[derive(thiserror::Error, Debug)]
pub(crate) enum Error {
    /// Requested capacity is greater than maximum allowed capacity.
    #[error("Capacity")]
    Capacity,
}

#[derive(Debug)]
pub(crate) struct Throttle {
    maximum_capacity: NonZeroU32,
    rate_limiter: RateLimiter<direct::NotKeyed, state::InMemoryState, clock::QuantaClock>,
}

impl Throttle {
    pub(crate) fn new(maximum_capacity: NonZeroU32) -> Self {
        Self {
            maximum_capacity,
            rate_limiter: RateLimiter::direct(Quota::per_second(maximum_capacity)),
        }
    }

    pub(crate) async fn wait(&self) -> Result<(), Error> {
        loop {
            if target::Meta::rss_bytes_limit_exceeded() {
                info!("RSS byte limit exceeded, backing off...");
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                break;
            }
        }
        self.rate_limiter.until_ready().await;
        Ok(())
    }

    pub(crate) async fn wait_for(&self, capacity: NonZeroU32) -> Result<(), Error> {
        loop {
            if target::Meta::rss_bytes_limit_exceeded() {
                info!("RSS byte limit exceeded, backing off...");
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            } else {
                break;
            }
        }
        if capacity > self.maximum_capacity {
            Err(Error::Capacity)
        } else {
            self.rate_limiter.until_n_ready(capacity).await.unwrap();
            Ok(())
        }
    }
}
