//! Crate regarding Lading's 'capture' files

use std::num::TryFromIntError;
use std::time::Instant;

use manager::{Counter, CounterValue, Gauge, GaugeValue, HISTORICAL_SENDER, Metric};
use metrics::Key;
use tracing::debug;

mod accumulator;
pub mod json;
pub mod manager;

/// Errors for historical write operations
#[derive(thiserror::Error, Debug, Copy, Clone)]
pub enum Error {
    /// Tick offset conversion failed
    #[error("Tick offset conversion failed")]
    InvalidTickOffset(#[from] TryFromIntError),
    /// [`CaptureManager`] not initialized
    #[error("CaptureManager not initialized, cannot send historical writes")]
    NotInitialized,
    /// Channel closed
    #[error("Historical write channel closed")]
    ChannelClosed,
}

/// Send a historical metric to the capture manager.
///
/// The tick offset is calculated from elapsed time since capture start.
/// Validation happens in the accumulator; metrics exceeding the historical window
/// will be rejected there.
async fn send_metric(metric: Metric, timestamp: Instant) -> Result<(), Error> {
    let sender = HISTORICAL_SENDER.lock().await;

    // TODO the naming conventions here are all wrong. We are not calculating
    // the tick_offset we are calculating the absolute `tick`, that is, we are
    // mapping real time into the logical time of the CaptureManager. It's then
    // up to the CaptureManager to figure out what the right offset is, whether
    // in the past or into the future.
    let tick = timestamp
        .duration_since(sender.as_ref().ok_or(Error::NotInitialized)?.start)
        .as_secs();
    let metric = match metric {
        Metric::Counter(c) => Metric::Counter(Counter { tick, ..c }),
        Metric::Gauge(g) => Metric::Gauge(Gauge { tick, ..g }),
    };

    debug!(tick = ?tick, timestamp = ?timestamp, metric = ?metric, "sending historical metric");

    sender
        .as_ref()
        .ok_or(Error::NotInitialized)?
        .snd
        .send(metric)
        .await
        .map_err(|_| Error::ChannelClosed)
}

/// Send a historical counter increment
///
/// # Errors
///
/// Returns error if sender not initialized or channel is closed.
pub async fn counter_incr(key: Key, value: u64, timestamp: Instant) -> Result<(), Error> {
    send_metric(
        Metric::Counter(Counter {
            key,
            tick: 0,
            value: CounterValue::Increment(value),
        }),
        timestamp,
    )
    .await
}

/// Send a historical counter absolute set
///
/// # Errors
///
/// Returns error if sender not initialized or channel is closed.
pub async fn counter_absolute(key: Key, value: u64, timestamp: Instant) -> Result<(), Error> {
    send_metric(
        Metric::Counter(Counter {
            key,
            tick: Instant::now().duration_since(timestamp).as_secs(),
            value: CounterValue::Absolute(value),
        }),
        timestamp,
    )
    .await
}

/// Send a historical gauge increment
///
/// # Errors
///
/// Returns error if sender not initialized or channel is closed.
pub async fn gauge_increment(key: Key, value: f64, timestamp: Instant) -> Result<(), Error> {
    send_metric(
        Metric::Gauge(Gauge {
            key,
            tick: Instant::now().duration_since(timestamp).as_secs(),
            value: GaugeValue::Increment(value),
        }),
        timestamp,
    )
    .await
}

/// Send a historical gauge decrement
///
/// # Errors
///
/// Returns error if sender not initialized or channel is closed.
pub async fn gauge_decrement(key: Key, value: f64, timestamp: Instant) -> Result<(), Error> {
    send_metric(
        Metric::Gauge(Gauge {
            key,
            tick: Instant::now().duration_since(timestamp).as_secs(),
            value: GaugeValue::Decrement(value),
        }),
        timestamp,
    )
    .await
}

/// Send a historical gauge set
///
/// # Errors
///
/// Returns error if sender not initialized or channel is closed.
pub async fn gauge_set(key: Key, value: f64, timestamp: Instant) -> Result<(), Error> {
    send_metric(
        Metric::Gauge(Gauge {
            key,
            tick: Instant::now().duration_since(timestamp).as_secs(),
            value: GaugeValue::Set(value),
        }),
        timestamp,
    )
    .await
}
