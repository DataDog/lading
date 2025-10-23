//! Crate regarding Lading's 'capture' files

use std::time::Instant;

use manager::{Counter, CounterValue, Gauge, GaugeValue, HISTORICAL_SENDER, Metric};
use metrics::Key;

mod accumulator;
pub mod json;
pub mod manager;

/// Errors for historical write operations
#[derive(thiserror::Error, Debug, Copy, Clone)]
pub enum Error {
    /// [`CaptureManager`] not initialized
    #[error("CaptureManager not initialized, cannot send historical writes")]
    NotInitialized,
    /// Channel closed
    #[error("Historical write channel closed")]
    ChannelClosed,
}

/// Send a historical metric to the capture manager.
async fn send_metric(metric: Metric, timestamp: Instant) -> Result<(), Error> {
    let sender = HISTORICAL_SENDER.lock().await;

    let metric = match metric {
        Metric::Counter(c) => Metric::Counter(Counter { timestamp, ..c }),
        Metric::Gauge(g) => Metric::Gauge(Gauge { timestamp, ..g }),
    };

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
            timestamp,
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
            timestamp,
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
            timestamp,
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
            timestamp,
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
            timestamp,
            value: GaugeValue::Set(value),
        }),
        timestamp,
    )
    .await
}
