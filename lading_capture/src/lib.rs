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
async fn send_metric(metric: Metric) -> Result<(), Error> {
    let sender = HISTORICAL_SENDER.lock().await;
    let start = sender.as_ref().ok_or(Error::NotInitialized)?.start;
    let tick_offset = Instant::now().duration_since(start).as_secs();
    let tick_offset = u8::try_from(tick_offset)?;

    let metric = match metric {
        Metric::Counter(c) => Metric::Counter(Counter { tick_offset, ..c }),
        Metric::Gauge(g) => Metric::Gauge(Gauge { tick_offset, ..g }),
    };

    debug!(tick = ?tick_offset, "Sending historical metric with tick_offset");

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
pub async fn counter_incr(key: Key, value: u64) -> Result<(), Error> {
    send_metric(Metric::Counter(Counter {
        key,
        tick_offset: 0,
        value: CounterValue::Increment(value),
    }))
    .await
}

/// Send a historical counter absolute set
///
/// # Errors
///
/// Returns error if sender not initialized or channel is closed.
pub async fn counter_absolute(key: Key, value: u64) -> Result<(), Error> {
    send_metric(Metric::Counter(Counter {
        key,
        tick_offset: 0,
        value: CounterValue::Absolute(value),
    }))
    .await
}

/// Send a historical gauge increment
///
/// # Errors
///
/// Returns error if sender not initialized or channel is closed.
pub async fn gauge_increment(key: Key, value: f64) -> Result<(), Error> {
    send_metric(Metric::Gauge(Gauge {
        key,
        tick_offset: 0,
        value: GaugeValue::Increment(value),
    }))
    .await
}

/// Send a historical gauge decrement
///
/// # Errors
///
/// Returns error if sender not initialized or channel is closed.
pub async fn gauge_decrement(key: Key, value: f64) -> Result<(), Error> {
    send_metric(Metric::Gauge(Gauge {
        key,
        tick_offset: 0,
        value: GaugeValue::Decrement(value),
    }))
    .await
}

/// Send a historical gauge set
///
/// # Errors
///
/// Returns error if sender not initialized or channel is closed.
pub async fn gauge_set(key: Key, value: f64) -> Result<(), Error> {
    send_metric(Metric::Gauge(Gauge {
        key,
        tick_offset: 0,
        value: GaugeValue::Set(value),
    }))
    .await
}
