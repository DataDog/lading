//! Crate regarding Lading's 'capture' files

use std::time::Instant;
use std::{num::TryFromIntError, time::Duration};

use manager::HISTORICAL_SENDER;
use metric::{Counter, CounterValue, Gauge, GaugeValue, Metric};
use ustr::Ustr;

mod accumulator;
pub mod json;
pub mod manager;
pub(crate) mod metric;
pub mod validate;

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

fn make_key(name: &str, labels: &[(&str, &str)]) -> metrics::Key {
    let name_static: &'static str = Ustr::from(name).as_str();
    let metric_labels: Vec<metrics::Label> = labels
        .iter()
        .map(|(k, v)| {
            let k_static: &'static str = Ustr::from(k).as_str();
            let v_static: &'static str = Ustr::from(v).as_str();
            metrics::Label::new(k_static, v_static)
        })
        .collect();
    metrics::Key::from_parts(name_static, metric_labels)
}

/// Send a historical metric to the capture manager.
#[inline]
async fn send_metric(metric: Metric) -> Result<(), Error> {
    let sender = HISTORICAL_SENDER.lock().await;

    // Calculate how many seconds ago this metric occurred, guarding against
    // timestamps that are both too old and too from the future. Any bit of
    // "from the future" is too much.
    let now = Instant::now();
    if timestamp > now {
        warn!(metric = ?metric,
              timestamp = ?timestamp,
              now = ?now,
              max_valid = ?manager::max_valid(),
              "historical metric from the future, cannot record");
        return Ok(());
    }
    let timestamp_in_past: Duration = now.duration_since(timestamp);
    if timestamp_in_past > manager::max_valid() {
        warn!(metric = ?metric,
              timestamp = ?timestamp,
              now = ?now,
              max_valid = ?manager::max_valid(),
              "historical metric too old, cannot record");
        return Ok(());
    }

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
pub async fn counter_incr(
    name: &str,
    labels: &[(&str, &str)],
    value: u64,
    timestamp: Instant,
) -> Result<(), Error> {
    let key = make_key(name, labels);
    send_metric(Metric::Counter(Counter {
        key,
        timestamp,
        value: CounterValue::Increment(value),
    }))
    .await
}

/// Send a historical counter absolute set
///
/// # Errors
///
/// Returns error if sender not initialized or channel is closed.
pub async fn counter_absolute(
    name: &str,
    labels: &[(&str, &str)],
    value: u64,
    timestamp: Instant,
) -> Result<(), Error> {
    let key = make_key(name, labels);
    send_metric(Metric::Counter(Counter {
        key,
        timestamp,
        value: CounterValue::Absolute(value),
    }))
    .await
}

/// Send a historical gauge increment
///
/// # Errors
///
/// Returns error if sender not initialized or channel is closed.
pub async fn gauge_increment(
    name: &str,
    labels: &[(&str, &str)],
    value: f64,
    timestamp: Instant,
) -> Result<(), Error> {
    let key = make_key(name, labels);
    send_metric(Metric::Gauge(Gauge {
        key,
        timestamp,
        value: GaugeValue::Increment(value),
    }))
    .await
}

/// Send a historical gauge decrement
///
/// # Errors
///
/// Returns error if sender not initialized or channel is closed.
pub async fn gauge_decrement(
    name: &str,
    labels: &[(&str, &str)],
    value: f64,
    timestamp: Instant,
) -> Result<(), Error> {
    let key = make_key(name, labels);
    send_metric(Metric::Gauge(Gauge {
        key,
        timestamp,
        value: GaugeValue::Decrement(value),
    }))
    .await
}

/// Send a historical gauge set
///
/// # Errors
///
/// Returns error if sender not initialized or channel is closed.
pub async fn gauge_set(
    name: &str,
    labels: &[(&str, &str)],
    value: f64,
    timestamp: Instant,
) -> Result<(), Error> {
    let key = make_key(name, labels);
    send_metric(Metric::Gauge(Gauge {
        key,
        timestamp,
        value: GaugeValue::Set(value),
    }))
    .await
}
