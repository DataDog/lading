//! Crate regarding Lading's 'capture' files

use std::time::Instant;

use manager::{Counter, CounterValue, Gauge, GaugeValue, HISTORICAL_SENDER, Metric};
use ustr::Ustr;

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

    // Calculate how many seconds ago this metric occurred.
    let now = Instant::now();
    let timestamp_millis_in_past: u128 = now.duration_since(timestamp).as_millis();
    if timestamp_millis_in_past > manager::max_valid_millis() {
        warn!(metric = ?metric,
              timestamp = ?timestamp,
              now = ?now,
              timestamp_millis_in_past,
              max_valid_millis = manager::max_valid_millis(),
              "unable to send historical metric");
        return Ok(());
    }
    let tick_offset: u128 = timestamp_millis_in_past / 1_000;
    // SAFETY: in max_valid_millis we assert that the maximum valid seconds is
    // within u8.
    let tick_offset = u8::try_from(tick_offset).expect("catastrophic programming error");

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
