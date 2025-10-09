//! Crate regarding Lading's 'capture' files

use std::{num::TryFromIntError, time::Instant};

use manager::{Counter, CounterValue, HISTORICAL_SENDER, Metric};
use metrics::Key;
use tokio::sync::MutexGuard;
use tracing::warn;

mod accumulator;
pub mod json;
pub mod manager;

/// Errors for historical write operations
#[derive(thiserror::Error, Debug, Copy, Clone)]
pub enum Error {
    /// Tick offset out of range
    #[error("tick_offset must be 0-29 for 30-second window, got {0}")]
    InvalidTickOffset(u8),
    /// [`CaptureManager`] not initialized
    #[error("CaptureManager not initialized, cannot send historical writes")]
    NotInitialized,
    /// Channel closed
    #[error("Historical write channel closed")]
    ChannelClosed,
    /// tick offset is out of scope
    #[error("Tick offset is out of allowable interval")]
    Int(#[from] TryFromIntError),
}

/// Send a historical counter increment
///
/// # Errors
///
/// Returns error if tick offset out of range or sender not initialized.
pub async fn counter_incr(key: Key, value: u64) -> Result<(), Error> {
    let sender: MutexGuard<Option<manager::Sender>> = HISTORICAL_SENDER.lock().await;

    let now = Instant::now();
    let start = sender.as_ref().ok_or(Error::NotInitialized)?.start;
    let tick_offset = now.duration_since(start).as_secs();

    if tick_offset > 60 {
        warn!("Unable to write counter older than 60 seconds");
        return Ok(());
    }
    let tick_offset = u8::try_from(tick_offset)?;
    let metric = Metric::Counter(Counter {
        key,
        tick_offset,
        value: CounterValue::Increment(value),
    });
    sender
        .as_ref()
        .ok_or(Error::NotInitialized)?
        .snd
        .send(metric)
        .await
        .map_err(|_| Error::ChannelClosed)
}

/// Send a historical counter absolute set
///
/// # Errors
///
/// Returns error if tick offset out of range or sender not initialized.
pub async fn counter_absolute(key: Key, value: u64) -> Result<(), Error> {
    let sender: MutexGuard<Option<manager::Sender>> = HISTORICAL_SENDER.lock().await;

    let now = Instant::now();
    let start = sender.as_ref().ok_or(Error::NotInitialized)?.start;
    let tick_offset = now.duration_since(start).as_secs();

    if tick_offset > 60 {
        warn!("Unable to write counter older than 60 seconds");
        return Ok(());
    }
    let tick_offset = u8::try_from(tick_offset)?;
    let metric = Metric::Counter(Counter {
        key,
        tick_offset,
        value: CounterValue::Absolute(value),
    });
    sender
        .as_ref()
        .ok_or(Error::NotInitialized)?
        .snd
        .send(metric)
        .await
        .map_err(|_| Error::ChannelClosed)
}

/// Send a historical gauge increment
///
/// # Errors
///
/// Returns error if tick offset out of range or sender not initialized.
pub async fn gauge_increment(key: Key, value: f64) -> Result<(), Error> {
    let sender: MutexGuard<Option<manager::Sender>> = HISTORICAL_SENDER.lock().await;

    let now = Instant::now();
    let start = sender.as_ref().ok_or(Error::NotInitialized)?.start;
    let tick_offset = now.duration_since(start).as_secs();

    if tick_offset > 60 {
        warn!("Unable to write gauge older than 60 seconds");
        return Ok(());
    }
    let tick_offset = u8::try_from(tick_offset)?;
    let metric = Metric::Gauge(manager::Gauge {
        key,
        tick_offset,
        value: manager::GaugeValue::Increment(value),
    });
    sender
        .as_ref()
        .ok_or(Error::NotInitialized)?
        .snd
        .send(metric)
        .await
        .map_err(|_| Error::ChannelClosed)
}

/// Send a historical gauge decrement
///
/// # Errors
///
/// Returns error if tick offset out of range or sender not initialized.
pub async fn gauge_decrement(key: Key, value: f64) -> Result<(), Error> {
    let sender: MutexGuard<Option<manager::Sender>> = HISTORICAL_SENDER.lock().await;

    let now = Instant::now();
    let start = sender.as_ref().ok_or(Error::NotInitialized)?.start;
    let tick_offset = now.duration_since(start).as_secs();

    if tick_offset > 60 {
        warn!("Unable to write gauge older than 60 seconds");
        return Ok(());
    }
    let tick_offset = u8::try_from(tick_offset)?;
    let metric = Metric::Gauge(manager::Gauge {
        key,
        tick_offset,
        value: manager::GaugeValue::Decrement(value),
    });
    sender
        .as_ref()
        .ok_or(Error::NotInitialized)?
        .snd
        .send(metric)
        .await
        .map_err(|_| Error::ChannelClosed)
}

/// Send a historical gauge set
///
/// # Errors
///
/// Returns error if tick offset out of range or sender not initialized.
pub async fn gauge_set(key: Key, value: f64) -> Result<(), Error> {
    let sender: MutexGuard<Option<manager::Sender>> = HISTORICAL_SENDER.lock().await;

    let now = Instant::now();
    let start = sender.as_ref().ok_or(Error::NotInitialized)?.start;
    let tick_offset = now.duration_since(start).as_secs();

    if tick_offset > 60 {
        warn!("Unable to write gauge older than 60 seconds");
        return Ok(());
    }
    let tick_offset = u8::try_from(tick_offset)?;
    let metric = Metric::Gauge(manager::Gauge {
        key,
        tick_offset,
        value: manager::GaugeValue::Set(value),
    });
    sender
        .as_ref()
        .ok_or(Error::NotInitialized)?
        .snd
        .send(metric)
        .await
        .map_err(|_| Error::ChannelClosed)
}
