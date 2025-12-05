use metrics::Key;
use std::time::Instant;

#[derive(Debug, Clone)]
pub(crate) enum CounterValue {
    Increment(u64),
    Absolute(u64),
}

#[derive(Debug, Clone)]
pub(crate) enum GaugeValue {
    Increment(f64),
    Decrement(f64),
    Set(f64),
}

#[derive(Debug, Clone)]
pub(crate) struct Counter {
    pub key: Key,
    pub timestamp: Instant,
    pub value: CounterValue,
}

#[derive(Debug, Clone)]
pub(crate) struct Gauge {
    pub key: Key,
    pub timestamp: Instant,
    pub value: GaugeValue,
}

#[derive(Debug, Clone)]
pub(crate) struct Histogram {
    pub key: Key,
    pub timestamp: Instant,
    pub value: f64,
}

#[derive(Debug, Clone)]
pub(crate) enum Metric {
    /// Counter increment
    Counter(Counter),
    /// Gauge set
    Gauge(Gauge),
    /// Histogram sample
    ///
    /// Constructed by generators via `histogram()` function or by `histogram!` macro
    /// through the `CaptureRecorder`. Captured as individual samples with timestamps.
    Histogram(Histogram),
}
