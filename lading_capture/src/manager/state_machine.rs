//! Pure state machine for capture manager event loop
//!
//! Following the pattern from `lading::generator::kubernetes::state_machine`, this
//! module extracts the select loop logic from `CaptureManager::start` into a pure,
//! testable state machine. The state machine owns all the capture state and provides
//! a single `next()` method that processes events and returns operations.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    accumulator::{self, Accumulator, MetricValue},
    formats::{self, OutputFormat},
    line,
    manager::Clock,
    metric::{Counter, CounterValue, Gauge, GaugeValue, Metric},
};

#[cfg(test)]
use crate::manager::{ClockFn, InstantClock};
use metrics::Key;
use metrics_util::registry::{AtomicStorage, Registry};
use rustc_hash::FxHashMap;
use std::sync::atomic::Ordering;
use tracing::{debug, info, trace, warn};
use uuid::Uuid;

/// Default duration of a single `Accumulator` tick in milliseconds.
/// This constant is kept for backwards compatibility in tests.
#[cfg(test)]
pub(crate) const DEFAULT_TICK_DURATION_MS: u128 = 1_000;

/// Reserved label names that collide with top-level JSON fields in `json::Line`.
/// Labels with these names will be filtered out to prevent duplicate field errors
/// during JSON serialization.
const RESERVED_LABEL_NAMES: &[&str] = &[
    "run_id",
    "time",
    "fetch_index",
    "metric_name",
    "metric_kind",
    "value",
];

/// Events that drive the capture manager state machine
#[derive(Debug)]
pub(crate) enum Event {
    /// A historical metric was received from a generator
    MetricReceived(Metric),
    /// The channel for historical metrics closed unexpectedly
    ChannelClosed,
    /// The 1-second flush interval ticked
    FlushTick,
    /// Shutdown signal received
    ShutdownSignaled,
}

/// Operations the state machine can request
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Operation {
    /// Continue the event loop
    Continue,
    /// Exit the event loop
    Exit,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Format-related errors (IO, serialization, etc.)
    #[error(transparent)]
    Format(#[from] formats::Error),
    /// Accumulator errors
    #[error(transparent)]
    Accumulator(#[from] accumulator::Error),
}

/// State machine for capture manager event loop
///
/// The goal of this component is to contain the logic for event handling within
/// the capture manager _without_ async encumbrance. This leaves
/// `CaptureManager::start` to deal with the tokio select loop. That is, the async
/// mechanism should follow the output of this mechanism's `next` without
/// consideration.
///
/// This struct owns all the state needed to process metrics, flush them to disk,
/// and handle shutdown. Following the kubernetes generator pattern, all state
/// lives here rather than being passed as parameters.
pub(crate) struct StateMachine<F: OutputFormat, C: Clock> {
    /// Unique run instance ID for this `StateMachine`
    run_id: Uuid,
    /// Reference start time for timestamp-to-tick conversion
    start: Instant,
    /// Start time in milliseconds for deriving metric timestamps from ticks
    start_ms: u128,
    /// Duration of a single tick in milliseconds
    tick_duration_ms: u64,
    /// How long metrics can age before being discarded
    expiration: Duration,
    /// Output format for writing metrics, optional only to accomodate
    /// non-consuming shutdown. This is a code smell.
    format: Option<F>,
    /// Number of intervals between flush calls
    flush_interval: u64,
    /// Last tick when we flushed
    last_flush_tick: u64,
    /// Registry containing current metric values
    registry: Arc<Registry<Key, AtomicStorage>>,
    /// Accumulator for windowed metrics
    accumulator: Accumulator,
    /// Pre-filtered global labels (without reserved names)
    filtered_global_labels: FxHashMap<String, String>,
    /// Clock for time operations
    clock: C,
}

impl<F: OutputFormat, C: Clock> StateMachine<F, C> {
    /// Create a new state machine
    ///
    /// # Arguments
    ///
    /// * `tick_duration_ms` - Duration of a single tick in milliseconds (e.g., 1000 for 1Hz)
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        expiration: Duration,
        format: F,
        flush_interval: u64,
        tick_duration_ms: u64,
        registry: Arc<Registry<Key, AtomicStorage>>,
        accumulator: Accumulator,
        mut global_labels: FxHashMap<String, String>,
        clock: C,
    ) -> Self
    where
        C: Clone + 'static,
    {
        let start = clock.start();
        let start_ms = clock.now_ms();
        let run_id = Uuid::new_v4();

        // Set the global clock for histogram timestamping to ensure determinism
        // In tests, use the controllable clock. In production, the default
        // ClockFn::Real is used (direct call to Instant::now).
        #[cfg(test)]
        {
            let clock_clone = clock.clone();
            let clock_arc: Arc<dyn InstantClock> = Arc::new(clock_clone);
            crate::manager::CAPTURE_CLOCK.store(Arc::new(ClockFn::Test(clock_arc)));
        }
        #[cfg(not(test))]
        {
            // Suppress unused variable warning in non-test builds
            let _ = &clock;
        }

        // Filter out reserved names from global labels in place
        for reserved in RESERVED_LABEL_NAMES {
            if global_labels.remove(*reserved).is_some() {
                warn!(
                    label_key = *reserved,
                    "Filtered out reserved global label that would collide with capture file field"
                );
            }
        }

        Self {
            run_id,
            start,
            start_ms,
            tick_duration_ms,
            expiration,
            format: Some(format),
            flush_interval,
            last_flush_tick: 0,
            registry,
            accumulator,
            filtered_global_labels: global_labels,
            clock,
        }
    }

    /// Process an event and return the next operation
    ///
    /// This is the core of the state machine: it receives an event from the
    /// async select loop and performs the appropriate action, returning whether
    /// to continue or exit.
    ///
    /// # Errors
    ///
    /// Returns an error if writing metrics fails or accumulator operations fail.
    pub(crate) fn next(&mut self, event: Event) -> Result<Operation, Error> {
        match event {
            Event::MetricReceived(metric) => self.handle_metric_received(metric),
            Event::ChannelClosed => Ok(Self::handle_channel_closed()),
            Event::FlushTick => self.handle_flush_tick(),
            Event::ShutdownSignaled => self.handle_shutdown(),
        }
    }

    fn handle_metric_received(&mut self, metric: Metric) -> Result<Operation, Error> {
        match metric {
            Metric::Counter(c) => {
                let tick = self.instant_to_tick(c.timestamp);
                self.accumulator.counter(c, tick)?;
            }
            Metric::Gauge(g) => {
                let tick = self.instant_to_tick(g.timestamp);
                self.accumulator.gauge(g, tick)?;
            }
            Metric::Histogram(h) => {
                let tick = self.instant_to_tick(h.timestamp);
                self.accumulator.histogram(h, tick)?;
            }
        }
        Ok(Operation::Continue)
    }

    fn handle_channel_closed() -> Operation {
        warn!("Timestamped metrics unexpected transmission shutdown");
        Operation::Exit
    }

    fn handle_flush_tick(&mut self) -> Result<Operation, Error> {
        let tick_start = self.clock.now();

        // Drift correction: if wall clock time has advanced faster than our
        // logical tick counter, advance the tick multiple times to catch up.
        let wall_clock_elapsed = tick_start.duration_since(self.start).as_secs();
        let tick_drift = wall_clock_elapsed.saturating_sub(self.accumulator.current_tick);

        if tick_drift > 0 {
            warn!(
                wall_clock_elapsed_secs = wall_clock_elapsed,
                current_tick = self.accumulator.current_tick,
                tick_drift_secs = tick_drift,
                "internal logical time drifted from wall clock, advancing logical time"
            );
            for _ in 0..tick_drift {
                self.accumulator.advance_tick();
            }
        }

        // Record current metrics from registry and flush mature data
        self.record_captures(tick_start)?;

        // Performance check
        let record_duration = self.clock.now().duration_since(tick_start);
        if record_duration > Duration::from_secs(1) {
            warn!(
                duration = ?record_duration,
                "Recording capture took more than 1s"
            );
        }

        Ok(Operation::Continue)
    }

    fn handle_shutdown(&mut self) -> Result<Operation, Error> {
        info!("shutdown signal received, flushing all remaining metrics");
        self.drain_and_write()?;
        // Close the format to finalize the output file. For Parquet this writes
        // the critical file footer, for JSONL it ensures all data is flushed.
        // Take ownership of the format to call close() which consumes it.
        if let Some(format) = self.format.take() {
            format.close()?;
        }
        Ok(Operation::Exit)
    }

    /// Convert an Instant timestamp to `Accumulator` logical tick time.
    #[inline]
    fn instant_to_tick(&self, timestamp: Instant) -> u64 {
        timestamp.duration_since(self.start).as_secs()
    }

    /// Record all current metrics from the registry and flush mature data
    fn record_captures(&mut self, now: Instant) -> Result<(), Error> {
        let tick = self.accumulator.current_tick;

        // Capture all counter values from the registry
        for (k, c) in self.registry.get_counter_handles() {
            let val = c.load(Ordering::Relaxed);
            let counter = Counter {
                key: k,
                timestamp: now,
                value: CounterValue::Absolute(val),
            };
            self.accumulator.counter(counter, tick)?;
        }

        // Capture all gauge values from the registry
        for (k, g) in self.registry.get_gauge_handles() {
            let bits = g.load(Ordering::Relaxed);
            // There's no atomic f64 so we have to convert from AtomicU64
            let value = f64::from_bits(bits);
            let gauge = Gauge {
                key: k,
                timestamp: now,
                value: GaugeValue::Set(value),
            };
            self.accumulator.gauge(gauge, tick)?;
        }

        let old_tick = self.accumulator.current_tick;
        self.accumulator.advance_tick();
        trace!(
            old_tick = old_tick,
            new_tick = self.accumulator.current_tick,
            "Advanced accumulator tick"
        );

        let mut line_count = 0;
        for (key, value, tick) in self.accumulator.flush() {
            // Calculate time from tick to ensure strictly increasing time values
            let time_ms = self.start_ms + (u128::from(tick) * u128::from(self.tick_duration_ms));
            self.write_metric_line(&key, &value, tick, time_ms)?;
            line_count += 1;
        }

        // Flush if flush_interval has elapsed
        let current_tick = self.accumulator.current_tick;
        if current_tick - self.last_flush_tick >= self.flush_interval {
            self.format
                .as_mut()
                .expect("format must be present during operation")
                .flush()?;
            self.last_flush_tick = current_tick;
        }

        debug!(tick = ?tick, flushed_captures = line_count, "Flushed mature captures",);
        Ok(())
    }

    /// Drain all accumulated metrics and write them to the capture file
    fn drain_and_write(&mut self) -> Result<(), Error> {
        // Replace the accumulator with a new one and consume the old one for draining
        // This is only called during shutdown, so we don't need the accumulator anymore
        let accumulator = std::mem::replace(&mut self.accumulator, Accumulator::new());

        // Drain all remaining data from the accumulator
        // Process each tick's metrics immediately to maintain order
        for metrics in accumulator.drain() {
            for (key, value, tick) in metrics {
                // Calculate time from tick to ensure strictly increasing time values
                let time_ms = self.start_ms + (u128::from(tick) * u128::from(self.tick_duration_ms));
                self.write_metric_line(&key, &value, tick, time_ms)?;
            }
        }
        self.format
            .as_mut()
            .expect("format must be present during operation")
            .flush()?;

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn write_metric_line(
        &mut self,
        key: &Key,
        value: &MetricValue,
        tick: u64,
        now_ms: u128,
    ) -> Result<(), Error> {
        // Use pre-filtered global labels
        let mut labels = self.filtered_global_labels.clone();

        // Add metric-specific labels, skipping reserved names
        for lbl in key.labels() {
            let key_str = lbl.key();
            if RESERVED_LABEL_NAMES.contains(&key_str) {
                warn!(
                    label_key = key_str,
                    metric_name = key.name(),
                    "Filtered out reserved metric label that would collide with capture file field"
                );
            } else {
                labels.insert(key_str.into(), lbl.value().into());
            }
        }

        // Calculate when this metric was actually recorded based on its tick.
        let tick_age = self.accumulator.current_tick.saturating_sub(tick);
        let tick_age_ms = u128::from(tick_age) * u128::from(self.tick_duration_ms);
        // Skip any line that has expired.
        if tick_age_ms > self.expiration.as_millis() {
            return Ok(());
        }

        match value {
            MetricValue::Counter(val) => {
                let line = line::Line {
                    run_id: self.run_id,
                    time: now_ms,
                    fetch_index: tick,
                    metric_name: key.name().into(),
                    metric_kind: line::MetricKind::Counter,
                    value: line::LineValue::Int(*val),
                    labels,
                    value_histogram: Vec::new(),
                };
                self.format
                    .as_mut()
                    .expect("format must be present during operation")
                    .write_metric(&line)?;
            }
            MetricValue::Gauge(val) => {
                let line = line::Line {
                    run_id: self.run_id,
                    time: now_ms,
                    fetch_index: tick,
                    metric_name: key.name().into(),
                    metric_kind: line::MetricKind::Gauge,
                    value: line::LineValue::Float(*val),
                    labels,
                    value_histogram: Vec::new(),
                };
                self.format
                    .as_mut()
                    .expect("format must be present during operation")
                    .write_metric(&line)?;
            }
            MetricValue::Histogram(sketch_bytes) => {
                let line = line::Line {
                    run_id: self.run_id,
                    time: now_ms,
                    fetch_index: tick,
                    metric_name: key.name().into(),
                    metric_kind: line::MetricKind::Histogram,
                    value: line::LineValue::Float(0.0),
                    labels,
                    value_histogram: sketch_bytes.clone(),
                };

                self.format
                    .as_mut()
                    .expect("format must be present during operation")
                    .write_metric(&line)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metric::{Counter, Gauge, Histogram};
    use crate::{formats::jsonl, test::writer::InMemoryWriter};
    use datadog_protos::metrics::Dogsketch;
    use ddsketch_agent::DDSketch;
    use metrics_util::registry::{AtomicStorage, Registry};
    use proptest::prelude::*;
    use protobuf::Message;
    use std::{
        collections::HashSet,
        sync::{Arc, Mutex},
        time::Duration,
    };

    /// Test clock for deterministic time control
    #[derive(Clone)]
    struct TestClock {
        time_ms: Arc<Mutex<u128>>,
        start_instant: Instant,
        start_time_ms: Arc<Mutex<u128>>,
    }

    impl TestClock {
        fn new(initial_time_ms: u128) -> Self {
            Self {
                time_ms: Arc::new(Mutex::new(initial_time_ms)),
                start_instant: Instant::now(),
                start_time_ms: Arc::new(Mutex::new(initial_time_ms)),
            }
        }

        fn advance(&self, millis: u128) {
            let mut time = self.time_ms.lock().unwrap();
            *time += millis;
        }

        fn rewind(&self, millis: u128) {
            let mut time = self.time_ms.lock().unwrap();
            *time = time.saturating_sub(millis);
        }
    }

    /// Test interval for state machine tests
    struct TestInterval {
        clock: TestClock,
        interval_ms: u128,
        next_tick_ms: Arc<Mutex<u128>>,
    }

    impl TestInterval {
        fn new(clock: TestClock, interval_ms: u128) -> Self {
            let now = clock.now_ms();
            let next_tick_ms = Arc::new(Mutex::new(now + interval_ms));
            Self {
                clock,
                interval_ms,
                next_tick_ms,
            }
        }
    }

    impl crate::manager::TickInterval for TestInterval {
        async fn tick(&mut self) {
            // Wait until clock time >= next tick deadline
            loop {
                let current_ms = self.clock.now_ms();
                let next = *self.next_tick_ms.lock().unwrap();

                if current_ms >= next {
                    // Deadline reached, update next tick and return
                    let mut next_tick = self.next_tick_ms.lock().unwrap();
                    *next_tick = current_ms + self.interval_ms;
                    return;
                }

                // Not ready yet, yield to other tasks
                tokio::task::yield_now().await;
            }
        }
    }

    impl Clock for TestClock {
        type Interval = TestInterval;

        fn now_ms(&self) -> u128 {
            *self.time_ms.lock().unwrap()
        }

        fn now(&self) -> Instant {
            let time_ms = *self.time_ms.lock().unwrap();
            self.start_instant + Duration::from_millis(time_ms as u64)
        }

        fn interval(&self, duration: Duration) -> Self::Interval {
            TestInterval::new(self.clone(), duration.as_millis())
        }

        fn start(&self) -> Instant {
            let start_time_ms = *self.start_time_ms.lock().unwrap();
            self.start_instant + Duration::from_millis(start_time_ms as u64)
        }

        fn mark_start(&mut self) {
            let current_time_ms = *self.time_ms.lock().unwrap();
            *self.start_time_ms.lock().unwrap() = current_time_ms;
        }
    }

    /// Operations for property testing the state machine
    #[derive(Clone)]
    enum CaptureOp {
        WriteCounter {
            name: String,
            value: u64,
        },
        WriteGauge {
            name: String,
            value: f64,
        },
        WriteHistogram {
            name: String,
            value: f64,
        },
        HistoricalCounterIncr {
            name: String,
            value: u64,
            tick_offset: u64,
        },
        HistoricalCounterAbs {
            name: String,
            value: u64,
            tick_offset: u64,
        },
        HistoricalGaugeIncr {
            name: String,
            value: f64,
            tick_offset: u64,
        },
        HistoricalGaugeDec {
            name: String,
            value: f64,
            tick_offset: u64,
        },
        HistoricalGaugeSet {
            name: String,
            value: f64,
            tick_offset: u64,
        },
        HistoricalHistogram {
            name: String,
            value: f64,
            tick_offset: u64,
        },
        AdvanceTime {
            millis: u128,
        },
        BackwardTime {
            millis: u128,
        },
        FlushTick,
    }

    impl std::fmt::Debug for CaptureOp {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::WriteCounter { name, value } => {
                    write!(f, "WriteCounter({name:?}, {value})")
                }
                Self::WriteGauge { name, value } => {
                    write!(f, "WriteGauge({name:?}, {value})")
                }
                Self::HistoricalCounterIncr {
                    name,
                    value,
                    tick_offset,
                } => {
                    write!(
                        f,
                        "HistoricalCounterIncr({name:?}, {value}, tick_offset={tick_offset})"
                    )
                }
                Self::HistoricalCounterAbs {
                    name,
                    value,
                    tick_offset,
                } => {
                    write!(
                        f,
                        "HistoricalCounterAbs({name:?}, {value}, tick_offset={tick_offset})"
                    )
                }
                Self::HistoricalGaugeIncr {
                    name,
                    value,
                    tick_offset,
                } => {
                    write!(
                        f,
                        "HistoricalGaugeIncr({name:?}, {value}, tick_offset={tick_offset})"
                    )
                }
                Self::HistoricalGaugeDec {
                    name,
                    value,
                    tick_offset,
                } => {
                    write!(
                        f,
                        "HistoricalGaugeDec({name:?}, {value}, tick_offset={tick_offset})"
                    )
                }
                Self::HistoricalGaugeSet {
                    name,
                    value,
                    tick_offset,
                } => {
                    write!(
                        f,
                        "HistoricalGaugeSet({name:?}, {value}, tick_offset={tick_offset})"
                    )
                }
                Self::WriteHistogram { name, value } => {
                    write!(f, "WriteHistogram({name:?}, {value})")
                }
                Self::HistoricalHistogram {
                    name,
                    value,
                    tick_offset,
                } => {
                    write!(
                        f,
                        "HistoricalHistogram({name:?}, {value}, tick_offset={tick_offset})"
                    )
                }
                Self::AdvanceTime { millis } => write!(f, "AdvanceTime({millis})"),
                Self::BackwardTime { millis } => write!(f, "BackwardTime({millis})"),
                Self::FlushTick => write!(f, "FlushTick"),
            }
        }
    }

    impl Arbitrary for CaptureOp {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            prop_oneof![
                ("[a-z]{1,5}", 1u64..1000u64)
                    .prop_map(|(name, value)| CaptureOp::WriteCounter { name, value }),
                (
                    "[a-z]{1,5}",
                    (-1000.0f64..1000.0f64).prop_filter("must be finite", |f| f.is_finite())
                )
                    .prop_map(|(name, value)| CaptureOp::WriteGauge { name, value }),
                ("[a-z]{1,5}", 1u64..1000u64, 0u64..=10u64).prop_map(
                    |(name, value, tick_offset)| CaptureOp::HistoricalCounterIncr {
                        name,
                        value,
                        tick_offset
                    }
                ),
                ("[a-z]{1,5}", 1u64..1000u64, 0u64..=10u64).prop_map(
                    |(name, value, tick_offset)| CaptureOp::HistoricalCounterAbs {
                        name,
                        value,
                        tick_offset
                    }
                ),
                (
                    "[a-z]{1,5}",
                    (-1000.0f64..1000.0f64).prop_filter("must be finite", |f| f.is_finite()),
                    0u64..=10u64
                )
                    .prop_map(|(name, value, tick_offset)| {
                        CaptureOp::HistoricalGaugeIncr {
                            name,
                            value,
                            tick_offset,
                        }
                    }),
                (
                    "[a-z]{1,5}",
                    (-1000.0f64..1000.0f64).prop_filter("must be finite", |f| f.is_finite()),
                    0u64..=10u64
                )
                    .prop_map(|(name, value, tick_offset)| {
                        CaptureOp::HistoricalGaugeDec {
                            name,
                            value,
                            tick_offset,
                        }
                    }),
                (
                    "[a-z]{1,5}",
                    (-1000.0f64..1000.0f64).prop_filter("must be finite", |f| f.is_finite()),
                    0u64..=10u64
                )
                    .prop_map(|(name, value, tick_offset)| {
                        CaptureOp::HistoricalGaugeSet {
                            name,
                            value,
                            tick_offset,
                        }
                    }),
                (
                    "[a-z]{1,5}",
                    (-1000.0f64..1000.0f64).prop_filter("must be finite", |f| f.is_finite())
                )
                    .prop_map(|(name, value)| CaptureOp::WriteHistogram { name, value }),
                (
                    "[a-z]{1,5}",
                    (-1000.0f64..1000.0f64).prop_filter("must be finite", |f| f.is_finite()),
                    0u64..=10u64
                )
                    .prop_map(|(name, value, tick_offset)| {
                        CaptureOp::HistoricalHistogram {
                            name,
                            value,
                            tick_offset,
                        }
                    }),
                (0u128..=1_000u128).prop_map(|millis| CaptureOp::AdvanceTime { millis }),
                (0u128..=500u128).prop_map(|millis| CaptureOp::BackwardTime { millis }),
                Just(CaptureOp::FlushTick),
            ]
            .boxed()
        }
    }

    proptest! {
        #[test]
        fn state_machine_output_satisfies_invariants(
            ops in prop::collection::vec(any::<CaptureOp>(), 10..50)
        ) {
            let writer = InMemoryWriter::new();
            let format = jsonl::Format::new(writer.clone());
            let clock = TestClock::new(1000);
            let registry = Arc::new(Registry::new(AtomicStorage));
            let accumulator = Accumulator::new();
            let labels = FxHashMap::default();

            let recorder = crate::manager::CaptureRecorder {
                registry: Arc::clone(&registry),
            };

            let mut machine = StateMachine::new(
                Duration::from_secs(60),
                format,
                1,
                DEFAULT_TICK_DURATION_MS as u64,
                registry,
                accumulator,
                labels,
                clock.clone(),
            );

            let flush_tick_count = ops
                .iter()
                .filter(|op| matches!(op, CaptureOp::FlushTick))
                .count();

            let written_metric_names: HashSet<String> = ops
                .iter()
                .filter_map(|op| match op {
                    CaptureOp::WriteCounter { name, .. }
                    | CaptureOp::WriteGauge { name, .. }
                    | CaptureOp::WriteHistogram { name, .. }
                    | CaptureOp::HistoricalCounterIncr { name, .. }
                    | CaptureOp::HistoricalCounterAbs { name, .. }
                    | CaptureOp::HistoricalGaugeIncr { name, .. }
                    | CaptureOp::HistoricalGaugeDec { name, .. }
                    | CaptureOp::HistoricalGaugeSet { name, .. }
                    | CaptureOp::HistoricalHistogram { name, .. } => Some(name.clone()),
                    CaptureOp::AdvanceTime { .. }
                    | CaptureOp::BackwardTime { .. }
                    | CaptureOp::FlushTick => None,
                })
                .collect();

            for op in ops {
                match op {
                    CaptureOp::WriteCounter { name, value } => {
                        metrics::with_local_recorder(&recorder, || {
                            metrics::counter!(name).increment(value);
                        });
                    }
                    CaptureOp::WriteGauge { name, value } => {
                        metrics::with_local_recorder(&recorder, || {
                            metrics::gauge!(name).set(value);
                        });
                    }
                    CaptureOp::WriteHistogram { name, value } => {
                        metrics::with_local_recorder(&recorder, || {
                            metrics::histogram!(name).record(value);
                        });
                    }
                    CaptureOp::HistoricalCounterIncr { name, value, tick_offset } => {
                        let timestamp = Clock::now(&clock)
                            .checked_sub(Duration::from_secs(tick_offset))
                            .unwrap_or(clock.start());
                        let counter = Counter {
                            key: Key::from_name(name),
                            timestamp,
                            value: CounterValue::Increment(value),
                        };
                        let _ = machine.next(Event::MetricReceived(Metric::Counter(counter)));
                    }
                    CaptureOp::HistoricalCounterAbs { name, value, tick_offset } => {
                        let timestamp = Clock::now(&clock)
                            .checked_sub(Duration::from_secs(tick_offset))
                            .unwrap_or(clock.start());
                        let counter = Counter {
                            key: Key::from_name(name),
                            timestamp,
                            value: CounterValue::Absolute(value),
                        };
                        let _ = machine.next(Event::MetricReceived(Metric::Counter(counter)));
                    }
                    CaptureOp::HistoricalGaugeIncr { name, value, tick_offset } => {
                        let timestamp = Clock::now(&clock)
                            .checked_sub(Duration::from_secs(tick_offset))
                            .unwrap_or(clock.start());
                        let gauge = Gauge {
                            key: Key::from_name(name),
                            timestamp,
                            value: GaugeValue::Increment(value),
                        };
                        let _ = machine.next(Event::MetricReceived(Metric::Gauge(gauge)));
                    }
                    CaptureOp::HistoricalGaugeDec { name, value, tick_offset } => {
                        let timestamp = Clock::now(&clock)
                            .checked_sub(Duration::from_secs(tick_offset))
                            .unwrap_or(clock.start());
                        let gauge = Gauge {
                            key: Key::from_name(name),
                            timestamp,
                            value: GaugeValue::Decrement(value),
                        };
                        let _ = machine.next(Event::MetricReceived(Metric::Gauge(gauge)));
                    }
                    CaptureOp::HistoricalGaugeSet { name, value, tick_offset } => {
                        let timestamp = Clock::now(&clock)
                            .checked_sub(Duration::from_secs(tick_offset))
                            .unwrap_or(clock.start());
                        let gauge = Gauge {
                            key: Key::from_name(name),
                            timestamp,
                            value: GaugeValue::Set(value),
                        };
                        let _ = machine.next(Event::MetricReceived(Metric::Gauge(gauge)));
                    }
                    CaptureOp::HistoricalHistogram { name, value, tick_offset } => {
                        let timestamp = Clock::now(&clock)
                            .checked_sub(Duration::from_secs(tick_offset))
                            .unwrap_or(clock.start());
                        let histogram = Histogram {
                            key: Key::from_name(name),
                            timestamp,
                            value,
                        };
                        let _ = machine.next(Event::MetricReceived(Metric::Histogram(histogram)));
                    }
                    CaptureOp::AdvanceTime { millis } => {
                        clock.advance(millis);
                    }
                    CaptureOp::BackwardTime { millis } => {
                        clock.rewind(millis);
                    }
                    CaptureOp::FlushTick => {
                        let _ = machine.next(Event::FlushTick);
                    }
                }
            }

            let _ = machine.next(Event::ShutdownSignaled);

            let lines = writer.parse_lines().unwrap();
            let result = crate::validate::jsonl::validate_lines(&lines, None);

            prop_assert!(
                result.is_valid(),
                "Invariant violation detected:\n  Line: {line}\n  Series: {series}\n  Message: {msg}",
                line = result.first_error.as_ref().map(|(l, _, _)| l).unwrap_or(&0),
                series = result.first_error.as_ref().map(|(_, s, _)| s.as_str()).unwrap_or(""),
                msg = result.first_error.as_ref().map(|(_, _, m)| m.as_str()).unwrap_or("")
            );

            if flush_tick_count >= 60 && !written_metric_names.is_empty() {
                let output_metric_names: HashSet<String> =
                    lines.iter().map(|line| line.metric_name.clone()).collect();

                for name in &written_metric_names {
                    prop_assert!(
                        output_metric_names.contains(name),
                        "Metric '{}' was written but not found in output (60+ FlushTicks occurred)",
                        name
                    );
                }

                // Verify histogram data integrity
                for line in &lines {
                    if line.metric_kind == line::MetricKind::Histogram {
                        if !line.value_histogram.is_empty() {
                            let sketch_bytes = &line.value_histogram;
                            let dogsketch = Dogsketch::parse_from_bytes(sketch_bytes)
                                .expect("should parse protobuf");
                            let sketch = DDSketch::try_from(dogsketch).expect("should convert");

                            prop_assert!(
                                sketch.count() > 0,
                                "Histogram {} has empty sketch but should have samples",
                                line.metric_name
                            );

                            // Verify min/max are finite when sketch has data
                            if let Some(min) = sketch.min() {
                                prop_assert!(
                                    min.is_finite(),
                                    "Histogram {} has non-finite min: {}",
                                    line.metric_name,
                                    min
                                );
                            }
                            if let Some(max) = sketch.max() {
                                prop_assert!(
                                    max.is_finite(),
                                    "Histogram {} has non-finite max: {}",
                                    line.metric_name,
                                    max
                                );
                            }

                            // Verify quantiles work and return finite values
                            let median = sketch.quantile(0.5).unwrap_or(f64::NAN);
                            prop_assert!(
                                median.is_finite(),
                                "Histogram {} has non-finite median: {}",
                                line.metric_name,
                                median
                            );
                        } else {
                            // Histogram line exists but has no data - this might be valid
                            // for empty sketches that weren't filtered
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn metric_received_adds_to_accumulator() {
        let writer = InMemoryWriter::new();
        let format = jsonl::Format::new(writer);
        let clock = TestClock::new(1000);
        let registry = Arc::new(Registry::new(AtomicStorage));
        let accumulator = Accumulator::new();
        let labels = FxHashMap::default();

        let mut machine = StateMachine::new(
            Duration::from_secs(60),
            format,
            1,
            DEFAULT_TICK_DURATION_MS as u64,
            registry,
            accumulator,
            labels,
            clock.clone(),
        );

        let counter = Metric::Counter(Counter {
            key: Key::from_name("test_counter"),
            timestamp: clock.start(),
            value: CounterValue::Absolute(42),
        });

        let result = machine.next(Event::MetricReceived(counter));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Operation::Continue);
    }

    #[test]
    fn channel_closed_returns_exit() {
        let writer = InMemoryWriter::new();
        let format = jsonl::Format::new(writer);
        let clock = TestClock::new(1000);
        let registry = Arc::new(Registry::new(AtomicStorage));
        let accumulator = Accumulator::new();
        let labels = FxHashMap::default();

        let mut machine = StateMachine::new(
            Duration::from_secs(60),
            format,
            1,
            DEFAULT_TICK_DURATION_MS as u64,
            registry,
            accumulator,
            labels,
            clock,
        );

        let result = machine.next(Event::ChannelClosed);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Operation::Exit);
    }

    #[test]
    fn flush_tick_advances_accumulator() {
        let writer = InMemoryWriter::new();
        let format = jsonl::Format::new(writer);
        let clock = TestClock::new(1000);
        let registry = Arc::new(Registry::new(AtomicStorage));
        let accumulator = Accumulator::new();
        let labels = FxHashMap::default();

        let mut machine = StateMachine::new(
            Duration::from_secs(60),
            format,
            1,
            DEFAULT_TICK_DURATION_MS as u64,
            registry,
            accumulator,
            labels,
            clock.clone(),
        );

        let initial_tick = machine.accumulator.current_tick;

        let result = machine.next(Event::FlushTick);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Operation::Continue);

        // Tick should have advanced
        assert_eq!(machine.accumulator.current_tick, initial_tick + 1);
    }

    #[test]
    fn shutdown_returns_exit() {
        let writer = InMemoryWriter::new();
        let format = jsonl::Format::new(writer);
        let clock = TestClock::new(1000);
        let registry = Arc::new(Registry::new(AtomicStorage));
        let accumulator = Accumulator::new();
        let labels = FxHashMap::default();

        let mut machine = StateMachine::new(
            Duration::from_secs(60),
            format,
            1,
            DEFAULT_TICK_DURATION_MS as u64,
            registry,
            accumulator,
            labels,
            clock,
        );

        let result = machine.next(Event::ShutdownSignaled);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Operation::Exit);
    }

    #[test]
    fn reserved_label_names_are_filtered() {
        let writer = InMemoryWriter::new();
        let format = jsonl::Format::new(writer.clone());
        let clock = TestClock::new(0);
        let registry = Arc::new(Registry::new(AtomicStorage));
        let accumulator = Accumulator::new();

        // Set up global labels with a reserved name: `run_id`.
        let mut global_labels = FxHashMap::default();
        global_labels.insert("run_id".into(), "user-global-value".into());
        global_labels.insert("safe_label".into(), "safe_value".into());

        let recorder = crate::manager::CaptureRecorder {
            registry: Arc::clone(&registry),
        };

        let mut machine = StateMachine::new(
            Duration::from_secs(60),
            format,
            1,
            DEFAULT_TICK_DURATION_MS as u64,
            registry,
            accumulator,
            global_labels,
            clock.clone(),
        );

        let stock_run_id = machine.run_id;

        clock.advance(1_000);
        // Write a metrics-rs metric that will receive global labels
        // Note: the global labels (including run_id) are added in write_metric_line()
        metrics::with_local_recorder(&recorder, || {
            metrics::counter!("test_counter", "other_label" => "other_value").increment(1);
        });

        // Flush to write the metric
        let _ = machine.next(Event::FlushTick);

        // Advance time again
        clock.advance(1_000);

        // Write a historical metric with its own reserved label
        let historical_counter = Counter {
            key: Key::from_parts(
                "historical_counter",
                vec![
                    metrics::Label::new("run_id", "historical-value"),
                    metrics::Label::new("valid_label", "valid_value"),
                ],
            ),
            timestamp: clock.start(),
            value: CounterValue::Absolute(42),
        };
        let _ = machine.next(Event::MetricReceived(Metric::Counter(historical_counter)));
        let _ = machine.next(Event::FlushTick);
        let _ = machine.next(Event::ShutdownSignaled);

        // Parse the output, assert we can find both metrics.
        let parsed_lines = writer.parse_lines().expect("should parse");

        println!("\n=== Parsed {} lines ===", parsed_lines.len());
        for line in &parsed_lines {
            println!("  {}: {:?}", line.metric_name, line.labels);
        }

        let metrics_rs_line = parsed_lines
            .iter()
            .find(|l| l.metric_name == "test_counter")
            .expect("should find metrics-rs counter");

        let historical_line = parsed_lines
            .iter()
            .find(|l| l.metric_name == "historical_counter")
            .expect("should find historical counter");

        // Verify the run_id field is the stock UUID, not the user value
        assert_eq!(metrics_rs_line.run_id, stock_run_id);
        assert_eq!(historical_line.run_id, stock_run_id);

        // Verify the run_id label was filtered out from both metrics
        assert!(
            !metrics_rs_line.labels.contains_key("run_id"),
            "run_id should be filtered from metrics-rs metric"
        );
        assert!(
            !historical_line.labels.contains_key("run_id"),
            "run_id should be filtered from historical metric"
        );

        // Verify metrics-rs metric has its own label, global labels
        assert_eq!(
            metrics_rs_line.labels.get("other_label"),
            Some(&"other_value".to_string()),
            "metrics-rs metric should have its own labels"
        );
        assert_eq!(
            metrics_rs_line.labels.get("safe_label"),
            Some(&"safe_value".to_string()),
            "metrics-rs metric should receive global labels"
        );

        // Verify historical metric has its own label, global labels
        assert_eq!(
            historical_line.labels.get("valid_label"),
            Some(&"valid_value".to_string()),
            "historical metric should have its own labels"
        );
        assert_eq!(
            historical_line.labels.get("safe_label"),
            Some(&"safe_value".to_string()),
            "historical metric should receive global labels"
        );

        // Verify the JSON doesn't have duplicate fields
        let raw_json = writer.get_string();
        for line in raw_json.lines() {
            let run_id_count = line.matches("\"run_id\"").count();
            assert_eq!(
                run_id_count, 1,
                "Each line should have exactly one run_id field, got {run_id_count} in: {line}"
            );
        }
    }

    #[test]
    fn drift_correction_advances_multiple_ticks() {
        let writer = InMemoryWriter::new();
        let format = jsonl::Format::new(writer);
        let clock = TestClock::new(1000);
        let registry = Arc::new(Registry::new(AtomicStorage));
        let accumulator = Accumulator::new();
        let labels = FxHashMap::default();

        let mut machine = StateMachine::new(
            Duration::from_secs(60),
            format,
            1,
            DEFAULT_TICK_DURATION_MS as u64,
            registry,
            accumulator,
            labels,
            clock.clone(),
        );

        // Simulate significant time drift: clock advances 5 seconds but ticks
        // haven't advanced
        clock.advance(5_000);

        let initial_tick = machine.accumulator.current_tick;
        let result = machine.next(Event::FlushTick);
        assert!(result.is_ok());

        // Should have caught up: 5 ticks for drift + 1 for the flush
        assert_eq!(machine.accumulator.current_tick, initial_tick + 6);
    }

    /// Test that strictly increasing counter and gauge values per interval
    /// produce strictly monotonic output in both flush and drain operations.
    ///
    /// Runs for 600 intervals, incrementing metrics each tick, and verifies
    /// the JSON output has strictly increasing values throughout.
    #[test]
    fn strictly_increasing_values_remain_monotonic() {
        let writer = InMemoryWriter::new();
        let format = jsonl::Format::new(writer.clone());
        let clock = TestClock::new(0);
        let registry = Arc::new(Registry::new(AtomicStorage));
        let accumulator = Accumulator::new();
        let labels = FxHashMap::default();

        let mut machine = StateMachine::new(
            Duration::from_secs(3600),
            format,
            1,
            DEFAULT_TICK_DURATION_MS as u64,
            registry.clone(),
            accumulator,
            labels,
            clock.clone(),
        );

        let counter_key = Key::from_static_name("monotonic_counter");
        let gauge_key = Key::from_static_name("monotonic_gauge");

        // Run for 600 intervals. Increment the counter, advance time by 1
        // second each iteration and trigger a flush. Then, shutdown.
        for _ in 0..600 {
            registry
                .get_or_create_counter(&counter_key, |c| metrics::Counter::from_arc(c.clone()))
                .increment(1);
            registry
                .get_or_create_gauge(&gauge_key, |g| metrics::Gauge::from_arc(g.clone()))
                .increment(1.0);

            clock.advance(DEFAULT_TICK_DURATION_MS);
            machine.next(Event::FlushTick).unwrap();
        }

        // Track how many lines were written during normal operation (before shutdown)
        let lines_before_shutdown = writer.parse_lines().unwrap().len();
        println!("Lines written during normal flush operations: {lines_before_shutdown}");

        machine.next(Event::ShutdownSignaled).unwrap();

        // Parse output and verify strictly increasing values
        let lines = writer.parse_lines().unwrap();
        println!("Total lines written (including drain): {}", lines.len());
        println!(
            "Lines written during drain: {}",
            lines.len() - lines_before_shutdown
        );

        let mut last_counter_value: Option<f64> = None;
        let mut last_gauge_value: Option<f64> = None;
        let mut counter_line_number = 0;
        let mut gauge_line_number = 0;

        // Check flush phase
        for line in &lines[..lines_before_shutdown] {
            if line.metric_name == "monotonic_counter" {
                let value = line.value.as_f64();
                if let Some(last) = last_counter_value {
                    if value <= last {
                        println!("\n=== MONOTONICITY VIOLATION IN FLUSH ===");
                        println!("Counter line #{counter_line_number}");
                        println!("Current value: {value}, Previous: {last}");
                        println!("Time: {} ms, Fetch index: {}", line.time, line.fetch_index);
                        panic!("Counter not strictly increasing during FLUSH: {value} <= {last}");
                    }
                }
                last_counter_value = Some(value);
                counter_line_number += 1;
            } else if line.metric_name == "monotonic_gauge" {
                let value = line.value.as_f64();
                if let Some(last) = last_gauge_value {
                    if value <= last {
                        println!("\n=== MONOTONICITY VIOLATION IN FLUSH ===");
                        println!("Gauge line #{gauge_line_number}");
                        println!("Current value: {value}, Previous: {last}");
                        println!("Time: {} ms, Fetch index: {}", line.time, line.fetch_index);
                        panic!("Gauge not strictly increasing during FLUSH: {value} <= {last}");
                    }
                }
                last_gauge_value = Some(value);
                gauge_line_number += 1;
            }
        }

        // Check drain phase
        for line in &lines[lines_before_shutdown..] {
            if line.metric_name == "monotonic_counter" {
                let value = line.value.as_f64();
                if let Some(last) = last_counter_value {
                    if value <= last {
                        println!("\n=== MONOTONICITY VIOLATION IN DRAIN ===");
                        println!("Counter line #{counter_line_number}");
                        println!("Current value: {value}, Previous: {last}");
                        println!("Time: {} ms, Fetch index: {}", line.time, line.fetch_index);
                        panic!("Counter not strictly increasing during DRAIN: {value} <= {last}");
                    }
                }
                last_counter_value = Some(value);
                counter_line_number += 1;
            } else if line.metric_name == "monotonic_gauge" {
                let value = line.value.as_f64();
                if let Some(last) = last_gauge_value {
                    if value <= last {
                        println!("\n=== MONOTONICITY VIOLATION IN DRAIN ===");
                        println!("Gauge line #{gauge_line_number}");
                        println!("Current value: {value}, Previous: {last}");
                        println!("Time: {} ms, Fetch index: {}", line.time, line.fetch_index);
                        panic!("Gauge not strictly increasing during DRAIN: {value} <= {last}");
                    }
                }
                last_gauge_value = Some(value);
                gauge_line_number += 1;
            }
        }

        // Verify we actually produced output
        assert!(last_counter_value.is_some(), "No counter values in output");
        assert!(last_gauge_value.is_some(), "No gauge values in output");
    }

    // Test that verifies the number of points to drain based on run duration.
    // For runs shorter than INTERVALS (60 seconds), we drain all N ticks. For
    // runs >= INTERVALS, we drain the unflushed window (up to INTERVALS ticks).
    #[test]
    fn drain_points_based_on_run_duration() {
        // Test case 1: Short run (30 seconds)
        {
            let writer = InMemoryWriter::new();
            let format = jsonl::Format::new(writer.clone());
            let clock = TestClock::new(0);
            let registry = Arc::new(Registry::new(AtomicStorage));
            let accumulator = Accumulator::new();
            let labels = FxHashMap::default();

            let recorder = crate::manager::CaptureRecorder {
                registry: Arc::clone(&registry),
            };

            let mut machine = StateMachine::new(
                Duration::from_secs(3600),
                format,
                1,
                DEFAULT_TICK_DURATION_MS as u64,
                registry,
                accumulator,
                labels,
                clock.clone(),
            );

            // Simulate 30 seconds of operation with metrics
            for _ in 0..30 {
                metrics::with_local_recorder(&recorder, || {
                    metrics::counter!("test_counter").increment(1);
                });
                clock.advance(1000); // Advance 1 second
                let _ = machine.next(Event::FlushTick);
            }

            // Shutdown and count drain output
            let _ = machine.next(Event::ShutdownSignaled);
            let lines = writer.parse_lines().expect("should parse");
            let unique_ticks: std::collections::HashSet<u64> =
                lines.iter().map(|l| l.fetch_index).collect();

            // We start at tick 0 and advance 30 times, so we have ticks 0-30 (31 total)
            assert_eq!(
                unique_ticks.len(),
                31,
                "30-second run should drain 31 unique ticks (0-30)"
            );
        }

        // Test case 2: Exactly INTERVALS seconds (60 seconds)
        {
            let writer = InMemoryWriter::new();
            let format = jsonl::Format::new(writer.clone());
            let clock = TestClock::new(0);
            let registry = Arc::new(Registry::new(AtomicStorage));
            let accumulator = Accumulator::new();
            let labels = FxHashMap::default();

            let recorder = crate::manager::CaptureRecorder {
                registry: Arc::clone(&registry),
            };

            let mut machine = StateMachine::new(
                Duration::from_secs(3600),
                format,
                1,
                DEFAULT_TICK_DURATION_MS as u64,
                registry,
                accumulator,
                labels,
                clock.clone(),
            );

            // Simulate 60 seconds with normal flush pattern
            for _ in 0..60 {
                metrics::with_local_recorder(&recorder, || {
                    metrics::counter!("test_counter").increment(1);
                });
                clock.advance(1000); // Advance 1 second
                let _ = machine.next(Event::FlushTick);
            }

            // Shutdown and count drain output
            let _ = machine.next(Event::ShutdownSignaled);
            let lines = writer.parse_lines().expect("should parse");
            let unique_ticks: std::collections::HashSet<u64> =
                lines.iter().map(|l| l.fetch_index).collect();

            // After 60 FlushTick events, we have:
            //
            // - Advanced to tick 60
            // - Flushed tick 0 (when we reached tick 60)
            // - Written metrics for ticks 0-60
            //
            // Total output includes both the flushed tick 0 and drained ticks
            // 1-60 (61 ticks total).
            assert_eq!(
                unique_ticks.len(),
                61,
                "60-second run should output 61 unique ticks total (0-60)"
            );
        }

        // Test case 3: Run for 120 seconds
        {
            let writer = InMemoryWriter::new();
            let format = jsonl::Format::new(writer.clone());
            let clock = TestClock::new(0);
            let registry = Arc::new(Registry::new(AtomicStorage));
            let accumulator = Accumulator::new();
            let labels = FxHashMap::default();

            let recorder = crate::manager::CaptureRecorder {
                registry: Arc::clone(&registry),
            };

            let mut machine = StateMachine::new(
                Duration::from_secs(3600),
                format,
                1,
                DEFAULT_TICK_DURATION_MS as u64,
                registry,
                accumulator,
                labels,
                clock.clone(),
            );

            // Simulate 120 seconds with normal flush pattern
            for _ in 0..120 {
                metrics::with_local_recorder(&recorder, || {
                    metrics::counter!("test_counter").increment(1);
                });
                clock.advance(1000); // Advance 1 second
                let _ = machine.next(Event::FlushTick);
            }

            // Shutdown and count drain output
            let _ = machine.next(Event::ShutdownSignaled);
            let lines = writer.parse_lines().expect("should parse");

            // Get unique ticks from drain (should be the unflushed window)
            let drain_ticks: std::collections::HashSet<u64> = lines
                .iter()
                .filter(|l| l.fetch_index >= 61) // Only count the drain output
                .map(|l| l.fetch_index)
                .collect();

            // After 120 ticks, we've flushed ticks 0-60, drain outputs ticks 61-120 (60 ticks)
            assert_eq!(
                drain_ticks.len(),
                60,
                "120-second run should drain 60 unique ticks (61-120)"
            );
        }

        // Test case 4: Run for 180 seconds
        {
            let writer = InMemoryWriter::new();
            let format = jsonl::Format::new(writer.clone());
            let clock = TestClock::new(0);
            let registry = Arc::new(Registry::new(AtomicStorage));
            let accumulator = Accumulator::new();
            let labels = FxHashMap::default();

            let recorder = crate::manager::CaptureRecorder {
                registry: Arc::clone(&registry),
            };

            let mut machine = StateMachine::new(
                Duration::from_secs(3600),
                format,
                1,
                DEFAULT_TICK_DURATION_MS as u64,
                registry,
                accumulator,
                labels,
                clock.clone(),
            );

            // Simulate 180 seconds with normal flush pattern
            for _ in 0..180 {
                metrics::with_local_recorder(&recorder, || {
                    metrics::counter!("test_counter").increment(1);
                });
                clock.advance(1000); // Advance 1 second
                let _ = machine.next(Event::FlushTick);
            }

            // Shutdown and count drain output
            let _ = machine.next(Event::ShutdownSignaled);
            let lines = writer.parse_lines().expect("should parse");

            // Get unique ticks from drain (should be the unflushed window)
            let drain_ticks: std::collections::HashSet<u64> = lines
                .iter()
                .filter(|l| l.fetch_index >= 121) // Only count the drain output
                .map(|l| l.fetch_index)
                .collect();

            // After 180 ticks, we've flushed ticks 0-120, drain outputs ticks 121-180 (60 ticks)
            assert_eq!(
                drain_ticks.len(),
                60,
                "180-second run should drain 60 unique ticks (121-180)"
            );
        }

        // Test case 5: Run with minimal flushes
        {
            let writer = InMemoryWriter::new();
            let format = jsonl::Format::new(writer.clone());
            let clock = TestClock::new(0);
            let registry = Arc::new(Registry::new(AtomicStorage));
            let accumulator = Accumulator::new();
            let labels = FxHashMap::default();

            let recorder = crate::manager::CaptureRecorder {
                registry: Arc::clone(&registry),
            };

            let mut machine = StateMachine::new(
                Duration::from_secs(3600),
                format,
                1,
                DEFAULT_TICK_DURATION_MS as u64,
                registry,
                accumulator,
                labels,
                clock.clone(),
            );

            // Write a metric and do one flush to get it into the accumulator
            metrics::with_local_recorder(&recorder, || {
                metrics::counter!("test_counter").increment(1);
            });

            // One FlushTick to record the metric at tick 0 and advance to tick 1
            let _ = machine.next(Event::FlushTick);

            // Shutdown immediately
            let _ = machine.next(Event::ShutdownSignaled);
            let lines = writer.parse_lines().expect("should parse");

            // Should have tick 0 from the single flush/advance
            let unique_ticks: std::collections::HashSet<u64> =
                lines.iter().map(|l| l.fetch_index).collect();

            assert_eq!(
                unique_ticks.len(),
                1,
                "Run with single FlushTick should drain tick 0"
            );
        }
    }

    #[test]
    fn histogram_end_to_end() {
        let writer = InMemoryWriter::new();
        let format = jsonl::Format::new(writer.clone());
        let clock = TestClock::new(0);
        let registry = Arc::new(Registry::new(AtomicStorage));
        let accumulator = Accumulator::new();
        let labels = FxHashMap::default();

        let mut machine = StateMachine::new(
            Duration::from_secs(60),
            format,
            1,
            DEFAULT_TICK_DURATION_MS as u64,
            registry,
            accumulator,
            labels,
            clock.clone(),
        );

        // Send histogram samples at tick 0
        for value in [10.0, 20.0, 30.0, 40.0, 50.0] {
            let hist = crate::metric::Histogram {
                key: Key::from_parts(
                    "test_histogram",
                    vec![metrics::Label::new("service", "api")],
                ),
                timestamp: clock.start(),
                value,
            };
            let _ = machine.next(Event::MetricReceived(Metric::Histogram(hist)));
        }

        // Advance and flush
        for _ in 0..61 {
            clock.advance(1000);
            let _ = machine.next(Event::FlushTick);
        }

        let _ = machine.next(Event::ShutdownSignaled);

        // Parse output
        let lines = writer.parse_lines().expect("should parse");

        // Should have 1 histogram row with value_histogram field
        let histogram_lines: Vec<_> = lines
            .iter()
            .filter(|l| l.metric_name == "test_histogram")
            .collect();

        assert_eq!(histogram_lines.len(), 1, "Should have 1 histogram row");

        let hist_line = histogram_lines[0];
        assert!(matches!(hist_line.metric_kind, line::MetricKind::Histogram));
        assert!(
            !hist_line.value_histogram.is_empty(),
            "Should have value_histogram"
        );

        // Verify sketch data
        let sketch_bytes = &hist_line.value_histogram;
        let dogsketch = Dogsketch::parse_from_bytes(sketch_bytes).expect("parse protobuf");
        let sketch = DDSketch::try_from(dogsketch).expect("convert");

        assert_eq!(sketch.min(), Some(10.0));
        assert_eq!(sketch.max(), Some(50.0));
        let median = sketch.quantile(0.5).unwrap();
        assert!((median - 30.0).abs() < 5.0, "median should be ~30");
    }
}
