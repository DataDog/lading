//! Pure state machine for capture manager event loop
//!
//! Following the pattern from `lading::generator::kubernetes::state_machine`, this
//! module extracts the select loop logic from `CaptureManager::start` into a pure,
//! testable state machine. The state machine owns all the capture state and provides
//! a single `next()` method that processes events and returns operations.

use std::{
    io::Write,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    accumulator::{self, Accumulator, MetricValue},
    json,
    manager::Clock,
    metric::{Counter, CounterValue, Gauge, GaugeValue, Metric},
};
use metrics::Key;
use metrics_util::registry::{AtomicStorage, Registry};
use rustc_hash::FxHashMap;
use std::sync::atomic::Ordering;
use tracing::{debug, info, warn};

/// Duration of a single `Accumulator` tick in milliseconds
pub(crate) const TICK_DURATION_MS: u128 = 1_000;

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
    /// Wrapper around [`std::io::Error`].
    #[error("[{context}] Io error: {err}")]
    Io {
        /// The context for the error, simple tag
        context: &'static str,
        /// The underlying error
        err: std::io::Error,
    },
    /// Wrapper around [`serde_json::Error`].
    #[error("Json serialization error: {0}")]
    Json(#[from] serde_json::Error),
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
pub(crate) struct StateMachine<W: Write, C: Clock> {
    /// Reference start time for timestamp-to-tick conversion
    start: Instant,
    /// Start time in milliseconds for deriving metric timestamps from ticks
    start_ms: u128,
    /// How long metrics can age before being discarded
    expiration: Duration,
    /// Output destination for metrics
    capture_writer: W,
    /// Registry containing current metric values
    registry: Arc<Registry<Key, AtomicStorage>>,
    /// Accumulator for windowed metrics
    accumulator: Accumulator,
    /// Labels attached to all metrics
    global_labels: FxHashMap<String, String>,
    /// Clock for time operations
    clock: C,
}

impl<W: Write, C: Clock> StateMachine<W, C> {
    /// Create a new state machine
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        start: Instant,
        expiration: Duration,
        capture_writer: W,
        registry: Arc<Registry<Key, AtomicStorage>>,
        accumulator: Accumulator,
        global_labels: FxHashMap<String, String>,
        clock: C,
    ) -> Self {
        let start_ms = clock.now_ms();
        Self {
            start,
            start_ms,
            expiration,
            capture_writer,
            registry,
            accumulator,
            global_labels,
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
            debug!(
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
        self.record_captures()?;

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
        Ok(Operation::Exit)
    }

    /// Convert an Instant timestamp to `Accumulator` logical tick time.
    #[inline]
    fn instant_to_tick(&self, timestamp: Instant) -> u64 {
        timestamp.duration_since(self.start).as_secs()
    }

    /// Record all current metrics from the registry and flush mature data
    fn record_captures(&mut self) -> Result<(), Error> {
        let now = self.clock.now();
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
        tracing::trace!(
            old_tick = old_tick,
            new_tick = self.accumulator.current_tick,
            "Advanced accumulator tick"
        );

        let mut line_count = 0;
        for (key, value, tick) in self.accumulator.flush() {
            // Calculate time from tick to ensure strictly increasing time values
            let time_ms = self.start_ms + (u128::from(tick) * TICK_DURATION_MS);
            self.write_metric_line(&key, &value, tick, time_ms)?;
            line_count += 1;
        }

        debug!("Recording {line_count} captures",);

        let elapsed = now.elapsed();
        if elapsed > Duration::from_secs(1) {
            tracing::error!("record_captures took {elapsed:?}, exceeded 1 second budget");
        }

        Ok(())
    }

    /// Drain all accumulated metrics and write them to the capture file
    fn drain_and_write(&mut self) -> Result<(), Error> {
        // Drain all remaining data from the accumulator. Collect to release the
        // mutable borrow so we can call write_metric_line.
        let drain: Vec<_> = self.accumulator.drain().collect();
        for (_, metrics) in drain {
            for (key, value, tick) in metrics {
                // Calculate time from tick to ensure strictly increasing time values
                let time_ms = self.start_ms + (u128::from(tick) * TICK_DURATION_MS);
                self.write_metric_line(&key, &value, tick, time_ms)?;
            }
        }

        Ok(())
    }

    fn write_metric_line(
        &mut self,
        key: &Key,
        value: &MetricValue,
        tick: u64,
        now_ms: u128,
    ) -> Result<(), Error> {
        let mut labels = self.global_labels.clone();
        for lbl in key.labels() {
            labels.insert(lbl.key().into(), lbl.value().into());
        }

        // Calculate when this metric was actually recorded based on its tick.
        let tick_age = self.accumulator.current_tick.saturating_sub(tick);
        let tick_age_ms = u128::from(tick_age) * TICK_DURATION_MS;
        // Skip any line that has expired.
        if tick_age_ms > self.expiration.as_millis() {
            return Ok(());
        }

        let line = match value {
            MetricValue::Counter(val) => json::Line {
                time: now_ms,
                fetch_index: tick,
                metric_name: key.name().into(),
                metric_kind: json::MetricKind::Counter,
                value: json::LineValue::Int(*val),
                labels,
            },
            MetricValue::Gauge(val) => json::Line {
                time: now_ms,
                fetch_index: tick,
                metric_name: key.name().into(),
                metric_kind: json::MetricKind::Gauge,
                value: json::LineValue::Float(*val),
                labels,
            },
        };

        let pyld = serde_json::to_string(&line)?;
        self.capture_writer
            .write_all(pyld.as_bytes())
            .map_err(|err| Error::Io {
                context: "payload write",
                err,
            })?;
        self.capture_writer
            .write_all(b"\n")
            .map_err(|err| Error::Io {
                context: "newline write",
                err,
            })?;
        self.capture_writer.flush().map_err(|err| Error::Io {
            context: "flush",
            err,
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::registry::{AtomicStorage, Registry};
    use proptest::prelude::*;
    use std::{
        io,
        sync::{Arc, Mutex},
        time::Duration,
    };

    /// In-memory writer for testing
    #[derive(Clone)]
    struct InMemoryWriter {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    impl InMemoryWriter {
        fn new() -> Self {
            Self {
                buffer: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn parse_lines(&self) -> Result<Vec<json::Line>, serde_json::Error> {
            let buffer = self.buffer.lock().unwrap();
            let content_str = String::from_utf8_lossy(&buffer);
            content_str
                .lines()
                .filter(|line| !line.is_empty())
                .map(serde_json::from_str)
                .collect()
        }
    }

    impl Write for InMemoryWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.buffer.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// Test clock for deterministic time control
    #[derive(Clone)]
    struct TestClock {
        time_ms: Arc<Mutex<u128>>,
        start_instant: Instant,
    }

    impl TestClock {
        fn new(initial_time_ms: u128) -> Self {
            Self {
                time_ms: Arc::new(Mutex::new(initial_time_ms)),
                start_instant: Instant::now(),
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

    impl Clock for TestClock {
        fn now_ms(&self) -> u128 {
            *self.time_ms.lock().unwrap()
        }

        fn now(&self) -> Instant {
            let time_ms = *self.time_ms.lock().unwrap();
            self.start_instant + Duration::from_millis(time_ms as u64)
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
                (0u128..=1_000u128).prop_map(|millis| CaptureOp::AdvanceTime { millis }),
                (0u128..=500u128).prop_map(|millis| CaptureOp::BackwardTime { millis }),
                Just(CaptureOp::FlushTick),
            ]
            .boxed()
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10_000))]
        #[test]
        fn state_machine_output_satisfies_invariants(
            ops in prop::collection::vec(any::<CaptureOp>(), 10..50)
        ) {
            let writer = InMemoryWriter::new();
            let clock = TestClock::new(1000);
            let start = clock.now();
            let registry = Arc::new(Registry::new(AtomicStorage));
            let accumulator = Accumulator::new();
            let labels = FxHashMap::default();

            let recorder = crate::manager::CaptureRecorder {
                registry: Arc::clone(&registry),
            };

            let mut machine = StateMachine::new(
                start,
                Duration::from_secs(60),
                writer.clone(),
                registry,
                accumulator,
                labels,
                clock.clone(),
            );

            // Execute operations. FlushTick is independent of time advancement,
            // allowing us to test scenarios where multiple flushes happen at
            // the same clock millisecond.
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
                    CaptureOp::HistoricalCounterIncr { name, value, tick_offset } => {
                        let timestamp = clock.now()
                            .checked_sub(Duration::from_secs(tick_offset))
                            .unwrap_or(start);
                        let counter = Counter {
                            key: Key::from_name(name),
                            timestamp,
                            value: CounterValue::Increment(value),
                        };
                        let _ = machine.next(Event::MetricReceived(Metric::Counter(counter)));
                    }
                    CaptureOp::HistoricalCounterAbs { name, value, tick_offset } => {
                        let timestamp = clock.now()
                            .checked_sub(Duration::from_secs(tick_offset))
                            .unwrap_or(start);
                        let counter = Counter {
                            key: Key::from_name(name),
                            timestamp,
                            value: CounterValue::Absolute(value),
                        };
                        let _ = machine.next(Event::MetricReceived(Metric::Counter(counter)));
                    }
                    CaptureOp::HistoricalGaugeIncr { name, value, tick_offset } => {
                        let timestamp = clock.now()
                            .checked_sub(Duration::from_secs(tick_offset))
                            .unwrap_or(start);
                        let gauge = Gauge {
                            key: Key::from_name(name),
                            timestamp,
                            value: GaugeValue::Increment(value),
                        };
                        let _ = machine.next(Event::MetricReceived(Metric::Gauge(gauge)));
                    }
                    CaptureOp::HistoricalGaugeDec { name, value, tick_offset } => {
                        let timestamp = clock.now()
                            .checked_sub(Duration::from_secs(tick_offset))
                            .unwrap_or(start);
                        let gauge = Gauge {
                            key: Key::from_name(name),
                            timestamp,
                            value: GaugeValue::Decrement(value),
                        };
                        let _ = machine.next(Event::MetricReceived(Metric::Gauge(gauge)));
                    }
                    CaptureOp::HistoricalGaugeSet { name, value, tick_offset } => {
                        let timestamp = clock.now()
                            .checked_sub(Duration::from_secs(tick_offset))
                            .unwrap_or(start);
                        let gauge = Gauge {
                            key: Key::from_name(name),
                            timestamp,
                            value: GaugeValue::Set(value),
                        };
                        let _ = machine.next(Event::MetricReceived(Metric::Gauge(gauge)));
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

            // Simulate shutdown: drain all remaining metrics
            let _ = machine.next(Event::ShutdownSignaled);

            // Parse the output and validate it satisfies all invariants
            let lines = writer.parse_lines().unwrap();
            let result = crate::validate::validate_lines(&lines);

            prop_assert!(
                result.is_valid(),
                "Invariant violation detected:\n  Line: {line}\n  Series: {series}\n  Message: {msg}",
                line = result.first_error.as_ref().map(|(l, _, _)| l).unwrap_or(&0),
                series = result.first_error.as_ref().map(|(_, s, _)| s.as_str()).unwrap_or(""),
                msg = result.first_error.as_ref().map(|(_, _, m)| m.as_str()).unwrap_or("")
            );
        }
    }

    #[test]
    fn metric_received_adds_to_accumulator() {
        let writer = InMemoryWriter::new();
        let clock = TestClock::new(1000);
        let start = clock.now();
        let registry = Arc::new(Registry::new(AtomicStorage));
        let accumulator = Accumulator::new();
        let labels = FxHashMap::default();

        let mut machine = StateMachine::new(
            start,
            Duration::from_secs(60),
            writer,
            registry,
            accumulator,
            labels,
            clock.clone(),
        );

        let counter = Metric::Counter(Counter {
            key: Key::from_name("test_counter"),
            timestamp: start,
            value: CounterValue::Absolute(42),
        });

        let result = machine.next(Event::MetricReceived(counter));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Operation::Continue);
    }

    #[test]
    fn channel_closed_returns_exit() {
        let writer = InMemoryWriter::new();
        let clock = TestClock::new(1000);
        let start = clock.now();
        let registry = Arc::new(Registry::new(AtomicStorage));
        let accumulator = Accumulator::new();
        let labels = FxHashMap::default();

        let mut machine = StateMachine::new(
            start,
            Duration::from_secs(60),
            writer,
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
        let clock = TestClock::new(1000);
        let start = clock.now();
        let registry = Arc::new(Registry::new(AtomicStorage));
        let accumulator = Accumulator::new();
        let labels = FxHashMap::default();

        let mut machine = StateMachine::new(
            start,
            Duration::from_secs(60),
            writer,
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
        let clock = TestClock::new(1000);
        let start = Instant::now();
        let registry = Arc::new(Registry::new(AtomicStorage));
        let accumulator = Accumulator::new();
        let labels = FxHashMap::default();

        let mut machine = StateMachine::new(
            start,
            Duration::from_secs(60),
            writer,
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
    fn drift_correction_advances_multiple_ticks() {
        let writer = InMemoryWriter::new();
        let clock = TestClock::new(1000);
        let start = clock.now();
        let registry = Arc::new(Registry::new(AtomicStorage));
        let accumulator = Accumulator::new();
        let labels = FxHashMap::default();

        let mut machine = StateMachine::new(
            start,
            Duration::from_secs(60),
            writer,
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
}
