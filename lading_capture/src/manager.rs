//! Capture and record lading's internal metrics
//!
//! The manner in which lading instruments its target is pretty simple: we use
//! the [`metrics`] library to record factual things about interaction with the
//! target and then write all that out to disk for later analysis. This means
//! that the generator, blackhole etc code are unaware of anything other than
//! their [`metrics`] integration while [`CaptureManager`] need only hook into
//! that same crate.

use std::{
    io::{self, BufWriter, Write},
    path::PathBuf,
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use tokio::sync::Mutex;
use tokio::{fs, sync::mpsc, time};

use crate::{
    accumulator::{self, Accumulator, MetricValue},
    json,
    metric::{Counter, CounterValue, Gauge, GaugeValue, Metric},
};
use metrics::Key;
use metrics_util::registry::{AtomicStorage, Registry};
use rustc_hash::FxHashMap;
use std::sync::LazyLock;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

/// Duration of a single `Accumulator` tick in milliseconds, drives the
/// `CaptureManager` polling interval.
const TICK_DURATION_MS: u128 = 1_000;

pub(crate) struct Sender {
    pub(crate) snd: mpsc::Sender<Metric>,
}

pub(crate) static HISTORICAL_SENDER: LazyLock<Mutex<Option<Sender>>> =
    LazyLock::new(|| Mutex::new(None));

/// Errors produced by [`CaptureManager`]
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Wrapper around [`SetRecorderError`].
    #[error("Failed to set recorder")]
    SetRecorderError,
    /// Wrapper around [`io::Error`].
    #[error("[{context} Io error: {err}")]
    Io {
        /// The context for the error, simple tag
        context: &'static str,
        /// The underlying error
        err: io::Error,
    },
    #[error("Time provided is later than right now : {0}")]
    /// Wrapper around [`std::time::SystemTimeError`].
    SystemTime(#[from] std::time::SystemTimeError),
    /// Wrapper around [`serde_json::Error`].
    #[error("Json serialization error: {0}")]
    Json(#[from] serde_json::Error),
    /// Error used for invalid capture path
    #[error("Invalid capture path")]
    CapturePath,
    /// Accumulator errors
    #[error(transparent)]
    Accumulator(#[from] accumulator::Error),
}

/// Clock abstraction for controllable time in tests
///
/// Following the pattern from `lading_throttle`, allows production code to
/// use real system time while tests can inject a controllable clock for
/// deterministic behavior.
// NOTE I want to extract both clocks out into a common lading_clock but will do
// so in a separate thread of work.
pub trait Clock {
    /// Returns the current time in milliseconds since `UNIX_EPOCH`
    fn now_ms(&self) -> u128;
}

/// Production clock implementation using real system time
#[derive(Debug, Copy, Clone)]
pub struct RealClock;

impl Clock for RealClock {
    fn now_ms(&self) -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time is before UNIX_EPOCH")
            .as_millis()
    }
}

/// Wrangles internal metrics into capture files
///
/// This struct is responsible for capturing all internal metrics sent through
/// [`metrics`] and periodically writing them to disk with format
/// [`json::Line`].
pub struct CaptureManager<W: Write + Send, C: Clock = RealClock> {
    /// Reference start time used to convert Instant timestamps to logical ticks.
    /// Initialized at `CaptureManager` construction time, before any historical
    /// metrics are sent. This synchronizes with `accumulator.current_tick` which
    /// begins at 0 and advances every `TICK_DURATION_MS`.
    start: Instant,
    expiration: Duration,
    capture_writer: W,
    shutdown: Option<lading_signal::Watcher>,
    _experiment_started: lading_signal::Watcher,
    target_running: lading_signal::Watcher,
    registry: Arc<Registry<Key, AtomicStorage>>,
    accumulator: Accumulator,
    run_id: Uuid,
    global_labels: FxHashMap<String, String>,
    snd: mpsc::Sender<Metric>,
    recv: mpsc::Receiver<Metric>,
    clock: C,
}

impl<W: Write + Send, C: Clock> std::fmt::Debug for CaptureManager<W, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CaptureManager")
            .field("start", &self.start)
            .field("accumulator", &self.accumulator)
            .field("global_labels", &self.global_labels)
            .finish_non_exhaustive()
    }
}

impl<W: Write + Send, C: Clock> CaptureManager<W, C> {
    /// Create a new [`CaptureManager`] with a custom writer and clock
    pub fn new_with_writer(
        capture_writer: W,
        shutdown: lading_signal::Watcher,
        experiment_started: lading_signal::Watcher,
        target_running: lading_signal::Watcher,
        expiration: Duration,
        clock: C,
    ) -> Self {
        let registry = Arc::new(Registry::new(AtomicStorage));
        let run_id = Uuid::new_v4();

        // Capture start time for timestamp-to-tick conversion. This is the
        // reference point for all historical metrics: accumulator.current_tick
        // begins at 0 and this Instant represents tick 0 in real time.
        let now = Instant::now();

        let (snd, recv) = mpsc::channel(10_000); // total arbitrary constant
        let accumulator = Accumulator::new();

        Self {
            start: now,
            expiration,
            capture_writer,
            shutdown: Some(shutdown),
            _experiment_started: experiment_started,
            target_running,
            registry,
            accumulator,
            run_id,
            global_labels: FxHashMap::default(),
            snd,
            recv,
            clock,
        }
    }

    /// Install the [`CaptureManager`] as global [`metrics::Recorder`]
    ///
    /// # Errors
    ///
    /// Returns an error if there is already a global recorder set.
    pub fn install(&self) -> Result<(), Error> {
        let recorder = CaptureRecorder {
            registry: Arc::clone(&self.registry),
        };
        metrics::set_global_recorder(recorder).map_err(|_| Error::SetRecorderError)?;
        Ok(())
    }

    /// Add a global label to all metrics managed by [`CaptureManager`].
    pub fn add_global_label<K, V>(&mut self, key: K, value: V)
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.global_labels.insert(key.into(), value.into());
    }

    /// Drain all accumulated metrics and write them to the capture file.
    ///
    /// Called during shutdown to flush any remaining metrics.
    ///
    /// # Errors
    ///
    /// Returns an error if writing metrics fails.
    fn drain_and_write(&mut self) -> Result<(), Error> {
        let run_id = self.run_id;
        let now_ms = self.clock.now_ms();

        // Drain all remaining data from the accumulator. Collect to
        // release the mutable borrow so we can call write_metric_line.
        let drain: Vec<_> = self.accumulator.drain().collect();
        for (_, metrics) in drain {
            for (key, value, tick) in metrics {
                self.write_metric_line(&key, &value, tick, now_ms, run_id)?;
            }
        }

        Ok(())
    }

    /// Convert an Instant timestamp to `Accumulator` logical tick time.
    #[inline]
    fn instant_to_tick(&self, timestamp: Instant) -> u64 {
        timestamp.duration_since(self.start).as_secs()
    }

    fn write_metric_line(
        &mut self,
        key: &Key,
        value: &MetricValue,
        tick: u64,
        now_ms: u128,
        run_id: Uuid,
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
                run_id,
                time: now_ms,
                fetch_index: tick,
                metric_name: key.name().into(),
                metric_kind: json::MetricKind::Counter,
                value: json::LineValue::Int(*val),
                labels,
            },
            MetricValue::Gauge(val) => json::Line {
                run_id,
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

    fn record_captures(&mut self) -> Result<(), Error> {
        let now = Instant::now();
        let tick = self.accumulator.current_tick;

        for (k, c) in self.registry.get_counter_handles() {
            let val = c.load(Ordering::Relaxed);
            let counter = Counter {
                key: k,
                timestamp: now,
                value: CounterValue::Absolute(val),
            };
            self.accumulator.counter(counter, tick)?;
        }

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

        let now_ms = self.clock.now_ms();
        let run_id = self.run_id;

        let mut line_count = 0;
        for (key, value, tick) in self.accumulator.flush() {
            self.write_metric_line(&key, &value, tick, now_ms, run_id)?;
            line_count += 1;
        }

        debug!("Recording {line_count} captures",);

        let elapsed = now.elapsed();
        if elapsed > Duration::from_secs(1) {
            error!("record_captures took {elapsed:?}, exceeded 1 second budget");
        }

        Ok(())
    }

    /// Run [`CaptureManager`] to completion
    ///
    /// Once a second any metrics produced by this program are flushed to disk.
    /// This function only exits once a shutdown signal is received.
    ///
    /// # Panics
    ///
    /// Does not intentionally panic.
    ///
    /// # Errors
    ///
    /// Will return an error if there is already a global recorder set.
    #[allow(clippy::cast_possible_truncation)]
    pub async fn start(mut self) -> Result<(), Error> {
        // Initialize historical sender to allow generators to send metrics with
        // Instant timestamps. Manager converts these to ticks using self.start
        // as the reference point synchronized with accumulator.current_tick.
        *HISTORICAL_SENDER.lock().await = Some(Sender {
            snd: self.snd.clone(),
        });

        // Installing the recorder immediately on startup. This does _not_ wait
        // on experiment_started signal, so warmup data will be included in the
        // capture.
        self.install()?;
        info!("Capture manager installed, recording to capture file.");

        while let Ok(false) = self.target_running.try_recv() {
            time::sleep(Duration::from_millis(100)).await;
        }

        let mut flush_interval = time::interval(Duration::from_millis(TICK_DURATION_MS as u64));
        let shutdown_wait = self
            .shutdown
            .take()
            .expect("shutdown watcher must be present")
            .recv();
        tokio::pin!(shutdown_wait);

        loop {
            tokio::select! {
                val = self.recv.recv() => {
                    match val {
                        Some(Metric::Counter(c)) => {
                            let tick = self.instant_to_tick(c.timestamp);
                             let _ = self.accumulator.counter(c, tick);
                        }
                        Some(Metric::Gauge(g)) => {
                            let tick = self.instant_to_tick(g.timestamp);
                            let _ = self.accumulator.gauge(g, tick);
                        }
                        None => {
                            warn!("Timestamped metrics unexpected transmission shutdown");
                            return Ok(());
                        }
                    }
                }
                _ = flush_interval.tick() => {
                    let tick_start = Instant::now();
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

                    self.record_captures()?;

                    let record_duration = tick_start.elapsed();
                    if record_duration > Duration::from_secs(1) {
                        warn!(
                            duration = ?record_duration,
                            "Recording capture took more than 1s"
                        );
                    }
                },
                () = &mut shutdown_wait => {
                    info!("shutdown signal received, flushing all remaining metrics");
                    self.drain_and_write()?;
                    return Ok(());
                }
            }
        }
    }
}

impl CaptureManager<BufWriter<std::fs::File>, RealClock> {
    /// Create a new [`CaptureManager`] with file-based writer
    ///
    /// # Errors
    ///
    /// Function will error if the underlying capture file cannot be opened.
    pub async fn new(
        capture_path: PathBuf,
        shutdown: lading_signal::Watcher,
        experiment_started: lading_signal::Watcher,
        target_running: lading_signal::Watcher,
        expiration: Duration,
    ) -> Result<Self, io::Error> {
        let fp = fs::File::create(&capture_path).await?;
        let fp = fp.into_std().await;
        let writer = BufWriter::new(fp);

        Ok(Self::new_with_writer(
            writer,
            shutdown,
            experiment_started,
            target_running,
            expiration,
            RealClock,
        ))
    }
}

struct CaptureRecorder {
    registry: Arc<Registry<Key, AtomicStorage>>,
}

impl metrics::Recorder for CaptureRecorder {
    fn describe_counter(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
        // nothing, intentionally
    }

    fn describe_gauge(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
        // nothing, intentionally
    }

    fn describe_histogram(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
        // nothing, intentionally
    }

    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        self.registry
            .get_or_create_counter(key, |c| metrics::Counter::from_arc(c.clone()))
    }

    fn register_gauge(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        self.registry
            .get_or_create_gauge(key, |c| metrics::Gauge::from_arc(c.clone()))
    }

    fn register_histogram(
        &self,
        _key: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        // nothing, intentionally
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use crate::json;

    /// In-memory writer for testing
    ///
    /// Captures all writes to a buffer that can be inspected after the test.
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
    }

    impl TestClock {
        fn new(initial_time_ms: u128) -> Self {
            Self {
                time_ms: Arc::new(Mutex::new(initial_time_ms)),
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
    }

    #[derive(Clone)]
    enum CaptureOp {
        WriteCounter { name: String, value: u64 },
        WriteGauge { name: String, value: f64 },
        AdvanceTime { millis: u128 },
        BackwardTime { millis: u128 },
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
                Self::AdvanceTime { millis } => write!(f, "AdvanceTime({millis})"),
                Self::BackwardTime { millis } => write!(f, "BackwardTime({millis})"),
            }
        }
    }

    impl Arbitrary for CaptureOp {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            prop_oneof![
                100 => ("[a-z]{1,5}", any::<u64>())
                    .prop_map(|(name, value)| CaptureOp::WriteCounter { name, value }),
                100 => (
                    "[a-z]{1,5}",
                    any::<f64>().prop_filter("must be finite", |f| f.is_finite())
                )
                    .prop_map(|(name, value)| CaptureOp::WriteGauge { name, value }),
                1 => (0u128..=500u128).prop_map(|millis| CaptureOp::AdvanceTime { millis }),
                1 => (0u128..=500u128).prop_map(|millis| CaptureOp::BackwardTime { millis }),
            ]
            .boxed()
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100_000))]
        #[test]
        fn capture_output_satisfies_invariants(
            ops in prop::collection::vec(any::<CaptureOp>(), 10..50)
        ) {
            let writer = InMemoryWriter::new();
            let clock = TestClock::new(1000);
            let (shutdown_rx, shutdown_tx) = lading_signal::signal();
            let (experiment_rx, _experiment_tx) = lading_signal::signal();
            let (target_rx, target_tx) = lading_signal::signal();

            target_tx.signal();

            let mut manager = CaptureManager::new_with_writer(
                writer.clone(),
                shutdown_rx,
                experiment_rx,
                target_rx,
                Duration::from_secs(60),
                clock.clone(),
            );

            let recorder = CaptureRecorder {
                registry: Arc::clone(&manager.registry),
            };

            // Execute operations.Track next tick boundary to model the real
            // interval flush timer.
            let mut next_tick_time = clock.now_ms() + TICK_DURATION_MS;
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
                    CaptureOp::AdvanceTime { millis } => {
                        clock.advance(millis);
                        let current_time = clock.now_ms();
                        while current_time >= next_tick_time {
                            let _ = manager.record_captures();
                            next_tick_time += TICK_DURATION_MS;
                        }
                    }
                    CaptureOp::BackwardTime { millis } => {
                        clock.rewind(millis);
                        let current_time = clock.now_ms();
                        while current_time < next_tick_time.saturating_sub(TICK_DURATION_MS) {
                            next_tick_time = next_tick_time.saturating_sub(TICK_DURATION_MS);
                        }
                    }
                }
            }

            // Simulate shutdown with drain
            shutdown_tx.signal();
            let _ = manager.drain_and_write();

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
    fn metrics_macros_create_capture_lines() {
        let writer = InMemoryWriter::new();
        let clock = TestClock::new(1000);
        let (shutdown_rx, _shutdown_tx) = lading_signal::signal();
        let (experiment_rx, _experiment_tx) = lading_signal::signal();
        let (target_rx, target_tx) = lading_signal::signal();

        target_tx.signal();

        let manager = CaptureManager::new_with_writer(
            writer.clone(),
            shutdown_rx,
            experiment_rx,
            target_rx,
            Duration::from_secs(60),
            clock,
        );

        let recorder = CaptureRecorder {
            registry: Arc::clone(&manager.registry),
        };

        let mut expected_counters: FxHashMap<metrics::Key, u64> = FxHashMap::default();
        let mut expected_gauges: FxHashMap<metrics::Key, f64> = FxHashMap::default();

        metrics::with_local_recorder(&recorder, || {
            metrics::counter!("counter_increment").increment(10);
            metrics::counter!("counter_increment").increment(20);
            expected_counters.insert(metrics::Key::from_name("counter_increment"), 30);

            metrics::counter!("counter_absolute").absolute(18);
            metrics::counter!("counter_absolute").absolute(42);
            expected_counters.insert(metrics::Key::from_name("counter_absolute"), 42);

            metrics::counter!("counter_with_labels", "env" => "test", "region" => "us-east")
                .increment(100);
            expected_counters.insert(
                metrics::Key::from_parts(
                    "counter_with_labels",
                    vec![
                        metrics::Label::new("env", "test"),
                        metrics::Label::new("region", "us-east"),
                    ],
                ),
                100,
            );

            metrics::gauge!("gauge_set").set(3.14);
            expected_gauges.insert(metrics::Key::from_name("gauge_set"), 3.14);

            metrics::gauge!("gauge_increment").set(10.0);
            metrics::gauge!("gauge_increment").increment(5.0);
            expected_gauges.insert(metrics::Key::from_name("gauge_increment"), 15.0);

            metrics::gauge!("gauge_decrement").set(10.0);
            metrics::gauge!("gauge_decrement").decrement(3.0);
            expected_gauges.insert(metrics::Key::from_name("gauge_decrement"), 7.0);

            metrics::gauge!("gauge_with_labels", "service" => "api", "host" => "server1").set(99.9);
            expected_gauges.insert(
                metrics::Key::from_parts(
                    "gauge_with_labels",
                    vec![
                        metrics::Label::new("service", "api"),
                        metrics::Label::new("host", "server1"),
                    ],
                ),
                99.9,
            );
        });

        // Collect metrics from registry
        let mut lines = Vec::new();
        for (key, counter) in manager.registry.get_counter_handles() {
            let value = counter.load(Ordering::Acquire);
            let mut labels = FxHashMap::default();
            for lbl in key.labels() {
                labels.insert(lbl.key().to_string(), lbl.value().to_string());
            }
            lines.push(json::Line {
                run_id: manager.run_id,
                time: 0,
                fetch_index: 0,
                metric_name: key.name().into(),
                metric_kind: json::MetricKind::Counter,
                value: json::LineValue::Int(value),
                labels,
            });
        }

        for (key, gauge) in manager.registry.get_gauge_handles() {
            let bits = gauge.load(Ordering::Acquire);
            // There's no atomic f64 so we have to convert from AtomicU64
            let value = f64::from_bits(bits);
            let mut labels = FxHashMap::default();
            for lbl in key.labels() {
                labels.insert(lbl.key().to_string(), lbl.value().to_string());
            }
            lines.push(json::Line {
                run_id: manager.run_id,
                time: 0,
                fetch_index: 0,
                metric_name: key.name().into(),
                metric_kind: json::MetricKind::Gauge,
                value: json::LineValue::Float(value),
                labels,
            });
        }

        for (key, expected_value) in &expected_counters {
            let mut labels_map = FxHashMap::default();
            for label in key.labels() {
                labels_map.insert(label.key().to_string(), label.value().to_string());
            }

            let line = lines.iter().find(|line| {
                line.metric_name == key.name()
                    && matches!(line.metric_kind, json::MetricKind::Counter)
                    && line.labels == labels_map
            });

            assert!(
                line.is_some(),
                "Expected to find counter '{}' with labels {:?}",
                key.name(),
                labels_map
            );

            let line = line.unwrap();

            let val = match line.value {
                json::LineValue::Int(val) => val,
                json::LineValue::Float(_) => {
                    assert!(
                        false,
                        "Expected Int value for counter {}, got Float",
                        key.name()
                    );
                    return;
                }
            };

            assert_eq!(
                val,
                *expected_value,
                "Counter {} value mismatch",
                key.name()
            );
        }

        for (key, expected_value) in &expected_gauges {
            let mut labels_map = FxHashMap::default();
            for label in key.labels() {
                labels_map.insert(label.key().to_string(), label.value().to_string());
            }

            let line = lines.iter().find(|line| {
                line.metric_name == key.name()
                    && matches!(line.metric_kind, json::MetricKind::Gauge)
                    && line.labels == labels_map
            });

            assert!(
                line.is_some(),
                "Expected to find gauge '{}' with labels {:?}",
                key.name(),
                labels_map
            );

            let line = line.unwrap();

            let val = match line.value {
                json::LineValue::Float(val) => val,
                json::LineValue::Int(_) => {
                    assert!(
                        false,
                        "Expected Float value for gauge {}, got Int",
                        key.name()
                    );
                    return;
                }
            };

            let diff = (val - expected_value).abs();
            assert!(diff < 0.0001, "Gauge {} value mismatch", key.name());
        }
    }

    #[test]
    fn fetch_index_monotonic_and_accumulator_window_enforced() {
        let writer = InMemoryWriter::new();
        let clock = TestClock::new(1000);
        let (shutdown_rx, _shutdown_tx) = lading_signal::signal();
        let (experiment_rx, _experiment_tx) = lading_signal::signal();
        let (target_rx, target_tx) = lading_signal::signal();

        target_tx.signal();

        let mut manager = CaptureManager::new_with_writer(
            writer.clone(),
            shutdown_rx,
            experiment_rx,
            target_rx,
            Duration::from_secs(60),
            clock,
        );

        let recorder = CaptureRecorder {
            registry: Arc::clone(&manager.registry),
        };

        metrics::with_local_recorder(&recorder, || {
            metrics::counter!("test_counter").increment(100);
            metrics::gauge!("test_gauge").set(42.0);
        });

        let mut last_fetch_index: Option<u64> = None;

        // Flush (INTERVALS - 1) times, metrics should not appear yet
        for _ in 0..(accumulator::INTERVALS - 1) {
            manager.record_captures().unwrap();
            let lines = writer.parse_lines().unwrap();
            assert!(
                lines.is_empty(),
                "Expected no lines before INTERVALS flushes, but got {}",
                lines.len()
            );

            // Track fetch_index even for empty results
            for line in lines {
                if let Some(prev) = last_fetch_index {
                    assert!(
                        line.fetch_index >= prev,
                        "fetch_index not monotonic: {} < {}",
                        line.fetch_index,
                        prev
                    );
                }
                last_fetch_index = Some(line.fetch_index);
            }
        }

        // On INTERVALS-th flush, metrics should appear
        manager.record_captures().unwrap();
        let lines = writer.parse_lines().unwrap();

        assert_eq!(
            lines.len(),
            2,
            "Expected 2 lines (counter + gauge) after INTERVALS flushes, got {}",
            lines.len()
        );

        // Verify fetch_index values are valid and monotonic
        for line in &lines {
            if let Some(prev) = last_fetch_index {
                assert!(
                    line.fetch_index >= prev,
                    "fetch_index not monotonic: {} < {}",
                    line.fetch_index,
                    prev
                );
            }
            last_fetch_index = Some(line.fetch_index);
        }

        let counter_line = lines
            .iter()
            .find(|l| l.metric_name == "test_counter")
            .expect("Expected to find test_counter");
        assert!(
            matches!(counter_line.value, json::LineValue::Int(100)),
            "Expected counter value 100"
        );

        let gauge_line = lines
            .iter()
            .find(|l| l.metric_name == "test_gauge")
            .expect("Expected to find test_gauge");
        let gauge_val = match gauge_line.value {
            json::LineValue::Float(v) => v,
            _ => panic!("Expected Float value for gauge"),
        };
        assert!(
            (gauge_val - 42.0).abs() < 0.0001,
            "Expected gauge value 42.0"
        );
    }

    #[test]
    fn fetch_index_strictly_increases_across_flushes() {
        let writer = InMemoryWriter::new();
        let clock = TestClock::new(1000);
        let (shutdown_rx, _shutdown_tx) = lading_signal::signal();
        let (experiment_rx, _experiment_tx) = lading_signal::signal();
        let (target_rx, target_tx) = lading_signal::signal();

        target_tx.signal();

        let mut manager = CaptureManager::new_with_writer(
            writer.clone(),
            shutdown_rx,
            experiment_rx,
            target_rx,
            Duration::from_secs(60),
            clock,
        );

        let recorder = CaptureRecorder {
            registry: Arc::clone(&manager.registry),
        };

        metrics::with_local_recorder(&recorder, || {
            metrics::counter!("counter1").increment(10);
        });

        // Run enough flushes to see metrics appear
        for _ in 0..(accumulator::INTERVALS + 10) {
            manager.record_captures().unwrap();
        }

        // Collect all unique fetch_index values from the captured output
        let lines = writer.parse_lines().unwrap();
        let mut unique_fetch_indices: Vec<u64> = lines
            .iter()
            .map(|l| l.fetch_index)
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        unique_fetch_indices.sort_unstable();

        // Verify fetch_index values are strictly increasing
        for window in unique_fetch_indices.windows(2) {
            assert!(
                window[1] > window[0],
                "fetch_index not strictly increasing: {} >= {}",
                window[0],
                window[1]
            );
        }

        // Verify we got at least some metrics
        assert!(
            !unique_fetch_indices.is_empty(),
            "Expected to see at least one unique fetch_index"
        );
    }

    #[test]
    fn drain_with_same_timestamp_violates_invariants() {
        let writer = InMemoryWriter::new();
        let clock = TestClock::new(1000);
        let (shutdown_rx, shutdown_tx) = lading_signal::signal();
        let (experiment_rx, _experiment_tx) = lading_signal::signal();
        let (target_rx, target_tx) = lading_signal::signal();

        target_tx.signal();

        let mut manager = CaptureManager::new_with_writer(
            writer.clone(),
            shutdown_rx,
            experiment_rx,
            target_rx,
            Duration::from_secs(60),
            clock.clone(),
        );

        let recorder = CaptureRecorder {
            registry: Arc::clone(&manager.registry),
        };

        // Write metrics across multiple ticks
        metrics::with_local_recorder(&recorder, || {
            metrics::counter!("test_counter").increment(10);
        });

        // Advance through several ticks to build up data
        for _ in 0..(accumulator::INTERVALS + 5) {
            manager.record_captures().unwrap();
            metrics::with_local_recorder(&recorder, || {
                metrics::counter!("test_counter").increment(1);
            });
        }

        // Now simulate shutdown drain
        shutdown_tx.signal();
        manager.drain_and_write().unwrap();

        // Parse and validate using the canonical validation logic
        let lines = writer.parse_lines().unwrap();

        let result = crate::validate::validate_lines(&lines);

        // Assert that all invariants are satisfied
        assert!(
            result.is_valid(),
            "Invariant violation detected:\n  Line: {:?}\n  Category: {:?}\n  Message: {:?}",
            result.first_error.as_ref().map(|(l, _, _)| l),
            result.first_error.as_ref().map(|(_, c, _)| c),
            result.first_error.as_ref().map(|(_, _, m)| m)
        );
    }
}
