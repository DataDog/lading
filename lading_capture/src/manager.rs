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

use crate::accumulator::{self, Accumulator, MetricValue};
use crate::json;
use metrics::Key;
use metrics_util::registry::{AtomicStorage, Registry};
use rustc_hash::FxHashMap;
use std::sync::LazyLock;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

pub(crate) struct Sender {
    pub(crate) start: Instant,
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

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum CounterValue {
    Increment(u64),
    Absolute(u64),
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum GaugeValue {
    Increment(f64),
    Decrement(f64),
    Set(f64),
}

#[derive(Debug, Clone)]
pub(crate) struct Counter {
    pub key: Key,
    pub tick_offset: u8,
    pub value: CounterValue,
}

#[derive(Debug, Clone)]
pub(crate) struct Gauge {
    pub key: Key,
    pub tick_offset: u8,
    pub value: GaugeValue,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum Metric {
    /// Counter increment
    Counter(Counter),
    /// Gauge set
    Gauge(Gauge),
}

/// Wrangles internal metrics into capture files
///
/// This struct is responsible for capturing all internal metrics sent through
/// [`metrics`] and periodically writing them to disk with format
/// [`json::Line`].
pub struct CaptureManager<W: Write + Send> {
    fetch_index: u64,
    #[allow(dead_code)]
    start: Instant,
    capture_writer: W,
    capture_path: PathBuf,
    shutdown: Option<lading_signal::Watcher>,
    _experiment_started: lading_signal::Watcher,
    target_running: lading_signal::Watcher,
    registry: Arc<Registry<Key, AtomicStorage>>,
    accumulator: Accumulator,
    run_id: Uuid,
    global_labels: FxHashMap<String, String>,
    snd: mpsc::Sender<Metric>,
    recv: mpsc::Receiver<Metric>,
}

impl<W: Write + Send> std::fmt::Debug for CaptureManager<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CaptureManager")
            .field("fetch_index", &self.fetch_index)
            .field("start", &self.start)
            .field("capture_path", &self.capture_path)
            .field("accumulator", &self.accumulator)
            .field("global_labels", &self.global_labels)
            .finish_non_exhaustive()
    }
}

impl<W: Write + Send> CaptureManager<W> {
    /// Create a new [`CaptureManager`] with a custom writer
    pub fn new_with_writer(
        capture_writer: W,
        capture_path: PathBuf,
        shutdown: lading_signal::Watcher,
        experiment_started: lading_signal::Watcher,
        target_running: lading_signal::Watcher,
        _expiration: Duration,
    ) -> Self {
        let registry = Arc::new(Registry::new(AtomicStorage));
        let run_id = Uuid::new_v4();
        let now = Instant::now();

        let (snd, recv) = mpsc::channel(10_000);
        let accumulator = Accumulator::new();

        Self {
            fetch_index: 0,
            start: now,
            capture_writer,
            capture_path,
            shutdown: Some(shutdown),
            _experiment_started: experiment_started,
            target_running,
            registry,
            accumulator,
            run_id,
            global_labels: FxHashMap::default(),
            snd,
            recv,
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

    fn write_metric_line(
        &mut self,
        key: &Key,
        value: &MetricValue,
        tick: u64,
        now_ms: u128,
        current_tick: u64,
        run_id: Uuid,
    ) -> Result<(), Error> {
        // Calculate recorded_at based on tick offset from current tick
        let recorded_at_ms = now_ms.saturating_sub(u128::from(current_tick - tick) * 1000);

        let mut labels = self.global_labels.clone();
        for lbl in key.labels() {
            labels.insert(lbl.key().into(), lbl.value().into());
        }

        let line = match value {
            MetricValue::Counter(val) => json::Line {
                run_id,
                time: now_ms,
                recorded_at: recorded_at_ms,
                fetch_index: self.fetch_index,
                metric_name: key.name().into(),
                metric_kind: json::MetricKind::Counter,
                value: json::LineValue::Int(*val),
                labels,
            },
            MetricValue::Gauge(val) => json::Line {
                run_id,
                time: now_ms,
                recorded_at: recorded_at_ms,
                fetch_index: self.fetch_index,
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
        let start = Instant::now();

        for (k, c) in self.registry.get_counter_handles() {
            let val = c.load(Ordering::Relaxed);
            let c = Counter {
                key: k,
                tick_offset: 0,
                value: CounterValue::Absolute(val),
            };
            self.accumulator.counter(c)?;
        }

        for (k, g) in self.registry.get_gauge_handles() {
            let bits = g.load(Ordering::Relaxed);
            let value = f64::from_bits(bits);
            let g = Gauge {
                key: k,
                tick_offset: 0,
                value: GaugeValue::Set(value),
            };
            self.accumulator.gauge(g)?;
        }

        self.accumulator.advance_tick();

        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let current_tick = self.accumulator.current_tick;
        let run_id = self.run_id;

        let mut line_count = 0;
        for (key, value, tick) in self.accumulator.flush() {
            self.write_metric_line(&key, &value, tick, now_ms, current_tick, run_id)?;
            line_count += 1;
        }

        debug!(
            "Recording {line_count} captures to {}",
            self.capture_path.display()
        );

        let elapsed = start.elapsed();
        if elapsed > Duration::from_secs(1) {
            error!("record_captures took {elapsed:?}, exceeded 1 second budget");
        }

        Ok(())
    }
}

impl CaptureManager<BufWriter<std::fs::File>> {
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
            capture_path,
            shutdown,
            experiment_started,
            target_running,
            expiration,
        ))
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
    pub async fn start(mut self) -> Result<(), Error> {
        // Initialize historical sender - done here in async context
        *HISTORICAL_SENDER.lock().await = Some(Sender {
            start: self.start,
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

        let mut flush_interval = time::interval(Duration::from_secs(1));
        flush_interval.tick().await; // first tick happens immediately

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
                        Some(Metric::Counter(c)) => self.accumulator.counter(c)?,
                        Some(Metric::Gauge(g)) => self.accumulator.gauge(g)?,
                        None => {
                            warn!("Timestamped metrics unexpected transmission shutdown");
                            return Ok(());
                        }
                    }
                }
                _ = flush_interval.tick() => {
                    let now = Instant::now();
                    if let Err(e) = self.record_captures() {
                        warn!(
                            "failed to record captures for idx {idx}: {e}",
                            idx = self.fetch_index
                        );
                    }
                    self.fetch_index += 1;

                    let delta = now.elapsed();
                    if delta > Duration::from_secs(1) {
                        warn!("Recording capture took more than 1s (took {delta:?})");
                    }
                },
                () = &mut shutdown_wait => {
                    info!("shutdown signal received, flushing all remaining metrics");
                    let run_id = self.run_id;
                    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();

                    // Drain all remaining data from the accumulator. Collect to
                    // release the mutable borrow so we can call
                    // write_metric_line, sort of an API / ownership smell but
                    // the allocation is not on a hot path.
                    let drain: Vec<_> = self.accumulator.drain().collect();
                    for (current_tick, metrics) in drain {
                        for (key, value, tick) in metrics {
                            self.write_metric_line(&key, &value, tick, now_ms, current_tick, run_id)?;
                        }
                    }

                    return Ok(());
                }
            }
        }
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
    use std::sync::{Arc, Mutex};

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

    #[test]
    fn metrics_macros_create_capture_lines() {
        let writer = InMemoryWriter::new();
        let (shutdown_rx, _shutdown_tx) = lading_signal::signal();
        let (experiment_rx, _experiment_tx) = lading_signal::signal();
        let (target_rx, target_tx) = lading_signal::signal();

        target_tx.signal();

        let manager = CaptureManager::new_with_writer(
            writer.clone(),
            PathBuf::from("/tmp/test-capture.json"),
            shutdown_rx,
            experiment_rx,
            target_rx,
            Duration::from_secs(60),
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
                recorded_at: 0,
                fetch_index: 0,
                metric_name: key.name().into(),
                metric_kind: json::MetricKind::Counter,
                value: json::LineValue::Int(value),
                labels,
            });
        }

        for (key, gauge) in manager.registry.get_gauge_handles() {
            let bits = gauge.load(Ordering::Acquire);
            let value = f64::from_bits(bits);
            let mut labels = FxHashMap::default();
            for lbl in key.labels() {
                labels.insert(lbl.key().to_string(), lbl.value().to_string());
            }
            lines.push(json::Line {
                run_id: manager.run_id,
                time: 0,
                recorded_at: 0,
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
    fn sixty_second_delay_enforced() {
        let writer = InMemoryWriter::new();
        let (shutdown_rx, _shutdown_tx) = lading_signal::signal();
        let (experiment_rx, _experiment_tx) = lading_signal::signal();
        let (target_rx, target_tx) = lading_signal::signal();

        target_tx.signal();

        let mut manager = CaptureManager::new_with_writer(
            writer.clone(),
            PathBuf::from("/tmp/test-capture.json"),
            shutdown_rx,
            experiment_rx,
            target_rx,
            Duration::from_secs(60),
        );

        let recorder = CaptureRecorder {
            registry: Arc::clone(&manager.registry),
        };

        metrics::with_local_recorder(&recorder, || {
            metrics::counter!("test_counter").increment(100);
            metrics::gauge!("test_gauge").set(42.0);
        });

        for i in 0..59 {
            manager.record_captures().unwrap();
            let lines = writer.parse_lines().unwrap();
            assert!(
                lines.is_empty(),
                "Expected no lines after {} flush calls, but got {}",
                i + 1,
                lines.len()
            );
        }

        manager.record_captures().unwrap();
        let lines = writer.parse_lines().unwrap();

        assert_eq!(
            lines.len(),
            2,
            "Expected 2 lines (counter + gauge) after 60th flush, got {}",
            lines.len()
        );

        for line in &lines {
            let delay_ms = line.time.saturating_sub(line.recorded_at);
            let delay_seconds = delay_ms / 1000;
            assert_eq!(
                delay_seconds, 60,
                "Expected 60 second delay between recorded_at and time, got {} seconds",
                delay_seconds
            );
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
}
