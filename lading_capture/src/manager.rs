//! Capture and record lading's internal metrics
//!
//! The manner in which lading instruments its target is pretty simple: we use
//! the [`metrics`] library to record factual things about interaction with the
//! target and then write all that out to disk for later analysis. This means
//! that the generator, blackhole etc code are unaware of anything other than
//! their [`metrics`] integration while [`CaptureManager`] need only hook into
//! that same crate.

use std::{
    ffi::OsStr,
    io::{self, BufWriter, Write},
    path::PathBuf,
    sync::{Arc, atomic::Ordering},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use tokio::fs;

use crate::json;
use metrics::Key;
use metrics_util::{
    MetricKindMask,
    registry::{AtomicStorage, GenerationalAtomicStorage, GenerationalStorage, Recency, Registry},
};
use rustc_hash::FxHashMap;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Trait for writing capture lines
///
/// This trait abstracts the writing of capture lines, allowing for both
/// file-based and in-memory implementations for testing.
pub trait CaptureWriter: Send {
    /// Write bytes to the capture destination
    ///
    /// # Errors
    ///
    /// Returns an error if the write operation fails.
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()>;

    /// Flush any buffered data
    ///
    /// # Errors
    ///
    /// Returns an error if the flush operation fails.
    fn flush(&mut self) -> io::Result<()>;
}

impl CaptureWriter for BufWriter<std::fs::File> {
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        Write::write_all(self, buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Write::flush(self)
    }
}

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
}

struct Inner {
    registry: Registry<Key, GenerationalAtomicStorage>,
    recency: Recency<Key>,
}

#[allow(missing_debug_implementations)]
/// Wrangles internal metrics into capture files
///
/// This struct is responsible for capturing all internal metrics sent through
/// [`metrics`] and periodically writing them to disk with format
/// [`json::Line`].
pub struct CaptureManager<W: CaptureWriter> {
    fetch_index: u64,
    run_id: Uuid,
    capture_writer: W,
    capture_path: PathBuf,
    shutdown: lading_signal::Watcher,
    _experiment_started: lading_signal::Watcher,
    target_running: lading_signal::Watcher,
    inner: Arc<Inner>,
    global_labels: FxHashMap<String, String>,
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

        let inner = Inner {
            registry: Registry::new(GenerationalStorage::new(AtomicStorage)),
            recency: Recency::new(
                quanta::Clock::new(),
                MetricKindMask::GAUGE | MetricKindMask::COUNTER,
                Some(expiration),
            ),
        };

        Ok(Self {
            run_id: Uuid::new_v4(),
            fetch_index: 0,
            capture_writer: BufWriter::new(fp),
            capture_path,
            shutdown,
            _experiment_started: experiment_started,
            target_running,
            inner: Arc::new(inner),
            global_labels: FxHashMap::default(),
        })
    }
}

impl<W: CaptureWriter + 'static> CaptureManager<W> {
    /// Create a new [`CaptureManager`] with a custom writer for testing
    #[cfg(test)]
    pub fn new_with_writer(
        capture_writer: W,
        shutdown: lading_signal::Watcher,
        experiment_started: lading_signal::Watcher,
        target_running: lading_signal::Watcher,
        expiration: Duration,
    ) -> Self {
        let inner = Inner {
            registry: Registry::new(GenerationalStorage::new(AtomicStorage)),
            recency: Recency::new(
                quanta::Clock::new(),
                MetricKindMask::GAUGE | MetricKindMask::COUNTER,
                Some(expiration),
            ),
        };

        Self {
            run_id: Uuid::new_v4(),
            fetch_index: 0,
            capture_writer,
            capture_path: PathBuf::from("/tmp/test-capture.json"),
            shutdown,
            _experiment_started: experiment_started,
            target_running,
            inner: Arc::new(inner),
            global_labels: FxHashMap::default(),
        }
    }

    /// Install the [`CaptureManager`] as global [`metrics::Recorder`]
    ///
    /// # Errors
    ///
    /// Returns an error if there is already a global recorder set.
    pub fn install(&self) -> Result<(), Error> {
        let recorder = CaptureRecorder {
            inner: Arc::clone(&self.inner),
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

    fn record_captures(&mut self) -> Result<(), Error> {
        let now_ms: u128 = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let mut lines = Vec::new();

        let counter_handles = self.inner.registry.get_counter_handles();
        for (key, counter) in counter_handles {
            let g = counter.get_generation();
            if !self
                .inner
                .recency
                .should_store_counter(&key, g, &self.inner.registry)
            {
                continue;
            }

            let mut labels = self.global_labels.clone();
            for lbl in key.labels() {
                // TODO we're allocating the same small strings over and over most likely
                labels.insert(lbl.key().into(), lbl.value().into());
            }
            let value: u64 = counter.get_inner().load(Ordering::Relaxed);
            let line = json::Line {
                run_id: self.run_id,
                time: now_ms,
                fetch_index: self.fetch_index,
                metric_name: key.name().into(),
                metric_kind: json::MetricKind::Counter,
                value: json::LineValue::Int(value),
                labels,
            };
            lines.push(line);
        }

        let gauge_handles = self.inner.registry.get_gauge_handles();
        for (key, gauge) in gauge_handles {
            let g = gauge.get_generation();
            if !self
                .inner
                .recency
                .should_store_gauge(&key, g, &self.inner.registry)
            {
                continue;
            }

            let mut labels = self.global_labels.clone();
            for lbl in key.labels() {
                // TODO we're allocating the same small strings over and over most likely
                labels.insert(lbl.key().into(), lbl.value().into());
            }
            let value: f64 = f64::from_bits(gauge.get_inner().load(Ordering::Relaxed));
            let line = json::Line {
                run_id: self.run_id,
                time: now_ms,
                fetch_index: self.fetch_index,
                metric_name: key.name().into(),
                metric_kind: json::MetricKind::Gauge,
                value: json::LineValue::Float(value),
                labels,
            };
            lines.push(line);
        }

        debug!(
            "Recording {} captures to {}",
            lines.len(),
            self.capture_path
                .file_name()
                .and_then(OsStr::to_str)
                .ok_or(Error::CapturePath)?
        );
        for line in lines.drain(..) {
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
        }

        Ok(())
    }

    /// Run [`CaptureManager`] to completion
    ///
    /// Once a second any metrics produced by this program are flushed to disk.
    /// This function only exits once a shutdown signal is received.
    /// # Panics
    /// None known.
    /// # Errors
    /// Will return 'error' if there is already a global recorder set
    pub fn start(mut self) -> Result<(), Error> {
        // Installing the recorder immediately on startup.
        // This does _not_ wait on experiment_started signal, so
        // warmup data will be included in the capture.
        self.install()?;
        info!("Capture manager installed, recording to capture file.");

        thread::Builder::new()
            .name("capture-manager".into())
            .spawn(move || {
                while let Ok(false) = self.target_running.try_recv() {
                    std::thread::sleep(Duration::from_millis(100));
                }
                loop {
                    if self.shutdown.try_recv().expect("polled after signal") {
                        info!("shutdown signal received");
                        return;
                    }
                    let now = Instant::now();
                    if let Err(e) = self.record_captures() {
                        warn!(
                            "failed to record captures for idx {idx}: {e}",
                            idx = self.fetch_index
                        );
                    }
                    self.fetch_index += 1;
                    // Sleep for 1 second minus however long we just spent recording captures
                    // assumption here is that the time spent recording captures is consistent
                    let delta = now.elapsed();
                    if delta > Duration::from_secs(1) {
                        warn!("Recording capture took more than 1s (took {delta:?})");
                        continue;
                    }
                    std::thread::sleep(Duration::from_secs(1).saturating_sub(delta));
                }
            })
            .map_err(|err| Error::Io {
                context: "thread building",
                err,
            })?;
        Ok(())
    }
}

struct CaptureRecorder {
    inner: Arc<Inner>,
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
        self.inner
            .registry
            .get_or_create_counter(key, |c| c.clone().into())
    }

    fn register_gauge(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        self.inner
            .registry
            .get_or_create_gauge(key, |c| c.clone().into())
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

        fn get_content(&self) -> Vec<u8> {
            self.buffer.lock().unwrap().clone()
        }

        fn parse_lines(&self) -> Result<Vec<json::Line>, serde_json::Error> {
            let content = self.get_content();
            let content_str = String::from_utf8_lossy(&content);
            content_str
                .lines()
                .filter(|line| !line.is_empty())
                .map(serde_json::from_str)
                .collect()
        }
    }

    impl CaptureWriter for InMemoryWriter {
        fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
            self.buffer.lock().unwrap().extend_from_slice(buf);
            Ok(())
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

        let mut manager = CaptureManager::new_with_writer(
            writer.clone(),
            shutdown_rx,
            experiment_rx,
            target_rx,
            Duration::from_secs(60),
        );

        let recorder = CaptureRecorder {
            inner: Arc::clone(&manager.inner),
        };

        let mut expected_counters: Vec<(String, FxHashMap<String, String>, u64)> = Vec::new();
        let mut expected_gauges: Vec<(String, FxHashMap<String, String>, f64)> = Vec::new();

        metrics::with_local_recorder(&recorder, || {
            metrics::counter!("counter_increment").increment(10);
            metrics::counter!("counter_increment").increment(20);
            expected_counters.push(("counter_increment".to_string(), FxHashMap::default(), 30));

            metrics::counter!("counter_absolute").absolute(42);
            expected_counters.push(("counter_absolute".to_string(), FxHashMap::default(), 42));

            metrics::counter!("counter_with_labels", "env" => "test", "region" => "us-east")
                .increment(100);
            let mut labels = FxHashMap::default();
            labels.insert("env".to_string(), "test".to_string());
            labels.insert("region".to_string(), "us-east".to_string());
            expected_counters.push(("counter_with_labels".to_string(), labels, 100));

            metrics::gauge!("gauge_set").set(3.14);
            expected_gauges.push(("gauge_set".to_string(), FxHashMap::default(), 3.14));

            metrics::gauge!("gauge_increment").set(10.0);
            metrics::gauge!("gauge_increment").increment(5.0);
            expected_gauges.push(("gauge_increment".to_string(), FxHashMap::default(), 15.0));

            metrics::gauge!("gauge_decrement").set(10.0);
            metrics::gauge!("gauge_decrement").decrement(3.0);
            expected_gauges.push(("gauge_decrement".to_string(), FxHashMap::default(), 7.0));

            metrics::gauge!("gauge_with_labels", "service" => "api", "host" => "server1")
                .set(99.9);
            let mut labels = FxHashMap::default();
            labels.insert("service".to_string(), "api".to_string());
            labels.insert("host".to_string(), "server1".to_string());
            expected_gauges.push(("gauge_with_labels".to_string(), labels, 99.9));
        });

        manager.record_captures().unwrap();
        let lines = writer.parse_lines().unwrap();

        for (name, labels, expected_value) in &expected_counters {
            let line = lines.iter().find(|line| {
                line.metric_name == *name
                    && matches!(line.metric_kind, json::MetricKind::Counter)
                    && line.labels == *labels
            });

            assert!(
                line.is_some(),
                "Expected to find counter '{name}' with labels {labels:?}"
            );

            let line = line.unwrap();

            let val = match line.value {
                json::LineValue::Int(val) => val,
                json::LineValue::Float(_) => {
                    assert!(false, "Expected Int value for counter {name}, got Float");
                    return;
                }
            };

            assert_eq!(val, *expected_value, "Counter {name} value mismatch");
        }

        for (name, labels, expected_value) in &expected_gauges {
            let line = lines.iter().find(|line| {
                line.metric_name == *name
                    && matches!(line.metric_kind, json::MetricKind::Gauge)
                    && line.labels == *labels
            });

            assert!(
                line.is_some(),
                "Expected to find gauge '{name}' with labels {labels:?}"
            );

            let line = line.unwrap();

            let val = match line.value {
                json::LineValue::Float(val) => val,
                json::LineValue::Int(_) => {
                    assert!(false, "Expected Float value for gauge {name}, got Int");
                    return;
                }
            };

            let diff = (val - expected_value).abs();
            assert!(diff < 0.0001, "Gauge {name} value mismatch");
        }
    }
}
