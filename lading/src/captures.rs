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
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use lading_capture::json;
use metrics::Key;
use metrics_util::registry::{AtomicStorage, Storage};
use rustc_hash::FxHashMap;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use uuid::Uuid;

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

enum MetricValue {
    Gauge(Arc<AtomicU64>),
    Counter(Arc<AtomicU64>),
}

impl MetricValue {
    fn inner(&self) -> Arc<AtomicU64> {
        match &self {
            MetricValue::Gauge(inner) | MetricValue::Counter(inner) => inner.clone(),
        }
    }
}

struct Inner {
    registry: FxHashMap<Key, MetricValue>,
}

#[allow(missing_debug_implementations)]
/// Wrangles internal metrics into capture files
///
/// This struct is responsible for capturing all internal metrics sent through
/// [`metrics`] and periodically writing them to disk with format
/// [`json::Line`].
pub struct CaptureManager {
    fetch_index: u64,
    run_id: Uuid,
    capture_fp: BufWriter<std::fs::File>,
    capture_path: PathBuf,
    shutdown: lading_signal::Watcher,
    _experiment_started: lading_signal::Watcher,
    target_running: lading_signal::Watcher,
    inner: Arc<Mutex<Inner>>,
    global_labels: FxHashMap<String, String>,
}

impl CaptureManager {
    /// Create a new [`CaptureManager`]
    ///
    /// # Errors
    ///
    /// Function will error if the underlying capture file cannot be opened.
    pub async fn new(
        capture_path: PathBuf,
        shutdown: lading_signal::Watcher,
        experiment_started: lading_signal::Watcher,
        target_running: lading_signal::Watcher,
    ) -> Result<Self, io::Error> {
        let fp = tokio::fs::File::create(&capture_path).await?;
        let fp = fp.into_std().await;

        let inner = Inner {
            registry: FxHashMap::default(),
        };

        Ok(Self {
            run_id: Uuid::new_v4(),
            fetch_index: 0,
            capture_fp: BufWriter::new(fp),
            capture_path,
            shutdown,
            _experiment_started: experiment_started,
            target_running,
            inner: Arc::new(Mutex::new(inner)),
            global_labels: FxHashMap::default(),
        })
    }

    /// Install the [`CaptureManager`] as global [`metrics::Recorder`]
    ///
    /// # Errors
    ///
    /// Returns an error if there is already a global recorder set.
    pub fn install(&self) -> Result<(), Error> {
        let recorder = CaptureRecorder {
            inner: self.inner.clone(),
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

        let mut state = self.inner.blocking_lock();
        #[allow(clippy::mutable_key_type)] // This is safe. See safety note on [`metrics::Key`].
        let mut map = FxHashMap::default();
        std::mem::swap(&mut state.registry, &mut map);
        drop(state);

        for (key, val) in map {
            let mut labels = self.global_labels.clone();
            for lbl in key.labels() {
                // TODO we're allocating the same small strings over and over most likely
                labels.insert(lbl.key().into(), lbl.value().into());
            }

            let line = match val {
                MetricValue::Gauge(gauge) => {
                    let value: f64 = f64::from_bits(gauge.load(Ordering::Relaxed));
                    json::Line {
                        run_id: self.run_id,
                        time: now_ms,
                        fetch_index: self.fetch_index,
                        metric_name: key.name().into(),
                        metric_kind: json::MetricKind::Gauge,
                        value: json::LineValue::Float(value),
                        labels,
                    }
                }
                MetricValue::Counter(counter) => {
                    let value: u64 = counter.load(Ordering::Relaxed);
                    json::Line {
                        run_id: self.run_id,
                        time: now_ms,
                        fetch_index: self.fetch_index,
                        metric_name: key.name().into(),
                        metric_kind: json::MetricKind::Counter,
                        value: json::LineValue::Int(value),
                        labels,
                    }
                }
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
            self.capture_fp
                .write_all(pyld.as_bytes())
                .map_err(|err| Error::Io {
                    context: "payload write",
                    err,
                })?;
            self.capture_fp.write_all(b"\n").map_err(|err| Error::Io {
                context: "newline write",
                err,
            })?;
            self.capture_fp.flush().map_err(|err| Error::Io {
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

        std::thread::Builder::new()
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
    inner: Arc<Mutex<Inner>>,
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
            .blocking_lock()
            .registry
            .entry(key.clone())
            .or_insert_with(|| {
                let storage = AtomicStorage;
                let counter = storage.counter(key);
                MetricValue::Counter(counter)
            })
            .inner()
            .into()
    }

    fn register_gauge(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        self.inner
            .blocking_lock()
            .registry
            .entry(key.clone())
            .or_insert_with(|| {
                let storage = AtomicStorage;
                let counter = storage.counter(key);
                MetricValue::Gauge(counter)
            })
            .inner()
            .into()
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
