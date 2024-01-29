//! Capture and record lading's internal metrics
//!
//! The manner in which lading instruments its target is pretty simple: we use
//! the [`metrics`] library to record factual things about interaction with the
//! target and then write all that out to disk for later analysis. This means
//! that the generator, blackhole etc code are unaware of anything other than
//! their [`metrics`] integration while [`CaptureManager`] need only hook into
//! that same crate.

use std::{
    borrow::Cow,
    ffi::OsStr,
    io::{self, BufWriter, Write},
    path::PathBuf,
    sync::{atomic::Ordering, Arc},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use lading_capture::json;
use metrics::SetRecorderError;
use metrics_util::registry::{AtomicStorage, Registry};
use rustc_hash::FxHashMap;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::signals::Phase;

/// Errors produced by [`CaptureManager`]
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Wrapper around [`SetRecorderError`].
    #[error("Set recorder error: {0}")]
    SetRecorderError(#[from] SetRecorderError),
}

struct Inner {
    registry: Registry<metrics::Key, AtomicStorage>,
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
    shutdown: Phase,
    _experiment_started: Phase,
    inner: Arc<Inner>,
    global_labels: FxHashMap<String, String>,
}

impl CaptureManager {
    /// Create a new [`CaptureManager`]
    ///
    /// # Panics
    ///
    /// Function will panic if the underlying capture file cannot be opened.
    pub async fn new(capture_path: PathBuf, shutdown: Phase, experiment_started: Phase) -> Self {
        let fp = tokio::fs::File::create(&capture_path)
            .await
            .expect("create either called from outside tokio or create errored");
        let fp = fp.into_std().await;
        Self {
            run_id: Uuid::new_v4(),
            fetch_index: 0,
            capture_fp: BufWriter::new(fp),
            capture_path,
            shutdown,
            _experiment_started: experiment_started,
            inner: Arc::new(Inner {
                registry: Registry::atomic(),
            }),
            global_labels: FxHashMap::default(),
        }
    }

    /// Install the [`CaptureManager`] as global [`metrics::Recorder`]
    ///
    /// # Errors
    ///
    /// Returns an error if there is already a global recorder set.
    /// Function if there is already a global recorder set.
    pub fn install(&self) -> Result<(), Error> {
        let recorder = CaptureRecorder {
            inner: Arc::clone(&self.inner),
        };
        metrics::set_boxed_recorder(Box::new(recorder))?;
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

    fn record_captures(&mut self) -> Result<(), io::Error> {
        let now_ms: u128 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("UNIX_EPOCH is earlier than the provided time")
            .as_millis();
        let mut lines = Vec::new();
        self.inner
            .registry
            .visit_counters(|key: &metrics::Key, counter| {
                let mut labels = self.global_labels.clone();
                for lbl in key.labels() {
                    // TODO we're allocating the same small strings over and over most likely
                    labels.insert(lbl.key().into(), lbl.value().into());
                }
                let line = json::Line {
                    run_id: Cow::Borrowed(&self.run_id),
                    time: now_ms,
                    fetch_index: self.fetch_index,
                    metric_name: key.name().into(),
                    metric_kind: json::MetricKind::Counter,
                    value: json::LineValue::Int(counter.load(Ordering::Relaxed)),
                    labels,
                };
                lines.push(line);
            });
        self.inner
            .registry
            .visit_gauges(|key: &metrics::Key, gauge| {
                let mut labels = self.global_labels.clone();
                for lbl in key.labels() {
                    // TODO we're allocating the same small strings over and over most likely
                    labels.insert(lbl.key().into(), lbl.value().into());
                }
                let value: f64 = f64::from_bits(gauge.load(Ordering::Relaxed));
                let line = json::Line {
                    run_id: Cow::Borrowed(&self.run_id),
                    time: now_ms,
                    fetch_index: self.fetch_index,
                    metric_name: key.name().into(),
                    metric_kind: json::MetricKind::Gauge,
                    value: json::LineValue::Float(value),
                    labels,
                };
                lines.push(line);
            });
        debug!(
            "Recording {} captures to {}",
            lines.len(),
            self.capture_path
                .file_name()
                .and_then(OsStr::to_str)
                .expect("capture path is not a valid file name")
        );
        for line in lines.drain(..) {
            let pyld = serde_json::to_string(&line)?;
            self.capture_fp.write_all(pyld.as_bytes())?;
            self.capture_fp.write_all(b"\n")?;
            self.capture_fp.flush()?;
        }

        Ok(())
    }

    /// Run [`CaptureManager`] to completion
    ///
    /// Once a second any metrics produced by this program are flushed to disk.
    /// This function only exits once a shutdown signal is received.
    ///
    /// # Panics
    /// # Errors
    ///
    /// Will return `Err` if there is already a global recorder set
    /// None known.
    pub fn start(mut self) -> Result<(), Error> {
        // Installing the recorder immediately on startup.
        // This does _not_ wait on experiment_started signal, so
        // warmup data will be included in the capture.
        self.install()?;
        info!("Capture manager installed, recording to capture file.");

        std::thread::Builder::new()
            .name("capture-manager".into())
            .spawn(move || loop {
                if let Err(e) = self.record_captures() {
                    warn!(
                        "failed to record captures for idx {idx}: {e}",
                        idx = self.fetch_index
                    );
                }
                self.fetch_index += 1;
                if self.shutdown.try_recv() {
                    info!("shutdown signal received");
                    return;
                }
                std::thread::sleep(Duration::from_secs(1));
            })
            .expect("failed to create capture manager thread");
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

    fn register_counter(&self, key: &metrics::Key) -> metrics::Counter {
        self.inner
            .registry
            .get_or_create_counter(key, |c| c.clone().into())
    }

    fn register_gauge(&self, key: &metrics::Key) -> metrics::Gauge {
        self.inner
            .registry
            .get_or_create_gauge(key, |c| c.clone().into())
    }

    fn register_histogram(&self, _key: &metrics::Key) -> metrics::Histogram {
        // nothing, intentionally
        unimplemented!()
    }
}
