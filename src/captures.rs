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
    collections::HashMap,
    ffi::OsStr,
    io,
    path::PathBuf,
    sync::{atomic::Ordering, Arc},
    time::{SystemTime, UNIX_EPOCH},
};

use metrics_util::registry::{AtomicStorage, Registry};
use serde::Serialize;
use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufWriter},
    time::{self, Duration},
};
use tracing::{debug, info};
use uuid::Uuid;

use crate::signals::Shutdown;

#[derive(Debug, Serialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
/// The kinds of metrics that are recorded in [`Line`].
pub enum MetricKind {
    /// A monotonically increasing value.
    Counter,
    /// A point-at-time value.
    Gauge,
}

#[derive(Debug, Serialize, Clone, Copy)]
/// The value for [`Line`].
#[serde(untagged)]
pub enum LineValue {
    /// A signless integer, 64 bits wide
    Int(u64),
    /// A floating point, 64 bits wide
    Float(f64),
}

#[derive(Debug, Serialize)]
/// The structure of a capture file line.
pub struct Line<'a> {
    #[serde(borrow)]
    /// An id that is mostly unique to this run, allowing us to distinguish
    /// duplications of the same observational setup.
    pub run_id: Cow<'a, Uuid>,
    /// The time in milliseconds that this line was written.
    pub time: u128,
    /// The "fetch index". Previous versions of lading scraped prometheus
    /// metrics from their targets and kept an increment index of polls. Now
    /// this records the number of times the internal metrics cache has been
    /// flushed.
    pub fetch_index: u64,
    /// The name of the metric recorded by this line.
    pub metric_name: String,
    /// The kind of metric recorded by this line.
    pub metric_kind: MetricKind,
    /// The value of the metric on this line.
    pub value: LineValue,
    #[serde(flatten)]
    /// The labels associated with this metric.
    pub labels: HashMap<String, String>,
}

struct Inner {
    registry: Registry<metrics::Key, AtomicStorage>,
}

#[allow(missing_debug_implementations)]
/// Wrangles internal metrics into capture files
///
/// This struct is responsible for capturing all internal metrics sent through
/// [`metrics`] and periodically writing them to disk with format [`Line`].
pub struct CaptureManager {
    fetch_index: u64,
    run_id: Uuid,
    capture_fp: BufWriter<File>,
    capture_path: PathBuf,
    shutdown: Shutdown,
    inner: Arc<Inner>,
    global_labels: HashMap<String, String>,
}

impl CaptureManager {
    /// Create a new [`CaptureManager`]
    ///
    /// # Panics
    ///
    /// Function will panic if the underlying capture file cannot be opened.
    pub async fn new(capture_path: PathBuf, shutdown: Shutdown) -> Self {
        let fp = File::create(&capture_path).await.unwrap();
        Self {
            run_id: Uuid::new_v4(),
            fetch_index: 0,
            capture_fp: BufWriter::new(fp),
            capture_path,
            shutdown,
            inner: Arc::new(Inner {
                registry: Registry::atomic(),
            }),
            global_labels: HashMap::new(),
        }
    }

    /// Install the [`CaptureManager`] as global [`metrics::Recorder`]
    ///
    /// # Panics
    ///
    /// Function will panic if there is already a global recorder set.
    pub fn install(&self) {
        let recorder = CaptureRecorder {
            inner: Arc::clone(&self.inner),
        };
        metrics::set_boxed_recorder(Box::new(recorder)).unwrap();
    }

    /// Add a global label to all metrics managed by [`CaptureManager`].
    pub fn add_global_label<K, V>(&mut self, key: K, value: V)
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.global_labels.insert(key.into(), value.into());
    }

    async fn record_captures(&mut self) {
        let now_ms: u128 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
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
                let line = Line {
                    run_id: Cow::Borrowed(&self.run_id),
                    time: now_ms,
                    fetch_index: self.fetch_index,
                    metric_name: key.name().into(),
                    metric_kind: MetricKind::Counter,
                    value: LineValue::Int(counter.load(Ordering::Relaxed)),
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
                let line = Line {
                    run_id: Cow::Borrowed(&self.run_id),
                    time: now_ms,
                    fetch_index: self.fetch_index,
                    metric_name: key.name().into(),
                    metric_kind: MetricKind::Gauge,
                    value: LineValue::Float(value),
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
                .unwrap()
        );
        for line in lines.drain(..) {
            let pyld = serde_json::to_string(&line).unwrap();
            self.capture_fp.write_all(pyld.as_bytes()).await.unwrap();
            self.capture_fp.write_all(b"\n").await.unwrap();
        }
    }

    /// Run [`CaptureManager`] to completion
    ///
    /// Once a second any metrics produced by this program are flushed to disk
    /// and this process only stops once an error occurs or a shutdown signal is
    /// received.
    ///
    /// # Errors
    ///
    /// Function will error if underlying IO writes do not succeed.
    ///
    /// # Panics
    ///
    /// None known.
    pub async fn run(mut self) -> Result<(), io::Error> {
        let mut write_delay = time::interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                _ = write_delay.tick() => {
                    self.record_captures().await;
                    self.fetch_index += 1;
                }
                _ = self.shutdown.recv() => {
                    self.record_captures().await;
                    info!("shutdown signal received");
                    return Ok(())
                }
            }
        }
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
