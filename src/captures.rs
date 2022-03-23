use crate::signals::Shutdown;
use metrics_util::registry::{AtomicStorage, Registry};
use serde::Serialize;
use std::{
    borrow::Cow,
    collections::HashMap,
    ffi::OsStr,
    io,
    path::PathBuf,
    sync::{atomic::Ordering, Arc},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufWriter},
    time::{self, Duration},
};
use tracing::info;
use uuid::Uuid;

#[derive(Debug, Serialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum MetricKind {
    Counter,
    Gauge,
}

#[derive(Debug, Serialize)]
pub struct Line<'a> {
    #[serde(borrow)]
    /// An id that is mostly unique to this run, allowing us to distinguish
    /// duplications of the same observational setup.
    pub run_id: Cow<'a, Uuid>,
    pub time: u128,
    pub fetch_index: u64,
    pub metric_name: String,
    pub metric_kind: MetricKind,
    pub value: u64,
    #[serde(flatten)]
    pub labels: HashMap<String, String>,
}

struct Inner {
    registry: Registry<metrics::Key, AtomicStorage>,
}

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
                    value: counter.load(Ordering::Relaxed),
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
                let line = Line {
                    run_id: Cow::Borrowed(&self.run_id),
                    time: now_ms,
                    fetch_index: self.fetch_index,
                    metric_name: key.name().into(),
                    metric_kind: MetricKind::Gauge,
                    value: gauge.load(Ordering::Relaxed),
                    labels,
                };
                lines.push(line);
            });
        info!(
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
        _description: &'static str,
    ) {
        // nothing, intentionally
    }

    fn describe_gauge(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: &'static str,
    ) {
        // nothing, intentionally
    }

    fn describe_histogram(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: &'static str,
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
