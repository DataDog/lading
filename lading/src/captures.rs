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
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use lading_capture::{json, parquet as parquet_format};
use metrics::Key;
use metrics_util::{
    MetricKindMask,
    registry::{AtomicStorage, GenerationalAtomicStorage, GenerationalStorage, Recency, Registry},
};
use rustc_hash::FxHashMap;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::config::CaptureFormat;

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

/// Metric data for capture writers
#[derive(Debug, Clone)]
pub struct MetricData {
    /// Unique identifier for this metric collection run
    pub run_id: Uuid,
    /// Timestamp in milliseconds since Unix epoch
    pub time: u128,
    /// Index of the metrics collection fetch/flush cycle
    pub fetch_index: u64,
    /// Name of the metric being recorded
    pub metric_name: String,
    /// Type of metric (counter or gauge)
    pub metric_kind: MetricKind,
    /// Numeric value of the metric
    pub value: MetricValue,
    /// Key-value pairs of labels associated with this metric
    pub labels: FxHashMap<String, String>,
}

/// The kind of metric being recorded
#[derive(Debug, Clone, Copy)]
pub enum MetricKind {
    /// A monotonically increasing value
    Counter,
    /// A point-in-time value that can increase or decrease
    Gauge,
}

/// The numeric value of a metric
#[derive(Debug, Clone, Copy)]
pub enum MetricValue {
    /// An unsigned 64-bit integer value
    Int(u64),
    /// A 64-bit floating point value
    Float(f64),
}

impl MetricValue {
    /// Convert this value to a 64-bit float for storage
    #[must_use]
    pub fn as_f64(&self) -> f64 {
        match self {
            MetricValue::Int(i) => *i as f64,
            MetricValue::Float(f) => *f,
        }
    }
}

/// Trait for different capture file writers
#[async_trait::async_trait]
trait CaptureWriter: Send {
    /// Write a single metric to the capture file
    async fn write_metric(&mut self, metric: &MetricData) -> Result<(), Error>;
    /// Flush any buffered data to disk
    async fn flush(&mut self) -> Result<(), Error>;
}

/// JSON capture writer - writes one JSON object per line
struct JsonCaptureWriter {
    capture_fp: BufWriter<std::fs::File>,
}

impl JsonCaptureWriter {
    fn new(capture_path: PathBuf) -> Result<Self, Error> {
        let fp = std::fs::File::create(capture_path).map_err(|err| Error::Io {
            context: "file creation",
            err,
        })?;
        Ok(Self {
            capture_fp: BufWriter::new(fp),
        })
    }
}

#[async_trait::async_trait]
impl CaptureWriter for JsonCaptureWriter {
    async fn write_metric(&mut self, metric: &MetricData) -> Result<(), Error> {
        let line = json::Line {
            run_id: metric.run_id,
            time: metric.time,
            fetch_index: metric.fetch_index,
            metric_name: metric.metric_name.clone(),
            metric_kind: match metric.metric_kind {
                MetricKind::Counter => json::MetricKind::Counter,
                MetricKind::Gauge => json::MetricKind::Gauge,
            },
            value: match metric.value {
                MetricValue::Int(i) => json::LineValue::Int(i),
                MetricValue::Float(f) => json::LineValue::Float(f),
            },
            labels: metric.labels.clone(),
        };

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
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Error> {
        self.capture_fp.flush().map_err(|err| Error::Io {
            context: "flush",
            err,
        })
    }
}

/// Parquet capture writer - batches metrics and writes in columnar format
struct ParquetCaptureWriter {
    batch_buffer: Vec<MetricData>,
    batch_size: usize,
    writer: Option<parquet::arrow::async_writer::AsyncArrowWriter<tokio::fs::File>>,
}

impl ParquetCaptureWriter {
    async fn new(capture_path: PathBuf) -> Result<Self, Error> {
        let schema = parquet_format::capture_schema();
        let file = tokio::fs::File::create(capture_path)
            .await
            .map_err(|err| Error::Io {
                context: "parquet file creation",
                err,
            })?;

        let props = parquet::file::properties::WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build();

        let writer = parquet::arrow::async_writer::AsyncArrowWriter::try_new(
            file,
            schema.clone(),
            Some(props),
        )
        .map_err(|err| Error::Io {
            context: "parquet writer creation",
            err: std::io::Error::other(err),
        })?;

        Ok(Self {
            batch_buffer: Vec::with_capacity(1000),
            batch_size: 1000,
            writer: Some(writer),
        })
    }

    async fn flush_batch(&mut self) -> Result<(), Error> {
        if self.batch_buffer.is_empty() {
            return Ok(());
        }

        // Convert MetricData to RawMetricData
        let raw_metrics: Vec<parquet_format::RawMetricData> = self
            .batch_buffer
            .iter()
            .map(|m| parquet_format::RawMetricData {
                run_id: m.run_id,
                time: m.time,
                fetch_index: m.fetch_index,
                metric_name: m.metric_name.clone(),
                metric_kind: match m.metric_kind {
                    MetricKind::Counter => parquet_format::RawMetricKind::Counter,
                    MetricKind::Gauge => parquet_format::RawMetricKind::Gauge,
                },
                value: match m.value {
                    MetricValue::Int(i) => parquet_format::RawMetricValue::Int(i),
                    MetricValue::Float(f) => parquet_format::RawMetricValue::Float(f),
                },
                labels: m.labels.clone(),
            })
            .collect();

        let batch =
            parquet_format::raw_metrics_to_record_batch(&raw_metrics).map_err(|err| Error::Io {
                context: "arrow record batch creation",
                err: std::io::Error::other(err),
            })?;

        if let Some(writer) = &mut self.writer {
            writer.write(&batch).await.map_err(|err| Error::Io {
                context: "parquet batch write",
                err: std::io::Error::other(err),
            })?;
        }

        self.batch_buffer.clear();
        Ok(())
    }
}

#[async_trait::async_trait]
impl CaptureWriter for ParquetCaptureWriter {
    async fn write_metric(&mut self, metric: &MetricData) -> Result<(), Error> {
        self.batch_buffer.push(metric.clone());

        if self.batch_buffer.len() >= self.batch_size {
            self.flush_batch().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Error> {
        self.flush_batch().await?;
        if let Some(writer) = self.writer.take() {
            writer.close().await.map_err(|err| Error::Io {
                context: "parquet writer close",
                err: std::io::Error::other(err),
            })?;
        }
        Ok(())
    }
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
pub struct CaptureManager {
    fetch_index: u64,
    run_id: Uuid,
    writer: Box<dyn CaptureWriter + Send>,
    capture_path: PathBuf,
    shutdown: lading_signal::Watcher,
    _experiment_started: lading_signal::Watcher,
    target_running: lading_signal::Watcher,
    inner: Arc<Inner>,
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
        format: CaptureFormat,
        shutdown: lading_signal::Watcher,
        experiment_started: lading_signal::Watcher,
        target_running: lading_signal::Watcher,
        expiration: Duration,
    ) -> Result<Self, io::Error> {
        let writer: Box<dyn CaptureWriter + Send> = match format {
            CaptureFormat::Json => Box::new(JsonCaptureWriter::new(capture_path.clone()).map_err(
                |e| match e {
                    Error::Io { err, .. } => err,
                    _ => io::Error::other(e),
                },
            )?),
            CaptureFormat::Parquet => Box::new(
                ParquetCaptureWriter::new(capture_path.clone())
                    .await
                    .map_err(|e| match e {
                        Error::Io { err, .. } => err,
                        _ => io::Error::other(e),
                    })?,
            ),
        };

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
            writer,
            capture_path,
            shutdown,
            _experiment_started: experiment_started,
            target_running,
            inner: Arc::new(inner),
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

    async fn record_captures(&mut self) -> Result<(), Error> {
        let now_ms: u128 = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let mut metrics = Vec::new();

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
            let metric = MetricData {
                run_id: self.run_id,
                time: now_ms,
                fetch_index: self.fetch_index,
                metric_name: key.name().into(),
                metric_kind: MetricKind::Counter,
                value: MetricValue::Int(value),
                labels,
            };
            metrics.push(metric);
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
            let metric = MetricData {
                run_id: self.run_id,
                time: now_ms,
                fetch_index: self.fetch_index,
                metric_name: key.name().into(),
                metric_kind: MetricKind::Gauge,
                value: MetricValue::Float(value),
                labels,
            };
            metrics.push(metric);
        }

        debug!(
            "Recording {} captures to {}",
            metrics.len(),
            self.capture_path
                .file_name()
                .and_then(OsStr::to_str)
                .ok_or(Error::CapturePath)?
        );
        for metric in metrics.drain(..) {
            self.writer.write_metric(&metric).await?;
        }
        self.writer.flush().await?;

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

        tokio::spawn(async move {
            while let Ok(false) = self.target_running.try_recv() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            loop {
                if self.shutdown.try_recv().expect("polled after signal") {
                    info!("shutdown signal received");
                    return;
                }
                let now = Instant::now();
                if let Err(e) = self.record_captures().await {
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
                tokio::time::sleep(Duration::from_secs(1).saturating_sub(delta)).await;
            }
        });
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
