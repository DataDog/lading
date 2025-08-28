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
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use lading_capture::json;
use parquet::arrow::ArrowWriter;
use arrow_array::{ArrayRef, RecordBatch, StringBuilder, Int64Builder, UInt64Builder, Float64Builder};
use arrow_schema::{Schema, Field, DataType};
use metrics::Key;
use metrics_util::{
    MetricKindMask,
    registry::{AtomicStorage, GenerationalAtomicStorage, GenerationalStorage, Recency, Registry},
};
use rustc_hash::FxHashMap;
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
    /// Error when writing parquet captures
    #[error("Parquet write error: {0}")]
    ParquetWrite(String),
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
pub struct CaptureManager {
    fetch_index: u64,
    run_id: Uuid,
    sink: CaptureSink,
    capture_path: PathBuf,
    shutdown: lading_signal::Watcher,
    _experiment_started: lading_signal::Watcher,
    target_running: lading_signal::Watcher,
    inner: Arc<Inner>,
    global_labels: FxHashMap<String, String>,
}

enum CaptureSink {
    Json(JsonSink),
    Parquet(ParquetSink),
}

impl CaptureSink {
    fn write_lines(&mut self, lines: &[json::Line]) -> Result<(), Error> {
        match self {
            CaptureSink::Json(s) => s.write_lines(lines),
            CaptureSink::Parquet(s) => s.write_lines(lines),
        }
    }

    fn finish(&mut self) -> Result<(), Error> {
        match self {
            CaptureSink::Json(s) => s.finish(),
            CaptureSink::Parquet(s) => s.finish(),
        }
    }
}

struct JsonSink {
    fp: BufWriter<std::fs::File>,
}

impl JsonSink {
    fn new(fp: std::fs::File) -> Self {
        Self {
            fp: BufWriter::new(fp),
        }
    }

    fn write_lines(&mut self, lines: &[json::Line]) -> Result<(), Error> {
        for line in lines {
            let pyld = serde_json::to_string(line)?;
            self.fp
                .write_all(pyld.as_bytes())
                .map_err(|err| Error::Io {
                    context: "payload write",
                    err,
                })?;
            self.fp.write_all(b"\n").map_err(|err| Error::Io {
                context: "newline write",
                err,
            })?;
        }
        self.fp.flush().map_err(|err| Error::Io {
            context: "flush",
            err,
        })?;
        Ok(())
    }

    fn finish(&mut self) -> Result<(), Error> {
        self.fp.flush().map_err(|err| Error::Io {
            context: "flush",
            err,
        })?;
        Ok(())
    }
}

struct ParquetSink {
    writer: ArrowWriter<std::fs::File>,
    schema: Arc<Schema>,
}

impl ParquetSink {
    fn new(fp: std::fs::File) -> Result<Self, Error> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("run_id", DataType::Utf8, false),
            Field::new("time_ms", DataType::Int64, false),
            Field::new("fetch_index", DataType::Int64, false),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("metric_kind", DataType::Utf8, false),
            Field::new("value_int", DataType::UInt64, true),
            Field::new("value_float", DataType::Float64, true),
            Field::new("labels_json", DataType::Utf8, false),
        ]));

        let writer = ArrowWriter::try_new(fp, schema.clone(), None)
            .map_err(|e| Error::ParquetWrite(e.to_string()))?;
        Ok(Self { writer, schema })
    }

    fn write_lines(&mut self, lines: &[json::Line]) -> Result<(), Error> {
        // Builders
        let cap = lines.len();
        let mut run_id_builder = StringBuilder::with_capacity(cap, 0);
        let mut time_builder = Int64Builder::with_capacity(cap);
        let mut fetch_index_builder = Int64Builder::with_capacity(cap);
        let mut metric_name_builder = StringBuilder::with_capacity(cap, 0);
        let mut metric_kind_builder = StringBuilder::with_capacity(cap, 0);
        let mut value_int_builder = UInt64Builder::with_capacity(cap);
        let mut value_float_builder = Float64Builder::with_capacity(cap);
        let mut labels_builder = StringBuilder::with_capacity(cap, 0);

        for line in lines {
            run_id_builder.append_value(line.run_id.to_string());
            time_builder.append_value(u128_to_i64_saturating(line.time));
            fetch_index_builder.append_value(u64_to_i64_saturating(line.fetch_index));
            metric_name_builder.append_value(&line.metric_name);
            let kind = match line.metric_kind {
                json::MetricKind::Counter => "counter",
                json::MetricKind::Gauge => "gauge",
            };
            metric_kind_builder.append_value(kind);
            match line.value {
                json::LineValue::Int(v) => {
                    value_int_builder.append_value(v);
                    value_float_builder.append_null();
                }
                json::LineValue::Float(v) => {
                    value_int_builder.append_null();
                    value_float_builder.append_value(v);
                }
            }
            let labels_json = serde_json::to_string(&line.labels)?;
            labels_builder.append_value(labels_json);
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(run_id_builder.finish()) as ArrayRef,
            Arc::new(time_builder.finish()) as ArrayRef,
            Arc::new(fetch_index_builder.finish()) as ArrayRef,
            Arc::new(metric_name_builder.finish()) as ArrayRef,
            Arc::new(metric_kind_builder.finish()) as ArrayRef,
            Arc::new(value_int_builder.finish()) as ArrayRef,
            Arc::new(value_float_builder.finish()) as ArrayRef,
            Arc::new(labels_builder.finish()) as ArrayRef,
        ];

        let batch = RecordBatch::try_new(self.schema.clone(), columns)
            .map_err(|e| Error::ParquetWrite(e.to_string()))?;
        self.writer
            .write(&batch)
            .map_err(|e| Error::ParquetWrite(e.to_string()))?;
        Ok(())
    }

    fn finish(&mut self) -> Result<(), Error> {
        let _sz = self
            .writer
            .close()
            .map_err(|e| Error::ParquetWrite(e.to_string()))?;
        Ok(())
    }
}

fn u128_to_i64_saturating(v: u128) -> i64 {
    if v > i64::MAX as u128 {
        i64::MAX
    } else {
        v as i64
    }
}

fn u64_to_i64_saturating(v: u64) -> i64 {
    if v > i64::MAX as u64 {
        i64::MAX
    } else {
        v as i64
    }
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
        expiration: Duration,
    ) -> Result<Self, io::Error> {
        let fp = tokio::fs::File::create(&capture_path).await?;
        let fp = fp.into_std().await;

        let inner = Inner {
            registry: Registry::new(GenerationalStorage::new(AtomicStorage)),
            recency: Recency::new(
                quanta::Clock::new(),
                MetricKindMask::GAUGE | MetricKindMask::COUNTER,
                Some(expiration),
            ),
        };

        // Choose sink based on extension
        let sink = if capture_path
            .extension()
            .is_some_and(|ext| ext == OsStr::new("parquet"))
        {
            // Safety: Any error from parquet creation is turned into io::Error here to satisfy signature
            match ParquetSink::new(fp) {
                Ok(p) => CaptureSink::Parquet(p),
                Err(e) => {
                    // convert to io::Error with context
                    return Err(io::Error::new(io::ErrorKind::Other, e.to_string()));
                }
            }
        } else {
            CaptureSink::Json(JsonSink::new(fp))
        };

        Ok(Self {
            run_id: Uuid::new_v4(),
            fetch_index: 0,
            sink,
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
        self.sink.write_lines(&lines)?;

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
                        // Finish sink before returning
                        if let Err(e) = self.sink.finish() {
                            warn!("failed to finish capture sink: {e}");
                        }
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
