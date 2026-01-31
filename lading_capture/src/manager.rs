//! Capture and record lading's internal metrics
//!
//! The manner in which lading instruments its target is pretty simple: we use
//! the [`metrics`] library to record factual things about interaction with the
//! target and then write all that out to disk for later analysis. This means
//! that the generator, blackhole etc code are unaware of anything other than
//! their [`metrics`] integration while [`CaptureManager`] need only hook into
//! that same crate.

pub(crate) mod state_machine;

use std::{
    io::{self, BufWriter},
    path::PathBuf,
    sync::{Arc, LazyLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use arc_swap::ArcSwap;
use tokio::{fs, sync::mpsc, time};

use crate::{
    accumulator,
    accumulator::Accumulator,
    formats::{self, OutputFormat, jsonl, multi, parquet},
    metric::Metric,
};
use metrics::Key;
use metrics_util::registry::{AtomicStorage, Registry};
use rustc_hash::FxHashMap;
use state_machine::{Event, Operation, StateMachine};
use tracing::{error, info, warn};

/// Default duration of a single `Accumulator` tick in milliseconds.
/// Can be overridden via [`CaptureManagerBuilder::tick_duration_ms`].
pub const DEFAULT_TICK_DURATION_MS: u64 = 1_000;

/// Signal watchers required by [`CaptureManager`].
///
/// Groups the three signal watchers to reduce argument count in constructors.
#[derive(Debug)]
pub struct SignalWatchers {
    /// Watcher for shutdown signal.
    pub shutdown: lading_signal::Watcher,
    /// Watcher for experiment started signal.
    pub experiment_started: lading_signal::Watcher,
    /// Watcher for target running signal.
    pub target_running: lading_signal::Watcher,
}

/// Builder for [`CaptureManager`].
///
/// # Example
///
/// ```ignore
/// let manager = CaptureManagerBuilder::new(format)
///     .flush_seconds(5)
///     .tick_duration_ms(100)  // 10Hz sampling
///     .expiration(Duration::from_secs(120))
///     .shutdown(shutdown_watcher)
///     .experiment_started(experiment_watcher)
///     .target_running(target_watcher)
///     .build();
/// ```
pub struct CaptureManagerBuilder<F: OutputFormat, C: Clock = RealClock> {
    format: F,
    flush_seconds: u64,
    tick_duration_ms: u64,
    expiration: Duration,
    shutdown: Option<lading_signal::Watcher>,
    experiment_started: Option<lading_signal::Watcher>,
    target_running: Option<lading_signal::Watcher>,
    clock: C,
}

impl<F: OutputFormat, C: Clock> std::fmt::Debug for CaptureManagerBuilder<F, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CaptureManagerBuilder")
            .field("flush_seconds", &self.flush_seconds)
            .field("tick_duration_ms", &self.tick_duration_ms)
            .field("expiration", &self.expiration)
            .finish_non_exhaustive()
    }
}

impl<F: OutputFormat> CaptureManagerBuilder<F, RealClock> {
    /// Create a new builder with the given output format.
    ///
    /// Uses sensible defaults:
    /// - `flush_seconds`: 1
    /// - `tick_duration_ms`: 1000 (1Hz)
    /// - `expiration`: 60 seconds
    /// - `clock`: [`RealClock`]
    pub fn new(format: F) -> Self {
        Self {
            format,
            flush_seconds: 1,
            tick_duration_ms: DEFAULT_TICK_DURATION_MS,
            expiration: Duration::from_secs(60),
            shutdown: None,
            experiment_started: None,
            target_running: None,
            clock: RealClock::default(),
        }
    }
}

impl<F: OutputFormat, C: Clock> CaptureManagerBuilder<F, C> {
    /// Set the flush interval in seconds.
    #[must_use]
    pub fn flush_seconds(mut self, seconds: u64) -> Self {
        self.flush_seconds = seconds;
        self
    }

    /// Set the tick duration in milliseconds.
    ///
    /// This controls the sampling frequency:
    /// - 1000ms = 1Hz (default)
    /// - 100ms = 10Hz
    /// - 10ms = 100Hz
    #[must_use]
    pub fn tick_duration_ms(mut self, ms: u64) -> Self {
        self.tick_duration_ms = ms;
        self
    }

    /// Set the metric expiration duration.
    #[must_use]
    pub fn expiration(mut self, expiration: Duration) -> Self {
        self.expiration = expiration;
        self
    }

    /// Set the shutdown signal watcher (required).
    #[must_use]
    pub fn shutdown(mut self, watcher: lading_signal::Watcher) -> Self {
        self.shutdown = Some(watcher);
        self
    }

    /// Set the experiment started signal watcher (required).
    #[must_use]
    pub fn experiment_started(mut self, watcher: lading_signal::Watcher) -> Self {
        self.experiment_started = Some(watcher);
        self
    }

    /// Set the target running signal watcher (required).
    #[must_use]
    pub fn target_running(mut self, watcher: lading_signal::Watcher) -> Self {
        self.target_running = Some(watcher);
        self
    }

    /// Set all signal watchers at once (required).
    #[must_use]
    pub fn signals(mut self, signals: SignalWatchers) -> Self {
        self.shutdown = Some(signals.shutdown);
        self.experiment_started = Some(signals.experiment_started);
        self.target_running = Some(signals.target_running);
        self
    }

    /// Set a custom clock (for testing).
    #[must_use]
    pub fn clock<C2: Clock>(self, clock: C2) -> CaptureManagerBuilder<F, C2> {
        CaptureManagerBuilder {
            format: self.format,
            flush_seconds: self.flush_seconds,
            tick_duration_ms: self.tick_duration_ms,
            expiration: self.expiration,
            shutdown: self.shutdown,
            experiment_started: self.experiment_started,
            target_running: self.target_running,
            clock,
        }
    }

    /// Build the [`CaptureManager`].
    ///
    /// # Panics
    ///
    /// Panics if `shutdown`, `experiment_started`, or `target_running` were not set.
    pub fn build(self) -> CaptureManager<F, C>
    where
        C: Clone + 'static,
    {
        let shutdown = self
            .shutdown
            .expect("shutdown watcher is required - call .shutdown()");
        let experiment_started = self
            .experiment_started
            .expect("experiment_started watcher is required - call .experiment_started()");
        let target_running = self
            .target_running
            .expect("target_running watcher is required - call .target_running()");

        let registry = Arc::new(Registry::new(AtomicStorage));
        let (snd, recv) = mpsc::channel(10_000);
        let accumulator = Accumulator::new();

        CaptureManager {
            expiration: self.expiration,
            format: self.format,
            flush_seconds: self.flush_seconds,
            tick_duration_ms: self.tick_duration_ms,
            shutdown: Some(shutdown),
            _experiment_started: experiment_started,
            target_running,
            registry,
            accumulator,
            global_labels: FxHashMap::default(),
            snd,
            recv,
            clock: self.clock,
        }
    }
}

pub(crate) struct Sender {
    pub(crate) snd: mpsc::Sender<Metric>,
}

pub(crate) static HISTORICAL_SENDER: LazyLock<ArcSwap<Option<Arc<Sender>>>> =
    LazyLock::new(|| ArcSwap::new(Arc::new(None)));

/// Minimal clock abstraction for histogram timestamping.
///
/// The full Clock trait has associated types which complicate trait objects.
/// For histogram timestamps we only need `now()`, so we use a minimal trait.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) trait InstantClock: Send + Sync {
    fn now(&self) -> Instant;
}

/// Blanket implementation for any type implementing Clock
impl<C: Clock> InstantClock for C {
    fn now(&self) -> Instant {
        Clock::now(self)
    }
}

/// Clock function abstraction that avoids dynamic dispatch in production.
///
/// Uses an enum to provide zero-cost abstraction for the production path while
/// still supporting test clocks. The `Real` variant compiles to a direct call
/// to `Instant::now()` with no vtable lookup.
pub(crate) enum ClockFn {
    /// Production: direct call to `Instant::now()` with no indirection
    Real,
    /// Test: uses `InstantClock` trait for controllable time
    #[cfg(test)]
    Test(Arc<dyn InstantClock>),
}

impl ClockFn {
    #[inline]
    pub(crate) fn now(&self) -> Instant {
        match self {
            ClockFn::Real => Instant::now(),
            #[cfg(test)]
            ClockFn::Test(clock) => clock.now(),
        }
    }
}

/// Global clock for histogram sample timestamps.
///
/// Counters and gauges are scraped from the registry and timestamped in bulk
/// during tick processing. Histogram samples must be timestamped when recorded,
/// not when scraped. The `metrics::HistogramFn::record` trait provides no
/// timestamp parameter. The clock must be globally accessible.
///
/// `StateMachine::new` stores its clock here. `CaptureHistogram::record` reads
/// from it. In production this is `Instant::now`. In tests this is a controlled
/// clock for deterministic behavior.
pub(crate) static CAPTURE_CLOCK: LazyLock<ArcSwap<ClockFn>> =
    LazyLock::new(|| ArcSwap::from_pointee(ClockFn::Real));

/// Custom histogram implementation that sends samples to `HISTORICAL_SENDER`
struct CaptureHistogram {
    key: Arc<Key>,
}

impl metrics::HistogramFn for CaptureHistogram {
    fn record(&self, value: f64) {
        use crate::metric::{Histogram, Metric};

        // Use the global clock for deterministic timestamping
        let clock_guard = CAPTURE_CLOCK.load();
        let timestamp = clock_guard.now();
        let histogram = Histogram {
            key: (*self.key).clone(),
            timestamp,
            value,
        };
        // Send through HISTORICAL_SENDER. Warn if samples are dropped since
        // this invalidates measurement accuracy.
        let sender_guard = HISTORICAL_SENDER.load();
        if let Some(sender) = sender_guard.as_ref().as_ref() {
            // Use try_send to avoid blocking. If the channel is full,
            // drop the sample to prevent backpressure on the caller.
            if let Err(e) = sender.snd.try_send(Metric::Histogram(histogram)) {
                warn!(
                    key = %self.key.name(),
                    error = %e,
                    "Histogram sample dropped - capture channel full or closed"
                );
            }
        } else {
            warn!(
                key = %self.key.name(),
                "Histogram sample dropped - capture system not initialized"
            );
        }
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
    /// Accumulator errors
    #[error(transparent)]
    Accumulator(#[from] accumulator::Error),
    /// State machine errors
    #[error(transparent)]
    StateMachine(#[from] state_machine::Error),
}

/// Interval abstraction for tick-based operations
pub trait TickInterval: Send {
    /// Wait for the next tick
    fn tick(&mut self) -> impl std::future::Future<Output = ()> + Send;
}

/// Clock abstraction for controllable time in tests
///
/// Following the pattern from `lading_throttle`, allows production code to
/// use real system time while tests can inject a controllable clock for
/// deterministic behavior.
pub trait Clock: Send + Sync {
    /// Interval type for this clock
    type Interval: TickInterval;
    /// Returns the current time in milliseconds since `UNIX_EPOCH`
    fn now_ms(&self) -> u128;
    /// Returns the current time as an Instant
    fn now(&self) -> Instant;
    /// Create an interval that ticks every duration
    fn interval(&self, duration: Duration) -> Self::Interval;
    /// Returns the time-zero instant, used to convert between real timestamps
    /// and logical ticks within the capture manager.
    fn start(&self) -> Instant;
    /// Sets time-zero to the current instant
    fn mark_start(&mut self);
}

/// Real-time interval implementation
pub struct RealInterval {
    inner: time::Interval,
}

impl std::fmt::Debug for RealInterval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RealInterval").finish_non_exhaustive()
    }
}

impl TickInterval for RealInterval {
    async fn tick(&mut self) {
        self.inner.tick().await;
    }
}

/// Production clock implementation using real system time
#[derive(Debug, Clone, Copy)]
pub struct RealClock {
    start_instant: Instant,
    start_system_time: SystemTime,
}

impl Default for RealClock {
    fn default() -> Self {
        Self {
            start_instant: Instant::now(),
            start_system_time: SystemTime::now(),
        }
    }
}

impl Clock for RealClock {
    type Interval = RealInterval;

    fn now_ms(&self) -> u128 {
        let now = Clock::now(self);
        let elapsed = now.duration_since(self.start_instant);
        (self.start_system_time + elapsed)
            .duration_since(UNIX_EPOCH)
            .expect("system time is before UNIX_EPOCH")
            .as_millis()
    }

    fn now(&self) -> Instant {
        Instant::now()
    }

    fn interval(&self, duration: Duration) -> Self::Interval {
        RealInterval {
            inner: time::interval(duration),
        }
    }

    fn start(&self) -> Instant {
        self.start_instant
    }

    fn mark_start(&mut self) {
        self.start_instant = Instant::now();
        self.start_system_time = SystemTime::now();
    }
}

/// Wrangles internal metrics into capture files
///
/// This struct is responsible for capturing all internal metrics sent through
/// [`metrics`] and periodically writing them to disk with format
/// [`line::Line`].
pub struct CaptureManager<F: OutputFormat, C: Clock = RealClock> {
    expiration: Duration,
    format: F,
    flush_seconds: u64,
    /// Duration of a single tick in milliseconds (default: 1000ms = 1Hz)
    tick_duration_ms: u64,
    shutdown: Option<lading_signal::Watcher>,
    _experiment_started: lading_signal::Watcher,
    target_running: lading_signal::Watcher,
    registry: Arc<Registry<Key, AtomicStorage>>,
    accumulator: Accumulator,
    global_labels: FxHashMap<String, String>,
    snd: mpsc::Sender<Metric>,
    recv: mpsc::Receiver<Metric>,
    clock: C,
}

impl<F: OutputFormat, C: Clock> std::fmt::Debug for CaptureManager<F, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CaptureManager")
            .field("accumulator", &self.accumulator)
            .field("global_labels", &self.global_labels)
            .finish_non_exhaustive()
    }
}

impl<F: OutputFormat, C: Clock + Clone + 'static> CaptureManager<F, C> {
    /// Create a new [`CaptureManager`] with a custom format and clock.
    ///
    /// Consider using [`CaptureManagerBuilder`] for a more ergonomic API.
    ///
    /// # Arguments
    ///
    /// * `tick_duration_ms` - Duration of a single tick in milliseconds (e.g., 1000 for 1Hz,
    ///   500 for 2Hz, 100 for 10Hz). This controls the sampling frequency.
    pub fn new_with_format(
        format: F,
        flush_seconds: u64,
        tick_duration_ms: u64,
        expiration: Duration,
        signals: SignalWatchers,
        clock: C,
    ) -> Self {
        CaptureManagerBuilder::new(format)
            .flush_seconds(flush_seconds)
            .tick_duration_ms(tick_duration_ms)
            .expiration(expiration)
            .signals(signals)
            .clock(clock)
            .build()
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
        // Instant timestamps. Manager converts these to ticks using clock.start()
        // as the reference point synchronized with accumulator.current_tick.
        HISTORICAL_SENDER.store(Arc::new(Some(Arc::new(Sender {
            snd: self.snd.clone(),
        }))));

        // Installing the recorder immediately on startup. This does _not_ wait
        // on experiment_started signal, so warmup data will be included in the
        // capture.
        self.install()?;
        info!("Capture manager installed, recording to capture file.");

        // Wait until the target is running then mark time-zero to this
        // event. Clock has started.
        self.target_running.recv().await;
        self.clock.mark_start();

        let mut flush_interval = self
            .clock
            .interval(Duration::from_millis(self.tick_duration_ms));
        let shutdown_wait = self
            .shutdown
            .take()
            .expect("shutdown watcher must be present")
            .recv();
        tokio::pin!(shutdown_wait);

        // Create state machine with owned state
        let mut state_machine = StateMachine::new(
            self.expiration,
            self.format,
            self.flush_seconds,
            self.tick_duration_ms,
            self.registry,
            self.accumulator,
            self.global_labels,
            self.clock,
        );

        // Event loop: tokio select produces Events, state machine processes them
        loop {
            let event = tokio::select! {
                val = self.recv.recv() => {
                    match val {
                        Some(metric) => Event::MetricReceived(metric),
                        None => Event::ChannelClosed,
                    }
                }
                () = flush_interval.tick() => Event::FlushTick,
                () = &mut shutdown_wait => Event::ShutdownSignaled,
            };

            match state_machine.next(event)? {
                Operation::Continue => {}
                Operation::Exit => return Ok(()),
            }
        }
    }
}

impl CaptureManager<formats::jsonl::Format<BufWriter<std::fs::File>>, RealClock> {
    /// Create a new [`CaptureManager`] with file-based JSONL writer
    ///
    /// # Arguments
    ///
    /// * `tick_duration_ms` - Duration of a single tick in milliseconds (e.g., 1000 for 1Hz)
    ///
    /// # Errors
    ///
    /// Function will error if the underlying capture file cannot be opened.
    pub async fn new_jsonl(
        capture_path: PathBuf,
        flush_seconds: u64,
        tick_duration_ms: u64,
        expiration: Duration,
        signals: SignalWatchers,
    ) -> Result<Self, io::Error> {
        let fp = fs::File::create(&capture_path).await?;
        let fp = fp.into_std().await;
        let writer = BufWriter::new(fp);
        let format = jsonl::Format::new(writer);

        Ok(CaptureManagerBuilder::new(format)
            .flush_seconds(flush_seconds)
            .tick_duration_ms(tick_duration_ms)
            .expiration(expiration)
            .signals(signals)
            .build())
    }
}

impl CaptureManager<formats::parquet::Format<BufWriter<std::fs::File>>, RealClock> {
    /// Create a new [`CaptureManager`] with file-based Parquet writer
    ///
    /// # Arguments
    ///
    /// * `tick_duration_ms` - Duration of a single tick in milliseconds (e.g., 1000 for 1Hz)
    ///
    /// # Errors
    ///
    /// Function will error if the underlying capture file cannot be opened or
    /// if Parquet writer creation fails.
    pub async fn new_parquet(
        capture_path: PathBuf,
        compression_level: i32,
        flush_seconds: u64,
        tick_duration_ms: u64,
        expiration: Duration,
        signals: SignalWatchers,
    ) -> Result<Self, formats::Error> {
        let fp = fs::File::create(&capture_path)
            .await
            .map_err(formats::Error::Io)?;
        let fp = fp.into_std().await;
        let writer = BufWriter::new(fp);
        let format = parquet::Format::new(writer, compression_level)?;

        Ok(CaptureManagerBuilder::new(format)
            .flush_seconds(flush_seconds)
            .tick_duration_ms(tick_duration_ms)
            .expiration(expiration)
            .signals(signals)
            .build())
    }
}

impl
    CaptureManager<
        formats::multi::Format<BufWriter<std::fs::File>, BufWriter<std::fs::File>>,
        RealClock,
    >
{
    /// Create a new [`CaptureManager`] with file-based multi-format writer
    ///
    /// Writes to both JSONL and Parquet formats simultaneously. The base path
    /// is used to generate two output files: `{base_path}.jsonl` and
    /// `{base_path}.parquet`.
    ///
    /// # Arguments
    ///
    /// * `tick_duration_ms` - Duration of a single tick in milliseconds (e.g., 1000 for 1Hz)
    ///
    /// # Errors
    ///
    /// Function will error if either capture file cannot be opened or if
    /// format creation fails.
    pub async fn new_multi(
        base_path: PathBuf,
        compression_level: i32,
        flush_seconds: u64,
        tick_duration_ms: u64,
        expiration: Duration,
        signals: SignalWatchers,
    ) -> Result<Self, formats::Error> {
        let jsonl_path = base_path.with_extension("jsonl");
        let parquet_path = base_path.with_extension("parquet");

        let jsonl_file = fs::File::create(&jsonl_path)
            .await
            .map_err(formats::Error::Io)?;
        let jsonl_file = jsonl_file.into_std().await;
        let jsonl_writer = BufWriter::new(jsonl_file);
        let jsonl_format = jsonl::Format::new(jsonl_writer);

        let parquet_file = fs::File::create(&parquet_path)
            .await
            .map_err(formats::Error::Io)?;
        let parquet_file = parquet_file.into_std().await;
        let parquet_writer = BufWriter::new(parquet_file);
        let parquet_format = parquet::Format::new(parquet_writer, compression_level)?;

        let format = multi::Format::new(jsonl_format, parquet_format);

        Ok(CaptureManagerBuilder::new(format)
            .flush_seconds(flush_seconds)
            .tick_duration_ms(tick_duration_ms)
            .expiration(expiration)
            .signals(signals)
            .build())
    }
}

/// Recorder that captures metrics into a registry for later export
#[derive(Clone)]
pub struct CaptureRecorder {
    /// Registry storing metric values
    pub registry: Arc<Registry<Key, AtomicStorage>>,
}

impl std::fmt::Debug for CaptureRecorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CaptureRecorder").finish_non_exhaustive()
    }
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
        key: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        // Histogram samples must be timestamped when recorded, not when scraped.
        // CaptureHistogram sends samples to HISTORICAL_SENDER with timestamps
        // for interval partitioning.
        self.registry
            .get_or_create_histogram(key, |_atomic_bucket| {
                let histogram = CaptureHistogram {
                    key: Arc::new(key.clone()),
                };
                metrics::Histogram::from_arc(Arc::new(histogram))
            })
    }
}
