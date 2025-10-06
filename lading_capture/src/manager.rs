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
    io::{self, BufWriter, Write},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use tokio::sync::Mutex;
use tokio::{fs, sync::mpsc, time};

use crate::{accumulator, accumulator::Accumulator, metric::Metric};
use metrics::Key;
use metrics_util::registry::{AtomicStorage, Registry};
use rustc_hash::FxHashMap;
use state_machine::{Event, Operation, StateMachine};
use std::sync::LazyLock;
use tracing::{error, info};

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
// NOTE I want to extract both clocks out into a common lading_clock but will do
// so in a separate thread of work.
pub trait Clock: Send + Sync {
    /// Interval type for this clock
    type Interval: TickInterval;
    /// Returns the current time in milliseconds since `UNIX_EPOCH`
    fn now_ms(&self) -> u128;
    /// Returns the current time as an Instant
    fn now(&self) -> Instant;
    /// Sleep for the given duration
    fn sleep(&self, duration: Duration) -> impl std::future::Future<Output = ()> + Send;
    /// Create an interval that ticks every duration
    fn interval(&self, duration: Duration) -> Self::Interval;
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
        let now = self.now();
        let elapsed = now.duration_since(self.start_instant);
        (self.start_system_time + elapsed)
            .duration_since(UNIX_EPOCH)
            .expect("system time is before UNIX_EPOCH")
            .as_millis()
    }

    fn now(&self) -> Instant {
        Instant::now()
    }

    async fn sleep(&self, duration: Duration) {
        time::sleep(duration).await;
    }

    fn interval(&self, duration: Duration) -> Self::Interval {
        RealInterval {
            inner: time::interval(duration),
        }
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
            self.clock.sleep(Duration::from_millis(100)).await;
        }

        let mut flush_interval = self
            .clock
            .interval(Duration::from_millis(TICK_DURATION_MS as u64));
        let shutdown_wait = self
            .shutdown
            .take()
            .expect("shutdown watcher must be present")
            .recv();
        tokio::pin!(shutdown_wait);

        // Create state machine with owned state
        let mut state_machine = StateMachine::new(
            self.start,
            self.expiration,
            self.capture_writer,
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
            RealClock::default(),
        ))
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
        _key: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        // nothing, intentionally
        unimplemented!()
    }
}
