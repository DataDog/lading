//! Pattern-focused file generator.
//!
//! This generator reuses the file generator infrastructure but precomputes a
//! deterministic cycle of log lines that follow common operational patterns.
//! The intent is to offer structured, repeated sequences (HTTP successes,
//! transient errors, background jobs, slow database queries, etc.) instead of
//! purely random log lines while maintaining determinism and throughput
//! guarantees.

use std::{
    fmt::Write as _,
    num::NonZeroU32,
    path::PathBuf,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use byte_unit::{Byte, Unit};
use metrics::counter;
use rand::{Rng, SeedableRng, rngs::StdRng};
use serde::{Deserialize, Serialize};
use tokio::{
    fs,
    io::{AsyncWriteExt, BufWriter},
    task::{JoinError, JoinSet},
};
use tracing::{debug, error, info};

use crate::generator::common::{
    BytesThrottleConfig, MetricsBuilder, ThrottleConversionError, create_throttle,
};

use super::General;

const HOSTS: &[&str] = &[
    "checkout-01",
    "checkout-02",
    "auth-01",
    "auth-02",
    "billing-01",
    "catalog-01",
];

const REMOTE_ADDRS: &[&str] = &[
    "10.42.0.12",
    "10.42.0.18",
    "10.43.1.7",
    "10.44.0.22",
    "10.55.12.5",
];

const METHODS: &[&str] = &["GET", "POST", "PUT", "PATCH"];

#[derive(Debug, Clone, Copy)]
struct HttpPath {
    service: &'static str,
    path: &'static str,
    controller: &'static str,
}

const HTTP_PATHS: &[HttpPath] = &[
    HttpPath {
        service: "checkout",
        path: "/api/cart",
        controller: "CartController",
    },
    HttpPath {
        service: "checkout",
        path: "/api/checkout",
        controller: "CheckoutController",
    },
    HttpPath {
        service: "auth",
        path: "/api/login",
        controller: "AuthController",
    },
    HttpPath {
        service: "auth",
        path: "/api/token",
        controller: "TokenController",
    },
    HttpPath {
        service: "billing",
        path: "/api/payments",
        controller: "PaymentsController",
    },
    HttpPath {
        service: "catalog",
        path: "/api/products",
        controller: "CatalogController",
    },
];

const USERS: &[&str] = &["alice", "bob", "carol", "dave", "erin", "frank"];
const REGIONS: &[&str] = &["us-east-1", "us-west-2", "eu-west-1"];
const JOB_NAMES: &[&str] = &[
    "daily_sync",
    "metrics_rollup",
    "email_digest",
    "billing_settlement",
    "cache_warmup",
];
const JOB_STEPS: &[&str] = &["enqueue", "fetch", "process", "aggregate", "finalize"];
const QUEUES: &[&str] = &["critical", "default", "low"];
const UPSTREAMS: &[&str] = &[
    "inventory",
    "payment",
    "users-service",
    "shipping",
    "pricing",
];
const HTTP_ERROR_REASONS: &[&str] = &[
    "upstream_timeout",
    "gateway_unreachable",
    "validation_failure",
];

#[derive(Debug, Clone, Copy)]
struct DbEvent {
    name: &'static str,
    service: &'static str,
    table: &'static str,
}

const DB_EVENTS: &[DbEvent] = &[
    DbEvent {
        name: "orders.load_pending",
        service: "checkout",
        table: "orders",
    },
    DbEvent {
        name: "users.refresh_session",
        service: "auth",
        table: "sessions",
    },
    DbEvent {
        name: "billing.apply_adjustment",
        service: "billing",
        table: "invoices",
    },
    DbEvent {
        name: "catalog.low_inventory",
        service: "catalog",
        table: "inventory",
    },
];

const DB_INSTANCES: &[&str] = &[
    "pg-primary",
    "pg-replica",
    "aurora-writer",
    "aurora-reader",
];

#[derive(Debug, Clone, Copy)]
enum PatternKind {
    HttpSuccess,
    HttpError,
    BackgroundJob,
    DatabaseSlow,
}

#[derive(thiserror::Error, Debug)]
/// Errors produced by the patterned log generator.
pub enum Error {
    /// Wrapper around [`std::io::Error`].
    #[error("Io error: {0}")]
    Io(#[from] ::std::io::Error),
    /// Failed to open a file for writing.
    #[error("Failed to open file {path:?}: {source}. Ensure parent directory exists.")]
    FileOpen {
        /// The path that failed to open
        path: PathBuf,
        /// The underlying IO error
        source: ::std::io::Error,
    },
    /// Pattern cache construction error.
    #[error("Pattern cache error: {0}")]
    PatternCache(#[from] PatternCacheError),
    /// Child sub-task error.
    #[error("Child join error: {0}")]
    Child(#[from] JoinError),
    /// Encountered a zero value that is not permitted.
    #[error("Value provided must not be zero")]
    Zero,
    /// Provided byte-sized value exceeded `u32::MAX`.
    #[error("Provided byte value overflowed u32 for {field}")]
    Overflow {
        /// Field name being converted
        field: &'static str,
    },
    /// Throttle error
    #[error("Throttle error: {0}")]
    Throttle(#[from] lading_throttle::Error),
    /// Throttle conversion error
    #[error("Throttle configuration error: {0}")]
    ThrottleConversion(#[from] ThrottleConversionError),
}

#[derive(thiserror::Error, Debug)]
/// Errors produced while building the patterned cache.
pub enum PatternCacheError {
    /// Distribution weights summed to zero.
    #[error("Pattern distribution weights must contain at least one non-zero value")]
    EmptyDistribution,
    /// Failed converting cache capacity to machine addressable size.
    #[error("Pattern cache size exceeded supported limits")]
    CacheTooLarge,
    /// Generated log line exceeds configured maximum size.
    #[error("Generated line size {size} bytes exceeds configured limit of {limit} bytes")]
    LineTooLarge {
        /// Size of the generated line
        size: usize,
        /// Configured limit
        limit: u32,
    },
    /// Cache build yielded no blocks.
    #[error("Pattern cache contained no log lines")]
    EmptyCache,
}

fn default_rotation() -> bool {
    true
}

fn default_maximum_line_size() -> Byte {
    Byte::from_u64_with_unit(4, Unit::KiB).expect("hard-coded unit conversion must succeed")
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
/// Configuration of the patterned generator.
pub struct Config {
    /// Seed for deterministic pattern selection.
    pub seed: [u8; 32],
    /// Template for file paths. "%NNN%" is replaced with duplicate number.
    pub path_template: String,
    /// Number of duplicate writers spawned from the same template.
    pub duplicates: u8,
    /// Weighting for pattern families.
    #[serde(default)]
    pub distribution: PatternDistribution,
    /// Maximum soft bytes per file before rotation.
    maximum_bytes_per_file: Byte,
    /// Byte budget enforced per second.
    bytes_per_second: Option<Byte>,
    /// Maximum bytes to prebuild in cache.
    maximum_prebuild_cache_size_bytes: Byte,
    /// Maximum generated line size.
    #[serde(default = "default_maximum_line_size")]
    maximum_line_size: Byte,
    /// Whether to rotate files when `maximum_bytes_per_file` is reached.
    #[serde(default = "default_rotation")]
    rotate: bool,
    /// Optional throttle override.
    pub throttle: Option<BytesThrottleConfig>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
#[serde(deny_unknown_fields)]
/// Pattern weight configuration.
pub struct PatternDistribution {
    /// Relative weight for HTTP success patterns.
    pub http_success: u32,
    /// Relative weight for HTTP error patterns.
    pub http_error: u32,
    /// Relative weight for background job patterns.
    pub background_job: u32,
    /// Relative weight for slow database query patterns.
    pub database_slow: u32,
}

impl Default for PatternDistribution {
    fn default() -> Self {
        Self {
            http_success: 60,
            http_error: 15,
            background_job: 15,
            database_slow: 10,
        }
    }
}

impl PatternDistribution {
    fn total_weight(&self) -> u32 {
        self.http_success
            .saturating_add(self.http_error)
            .saturating_add(self.background_job)
            .saturating_add(self.database_slow)
    }
}

#[derive(Debug)]
/// Server managing patterned file generation.
pub struct Server {
    handles: JoinSet<Result<(), Error>>,
    shutdown: lading_signal::Watcher,
}

impl Server {
    /// Create a new patterned generator server.
    pub fn new(
        general: General,
        config: Config,
        shutdown: lading_signal::Watcher,
    ) -> Result<Self, Error> {
        let mut rng = StdRng::from_seed(config.seed);
        let _labels = MetricsBuilder::new("file_gen_patterned")
            .with_id(general.id)
            .build();

        let maximum_bytes_per_file =
            byte_to_non_zero_u32(&config.maximum_bytes_per_file, "maximum_bytes_per_file")?;
        let maximum_prebuild_cache_size_bytes = byte_to_non_zero_u32(
            &config.maximum_prebuild_cache_size_bytes,
            "maximum_prebuild_cache_size_bytes",
        )?;
        let maximum_line_size =
            byte_to_u32(&config.maximum_line_size, "maximum_line_size")?;
        if maximum_line_size == 0 {
            return Err(Error::Zero);
        }

        let pattern_cache = Arc::new(build_pattern_cache(
            config.seed,
            maximum_prebuild_cache_size_bytes,
            maximum_line_size,
            config.distribution,
        )?);

        let mut handles = JoinSet::new();
        let file_index = Arc::new(AtomicU32::new(0));

        for _ in 0..config.duplicates {
            let throttle = create_throttle(
                config.throttle.as_ref(),
                config.bytes_per_second.as_ref(),
            )?;

            let child = Child {
                path_template: config.path_template.clone(),
                maximum_bytes_per_file,
                throttle,
                cache: Arc::clone(&pattern_cache),
                file_index: Arc::clone(&file_index),
                rotate: config.rotate,
                shutdown: shutdown.clone(),
            };

            handles.spawn(child.spin());
        }

        // Mix rng so that each server has distinct internal state across duplicates.
        // This keeps determinism tied to configuration seed while avoiding identical
        // scheduling across children in the unlikely event of identical runtime.
        let _: u64 = rng.random();

        Ok(Self { handles, shutdown })
    }

    /// Run the generator until completion or shutdown.
    pub async fn spin(mut self) -> Result<(), Error> {
        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);

        loop {
            tokio::select! {
                () = &mut shutdown_wait => {
                    info!("shutdown signal received");
                    while let Some(child_result) = self.handles.join_next().await {
                        let child_result: Result<Result<(), Error>, JoinError> = child_result;
                        let child_spin_result: Result<(), Error> = child_result.map_err(Error::Child)?;
                        child_spin_result?;
                    }
                    return Ok(());
                }
                Some(child_result) = self.handles.join_next() => {
                    let child_result: Result<Result<(), Error>, JoinError> = child_result;

                    let child_spin_result: Result<(), Error> = match child_result {
                        Ok(r) => r,
                        Err(join_err) => {
                            error!("Child task panicked: {join_err}");
                            return Err(Error::Child(join_err));
                        }
                    };

                    if let Err(err) = child_spin_result {
                        error!("Child task failed: {err}");
                        return Err(err);
                    }

                    if self.handles.is_empty() {
                        error!("All child tasks completed unexpectedly before shutdown");
                        return Ok(());
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
struct PatternCache {
    blocks: Vec<PatternBlock>,
}

#[derive(Debug, Clone)]
struct PatternBlock {
    bytes: Vec<u8>,
    total_bytes: NonZeroU32,
}

#[derive(Debug)]
struct PatternHandle {
    idx: usize,
}

impl PatternCache {
    fn handle(&self) -> PatternHandle {
        PatternHandle { idx: 0 }
    }

    fn peek_next_size(&self, handle: &PatternHandle) -> NonZeroU32 {
        self.blocks[handle.idx].total_bytes
    }

    fn advance<'a>(&'a self, handle: &mut PatternHandle) -> &'a PatternBlock {
        let block = &self.blocks[handle.idx];
        handle.idx = (handle.idx + 1) % self.blocks.len();
        block
    }
}

impl PatternBlock {
    fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

fn build_pattern_cache(
    seed: [u8; 32],
    capacity: NonZeroU32,
    maximum_line_size: u32,
    distribution: PatternDistribution,
) -> Result<PatternCache, PatternCacheError> {
    let capacity_usize = usize::try_from(capacity.get()).map_err(|_| PatternCacheError::CacheTooLarge)?;
    let mut generator = PatternGenerator::new(seed, distribution)?;
    let mut blocks: Vec<PatternBlock> = Vec::with_capacity(512);
    let mut accumulated: usize = 0;
    let line_limit = usize::try_from(maximum_line_size)
        .map_err(|_| PatternCacheError::LineTooLarge {
            size: maximum_line_size as usize,
            limit: maximum_line_size,
        })?;

    while accumulated < capacity_usize {
        let sequence = generator.next_sequence();
        if sequence.is_empty() {
            debug!("Generated empty sequence while building pattern cache");
            continue;
        }

        for line in sequence {
            let size = line.len();
            if size == 0 {
                continue;
            }
            if size > line_limit {
                return Err(PatternCacheError::LineTooLarge {
                    size,
                    limit: maximum_line_size,
                });
            }

            let size_u32 = u32::try_from(size).map_err(|_| PatternCacheError::LineTooLarge {
                size,
                limit: maximum_line_size,
            })?;
            let total_bytes = NonZeroU32::new(size_u32).ok_or(PatternCacheError::LineTooLarge {
                size,
                limit: maximum_line_size,
            })?;

            accumulated = accumulated.saturating_add(size);
            blocks.push(PatternBlock { bytes: line, total_bytes });

            if accumulated >= capacity_usize {
                break;
            }
        }
    }

    if blocks.is_empty() {
        return Err(PatternCacheError::EmptyCache);
    }

    info!(
        cache_bytes = capacity.get(),
        line_count = blocks.len(),
        "constructed patterned cache",
    );

    Ok(PatternCache { blocks })
}

#[derive(Debug)]
struct PatternGenerator {
    rng: StdRng,
    timestamp_ms: u64,
    weights: PatternDistribution,
    total_weight: u32,
    request_id: u64,
    job_id: u64,
    db_query_id: u64,
    host_cursor: usize,
    user_cursor: usize,
    method_cursor: usize,
    path_cursor: usize,
    remote_cursor: usize,
    region_cursor: usize,
    queue_cursor: usize,
    job_cursor: usize,
    db_event_cursor: usize,
    db_instance_cursor: usize,
}

impl PatternGenerator {
    fn new(seed: [u8; 32], weights: PatternDistribution) -> Result<Self, PatternCacheError> {
        let total_weight = weights.total_weight();
        if total_weight == 0 {
            return Err(PatternCacheError::EmptyDistribution);
        }

        Ok(Self {
            rng: StdRng::from_seed(seed),
            timestamp_ms: 1_700_000_000_000,
            weights,
            total_weight,
            request_id: 1,
            job_id: 1,
            db_query_id: 1,
            host_cursor: 0,
            user_cursor: 0,
            method_cursor: 0,
            path_cursor: 0,
            remote_cursor: 0,
            region_cursor: 0,
            queue_cursor: 0,
            job_cursor: 0,
            db_event_cursor: 0,
            db_instance_cursor: 0,
        })
    }

    fn next_sequence(&mut self) -> Vec<Vec<u8>> {
        match self.choose_pattern() {
            PatternKind::HttpSuccess => self.http_success_sequence(),
            PatternKind::HttpError => self.http_error_sequence(),
            PatternKind::BackgroundJob => self.background_job_sequence(),
            PatternKind::DatabaseSlow => self.database_slow_sequence(),
        }
    }

    fn choose_pattern(&mut self) -> PatternKind {
        let roll = self.rng.random_range(0..self.total_weight);
        let mut cumulative = self.weights.http_success;
        if roll < cumulative {
            return PatternKind::HttpSuccess;
        }
        cumulative = cumulative.saturating_add(self.weights.http_error);
        if roll < cumulative {
            return PatternKind::HttpError;
        }
        cumulative = cumulative.saturating_add(self.weights.background_job);
        if roll < cumulative {
            return PatternKind::BackgroundJob;
        }
        PatternKind::DatabaseSlow
    }

    fn http_success_sequence(&mut self) -> Vec<Vec<u8>> {
        let host = self.next_host();
        let service = service_from_host(host);
        let http_path = self.next_http_path(service);
        let path = http_path.path;
        let controller = http_path.controller;
        let method = self.next_method();
        let user = self.next_user();
        let remote_addr = self.next_remote_addr();
        let region = self.next_region();
        let request_id = self.next_request_id();
        let trace_id = format!("trace-{:#016x}", self.rng.random::<u64>());
        let latency_ms = self.rng.random_range(45_u64..220_u64);
        let db_time_ms = self.rng.random_range(6_u64..45_u64);
        let bytes_out = self.rng.random_range(700_u64..4096_u64);
        let queue_time_ms = self.rng.random_range(1_u64..12_u64);
        let stage = JOB_STEPS[self.job_cursor % JOB_STEPS.len()];
        self.job_cursor = (self.job_cursor + 1) % JOB_STEPS.len();
        let db_calls = self.rng.random_range(1_u32..4_u32);

        let mut lines = Vec::with_capacity(3);

        let ts_start = self.advance_time(5, 20);
        lines.push(render_line(|line| {
            write!(
                line,
                "ts={ts_start} level=INFO service={service} host={host} env=prod region={region} msg=\"request_received\" request_id={request_id} method={method} path={path} user={user} remote_addr={remote_addr} trace_id={trace_id}"
            )
        }));

        let ts_controller = self.advance_time(1, 8);
        lines.push(render_line(|line| {
            write!(
                line,
                "ts={ts_controller} level=INFO service={service} host={host} env=prod region={region} msg=\"controller_dispatch\" request_id={request_id} controller={controller} stage={stage} db_calls={db_calls} trace_id={trace_id}"
            )
        }));

        let ts_finish = self.advance_time(latency_ms, latency_ms + 40);
        lines.push(render_line(|line| {
            write!(
                line,
                "ts={ts_finish} level=INFO service={service} host={host} env=prod region={region} msg=\"request_completed\" request_id={request_id} status_code=200 latency_ms={latency_ms} db_duration_ms={db_time_ms} queue_ms={queue_time_ms} bytes_out={bytes_out} trace_id={trace_id}"
            )
        }));

        lines
    }

    fn http_error_sequence(&mut self) -> Vec<Vec<u8>> {
        let host = self.next_host();
        let service = service_from_host(host);
        let http_path = self.next_http_path(service);
        let path = http_path.path;
        let method = self.next_method();
        let user = self.next_user();
        let remote_addr = self.next_remote_addr();
        let region = self.next_region();
        let request_id = self.next_request_id();
        let trace_id = format!("trace-{:#016x}", self.rng.random::<u64>());
        let latency_ms = self.rng.random_range(220_u64..620_u64);
        let retry_in_ms = self.rng.random_range(150_u64..450_u64);
        let upstream = UPSTREAMS[self.rng.random_range(0..UPSTREAMS.len())];
        let error_reason = HTTP_ERROR_REASONS[self.rng.random_range(0..HTTP_ERROR_REASONS.len())];

        let mut lines = Vec::with_capacity(4);

        let ts_start = self.advance_time(5, 18);
        lines.push(render_line(|line| {
            write!(
                line,
                "ts={ts_start} level=INFO service={service} host={host} env=prod region={region} msg=\"request_received\" request_id={request_id} method={method} path={path} user={user} remote_addr={remote_addr} trace_id={trace_id}"
            )
        }));

        let ts_warn = self.advance_time(20, 80);
        lines.push(render_line(|line| {
            write!(
                line,
                "ts={ts_warn} level=WARN service={service} host={host} env=prod region={region} msg=\"backend_timeout\" request_id={request_id} upstream={upstream} attempt=1 trace_id={trace_id}"
            )
        }));

        let ts_error = self.advance_time(5, 20);
        lines.push(render_line(|line| {
            write!(
                line,
                "ts={ts_error} level=ERROR service={service} host={host} env=prod region={region} msg=\"request_failed\" request_id={request_id} error={error_reason} trace_id={trace_id}"
            )
        }));

        let ts_finish = self.advance_time(latency_ms, latency_ms + 80);
        lines.push(render_line(|line| {
            write!(
                line,
                "ts={ts_finish} level=INFO service={service} host={host} env=prod region={region} msg=\"request_completed\" request_id={request_id} status_code=504 latency_ms={latency_ms} retry_in_ms={retry_in_ms} trace_id={trace_id}"
            )
        }));

        lines
    }

    fn background_job_sequence(&mut self) -> Vec<Vec<u8>> {
        let host = self.next_host();
        let service = service_from_host(host);
        let region = self.next_region();
        let job_name = self.next_job_name();
        let job_id = self.next_job_id();
        let queue = self.next_queue();
        let attempt = self.rng.random_range(1_u32..3_u32);
        let processed = self.rng.random_range(500_u64..2_000_u64);
        let batch_size = self.rng.random_range(200_u64..800_u64);
        let duration = self.rng.random_range(1_200_u64..6_000_u64);

        let mut lines = Vec::with_capacity(3);

        let ts_start = self.advance_time(30, 90);
        lines.push(render_line(|line| {
            write!(
                line,
                "ts={ts_start} level=INFO service={service} host={host} env=prod region={region} msg=\"job_started\" job={job_name} job_id={job_id} queue={queue} attempt={attempt}"
            )
        }));

        let ts_progress = self.advance_time(50, 200);
        lines.push(render_line(|line| {
            write!(
                line,
                "ts={ts_progress} level=INFO service={service} host={host} env=prod region={region} msg=\"job_progress\" job={job_name} job_id={job_id} processed={processed} batch_size={batch_size}"
            )
        }));

        let ts_finish = self.advance_time(duration, duration + 600);
        lines.push(render_line(|line| {
            write!(
                line,
                "ts={ts_finish} level=INFO service={service} host={host} env=prod region={region} msg=\"job_completed\" job={job_name} job_id={job_id} duration_ms={duration} success=true"
            )
        }));

        lines
    }

    fn database_slow_sequence(&mut self) -> Vec<Vec<u8>> {
        let db_event = self.next_db_event();
        let service = db_event.service;
        let host = self.next_host_for_service(service);
        let region = self.next_region();
        let db_instance = DB_INSTANCES[self.db_instance_cursor % DB_INSTANCES.len()];
        self.db_instance_cursor = (self.db_instance_cursor + 1) % DB_INSTANCES.len();

        let query_id = self.next_db_query_id();
        let name = db_event.name;
        let table = db_event.table;
        let threshold_ms = self.rng.random_range(250_u64..450_u64);
        let elapsed_ms = threshold_ms + self.rng.random_range(120_u64..540_u64);
        let rows = self.rng.random_range(1_u64..5_000_u64);
        let cache_hit = if self.rng.random_range(0..3) == 0 { "false" } else { "true" };

        let mut lines = Vec::with_capacity(3);

        let ts_start = self.advance_time(15, 40);
        lines.push(render_line(|line| {
            write!(
                line,
                "ts={ts_start} level=INFO service={service} host={host} env=prod region={region} msg=\"query_started\" query_name={name} db_instance={db_instance} query_id={query_id}"
            )
        }));

        let ts_warn = self.advance_time(threshold_ms, threshold_ms + 30);
        lines.push(render_line(|line| {
            write!(
                line,
                "ts={ts_warn} level=WARN service={service} host={host} env=prod region={region} msg=\"query_slow\" query_name={name} table={table} threshold_ms={threshold_ms} elapsed_ms={elapsed_ms} query_id={query_id}"
            )
        }));

        let ts_finish = self.advance_time(10, 25);
        lines.push(render_line(|line| {
            write!(
                line,
                "ts={ts_finish} level=INFO service={service} host={host} env=prod region={region} msg=\"query_completed\" query_name={name} rows={rows} cache_hit={cache_hit} elapsed_ms={elapsed_ms} query_id={query_id}"
            )
        }));

        lines
    }

    fn next_host(&mut self) -> &'static str {
        let host = HOSTS[self.host_cursor % HOSTS.len()];
        self.host_cursor = (self.host_cursor + 1) % HOSTS.len();
        host
    }

    fn next_host_for_service(&mut self, service: &str) -> &'static str {
        for offset in 0..HOSTS.len() {
            let idx = (self.host_cursor + offset) % HOSTS.len();
            let host = HOSTS[idx];
            if service_from_host(host) == service {
                self.host_cursor = (idx + 1) % HOSTS.len();
                return host;
            }
        }
        self.next_host()
    }

    fn next_user(&mut self) -> &'static str {
        let user = USERS[self.user_cursor % USERS.len()];
        self.user_cursor = (self.user_cursor + 1) % USERS.len();
        user
    }

    fn next_method(&mut self) -> &'static str {
        let method = METHODS[self.method_cursor % METHODS.len()];
        self.method_cursor = (self.method_cursor + 1) % METHODS.len();
        method
    }

    fn next_http_path(&mut self, service: &str) -> HttpPath {
        for offset in 0..HTTP_PATHS.len() {
            let idx = (self.path_cursor + offset) % HTTP_PATHS.len();
            let candidate = HTTP_PATHS[idx];
            if candidate.service == service {
                self.path_cursor = (idx + 1) % HTTP_PATHS.len();
                return candidate;
            }
        }
        let candidate = HTTP_PATHS[self.path_cursor % HTTP_PATHS.len()];
        self.path_cursor = (self.path_cursor + 1) % HTTP_PATHS.len();
        candidate
    }

    fn next_remote_addr(&mut self) -> &'static str {
        let addr = REMOTE_ADDRS[self.remote_cursor % REMOTE_ADDRS.len()];
        self.remote_cursor = (self.remote_cursor + 1) % REMOTE_ADDRS.len();
        addr
    }

    fn next_region(&mut self) -> &'static str {
        let region = REGIONS[self.region_cursor % REGIONS.len()];
        self.region_cursor = (self.region_cursor + 1) % REGIONS.len();
        region
    }

    fn next_job_name(&mut self) -> &'static str {
        let job = JOB_NAMES[self.job_cursor % JOB_NAMES.len()];
        self.job_cursor = (self.job_cursor + 1) % JOB_NAMES.len();
        job
    }

    fn next_queue(&mut self) -> &'static str {
        let queue = QUEUES[self.queue_cursor % QUEUES.len()];
        self.queue_cursor = (self.queue_cursor + 1) % QUEUES.len();
        queue
    }

    fn next_db_event(&mut self) -> DbEvent {
        let event = DB_EVENTS[self.db_event_cursor % DB_EVENTS.len()];
        self.db_event_cursor = (self.db_event_cursor + 1) % DB_EVENTS.len();
        event
    }

    fn next_request_id(&mut self) -> String {
        let request_id = format!("req-{:#08x}", self.request_id);
        self.request_id = self.request_id.saturating_add(1);
        request_id
    }

    fn next_job_id(&mut self) -> String {
        let job_id = format!("job-{:#010x}", self.job_id);
        self.job_id = self.job_id.saturating_add(1);
        job_id
    }

    fn next_db_query_id(&mut self) -> String {
        let query_id = format!("query-{:#010x}", self.db_query_id);
        self.db_query_id = self.db_query_id.saturating_add(1);
        query_id
    }

    fn advance_time(&mut self, min_step: u64, max_step: u64) -> u64 {
        let step = if max_step > min_step {
            self.rng.random_range(min_step..max_step)
        } else {
            min_step.max(1)
        };
        self.timestamp_ms = self.timestamp_ms.saturating_add(step);
        self.timestamp_ms
    }
}

fn render_line<F>(mut builder: F) -> Vec<u8>
where
    F: FnMut(&mut String) -> std::fmt::Result,
{
    let mut line = String::with_capacity(256);
    builder(&mut line).expect("writing to string cannot fail");
    line.push('\n');
    line.into_bytes()
}

#[derive(Debug)]
struct Child {
    path_template: String,
    maximum_bytes_per_file: NonZeroU32,
    throttle: lading_throttle::Throttle,
    cache: Arc<PatternCache>,
    rotate: bool,
    file_index: Arc<AtomicU32>,
    shutdown: lading_signal::Watcher,
}

impl Child {
    async fn spin(mut self) -> Result<(), Error> {
        let buffer_capacity = self.throttle.maximum_capacity() as usize;
        let mut total_bytes_written: u64 = 0;
        let maximum_bytes_per_file: u64 = u64::from(self.maximum_bytes_per_file.get());

        let mut file_index = self.file_index.fetch_add(1, Ordering::Relaxed);
        let mut path = path_from_template(&self.path_template, file_index);

        let mut fp = BufWriter::with_capacity(
            buffer_capacity,
            fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&path)
                .await
                .map_err(|source| {
                    error!("Failed to open file {path:?}: {source}. Ensure parent directory exists.");
                    Error::FileOpen {
                        path: path.clone(),
                        source,
                    }
                })?,
        );

        let mut handle = self.cache.handle();
        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);

        loop {
            let total_bytes = self.cache.peek_next_size(&handle);

            tokio::select! {
                result = self.throttle.wait_for(total_bytes) => {
                    match result {
                        Ok(()) => {
                            let block = self.cache.advance(&mut handle);
                            let total_bytes = u64::from(total_bytes.get());

                            fp.write_all(block.bytes()).await?;
                            counter!("bytes_written").increment(total_bytes);
                            total_bytes_written += total_bytes;

                            if total_bytes_written > maximum_bytes_per_file {
                                fp.flush().await?;
                                if self.rotate {
                                    fs::remove_file(&path).await?;
                                }
                                file_index = self.file_index.fetch_add(1, Ordering::Relaxed);
                                path = path_from_template(&self.path_template, file_index);
                                fp = BufWriter::with_capacity(
                                    buffer_capacity,
                                    fs::OpenOptions::new()
                                        .create(true)
                                        .truncate(false)
                                        .write(true)
                                        .open(&path)
                                        .await
                                        .map_err(|source| {
                                            error!("Failed to open file {path:?}: {source}. Ensure parent directory exists.");
                                            Error::FileOpen {
                                                path: path.clone(),
                                                source,
                                            }
                                        })?,
                                );
                                total_bytes_written = 0;
                            }
                        }
                        Err(err) => {
                            error!("Throttle request of {total_bytes} is larger than throttle capacity. Block will be discarded. Error: {err}");
                        }
                    }
                }
                () = &mut shutdown_wait => {
                    fp.flush().await?;
                    info!("shutdown signal received");
                    return Ok(());
                }
            }
        }
    }
}

fn byte_to_non_zero_u32(byte: &Byte, field: &'static str) -> Result<NonZeroU32, Error> {
    let value = byte_to_u32(byte, field)?;
    NonZeroU32::new(value).ok_or(Error::Zero)
}

fn byte_to_u32(byte: &Byte, field: &'static str) -> Result<u32, Error> {
    let value = byte.as_u128();
    u32::try_from(value).map_err(|_| Error::Overflow { field })
}

fn service_from_host(host: &str) -> &str {
    host.split_once('-').map_or(host, |(service, _)| service)
}

fn path_from_template(path_template: &str, index: u32) -> PathBuf {
    let fidx = format!("{index:04}");
    let full_path = path_template.replace("%NNN%", &fidx);
    PathBuf::from(full_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generator_maintains_request_id_across_sequence() {
        let mut generator = PatternGenerator::new([7; 32], PatternDistribution::default())
            .expect("valid generator");
        let lines = generator.http_success_sequence();
        assert!(lines.len() >= 3);

        let request_ids: Vec<String> = lines
            .iter()
            .map(|line| {
                let content = std::str::from_utf8(line).expect("utf8");
                content
                    .split_whitespace()
                    .find(|token| token.starts_with("request_id="))
                    .expect("request_id present")
                    .trim_end_matches('\n')
                    .to_string()
            })
            .collect();
        assert!(request_ids.windows(2).all(|w| w[0] == w[1]));
    }

    #[test]
    fn cache_builds_with_defaults() {
        let cache = build_pattern_cache(
            [1; 32],
            NonZeroU32::new(2048).expect("non-zero"),
            2048,
            PatternDistribution::default(),
        )
        .expect("cache builds");

        assert!(!cache.blocks.is_empty());
    }
}
