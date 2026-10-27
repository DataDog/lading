//! Protobuf `AgentPayload` generation for the agent-to-intake `/api/v0.2/traces` endpoint.
//!
//! This is distinct from the tracer-to-agent `/v0.2/traces` `MessagePack` protocol.
//! Reuse the v0.4 trace generator so both wire formats share the same trace-shape controls.
//!
//! An Agent runs once per host and forwards traces from every tracer (application) on that
//! host. Receivers partition and aggregate by these identifiers, so their cardinality is
//! configured rather than fixed: `hosts` bounds the distinct Agent identities and `tracers`
//! bounds the distinct tracer identities, each tracer belonging to exactly one host.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
};

use prost::Message;
use rand::{Rng, RngExt};
use serde::{Deserialize, Serialize};

use super::v04;
use crate::{
    Error, Generator,
    common::{config::ConfRange, strings::RandomStringPool},
};

#[cfg(test)]
mod tests;
mod wire;

/// The Agent's `PriorityNone` (`math.MinInt8`): the chunk carries no sampling decision.
const PRIORITY_NONE: i32 = -128;
/// Upper bound on `hosts` and `tracers`, keeping precomputed identities small.
const MAX_IDENTITIES: u32 = 100_000;
// Identity strings are windows into a random pool. Sizing it to several times the
// identity bytes keeps windows distinct without building a large pool per generator.
const MIN_STRING_POOL_SIZE: usize = 16 * 1024;
const MAX_STRING_POOL_SIZE: usize = 1_000_000;
// Redraws allowed when a drawn identity string is already in use.
const MAX_UNIQUE_ATTEMPTS: usize = 64;

// Identity string lengths. Receivers act on cardinality, not length, so these are fixed.
const HOST_NAME_LENGTH: usize = 16;
const ENV_LENGTH: usize = 8;
const VERSION_LENGTH: usize = 8;
const LANGUAGE_LENGTH: usize = 8;
const CONTAINER_ID_LENGTH: usize = 64;
const RUNTIME_ID_LENGTH: usize = 36;
const HOST_IDENTITY_BYTES: usize = HOST_NAME_LENGTH + ENV_LENGTH + VERSION_LENGTH;
const TRACER_IDENTITY_BYTES: usize =
    CONTAINER_ID_LENGTH + LANGUAGE_LENGTH + 3 * VERSION_LENGTH + RUNTIME_ID_LENGTH;

/// Configuration validation error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigError {
    /// The nested trace-shape configuration is invalid.
    Traces(v04::ConfigError),
    /// `hosts` or `tracers` is an invalid range (min > max).
    InvalidRange,
    /// `hosts` or `tracers` may be zero.
    ZeroValue,
    /// `hosts` or `tracers` exceeds the maximum allowed value.
    TooManyIdentities,
    /// `tracers` may be smaller than `hosts`, leaving a host without a tracer.
    FewerTracersThanHosts,
}

/// Configuration for agent-to-intake payload generation.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Number of distinct Agent identities (`host_name`, `env`, `agent_version`).
    /// Each payload comes from one host.
    #[serde(default = "default_hosts")]
    pub hosts: ConfRange<u32>,
    /// Number of distinct tracer identities (`container_id`, language, tracer and
    /// application versions, `runtime_id`), distributed across hosts. Must be at
    /// least `hosts`.
    #[serde(default = "default_tracers")]
    pub tracers: ConfRange<u32>,
    /// Trace-shape settings, shared with the v0.4 variant.
    #[serde(default)]
    pub traces: v04::Config,
}

fn default_hosts() -> ConfRange<u32> {
    ConfRange::Inclusive { min: 1, max: 10 }
}

fn default_tracers() -> ConfRange<u32> {
    ConfRange::Inclusive { min: 10, max: 100 }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            hosts: default_hosts(),
            tracers: default_tracers(),
            traces: v04::Config::default(),
        }
    }
}

impl Config {
    /// Validate configuration parameters.
    ///
    /// # Errors
    ///
    /// Returns a `ConfigError` if the trace shape, `hosts`, or `tracers` is invalid.
    pub fn valid(&self) -> Result<(), ConfigError> {
        self.traces.valid().map_err(ConfigError::Traces)?;
        for range in [&self.hosts, &self.tracers] {
            if !range.valid().0 {
                return Err(ConfigError::InvalidRange);
            }
            if range.start() == 0 {
                return Err(ConfigError::ZeroValue);
            }
            if range.end() > MAX_IDENTITIES {
                return Err(ConfigError::TooManyIdentities);
            }
        }
        if self.tracers.start() < self.hosts.end() {
            return Err(ConfigError::FewerTracersThanHosts);
        }
        Ok(())
    }
}

/// One Agent identity and the tracers it forwards.
#[derive(Debug, Clone)]
struct Host {
    /// Envelope fields; `tracer_payloads` is empty.
    envelope: wire::AgentPayload,
    /// Indices into `AgentPayload::tracers`.
    tracers: Vec<usize>,
}

/// Generate protobuf payloads containing complete trace chunks.
#[derive(Debug, Clone)]
pub struct AgentPayload {
    traces: v04::V04,
    hosts: Vec<Host>,
    /// Tracer identities; `chunks` is empty.
    tracers: Vec<wire::TracerPayload>,
    /// Encoded size of each empty tracer identity, so blocks need not clone unused ones.
    tracer_bytes: Vec<usize>,
    last_span_count: u64,
}

impl AgentPayload {
    /// Create a generator, precomputing its host and tracer identities.
    ///
    /// # Errors
    ///
    /// Return an error if configuration validation or string-pool construction fails.
    pub fn with_config<R: Rng>(config: Config, rng: &mut R) -> Result<Self, Error> {
        config.valid().map_err(|error| {
            Error::Validation(format!("invalid agent payload configuration: {error:?}"))
        })?;
        let host_count = config.hosts.sample(rng) as usize;
        let tracer_count = config.tracers.sample(rng) as usize;
        let identity_bytes =
            host_count * HOST_IDENTITY_BYTES + tracer_count * TRACER_IDENTITY_BYTES;
        let pool_size = (identity_bytes * 4).clamp(MIN_STRING_POOL_SIZE, MAX_STRING_POOL_SIZE);
        let pool = RandomStringPool::with_size(rng, pool_size);
        // Host names, container IDs and runtime IDs identify an Agent or tracer, so
        // they are distinct; otherwise the configured cardinality would not be met.
        let mut host_names = BTreeSet::new();
        let mut container_ids = BTreeSet::new();
        let mut runtime_ids = BTreeSet::new();

        let mut hosts = Vec::with_capacity(host_count);
        for _ in 0..host_count {
            hosts.push(Host {
                envelope: wire::AgentPayload {
                    host_name: unique_text(&pool, rng, HOST_NAME_LENGTH, &mut host_names)?,
                    env: text(&pool, rng, ENV_LENGTH)?,
                    agent_version: text(&pool, rng, VERSION_LENGTH)?,
                    tracer_payloads: Vec::new(),
                },
                tracers: Vec::new(),
            });
        }
        let mut tracers = Vec::with_capacity(tracer_count);
        for index in 0..tracer_count {
            tracers.push(wire::TracerPayload {
                container_id: unique_text(&pool, rng, CONTAINER_ID_LENGTH, &mut container_ids)?,
                language_name: text(&pool, rng, LANGUAGE_LENGTH)?,
                language_version: text(&pool, rng, VERSION_LENGTH)?,
                tracer_version: text(&pool, rng, VERSION_LENGTH)?,
                runtime_id: unique_text(&pool, rng, RUNTIME_ID_LENGTH, &mut runtime_ids)?,
                app_version: text(&pool, rng, VERSION_LENGTH)?,
                chunks: Vec::new(),
            });
            // Validation guarantees tracer_count >= host_count, so every host gets one.
            hosts[index % host_count].tracers.push(index);
        }

        Ok(Self {
            traces: v04::V04::with_config(config.traces, rng)?,
            hosts,
            tracer_bytes: tracers.iter().map(Message::encoded_len).collect(),
            tracers,
            last_span_count: 0,
        })
    }
}

fn text<R: Rng + ?Sized>(
    pool: &RandomStringPool,
    rng: &mut R,
    length: usize,
) -> Result<String, Error> {
    pool.of_size(rng, length)
        .map(str::to_owned)
        .ok_or(Error::StringGenerate)
}

/// Draw a string of `length` not already in `seen`, redrawing on collision.
fn unique_text<R: Rng + ?Sized>(
    pool: &RandomStringPool,
    rng: &mut R,
    length: usize,
    seen: &mut BTreeSet<String>,
) -> Result<String, Error> {
    for _ in 0..MAX_UNIQUE_ATTEMPTS {
        let candidate = text(pool, rng, length)?;
        if seen.insert(candidate.clone()) {
            return Ok(candidate);
        }
    }
    Err(Error::StringGenerate)
}

// The containing message fields (AgentPayload.tracerPayloads = 5 and
// TracerPayload.chunks = 6) have a one-byte key. Include each length prefix,
// including changes in its width, without rescanning previously generated chunks.
fn message_size(bytes: usize) -> usize {
    1 + prost::length_delimiter_len(bytes) + bytes
}

impl crate::Serialize for AgentPayload {
    fn to_bytes<W, R>(&mut self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        W: Write,
        R: Rng + Sized,
    {
        self.last_span_count = 0;
        if max_bytes == 0 {
            return Ok(());
        }

        let host = &self.hosts[rng.random_range(0..self.hosts.len())];
        // Only tracers that receive a chunk are materialized: tracer index ->
        // (encoded size including its chunks, chunks). A host may own many tracers.
        let mut groups: BTreeMap<usize, (usize, Vec<wire::TraceChunk>)> = BTreeMap::new();
        let mut total_bytes = host.envelope.encoded_len();
        loop {
            let trace = self.traces.generate(&mut rng)?;
            let chunk = wire::TraceChunk::from(trace);
            let tracer = host.tracers[rng.random_range(0..host.tracers.len())];
            let (previous, current_bytes) = match groups.get(&tracer) {
                Some(&(bytes, _)) => (message_size(bytes), bytes),
                None => (0, self.tracer_bytes[tracer]),
            };
            let next_tracer_bytes = current_bytes + message_size(chunk.encoded_len());
            let next_total = total_bytes - previous + message_size(next_tracer_bytes);
            if next_total > max_bytes {
                break;
            }
            self.last_span_count += chunk.spans.len() as u64;
            let group = groups.entry(tracer).or_insert_with(|| (0, Vec::new()));
            group.0 = next_tracer_bytes;
            group.1.push(chunk);
            total_bytes = next_total;
        }

        // A budget smaller than one complete trace produces no block, not an
        // empty protobuf message that could be mistaken for useful traffic.
        if self.last_span_count == 0 {
            return Ok(());
        }
        let mut payload = host.envelope.clone();
        payload.tracer_payloads = groups
            .into_iter()
            .map(|(tracer, (_, chunks))| wire::TracerPayload {
                chunks,
                ..self.tracers[tracer].clone()
            })
            .collect();
        let mut buffer = Vec::with_capacity(payload.encoded_len());
        payload.encode(&mut buffer)?;
        writer.write_all(&buffer)?;
        Ok(())
    }

    fn data_points_generated(&self) -> Option<u64> {
        Some(self.last_span_count)
    }
}

impl From<v04::Trace<'_>> for wire::TraceChunk {
    #[expect(
        clippy::cast_possible_truncation,
        reason = "priority metrics originate from i32 weights"
    )]
    fn from(trace: v04::Trace<'_>) -> Self {
        let priority = trace
            .spans
            .first()
            .and_then(|span| span.metrics.get(v04::SAMPLING_PRIORITY_KEY))
            .map_or(PRIORITY_NONE, |priority| *priority as i32);
        Self {
            priority,
            spans: trace.spans.into_iter().map(wire::Span::from).collect(),
        }
    }
}

impl From<v04::Span<'_>> for wire::Span {
    fn from(span: v04::Span<'_>) -> Self {
        Self {
            service: span.service.to_owned(),
            name: span.name.to_owned(),
            resource: span.resource.to_owned(),
            trace_id: span.trace_id,
            span_id: span.span_id,
            parent_id: span.parent_id,
            start: span.start,
            duration: span.duration,
            error: span.error,
            meta: span
                .meta
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .collect(),
            metrics: span
                .metrics
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value))
                .collect(),
            r#type: span.kind.to_owned(),
            meta_struct: span
                .meta_struct
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value))
                .collect(),
        }
    }
}
