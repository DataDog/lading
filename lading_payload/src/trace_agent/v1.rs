//! Datadog Trace Agent v1.0 endpoint payload implementation.
//!
//! This module generates payloads compatible with the `/v1.0/traces` endpoint, also known as the
//! `idx`/ETP format. Implemented with reference to the reference encoder in
//! `datadog-agent` (`pkg/proto/pbgo/trace/idx`) and to the decoder in the Agent Data Plane
//! (`saluki-components/src/decoders/datadog`).
//!
//! Unlike v0.4, a v1.0 payload is a structured object: a tracer payload containing trace chunks,
//! each containing spans. The generator models traffic as a service graph: a set of services,
//! each declaring operations, each of which may call operations on other services. It walks the
//! graph from an entry-point operation to produce one span per call, nested inside its parent's
//! time window.
//!
//! # Generating valid spans
//!
//! The trace-agent normalizes v1.0 spans on receipt (`pkg/trace/agent/normalizer.go`), so a corpus
//! that trips normalization can add work to the pipeline under test. The generator guarantees:
//! non-empty services, names, and resources, non-zero span IDs, and timestamps constrained so
//! that neither the start time nor the start-plus-duration overflows a signed 64-bit integer.
//! Configurations that violate these constraints are rejected by [`Config::valid`] where they
//! can be detected statically. Other strings are emitted verbatim: the agent may still normalize
//! names, services, environments, and attributes according to its own rules.
//!
//! # Wire-format validation
//!
//! The encoder is validated against a raw `/v1.0/traces` body captured from a real
//! `dd-trace-go` tracer: the golden tests in this module decode both the captured fixture and
//! this encoder's output through a decoder transcribed from the reference implementations, and
//! require them to agree. Regenerate the fixture only alongside an intentional encoder change,
//! and re-verify it against a real tracer.
use std::io::Write;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rand::{Rng, RngExt, seq::IndexedRandom};

use crate::Error;

mod config;
mod encoder;
pub use config::{Config, ConfigAttributeValue, Operation, Service, SubOperation};

/// Nanoseconds between the Unix epoch and 2000-01-01T00:00:00Z.
///
/// The trace-agent treats any span starting before this as malformed and rewrites its start time
/// to the moment of receipt. Every generated start time sits at or after this instant.
const YEAR_2000_NANOS: u64 = 946_684_800_000_000_000;

/// Upper bound on a generated span's duration, in nanoseconds. Ten seconds.
const MAX_SPAN_DURATION_NANOS: u64 = 10_000_000_000;

/// Maximum call depth walked from an entry-point operation.
///
/// A service graph may be cyclic, so the walk needs a bound. Reaching it truncates the trace
/// rather than failing: a shorter trace is still a valid one.
const MAX_CALL_DEPTH: usize = 10;

/// Maximum number of spans in one generated trace chunk, as a second guard on a broad graph.
const MAX_SPANS_PER_CHUNK: usize = 100;

/// A value attached to a payload, chunk, span, or span link.
#[derive(Clone, Debug, PartialEq)]
enum AttributeValue {
    String(String),
    Bool(bool),
    Double(f64),
    Int(i64),
}

/// A single span.
///
/// Strings and attributes are reference-counted handles into the resolved operation graph, so a
/// span is assembled by cloning cheap handles rather than re-allocating its fields.
#[allow(clippy::struct_field_names)]
#[derive(Clone, Debug, Default, PartialEq)]
struct Span {
    service: Arc<str>,
    name: Arc<str>,
    resource: Arc<str>,
    span_id: u64,
    parent_id: u64,
    start: u64,
    duration: u64,
    error: bool,
    attributes: Arc<[(String, AttributeValue)]>,
    span_type: Arc<str>,
    links: Vec<SpanLink>,
    events: Vec<SpanEvent>,
    env: Arc<str>,
    version: Arc<str>,
    component: Arc<str>,
    kind: u32,
}

/// A link from a span to a span in another trace.
#[derive(Clone, Debug, Default, PartialEq)]
struct SpanLink {
    trace_id: [u8; 16],
    span_id: u64,
    attributes: Vec<(String, AttributeValue)>,
    tracestate: String,
    flags: u32,
}

/// A timestamped event recorded during a span.
#[derive(Clone, Debug, Default, PartialEq)]
struct SpanEvent {
    time: u64,
    name: String,
    attributes: Vec<(String, AttributeValue)>,
}

/// A group of spans belonging to one trace.
#[derive(Clone, Debug, Default, PartialEq)]
struct TraceChunk {
    priority: Option<i32>,
    origin: String,
    attributes: Vec<(String, AttributeValue)>,
    spans: Vec<Span>,
    dropped_trace: bool,
    trace_id: [u8; 16],
    sampling_mechanism: u32,
}

/// One v1.0 tracer payload: the unit a tracer POSTs to `/v1.0/traces`.
#[derive(Clone, Debug, Default, PartialEq)]
struct TracerPayload {
    container_id: String,
    language_name: String,
    language_version: String,
    tracer_version: String,
    runtime_id: String,
    env: String,
    hostname: String,
    app_version: String,
    attributes: Vec<(String, AttributeValue)>,
    chunks: Vec<TraceChunk>,
}

/// An operation with its service resolved and its span fields interned.
///
/// Everything a span needs except its identity and timing is computed once at construction, so
/// the per-span hot path clones reference-counted handles instead of re-deriving sorted
/// attributes from the configuration's `HashMap`.
#[derive(Debug)]
struct ResolvedOperation {
    service: Arc<str>,
    name: Arc<str>,
    resource: Arc<str>,
    span_type: Arc<str>,
    component: Arc<str>,
    kind: u32,
    /// Attributes sorted by key: a `HashMap` iterates in an unspecified order, and two runs of
    /// the same seed must produce byte-identical payloads.
    attributes: Arc<[(String, AttributeValue)]>,
    suboperations: Vec<SubOperation>,
}

/// Generates v1.0 trace payloads from a service graph.
#[derive(Debug)]
pub struct V1 {
    config: Config,
    /// Every operation in the graph, keyed by `service-name/operation-id`.
    operations: FxHashMap<String, ResolvedOperation>,
    /// Entry-point keys: the operations of the first configured service.
    entry_points: Vec<String>,
    /// Span `env` and `version` fields, shared by every generated span.
    env: Arc<str>,
    version: Arc<str>,
    /// Number of spans in the most recently generated payload, for metrics.
    last_span_count: u64,
}

impl V1 {
    /// Create a new v1.0 payload generator with the provided configuration.
    ///
    /// `_rng` is accepted for signature parity with the other payload generators, which seed
    /// internal state from it at construction; v1.0 generation keeps no such state.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid. See [`Config::valid`].
    pub fn with_config(config: Config, _rng: &mut impl Rng) -> Result<Self, Error> {
        config.valid()?;

        let mut operations = FxHashMap::default();
        for service in &config.services {
            for operation in &service.operations {
                let mut attributes: Vec<(String, AttributeValue)> = operation
                    .attributes
                    .iter()
                    .map(|(key, value)| (key.clone(), value.to_attribute_value()))
                    .collect();
                attributes.sort_by(|(left, _), (right, _)| left.cmp(right));

                operations.insert(
                    format!("{}/{}", service.name, operation.id),
                    ResolvedOperation {
                        service: service.name.as_str().into(),
                        name: operation.name.as_str().into(),
                        resource: operation.resource.as_str().into(),
                        span_type: operation.span_type.as_str().into(),
                        component: operation.component.as_str().into(),
                        kind: operation.kind,
                        attributes: attributes.into(),
                        suboperations: operation.suboperations.clone(),
                    },
                );
            }
        }

        let entry_points = config.services[0]
            .operations
            .iter()
            .map(|operation| format!("{}/{}", config.services[0].name, operation.id))
            .collect();
        let env = config.env.as_str().into();
        let version = config.app_version.as_str().into();

        Ok(Self {
            config,
            operations,
            entry_points,
            env,
            version,
            last_span_count: 0,
        })
    }

    /// Generates one encoded tracer payload with `chunk_count` trace chunks, paired with the
    /// number of spans it contains.
    fn generate_payload<R>(
        &mut self,
        rng: &mut R,
        chunk_count: usize,
    ) -> Result<(Vec<u8>, u64), Error>
    where
        R: Rng + ?Sized,
    {
        let mut chunks = Vec::with_capacity(chunk_count);
        let mut span_count = 0;
        for _ in 0..chunk_count {
            let chunk = self.generate_chunk(rng)?;
            span_count += chunk.spans.len() as u64;
            chunks.push(chunk);
        }

        let payload = TracerPayload {
            language_name: self.config.language_name.clone(),
            language_version: self.config.language_version.clone(),
            tracer_version: self.config.tracer_version.clone(),
            env: self.config.env.clone(),
            app_version: self.config.app_version.clone(),
            chunks,
            ..Default::default()
        };

        Ok((payload.encode()?, span_count))
    }

    fn generate_chunk<R>(&self, rng: &mut R) -> Result<TraceChunk, Error>
    where
        R: Rng + ?Sized,
    {
        let Some(entry_point) = self.entry_points.choose(rng) else {
            // Unreachable while `Config::valid` rejects a first service with no operations, but
            // reported rather than panicked on, per the crate's no-panic rule.
            return Err(Error::Validation(
                "configuration has no entry-point operations.".to_string(),
            ));
        };

        let (start, duration) = safe_start_and_duration(rng);
        let mut spans = Vec::new();
        self.append_spans(rng, entry_point, 0, start, duration, &mut spans, 0);

        Ok(TraceChunk {
            priority: Some(self.config.priority),
            spans,
            trace_id: random_trace_id(rng),
            ..Default::default()
        })
    }

    /// Appends the span for `operation_key` and, recursively, the spans of the operations it
    /// calls.
    ///
    /// Child spans are nested inside their parent's window, so a trace reads as a plausible call
    /// tree rather than a set of unrelated intervals.
    #[allow(clippy::too_many_arguments)]
    fn append_spans<R>(
        &self,
        rng: &mut R,
        operation_key: &str,
        parent_id: u64,
        start: u64,
        duration: u64,
        spans: &mut Vec<Span>,
        depth: usize,
    ) where
        R: Rng + ?Sized,
    {
        if depth >= MAX_CALL_DEPTH || spans.len() >= MAX_SPANS_PER_CHUNK {
            return;
        }

        let Some(resolved) = self.operations.get(operation_key) else {
            return;
        };

        let span_id = random_span_id(rng);
        let error = rng.random_bool(self.config.error_rate);
        let attributes = if error {
            // Real tracers mark a span as failed with the error bit plus error attributes rather
            // than a transport status code, which is what the receiving agent's dashboards and
            // error tracking read.
            let mut attributes = resolved.attributes.to_vec();
            for (key, value) in [
                ("error.type", "RuntimeError"),
                ("error.message", "synthetic error"),
            ] {
                if !attributes.iter().any(|(existing, _)| existing == key) {
                    attributes.push((key.to_string(), AttributeValue::String(value.to_string())));
                }
            }
            Arc::from(attributes)
        } else {
            Arc::clone(&resolved.attributes)
        };

        let links = if rng.random_bool(self.config.link_rate) {
            vec![SpanLink {
                trace_id: random_trace_id(rng),
                span_id: random_span_id(rng),
                attributes: vec![(
                    "link.kind".to_string(),
                    AttributeValue::String("follows-from".to_string()),
                )],
                ..Default::default()
            }]
        } else {
            Vec::new()
        };

        let events = if rng.random_bool(self.config.event_rate) {
            vec![SpanEvent {
                time: start,
                name: "exception".to_string(),
                attributes: vec![(
                    "exception.type".to_string(),
                    AttributeValue::String("RuntimeError".to_string()),
                )],
            }]
        } else {
            Vec::new()
        };

        spans.push(Span {
            service: Arc::clone(&resolved.service),
            name: Arc::clone(&resolved.name),
            resource: Arc::clone(&resolved.resource),
            span_id,
            parent_id,
            start,
            duration,
            error,
            attributes,
            span_type: Arc::clone(&resolved.span_type),
            links,
            events,
            env: Arc::clone(&self.env),
            version: Arc::clone(&self.version),
            component: Arc::clone(&resolved.component),
            kind: resolved.kind,
        });

        for suboperation in &resolved.suboperations {
            if !rng.random_bool(suboperation.rate) {
                continue;
            }

            let (child_start, child_duration) = nested_start_and_duration(rng, start, duration);
            self.append_spans(
                rng,
                &suboperation.to,
                span_id,
                child_start,
                child_duration,
                spans,
                depth + 1,
            );
        }
    }
}

impl crate::Serialize for V1 {
    fn to_bytes<W, R>(&mut self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        W: Write,
        R: Rng + Sized,
    {
        self.last_span_count = 0;
        if max_bytes == 0 {
            return Ok(());
        }

        // One block is one payload with exactly the configured batch size. A small block
        // budget rejects the whole payload rather than changing the offered trace workload.
        let (bytes, span_count) =
            self.generate_payload(&mut rng, self.config.chunks_per_payload)?;
        if bytes.len() > max_bytes {
            return Ok(());
        }
        writer.write_all(&bytes)?;
        self.last_span_count = span_count;
        Ok(())
    }

    fn data_points_generated(&self) -> Option<u64> {
        Some(self.last_span_count)
    }
}

/// Draws a start time and duration the trace-agent's normalizer leaves untouched.
///
/// Start at or after [`YEAR_2000_NANOS`] to avoid the agent's start-time repair.
/// Conservatively bound start and end by `i64::MAX` for downstream paths using signed
/// timestamps, even though the v1.0 wire fields are unsigned.
fn safe_start_and_duration<R>(rng: &mut R) -> (u64, u64)
where
    R: Rng + ?Sized,
{
    let start = rng.random_range(YEAR_2000_NANOS..=i64::MAX as u64);
    let max_duration = (i64::MAX as u64 - start).min(MAX_SPAN_DURATION_NANOS);
    let duration = rng.random_range(0..=max_duration);
    (start, duration)
}

/// Draws a start time and duration for a child span nested inside its parent's window.
///
/// A zero-length parent yields a zero-length child at the same instant, which keeps the nesting
/// invariant intact without a special case at the call site.
fn nested_start_and_duration<R>(rng: &mut R, parent_start: u64, parent_duration: u64) -> (u64, u64)
where
    R: Rng + ?Sized,
{
    if parent_duration == 0 {
        return (parent_start, 0);
    }

    let offset = rng.random_range(0..parent_duration);
    let duration = rng.random_range(0..=(parent_duration - offset));
    (parent_start + offset, duration)
}

/// Draws a 128-bit trace ID, never all zeroes.
fn random_trace_id<R>(rng: &mut R) -> [u8; 16]
where
    R: Rng + ?Sized,
{
    let mut id = [0u8; 16];
    rng.fill(&mut id);
    if id == [0u8; 16] {
        id[15] = 1;
    }
    id
}

/// Draws a 64-bit span ID, never zero.
///
/// The trace-agent drops a trace containing a zero span ID outright, so a zero would silently
/// shrink the received output relative to what was sent.
fn random_span_id<R>(rng: &mut R) -> u64
where
    R: Rng + ?Sized,
{
    rng.random_range(1..=u64::MAX)
}

#[cfg(test)]
mod golden;
#[cfg(test)]
mod test;
