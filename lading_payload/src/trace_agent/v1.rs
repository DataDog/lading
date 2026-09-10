//! Datadog Trace Agent v1.0 endpoint payload implementation.
//!
//! This module generates payloads compatible with the `/v1.0/traces` endpoint, also known as the
//! `idx`/ETP format. Implemented with reference to the reference encoder in
//! `datadog-agent` (`pkg/proto/pbgo/trace/idx`) and to the decoder in the Agent Data Plane
//! (`saluki-components/src/decoders/datadog`).
//!
//! Unlike v0.4, a v1.0 payload is a structured object: a tracer payload containing trace chunks,
//! each containing spans. The generator models traffic as a service graph — a set of services,
//! each declaring operations, each of which may call operations on other services — and walks the
//! graph from an entry-point operation to produce one span per call, nested inside its parent's
//! time window.
//!
//! # Generating spans the trace-agent will not rewrite
//!
//! The trace-agent normalizes v1.0 spans on receipt (`pkg/trace/agent/normalizer.go`), so a corpus
//! that trips normalization would diverge from what the sender produced for reasons unrelated to
//! the pipeline under test. The generator therefore emits values that are already normalized:
//! non-empty services, names, and resources, non-zero span IDs distinct from their parents, and
//! timestamps constrained so that neither the start time nor the start-plus-duration overflows a
//! signed 64-bit integer. Configurations that violate this are rejected by [`Config::validate`]
//! where they can be detected statically.
use std::io::Write;

use rustc_hash::FxHashMap;

use rand::{Rng, RngExt, seq::IndexedRandom};
use serde::{Deserialize, Serialize};

use crate::Error;

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

/// A statically declared attribute value.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(untagged)]
pub enum ConfigAttributeValue {
    /// A boolean.
    Bool(bool),
    /// A signed integer.
    Int(i64),
    /// A double-precision float.
    Double(f64),
    /// A string.
    String(String),
}

impl ConfigAttributeValue {
    fn to_attribute_value(&self) -> AttributeValue {
        match self {
            Self::Bool(v) => AttributeValue::Bool(*v),
            Self::Int(v) => AttributeValue::Int(*v),
            Self::Double(v) => AttributeValue::Double(*v),
            Self::String(v) => AttributeValue::String(v.clone()),
        }
    }
}

/// A call from one operation to an operation on another service.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct SubOperation {
    /// The called operation, as `service-name/operation-id`.
    pub to: String,
    /// Probability in `0.0..=1.0` that a generated trace follows this call. Defaults to `1.0`.
    #[serde(default = "default_rate")]
    pub rate: f64,
}

fn default_rate() -> f64 {
    1.0
}

/// An operation a service can perform, producing one span.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Operation {
    /// Identifier used to reference this operation from a [`SubOperation`]. Unique within a
    /// service.
    pub id: String,
    /// Span operation name, for example `http.request`.
    pub name: String,
    /// Span resource, for example `GET /api/v1/products/{id}`.
    pub resource: String,
    /// Span type, for example `web`, `sql`, or `redis`. Empty by default.
    #[serde(default)]
    pub span_type: String,
    /// Span kind, following the OpenTelemetry enumeration. Zero, meaning unspecified, by default.
    #[serde(default)]
    pub kind: u32,
    /// Instrumentation component that produced the span. Empty by default.
    #[serde(default)]
    pub component: String,
    /// Attributes attached to every span this operation produces.
    #[serde(default)]
    pub attributes: FxHashMap<String, ConfigAttributeValue>,
    /// Operations this one calls, each producing a child span.
    #[serde(default)]
    pub suboperations: Vec<SubOperation>,
}

/// A service in the generated graph.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Service {
    /// Service name, used verbatim as the span's service.
    pub name: String,
    /// Operations the service can perform.
    pub operations: Vec<Operation>,
}

/// Configuration for v1.0 trace payload generation.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Probability in `0.0..=1.0` that a generated span is marked as an error. Defaults to `0.0`,
    /// meaning no span is ever marked as an error.
    pub error_rate: f64,

    /// Probability in `0.0..=1.0` that a generated span carries one link to another trace.
    /// Defaults to `0.0`, meaning no span carries a link.
    pub link_rate: f64,

    /// Probability in `0.0..=1.0` that a generated span carries one timestamped event.
    /// Defaults to `0.0`, meaning no span carries an event.
    pub event_rate: f64,

    /// Number of trace chunks packed into each payload.
    ///
    /// This is a floor, not a cap: when filling a block, chunks are added until the encoded
    /// payload approaches the block size, starting from this many.
    ///
    /// Defaults to `1`.
    pub chunks_per_payload: usize,

    /// Tracer language name reported in the payload, for example `go`. Empty by default.
    pub language_name: String,

    /// Tracer language runtime version reported in the payload. Empty by default.
    pub language_version: String,

    /// Tracer library version reported in the payload. Empty by default.
    pub tracer_version: String,

    /// Environment reported in the payload, for example `production`. Empty by default.
    pub env: String,

    /// Version of the traced application reported in the payload. Empty by default.
    pub app_version: String,

    /// Sampling priority set on every chunk.
    ///
    /// Defaults to `1`, the tracer's automatic keep decision, which leaves the sampling decision
    /// to the receiving agent.
    pub priority: i32,

    /// Services in the graph. The operations of the **first** service are the entry points: every
    /// generated trace starts at one of them.
    pub services: Vec<Service>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            error_rate: 0.0,
            link_rate: 0.0,
            event_rate: 0.0,
            chunks_per_payload: 1,
            language_name: String::new(),
            language_version: String::new(),
            tracer_version: String::new(),
            env: String::new(),
            app_version: String::new(),
            priority: 1,
            services: Vec::new(),
        }
    }
}

impl Config {
    /// Validates the configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if a rate falls outside `0.0..=1.0`, if there are no services or no
    /// operations to start from, if any span field the trace-agent's normalizer would rewrite is
    /// left empty, or if a suboperation references an operation that does not exist.
    pub fn valid(&self) -> Result<(), Error> {
        for (name, rate) in [
            ("error_rate", self.error_rate),
            ("link_rate", self.link_rate),
            ("event_rate", self.event_rate),
        ] {
            if !(0.0..=1.0).contains(&rate) {
                return Err(Error::Validation(format!(
                    "{name} must be between 0.0 and 1.0, got {rate}."
                )));
            }
        }

        if self.chunks_per_payload == 0 {
            return Err(Error::Validation(
                "chunks_per_payload must be at least 1.".to_string(),
            ));
        }

        if self.services.is_empty() {
            return Err(Error::Validation(
                "At least one service must be configured.".to_string(),
            ));
        }

        let mut operation_keys = Vec::new();
        for service in &self.services {
            if service.name.is_empty() {
                return Err(Error::Validation(
                    "Every service must have a non-empty name.".to_string(),
                ));
            }

            for operation in &service.operations {
                // The trace-agent's normalizer substitutes the span name for an empty resource and
                // rejects an empty name outright, so both must be set here or the sent payload and
                // the received one disagree on every span the operation produces.
                if operation.name.is_empty() || operation.resource.is_empty() {
                    return Err(Error::Validation(format!(
                        "Operation '{}/{}' must have a non-empty name and resource.",
                        service.name, operation.id
                    )));
                }

                operation_keys.push(format!("{}/{}", service.name, operation.id));
            }
        }

        if self.services[0].operations.is_empty() {
            return Err(Error::Validation(format!(
                "The first service ('{}') declares the entry-point operations, so it must have at least one.",
                self.services[0].name
            )));
        }

        for service in &self.services {
            for operation in &service.operations {
                for suboperation in &operation.suboperations {
                    if !(0.0..=1.0).contains(&suboperation.rate) {
                        return Err(Error::Validation(format!(
                            "Suboperation rate for '{}' must be between 0.0 and 1.0, got {}.",
                            suboperation.to, suboperation.rate
                        )));
                    }

                    if !operation_keys.contains(&suboperation.to) {
                        return Err(Error::Validation(format!(
                            "Operation '{}/{}' references unknown suboperation '{}'.",
                            service.name, operation.id, suboperation.to
                        )));
                    }
                }
            }
        }

        Ok(())
    }
}

/// A value attached to a payload, chunk, span, or span link.
#[derive(Clone, Debug, PartialEq)]
enum AttributeValue {
    String(String),
    Bool(bool),
    Double(f64),
    Int(i64),
}

/// A single span.
#[allow(clippy::struct_field_names)]
#[derive(Clone, Debug, Default)]
struct Span {
    service: String,
    name: String,
    resource: String,
    span_id: u64,
    parent_id: u64,
    start: u64,
    duration: u64,
    error: bool,
    attributes: Vec<(String, AttributeValue)>,
    span_type: String,
    links: Vec<SpanLink>,
    events: Vec<SpanEvent>,
    env: String,
    version: String,
    component: String,
    kind: u32,
}

/// A link from a span to a span in another trace.
#[derive(Clone, Debug, Default)]
struct SpanLink {
    trace_id: [u8; 16],
    span_id: u64,
    attributes: Vec<(String, AttributeValue)>,
    tracestate: String,
    flags: u32,
}

/// A timestamped event recorded during a span.
#[derive(Clone, Debug, Default)]
struct SpanEvent {
    time: u64,
    name: String,
    attributes: Vec<(String, AttributeValue)>,
}

/// A group of spans belonging to one trace.
#[derive(Clone, Debug, Default)]
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
#[derive(Clone, Debug, Default)]
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

/// Generates v1.0 trace payloads from a service graph.
#[derive(Debug)]
pub struct V1 {
    config: Config,
    /// Every operation in the graph, keyed by `service-name/operation-id`, paired with its service
    /// name.
    operations: FxHashMap<String, (String, Operation)>,
    /// Entry-point keys: the operations of the first configured service.
    entry_points: Vec<String>,
    /// Number of spans in the most recently generated payload, for metrics.
    last_span_count: u64,
}

impl V1 {
    /// Create a new v1.0 payload generator with the provided configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid. See [`Config::valid`].
    pub fn with_config(config: Config, _rng: &mut impl Rng) -> Result<Self, Error> {
        config.valid()?;

        let mut operations = FxHashMap::default();
        for service in &config.services {
            for operation in &service.operations {
                let key = format!("{}/{}", service.name, operation.id);
                if operations
                    .insert(key.clone(), (service.name.clone(), operation.clone()))
                    .is_some()
                {
                    return Err(Error::Validation(format!("Duplicate operation '{key}'.")));
                }
            }
        }

        let entry_points = config.services[0]
            .operations
            .iter()
            .map(|operation| format!("{}/{}", config.services[0].name, operation.id))
            .collect();

        Ok(Self {
            config,
            operations,
            entry_points,
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
        let mut span_count = 0;
        let chunks = (0..chunk_count)
            .map(|_| {
                let chunk = self.generate_chunk(rng);
                span_count += chunk.spans.len() as u64;
                chunk
            })
            .collect();

        let payload = TracerPayload {
            language_name: self.config.language_name.clone(),
            language_version: self.config.language_version.clone(),
            tracer_version: self.config.tracer_version.clone(),
            env: self.config.env.clone(),
            app_version: self.config.app_version.clone(),
            chunks,
            ..Default::default()
        };

        let encoded = payload.encode()?;
        Ok((encoded, span_count))
    }

    fn generate_chunk<R>(&self, rng: &mut R) -> TraceChunk
    where
        R: Rng + ?Sized,
    {
        let entry_point = self
            .entry_points
            .choose(rng)
            .expect("entry points are non-empty, as `Config::valid` requires");

        let (start, duration) = safe_start_and_duration(rng);
        let mut spans = Vec::new();
        self.append_spans(rng, entry_point, 0, start, duration, &mut spans, 0);

        TraceChunk {
            priority: Some(self.config.priority),
            spans,
            trace_id: random_trace_id(rng),
            ..Default::default()
        }
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

        let Some((service_name, operation)) = self.operations.get(operation_key) else {
            return;
        };

        let span_id = random_span_id(rng);
        let mut attributes = operation
            .attributes
            .iter()
            .map(|(key, value)| (key.clone(), value.to_attribute_value()))
            .collect::<Vec<_>>();
        // A `HashMap` iterates in an unspecified order, so sort to keep generation reproducible
        // across runs: two runs of the same seed must produce byte-identical payloads.
        attributes.sort_by(|(left, _), (right, _)| left.cmp(right));

        let error = rng.random_bool(self.config.error_rate);
        if error {
            attributes.push((
                "http.status_code".to_string(),
                AttributeValue::String("500".to_string()),
            ));
        }

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
            service: service_name.clone(),
            name: operation.name.clone(),
            resource: operation.resource.clone(),
            span_id,
            parent_id,
            start,
            duration,
            error,
            attributes,
            span_type: operation.span_type.clone(),
            links,
            events,
            env: self.config.env.clone(),
            version: self.config.app_version.clone(),
            component: operation.component.clone(),
            kind: operation.kind,
        });

        // Cloned so the recursive call can borrow `self.operations` again.
        let suboperations = operation.suboperations.clone();
        for suboperation in &suboperations {
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
        if max_bytes == 0 {
            return Ok(());
        }

        // A v1.0 payload is a single structured object, so a block is one tracer payload whose
        // chunk count is scaled to approach `max_bytes`. The count starts at the configured floor
        // and grows geometrically: chunk generation dominates the cost, so re-probing every
        // candidate additively would be quadratic in the block size. A binary search over the
        // last doubling window then finds the largest payload that fits. The largest fitting
        // encoding is written directly — re-encoding a found count would generate a *different*
        // payload, since the source of randomness has advanced.
        let floor = self.config.chunks_per_payload.max(1);

        // A payload must carry at least the configured number of chunks. If even that exceeds
        // `max_bytes`, nothing is written: the caller treats an empty block as a rejection and
        // scales up the minimum block size it asks for.
        let (mut best, mut best_spans) = self.generate_payload(&mut rng, floor)?;
        if best.len() > max_bytes {
            return Ok(());
        }

        let mut low = floor;
        let mut high = floor * 2;
        loop {
            let (encoded, spans) = self.generate_payload(&mut rng, high)?;
            if encoded.len() > max_bytes {
                break;
            }
            low = high;
            best = encoded;
            best_spans = spans;
            high *= 2;
        }

        while low + 1 < high {
            let mid = usize::midpoint(low, high);
            let (encoded, spans) = self.generate_payload(&mut rng, mid)?;
            if encoded.len() <= max_bytes {
                low = mid;
                best = encoded;
                best_spans = spans;
            } else {
                high = mid;
            }
        }

        writer.write_all(&best)?;
        // Report the span count of the payload actually written, not whichever probe happened to
        // run last: the final binary-search probe is thrown away roughly half the time.
        self.last_span_count = best_spans;

        Ok(())
    }

    fn data_points_generated(&self) -> Option<u64> {
        Some(self.last_span_count)
    }
}

/// Draws a start time and duration the trace-agent's normalizer leaves untouched.
///
/// Three constraints, all of them the normalizer's: the start must be at or after
/// [`YEAR_2000_NANOS`], the start must not exceed `i64::MAX` because the reference implementation
/// holds timestamps as signed, and `start + duration` must not overflow `i64::MAX`. Violating any
/// of them makes the trace-agent rewrite the span while the receiving agent leaves it alone.
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

/// Encoder-side streaming string table.
///
/// Assigns each distinct non-empty string the index it will occupy on the decoding side, which is
/// determined solely by the order strings are first written. Index 0 is the pre-seeded empty
/// string and is never assigned here.
struct StringTable {
    indices: FxHashMap<String, u32>,
    next_index: u32,
}

impl StringTable {
    fn new() -> Self {
        Self {
            indices: FxHashMap::default(),
            next_index: 1,
        }
    }
}

/// Writes a streaming string: inline on its first appearance, by index thereafter.
///
/// The empty string is always index 0. Writing it inline would consume a table slot on the reading
/// side and shift every later index, so it is never written as a literal.
fn write_streaming_string<W: Write>(
    w: &mut W,
    strings: &mut StringTable,
    s: &str,
) -> Result<(), Error> {
    if s.is_empty() {
        return write_u32(w, 0);
    }

    if let Some(index) = strings.indices.get(s) {
        return write_u32(w, *index);
    }

    rmp::encode::write_str(w, s).map_err(map_write_error)?;
    strings.indices.insert(s.to_string(), strings.next_index);
    strings.next_index += 1;
    Ok(())
}

/// Maps a length that exceeds the format's `u32` bound into the crate error type.
fn cast_error(error: std::num::TryFromIntError) -> Error {
    Error::Validation(error.to_string())
}

/// Maps an `rmp` encoding error, which wraps an `io::Error` when writing to a std writer, into the
/// crate error type.
fn map_write_error(error: rmp::encode::ValueWriteError<std::io::Error>) -> Error {
    match error {
        rmp::encode::ValueWriteError::InvalidMarkerWrite(e)
        | rmp::encode::ValueWriteError::InvalidDataWrite(e) => Error::Io(e),
    }
}

fn write_u32<W: Write>(w: &mut W, value: u32) -> Result<(), Error> {
    rmp::encode::write_uint(w, u64::from(value))
        .map(|_| ())
        .map_err(map_write_error)
}

fn write_u64<W: Write>(w: &mut W, value: u64) -> Result<(), Error> {
    rmp::encode::write_uint(w, value)
        .map(|_| ())
        .map_err(map_write_error)
}

fn write_field<W: Write>(w: &mut W, field: u32) -> Result<(), Error> {
    write_u32(w, field)
}

fn write_map_len<W: Write>(w: &mut W, len: usize) -> Result<(), Error> {
    rmp::encode::write_map_len(w, u32::try_from(len).map_err(cast_error)?)
        .map(|_| ())
        .map_err(map_write_error)
}

fn write_array_len<W: Write>(w: &mut W, len: usize) -> Result<(), Error> {
    rmp::encode::write_array_len(w, u32::try_from(len).map_err(cast_error)?)
        .map(|_| ())
        .map_err(map_write_error)
}

fn write_bool<W: Write>(w: &mut W, value: bool) -> Result<(), Error> {
    rmp::encode::write_bool(w, value).map_err(Error::Io)
}

fn write_bin<W: Write>(w: &mut W, value: &[u8]) -> Result<(), Error> {
    rmp::encode::write_bin(w, value).map_err(map_write_error)
}

/// Writes an attribute map as a flat `[key, type, value]` array, three slots per entry.
fn write_attributes<W: Write>(
    w: &mut W,
    strings: &mut StringTable,
    attributes: &[(String, AttributeValue)],
) -> Result<(), Error> {
    write_array_len(w, attributes.len() * 3)?;
    for (key, value) in attributes {
        write_streaming_string(w, strings, key)?;
        write_any_value(w, strings, value)?;
    }
    Ok(())
}

/// Writes an `AnyValue`: a `uint32` type discriminant followed by the value itself.
fn write_any_value<W: Write>(
    w: &mut W,
    strings: &mut StringTable,
    value: &AttributeValue,
) -> Result<(), Error> {
    match value {
        AttributeValue::String(s) => {
            write_u32(w, 1)?;
            write_streaming_string(w, strings, s)
        }
        AttributeValue::Bool(b) => {
            write_u32(w, 2)?;
            write_bool(w, *b)
        }
        AttributeValue::Double(d) => {
            write_u32(w, 3)?;
            rmp::encode::write_f64(w, *d).map_err(map_write_error)
        }
        AttributeValue::Int(i) => {
            write_u32(w, 4)?;
            rmp::encode::write_sint(w, *i)
                .map(|_| ())
                .map_err(map_write_error)
        }
    }
}

fn write_tracer_payload<W: Write>(
    w: &mut W,
    strings: &mut StringTable,
    payload: &TracerPayload,
) -> Result<(), Error> {
    // The reference encoder omits every field holding its zero value, and counts the survivors to
    // size the map header. Field 1, the explicit string table, is never emitted: strings stream
    // inline instead.
    let string_fields = [
        (2, &payload.container_id),
        (3, &payload.language_name),
        (4, &payload.language_version),
        (5, &payload.tracer_version),
        (6, &payload.runtime_id),
        (7, &payload.env),
        (8, &payload.hostname),
        (9, &payload.app_version),
    ];

    let num_fields = string_fields.iter().filter(|(_, v)| !v.is_empty()).count()
        + usize::from(!payload.attributes.is_empty())
        + usize::from(!payload.chunks.is_empty());
    write_map_len(w, num_fields)?;

    for (field, value) in string_fields {
        if !value.is_empty() {
            write_field(w, field)?;
            write_streaming_string(w, strings, value)?;
        }
    }

    if !payload.attributes.is_empty() {
        write_field(w, 10)?;
        write_attributes(w, strings, &payload.attributes)?;
    }

    if !payload.chunks.is_empty() {
        write_field(w, 11)?;
        write_array_len(w, payload.chunks.len())?;
        for chunk in &payload.chunks {
            write_chunk(w, strings, chunk)?;
        }
    }

    Ok(())
}

fn write_chunk<W: Write>(
    w: &mut W,
    strings: &mut StringTable,
    chunk: &TraceChunk,
) -> Result<(), Error> {
    let num_fields = usize::from(chunk.priority.is_some())
        + usize::from(!chunk.origin.is_empty())
        + usize::from(!chunk.attributes.is_empty())
        + usize::from(!chunk.spans.is_empty())
        + usize::from(chunk.dropped_trace)
        + usize::from(chunk.trace_id != [0u8; 16])
        + usize::from(chunk.sampling_mechanism != 0);
    write_map_len(w, num_fields)?;

    if let Some(priority) = chunk.priority {
        write_field(w, 1)?;
        rmp::encode::write_sint(w, i64::from(priority))
            .map(|_| ())
            .map_err(map_write_error)?;
    }

    if !chunk.origin.is_empty() {
        write_field(w, 2)?;
        write_streaming_string(w, strings, &chunk.origin)?;
    }

    if !chunk.attributes.is_empty() {
        write_field(w, 3)?;
        write_attributes(w, strings, &chunk.attributes)?;
    }

    if !chunk.spans.is_empty() {
        write_field(w, 4)?;
        write_array_len(w, chunk.spans.len())?;
        for span in &chunk.spans {
            write_span(w, strings, span)?;
        }
    }

    if chunk.dropped_trace {
        write_field(w, 5)?;
        write_bool(w, true)?;
    }

    if chunk.trace_id != [0u8; 16] {
        write_field(w, 6)?;
        write_bin(w, &chunk.trace_id)?;
    }

    if chunk.sampling_mechanism != 0 {
        write_field(w, 7)?;
        write_u32(w, chunk.sampling_mechanism)?;
    }

    Ok(())
}

fn write_span<W: Write>(w: &mut W, strings: &mut StringTable, span: &Span) -> Result<(), Error> {
    let num_fields = usize::from(!span.service.is_empty())
        + usize::from(!span.name.is_empty())
        + usize::from(!span.resource.is_empty())
        + usize::from(span.span_id != 0)
        + usize::from(span.parent_id != 0)
        + usize::from(span.start != 0)
        + usize::from(span.duration != 0)
        + usize::from(span.error)
        + usize::from(!span.attributes.is_empty())
        + usize::from(!span.span_type.is_empty())
        + usize::from(!span.links.is_empty())
        + usize::from(!span.events.is_empty())
        + usize::from(!span.env.is_empty())
        + usize::from(!span.version.is_empty())
        + usize::from(!span.component.is_empty())
        + usize::from(span.kind != 0);
    write_map_len(w, num_fields)?;

    if !span.service.is_empty() {
        write_field(w, 1)?;
        write_streaming_string(w, strings, &span.service)?;
    }
    if !span.name.is_empty() {
        write_field(w, 2)?;
        write_streaming_string(w, strings, &span.name)?;
    }
    if !span.resource.is_empty() {
        write_field(w, 3)?;
        write_streaming_string(w, strings, &span.resource)?;
    }
    if span.span_id != 0 {
        write_field(w, 4)?;
        write_u64(w, span.span_id)?;
    }
    if span.parent_id != 0 {
        write_field(w, 5)?;
        write_u64(w, span.parent_id)?;
    }
    if span.start != 0 {
        write_field(w, 6)?;
        write_u64(w, span.start)?;
    }
    if span.duration != 0 {
        write_field(w, 7)?;
        write_u64(w, span.duration)?;
    }
    if span.error {
        write_field(w, 8)?;
        write_bool(w, true)?;
    }
    if !span.attributes.is_empty() {
        write_field(w, 9)?;
        write_attributes(w, strings, &span.attributes)?;
    }
    if !span.span_type.is_empty() {
        write_field(w, 10)?;
        write_streaming_string(w, strings, &span.span_type)?;
    }
    if !span.links.is_empty() {
        write_field(w, 11)?;
        write_array_len(w, span.links.len())?;
        for link in &span.links {
            write_span_link(w, strings, link)?;
        }
    }
    if !span.events.is_empty() {
        write_field(w, 12)?;
        write_array_len(w, span.events.len())?;
        for event in &span.events {
            write_span_event(w, strings, event)?;
        }
    }
    if !span.env.is_empty() {
        write_field(w, 13)?;
        write_streaming_string(w, strings, &span.env)?;
    }
    if !span.version.is_empty() {
        write_field(w, 14)?;
        write_streaming_string(w, strings, &span.version)?;
    }
    if !span.component.is_empty() {
        write_field(w, 15)?;
        write_streaming_string(w, strings, &span.component)?;
    }
    if span.kind != 0 {
        write_field(w, 16)?;
        write_u32(w, span.kind)?;
    }

    Ok(())
}

fn write_span_link<W: Write>(
    w: &mut W,
    strings: &mut StringTable,
    link: &SpanLink,
) -> Result<(), Error> {
    let num_fields = usize::from(link.trace_id != [0u8; 16])
        + usize::from(link.span_id != 0)
        + usize::from(!link.attributes.is_empty())
        + usize::from(!link.tracestate.is_empty())
        + usize::from(link.flags != 0);
    write_map_len(w, num_fields)?;

    if link.trace_id != [0u8; 16] {
        write_field(w, 1)?;
        write_bin(w, &link.trace_id)?;
    }
    if link.span_id != 0 {
        write_field(w, 2)?;
        write_u64(w, link.span_id)?;
    }
    if !link.attributes.is_empty() {
        write_field(w, 3)?;
        write_attributes(w, strings, &link.attributes)?;
    }
    if !link.tracestate.is_empty() {
        write_field(w, 4)?;
        write_streaming_string(w, strings, &link.tracestate)?;
    }
    if link.flags != 0 {
        write_field(w, 5)?;
        write_u32(w, link.flags)?;
    }

    Ok(())
}

fn write_span_event<W: Write>(
    w: &mut W,
    strings: &mut StringTable,
    event: &SpanEvent,
) -> Result<(), Error> {
    let num_fields = usize::from(event.time != 0)
        + usize::from(!event.name.is_empty())
        + usize::from(!event.attributes.is_empty());
    write_map_len(w, num_fields)?;

    if event.time != 0 {
        write_field(w, 1)?;
        write_u64(w, event.time)?;
    }
    if !event.name.is_empty() {
        write_field(w, 2)?;
        write_streaming_string(w, strings, &event.name)?;
    }
    if !event.attributes.is_empty() {
        write_field(w, 3)?;
        write_attributes(w, strings, &event.attributes)?;
    }

    Ok(())
}

impl TracerPayload {
    /// Encodes the payload into its `v1.0` `MessagePack` representation.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying writer fails. Writing into an in-memory buffer cannot
    /// fail, so callers that do so can treat this as infallible.
    fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut buf = Vec::new();
        let mut strings = StringTable::new();
        write_tracer_payload(&mut buf, &mut strings, self)?;
        Ok(buf)
    }
}

#[cfg(test)]
mod test {
    use rand::{SeedableRng, rngs::SmallRng};
    use rustc_hash::FxHashMap;

    use super::{
        AttributeValue, Config, Operation, Service, Span, SubOperation, TraceChunk, TracerPayload,
        V1,
    };
    use crate::Serialize as _;

    fn service_graph() -> Config {
        Config {
            error_rate: 0.0,
            link_rate: 0.0,
            event_rate: 0.0,
            chunks_per_payload: 1,
            language_name: "go".to_string(),
            language_version: "1.24.2".to_string(),
            tracer_version: "2.5.0".to_string(),
            env: "production".to_string(),
            app_version: "1.0.0".to_string(),
            priority: 1,
            services: vec![
                Service {
                    name: "api-gateway".to_string(),
                    operations: vec![Operation {
                        id: "get-product".to_string(),
                        name: "http.request".to_string(),
                        resource: "GET /api/v1/products/{id}".to_string(),
                        span_type: "web".to_string(),
                        kind: 2,
                        component: "net/http".to_string(),
                        attributes: FxHashMap::from_iter([(
                            "http.method".to_string(),
                            super::ConfigAttributeValue::String("GET".to_string()),
                        )]),
                        suboperations: vec![SubOperation {
                            to: "product-service/get-product".to_string(),
                            rate: 1.0,
                        }],
                    }],
                },
                Service {
                    name: "product-service".to_string(),
                    operations: vec![Operation {
                        id: "get-product".to_string(),
                        name: "grpc.server.request".to_string(),
                        resource: "/ProductService/GetProduct".to_string(),
                        span_type: "web".to_string(),
                        kind: 2,
                        component: "grpc".to_string(),
                        attributes: FxHashMap::default(),
                        suboperations: vec![
                            SubOperation {
                                to: "product-cache/get-product-by-id".to_string(),
                                rate: 1.0,
                            },
                            SubOperation {
                                to: "product-db/select-product-by-id".to_string(),
                                rate: 0.1,
                            },
                        ],
                    }],
                },
                Service {
                    name: "product-cache".to_string(),
                    operations: vec![Operation {
                        id: "get-product-by-id".to_string(),
                        name: "redis.command".to_string(),
                        resource: "GET products:by_id:$1".to_string(),
                        span_type: "redis".to_string(),
                        kind: 3,
                        component: "redis".to_string(),
                        attributes: FxHashMap::default(),
                        suboperations: Vec::new(),
                    }],
                },
                Service {
                    name: "product-db".to_string(),
                    operations: vec![Operation {
                        id: "select-product-by-id".to_string(),
                        name: "postgres.query".to_string(),
                        resource: "SELECT * FROM products WHERE id = $1".to_string(),
                        span_type: "sql".to_string(),
                        kind: 3,
                        component: "postgresql".to_string(),
                        attributes: FxHashMap::default(),
                        suboperations: Vec::new(),
                    }],
                },
            ],
        }
    }

    #[test]
    fn the_same_seed_produces_byte_identical_payloads() {
        let config = service_graph();
        let mut first = V1::with_config(config.clone(), &mut SmallRng::seed_from_u64(0))
            .expect("config should be valid");
        let mut second = V1::with_config(config, &mut SmallRng::seed_from_u64(0))
            .expect("config should be valid");

        let mut rng_one = SmallRng::seed_from_u64(42);
        let mut rng_two = SmallRng::seed_from_u64(42);

        let (payload_one, _) = first
            .generate_payload(&mut rng_one, 1)
            .expect("should not fail to generate");
        let (payload_two, _) = second
            .generate_payload(&mut rng_two, 1)
            .expect("should not fail to generate");

        assert_eq!(payload_one, payload_two);
    }

    #[test]
    fn child_spans_are_nested_inside_their_parents_window() {
        let config = service_graph();
        let mut generator = V1::with_config(config, &mut SmallRng::seed_from_u64(0))
            .expect("config should be valid");

        let mut rng = SmallRng::seed_from_u64(1234);
        for _ in 0..100 {
            let (payload, _) = generator
                .generate_payload(&mut rng, 1)
                .expect("should not fail to generate");
            assert!(!payload.is_empty());
        }
    }

    #[test]
    fn validation_rejects_a_rate_outside_the_unit_interval() {
        let mut config = service_graph();
        config.error_rate = 1.5;
        assert!(config.valid().is_err());

        config.error_rate = -0.1;
        assert!(config.valid().is_err());
    }

    #[test]
    fn validation_rejects_an_unknown_suboperation() {
        let mut config = service_graph();
        config.services[0].operations[0].suboperations[0].to = "nowhere/nothing".to_string();
        assert!(config.valid().is_err());
    }

    #[test]
    fn validation_rejects_an_operation_the_normalizer_would_rewrite() {
        let mut config = service_graph();
        config.services[0].operations[0].resource = String::new();
        assert!(config.valid().is_err());
    }

    #[test]
    fn a_suboperation_rate_of_zero_never_produces_a_child_span() {
        let mut config = service_graph();
        for service in &mut config.services {
            for operation in &mut service.operations {
                for suboperation in &mut operation.suboperations {
                    suboperation.rate = 0.0;
                }
            }
        }

        let mut generator = V1::with_config(config, &mut SmallRng::seed_from_u64(0))
            .expect("config should be valid");
        let mut rng = SmallRng::seed_from_u64(7);

        // With every suboperation rate at zero, each payload's single chunk holds exactly one
        // span: the entry point.
        let (payload, span_count) = generator
            .generate_payload(&mut rng, 1)
            .expect("should not fail to generate");
        assert_eq!(span_count, 1);
        assert!(!payload.is_empty());

        // `to_bytes` reports the span count of the payload it actually wrote. With one span per
        // chunk, the count equals the number of chunks in the written payload, which scales with
        // `max_bytes` — so assert the floor was honored rather than an exact count.
        let mut bytes = Vec::new();
        generator
            .to_bytes(&mut rng, 65_536, &mut bytes)
            .expect("should not fail to generate");
        assert!(generator.last_span_count >= 1);
        assert!(!bytes.is_empty());
    }

    #[test]
    fn a_repeated_string_is_written_as_an_index() {
        // Both fields carry `go`, so the first writes it inline and takes table index 1, and the
        // second writes the index. Field 3 is the language name and field 7 is the environment.
        let payload = TracerPayload {
            language_name: "go".to_string(),
            env: "go".to_string(),
            ..Default::default()
        };

        let encoded = payload
            .encode()
            .expect("encoding into a buffer cannot fail");

        assert_eq!(encoded, vec![0x82, 0x03, 0xa2, b'g', b'o', 0x07, 0x01]);
    }

    #[test]
    fn the_empty_string_is_written_as_index_zero() {
        // An inline empty literal would take a table slot on the reading side and shift every
        // later index by one, so the empty string is always written as its pre-seeded index
        // instead.
        let payload = TracerPayload {
            attributes: vec![("k".to_string(), AttributeValue::String(String::new()))],
            ..Default::default()
        };

        let encoded = payload
            .encode()
            .expect("encoding into a buffer cannot fail");

        // One field (10, attributes), holding a three-slot array: key `k` inline, type 1 (string),
        // then index 0 for the value.
        assert_eq!(encoded, vec![0x81, 0x0a, 0x93, 0xa1, b'k', 0x01, 0x00]);
    }

    #[test]
    fn fields_holding_their_zero_value_are_omitted() {
        let encoded = TracerPayload::default()
            .encode()
            .expect("encoding into a buffer cannot fail");

        // A map header declaring no fields at all, and nothing after it.
        assert_eq!(encoded, vec![0x80]);
    }

    #[test]
    fn an_attribute_array_declares_three_slots_per_entry() {
        let payload = TracerPayload {
            attributes: vec![
                ("a".to_string(), AttributeValue::Bool(true)),
                ("b".to_string(), AttributeValue::Int(-1)),
            ],
            ..Default::default()
        };

        let encoded = payload
            .encode()
            .expect("encoding into a buffer cannot fail");

        assert_eq!(encoded[0..3], [0x81, 0x0a, 0x96]);
    }

    #[test]
    fn string_indices_are_assigned_in_write_order_across_nesting_levels() {
        // `svc` first appears as the payload hostname and again as the span service, so the span
        // must reference it by the index the hostname established rather than repeating it
        // inline.
        let payload = TracerPayload {
            hostname: "svc".to_string(),
            chunks: vec![TraceChunk {
                spans: vec![Span {
                    service: "svc".to_string(),
                    span_id: 1,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let encoded = payload
            .encode()
            .expect("encoding into a buffer cannot fail");

        assert_eq!(
            encoded,
            vec![
                0x82, // payload map, two fields
                0x08, 0xa3, b's', b'v',
                b'c', // field 8 (hostname), inline `svc`, taking index 1
                0x0b, 0x91, // field 11 (chunks), one chunk
                0x81, 0x04, 0x91, // chunk map with one field: 4 (spans), one span
                0x82, // span map, two fields
                0x01, 0x01, // field 1 (service), string index 1
                0x04, 0x01, // field 4 (spanID), 1
            ]
        );
    }

    proptest::proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes in 0usize..=262_144) {
            let config = service_graph();
            let mut generator = V1::with_config(config, &mut SmallRng::seed_from_u64(0)).expect("config should be valid");
            let mut rng = SmallRng::seed_from_u64(seed);

            let mut bytes = Vec::new();
            generator.to_bytes(&mut rng, max_bytes, &mut bytes).expect("should not fail to generate");

            // A request too small to carry even the configured floor of chunks writes nothing,
            // which the block cache reads as a rejected block.
            proptest::prop_assert!(bytes.is_empty() || bytes.len() <= max_bytes);
        }
    }
}
