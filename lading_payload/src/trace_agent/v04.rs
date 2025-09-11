//! Datadog Trace Agent v0.4 endpoint payload implementation.
//!
//! This module generates payloads compatible with the `/v0.4/traces` endpoint.
//!
//! V0.4 SPECIFICATION (pkg/trace/api/version.go:22-57):
//! - Request: application/msgpack
//! - Payload: "An array of arrays of Span" (pb.Traces = [][]Span)
//! - Response: application/json service sampling rates
//!
//! V0.4 PROCESSING PATH:
//! - Uses default case in `decodeTracerPayload()` (api.go:522)
//! - Calls `decodeRequest()` expecting `pb.Traces` (api.go:523-524)
//! - `MessagePack` decoded via `pb.Traces.UnmarshalMsg()` (api.go:897)
//!
//! Our implementation generates `Vec<Vec<Span>>` that serializes to msgpack
//! in the exact format the v0.4 endpoint expects for `pb.Traces`.

use std::{io::Write, time};

use rand::{Rng, seq::IndexedRandom};
use rmp_serde::Serializer;
use rustc_hash::FxHashMap;
use serde::Serialize;

use crate::{
    Error, Generator,
    common::{config::ConfRange, strings},
};

// String pool and context sizing constants
const STRING_POOL_SIZE: usize = 1_000_000;

// Trace generation constants
const MAX_TRACE_DEPTH: usize = 3;
const INITIAL_TRACE_COUNT: usize = 10;
const TRACE_BATCH_SIZE: usize = 10;

// Timestamp constants
//
// NOTE(claude) You MAY NOT change these.
const MAX_SPAN_AGE: time::Duration = time::Duration::from_secs(3600); // 1 hour
const MIN_SPAN_DURATION: time::Duration = time::Duration::from_millis(1);
const MAX_SPAN_DURATION: time::Duration = time::Duration::from_secs(1);

// NOTE(claude) You _may_ use allow to avoid clippy dings but you may only use
// them on single functions. You MUST not have them inline in functions. You
// MUST carefully justify why you have used an allow. You MAY NOT use them just
// to make clippy pass; clippy is here for a reason and it forces better code,
// code that you try to skip writing.

/// Configuration for v0.4 trace payload generation.
///
/// Focuses on parameters that affect trace-agent scaling performance:
/// cardinality (contexts), payload structure (spans per trace), and
/// string variety (tags/metrics per span).
#[derive(Debug, serde::Deserialize, serde::Serialize, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// The unique trace contexts to generate. A context is a unique combination
    /// of service + operation + resource. This is the primary cardinality driver
    /// for trace-agent memory usage and processing overhead.
    pub contexts: ConfRange<u32>,
    /// Number of spans per trace. Affects trace processing complexity and
    /// memory allocation patterns in the agent.
    pub spans_per_trace: ConfRange<u16>,
    /// Number of tags per span. Tags are the main source of cardinality
    /// and memory pressure in trace processing.
    pub tags_per_span: ConfRange<u8>,
    /// Number of metrics per span. Affects metric processing overhead.
    pub metrics_per_span: ConfRange<u8>,
    /// Probability of a span having an error. Affects error tracking overhead.
    pub error_rate: f32,
    /// Length range for service names
    pub service_name_length: ConfRange<u8>,
    /// Length range for operation names
    pub operation_name_length: ConfRange<u8>,
    /// Length range for resource names
    pub resource_name_length: ConfRange<u8>,
    /// Length range for span type names
    pub span_type_length: ConfRange<u8>,
    /// Length range for tag keys
    pub tag_key_length: ConfRange<u8>,
    /// Length range for tag values
    pub tag_value_length: ConfRange<u8>,
    /// Length range for metric keys
    pub metric_key_length: ConfRange<u8>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            // High cardinality similar to dogstatsd contexts (5K-10K)
            contexts: ConfRange::Inclusive {
                min: 5_000,
                max: 10_000,
            },
            // Realistic span counts that stress trace assembly
            spans_per_trace: ConfRange::Inclusive { min: 1, max: 50 },
            // Tag cardinality is critical for agent memory usage
            tags_per_span: ConfRange::Inclusive { min: 2, max: 20 },
            // Metrics processing overhead
            metrics_per_span: ConfRange::Inclusive { min: 0, max: 5 },
            // Low error rate (most traces succeed)
            error_rate: 0.01, // 1% error rate
            // String length ranges - configurable from 0 to 128
            service_name_length: ConfRange::Inclusive { min: 0, max: 128 },
            operation_name_length: ConfRange::Inclusive { min: 0, max: 128 },
            resource_name_length: ConfRange::Inclusive { min: 0, max: 128 },
            span_type_length: ConfRange::Inclusive { min: 0, max: 128 },
            tag_key_length: ConfRange::Inclusive { min: 0, max: 128 },
            tag_value_length: ConfRange::Inclusive { min: 0, max: 128 },
            metric_key_length: ConfRange::Inclusive { min: 0, max: 128 },
        }
    }
}

impl Config {
    /// Validate the configuration parameters
    /// # Errors
    /// Returns an error string if configuration is invalid
    pub fn valid(&self) -> Result<(), String> {
        let (contexts_valid, reason) = self.contexts.valid();
        if !contexts_valid {
            return Err(format!("contexts is invalid: {reason}"));
        }
        if self.contexts.start() == 0 {
            return Err("contexts start value cannot be 0".to_string());
        }

        let (spans_valid, reason) = self.spans_per_trace.valid();
        if !spans_valid {
            return Err(format!("spans_per_trace is invalid: {reason}"));
        }
        if self.spans_per_trace.start() == 0 {
            return Err("spans_per_trace start value cannot be 0".to_string());
        }

        let (tags_valid, reason) = self.tags_per_span.valid();
        if !tags_valid {
            return Err(format!("tags_per_span is invalid: {reason}"));
        }

        let (metrics_valid, reason) = self.metrics_per_span.valid();
        if !metrics_valid {
            return Err(format!("metrics_per_span is invalid: {reason}"));
        }

        if !self.error_rate.is_finite() || !(0.0..=1.0).contains(&self.error_rate) {
            return Err(format!(
                "error_rate must be finite and in range [0.0, 1.0], got {}",
                self.error_rate
            ));
        }

        // Validate string length ranges
        let string_ranges = [
            ("service_name_length", &self.service_name_length),
            ("operation_name_length", &self.operation_name_length),
            ("resource_name_length", &self.resource_name_length),
            ("span_type_length", &self.span_type_length),
            ("tag_key_length", &self.tag_key_length),
            ("tag_value_length", &self.tag_value_length),
            ("metric_key_length", &self.metric_key_length),
        ];

        for (name, range) in &string_ranges {
            let (valid, reason) = range.valid();
            if !valid {
                return Err(format!("{name} is invalid: {reason}"));
            }
        }

        Ok(())
    }
}

/// V0.4 span structure matching protobuf definition with `MessagePack` encoding.
///
/// This struct corresponds exactly to the Span message in pkg/proto/datadog/trace/span.proto
/// (lines 101-147). The protobuf uses `@gotags: msg:"field_name"` annotations to generate
/// `MessagePack` serialization methods via github.com/tinylib/msgp.
///
/// Our serde-based serialization produces the same `MessagePack` format that the datadog-agent
/// expects when calling `pb.Traces.UnmarshalMsg()` in pkg/trace/api/api.go:897.
#[derive(Debug, serde::Serialize)]
pub struct Span<'a> {
    /// service is the name of the service with which this span is associated.
    service: &'a str,
    /// name is the operation name of this span.
    name: &'a str,
    /// resource is the resource name of this span, also sometimes called the endpoint (for web spans).
    resource: &'a str,
    /// traceID is the ID of the trace to which this span belongs.
    trace_id: u64,
    /// spanID is the ID of this span.
    span_id: u64,
    /// parentID is the ID of this span's parent, or zero if this span has no parent.
    parent_id: u64,
    /// start is the number of nanoseconds between the Unix epoch and the beginning of this span.
    start: i64,
    /// duration is the time length of this span in nanoseconds.
    duration: i64,
    /// error is 1 if there is an error associated with this span, or 0 if there is not.
    error: i32,
    /// meta is a mapping from tag name to tag value for string-valued tags.
    meta: FxHashMap<&'a str, &'a str>,
    /// metrics is a mapping from tag name to tag value for numeric-valued tags.
    metrics: FxHashMap<&'a str, f64>,
    /// type is the type of the service with which this span is associated.  Example values: web, db, lambda.
    #[serde(alias = "type")]
    kind: &'a str,
    /// `meta_struct` is a registry of structured "other" data used by, e.g., `AppSec`.
    meta_struct: FxHashMap<&'a str, Vec<u8>>,
}

/// Context represents a unique service+operation+resource combination.
/// This is the key cardinality driver for trace-agent performance.
#[derive(Debug, Clone)]
struct Context {
    service: String,
    operation: String,
    resource: String,
    span_type: String,
}

/// Context generator for v0.4 following lading's pre-computation pattern.
///
/// Pre-generates contexts (unique service+operation+resource combinations)
/// to create realistic cardinality pressure on the trace agent.
#[derive(Debug, Clone)]
struct ContextGenerator {
    /// Pre-generated trace contexts (service+operation+resource combinations)
    contexts: Vec<Context>,
}

impl ContextGenerator {
    fn new<R>(config: &Config, rng: &mut R) -> Self
    where
        R: Rng + ?Sized,
    {
        let str_pool = strings::Pool::with_size(rng, STRING_POOL_SIZE);

        // Generate unique contexts (like dogstatsd contexts)
        let context_count = config.contexts.sample(rng);
        let mut contexts = Vec::new();

        for _ in 0..context_count {
            let service_len = config.service_name_length.sample(rng);
            let service = if service_len == 0 {
                String::new()
            } else {
                str_pool
                    .of_size_range(rng, service_len..service_len + 1)
                    .unwrap_or("service")
                    .to_string()
            };

            let operation_len = config.operation_name_length.sample(rng);
            let operation = if operation_len == 0 {
                String::new()
            } else {
                str_pool
                    .of_size_range(rng, operation_len..operation_len + 1)
                    .unwrap_or("operation")
                    .to_string()
            };

            let resource_len = config.resource_name_length.sample(rng);
            let resource = if resource_len == 0 {
                String::new()
            } else {
                str_pool
                    .of_size_range(rng, resource_len..resource_len + 1)
                    .unwrap_or("resource")
                    .to_string()
            };

            let span_type_len = config.span_type_length.sample(rng);
            let span_type = if span_type_len == 0 {
                String::new()
            } else {
                str_pool
                    .of_size_range(rng, span_type_len..span_type_len + 1)
                    .unwrap_or("web")
                    .to_string()
            };

            contexts.push(Context {
                service,
                operation,
                resource,
                span_type,
            });
        }

        Self { contexts }
    }
}

/// Represents a single trace (array of spans with same `trace_id`)
#[derive(Debug)]
pub struct Trace<'a> {
    /// The spans that make up this trace
    pub spans: Vec<Span<'a>>,
}

/// V0.4 payload generator
#[derive(Debug, Clone)]
pub struct V04 {
    config: Config,
    str_pool: strings::Pool,
    context_generator: ContextGenerator,
}

impl V04 {
    /// Create a new v0.4 payload generator with default configuration
    pub fn new<R>(rng: &mut R) -> Self
    where
        R: Rng + ?Sized,
    {
        let config = Config::default();
        Self::with_config(config, rng)
    }

    /// Create a new v0.4 payload generator with provided configuration
    pub fn with_config<R>(config: Config, rng: &mut R) -> Self
    where
        R: Rng + ?Sized,
    {
        let str_pool = strings::Pool::with_size(rng, STRING_POOL_SIZE);
        let context_generator = ContextGenerator::new(&config, rng);
        Self {
            config,
            str_pool,
            context_generator,
        }
    }

    /// Generate a complete trace matching the datadog-agent's pb.Traces format.
    ///
    /// Based on analysis of pkg/trace/testutil/trace.go:102-128, this generates
    /// proper trace hierarchies where all spans share the same `trace_id` and
    /// have realistic parent-child timestamp relationships.
    ///
    /// Critical for v0.4: Each trace becomes one element in the `pb.Traces` array,
    /// and all spans within must have the same `trace_id` for agent acceptance.
    pub fn generate_trace<R>(&self, rng: &mut R) -> Result<Trace<'_>, Error>
    where
        R: Rng + ?Sized,
    {
        let trace_id = rng.random();
        let span_count = self.config.spans_per_trace.sample(rng);
        let mut spans = Vec::with_capacity(span_count as usize);

        // Generate realistic base timestamp (recent, within configured age)
        let now = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        let base_time = now - rng.random_range(0..MAX_SPAN_AGE.as_nanos() as i64);

        // Generate root span (parent_id = 0)
        let root_span_id = rng.random();
        let root_duration = rng
            .random_range(MIN_SPAN_DURATION.as_nanos() as i64..MAX_SPAN_DURATION.as_nanos() as i64);
        let root_span = generate_span(
            self,
            rng,
            trace_id,
            root_span_id,
            0, // Root spans have parent_id = 0 per agent expectations
            base_time,
            root_duration,
        );
        spans.push(root_span);

        // Generate child spans following testutil/trace.go:33-67 pattern
        // Each child span must fit within its parent's time window
        let mut parent_candidates = vec![(root_span_id, base_time, base_time + root_duration)];

        for _ in 1..span_count {
            let (parent_id, parent_start, parent_end) = parent_candidates
                .choose(rng)
                .copied()
                .unwrap_or((root_span_id, base_time, base_time + root_duration));

            // Child must start within parent and end before parent ends
            // This matches the logic in testutil/trace.go:56-61
            let min_duration = MIN_SPAN_DURATION.as_nanos() as i64;
            let span_start =
                rng.random_range(parent_start..parent_end.saturating_sub(min_duration));
            let max_duration = parent_end - span_start;
            let span_duration = rng.random_range(min_duration..=max_duration.max(min_duration));

            let span_id = rng.random();
            let span = generate_span(
                self,
                rng,
                trace_id,
                span_id,
                parent_id,
                span_start,
                span_duration,
            );

            // Add as potential parent for deeper nesting
            if spans.len() < MAX_TRACE_DEPTH {
                parent_candidates.push((span_id, span_start, span_start + span_duration));
            }

            spans.push(span);
        }

        Ok(Trace { spans })
    }
}

/// Generate a single span using context-based approach like dogstatsd.
///
/// Selects from pre-generated contexts to create realistic cardinality
/// that stresses trace-agent processing and memory usage patterns.
fn generate_span<'a, R>(
    generator: &'a V04,
    rng: &mut R,
    trace_id: u64,
    span_id: u64,
    parent_id: u64,
    start_time: i64,
    duration: i64,
) -> Span<'a>
where
    R: Rng + ?Sized,
{
    // Choose a context (service+operation+resource combination)
    // This drives cardinality like dogstatsd contexts
    let context = generator
        .context_generator
        .contexts
        .choose(rng)
        .expect("contexts should not be empty");

    // Generate tags (meta) - key source of cardinality
    let tag_count = generator.config.tags_per_span.sample(rng);
    let mut meta = FxHashMap::default();
    for _ in 0..tag_count {
        let key_len = generator.config.tag_key_length.sample(rng);
        let value_len = generator.config.tag_value_length.sample(rng);

        let key = if key_len == 0 {
            ""
        } else {
            generator
                .str_pool
                .of_size_range(rng, key_len..key_len + 1)
                .unwrap_or("key")
        };

        let value = if value_len == 0 {
            ""
        } else {
            generator
                .str_pool
                .of_size_range(rng, value_len..value_len + 1)
                .unwrap_or("value")
        };

        meta.insert(key, value);
    }

    // Generate metrics - affects processing overhead
    let metric_count = generator.config.metrics_per_span.sample(rng);
    let mut metrics = FxHashMap::default();
    for _ in 0..metric_count {
        let key_len = generator.config.metric_key_length.sample(rng);
        let key = if key_len == 0 {
            ""
        } else {
            generator
                .str_pool
                .of_size_range(rng, key_len..key_len + 1)
                .unwrap_or("metric")
        };
        metrics.insert(key, rng.random::<f64>());
    }

    // Error rate affects agent error handling paths
    let error = i32::from(rng.random::<f32>() < generator.config.error_rate);

    Span {
        service: &context.service,
        name: &context.operation,
        resource: &context.resource,
        trace_id,
        span_id,
        parent_id,
        start: start_time,
        duration,
        error,
        meta,
        metrics,
        kind: &context.span_type,
        meta_struct: FxHashMap::default(),
    }
}

impl<'a> Generator<'a> for V04 {
    type Output = Trace<'a>;
    type Error = Error;

    fn generate<R>(&'a self, rng: &mut R) -> Result<Self::Output, Error>
    where
        R: rand::Rng + ?Sized,
    {
        self.generate_trace(rng)
    }
}

impl crate::Serialize for V04 {
    /// Serialize traces in v0.4 msgpack format exactly as agent expects.
    ///
    /// CRITICAL V0.4 FORMAT: "An array of arrays of Span" → `pb.Traces = [][]Span`
    fn to_bytes<W, R>(&mut self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        let mut traces: Vec<Vec<Span>> = vec![];

        // Start with initial traces
        for _ in 0..INITIAL_TRACE_COUNT {
            let trace = self.generate(&mut rng)?;
            traces.push(trace.spans);
        }

        // Grow until we exceed max_bytes
        loop {
            let mut buf = Vec::with_capacity(max_bytes);
            traces.serialize(&mut Serializer::new(&mut buf))?;

            if buf.len() > max_bytes {
                break;
            }

            // Add more traces
            for _ in 0..TRACE_BATCH_SIZE {
                let trace = self.generate(&mut rng)?;
                traces.push(trace.spans);
            }
        }

        // Binary search for optimal size
        let mut high = traces.len();
        let mut low = 0;

        while low < high {
            let mid = (low + high).div_ceil(2);
            let mut buf = Vec::with_capacity(max_bytes);
            traces[0..mid].serialize(&mut Serializer::new(&mut buf))?;

            if buf.len() <= max_bytes {
                low = mid;
            } else {
                high = mid - 1;
            }
        }

        let mut buf = Vec::with_capacity(max_bytes);
        traces[0..low].serialize(&mut Serializer::new(&mut buf))?;
        writer.write_all(&buf)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;
    use rand::{SeedableRng, rngs::SmallRng};

    use super::{ConfRange, Config, V04};
    use crate::{Generator, Serialize};

    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes_v04(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut generator = V04::new(&mut rng);

            let mut bytes = Vec::with_capacity(max_bytes);
            generator.to_bytes(rng, max_bytes, &mut bytes)?;
            debug_assert!(bytes.len() <= max_bytes);
        }

        #[test]
        fn trace_spans_have_same_trace_id_v04(seed: u64) {
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut generator = V04::new(&mut rng);

            let trace = generator.generate(&mut rng)?;

            // All spans in a trace should have the same trace_id
            if let Some(first_span) = trace.spans.first() {
                let expected_trace_id = first_span.trace_id;
                for span in &trace.spans {
                    prop_assert_eq!(span.trace_id, expected_trace_id);
                }
            }
        }

        #[test]
        fn parent_child_timestamp_validity_v04(seed: u64) {
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut generator = V04::new(&mut rng);

            let trace = generator.generate(&mut rng)?;

            for span in &trace.spans {
                if span.parent_id != 0 {
                    // Find parent span
                    if let Some(parent) = trace.spans.iter().find(|s| s.span_id == span.parent_id) {
                        // Child should start after or at the same time as parent
                        prop_assert!(span.start >= parent.start);
                        // Child should end before or at the same time as parent
                        prop_assert!(span.start + span.duration <= parent.start + parent.duration);
                    }
                }
            }
        }

        #[test]
        fn config_validation_works_v04(
            contexts_min in 1u32..=1000,
            contexts_max in 1u32..=1000,
            error_rate in 0.0f32..=1.0
        ) {
            let mut config = Config::default();
            config.contexts = if contexts_min <= contexts_max {
                ConfRange::Inclusive { min: contexts_min, max: contexts_max }
            } else {
                ConfRange::Inclusive { min: contexts_max, max: contexts_min }
            };
            config.error_rate = error_rate;

            prop_assert!(config.valid().is_ok());
        }
    }

    #[test]
    fn config_validation_catches_invalid_error_rate_v04() {
        let mut config = Config::default();
        config.error_rate = 1.5; // Invalid
        assert!(config.valid().is_err());

        config.error_rate = -0.1; // Invalid
        assert!(config.valid().is_err());
    }

    #[test]
    fn config_validation_catches_zero_contexts_v04() {
        let mut config = Config::default();
        config.contexts = ConfRange::Constant(0);
        assert!(config.valid().is_err());
    }
}
