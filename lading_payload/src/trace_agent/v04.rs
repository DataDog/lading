//! Datadog Trace Agent v0.4 endpoint payload implementation.
//!
//! This module generates payloads compatible with the `/v0.4/traces`
//! endpoint. Implemented with reference to Agent version
//! 8cc5eb3e024ee54283efad4614175a065642bd9c.
use std::{
    collections::BTreeMap,
    io::{self, Write},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rand::{Rng, seq::IndexedRandom};
use rmp_serde::Serializer;
use serde::Serialize;

use crate::{
    Error, Generator,
    common::{config::ConfRange, strings},
};

/// Configuration validation error
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigError {
    /// Configuration range is invalid (min > max)
    InvalidRange,
    /// Configuration value cannot be zero
    ZeroValue,
    /// Error rate is not finite or outside [0.0, 1.0]
    InvalidErrorRate,
}

const STRING_POOL_SIZE: usize = 1_000_000;
const MAX_TRACE_DEPTH: usize = 8;
const MIN_SPAN_DURATION: Duration = Duration::from_millis(1);
const MAX_SPAN_DURATION: Duration = Duration::from_secs(1);

/// Generate a random duration within the given range
#[allow(clippy::cast_possible_truncation)] // u128 nanos to u64 safe for reasonable duration ranges
fn random_duration<R>(rng: &mut R, min: Duration, max: Duration) -> Duration
where
    R: Rng + ?Sized,
{
    let min_nanos = min.as_nanos() as u64;
    let max_nanos = max.as_nanos() as u64;
    let random_nanos = rng.random_range(min_nanos..=max_nanos);
    Duration::from_nanos(random_nanos)
}

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
            // All defaults arbitrarily chosen to be "large, but not too
            // large". A nice way of saying these values are all guesses.
            contexts: ConfRange::Inclusive {
                min: 5_000,
                max: 10_000,
            },
            spans_per_trace: ConfRange::Inclusive { min: 1, max: 50 },
            tags_per_span: ConfRange::Inclusive { min: 2, max: 20 },
            metrics_per_span: ConfRange::Inclusive { min: 0, max: 5 },
            error_rate: 0.01, // 1% error rate
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
    /// Validate configuration parameters
    ///
    /// # Errors
    ///
    /// Returns a `ConfigError` if configuration is invalid
    pub fn valid(&self) -> Result<(), ConfigError> {
        if !self.contexts.valid().0 {
            return Err(ConfigError::InvalidRange);
        }
        if self.contexts.start() == 0 {
            return Err(ConfigError::ZeroValue);
        }

        if !self.spans_per_trace.valid().0 {
            return Err(ConfigError::InvalidRange);
        }
        if self.spans_per_trace.start() == 0 {
            return Err(ConfigError::ZeroValue);
        }

        if !self.tags_per_span.valid().0 {
            return Err(ConfigError::InvalidRange);
        }

        if !self.metrics_per_span.valid().0 {
            return Err(ConfigError::InvalidRange);
        }

        if !self.error_rate.is_finite() || !(0.0..=1.0).contains(&self.error_rate) {
            return Err(ConfigError::InvalidErrorRate);
        }

        let ranges = [
            &self.service_name_length,
            &self.operation_name_length,
            &self.resource_name_length,
            &self.span_type_length,
            &self.tag_key_length,
            &self.tag_value_length,
            &self.metric_key_length,
        ];

        for range in ranges {
            if !range.valid().0 {
                return Err(ConfigError::InvalidRange);
            }
        }

        Ok(())
    }
}

/// V0.4 span structure matching protobuf definition with `MessagePack`
/// encoding.
///
/// This struct corresponds exactly to the Span message in
/// pkg/proto/datadog/trace/span.proto (lines 101-147), less `spanLinks` and
/// `spanEvents` which we may choose to support in the future. The protobuf uses
/// `@gotags: msg:"field_name"` annotations to generate `MessagePack`
/// serialization methods via github.com/tinylib/msgp.
///
/// Our serde-based serialization produces the same `MessagePack` format that
/// the datadog-agent expects when calling `pb.Traces.UnmarshalMsg()` in
/// pkg/trace/api/api.go:897.
#[derive(Debug, serde::Serialize)]
#[allow(clippy::struct_field_names)] // Field names match protobuf definition exactly
pub struct Span<'a> {
    /// `service` is the name of the service with which this span is associated.
    service: &'a str,
    /// `name` is the operation name of this span.
    name: &'a str,
    /// `resource` is the resource name of this span, also sometimes called the
    /// endpoint (for web spans).
    resource: &'a str,
    /// `traceID` is the ID of the trace to which this span belongs.
    trace_id: u64,
    /// `spanID` is the ID of this span.
    span_id: u64,
    /// `parentID` is the ID of this span's parent, or zero if this span has no
    /// parent.
    parent_id: u64,
    /// `start` is the number of nanoseconds between the Unix epoch and the
    /// beginning of this span.
    start: i64,
    /// `duration` is the time length of this span in nanoseconds.
    duration: i64,
    /// `error` is 1 if there is an error associated with this span, or 0 if
    /// there is not.
    error: i32,
    /// `meta` is a mapping from tag name to tag value for string-valued tags.
    meta: BTreeMap<&'a str, &'a str>,
    /// `metrics` is a mapping from tag name to tag value for numeric-valued
    /// tags.
    metrics: BTreeMap<&'a str, f64>,
    /// `kind` is the type of the service with which this span is associated.
    /// Example values: web, db, lambda.
    #[serde(alias = "type")]
    kind: &'a str,
    /// `meta_struct` is a registry of structured "other" data used by, e.g.,
    /// `AppSec`.
    meta_struct: BTreeMap<&'a str, Vec<u8>>,
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
    fn new<R>(config: &Config, rng: &mut R) -> Result<Self, Error>
    where
        R: Rng + ?Sized,
    {
        let str_pool = strings::Pool::with_size(rng, STRING_POOL_SIZE);

        let context_count = config.contexts.sample(rng);
        let mut contexts = Vec::new();
        for _ in 0..context_count {
            let service_len = config.service_name_length.sample(rng);
            let service = if service_len == 0 {
                String::new()
            } else {
                str_pool
                    .of_size(rng, service_len.into())
                    .ok_or(Error::StringGenerate)?
                    .to_string()
            };

            let operation_len = config.operation_name_length.sample(rng);
            let operation = if operation_len == 0 {
                String::new()
            } else {
                str_pool
                    .of_size(rng, operation_len.into())
                    .ok_or(Error::StringGenerate)?
                    .to_string()
            };

            let resource_len = config.resource_name_length.sample(rng);
            let resource = if resource_len == 0 {
                String::new()
            } else {
                str_pool
                    .of_size(rng, resource_len.into())
                    .ok_or(Error::StringGenerate)?
                    .to_string()
            };

            let span_type_len = config.span_type_length.sample(rng);
            let span_type = if span_type_len == 0 {
                String::new()
            } else {
                str_pool
                    .of_size(rng, span_type_len.into())
                    .ok_or(Error::StringGenerate)?
                    .to_string()
            };

            contexts.push(Context {
                service,
                operation,
                resource,
                span_type,
            });
        }

        Ok(Self { contexts })
    }
}

/// Represents a single trace (array of spans with same `trace_id`)
#[derive(Debug)]
pub struct Trace<'a> {
    /// The spans that make up this trace
    spans: Vec<Span<'a>>,
}

/// V0.4 payload generator
#[derive(Debug, Clone)]
pub struct V04 {
    config: Config,
    str_pool: strings::Pool,
    context_generator: ContextGenerator,
}

impl V04 {
    /// Create a new v0.4 payload generator with provided configuration
    ///
    /// # Errors
    ///
    /// Returns error if string generation from the string pool fails
    pub fn with_config<R>(config: Config, rng: &mut R) -> Result<Self, Error>
    where
        R: Rng + ?Sized,
    {
        let str_pool = strings::Pool::with_size(rng, STRING_POOL_SIZE);
        let context_generator = ContextGenerator::new(&config, rng)?;
        Ok(Self {
            config,
            str_pool,
            context_generator,
        })
    }

    /// Generate a complete trace matching the datadog-agent's pb.Traces format.
    ///
    /// Based on analysis of pkg/trace/testutil/trace.go:102-128, this generates
    /// proper trace hierarchies where all spans share the same `trace_id` and
    /// have realistic parent-child timestamp relationships.
    ///
    /// Critical for v0.4: Each trace becomes one element in the `pb.Traces`
    /// array, and all spans within must have the same `trace_id` for agent
    /// acceptance.
    ///
    /// # Errors
    ///
    /// Returns error if timestamp calculations result in invalid ranges
    fn generate_trace<R>(&self, rng: &mut R) -> Result<Trace<'_>, Error>
    where
        R: Rng + ?Sized,
    {
        let trace_id = rng.random();
        let span_count = self.config.spans_per_trace.sample(rng);
        let mut spans = Vec::with_capacity(span_count as usize);

        let base_timestamp = UNIX_EPOCH;
        let root_span_id = rng.random();
        let root_duration = random_duration(rng, MIN_SPAN_DURATION, MAX_SPAN_DURATION);
        let root_span = generate_span(
            self,
            rng,
            trace_id,
            root_span_id,
            0, // Root spans have parent_id = 0
            base_timestamp,
            root_duration,
        )?;
        spans.push(root_span);

        // Generate child spans with random timing, allowing for async patterns
        // where child spans may have gaps or overlaps with parents.
        let mut parent_candidates = vec![root_span_id];
        for _ in 1..span_count {
            let parent_id = parent_candidates
                .choose(rng)
                .copied()
                .ok_or(Error::StringGenerate)?;

            // Child spans get independent random timing, not constrained by
            // parent.
            let span_start_offset = random_duration(rng, Duration::ZERO, MAX_SPAN_DURATION);
            let span_start = base_timestamp + span_start_offset;
            let span_duration = random_duration(rng, MIN_SPAN_DURATION, MAX_SPAN_DURATION);

            let span_id = rng.random();
            let span = generate_span(
                self,
                rng,
                trace_id,
                span_id,
                parent_id,
                span_start,
                span_duration,
            )?;

            // If we aren't past the max trace depth, add as a potential parent.
            if spans.len() < MAX_TRACE_DEPTH {
                parent_candidates.push(span_id);
            }
            spans.push(span);
        }

        Ok(Trace { spans })
    }
}

/// Generate a single span using context-based approach. This follows our
/// dogstatsd and otel pattern.
fn generate_span<'a, R>(
    generator: &'a V04,
    rng: &mut R,
    trace_id: u64,
    span_id: u64,
    parent_id: u64,
    start_time: SystemTime,
    duration: Duration,
) -> Result<Span<'a>, Error>
where
    R: Rng + ?Sized,
{
    let context = generator
        .context_generator
        .contexts
        .choose(rng)
        .ok_or(Error::StringGenerate)?;

    let tag_count = generator.config.tags_per_span.sample(rng);
    let mut meta = BTreeMap::default();
    for _ in 0..tag_count {
        let key_len = generator.config.tag_key_length.sample(rng);
        let value_len = generator.config.tag_value_length.sample(rng);

        let key = if key_len == 0 {
            ""
        } else {
            generator
                .str_pool
                .of_size(rng, key_len.into())
                .ok_or(Error::StringGenerate)?
        };

        let value = if value_len == 0 {
            ""
        } else {
            generator
                .str_pool
                .of_size(rng, value_len.into())
                .ok_or(Error::StringGenerate)?
        };

        meta.insert(key, value);
    }

    let metric_count = generator.config.metrics_per_span.sample(rng);
    let mut metrics = BTreeMap::default();
    for _ in 0..metric_count {
        let key_len = generator.config.metric_key_length.sample(rng);
        let key = if key_len == 0 {
            ""
        } else {
            generator
                .str_pool
                .of_size(rng, key_len.into())
                .ok_or(Error::StringGenerate)?
        };
        metrics.insert(key, rng.random::<f64>());
    }

    let error = i32::from(rng.random::<f32>() < generator.config.error_rate);
    // SAFETY: 2**63 nanoseconds is ~292 years, which means that since we take
    // UNIX_EPOCH as time zero we're safet to cast nanos to i64 until the year
    // 2262.
    #[allow(clippy::cast_possible_truncation)]
    Ok(Span {
        service: &context.service,
        name: &context.operation,
        resource: &context.resource,
        trace_id,
        span_id,
        parent_id,
        start: start_time
            .duration_since(UNIX_EPOCH)
            // NOTE we're constrained by the lading_payload::Error via
            // Serialize, else we'd have a slightly less awkward construction
            // here.
            .map_err(|_| {
                Error::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid timestamp",
                ))
            })?
            .as_nanos() as i64,
        duration: duration.as_nanos() as i64,
        error,
        meta,
        metrics,
        kind: &context.span_type,
        meta_struct: BTreeMap::default(),
    })
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
    fn to_bytes<W, R>(&mut self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        let mut traces: Vec<Vec<Span>> = vec![];

        // Elide the cost of per-message serialization, batching in fixed size
        // chunks.
        let batch_size = 10;

        for _ in 0..batch_size {
            let trace = self.generate(&mut rng)?;
            traces.push(trace.spans);
        }
        loop {
            let mut buf = Vec::with_capacity(max_bytes);
            traces.serialize(&mut Serializer::new(&mut buf))?;

            if buf.len() > max_bytes {
                break;
            }
            for _ in 0..batch_size {
                let trace = self.generate(&mut rng)?;
                traces.push(trace.spans);
            }
        }

        // Unlike a protobuf based format, or an in-place string we can't know
        // the exact size of the msgpack serialized form. We've made a good
        // guess, now use a binary search to refine.
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
            let mut generator = V04::with_config(Config::default(), &mut rng)?;

            let mut bytes = Vec::with_capacity(max_bytes);
            generator.to_bytes(rng, max_bytes, &mut bytes)?;
            debug_assert!(bytes.len() <= max_bytes);
        }

        #[test]
        fn trace_spans_have_same_trace_id_v04(seed: u64) {
            let mut rng = SmallRng::seed_from_u64(seed);
            let generator = V04::with_config(Config::default(), &mut rng)?;

            let trace = generator.generate(&mut rng)?;
            if let Some(first_span) = trace.spans.first() {
                let expected_trace_id = first_span.trace_id;
                for span in &trace.spans {
                    prop_assert_eq!(span.trace_id, expected_trace_id);
                }
            }
        }

        #[test]
        fn parent_child_structure_validity_v04(seed: u64) {
            let mut rng = SmallRng::seed_from_u64(seed);
            let generator = V04::with_config(Config::default(), &mut rng)?;

            let trace = generator.generate(&mut rng)?;

            // Verify structural properties but not temporal constraints
            for span in &trace.spans {
                if span.parent_id != 0 {
                    // Parent must exist in the trace
                    prop_assert!(trace.spans.iter().any(|s| s.span_id == span.parent_id));
                }
            }

            // All spans should have the same trace_id
            let trace_id = trace.spans[0].trace_id;
            for span in &trace.spans {
                prop_assert_eq!(span.trace_id, trace_id);
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
