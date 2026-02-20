//! Datadog Trace Agent v0.4 endpoint payload implementation.
//!
//! This module generates payloads compatible with the `/v0.4/traces`
//! endpoint. Implemented with reference to Agent version
//! 8cc5eb3e024ee54283efad4614175a065642bd9c.
use std::{collections::BTreeMap, io::Write, rc::Rc};

use rand::{Rng, seq::IndexedRandom};
use rmp_serde::Serializer;
use serde::Serialize;

use crate::{
    Error, Generator,
    common::{
        config::ConfRange,
        strings::{self, Pool},
    },
};

// A v0.4 Trace is a Vec<Span> that obeys the following properties:
//
// - Spans within a trace share the same trace-id
// - Spans within a trace have the same service name
// - Exactly one span has parent_id = 0 (the root span)
// - All other spans within a trace have parent_id pointing to another span
//   within the trace
// - Each span has a unique span_id within the trace

/// Configuration validation error
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigError {
    /// Configuration range is invalid (min > max)
    InvalidRange,
    /// Configuration value cannot be zero
    ZeroValue,
    /// Error rate is not finite or outside [0.0, 1.0]
    InvalidErrorRate,
    /// Context count exceeds maximum allowed value
    TooManyContexts,
}

const STRING_POOL_SIZE: usize = 1_000_000;
const MAX_TRACE_DEPTH: usize = 8;
const MAX_CONTEXTS: u32 = 1_000_000;

/// Minimum valid start timestamp for trace-agent (Year 2000 in nanoseconds
/// since `UNIX_EPOCH`).
///
/// Via datadog-agent/pkg/trace/agent/normalizer.go:39
const YEAR_2000_NANOS: i64 = 946_684_800_000_000_000;

/// Configuration for v0.4 trace payload generation.
///
/// Focuses on parameters that affect trace-agent scaling performance:
/// cardinality (service variety), payload structure (spans per trace), and
/// string variety (tags/metrics per span).
#[derive(Debug, serde::Deserialize, serde::Serialize, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Number of unique services to generate across all traces.
    /// Each trace gets one service, but different traces can have different services.
    pub contexts: ConfRange<u32>,
    /// Number of spans per trace.
    pub spans_per_trace: ConfRange<u8>,
    /// Number of tags per span.
    pub tags_per_span: ConfRange<u8>,
    /// Number of metrics per span.
    pub metrics_per_span: ConfRange<u8>,
    /// Probability of a span having an error.
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
        if self.contexts.end() > MAX_CONTEXTS {
            return Err(ConfigError::TooManyContexts);
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
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    meta: BTreeMap<&'a str, &'a str>,
    /// `metrics` is a mapping from tag name to tag value for numeric-valued
    /// tags.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    metrics: BTreeMap<&'a str, f64>,
    /// `kind` is the type of the service with which this span is associated.
    /// Example values: web, db, lambda.
    #[serde(rename = "type")]
    kind: &'a str,
    /// `meta_struct` is a registry of structured "other" data used by, e.g.,
    /// `AppSec`.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    meta_struct: BTreeMap<&'a str, Vec<u8>>,
}

/// Represents a single trace (array of spans with same `trace_id`)
#[derive(Debug)]
pub struct Trace<'a> {
    /// The spans that make up this trace
    spans: Vec<Span<'a>>,
}

/// A template for service/operation/resource combinations used in trace generation
#[derive(Debug, Clone)]
struct TraceTemplate {
    /// Handle to service name in the string pool
    service: strings::Handle,
    /// Handle to operation name in the string pool
    operation: strings::Handle,
    /// Handle to resource name in the string pool
    resource: strings::Handle,
    /// Handle to span type in the string pool
    span_type: strings::Handle,
}

/// V0.4 payload generator
#[derive(Debug, Clone)]
pub struct V04 {
    config: Config,
    str_pool: Rc<strings::RandomStringPool>,
    /// Pre-generated templates indexed by context ID
    templates: Vec<TraceTemplate>,
}

impl V04 {
    /// Create a new v0.4 payload generator with provided configuration
    ///
    /// # Errors
    ///
    /// Returns error if string generation from the string pool fails
    pub fn with_config<R>(config: Config, rng: &mut R) -> Result<Self, Error>
    where
        R: Rng,
    {
        let str_pool = Rc::new(strings::RandomStringPool::with_size(rng, STRING_POOL_SIZE));

        let num_contexts = config.contexts.sample(rng) as usize;
        let mut templates = Vec::with_capacity(num_contexts);

        for _ in 0..num_contexts {
            let service_len = config.service_name_length.sample(rng);
            let operation_len = config.operation_name_length.sample(rng);
            let resource_len = config.resource_name_length.sample(rng);
            let span_type_len = config.span_type_length.sample(rng);

            let (_, service) = str_pool
                .of_size_with_handle(rng, service_len.into())
                .ok_or(Error::StringGenerate)?;
            let (_, operation) = str_pool
                .of_size_with_handle(rng, operation_len.into())
                .ok_or(Error::StringGenerate)?;
            let (_, resource) = str_pool
                .of_size_with_handle(rng, resource_len.into())
                .ok_or(Error::StringGenerate)?;
            let (_, span_type) = str_pool
                .of_size_with_handle(rng, span_type_len.into())
                .ok_or(Error::StringGenerate)?;

            templates.push(TraceTemplate {
                service,
                operation,
                resource,
                span_type,
            });
        }

        Ok(Self {
            config,
            str_pool,
            templates,
        })
    }
}

impl<'a> Generator<'a> for V04 {
    type Output = Trace<'a>;
    type Error = Error;

    fn generate<R>(&'a self, rng: &mut R) -> Result<Self::Output, Error>
    where
        R: rand::Rng + ?Sized,
    {
        let trace_id = rng.random();
        let span_count = self.config.spans_per_trace.sample(rng);
        let mut spans = Vec::with_capacity(span_count as usize);

        // Select a template for this trace - all spans share the same
        // service/operation/resource/type
        let template_index = rng.random_range(0..self.templates.len());
        let template = &self.templates[template_index];

        // Generate random start timestamp that satisfies trace-agent
        // constraints:
        //
        // 1. Must be >= Year 2000 expressed in nanoseconds (946684800000000000)
        // 2. Must be <= i64::MAX to avoid overflow in Go
        let root_start_nanos = rng.random_range(YEAR_2000_NANOS..=i64::MAX);

        // Generate random duration that doesn't cause overflow:
        // start + duration must fit in i64::MAX
        let max_duration = i64::MAX - root_start_nanos;
        let root_duration_nanos = rng.random_range(0..=max_duration);

        let root_span_id = rng.random();

        let service = self
            .str_pool
            .using_handle(template.service)
            .ok_or(Error::StringGenerate)?;
        let operation = self
            .str_pool
            .using_handle(template.operation)
            .ok_or(Error::StringGenerate)?;
        let resource = self
            .str_pool
            .using_handle(template.resource)
            .ok_or(Error::StringGenerate)?;
        let span_type = self
            .str_pool
            .using_handle(template.span_type)
            .ok_or(Error::StringGenerate)?;

        let root_span = self.create_span(
            rng,
            service,
            operation,
            resource,
            span_type,
            trace_id,
            root_span_id,
            0, // Root spans have parent_id = 0
            root_start_nanos,
            root_duration_nanos,
        );
        spans.push(root_span);

        // Generate child spans with random timing, allowing for async patterns
        // where child spans may have gaps or overlaps with parents.
        //
        // We maintain a span stack that is sampled from to select the "parent" span
        // for the child span being generated. The stack has a maximum depth of `MAX_TRACE_DEPTH`
        // to avoid recursing too deeply.
        let mut parent_candidates = vec![vec![0]];
        for _ in 1..span_count {
            let parent_span_level = rng.random_range(0..parent_candidates.len());
            let parent_span_idx = parent_candidates[parent_span_level]
                .choose(rng)
                .copied()
                .ok_or(Error::StringGenerate)?;
            let parent_span = &spans[parent_span_idx];
            let parent_id = parent_span.span_id;

            // Child spans get independent random timing, not constrained by parent.
            // Each span gets its own random start time and duration that satisfy trace-agent bounds.
            let span_start_nanos = rng.random_range(YEAR_2000_NANOS..=i64::MAX);
            let max_duration = i64::MAX - span_start_nanos;
            let span_duration_nanos = rng.random_range(0..=max_duration);

            let span_id = rng.random();
            let span = self.create_span(
                rng,
                service,
                operation,
                resource,
                span_type,
                trace_id,
                span_id,
                parent_id,
                span_start_nanos,
                span_duration_nanos,
            );
            spans.push(span);

            // Add this new child span as a potential parent for future spans.
            //
            // Figure out what "level" this child span is at, and as long as we're not yet at the maximum depth,
            // add it to the appropriate level in `parent_candidates` to make it eligible.
            let next_span_level = parent_span_level + 1;
            if next_span_level < MAX_TRACE_DEPTH {
                // Create the level if it doesn't exist.
                if parent_candidates.len() <= next_span_level {
                    parent_candidates.push(Vec::new());
                }
                parent_candidates[next_span_level].push(spans.len() - 1);
            }
        }

        Ok(Trace { spans })
    }
}

impl V04 {
    /// Create a single span with the provided strings
    #[allow(clippy::too_many_arguments)]
    fn create_span<'a, R>(
        &'a self,
        rng: &mut R,
        service: &'a str,
        operation: &'a str,
        resource: &'a str,
        span_type: &'a str,
        trace_id: u64,
        span_id: u64,
        parent_id: u64,
        start_nanos: i64,
        duration_nanos: i64,
    ) -> Span<'a>
    where
        R: Rng + ?Sized,
    {
        let tag_count = self.config.tags_per_span.sample(rng);
        let mut meta = BTreeMap::default();
        for _ in 0..tag_count {
            let key_len = self.config.tag_key_length.sample(rng);
            let value_len = self.config.tag_value_length.sample(rng);

            let key = self.str_pool.of_size(rng, key_len.into()).unwrap_or("");
            let value = self.str_pool.of_size(rng, value_len.into()).unwrap_or("");

            meta.insert(key, value);
        }

        let metric_count = self.config.metrics_per_span.sample(rng);
        let mut metrics = BTreeMap::default();
        for _ in 0..metric_count {
            let key_len = self.config.metric_key_length.sample(rng);
            let key = self.str_pool.of_size(rng, key_len.into()).unwrap_or("");
            metrics.insert(key, rng.random::<f64>());
        }

        let error = i32::from(rng.random::<f32>() < self.config.error_rate);

        Span {
            service,
            name: operation,
            resource,
            trace_id,
            span_id,
            parent_id,
            start: start_nanos,
            duration: duration_nanos,
            error,
            meta,
            metrics,
            kind: span_type,
            meta_struct: BTreeMap::default(),
        }
    }
}

impl crate::Serialize for V04 {
    fn to_bytes<W, R>(&mut self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        if max_bytes == 0 {
            return Ok(());
        }

        let mut traces: Vec<Vec<Span>> = vec![];
        let mut buf = Vec::with_capacity(max_bytes);

        // Elide the cost of per-message serialization, batching in fixed size
        // chunks.
        let batch_size = 10;
        for _ in 0..batch_size {
            let trace = self.generate(&mut rng)?;
            traces.push(trace.spans);
        }
        loop {
            buf.clear();
            traces.serialize(&mut Serializer::new(&mut buf).with_struct_map())?;

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
            buf.clear();
            traces[0..mid].serialize(&mut Serializer::new(&mut buf).with_struct_map())?;

            if buf.len() <= max_bytes {
                low = mid;
            } else {
                high = mid - 1;
            }
        }

        buf.clear();
        traces[0..low].serialize(&mut Serializer::new(&mut buf).with_struct_map())?;
        writer.write_all(&buf)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use proptest::prelude::*;
    use rand::{SeedableRng, rngs::SmallRng};
    use serde::Serialize;

    use super::{ConfRange, Config, V04};
    use crate::Generator;

    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut generator = V04::with_config(Config::default(), &mut rng)?;

            let mut bytes = Vec::with_capacity(max_bytes);
            crate::Serialize::to_bytes(&mut generator, rng, max_bytes, &mut bytes)?;
            debug_assert!(bytes.len() <= max_bytes);
        }

        /// Property: Spans within a trace share the same trace-id.
        #[test]
        fn trace_spans_have_same_trace_id(seed: u64) {
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

        /// Property: All other spans within a trace have parent_id pointing to
        /// another span within the trace.
        #[test]
        fn parent_child_structure_validity(seed: u64) {
            let mut rng = SmallRng::seed_from_u64(seed);
            let generator = V04::with_config(Config::default(), &mut rng)?;

            let trace = generator.generate(&mut rng)?;

            for span in &trace.spans {
                if span.parent_id != 0 {
                    prop_assert!(trace.spans.iter().any(|s| s.span_id == span.parent_id),
                        "Non-root spans must have parent_id pointing to another span in the trace");
                }
            }
        }

        #[test]
        fn config_validation_works(
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

        /// Property: Spans within a trace have the same service name.
        #[test]
        fn trace_spans_share_same_service(seed: u64) {
            let mut rng = SmallRng::seed_from_u64(seed);
            let generator = V04::with_config(Config::default(), &mut rng)?;

            let trace = generator.generate(&mut rng)?;
            if let Some(first_span) = trace.spans.first() {
                let expected_service = first_span.service;
                for span in &trace.spans {
                    prop_assert_eq!(span.service, expected_service,
                        "All spans in a trace must share the same service name");
                }
            }
        }

        /// Property: Exactly one span has parent_id = 0: the root span.
        #[test]
        fn exactly_one_root_span(seed: u64) {
            let mut rng = SmallRng::seed_from_u64(seed);
            let generator = V04::with_config(Config::default(), &mut rng)?;

            let trace = generator.generate(&mut rng)?;
            let root_span_count = trace.spans.iter().filter(|span| span.parent_id == 0).count();
            prop_assert_eq!(root_span_count, 1, "Trace must have exactly one root span (parent_id = 0)");
        }

        /// Property: Each span has a unique span_id within the trace.
        #[test]
        fn unique_span_ids_within_trace(seed: u64) {
            let mut rng = SmallRng::seed_from_u64(seed);
            let generator = V04::with_config(Config::default(), &mut rng)?;

            let trace = generator.generate(&mut rng)?;
            let mut span_ids = std::collections::HashSet::new();
            for span in &trace.spans {
                prop_assert!(span_ids.insert(span.span_id), "Each span must have a unique span_id within the trace");
            }
        }

        /// Property: Context are bounded.
        #[test]
        fn contexts_are_bounded(seed: u64, contexts in 1u32..1_000, total_traces in 1u32..1_000) {
            let mut rng = SmallRng::seed_from_u64(seed);

            let mut config = Config::default();
            config.contexts = ConfRange::Constant(contexts);

            let generator = V04::with_config(config, &mut rng)?;

            let mut combinations = HashSet::new();
            for _ in 0..total_traces {
                let trace = generator.generate(&mut rng)?;
                for span in &trace.spans {
                    let combination = (span.service, span.name, span.resource, span.kind);
                    combinations.insert(combination);
                }
            }

            prop_assert!(combinations.len() <= contexts as usize,
                "Found {} unique contexts, expected <= {contexts}",
                combinations.len());
        }
    }

    #[test]
    fn config_validation_catches_invalid_error_rate() {
        let mut config = Config::default();
        config.error_rate = 1.5; // Invalid
        assert!(config.valid().is_err());

        config.error_rate = -0.1; // Invalid
        assert!(config.valid().is_err());
    }

    #[test]
    fn config_validation_catches_too_many_contexts() {
        use crate::common::config::ConfRange;

        let mut config = Config::default();
        config.contexts = ConfRange::Constant(super::MAX_CONTEXTS + 1);
        assert_eq!(config.valid(), Err(super::ConfigError::TooManyContexts));

        config.contexts = ConfRange::Inclusive {
            min: 1,
            max: super::MAX_CONTEXTS + 100,
        };
        assert_eq!(config.valid(), Err(super::ConfigError::TooManyContexts));
    }

    #[test]
    fn config_validation_catches_zero_contexts() {
        let mut config = Config::default();
        config.contexts = ConfRange::Constant(0);
        assert!(config.valid().is_err());
    }

    // Test to enforce size limits on in-memory representations, making sure we
    // fit well under MAX_CONTEXTS without OOM'ing.
    #[test]
    fn template_memory_usage_measurement() {
        let mut rng = SmallRng::seed_from_u64(42);

        let template_size = std::mem::size_of::<super::TraceTemplate>();
        assert_eq!(template_size, 64);

        let mut small_config = Config::default();
        small_config.contexts = ConfRange::Constant(1000);
        let small_generator = V04::with_config(small_config, &mut rng).unwrap();
        assert_eq!(small_generator.templates.len(), 1000);

        let mut large_config = Config::default();
        large_config.contexts = ConfRange::Constant(100_000);
        let large_generator = V04::with_config(large_config, &mut rng).unwrap();
        assert_eq!(large_generator.templates.len(), 100_000);

        let trace = large_generator.generate(&mut rng).unwrap();
        assert!(!trace.spans.is_empty());
        assert!(trace.spans.len() <= 50); // max from default config

        let mut serialized = Vec::new();
        let traces = vec![trace.spans];
        traces
            .serialize(&mut rmp_serde::Serializer::new(&mut serialized).with_struct_map())
            .unwrap();

        // NOTE this assertion must be exact. As the payload is updated this
        // value will need to be modified, but keeping it exact allows us to set
        // accurate bounds on memory consumption. Do not make this an
        // inequality.
        assert_eq!(serialized.len(), 21_075);
    }
}
