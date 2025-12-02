//! OpenTelemetry OTLP trace payload.
//!
//! [Specification](https://opentelemetry.io/docs/reference/specification/protocol/otlp/)
//!
//! This format is valid for OTLP/gRPC and binary OTLP/HTTP messages. The
//! experimental JSON OTLP/HTTP format can also be supported but is not
//! currently implemented.

use crate::{
    Error,
    common::{config::ConfRange, strings},
};
use opentelemetry_proto::tonic::{
    collector::trace,
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value::Value},
    trace::v1::Status,
};
use opentelemetry_proto::tonic::{resource::v1::Resource, trace::v1};
use prost::Message;
use rand::{Rng, seq::IndexedRandom};
use std::{io::Write, rc::Rc};

use crate::Generator;

const STRING_POOL_SIZE: usize = 1_000_000;
const MAX_TRACE_DEPTH: usize = 8;
const MAX_CONTEXTS: u32 = 1_000_000;
const MAX_TRACE_DURATION_NS: u64 = 30 * 1_000_000_000; // 30 seconds

/// Minimum valid start timestamp (Year 2000 in nanoseconds since `UNIX_EPOCH`).
///
/// While this isn't a limitation in OTLP, we do this for practical interoperation with the Datadog Agent and related
/// components, which _do_ have a minimum timestamp requirement.
const YEAR_2000_NANOS: u64 = 946_684_800_000_000_000;

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

/// Configuration for OpenTelemetry trace payload generation.
///
/// Focuses on parameters that affect scaling performance: cardinality (service variety), payload structure (spans per
/// trace), and string variety (tags/metrics per span).
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
    /// Probability of a span having an error.
    pub error_rate: f32,
    /// Length range for service names
    pub service_name_length: ConfRange<u8>,
    /// Length range for operation names
    pub operation_name_length: ConfRange<u8>,
    /// Length range for scope names
    pub scope_name_length: ConfRange<u8>,
    /// Length range for tag keys
    pub tag_key_length: ConfRange<u8>,
    /// Length range for tag values
    pub tag_value_length: ConfRange<u8>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            // All defaults arbitrarily chosen to be "large, but not too large". A nice way of saying these values are
            // all guesses.
            contexts: ConfRange::Inclusive {
                min: 5_000,
                max: 10_000,
            },
            spans_per_trace: ConfRange::Inclusive { min: 1, max: 48 },
            tags_per_span: ConfRange::Inclusive { min: 2, max: 8 },
            error_rate: 0.01, // 1% error rate
            service_name_length: ConfRange::Inclusive { min: 0, max: 16 },
            operation_name_length: ConfRange::Inclusive { min: 0, max: 32 },
            scope_name_length: ConfRange::Inclusive { min: 0, max: 32 },
            tag_key_length: ConfRange::Inclusive { min: 0, max: 24 },
            tag_value_length: ConfRange::Inclusive { min: 0, max: 32 },
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

        if !self.error_rate.is_finite() || !(0.0..=1.0).contains(&self.error_rate) {
            return Err(ConfigError::InvalidErrorRate);
        }

        let ranges = [
            &self.service_name_length,
            &self.operation_name_length,
            &self.scope_name_length,
            &self.tag_key_length,
            &self.tag_value_length,
        ];

        for range in ranges {
            if !range.valid().0 {
                return Err(ConfigError::InvalidRange);
            }
        }

        Ok(())
    }
}

/// A template for baseline attributes used in trace generation.
///
/// All spans in a trace will be generated from the common attributes defined in the template
/// where relevant.
#[derive(Debug, Clone)]
struct TraceTemplate {
    resource: Resource,
    scope: InstrumentationScope,
}

impl TraceTemplate {
    fn from_rng<R>(
        str_pool: &Rc<strings::Pool>,
        rng: &mut R,
        config: &Config,
    ) -> Result<Self, Error>
    where
        R: Rng + ?Sized,
    {
        let service_len = config.service_name_length.sample(rng);
        let scope_len = config.scope_name_length.sample(rng);

        let service_name = str_pool
            .of_size(rng, service_len.into())
            .ok_or(Error::StringGenerate)?;
        let scope_name = str_pool
            .of_size(rng, scope_len.into())
            .ok_or(Error::StringGenerate)?;

        let resource = Resource {
            attributes: vec![KeyValue {
                key: "service.name".into(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(service_name.to_string())),
                }),
            }],
            ..Default::default()
        };

        let scope = InstrumentationScope {
            name: scope_name.to_string(),
            version: "1.0.0".to_string(),
            ..Default::default()
        };

        Ok(Self { resource, scope })
    }

    fn resource(&self) -> Resource {
        self.resource.clone()
    }

    fn scope(&self) -> InstrumentationScope {
        self.scope.clone()
    }
}

/// An OpenTelemetry "trace".
///
/// This is a human-friendly representation of a trace, which is a collection of spans with the same `trace_id`. OpenTelemetry
/// doesn't explicitly represent traces in the protocol, but we do so here for convenience.
#[derive(Debug)]
pub struct Trace {
    resource: Resource,
    scope: InstrumentationScope,
    spans: Vec<v1::Span>,
}

impl Trace {
    fn into_export_request(self, max_bytes: usize) -> trace::v1::ExportTraceServiceRequest {
        let mut request = trace::v1::ExportTraceServiceRequest {
            resource_spans: [v1::ResourceSpans {
                resource: Some(self.resource),
                scope_spans: [v1::ScopeSpans {
                    scope: Some(self.scope),
                    spans: self.spans,
                    schema_url: String::new(),
                }]
                .to_vec(),
                schema_url: String::new(),
            }]
            .to_vec(),
        };

        // Given the maximum byte size allowed for this trace when encoded, incrementally remove spans
        // from the end of `spans` until the total size is less than or equal to `max_bytes`.
        while request.encoded_len() > max_bytes {
            let inner_spans = &mut request.resource_spans[0].scope_spans[0].spans;

            inner_spans.pop();

            // Break out of the loop if we have no more spans left, since we can't remove any more.
            if inner_spans.is_empty() {
                break;
            }
        }

        request
    }
}

#[derive(Debug, Clone)]
/// OpenTelemetry trace generator
pub struct OpentelemetryTraces {
    str_pool: Rc<strings::Pool>,
    config: Config,
    templates: Vec<TraceTemplate>,
}

impl OpentelemetryTraces {
    /// Create a new OpenTelemetry trace generator with provided configuration
    ///
    /// # Errors
    ///
    /// Returns error if string generation from the string pool fails
    pub fn with_config<R>(config: Config, rng: &mut R) -> Result<Self, Error>
    where
        R: Rng + ?Sized,
    {
        let str_pool = Rc::new(strings::Pool::with_size(rng, STRING_POOL_SIZE));

        let num_contexts = config.contexts.sample(rng) as usize;
        let mut templates = Vec::with_capacity(num_contexts);

        for _ in 0..num_contexts {
            let template = TraceTemplate::from_rng(&str_pool, rng, &config)?;
            templates.push(template);
        }

        Ok(Self {
            str_pool,
            config,
            templates,
        })
    }

    fn create_span<R>(
        &self,
        rng: &mut R,
        trace_id: [u8; 16],
        span_id: [u8; 8],
        parent_span: Option<&v1::Span>,
    ) -> Result<v1::Span, Error>
    where
        R: Rng + ?Sized,
    {
        let operation_name_len = self.config.operation_name_length.sample(rng);
        let operation_name = self
            .str_pool
            .of_size(rng, operation_name_len.into())
            .ok_or(Error::StringGenerate)?;

        let (start_timestamp_ns, end_timestamp_ns) = match parent_span.as_ref() {
            // We still randomly generate timestamps for child spans, but bounded by the parent's start and end
            // timestamps.
            Some(parent) => {
                let parent_start = parent.start_time_unix_nano;
                let parent_end = parent.end_time_unix_nano;
                let child_start = rng.random_range(parent_start..=parent_end);
                let child_end = rng.random_range(child_start..=parent_end);

                (child_start, child_end)
            }

            // Root span, so randomly generate timestamps out of the maximum possible range.
            None => get_safe_start_end_timestamps(rng),
        };

        let tag_count = self.config.tags_per_span.sample(rng);
        let mut attributes = Vec::with_capacity(tag_count as usize);
        for _ in 0..tag_count {
            let key_len = self.config.tag_key_length.sample(rng);
            let value_len = self.config.tag_value_length.sample(rng);

            let key = self.str_pool.of_size(rng, key_len.into()).unwrap_or("");
            let value = self.str_pool.of_size(rng, value_len.into()).unwrap_or("");

            attributes.push(str_kv(key, value));
        }

        let is_error_status = rng.random::<f32>() < self.config.error_rate;

        Ok(v1::Span {
            trace_id: trace_id.to_vec(),
            span_id: span_id.to_vec(),
            parent_span_id: parent_span.map_or(vec![], |parent| parent.span_id.clone()),
            start_time_unix_nano: start_timestamp_ns,
            end_time_unix_nano: end_timestamp_ns,
            name: operation_name.to_string(),
            kind: rng.random_range(0..=5),
            attributes,
            status: is_error_status.then_some(Status {
                code: 2,
                ..Default::default()
            }),
            ..Default::default()
        })
    }
}

impl<'a> Generator<'a> for OpentelemetryTraces {
    type Output = Trace;
    type Error = Error;

    fn generate<R>(&'a self, rng: &mut R) -> Result<Self::Output, Error>
    where
        R: rand::Rng + ?Sized,
    {
        let template_index = rng.random_range(0..self.templates.len());
        let template = &self.templates[template_index];

        let trace_id = rng.random();
        let span_count = self.config.spans_per_trace.sample(rng);
        let mut spans = Vec::with_capacity(span_count as usize);

        // Generate our root span.
        let root_span_id = rng.random();
        let root_span = self.create_span(rng, trace_id, root_span_id, None)?;
        spans.push(root_span);

        // Generate our child spans.
        //
        // We maintain a span stack that is sampled from to select the "parent" span for the child span being generated.
        // The stack has a maximum depth of `MAX_TRACE_DEPTH` to avoid recursing too deeply.
        let mut parent_candidates = vec![vec![0]];
        for _ in 1..span_count {
            let parent_span_level = rng.random_range(0..parent_candidates.len());
            let parent_span_idx = parent_candidates[parent_span_level]
                .choose(rng)
                .copied()
                .ok_or(Error::StringGenerate)?;
            let parent_span = &spans[parent_span_idx];

            let child_span_id = rng.random();
            let child_span = self.create_span(rng, trace_id, child_span_id, Some(parent_span))?;
            spans.push(child_span);

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

        Ok(Trace {
            resource: template.resource(),
            scope: template.scope(),
            spans,
        })
    }
}

impl crate::Serialize for OpentelemetryTraces {
    fn to_bytes<W, R>(&mut self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        let trace = self.generate(&mut rng)?;

        // `into_export_request` will try to slim down the number of spans in the trace in order to fit
        // within the maximum byte limit, but it may exceed the limit just from non-span fields, so we
        // do a final check to see if the payload fits within the limit... and if it doesn't, we avoid
        // writing it to the writer.
        let buf = trace.into_export_request(max_bytes).encode_to_vec();
        if buf.len() <= max_bytes {
            writer.write_all(&buf)?;
        }

        Ok(())
    }
}

fn get_safe_start_end_timestamps<R>(rng: &mut R) -> (u64, u64)
where
    R: Rng + ?Sized,
{
    // We generate a random start timestamp, and duration, to satisfies some basic constraints:
    //
    // 1. Must be >= Year 2000 expressed in nanoseconds (946684800000000000)
    // 2. Must be <= i64::MAX to avoid overflow in Go
    // 3. Duration must not overflow i64::MAX when added to the start timestamp
    //
    // We do this to make these traces/spans interoperable with the Datadog Agent.
    let start_timestamp_ns = rng.random_range(YEAR_2000_NANOS..=i64::MAX as u64);
    let max_duration_ns = i64::MAX as u64 - start_timestamp_ns;
    let duration_upper_bound = std::cmp::min(max_duration_ns, MAX_TRACE_DURATION_NS);
    let duration_ns = rng.random_range(0..=duration_upper_bound);

    (start_timestamp_ns, start_timestamp_ns + duration_ns)
}

fn str_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }),
    }
}

#[cfg(test)]
mod test {
    use super::{Config, OpentelemetryTraces};
    use crate::Serialize;
    use opentelemetry_proto::tonic::collector::trace;
    use proptest::prelude::*;
    use prost::Message;
    use rand::{SeedableRng, rngs::SmallRng};

    // We want to be sure that the serialized size of the payload does not exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut traces = OpentelemetryTraces::with_config(Config::default(), &mut rng)?;

            let mut bytes = Vec::with_capacity(max_bytes);
            traces.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");
            assert!(bytes.len() <= max_bytes, "max len: {max_bytes}, actual: {}", bytes.len());
        }
    }

    // We want to know that every payload produced by this type actually deserializes as a collection of OTEL Spans.
    proptest! {
        #[test]
        fn payload_deserializes(seed: u64, max_bytes: u16)  {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut traces = OpentelemetryTraces::with_config(Config::default(), &mut rng)?;

            let mut bytes: Vec<u8> = Vec::with_capacity(max_bytes);
            traces.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");

            trace::v1::ExportTraceServiceRequest::decode(bytes.as_slice()).expect("failed to decode the message from the buffer");
        }
    }
}
