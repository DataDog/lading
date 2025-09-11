//! Datadog Trace Agent v0.4 endpoint payload implementation.
//!
//! This module generates payloads compatible with the `/v0.4/traces` endpoint
//! as specified in `pkg/trace/api/version.go:22-57` of the datadog-agent.

use std::io::Write;

use rand::{Rng, seq::IndexedRandom};
use rmp_serde::Serializer;
use rustc_hash::FxHashMap;
use serde::Serialize;

use crate::{
    Error, Generator,
    common::{config::ConfRange, strings},
};

/// Configuration for v0.4 trace payload generation
#[derive(Debug, serde::Deserialize, serde::Serialize, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Number of traces per payload
    pub traces_per_payload: ConfRange<u16>,
    /// Number of spans per trace
    pub spans_per_trace: ConfRange<u16>,
    /// Maximum depth of span hierarchy
    pub trace_depth: u8,
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
    /// Length range for metric keys
    pub metric_key_length: ConfRange<u8>,
    /// Number of tags per span
    pub tags_per_span: ConfRange<u8>,
    /// Size of pre-generated tag key pool
    pub tag_key_pool_size: u32,
    /// Size of pre-generated tag value pool
    pub tag_value_pool_size: u32,
    /// Number of metrics per span
    pub metrics_per_span: ConfRange<u8>,
    /// Size of pre-generated metric key pool
    pub metric_key_pool_size: u32,
    /// Probability of a span having an error (0.0-1.0)
    pub error_rate: f32,
    /// Size of the string pool for dynamic strings
    pub string_pool_size: u32,
    /// Number of service names to pre-generate
    pub service_name_pool_size: u32,
    /// Number of operation names to pre-generate
    pub operation_name_pool_size: u32,
    /// Number of resource names to pre-generate
    pub resource_name_pool_size: u32,
    /// Number of span types to pre-generate
    pub span_type_pool_size: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            traces_per_payload: ConfRange::Inclusive { min: 1, max: 10 },
            spans_per_trace: ConfRange::Inclusive { min: 1, max: 20 },
            trace_depth: 3,
            service_name_length: ConfRange::Inclusive { min: 4, max: 16 },
            operation_name_length: ConfRange::Inclusive { min: 6, max: 20 },
            resource_name_length: ConfRange::Inclusive { min: 8, max: 64 },
            span_type_length: ConfRange::Inclusive { min: 2, max: 8 },
            tag_key_length: ConfRange::Inclusive { min: 3, max: 24 },
            metric_key_length: ConfRange::Inclusive { min: 4, max: 32 },
            tags_per_span: ConfRange::Inclusive { min: 0, max: 10 },
            tag_key_pool_size: 100,
            tag_value_pool_size: 1000,
            metrics_per_span: ConfRange::Inclusive { min: 0, max: 5 },
            metric_key_pool_size: 50,
            error_rate: 0.05, // 5% error rate
            string_pool_size: 1_000_000,
            service_name_pool_size: 50,
            operation_name_pool_size: 100,
            resource_name_pool_size: 200,
            span_type_pool_size: 20,
        }
    }
}

impl Config {
    /// Validate the configuration parameters
    /// # Errors
    /// Returns an error string if configuration is invalid
    pub fn valid(&self) -> Result<(), String> {
        let (traces_valid, reason) = self.traces_per_payload.valid();
        if !traces_valid {
            return Err(format!("traces_per_payload is invalid: {reason}"));
        }
        if self.traces_per_payload.start() == 0 {
            return Err("traces_per_payload start value cannot be 0".to_string());
        }

        let (spans_valid, reason) = self.spans_per_trace.valid();
        if !spans_valid {
            return Err(format!("spans_per_trace is invalid: {reason}"));
        }
        if self.spans_per_trace.start() == 0 {
            return Err("spans_per_trace start value cannot be 0".to_string());
        }

        if self.trace_depth == 0 {
            return Err("trace_depth cannot be 0".to_string());
        }
        if self.trace_depth > 10 {
            return Err("trace_depth cannot be greater than 10".to_string());
        }

        let (service_length_valid, reason) = self.service_name_length.valid();
        if !service_length_valid {
            return Err(format!("service_name_length is invalid: {reason}"));
        }
        if self.service_name_length.start() == 0 {
            return Err("service_name_length start value cannot be 0".to_string());
        }

        let (operation_length_valid, reason) = self.operation_name_length.valid();
        if !operation_length_valid {
            return Err(format!("operation_name_length is invalid: {reason}"));
        }
        if self.operation_name_length.start() == 0 {
            return Err("operation_name_length start value cannot be 0".to_string());
        }

        let (resource_length_valid, reason) = self.resource_name_length.valid();
        if !resource_length_valid {
            return Err(format!("resource_name_length is invalid: {reason}"));
        }
        if self.resource_name_length.start() == 0 {
            return Err("resource_name_length start value cannot be 0".to_string());
        }

        let (span_type_length_valid, reason) = self.span_type_length.valid();
        if !span_type_length_valid {
            return Err(format!("span_type_length is invalid: {reason}"));
        }
        if self.span_type_length.start() == 0 {
            return Err("span_type_length start value cannot be 0".to_string());
        }

        let (tag_key_length_valid, reason) = self.tag_key_length.valid();
        if !tag_key_length_valid {
            return Err(format!("tag_key_length is invalid: {reason}"));
        }
        if self.tag_key_length.start() == 0 {
            return Err("tag_key_length start value cannot be 0".to_string());
        }

        let (metric_key_length_valid, reason) = self.metric_key_length.valid();
        if !metric_key_length_valid {
            return Err(format!("metric_key_length is invalid: {reason}"));
        }
        if self.metric_key_length.start() == 0 {
            return Err("metric_key_length start value cannot be 0".to_string());
        }

        let (tags_valid, reason) = self.tags_per_span.valid();
        if !tags_valid {
            return Err(format!("tags_per_span is invalid: {reason}"));
        }

        let (metrics_valid, reason) = self.metrics_per_span.valid();
        if !metrics_valid {
            return Err(format!("metrics_per_span is invalid: {reason}"));
        }

        if self.metric_key_pool_size == 0 {
            return Err("metric_key_pool_size cannot be 0".to_string());
        }

        if !self.error_rate.is_finite() || !(0.0..=1.0).contains(&self.error_rate) {
            return Err(format!(
                "error_rate must be finite and in range [0.0, 1.0], got {}",
                self.error_rate
            ));
        }

        if self.tag_key_pool_size == 0 {
            return Err("tag_key_pool_size cannot be 0".to_string());
        }
        if self.tag_value_pool_size == 0 {
            return Err("tag_value_pool_size cannot be 0".to_string());
        }
        if self.string_pool_size == 0 {
            return Err("string_pool_size cannot be 0".to_string());
        }
        if self.service_name_pool_size == 0 {
            return Err("service_name_pool_size cannot be 0".to_string());
        }
        if self.operation_name_pool_size == 0 {
            return Err("operation_name_pool_size cannot be 0".to_string());
        }
        if self.resource_name_pool_size == 0 {
            return Err("resource_name_pool_size cannot be 0".to_string());
        }
        if self.span_type_pool_size == 0 {
            return Err("span_type_pool_size cannot be 0".to_string());
        }

        Ok(())
    }
}

/// V0.4 span structure matching protobuf definition with MessagePack encoding.
///
/// This struct corresponds exactly to the Span message in pkg/proto/datadog/trace/span.proto
/// (lines 101-147). The protobuf uses `@gotags: msg:"field_name"` annotations to generate
/// MessagePack serialization methods via github.com/tinylib/msgp.
///
/// Our serde-based serialization produces the same MessagePack format that the datadog-agent
/// expects when calling pb.Traces.UnmarshalMsg() in pkg/trace/api/api.go:897.
#[derive(Debug, serde::Serialize)]
#[allow(clippy::struct_field_names)]
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

/// String pools for v0.4 payload generation following lading's pre-computation principle.
#[derive(Debug, Clone)]
struct StringPools {
    /// Pool for dynamic string generation
    dynamic: strings::Pool,
    /// Pre-generated service names
    service_names: Vec<String>,
    /// Pre-generated operation names
    operation_names: Vec<String>,
    /// Pre-generated resource names  
    resource_names: Vec<String>,
    /// Pre-generated span types
    span_types: Vec<String>,
    /// Pre-generated tag keys
    tag_keys: Vec<String>,
    /// Pre-generated tag values
    tag_values: Vec<String>,
    /// Pre-generated metric keys
    metric_keys: Vec<String>,
}

impl StringPools {
    fn new<R>(config: &Config, rng: &mut R) -> Self
    where
        R: Rng + ?Sized,
    {
        let dynamic = strings::Pool::with_size(rng, config.string_pool_size as usize);
        
        // Pre-generate service names
        let mut service_names = Vec::new();
        for _ in 0..config.service_name_pool_size {
            let length = config.service_name_length.sample(rng);
            if let Some(name) = dynamic.of_size_range(rng, length..length+1) {
                service_names.push(name.to_string());
            }
        }

        // Pre-generate operation names
        let mut operation_names = Vec::new();
        for _ in 0..config.operation_name_pool_size {
            let length = config.operation_name_length.sample(rng);
            if let Some(name) = dynamic.of_size_range(rng, length..length+1) {
                operation_names.push(name.to_string());
            }
        }

        // Pre-generate resource names
        let mut resource_names = Vec::new();
        for _ in 0..config.resource_name_pool_size {
            let length = config.resource_name_length.sample(rng);
            if let Some(name) = dynamic.of_size_range(rng, length..length+1) {
                resource_names.push(name.to_string());
            }
        }

        // Pre-generate span types
        let mut span_types = Vec::new();
        for _ in 0..config.span_type_pool_size {
            let length = config.span_type_length.sample(rng);
            if let Some(span_type) = dynamic.of_size_range(rng, length..length+1) {
                span_types.push(span_type.to_string());
            }
        }

        // Pre-generate tag keys
        let mut tag_keys = Vec::new();
        for _ in 0..config.tag_key_pool_size {
            let length = config.tag_key_length.sample(rng);
            if let Some(key) = dynamic.of_size_range(rng, length..length+1) {
                tag_keys.push(key.to_string());
            }
        }

        // Pre-generate tag values
        let mut tag_values = Vec::new();
        for _ in 0..config.tag_value_pool_size {
            if let Some(value) = dynamic.of_size_range(rng, 1_u8..32) {
                tag_values.push(value.to_string());
            }
        }

        // Pre-generate metric keys
        let mut metric_keys = Vec::new();
        for _ in 0..config.metric_key_pool_size {
            let length = config.metric_key_length.sample(rng);
            if let Some(key) = dynamic.of_size_range(rng, length..length+1) {
                metric_keys.push(key.to_string());
            }
        }

        Self {
            dynamic,
            service_names,
            operation_names,
            resource_names,
            span_types,
            tag_keys,
            tag_values,
            metric_keys,
        }
    }
}

/// Represents a single trace (array of spans with same trace_id)
#[derive(Debug)]
pub struct Trace<'a> {
    /// The spans that make up this trace
    pub spans: Vec<Span<'a>>,
}

/// V0.4 payload generator
#[derive(Debug, Clone)]
pub struct V04 {
    config: Config,
    pools: StringPools,
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
        let pools = StringPools::new(&config, rng);
        Self { config, pools }
    }

    /// Generate a complete trace with proper parent-child relationships.
    ///
    /// Follows the trace generation pattern from pkg/trace/testutil/trace.go:74-87
    /// but uses dynamic string pools for performance and configurability.
    pub fn generate_trace<R>(&self, rng: &mut R) -> Result<Trace<'_>, Error>
    where
        R: Rng + ?Sized,
    {
        let trace_id = rng.random();
        let span_count = self.config.spans_per_trace.sample(rng);
        let mut spans = Vec::with_capacity(span_count as usize);

        // Generate base timestamp (within last hour)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        let base_time = now - rng.random_range(0..3_600_000_000_000i64);
        
        // Generate root span
        let root_span_id = rng.random();
        let root_duration = rng.random_range(1_000_000i64..1_000_000_000); // 1ms to 1s
        let root_span = generate_span(
            self,
            rng,
            trace_id,
            root_span_id,
            0, // Root span has no parent
            base_time,
            root_duration,
        )?;
        spans.push(root_span);
        
        // Generate child spans with proper parent-child relationships
        let mut parent_candidates = vec![(root_span_id, base_time, base_time + root_duration)];
        
        for _ in 1..span_count {
            let (parent_id, parent_start, parent_end) = parent_candidates
                .choose(rng)
                .copied()
                .unwrap_or((root_span_id, base_time, base_time + root_duration));
            
            // Child span must start after parent and end before parent
            let span_start = rng.random_range(parent_start..parent_end.saturating_sub(1_000_000));
            let max_duration = parent_end - span_start;
            let span_duration = rng.random_range(1_000_000..=max_duration.max(1_000_000));
            
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
            
            // Add this span as potential parent if we haven't reached max depth
            if spans.len() < self.config.trace_depth as usize {
                parent_candidates.push((span_id, span_start, span_start + span_duration));
            }

            spans.push(span);
        }

        Ok(Trace { spans })
    }
}

/// Generate a single span using string pools.
///
/// Creates spans compatible with the protobuf Span message but uses pre-generated
/// string pools instead of static values for optimal performance.
fn generate_span<'a, R>(
    generator: &'a V04,
    rng: &mut R,
    trace_id: u64,
    span_id: u64,
    parent_id: u64,
    start_time: i64,
    duration: i64,
) -> Result<Span<'a>, Error>
where
    R: Rng + ?Sized,
{

    let service = generator
        .pools
        .service_names
        .choose(rng)
        .expect("service_names should not be empty");

    let name = generator
        .pools
        .operation_names
        .choose(rng)
        .expect("operation_names should not be empty");

    let resource = generator
        .pools
        .resource_names
        .choose(rng)
        .expect("resource_names should not be empty");

    let kind = generator
        .pools
        .span_types
        .choose(rng)
        .expect("span_types should not be empty");

    // Generate tags (meta)
    let tag_count = generator.config.tags_per_span.sample(rng);
    let mut meta = FxHashMap::default();
    for _ in 0..tag_count {
        if let (Some(key), Some(value)) = (
            generator.pools.tag_keys.choose(rng),
            generator.pools.tag_values.choose(rng),
        ) {
            meta.insert(key.as_str(), value.as_str());
        }
    }

    // Generate metrics
    let metric_count = generator.config.metrics_per_span.sample(rng);
    let mut metrics = FxHashMap::default();
    for _ in 0..metric_count {
        if let Some(key) = generator.pools.metric_keys.choose(rng) {
            metrics.insert(key.as_str(), rng.random::<f64>());
        }
    }

    // Determine if this span should have an error
    let error = if rng.random::<f32>() < generator.config.error_rate {
        1
    } else {
        0
    };

    Ok(Span {
        service,
        name,
        resource,
        trace_id,
        span_id,
        parent_id,
        start: start_time,
        duration,
        error,
        meta,
        metrics,
        kind,
        meta_struct: FxHashMap::default(),
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
    /// Serialize traces in v0.4 format per official specification.
    ///
    /// OFFICIAL V0.4 SPEC (pkg/trace/api/version.go:24-26):
    /// "An array of arrays of Span (pkg/proto/datadog/trace/span.proto)"
    ///
    /// This generates `Vec<Vec<Span>>` serialized as MessagePack, which is exactly
    /// what the v0.4 endpoint expects when calling pb.Traces.UnmarshalMsg().
    fn to_bytes<W, R>(&mut self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        // Generate traces (Vec<Vec<Span>>) matching pb.Traces format
        let mut traces: Vec<Vec<Span>> = vec![];

        // Start by generating the configured number of traces
        let initial_trace_count = self.config.traces_per_payload.sample(&mut rng);
        for _ in 0..initial_trace_count {
            let trace = self.generate(&mut rng)?;
            traces.push(trace.spans);
        }

        // Keep adding traces until we exceed max_bytes
        loop {
            let mut buf = Vec::with_capacity(max_bytes);
            traces.serialize(&mut Serializer::new(&mut buf))?;

            if buf.len() > max_bytes {
                break;
            }

            // Add more traces
            let additional_traces = self.config.traces_per_payload.sample(&mut rng).min(100);
            for _ in 0..additional_traces {
                let trace = self.generate(&mut rng)?;
                traces.push(trace.spans);
            }
        }

        // Binary search to find the right number of traces that fit in max_bytes
        let mut high = traces.len();
        let mut low = 0;

        while low < high {
            let mid = (low + high + 1) / 2;
            let mut buf = Vec::with_capacity(max_bytes);
            traces[0..mid].serialize(&mut Serializer::new(&mut buf))?;

            if buf.len() <= max_bytes {
                low = mid;
            } else {
                high = mid - 1;
            }
        }

        // Serialize the final result
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
    }

    proptest! {
        #[test]
        fn trace_spans_have_same_trace_id_v04(seed: u64) {
            let mut rng = SmallRng::seed_from_u64(seed);
            let generator = V04::new(&mut rng);
            
            let trace = generator.generate(&mut rng)?;
            
            // All spans in a trace should have the same trace_id
            if let Some(first_span) = trace.spans.first() {
                let expected_trace_id = first_span.trace_id;
                for span in &trace.spans {
                    prop_assert_eq!(span.trace_id, expected_trace_id);
                }
            }
        }
    }

    proptest! {
        #[test]
        fn parent_child_timestamp_validity_v04(seed: u64) {
            let mut rng = SmallRng::seed_from_u64(seed);
            let generator = V04::new(&mut rng);
            
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
    }

    proptest! {
        #[test]
        fn config_validation_works_v04(
            traces_per_payload_min in 1u16..=100,
            traces_per_payload_max in 1u16..=100,
            error_rate in 0.0f32..=1.0
        ) {
            let mut config = Config::default();
            config.traces_per_payload = if traces_per_payload_min <= traces_per_payload_max {
                ConfRange::Inclusive { min: traces_per_payload_min, max: traces_per_payload_max }
            } else {
                ConfRange::Inclusive { min: traces_per_payload_max, max: traces_per_payload_min }
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
    fn config_validation_catches_zero_pool_sizes_v04() {
        let mut config = Config::default();
        config.service_name_pool_size = 0;
        assert!(config.valid().is_err());

        let mut config = Config::default();
        config.operation_name_pool_size = 0;
        assert!(config.valid().is_err());

        let mut config = Config::default();
        config.resource_name_pool_size = 0;
        assert!(config.valid().is_err());

        let mut config = Config::default();
        config.span_type_pool_size = 0;
        assert!(config.valid().is_err());

        let mut config = Config::default();
        config.metric_key_pool_size = 0;
        assert!(config.valid().is_err());
    }
}