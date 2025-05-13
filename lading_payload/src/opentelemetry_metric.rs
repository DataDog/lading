//! OpenTelemetry OTLP metric payload.
//!
//! [Specification](https://opentelemetry.io/docs/reference/specification/protocol/otlp/),
//! [data model](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/data-model.md)
//!
//! This format is valid for OTLP/gRPC and binary OTLP/HTTP messages. The
//! experimental JSON OTLP/HTTP format can also be supported but is not
//! currently implemented.

// Alright, to summarize this payload generator's understanding of the
// data-model described above:
//
// The central concern is a `Metric`. A `Metric` is:
//
// * name: String -- unique metric identifier
// * description: String -- optional human friendly description
// * unit: String -- unit of measurement, described by http://unitsofmeasure.org/ucum.html
// * data: enum { Gauge, Sum, Histogram, ExponentialHistogram, Summary }
//
// Each `Metric` is associated with a `Resource` -- called `ResourceMetrics` in
// the proto -- which defines the origination point of the `Metric`. Interior to
// the `Resource` is the `Scope` -- called `ScopeMetrics` in the proto -- which
// defines the library/smaller-than-resource scope that generated the
// `Metric`. We will not set `Scope` until we find out that it's important to do
// so for modeling purposes.
//
// The data types are as follows:
//
// * Gauge -- a collection of `NumberDataPoint`, values sampled at specific times
// * Sum -- `Gauge` with the addition of monotonic flag, aggregation metadata
//
// We omit Histogram / ExponentialHistogram / Summary in this current version
// but will introduce them in the near-term. The `NumberDataPoint` is a
//
// * attributes: Vec<KeyValue> -- tags
// * start_time_unix_nano: u64 -- represents the first possible moment a measurement could be recorded, optional
// * time_unix_nano: u64 -- a timestamp when the value was sampled
// * value: enum { u64, f64 } -- the value
// * flags: uu32 -- I'm not sure what to make of this yet

pub(crate) mod tags;
pub(crate) mod templates;
pub(crate) mod unit;

use std::rc::Rc;
use std::{cell::RefCell, io::Write};

use crate::SizedGenerator;
use crate::{Error, common::config::ConfRange, common::strings};
use bytes::BytesMut;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_proto::tonic::metrics::v1::{self, number_data_point};
use prost::Message;
use serde::{Deserialize, Serialize as SerdeSerialize};
use templates::{Pool, ResourceTemplateGenerator};
use tracing::debug;
use unit::UnitGenerator;

const SMALLEST_PROTOBUF: usize = 64; // smallest useful protobuf, arbitrary low-ish cut-off

/// Configure the OpenTelemetry metric payload.
#[derive(Debug, Deserialize, SerdeSerialize, Clone, PartialEq, Copy)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields, default)]
/// OpenTelemetry metric contexts
pub struct Contexts {
    /// The range of contexts -- see documentation for `context_id` function --
    /// that will be generated.
    pub total_contexts: ConfRange<u32>,

    /// The range of attributes for resources.
    pub attributes_per_resource: ConfRange<u8>,
    /// The range of scopes that will be generated per resource.
    pub scopes_per_resource: ConfRange<u8>,
    /// The range of attributes for each scope.
    pub attributes_per_scope: ConfRange<u8>,
    /// The range of metrics that will be generated per scope.
    pub metrics_per_scope: ConfRange<u8>,
    /// The range of attributes for each metric.
    pub attributes_per_metric: ConfRange<u8>,
}

impl Default for Contexts {
    fn default() -> Self {
        Self {
            total_contexts: ConfRange::Constant(100),
            attributes_per_resource: ConfRange::Inclusive { min: 1, max: 20 },
            scopes_per_resource: ConfRange::Inclusive { min: 1, max: 20 },
            attributes_per_scope: ConfRange::Constant(0),
            metrics_per_scope: ConfRange::Inclusive { min: 1, max: 20 },
            attributes_per_metric: ConfRange::Inclusive { min: 0, max: 10 },
        }
    }
}

/// Defines the relative probability of each kind of OpenTelemetry metric.
#[derive(Debug, Deserialize, SerdeSerialize, Clone, PartialEq, Copy)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields, default)]
pub struct MetricWeights {
    /// The relative probability of generating a gauge metric.
    pub gauge: u8,
    /// The relative probability of generating a sum metric.
    pub sum: u8,
}

impl Default for MetricWeights {
    fn default() -> Self {
        Self {
            gauge: 50, // 50%
            sum: 50,   // 50%
        }
    }
}

/// Configure the OpenTelemetry metric payload.
#[derive(Debug, Default, Deserialize, SerdeSerialize, Clone, PartialEq, Copy)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// Defines the relative probability of each kind of OpenTelemetry metric.
    pub metric_weights: MetricWeights,
    /// Define the contexts available when generating metrics
    pub contexts: Contexts,
}

impl Config {
    /// Determine whether the passed configuration obeys validation criteria.
    ///
    /// # Errors
    /// Function will error if the configuration is invalid
    pub fn valid(&self) -> Result<(), String> {
        // None of the metric weights are 0.
        if self.metric_weights.gauge == 0 || self.metric_weights.sum == 0 {
            return Err("Metric weights cannot be 0".to_string());
        }

        // total_contexts cannot be zero
        match self.contexts.total_contexts {
            ConfRange::Constant(0) => return Err("total_contexts cannot be zero".to_string()),
            ConfRange::Constant(_) => (), // valid case
            ConfRange::Inclusive { min, max } => {
                if min == 0 {
                    return Err("total_contexts minimum cannot be zero".to_string());
                }
                if min > max {
                    return Err("total_contexts minimum cannot be greater than maximum".to_string());
                }
            }
        }

        let min_contexts = match self.contexts.total_contexts {
            ConfRange::Constant(n) => n,
            ConfRange::Inclusive { min, .. } => min,
        };
        let max_contexts = match self.contexts.total_contexts {
            ConfRange::Constant(n) => n,
            ConfRange::Inclusive { max, .. } => max,
        };

        // Calculate minimum and maximum possible contexts based on composition
        let min_configured = match self.contexts.scopes_per_resource {
            ConfRange::Constant(0) => 0u32,
            ConfRange::Constant(n) => {
                let metrics = match self.contexts.metrics_per_scope {
                    ConfRange::Constant(0) => 0u32,
                    ConfRange::Constant(m) => u32::from(m),
                    ConfRange::Inclusive { min, .. } => u32::from(min),
                };
                u32::from(n) * metrics
            }
            ConfRange::Inclusive { min, .. } => {
                let metrics = match self.contexts.metrics_per_scope {
                    ConfRange::Constant(0) => 0u32,
                    ConfRange::Constant(m) => u32::from(m),
                    ConfRange::Inclusive { min, .. } => u32::from(min),
                };
                u32::from(min) * metrics
            }
        };

        let max_configured = match self.contexts.scopes_per_resource {
            ConfRange::Constant(0) => 0u32,
            ConfRange::Constant(n) => {
                let metrics = match self.contexts.metrics_per_scope {
                    ConfRange::Constant(0) => 0u32,
                    ConfRange::Constant(m) => u32::from(m),
                    ConfRange::Inclusive { max, .. } => u32::from(max),
                };
                u32::from(n) * metrics
            }
            ConfRange::Inclusive { max, .. } => {
                let metrics = match self.contexts.metrics_per_scope {
                    ConfRange::Constant(0) => 0u32,
                    ConfRange::Constant(m) => u32::from(m),
                    ConfRange::Inclusive { max, .. } => u32::from(max),
                };
                u32::from(max) * metrics
            }
        };

        // Validate that the requested contexts are achievable
        if min_contexts > max_configured {
            return Err(format!(
                "Minimum requested contexts {min_contexts} cannot be achieved with current configuration (max possible: {max_configured})"
            ));
        }

        if max_contexts < min_configured {
            return Err(format!(
                "Maximum requested contexts {max_contexts} is less than minimum possible contexts {min_configured}"
            ));
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
/// OTLP metric payload
pub struct OpentelemetryMetrics {
    pool: Pool,
    scratch: RefCell<BytesMut>,
}

impl OpentelemetryMetrics {
    /// Construct a new instance of `OpentelemetryMetrics`
    ///
    /// # Errors
    /// Function will error if the configuration is invalid
    pub fn new<R>(config: Config, rng: &mut R) -> Result<Self, Error>
    where
        R: rand::Rng + ?Sized,
    {
        let context_cap = config.contexts.total_contexts.sample(rng);
        // Moby Dick is 1.2Mb. 256Kb should be more than enough for metric
        // names, descriptions, etc.
        let str_pool = Rc::new(strings::Pool::with_size(rng, 256_000));
        let rt_gen = ResourceTemplateGenerator::new(&config, &str_pool, rng)?;

        Ok(Self {
            pool: Pool::new(context_cap, rt_gen),
            scratch: RefCell::new(BytesMut::with_capacity(4096)),
        })
    }
}

impl<'a> SizedGenerator<'a> for OpentelemetryMetrics {
    type Output = v1::ResourceMetrics;
    type Error = Error;

    fn generate<R>(&'a mut self, rng: &mut R, budget: &mut usize) -> Result<Self::Output, Error>
    where
        R: rand::Rng + ?Sized,
    {
        // Retrieve a template from the pool, modify its timestamp and data
        // points to randomize the data we send out.
        let mut tpl: v1::ResourceMetrics = self.pool.fetch(rng, budget)?.to_owned();

        // Randomize the data points in each metric. Metric kinds are preserved
        // but timestamps are completely random as are point values.
        for scope_metrics in &mut tpl.scope_metrics {
            for metric in &mut scope_metrics.metrics {
                if let Some(data) = &mut metric.data {
                    match data {
                        Data::Gauge(gauge) => {
                            for point in &mut gauge.data_points {
                                point.time_unix_nano = rng.random();
                                if let Some(value) = &mut point.value {
                                    match value {
                                        number_data_point::Value::AsDouble(v) => {
                                            *v = rng.random();
                                        }
                                        number_data_point::Value::AsInt(v) => {
                                            *v = rng.random();
                                        }
                                    }
                                }
                            }
                        }
                        Data::Sum(sum) => {
                            for point in &mut sum.data_points {
                                point.time_unix_nano = rng.random();
                                if let Some(value) = &mut point.value {
                                    match value {
                                        number_data_point::Value::AsDouble(v) => {
                                            *v = rng.random();
                                        }
                                        number_data_point::Value::AsInt(v) => {
                                            *v = rng.random();
                                        }
                                    }
                                }
                            }
                        }
                        _ => unimplemented!(),
                    }
                }
            }
        }

        Ok(tpl)
    }
}

impl crate::Serialize for OpentelemetryMetrics {
    fn to_bytes<W, R>(&mut self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: rand::Rng + Sized,
        W: Write,
    {
        // Our approach here is simple: pack v1::ResourceMetrics from the
        // OpentelemetryMetrics generator into a ExportMetricsServiceRequest
        // until we hit max_bytes worth of serialized bytes.
        let mut bytes_remaining = max_bytes;
        let mut request = ExportMetricsServiceRequest {
            resource_metrics: Vec::with_capacity(8),
        };

        let loop_id: u32 = rng.random();
        while bytes_remaining >= SMALLEST_PROTOBUF {
            if let Ok(rm) = self.generate(&mut rng, &mut bytes_remaining) {
                request.resource_metrics.push(rm);
                let required_bytes = request.encoded_len();
                debug!(
                    ?loop_id,
                    ?max_bytes,
                    ?bytes_remaining,
                    ?required_bytes,
                    ?SMALLEST_PROTOBUF,
                    "to_bytes inner loop"
                );
                if required_bytes > max_bytes {
                    drop(request.resource_metrics.pop());
                    break;
                }
                bytes_remaining = max_bytes.saturating_sub(required_bytes);
            } else {
                debug!(
                    ?bytes_remaining,
                    ?SMALLEST_PROTOBUF,
                    "could not generate ResourceMetrics instance"
                );
            }
        }

        let needed = request.encoded_len();
        {
            let mut buf = self.scratch.borrow_mut();
            buf.clear(); // keep the allocation, drop the contents
            let capacity = buf.capacity();
            let diff = capacity.saturating_sub(needed);
            if buf.capacity() < needed {
                buf.reserve(diff); // at most one malloc here
            }
            request.encode(&mut *buf)?;
            writer.write_all(&buf)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{Config, Contexts, OpentelemetryMetrics};
    use crate::{
        Serialize, SizedGenerator,
        common::config::ConfRange,
        opentelemetry_metric::v1::{ResourceMetrics, metric},
    };
    use opentelemetry_proto::tonic::common::v1::any_value;
    use proptest::prelude::*;
    use prost::Message;
    use rand::{SeedableRng, rngs::SmallRng};
    use std::{
        collections::HashSet,
        hash::{DefaultHasher, Hash, Hasher},
    };

    // Budget always decreases equivalent to the size of the returned value.
    proptest! {
        #[test]
        fn generate_decrease_budget(
            seed: u64,
            total_contexts in 1..1_000_u32,
            attributes_per_resource in 0..20_u8,
            scopes_per_resource in 0..20_u8,
            attributes_per_scope in 0..20_u8,
            metrics_per_scope in 0..20_u8,
            attributes_per_metric in 0..10_u8,
            steps in 1..u8::MAX,
        ) {
            let config = Config {
                contexts: Contexts {
                    total_contexts: ConfRange::Constant(total_contexts),
                    attributes_per_resource: ConfRange::Constant(attributes_per_resource),
                    scopes_per_resource: ConfRange::Constant(scopes_per_resource),
                    attributes_per_scope: ConfRange::Constant(attributes_per_scope),
                    metrics_per_scope: ConfRange::Constant(metrics_per_scope),
                    attributes_per_metric: ConfRange::Constant(attributes_per_metric),
                },
                ..Default::default()
            };

            let mut budget = 10_000_000;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut otel_metrics = OpentelemetryMetrics::new(config, &mut rng)?;

            for _ in 0..steps {
                let prev = budget;
                match otel_metrics.generate(&mut rng, &mut budget) {
                    Ok(rm) => prop_assert_eq!(prev-rm.encoded_len(), budget),
                    Err(crate::Error::Pool(_)) => break,
                    Err(e) => return Err(e.into())
                }
            }
        }
    }

    // Generation of metrics must be deterministic. In order to assert this
    // property we generate two instances of OpentelemetryMetrics from disinct
    // rngs and drive them forward for a random amount of steps, asserting equal
    // outcomes.
    proptest! {
        #[test]
        fn opentelemetry_metrics_generate_is_deterministic(
            seed: u64,
            total_contexts in 1..1_000_u32,
            attributes_per_resource in 0..20_u8,
            scopes_per_resource in 0..20_u8,
            attributes_per_scope in 0..20_u8,
            metrics_per_scope in 0..20_u8,
            attributes_per_metric in 0..10_u8,
            steps in 1..u8::MAX,
            budget in 128..2048_usize,
        ) {
            let config = Config {
                contexts: Contexts {
                    total_contexts: ConfRange::Constant(total_contexts),
                    attributes_per_resource: ConfRange::Constant(attributes_per_resource),
                    scopes_per_resource: ConfRange::Constant(scopes_per_resource),
                    attributes_per_scope: ConfRange::Constant(attributes_per_scope),
                    metrics_per_scope: ConfRange::Constant(metrics_per_scope),
                    attributes_per_metric: ConfRange::Constant(attributes_per_metric),
                },
                ..Default::default()
            };

            let mut b1 = budget;
            let mut rng1 = SmallRng::seed_from_u64(seed);
            let mut otel_metrics1 = OpentelemetryMetrics::new(config, &mut rng1)?;
            let mut b2 = budget;
            let mut rng2 = SmallRng::seed_from_u64(seed);
            let mut otel_metrics2 = OpentelemetryMetrics::new(config, &mut rng2)?;

            for _ in 0..steps {
                let gen_1 = otel_metrics1.generate(&mut rng1, &mut b1)?;
                let gen_2 = otel_metrics2.generate(&mut rng2, &mut b2)?;
                prop_assert_eq!(gen_1, gen_2);
                prop_assert_eq!(b1, b2);
            }
        }
    }

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(
            seed: u64,
            total_contexts in 1..1_000_u32,
            attributes_per_resource in 0..20_u8,
            scopes_per_resource in 0..20_u8,
            attributes_per_scope in 0..20_u8,
            metrics_per_scope in 0..20_u8,
            attributes_per_metric in 0..10_u8,
            steps in 1..u8::MAX,
            max_bytes in 64u16..u16::MAX)
        {
            let config = Config {
                contexts: Contexts {
                    total_contexts: ConfRange::Constant(total_contexts),
                    attributes_per_resource: ConfRange::Constant(attributes_per_resource),
                    scopes_per_resource: ConfRange::Constant(scopes_per_resource),
                    attributes_per_scope: ConfRange::Constant(attributes_per_scope),
                    metrics_per_scope: ConfRange::Constant(metrics_per_scope),
                    attributes_per_metric: ConfRange::Constant(attributes_per_metric),
                },
                ..Default::default()
            };

            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut metrics = OpentelemetryMetrics::new(config, &mut rng).expect("failed to create metrics generator");

            let mut bytes = Vec::with_capacity(max_bytes);
            for _ in 0..steps {
                bytes.clear();
                metrics.to_bytes(&mut rng, max_bytes, &mut bytes).expect("failed to convert to bytes");
                prop_assert!(bytes.len() <= max_bytes, "max len: {max_bytes}, actual: {}", bytes.len());
            }
        }
    }

    // Generation of metrics must be context bounded. If `generate` is called
    // more than total_context times only total_context contexts should be
    // produced.
    proptest! {
        #[test]
        fn contexts_bound_metric_generation(
            seed: u64,
            total_contexts_min in 1..4_u32,
            total_contexts_max in 5..100_u32,
            attributes_per_resource in 0..25_u8,
            scopes_per_resource in 1..10_u8,
            attributes_per_scope in 0..20_u8,
            metrics_per_scope in 1..10_u8,
            attributes_per_metric in 0..100_u8,
            budget in 128..2048_usize,
        ) {
            let config = Config {
                contexts: Contexts {
                    total_contexts: ConfRange::Inclusive { min: total_contexts_min, max: total_contexts_max },
                    attributes_per_resource: ConfRange::Constant(attributes_per_resource),
                    scopes_per_resource: ConfRange::Constant(scopes_per_resource),
                    attributes_per_scope: ConfRange::Constant(attributes_per_scope),
                    metrics_per_scope: ConfRange::Constant(metrics_per_scope),
                    attributes_per_metric: ConfRange::Constant(attributes_per_metric),
                },
                ..Default::default()
            };

            prop_assume!(config.valid().is_ok());

            let mut ids = HashSet::new();
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut b = budget;
            let mut otel_metrics = OpentelemetryMetrics::new(config, &mut rng)?;

            let total_generations = total_contexts_max + (total_contexts_max / 2);
            for _ in 0..total_generations {
                let res = otel_metrics.generate(&mut rng, &mut b)?;
                let id = context_id(&res);
                ids.insert(id);
            }

            let actual_contexts = ids.len();
            let bounded_above = actual_contexts <= total_contexts_max as usize;
            prop_assert!(bounded_above,
                         "expected {} ≤ {}",
                         actual_contexts, total_contexts_max);
        }
    }

    // We want to know that every payload produced by this type actually
    // deserializes as a collection of OTEL metrics.
    proptest! {
        #[test]
        fn payload_deserializes(
            seed: u64,
            total_contexts in 1..1_000_u32,
            attributes_per_resource in 0..20_u8,
            scopes_per_resource in 0..20_u8,
            attributes_per_scope in 0..20_u8,
            metrics_per_scope in 0..20_u8,
            attributes_per_metric in 0..10_u8,
            max_bytes in 8u16..u16::MAX
        ) {
            let config = Config {
                contexts: Contexts {
                    total_contexts: ConfRange::Constant(total_contexts),
                    attributes_per_resource: ConfRange::Constant(attributes_per_resource),
                    scopes_per_resource: ConfRange::Constant(scopes_per_resource),
                    attributes_per_scope: ConfRange::Constant(attributes_per_scope),
                    metrics_per_scope: ConfRange::Constant(metrics_per_scope),
                    attributes_per_metric: ConfRange::Constant(attributes_per_metric),
                },
                ..Default::default()
            };

            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut metrics = OpentelemetryMetrics::new(config, &mut rng).expect("failed to create metrics generator");

            let mut bytes: Vec<u8> = Vec::with_capacity(max_bytes);
            metrics.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");

            opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest::decode(bytes.as_slice()).expect("failed to decode the message from the buffer");
        }
    }

    // Confirm that configuration bounds are naively obeyed. For instance, this
    // test will pass if every resource generated has identical attributes, etc
    // etc. Confirmation of context bounds being obeyed -- implying examination
    // of attributes et al -- is appropriate for another property test.
    proptest! {
        #[test]
        fn counts_within_bounds(
            seed: u64,
            total_contexts in 1..1_000_u32,
            attributes_per_resource in 0..20_u8,
            scopes_per_resource in 0..20_u8,
            attributes_per_scope in 0..20_u8,
            metrics_per_scope in 0..20_u8,
            attributes_per_metric in 0..10_u8,
            steps in 1..u8::MAX,
            budget in 128..2048_usize,
        ) {
            let config = Config {
                contexts: Contexts {
                    total_contexts: ConfRange::Constant(total_contexts),
                    attributes_per_resource: ConfRange::Constant(attributes_per_resource),
                    scopes_per_resource: ConfRange::Constant(scopes_per_resource),
                    attributes_per_scope: ConfRange::Constant(attributes_per_scope),
                    metrics_per_scope: ConfRange::Constant(metrics_per_scope),
                    attributes_per_metric: ConfRange::Constant(attributes_per_metric),
                },
                ..Default::default()
            };

            let mut b = budget;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut otel_metrics = OpentelemetryMetrics::new(config, &mut rng)?;

            for _ in 0..steps {
                let metric = otel_metrics.generate(&mut rng, &mut b)?;

                if let Some(resource) = &metric.resource {
                    prop_assert!(
                        resource.attributes.len() <= attributes_per_resource as usize,
                        "Resource attributes count {len} exceeds configured maximum {attributes_per_resource}",
                        len = resource.attributes.len(),
                    );
                }

                prop_assert!(
                    metric.scope_metrics.len() <= scopes_per_resource as usize,
                    "Scopes per resource count {len} exceeds configured maximum {scopes_per_resource}",
                    len = metric.scope_metrics.len(),
                );

                for scope in &metric.scope_metrics {
                    if let Some(scope) = &scope.scope {
                        prop_assert!(
                            scope.attributes.len() <= attributes_per_scope as usize,
                            "Scope attributes count {len} exceeds configured maximum {attributes_per_scope}",
                            len = scope.attributes.len(),
                        );
                    }

                    prop_assert!(
                        scope.metrics.len() <= metrics_per_scope as usize,
                        "Metrics per scope count {len} exceeds configured maximum {metrics_per_scope}",
                        len = scope.metrics.len(),
                    );

                    for metric in &scope.metrics {
                        prop_assert!(
                            metric.metadata.len() <= attributes_per_metric as usize,
                            "Metric attributes count {len} exceeds configured maximum {attributes_per_metric}",
                            len = metric.metadata.len(),
                        );
                    }
                }
            }
        }
    }

    /// Extracts and hashes the context from a `ResourceMetrics`.
    ///
    /// A context is defined by the unique combination of:
    /// - Resource attributes
    /// - Scope attributes
    /// - Metric name, attributes, data kind, and unit
    fn context_id(metric: &ResourceMetrics) -> u64 {
        let mut hasher = DefaultHasher::new();

        // Hash resource attributes
        if let Some(resource) = &metric.resource {
            for attr in &resource.attributes {
                attr.key.hash(&mut hasher);
                if let Some(value) = &attr.value {
                    if let Some(any_value::Value::StringValue(s)) = &value.value {
                        s.hash(&mut hasher);
                    }
                }
            }
        }

        // Hash each scope's context
        for scope_metrics in &metric.scope_metrics {
            // Hash scope attributes
            if let Some(scope) = &scope_metrics.scope {
                for attr in &scope.attributes {
                    attr.key.hash(&mut hasher);
                    if let Some(value) = &attr.value {
                        if let Some(any_value::Value::StringValue(s)) = &value.value {
                            s.hash(&mut hasher);
                        }
                    }
                }
            }

            // Hash each metric's context
            for metric in &scope_metrics.metrics {
                // Hash metric name
                metric.name.hash(&mut hasher);

                // Hash metric attributes
                for attr in &metric.metadata {
                    attr.key.hash(&mut hasher);
                    if let Some(value) = &attr.value {
                        if let Some(any_value::Value::StringValue(s)) = &value.value {
                            s.hash(&mut hasher);
                        }
                    }
                }

                // Hash metric data kind and unit
                metric.unit.hash(&mut hasher);
                if let Some(data) = &metric.data {
                    match data {
                        metric::Data::Gauge(_) => "gauge".hash(&mut hasher),
                        metric::Data::Sum(sum) => {
                            "sum".hash(&mut hasher);
                            sum.aggregation_temporality.hash(&mut hasher);
                            sum.is_monotonic.hash(&mut hasher);
                        }
                        // Add other metric types as needed
                        _ => "unknown".hash(&mut hasher),
                    }
                }
            }
        }

        hasher.finish()
    }

    // Ensures that the context_id function serves as an equality test, ignoring
    // value differences etc.
    proptest! {
        #[test]
        fn context_is_equality(
            seed: u64,
            total_contexts in 1..1_000_u32,
            attributes_per_resource in 0..20_u8,
            scopes_per_resource in 0..20_u8,
            attributes_per_scope in 0..20_u8,
            metrics_per_scope in 0..20_u8,
            attributes_per_metric in 0..10_u8,
            budget in 128..2048_usize,
        ) {
            let config = Config {
                contexts: Contexts {
                    total_contexts: ConfRange::Constant(total_contexts),
                    attributes_per_resource: ConfRange::Constant(attributes_per_resource),
                    scopes_per_resource: ConfRange::Constant(scopes_per_resource),
                    attributes_per_scope: ConfRange::Constant(attributes_per_scope),
                    metrics_per_scope: ConfRange::Constant(metrics_per_scope),
                    attributes_per_metric: ConfRange::Constant(attributes_per_metric),
                },
                ..Default::default()
            };

            let mut b1 = budget;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut otel_metrics = OpentelemetryMetrics::new(config, &mut rng)?;
            let metric1 = otel_metrics.generate(&mut rng, &mut b1)?;

            // Generate two identical metrics
            let mut b2 = budget;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut otel_metrics = OpentelemetryMetrics::new(config, &mut rng)?;
            let metric2 = otel_metrics.generate(&mut rng, &mut b2)?;

            // Ensure that the metrics are equal.
            assert_eq!(metric1, metric2);
            // If the metrics are equal then their contexts must be equal.
            assert_eq!(context_id(&metric1), context_id(&metric2));
        }
    }

    #[test]
    fn config_validation() {
        // Valid cases
        let valid_config = Config {
            contexts: Contexts {
                total_contexts: ConfRange::Constant(100),
                attributes_per_resource: ConfRange::Inclusive { min: 1, max: 20 },
                scopes_per_resource: ConfRange::Inclusive { min: 1, max: 20 },
                attributes_per_scope: ConfRange::Constant(0),
                metrics_per_scope: ConfRange::Inclusive { min: 1, max: 20 },
                attributes_per_metric: ConfRange::Inclusive { min: 0, max: 10 },
            },
            ..Default::default()
        };
        assert!(valid_config.valid().is_ok());

        // Zero total_contexts
        let zero_contexts = Config {
            contexts: Contexts {
                total_contexts: ConfRange::Constant(0),
                ..valid_config.contexts
            },
            ..valid_config
        };
        assert!(zero_contexts.valid().is_err());

        // Zero min in range
        let zero_min_range = Config {
            contexts: Contexts {
                total_contexts: ConfRange::Inclusive { min: 0, max: 100 },
                ..valid_config.contexts
            },
            ..valid_config
        };
        assert!(zero_min_range.valid().is_err());

        // Min greater than max
        let min_gt_max = Config {
            contexts: Contexts {
                total_contexts: ConfRange::Inclusive { min: 100, max: 50 },
                ..valid_config.contexts
            },
            ..valid_config
        };
        assert!(min_gt_max.valid().is_err());

        // Impossible to achieve minimum contexts
        let impossible_min = Config {
            contexts: Contexts {
                total_contexts: ConfRange::Inclusive {
                    min: 1000,
                    max: 2000,
                },
                scopes_per_resource: ConfRange::Constant(1),
                metrics_per_scope: ConfRange::Constant(1),
                ..valid_config.contexts
            },
            ..valid_config
        };
        assert!(impossible_min.valid().is_err());

        // Impossible to achieve maximum contexts
        let impossible_max = Config {
            contexts: Contexts {
                total_contexts: ConfRange::Inclusive { min: 1, max: 1 },
                scopes_per_resource: ConfRange::Constant(2),
                metrics_per_scope: ConfRange::Constant(2),
                ..valid_config.contexts
            },
            ..valid_config
        };
        assert!(impossible_max.valid().is_err());

        // Zero metric weights
        let zero_weights = Config {
            metric_weights: super::MetricWeights { gauge: 0, sum: 0 },
            ..valid_config
        };
        assert!(zero_weights.valid().is_err());
    }
}
