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
use tracing::{debug, error};
use unit::UnitGenerator;

/// Smallest useful protobuf, determined by experimentation and enforced in
/// `smallest_protobuf` test.
pub const SMALLEST_PROTOBUF: usize = 31;

/// Increment timestamps by 100 milliseconds (in nanoseconds) per tick
const TIME_INCREMENT_NANOS: u64 = 1_000_000;

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
    #[allow(clippy::too_many_lines)]
    pub fn valid(&self) -> Result<(), String> {
        // Validate metric weights - both types must have non-zero probability to ensure
        // we can generate a diverse set of metrics
        if self.metric_weights.gauge == 0 || self.metric_weights.sum == 0 {
            return Err("Metric weights cannot be 0".to_string());
        }

        // Validate total_contexts - we need at least one context to generate metrics
        match self.contexts.total_contexts {
            ConfRange::Constant(0) => return Err("total_contexts cannot be zero".to_string()),
            ConfRange::Constant(_) => (), // Non-zero constant is valid
            ConfRange::Inclusive { min, max } => {
                if min == 0 {
                    return Err("total_contexts minimum cannot be zero".to_string());
                }
                if min > max {
                    return Err("total_contexts minimum cannot be greater than maximum".to_string());
                }
            }
        }

        // Validate scopes_per_resource - each resource must have at least one scope
        // to contain metrics
        match self.contexts.scopes_per_resource {
            ConfRange::Constant(0) => return Err("scopes_per_resource cannot be zero".to_string()),
            ConfRange::Constant(_) => (), // Non-zero constant is valid
            ConfRange::Inclusive { min, max } => {
                if min == 0 {
                    return Err("scopes_per_resource minimum cannot be zero".to_string());
                }
                if min > max {
                    return Err(
                        "scopes_per_resource minimum cannot be greater than maximum".to_string()
                    );
                }
            }
        }

        // Validate metrics_per_scope - each scope must contain at least one metric
        // to be meaningful
        match self.contexts.metrics_per_scope {
            ConfRange::Constant(0) => return Err("metrics_per_scope cannot be zero".to_string()),
            ConfRange::Constant(_) => (), // Non-zero constant is valid
            ConfRange::Inclusive { min, max } => {
                if min == 0 {
                    return Err("metrics_per_scope minimum cannot be zero".to_string());
                }
                if min > max {
                    return Err(
                        "metrics_per_scope minimum cannot be greater than maximum".to_string()
                    );
                }
            }
        }

        // Validate attributes_per_resource range
        if let ConfRange::Inclusive { min, max } = self.contexts.attributes_per_resource
            && min > max
        {
            return Err(
                "attributes_per_resource minimum cannot be greater than maximum".to_string(),
            );
        }

        // Validate attributes_per_scope range
        if let ConfRange::Inclusive { min, max } = self.contexts.attributes_per_scope
            && min > max
        {
            return Err("attributes_per_scope minimum cannot be greater than maximum".to_string());
        }

        // Validate attributes_per_metric range
        if let ConfRange::Inclusive { min, max } = self.contexts.attributes_per_metric
            && min > max
        {
            return Err("attributes_per_metric minimum cannot be greater than maximum".to_string());
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
    /// Current tick count for monotonic timing (starts at 0)
    tick: u64,
    /// Accumulating sum increment, floating point
    incr_f: f64,
    /// Accumulating sum increment, integer
    incr_i: i64,
    /// Number of data points in the most recent `ResourceMetrics` (set by
    /// `generate`).
    data_points_per_resource: u64,
    /// Number of data points in the most recent block (set by `to_bytes`).
    data_points_per_block: u64,
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
        // Moby Dick is 1.2Mb. 128Kb should be more than enough for metric
        // names, descriptions, etc.
        let str_pool = Rc::new(strings::Pool::with_size(rng, 128_000));
        let rt_gen = ResourceTemplateGenerator::new(&config, &str_pool, rng)?;

        Ok(Self {
            pool: Pool::new(context_cap, rt_gen),
            scratch: RefCell::new(BytesMut::with_capacity(4096)),
            tick: 0,
            incr_f: 0.0,
            incr_i: 0,
            data_points_per_resource: 0,
            data_points_per_block: 0,
        })
    }
}

impl<'a> SizedGenerator<'a> for OpentelemetryMetrics {
    type Output = v1::ResourceMetrics;
    type Error = Error;

    /// Generate OTLP metrics with the following enhancements:
    ///
    /// * Monotonic sums are truly monotonic, incrementing by a random amount
    ///   each tick
    /// * Timestamps advance monotonically based on internal tick counter
    ///   starting at epoch
    /// * Each call advances the tick counter by a random amount (1-60)
    fn generate<R>(&'a mut self, rng: &mut R, budget: &mut usize) -> Result<Self::Output, Error>
    where
        R: rand::Rng + ?Sized,
    {
        self.tick += rng.random_range(1..=60);
        self.incr_f += rng.random_range(1.0..=100.0);
        self.incr_i += rng.random_range(1_i64..=100_i64);

        let mut tpl: v1::ResourceMetrics = match self.pool.fetch(rng, budget) {
            Ok(t) => t.to_owned(),
            Err(crate::opentelemetry::common::templates::PoolError::EmptyChoice) => {
                error!("Pool was unable to satify request for {budget} size");
                Err(crate::opentelemetry::common::templates::PoolError::EmptyChoice)?
            }
            Err(e) => Err(e)?,
        };

        // Update data points in each metric. For gauges we use random values,
        // for accumulating sums we increment by a fixed amount per tick.
        // All timestamps are updated based on the current tick.
        let mut data_points_count = 0;

        for scope_metrics in &mut tpl.scope_metrics {
            for metric in &mut scope_metrics.metrics {
                if let Some(data) = &mut metric.data {
                    match data {
                        Data::Gauge(gauge) => {
                            data_points_count += gauge.data_points.len() as u64;
                            for point in &mut gauge.data_points {
                                point.time_unix_nano = self.tick * TIME_INCREMENT_NANOS;
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
                            data_points_count += sum.data_points.len() as u64;
                            let is_accumulating = sum.is_monotonic;
                            for point in &mut sum.data_points {
                                point.time_unix_nano = self.tick * TIME_INCREMENT_NANOS;
                                if is_accumulating {
                                    // For accumulating sums, monotonically
                                    // increase by some factor of `tick_diff`
                                    if let Some(value) = &mut point.value {
                                        match value {
                                            number_data_point::Value::AsDouble(v) => {
                                                *v += self.incr_f;
                                            }
                                            #[allow(clippy::cast_possible_wrap)]
                                            number_data_point::Value::AsInt(v) => {
                                                *v += self.incr_i;
                                            }
                                        }
                                    }
                                } else {
                                    // For non-accumulating sums, use random
                                    // values
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
                        }
                        _ => unimplemented!(),
                    }
                }
            }
        }

        self.data_points_per_resource = data_points_count;

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

        let mut total_data_points = 0;
        let loop_id: u32 = rng.random();

        // Build up the request by adding ResourceMetrics one by one until we
        // would exceed max_bytes.
        //
        // Example: max_bytes = 1000
        //
        // - Request with 0 ResourceMetrics: encoded size = 100 bytes
        // - Add ResourceMetrics #1: encoded size = 400 bytes (still under 1000,
        //   continue)
        // - Add ResourceMetrics #2: encoded size = 700 bytes (still under 1000,
        //   continue)
        // - Add ResourceMetrics #3: encoded size = 1050 bytes (over 1000!)
        // - Code detects overflow, pops #3, breaks the loop
        // - Request now has #1 and #2, encoded size = 700 bytes
        //
        // When we call generate we pass bytes_remaining as the budget. After
        // adding ResourceMetrics #2, we set bytes_remaining = 1000 - 700 =
        // 300. So when generating #3, we tell it "you have 300 bytes to work
        // with". The generate method might return something that encodes to 250
        // bytes on its own.
        //
        // But, protobuf encoding isn't strictly additive. When we add that 250
        // byte ResourceMetrics to a request that's already 700 bytes, the
        // combined encoding might be > 1000 bytes (not 950) due to additional
        // framing, length prefixes, field tags or whatever.  We handle this by
        // checking after adding and removing the item if it exceeds the budget.
        while bytes_remaining >= SMALLEST_PROTOBUF {
            if let Ok(rm) = self.generate(&mut rng, &mut bytes_remaining) {
                total_data_points += self.data_points_per_resource;
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
                    total_data_points -= self.data_points_per_resource;
                    drop(request.resource_metrics.pop());
                    break;
                }
                bytes_remaining = max_bytes.saturating_sub(required_bytes);
            } else {
                // Belt with suspenders time: verify no templates could possibly
                // fit. If we pass this assertion, break as no template will
                // ever fit the requested max_bytes.
                assert!(
                    !self.pool.template_fits(bytes_remaining),
                    "Pool claims template fits {bytes_remaining} bytes but generate() failed, indicative of a logic error",
                );
                break;
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

        self.data_points_per_block = total_data_points;

        Ok(())
    }

    fn data_points_generated(&self) -> Option<u64> {
        Some(self.data_points_per_block)
    }
}

#[cfg(test)]
mod test {
    use super::{Config, Contexts, OpentelemetryMetrics, SMALLEST_PROTOBUF};
    use crate::{
        Serialize, SizedGenerator,
        common::config::ConfRange,
        opentelemetry::metric::v1::{ResourceMetrics, metric},
    };
    use opentelemetry_proto::tonic::common::v1::any_value;
    use opentelemetry_proto::tonic::metrics::v1::{
        Metric, NumberDataPoint, ScopeMetrics, number_data_point,
    };
    use opentelemetry_proto::tonic::{
        collector::metrics::v1::ExportMetricsServiceRequest, metrics::v1::Gauge,
    };
    use proptest::prelude::*;
    use prost::Message;
    use rand::{SeedableRng, rngs::SmallRng};
    use std::{
        collections::{HashMap, HashSet},
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
        fn is_deterministic(
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
                if let Ok(gen_1) = otel_metrics1.generate(&mut rng1, &mut b1) {
                    let gen_2 = otel_metrics2.generate(&mut rng2, &mut b2).expect("gen_2 was not Ok");
                    prop_assert_eq!(gen_1, gen_2);
                    prop_assert_eq!(b1, b2);
                } else {
                    break;
                }
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

            let mut ids = HashSet::new();
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut b = budget;
            let mut otel_metrics = OpentelemetryMetrics::new(config, &mut rng)?;

            let total_generations = total_contexts_max + (total_contexts_max / 2);
            for _ in 0..total_generations {
                if let Ok(res) = otel_metrics.generate(&mut rng, &mut b) {
                    let id = context_id(&res);
                    ids.insert(id);
                }
            }

            let actual_contexts = ids.len();
            let bounded_above = actual_contexts <= total_contexts_max as usize;
            prop_assert!(bounded_above,
                         "expected {actual_contexts} ≤ {total_contexts_max}");
        }
    }

    // We want to be sure that the serialized size of the payload does not
    // exceed `budget`.
    #[test]
    fn payload_not_exceed_max_bytes() {
        let config = Config {
            contexts: Contexts {
                total_contexts: ConfRange::Constant(10),
                attributes_per_resource: ConfRange::Constant(5),
                scopes_per_resource: ConfRange::Constant(2),
                attributes_per_scope: ConfRange::Constant(3),
                metrics_per_scope: ConfRange::Constant(4),
                attributes_per_metric: ConfRange::Constant(2),
            },
            ..Default::default()
        };

        let max_bytes = 512;
        let mut rng = SmallRng::seed_from_u64(42);
        let mut metrics = OpentelemetryMetrics::new(config, &mut rng)
            .expect("failed to create metrics generator");

        let mut bytes = Vec::new();
        metrics
            .to_bytes(&mut rng, max_bytes, &mut bytes)
            .expect("failed to convert to bytes");
        assert!(
            bytes.len() <= max_bytes,
            "max len: {max_bytes}, actual: {}",
            bytes.len()
        );
    }

    // // We want to be sure that the serialized size of the payload does not
    // // exceed `budget`.
    // proptest! {
    //     #[test]
    //     fn payload_not_exceed_max_bytes(
    //         seed: u64,
    //         total_contexts in 1..1_000_u32,
    //         attributes_per_resource in 0..20_u8,
    //         scopes_per_resource in 0..20_u8,
    //         attributes_per_scope in 0..20_u8,
    //         metrics_per_scope in 0..20_u8,
    //         attributes_per_metric in 0..10_u8,
    //         budget in SMALLEST_PROTOBUF..2048_usize,
    //     ) {
    //         let config = Config {
    //             contexts: Contexts {
    //                 total_contexts: ConfRange::Constant(total_contexts),
    //                 attributes_per_resource: ConfRange::Constant(attributes_per_resource),
    //                 scopes_per_resource: ConfRange::Constant(scopes_per_resource),
    //                 attributes_per_scope: ConfRange::Constant(attributes_per_scope),
    //                 metrics_per_scope: ConfRange::Constant(metrics_per_scope),
    //                 attributes_per_metric: ConfRange::Constant(attributes_per_metric),
    //             },
    //             ..Default::default()
    //         };
    //         let mut rng = SmallRng::seed_from_u64(seed);
    //         let mut metrics = OpentelemetryMetrics::new(config, &mut rng)
    //             .expect("failed to create metrics generator");

    //         let mut bytes = Vec::new();
    //         metrics
    //             .to_bytes(&mut rng, budget, &mut bytes)
    //             .expect("failed to convert to bytes");
    //         assert!(
    //             bytes.len() <= budget,
    //             "max len: {budget}, actual: {}",
    //             bytes.len()
    //         );
    //     }
    // }

    // We want to know that every payload produced by this type actually
    // deserializes as a collection of OTEL metrics.
    #[test]
    fn payload_deserializes() {
        let config = Config {
            contexts: Contexts {
                total_contexts: ConfRange::Constant(5),
                attributes_per_resource: ConfRange::Constant(3),
                scopes_per_resource: ConfRange::Constant(2),
                attributes_per_scope: ConfRange::Constant(1),
                metrics_per_scope: ConfRange::Constant(3),
                attributes_per_metric: ConfRange::Constant(2),
            },
            ..Default::default()
        };

        let max_bytes = 256;
        let mut rng = SmallRng::seed_from_u64(42);
        let mut metrics = OpentelemetryMetrics::new(config, &mut rng)
            .expect("failed to create metrics generator");

        let mut bytes: Vec<u8> = Vec::new();
        metrics
            .to_bytes(&mut rng, max_bytes, &mut bytes)
            .expect("failed to convert to bytes");

        opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest::decode(
            bytes.as_slice(),
        )
        .expect("failed to decode the message from the buffer");
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
            budget in SMALLEST_PROTOBUF..2048_usize,
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
                if let Ok(metric) = otel_metrics.generate(&mut rng, &mut b) {
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
            scopes_per_resource in 1..20_u8,
            attributes_per_scope in 0..20_u8,
            metrics_per_scope in 1..20_u8,
            attributes_per_metric in 0..10_u8,
            budget in SMALLEST_PROTOBUF..4098_usize,
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
            let metric1 = otel_metrics.generate(&mut rng, &mut b1);

            // Generate two identical metrics
            let mut b2 = budget;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut otel_metrics = OpentelemetryMetrics::new(config, &mut rng)?;
            let metric2 = otel_metrics.generate(&mut rng, &mut b2);

            // Ensure that the metrics are equal and that their contexts, when
            // applicable, are also equal.
            if let Ok(m1) = metric1 {
                let m2 = metric2.unwrap();
                prop_assert_eq!(context_id(&m1), context_id(&m2));
                prop_assert_eq!(m1, m2);
            }
        }
    }

    // Property: timestamps are monotonic
    proptest! {
        #[test]
        fn timestamps_increase_monotonically(
            seed: u64,
            total_contexts in 1..1_000_u32,
            attributes_per_resource in 0..20_u8,
            scopes_per_resource in 0..20_u8,
            attributes_per_scope in 0..20_u8,
            metrics_per_scope in 0..20_u8,
            attributes_per_metric in 0..10_u8,
            steps in 1..u8::MAX,
            budget in SMALLEST_PROTOBUF..2048_usize,
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

            let mut budget = budget;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut otel_metrics = OpentelemetryMetrics::new(config, &mut rng)?;

            let mut timestamps_by_metric: HashMap<u64, Vec<u64>> = HashMap::new();

            for _ in 0..steps {
                if let Ok(resource_metric) = otel_metrics.generate(&mut rng, &mut budget) {
                    for scope_metric in &resource_metric.scope_metrics {
                        for metric in &scope_metric.metrics {
                            let id = context_id(&resource_metric);

                            if let Some(data) = &metric.data {
                                match data {
                                    metric::Data::Gauge(gauge) => {
                                        for point in &gauge.data_points {
                                            timestamps_by_metric
                                                .entry(id)
                                                .or_insert_with(Vec::new)
                                                .push(point.time_unix_nano);
                                        }
                                    },
                                    metric::Data::Sum(sum) => {
                                        for point in &sum.data_points {
                                            timestamps_by_metric
                                                .entry(id)
                                                .or_insert_with(Vec::new)
                                                .push(point.time_unix_nano);
                                        }
                                    },
                                    _ => {},
                                }
                            }
                        }
                    }
                }
            }

            if !timestamps_by_metric.is_empty() {
                // For each metric, verify its timestamps increase monotonically
                for (metric_id, timestamps) in &timestamps_by_metric {
                    if timestamps.len() > 1 {
                        for i in 1..timestamps.len() {
                            let current = timestamps[i];
                            let previous = timestamps[i-1]; // safety: iterator begins at 1
                            prop_assert!(
                                current >= previous,
                                "Timestamp for metric {metric_id} did not increase monotonically: current={current}, previous={previous}",
                            );
                        }
                    }
                }
            }
        }
    }

    // Property: tick tally in OpentelemetryMetrics increase with calls to
    // `generate`.
    proptest! {
        #[test]
        fn increasing_ticks(
            seed: u64,
            total_contexts in 1..1_000_u32,
            attributes_per_resource in 0..20_u8,
            scopes_per_resource in 0..20_u8,
            attributes_per_scope in 0..20_u8,
            metrics_per_scope in 0..20_u8,
            attributes_per_metric in 0..10_u8,
            budget in SMALLEST_PROTOBUF..4098_usize,
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
                metric_weights: super::MetricWeights {
                    gauge: 0,   // Only generate sum metrics
                    sum: 100,
                },
            };

            let mut budget = budget;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut otel_metrics = OpentelemetryMetrics::new(config, &mut rng)?;
            let prev = otel_metrics.tick;
            let _ = otel_metrics.generate(&mut rng, &mut budget);
            let cur = otel_metrics.tick;

            prop_assert!(cur > prev, "Ticks did not advance properly: current: {cur}, previous: {prev}");
        }
    }

    // Property: accumulated sums are monotonic
    proptest! {
        #[test]
        fn accumulating_sums_only_increase(
            seed: u64,
            total_contexts in 1..1_000_u32,
            attributes_per_resource in 0..20_u8,
            scopes_per_resource in 0..20_u8,
            attributes_per_scope in 0..20_u8,
            metrics_per_scope in 0..20_u8,
            attributes_per_metric in 0..10_u8,
            budget in SMALLEST_PROTOBUF..512_usize, // see note below about repetition
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
                metric_weights: super::MetricWeights {
                    gauge: 0,   // Only generate sum metrics
                    sum: 100,
                },
            };

            let mut rng = SmallRng::seed_from_u64(seed);
            let mut otel_metrics = OpentelemetryMetrics::new(config, &mut rng)?;

            let mut values: HashMap<u64, f64> = HashMap::new();

            // Generate the initial batch of values. It's entirely possible we
            // will not run into the same context_ids again. Please run this
            // test with a very high number of cases. We have arbitrarily
            // constrained the budget relative to other tests in this module for
            // try and force repetition of contexts.
            let mut budget = budget;
            {
                if let Ok(resource_metrics) = otel_metrics.generate(&mut rng, &mut budget) {
                    let id = context_id(&resource_metrics);
                    for scope_metric in &resource_metrics.scope_metrics {
                        for metric in &scope_metric.metrics {
                            if let Some(metric::Data::Sum(sum)) = &metric.data {
                                if sum.is_monotonic {
                                    for point in sum.data_points.iter() {
                                        if let Some(number_data_point::Value::AsDouble(v)) = point.value {
                                            values.insert(id, v);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            let mut budget = budget;
            {
                if let Ok(resource_metrics) = otel_metrics.generate(&mut rng, &mut budget) {
                    let id = context_id(&resource_metrics);
                    for scope_metric in &resource_metrics.scope_metrics {
                        for metric in &scope_metric.metrics {
                            if let Some(metric::Data::Sum(sum)) = &metric.data {
                                if sum.is_monotonic {
                                    for point in sum.data_points.iter() {
                                        if let Some(number_data_point::Value::AsDouble(v)) = point.value {
                                            if let Some(&previous_value) = values.get(&id) {
                                                prop_assert!(
                                                    v >= previous_value,
                                                    "Monotonic sum decreased for {id}: previous={previous_value}, current={v}",
                                                );
                                            }
                                            values.insert(id, v);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn smallest_protobuf() {
        let data_point = NumberDataPoint {
            attributes: Vec::new(),
            start_time_unix_nano: 0,
            time_unix_nano: 1, // Minimal non-zero timestamp
            value: Some(number_data_point::Value::AsInt(1)), // Smallest value
            exemplars: Vec::new(),
            flags: 0,
        };

        let gauge = Gauge {
            data_points: vec![data_point],
        };

        let metric = Metric {
            name: "x".into(), // Minimal valid name
            description: "".into(),
            unit: "".into(),
            data: Some(metric::Data::Gauge(gauge)),
            metadata: Vec::new(), // No metadata
        };

        let scope_metrics = ScopeMetrics {
            scope: None, // No scope info
            metrics: vec![metric],
            schema_url: "".into(),
        };

        let resource_metrics = ResourceMetrics {
            resource: None, // No resource info
            scope_metrics: vec![scope_metrics],
            schema_url: "".into(),
        };

        // The most minimal request we care to support, just one single data point
        let minimal_request = ExportMetricsServiceRequest {
            resource_metrics: vec![resource_metrics],
        };

        let encoded_size = minimal_request.encoded_len();

        assert!(
            encoded_size == SMALLEST_PROTOBUF,
            "Minimal useful request size ({encoded_size}) should be == SMALLEST_PROTOBUF ({SMALLEST_PROTOBUF})"
        );
    }

    #[test]
    fn config_validation() {
        // Valid configuration
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

        // Invalid: Zero total_contexts
        let zero_contexts = Config {
            contexts: Contexts {
                total_contexts: ConfRange::Constant(0),
                ..valid_config.contexts
            },
            ..valid_config
        };
        assert!(zero_contexts.valid().is_err());

        // Invalid: Zero minimum total_contexts in range
        let zero_min_contexts = Config {
            contexts: Contexts {
                total_contexts: ConfRange::Inclusive { min: 0, max: 100 },
                ..valid_config.contexts
            },
            ..valid_config
        };
        assert!(zero_min_contexts.valid().is_err());

        // Invalid: Min greater than max in total_contexts
        let min_gt_max_contexts = Config {
            contexts: Contexts {
                total_contexts: ConfRange::Inclusive { min: 100, max: 50 },
                ..valid_config.contexts
            },
            ..valid_config
        };
        assert!(min_gt_max_contexts.valid().is_err());

        // Invalid: Zero scopes_per_resource
        let zero_scopes = Config {
            contexts: Contexts {
                scopes_per_resource: ConfRange::Constant(0),
                ..valid_config.contexts
            },
            ..valid_config
        };
        assert!(zero_scopes.valid().is_err());

        // Invalid: Zero minimum scopes_per_resource in range
        let zero_min_scopes = Config {
            contexts: Contexts {
                scopes_per_resource: ConfRange::Inclusive { min: 0, max: 10 },
                ..valid_config.contexts
            },
            ..valid_config
        };
        assert!(zero_min_scopes.valid().is_err());

        // Invalid: Min greater than max in scopes_per_resource
        let min_gt_max_scopes = Config {
            contexts: Contexts {
                scopes_per_resource: ConfRange::Inclusive { min: 20, max: 10 },
                ..valid_config.contexts
            },
            ..valid_config
        };
        assert!(min_gt_max_scopes.valid().is_err());

        // Invalid: Zero metrics_per_scope
        let zero_metrics = Config {
            contexts: Contexts {
                metrics_per_scope: ConfRange::Constant(0),
                ..valid_config.contexts
            },
            ..valid_config
        };
        assert!(zero_metrics.valid().is_err());

        // Invalid: Zero minimum metrics_per_scope in range
        let zero_min_metrics = Config {
            contexts: Contexts {
                metrics_per_scope: ConfRange::Inclusive { min: 0, max: 10 },
                ..valid_config.contexts
            },
            ..valid_config
        };
        assert!(zero_min_metrics.valid().is_err());

        // Invalid: Min greater than max in metrics_per_scope
        let min_gt_max_metrics = Config {
            contexts: Contexts {
                metrics_per_scope: ConfRange::Inclusive { min: 20, max: 10 },
                ..valid_config.contexts
            },
            ..valid_config
        };
        assert!(min_gt_max_metrics.valid().is_err());

        // Invalid: Impossible to achieve minimum contexts (context requirement exceeds capacity)
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

        // Invalid: Impossible to achieve maximum contexts (max is too small)
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

        // Invalid: Zero metric weights
        let zero_gauge_weight = Config {
            metric_weights: super::MetricWeights { gauge: 0, sum: 50 },
            ..valid_config
        };
        assert!(zero_gauge_weight.valid().is_err());

        let zero_sum_weight = Config {
            metric_weights: super::MetricWeights { gauge: 50, sum: 0 },
            ..valid_config
        };
        assert!(zero_sum_weight.valid().is_err());

        let zero_weights = Config {
            metric_weights: super::MetricWeights { gauge: 0, sum: 0 },
            ..valid_config
        };
        assert!(zero_weights.valid().is_err());
    }
}
