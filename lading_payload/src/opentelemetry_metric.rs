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

mod templates;
pub(crate) mod unit;

use std::io::Write;
use std::rc::Rc;

use crate::{Error, Generator, common::config::ConfRange, common::strings};
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    metrics::v1,
    resource,
};
use prost::Message;
use rand::{
    Rng,
    distr::{StandardUniform, weighted::WeightedIndex},
    prelude::Distribution,
    seq::IndexedRandom,
};
use serde::{Deserialize, Serialize as SerdeSerialize};
use templates::{Kind, MetricTemplate, ResourceTemplate, ScopeTemplate};
use unit::UnitGenerator;

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
    pub attributes_per_resource: ConfRange<u32>,
    /// TODO
    pub scopes_per_resource: ConfRange<u32>,
    /// TODO
    pub attributes_per_scope: ConfRange<u32>,
    /// The range of attributes for scopes per resource.
    pub metrics_per_scope: ConfRange<u32>,
    /// The range of attributes for scopes per resource.
    pub attributes_per_metric: ConfRange<u32>,
}

impl Default for Contexts {
    fn default() -> Self {
        Self {
            total_contexts: ConfRange::Constant(100),
            attributes_per_resource: ConfRange::Inclusive { min: 1, max: 20 },
            scopes_per_resource: ConfRange::Inclusive { min: 1, max: 20 },
            attributes_per_scope: ConfRange::Constant(0),
            metrics_per_scope: ConfRange::Inclusive { min: 0, max: 20 },
            attributes_per_metric: ConfRange::Inclusive { min: 0, max: 10 },
        }
    }
}

/// Defines the relative probability of each kind of OpenTelemetry metric.
#[derive(Debug, Deserialize, SerdeSerialize, Clone, PartialEq, Copy)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields, default)]
pub struct MetricWeights {
    gauge: u8,
    sum: u8,
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
    contexts: Contexts,
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
        Ok(())
    }
}

// Okay if you think about it OTel Metrics are in a tree. That tree is rooted at
// the Resource, below the resources are Scope which contain Metrics. If we want
// to have a bounded amount of contexts we need some way of counting how many
// we've made where a context is the Resources X Scopes X Metric Names and so
// the Resource is the top generator etc.

#[derive(Debug, Clone)]
pub(crate) struct ResourceGenerator {
    attributes_per_resource: ConfRange<u32>,
    scopes_per_resource: ConfRange<u32>,
    scope_generator: ScopeGenerator,
    str_pool: Rc<strings::Pool>,
}

impl ResourceGenerator {
    pub(crate) fn new(config: Config, str_pool: &Rc<strings::Pool>) -> Result<Self, Error> {
        Ok(Self {
            str_pool: Rc::clone(str_pool),
            attributes_per_resource: config.contexts.attributes_per_resource,
            scopes_per_resource: config.contexts.scopes_per_resource,
            scope_generator: ScopeGenerator::new(config, str_pool)?,
        })
    }
}

impl<'a> Generator<'a> for ResourceGenerator {
    type Output = v1::ResourceMetrics;
    type Error = Error;

    fn generate<R>(&'a self, rng: &mut R) -> Result<Self::Output, Error>
    where
        R: rand::Rng + ?Sized,
    {
        let unknown_resource = self.attributes_per_resource.start() == 0;
        // If the range of resources admits 0 the field `unknown_resources` will
        // be set and we give half-odds on there be no `resource` field set in
        // the message.
        let resource: Option<resource::v1::Resource> = if !unknown_resource && rng.random_bool(0.5)
        {
            let count = self.attributes_per_resource.sample(rng);
            let attributes = generate_attributes(&self.str_pool, rng, count)?;
            let res = resource::v1::Resource {
                attributes,
                dropped_attributes_count: 0,
            };
            Some(res)
        } else {
            None
        };

        let total_scopes = self.scopes_per_resource.sample(rng);
        let mut scope_metrics = Vec::with_capacity(total_scopes as usize);
        for _ in 0..total_scopes {
            scope_metrics.push(self.scope_generator.generate(rng)?);
        }

        Ok(v1::ResourceMetrics {
            resource,
            scope_metrics,
            schema_url: String::new(),
        })
    }
}

// NOTE the scope generator today is a pass-through, existing really only to
// contain the metric generator. Left to leave the machinery in place for if we
// want to start generating scopes.
#[derive(Debug, Clone)]
pub(crate) struct ScopeGenerator {
    metrics_per_scope: ConfRange<u32>,
    attributes_per_scope: ConfRange<u32>,
    metric_generator: MetricGenerator,
    str_pool: Rc<strings::Pool>,
}

impl ScopeGenerator {
    pub(crate) fn new(config: Config, str_pool: &Rc<strings::Pool>) -> Result<Self, Error> {
        Ok(Self {
            str_pool: Rc::clone(str_pool),
            attributes_per_scope: config.contexts.attributes_per_scope,
            metrics_per_scope: config.contexts.metrics_per_scope,
            metric_generator: MetricGenerator::new(config, str_pool)?,
        })
    }
}

impl<'a> Generator<'a> for ScopeGenerator {
    type Output = v1::ScopeMetrics;
    type Error = Error;

    fn generate<R>(&'a self, rng: &mut R) -> Result<Self::Output, Error>
    where
        R: rand::Rng + ?Sized,
    {
        let total_metrics = self.metrics_per_scope.sample(rng);
        let mut metrics = Vec::with_capacity(total_metrics as usize);
        for _ in 0..total_metrics {
            metrics.push(self.metric_generator.generate(rng)?);
        }

        let instrumentation_scope: InstrumentationScope = {
            let name = self
                .str_pool
                .of_size_range(rng, 1_u8..16)
                .ok_or(Error::StringGenerate)?;

            let count = self.attributes_per_scope.sample(rng);
            let attributes = generate_attributes(&self.str_pool, rng, count)?;

            InstrumentationScope {
                name: String::from(name),
                version: String::new(),
                attributes,
                dropped_attributes_count: 0,
            }
        };

        Ok(v1::ScopeMetrics {
            scope: Some(instrumentation_scope),
            metrics,
            schema_url: String::new(), // 0-sized alloc
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MetricGenerator {
    metric_weights: WeightedIndex<u16>,
    attributes_per_metric: ConfRange<u32>,
    unit_generator: UnitGenerator,
    str_pool: Rc<strings::Pool>,
}

impl MetricGenerator {
    pub(crate) fn new(config: Config, str_pool: &Rc<strings::Pool>) -> Result<Self, Error> {
        let member_choices = [
            u16::from(config.metric_weights.gauge),
            u16::from(config.metric_weights.sum),
        ];
        Ok(Self {
            str_pool: Rc::clone(str_pool),
            metric_weights: WeightedIndex::new(member_choices)?,
            unit_generator: UnitGenerator::new(),
            attributes_per_metric: config.contexts.attributes_per_metric,
        })
    }
}

impl<'a> Generator<'a> for MetricGenerator {
    type Output = v1::Metric;
    type Error = Error;

    fn generate<R>(&'a self, rng: &mut R) -> Result<Self::Output, Error>
    where
        R: rand::Rng + ?Sized,
    {
        let data = match self.metric_weights.sample(rng) {
            0 => v1::metric::Data::Gauge(rng.random::<Gauge>().0),
            1 => v1::metric::Data::Sum(rng.random::<Sum>().0),
            // Currently unsupported: Histogram, ExponentialHistogram, Summary
            _ => unreachable!(),
        };
        let data = Some(data);

        let name = self
            .str_pool
            .of_size_range(rng, 1_u8..16)
            .ok_or(Error::StringGenerate)?;
        let description = self
            .str_pool
            .of_size_range(rng, 1_u8..16)
            .ok_or(Error::StringGenerate)?;

        // Units are optional, "" is the None unit.
        let unit = if rng.random_bool(0.1) {
            self.unit_generator.generate(rng)?
        } else {
            ""
        };
        let count = self.attributes_per_metric.sample(rng);
        let metadata = generate_attributes(&self.str_pool, rng, count)?;

        Ok(v1::Metric {
            name: String::from(name),
            description: String::from(description),
            unit: String::from(unit),
            data,
            metadata,
        })
    }
}

// Wrappers allowing us to implement Distribution
struct NumberDataPoint(v1::NumberDataPoint);
struct Gauge(v1::Gauge);
struct Sum(v1::Sum);

impl Distribution<NumberDataPoint> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> NumberDataPoint
    where
        R: Rng + ?Sized,
    {
        let value = match rng.random_range(0..=1) {
            0 => v1::number_data_point::Value::AsDouble(rng.random()),
            1 => v1::number_data_point::Value::AsInt(rng.random()),
            _ => unreachable!(),
        };

        NumberDataPoint(v1::NumberDataPoint {
            // NOTE absent a reason to set attributes to not-empty, it's unclear
            // that we should.
            attributes: Vec::new(),
            start_time_unix_nano: 0, // epoch instant
            time_unix_nano: rng.random(),
            // Unclear that this needs to be set.
            exemplars: Vec::new(),
            // Equivalent to DoNotUse, the flag is ignored. If we ever set
            // `value` to None this must be set to
            // DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK
            flags: 0,
            value: Some(value),
        })
    }
}

impl Distribution<Gauge> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> Gauge
    where
        R: Rng + ?Sized,
    {
        let total = rng.random_range(0..64);
        let data_points = StandardUniform
            .sample_iter(rng)
            .map(|ndp: NumberDataPoint| ndp.0)
            .take(total)
            .collect();
        Gauge(v1::Gauge { data_points })
    }
}

impl Distribution<Sum> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> Sum
    where
        R: Rng + ?Sized,
    {
        // 0: Unspecified AggregationTemporality, MUST not be used
        // 1: Delta
        // 2: Cumulative
        let aggregation_temporality = *[1, 2]
            .choose(rng)
            .expect("failed to choose aggregation temporality");
        let is_monotonic = rng.random();
        let total = rng.random_range(0..64);
        let data_points = StandardUniform
            .sample_iter(rng)
            .map(|ndp: NumberDataPoint| ndp.0)
            .take(total)
            .collect();

        Sum(v1::Sum {
            data_points,
            aggregation_temporality,
            is_monotonic,
        })
    }
}

#[derive(Debug, Clone)]
/// OTLP metric payload
pub struct OpentelemetryMetrics {
    pool: Vec<templates::ResourceTemplate>,
    // // TODO insert a 'templates' field that will be a Vec of a yet-to-be defined
    // generator: ResourceGenerator,
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
        // NOTE okay this huge constructor is a mess. What I want to do is move
        // the bulk of this to generate and generate _if_ we have not reached
        // the cap else choose from the templates. But I've got seriously turned
        // around somewhere.

        //
        // Ok(Self {
        //     generator: ResourceGenerator::new(config, &str_pool)?,
        // })

        let context_cap = config.contexts.total_contexts.sample(rng);
        // NOTE does this need to be Rc? I think not, since we get the strings
        // we need immediately.
        let str_pool = Rc::new(strings::Pool::with_size(rng, 1_000_000));
        let unit_gen = UnitGenerator::new();
        let metric_kinds = WeightedIndex::new([
            u16::from(config.metric_weights.gauge),
            u16::from(config.metric_weights.sum),
        ])?;

        let mut pool = Vec::with_capacity(context_cap as usize);
        for _ in 0..context_cap {
            let unknown_resource = config.contexts.attributes_per_resource.start() == 0;
            // If the range of resources admits 0 the field `unknown_resources` will
            // be set and we give half-odds on there be no `resource` field set in
            // the message.
            let resource: Option<resource::v1::Resource> =
                if !unknown_resource && rng.random_bool(0.5) {
                    let count = config.contexts.attributes_per_resource.sample(rng);
                    let attributes = generate_attributes(&str_pool, rng, count)?;
                    let res = resource::v1::Resource {
                        attributes,
                        dropped_attributes_count: 0,
                    };
                    Some(res)
                } else {
                    None
                };
            let total_scopes = config.contexts.scopes_per_resource.sample(rng);
            let mut scopes = Vec::with_capacity(total_scopes as usize);
            for _ in 0..total_scopes {
                // InstrumentationScope
                let total_scope_attrs = config.contexts.attributes_per_scope.sample(rng);
                let scope_attrs = generate_attributes(&str_pool, rng, total_scope_attrs)?;
                let scope = InstrumentationScope {
                    name: str_pool
                        .of_size_range(rng, 1_u8..16)
                        .ok_or(Error::StringGenerate)?
                        .to_owned(),
                    version: String::new(),
                    attributes: scope_attrs,
                    dropped_attributes_count: 0,
                };
                // Metrics
                let total_metrics = config.contexts.metrics_per_scope.sample(rng);
                let mut metric_tpls = Vec::with_capacity(total_metrics as usize);
                for _ in 0..total_metrics {
                    let meta_count = config.contexts.attributes_per_metric.sample(rng);
                    let metadata = generate_attributes(&str_pool, rng, meta_count)?;
                    let name = str_pool
                        .of_size_range(rng, 1_u8..16)
                        .ok_or(Error::StringGenerate)?
                        .to_owned();

                    let description = if rng.random_bool(0.1) {
                        str_pool
                            .of_size_range(rng, 1_u8..16)
                            .ok_or(Error::StringGenerate)?
                            .to_owned()
                    } else {
                        String::new()
                    };
                    let unit = if rng.random_bool(0.1) {
                        unit_gen.generate(rng)?.to_owned()
                    } else {
                        String::new()
                    };

                    let kind = match metric_kinds.sample(rng) {
                        0 => Kind::Gauge,
                        1 => Kind::Sum {
                            aggregation_temporality: *[1, 2].choose(rng).unwrap(),
                            is_monotonic: rng.random_bool(0.5),
                        },
                        _ => unreachable!(),
                    };

                    metric_tpls.push(MetricTemplate {
                        name,
                        description,
                        unit,
                        metadata,
                        kind,
                    });
                }

                scopes.push(ScopeTemplate {
                    scope,
                    metrics: metric_tpls,
                });
            }
            pool.push(ResourceTemplate { resource, scopes });
        }
        Ok(Self { pool })
    }
}

impl<'a> Generator<'a> for OpentelemetryMetrics {
    type Output = v1::ResourceMetrics;
    type Error = Error;

    fn generate<R>(&'a self, rng: &mut R) -> Result<Self::Output, Error>
    where
        R: rand::Rng + ?Sized,
    {
        let tpl = self
            .pool
            .choose(rng)
            .expect("template pool cannot be empty")
            .clone();

        let mut scopes = Vec::with_capacity(tpl.scopes.len());
        for s_tpl in tpl.scopes {
            let mut metrics = Vec::with_capacity(s_tpl.metrics.len());
            for m_tpl in s_tpl.metrics {
                metrics.push(m_tpl.instantiate(rng));
            }
            scopes.push(v1::ScopeMetrics {
                scope: Some(s_tpl.scope.clone()),
                metrics,
                schema_url: String::new(),
            });
        }

        Ok(v1::ResourceMetrics {
            resource: tpl.resource,
            scope_metrics: scopes,
            schema_url: String::new(),
        })
    }
}

impl crate::Serialize for OpentelemetryMetrics {
    fn to_bytes<W, R>(&mut self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        // What we're making here is the ExportMetricServiceRequest. It has 5
        // bytes of fixed values plus a varint-encoded message length field to
        // it. The worst case for the message length field is the max message
        // size divided by 0b0111_1111.
        //
        // The user _does not_ set the number of Resoures per request -- we pack
        // those in until max_bytes -- but they do set the scopes per request
        // etc. All of that is transparent here, handled by the generators
        // above.
        let bytes_remaining = max_bytes.checked_sub(5 + max_bytes.div_ceil(0b0111_1111));
        let Some(mut bytes_remaining) = bytes_remaining else {
            return Ok(());
        };

        let mut acc = Vec::with_capacity(128); // arbitrary constant
        loop {
            let resource: v1::ResourceMetrics = self.generate(&mut rng)?;
            let len = resource.encoded_len() + 2;
            match bytes_remaining.checked_sub(len) {
                Some(remainder) => {
                    acc.push(resource);
                    bytes_remaining = remainder;
                }
                None => break,
            }
        }

        let proto =
            opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest {
                resource_metrics: acc,
            };
        let buf = proto.encode_to_vec();
        writer.write_all(&buf)?;
        Ok(())
    }
}

fn generate_attributes<R>(
    str_pool: &strings::Pool,
    rng: &mut R,
    count: u32,
) -> Result<Vec<KeyValue>, Error>
where
    R: Rng + ?Sized,
{
    let mut kvs = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let key = str_pool
            .of_size_range(rng, 1_u8..16)
            .ok_or(Error::StringGenerate)?;
        let val = str_pool
            .of_size_range(rng, 1_u8..16)
            .ok_or(Error::StringGenerate)?;

        kvs.push(KeyValue {
            key: String::from(key),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(String::from(val))),
            }),
        });
    }
    Ok(kvs)
}

#[cfg(test)]
mod test {
    use super::{Config, Contexts, OpentelemetryMetrics};
    use crate::{
        Generator, Serialize,
        common::config::ConfRange,
        opentelemetry_metric::v1::{ResourceMetrics, metric},
    };
    use opentelemetry_proto::tonic::common::v1::any_value;
    use proptest::prelude::*;
    use rand::{SeedableRng, rngs::SmallRng};
    use std::{
        collections::HashSet,
        hash::{DefaultHasher, Hash, Hasher},
    };

    // Generation of metrics must be deterministic. In order to assert this
    // property we generate two instances of OpentelemetryMetrics from disinct
    // rngs and drive them forward for a random amount of steps, asserting equal
    // outcomes.
    proptest! {
        #[test]
        fn opentelemetry_metrics_generate_is_deterministic(
            seed: u64,
            total_contexts in 1..1_000_u32,
            attributes_per_resource in 0..20_u32,
            scopes_per_resource in 0..20_u32,
            attributes_per_scope in 0..20_u32,
            metrics_per_scope in 0..20_u32,
            attributes_per_metric in 0..10_u32,
            steps in 1..u8::MAX
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

            let mut rng1 = SmallRng::seed_from_u64(seed);
            let otel_metrics1 = OpentelemetryMetrics::new(config.clone(), &mut rng1)?;
            let mut rng2 = SmallRng::seed_from_u64(seed);
            let otel_metrics2 = OpentelemetryMetrics::new(config.clone(), &mut rng2)?;

            for _ in 0..steps {
                let gen_1 = otel_metrics1.generate(&mut rng1)?;
                let gen_2 = otel_metrics2.generate(&mut rng2)?;
                prop_assert_eq!(gen_1, gen_2);
            }
        }
    }

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut metrics = OpentelemetryMetrics::new(Config::default(), &mut rng).expect("failed to create metrics generator");

            let mut bytes = Vec::with_capacity(max_bytes);
            metrics.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");
            prop_assert!(bytes.len() <= max_bytes, "max len: {max_bytes}, actual: {}", bytes.len());
        }
    }

    // Generation of metrics must be context bounded. If `generate` is called
    // more than total_context times only total_context contexts should be
    // produced.
    proptest! {
        #[test]
        fn contexts_bound_metric_generation(
            seed: u64,
            total_contexts_min in 0..4_u32,
            total_contexts_max in 5..32_u32,
            attributes_per_resource in 0..25_u32,
            scopes_per_resource in 0..50_u32,
            attributes_per_scope in 0..20_u32,
            metrics_per_scope in 1..100_u32,
            attributes_per_metric in 0..100_u32,
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
            let otel_metrics = OpentelemetryMetrics::new(config.clone(), &mut rng)?;

            let total_generations = total_contexts_max + (total_contexts_max / 2);
            for _ in 0..total_generations {
                let res = otel_metrics.generate(&mut rng)?;
                let id = context_id(&res);
                ids.insert(id);
            }

            let actual_contexts = ids.len();
            let below = total_contexts_min as usize <= actual_contexts;
            let above = actual_contexts <= total_contexts_max as usize;
            prop_assert!(below && above,
                         "expected {} ≤ {} ≤ {}",
                         total_contexts_min, actual_contexts, total_contexts_max);
        }
    }

    // NOTE disabled temporarily, will re-enable in up-stack commit
    // // We want to be sure that the payloads are not being left empty.
    // proptest! {
    //     #[test]
    //     fn payload_is_at_least_half_of_max_bytes(seed: u64, max_bytes in 16u16..u16::MAX) {
    //         let max_bytes = max_bytes as usize;
    //         let mut rng = SmallRng::seed_from_u64(seed);
    //         let mut metrics = OpentelemetryMetrics::new(Config::default(), &mut rng).expect("failed to create metrics generator");

    //         let mut bytes = Vec::with_capacity(max_bytes);
    //         metrics.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");

    //         assert!(!bytes.is_empty());
    //     }
    // }

    // // We want to know that every payload produced by this type actually
    // // deserializes as a collection of OTEL metrics.
    // proptest! {
    //     #[test]
    //     fn payload_deserializes(seed: u64, max_bytes: u16)  {
    //         let max_bytes = max_bytes as usize;
    //         let mut rng = SmallRng::seed_from_u64(seed);
    //         let mut metrics = OpentelemetryMetrics::new(Config::default(), &mut rng).expect("failed to create metrics generator");

    //         let mut bytes: Vec<u8> = Vec::with_capacity(max_bytes);
    //         metrics.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");

    //         opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest::decode(bytes.as_slice()).expect("failed to decode the message from the buffer");
    //     }
    // }

    // Confirm that configuration bounds are naively obeyed. For instance, this
    // test will pass if every resource generated has identical attributes, etc
    // etc. Confirmation of context bounds being obeyed -- implying examination
    // of attributes et al -- is appropriate for another property test.
    proptest! {
        #[test]
        fn counts_within_bounds(
            seed: u64,
            total_contexts in 1..1_000_u32,
            attributes_per_resource in 0..20_u32,
            scopes_per_resource in 0..20_u32,
            attributes_per_scope in 0..20_u32,
            metrics_per_scope in 0..20_u32,
            attributes_per_metric in 0..10_u32,
            steps in 1..u8::MAX
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

            let mut rng = SmallRng::seed_from_u64(seed);
            let otel_metrics = OpentelemetryMetrics::new(config.clone(), &mut rng)?;

            for _ in 0..steps {
                let metric = otel_metrics.generate(&mut rng)?;

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

    /// Extracts and hashes the context from a ResourceMetrics.
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
            attributes_per_resource in 0..20_u32,
            scopes_per_resource in 0..20_u32,
            attributes_per_scope in 0..20_u32,
            metrics_per_scope in 0..20_u32,
            attributes_per_metric in 0..10_u32,
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

            let mut rng = SmallRng::seed_from_u64(seed);
            let otel_metrics = OpentelemetryMetrics::new(config.clone(), &mut rng)?;

            // Generate two identical metrics
            let metric1 = otel_metrics.generate(&mut rng)?;
            let mut rng = SmallRng::seed_from_u64(seed);
            let otel_metrics = OpentelemetryMetrics::new(config.clone(), &mut rng)?;
            let metric2 = otel_metrics.generate(&mut rng)?;

            // Ensure that the metrics are equal.
            assert_eq!(metric1, metric2);
            // If the metrics are equal then their contexts must be equal.
            assert_eq!(context_id(&metric1), context_id(&metric2));
        }
    }
}
