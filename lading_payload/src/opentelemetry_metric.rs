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
mod templates;
pub(crate) mod unit;

use std::io::Write;
use std::rc::Rc;

use crate::{Error, Generator, common::config::ConfRange, common::strings};
use opentelemetry_proto::tonic::metrics::v1;
use prost::Message;
use rand::{Rng, seq::IndexedRandom};
use serde::{Deserialize, Serialize as SerdeSerialize};
use templates::ResourceTemplateGenerator;
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
            ConfRange::Constant(0) => 0,
            ConfRange::Constant(n) => {
                let metrics = match self.contexts.metrics_per_scope {
                    ConfRange::Constant(0) => 0,
                    ConfRange::Constant(m) => m,
                    ConfRange::Inclusive { min, .. } => min,
                };
                n * metrics
            }
            ConfRange::Inclusive { min, .. } => {
                let metrics = match self.contexts.metrics_per_scope {
                    ConfRange::Constant(0) => 0,
                    ConfRange::Constant(m) => m,
                    ConfRange::Inclusive { min, .. } => min,
                };
                min * metrics
            }
        };

        let max_configured = match self.contexts.scopes_per_resource {
            ConfRange::Constant(0) => 0,
            ConfRange::Constant(n) => {
                let metrics = match self.contexts.metrics_per_scope {
                    ConfRange::Constant(0) => 0,
                    ConfRange::Constant(m) => m,
                    ConfRange::Inclusive { max, .. } => max,
                };
                n * metrics
            }
            ConfRange::Inclusive { max, .. } => {
                let metrics = match self.contexts.metrics_per_scope {
                    ConfRange::Constant(0) => 0,
                    ConfRange::Constant(m) => m,
                    ConfRange::Inclusive { max, .. } => max,
                };
                max * metrics
            }
        };

        // Validate that the requested contexts are achievable
        if min_contexts > u32::from(max_configured) {
            return Err(format!(
                "Minimum requested contexts {min_contexts} cannot be achieved with current configuration (max possible: {max_configured})"
            ));
        }

        if max_contexts < u32::from(min_configured) {
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
    pool: Vec<templates::ResourceTemplate>,
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
        let str_pool = Rc::new(strings::Pool::with_size(rng, 1_000_000));
        let resource_template_generator = ResourceTemplateGenerator::new(&config, &str_pool)?;

        let mut pool = Vec::with_capacity(context_cap as usize);
        for _ in 0..context_cap {
            let r = resource_template_generator.generate(rng)?;
            pool.push(r);
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
    fn to_bytes<W, R>(&self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
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
            let metrics = OpentelemetryMetrics::new(Config::default(), &mut rng).expect("failed to create metrics generator");

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
            total_contexts_min in 1..4_u32,
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
    //         let metrics = OpentelemetryMetrics::new(Config::default(), &mut rng).expect("failed to create metrics generator");

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
    //         let metrics = OpentelemetryMetrics::new(Config::default(), &mut rng).expect("failed to create metrics generator");

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
