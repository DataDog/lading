use std::{cmp, rc::Rc};

use opentelemetry_proto::tonic::{
    common::v1::InstrumentationScope,
    metrics::{
        self,
        v1::{
            ExponentialHistogramDataPoint, HistogramDataPoint, Metric,
            NumberDataPoint, ResourceMetrics, ScopeMetrics, SummaryDataPoint,
            exponential_histogram_data_point, metric::Data, number_data_point, summary_data_point,
        },
    },
    resource,
};
use prost::Message;
use rand::{
    Rng,
    distr::{Distribution, StandardUniform, weighted::WeightedIndex},
};
use tracing::debug;

use super::{Config, UnitGenerator};
use crate::opentelemetry::common::{GeneratorError, TagGenerator, UNIQUE_TAG_RATIO, templates};
use crate::{Error, Generator, common::config::ConfRange, common::strings};

pub(crate) type Pool = templates::Pool<ResourceMetrics, ResourceTemplateGenerator>;

/// Generate a random number between min and max (inclusive) with heavy bias
/// toward min. Uses exponential decay: each doubling of the range has half the
/// probability.
///
/// For example, with min=1, max=60:
/// - ~50% chance of returning 1
/// - ~25% chance of returning 2-3
/// - ~12.5% chance of returning 4-7
/// - And so on...
fn exponential_weighted_range<R: Rng + ?Sized>(rng: &mut R, min: u32, max: u32) -> u32 {
    if min >= max {
        return min;
    }

    let mut current = min;
    let mut step = 1;

    while current < max {
        if rng.random_bool(0.5) {
            return rng.random_range(current..=current.min(max));
        }
        current = (current + step).min(max);
        step *= 2;
    }

    max
}

struct Ndp(NumberDataPoint);
impl Distribution<Ndp> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> Ndp
    where
        R: Rng + ?Sized,
    {
        let value = match rng.random_range(0..=1) {
            0 => number_data_point::Value::AsDouble(0.0),
            1 => number_data_point::Value::AsInt(0),
            _ => unreachable!(),
        };

        Ndp(NumberDataPoint {
            // NOTE absent a reason to set attributes to not-empty, it's unclear
            // that we should.
            attributes: Vec::new(),
            start_time_unix_nano: 0, // epoch instant
            time_unix_nano: rng.random(),
            // Unclear that this needs to be set.
            exemplars: Vec::new(),
            // Equivalent to DoNotUse, the flag is ignored. This is discussed in
            // the upstream OTLP protobuf definition, which we inherit from the
            // SDK. If we ever set `value` to None this must be set to
            // DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK
            flags: 0,
            value: Some(value),
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MetricTemplateGenerator {
    kind_dist: WeightedIndex<u16>,
    unit_gen: UnitGenerator,
    str_pool: Rc<strings::RandomStringPool>,
    tags: TagGenerator,
}

impl MetricTemplateGenerator {
    pub(crate) fn new<R>(
        config: &Config,
        str_pool: &Rc<strings::RandomStringPool>,
        rng: &mut R,
    ) -> Result<Self, Error>
    where
        R: Rng + ?Sized,
    {
        let tags = TagGenerator::new(
            rng.random(),
            config.contexts.attributes_per_metric,
            ConfRange::Inclusive { min: 3, max: 32 },
            config.contexts.total_contexts.end() as usize,
            str_pool,
            UNIQUE_TAG_RATIO,
        )?;

        Ok(Self {
            kind_dist: WeightedIndex::new([
                u16::from(config.metric_weights.gauge),
                u16::from(config.metric_weights.sum_delta),
                u16::from(config.metric_weights.sum_cumulative),
                u16::from(config.metric_weights.summary),
                u16::from(config.metric_weights.exponential_histogram),
                u16::from(config.metric_weights.histogram),
            ])?,
            unit_gen: UnitGenerator::new(),
            str_pool: Rc::clone(str_pool),
            tags,
        })
    }
}

impl<'a> crate::SizedGenerator<'a> for MetricTemplateGenerator {
    type Output = Metric;
    type Error = GeneratorError;

    fn generate<R>(
        &'a mut self,
        rng: &mut R,
        budget: &mut usize,
    ) -> Result<Self::Output, Self::Error>
    where
        R: Rng + ?Sized,
    {
        // We record the original budget because if we bail out on generation we
        // are obligated by trait semantics to NOT alter the passed budget.
        let original_budget: usize = *budget;
        let mut inner_budget: usize = *budget;

        let metadata = match self.tags.generate(rng, &mut inner_budget) {
            Ok(md) => md,
            Err(GeneratorError::SizeExhausted) => {
                debug!("Tag generator unable to satify request for {inner_budget} size");
                Vec::new()
            }
            Err(e) => Err(e)?,
        };

        let name = self
            .str_pool
            .of_size_range(rng, 1_u8..16)
            .ok_or(Self::Error::StringGenerate)?
            .to_owned();
        let description = if rng.random_bool(0.1) {
            self.str_pool
                .of_size_range(rng, 1_u8..16)
                .ok_or(Self::Error::StringGenerate)?
                .to_owned()
        } else {
            String::new()
        };
        let unit = if rng.random_bool(0.1) {
            self.unit_gen.generate(rng)?.to_owned()
        } else {
            String::new()
        };

        let kind = match self.kind_dist.sample(rng) {
            0 => Kind::Gauge,
            1 => Kind::Sum {
                aggregation_temporality: 1,
                is_monotonic: rng.random_bool(0.5),
            },
            2 => Kind::Sum {
                aggregation_temporality: 2,
                is_monotonic: rng.random_bool(0.5),
            },
            3 => Kind::Histogram {
                aggregation_temporality: 2,
            },
            4 => Kind::ExponentialHistogram {
                aggregation_temporality: 2,
                scale: rng.random_range(-3_i32..=10),
            },
            5 => Kind::Summary,
            _ => unreachable!(),
        };

        // Use weighted distribution: heavily favors small numbers (1-2) but can go up to 60
        let total_data_points = exponential_weighted_range(rng, 1, 60);
        let data_points = (0..total_data_points)
            .map(|_| rng.random::<Ndp>().0)
            .collect();
        let data = match kind {
            Kind::Gauge => Data::Gauge(metrics::v1::Gauge { data_points }),
            Kind::Sum {
                aggregation_temporality,
                is_monotonic,
            } => Data::Sum(metrics::v1::Sum {
                data_points,
                aggregation_temporality,
                is_monotonic,
            }),
            Kind::Summary => {
                let data_points = (0..total_data_points)
                    .map(|_| {
                        let mut val = 0.0_f64;
                        let quantile_values = [0.0_f64, 0.25, 0.5, 0.75, 1.0]
                            .iter()
                            .map(|&q| {
                                val += rng.random_range(0.0_f64..=10.0);
                                summary_data_point::ValueAtQuantile {
                                    quantile: q,
                                    value: val,
                                }
                            })
                            .collect();
                        SummaryDataPoint {
                            attributes: Vec::new(),
                            start_time_unix_nano: 0,
                            time_unix_nano: rng.random(),
                            count: rng.random_range(1..=100),
                            sum: rng.random_range(0.0_f64..=10_000.0),
                            quantile_values,
                            flags: 0,
                        }
                    })
                    .collect();
                Data::Summary(metrics::v1::Summary { data_points })
            }
            Kind::Histogram {
                aggregation_temporality,
            } => {
                let data_points = (0..total_data_points as usize)
                    .map(|_| {
                        //Strictly increasing bounds
                        let n_bounds = rng.random_range(1_usize..=10);
                        let mut bounds: Vec<f64> = (0..n_bounds)
                            .map(|_| rng.random_range(0.0_f64..1000.0))
                            .collect();
                        bounds.sort_by(f64::total_cmp);
                        bounds.dedup();

                        let n_buckets = bounds.len() + 1;
                        let count: u64 = rng.random_range(1..=1000);
                        let bucket_counts = random_partition(count, n_buckets, rng);

                        HistogramDataPoint {
                            attributes: Vec::new(),
                            start_time_unix_nano: 0,
                            time_unix_nano: rng.random(),
                            count,
                            sum: Some(rng.random_range(0.0_f64..=10_000.0)),
                            explicit_bounds: bounds,
                            bucket_counts,
                            exemplars: Vec::new(),
                            flags: 0,
                            min: None,
                            max: None,
                        }
                    })
                    .collect();
                Data::Histogram(metrics::v1::Histogram {
                    data_points,
                    aggregation_temporality,
                })
            }
            Kind::ExponentialHistogram {
                aggregation_temporality,
                scale,
            } => {
                let data_points = (0..total_data_points as usize)
                    .map(|_| {
                        let n_buckets = rng.random_range(1_usize..=16);
                        let count: u64 = rng.random_range(1..=1000);

                        //Three-way partition

                        let pos_count = rng.random_range(0..=count);
                        let remaining = count - pos_count;
                        let neg_count = rng.random_range(0..=remaining);
                        let zero_count = remaining - neg_count;

                        let positive = if pos_count > 0 {
                            Some(exponential_histogram_data_point::Buckets {
                                offset: rng.random_range(-10_i32..=10),
                                bucket_counts: random_partition(pos_count, n_buckets, rng),
                            })
                        } else {
                            None
                        };

                        let negative = if neg_count > 0 {
                            Some(exponential_histogram_data_point::Buckets {
                                offset: rng.random_range(-10_i32..=10),
                                bucket_counts: random_partition(neg_count, n_buckets, rng),
                            })
                        } else {
                            None
                        };

                        ExponentialHistogramDataPoint {
                            attributes: Vec::new(),
                            start_time_unix_nano: 0,
                            time_unix_nano: rng.random(),
                            count,
                            sum: Some(rng.random_range(0.0_f64..=10_000.0)),
                            scale,
                            zero_count,
                            positive,
                            negative,
                            exemplars: Vec::new(),
                            flags: 0,
                            min: None,
                            max: None,
                            zero_threshold: 0.0,
                        }
                    })
                    .collect();

                Data::ExponentialHistogram(metrics::v1::ExponentialHistogram {
                    data_points,
                    aggregation_temporality,
                })
            }
        };
        let mut metric = Metric {
            name,
            description,
            unit,
            data: Some(data),
            metadata,
        };

        while data_points_total(&metric) > 0 {
            let required_bytes = metric.encoded_len();

            assert_eq!(original_budget, *budget);
            match original_budget.cmp(&required_bytes) {
                cmp::Ordering::Equal | cmp::Ordering::Greater => {
                    *budget -= required_bytes;
                    return Ok(metric);
                }
                cmp::Ordering::Less => {
                    // Too many metric points, go around the loop again and try
                    // again.
                    metric = cut_data_points(metric);
                }
            }
        }
        debug!("MetricTemplateGenerator unable to satisfy request for {original_budget} bytes.");
        Err(Self::Error::SizeExhausted)
    }
}

fn random_partition<R: Rng + ?Sized>(count: u64, n_buckets: usize, rng: &mut R) -> Vec<u64> {
    let mut remaining = count;
    let mut result = Vec::with_capacity(n_buckets);
    let mut buckets_left = n_buckets;
    while buckets_left > 1 {
        let consumed = rng.random_range(0..=remaining);
        result.push(consumed);
        remaining -= consumed;
        buckets_left -= 1;
    }
    result.push(remaining);
    result
}

fn data_points_total(metric: &Metric) -> usize {
    let data = &metric.data;
    match data {
        Some(
            Data::Gauge(metrics::v1::Gauge { data_points })
            | Data::Sum(metrics::v1::Sum { data_points, .. }),
        ) => data_points.len(),
        Some(Data::Histogram(metrics::v1::Histogram { data_points, .. })) => data_points.len(),
        Some(Data::ExponentialHistogram(metrics::v1::ExponentialHistogram {
            data_points, ..
        })) => data_points.len(),
        Some(Data::Summary(metrics::v1::Summary { data_points })) => data_points.len(),
        None => 0,
    }
}

fn cut_data_points(metric: Metric) -> Metric {
    let name = metric.name;
    let description = metric.description;
    let unit = metric.unit;
    let metadata = metric.metadata;
    let data = metric.data;

    let new_data = match data {
        Some(Data::Gauge(metrics::v1::Gauge { mut data_points })) => {
            let new_len = data_points.len() / 2;
            data_points.truncate(new_len);
            Some(Data::Gauge(metrics::v1::Gauge { data_points }))
        }
        Some(Data::Sum(metrics::v1::Sum {
            mut data_points,
            aggregation_temporality,
            is_monotonic,
        })) => {
            let new_len = data_points.len() / 2;
            data_points.truncate(new_len);
            Some(Data::Sum(metrics::v1::Sum {
                data_points,
                aggregation_temporality,
                is_monotonic,
            }))
        }
        Some(Data::ExponentialHistogram(metrics::v1::ExponentialHistogram {
            mut data_points,
            aggregation_temporality,
        })) => {
            let new_len = data_points.len() / 2;
            data_points.truncate(new_len);
            Some(Data::ExponentialHistogram(
                metrics::v1::ExponentialHistogram {
                    data_points,
                    aggregation_temporality,
                },
            ))
        }
        Some(Data::Histogram(metrics::v1::Histogram {
            mut data_points,
            aggregation_temporality,
        })) => {
            let new_len = data_points.len() / 2;
            data_points.truncate(new_len);
            Some(Data::Histogram(metrics::v1::Histogram {
                data_points,
                aggregation_temporality,
            }))
        }
        Some(Data::Summary(metrics::v1::Summary { mut data_points })) => {
            let new_len = data_points.len() / 2;
            data_points.truncate(new_len);
            Some(Data::Summary(metrics::v1::Summary { data_points }))
        }
        None => None,
    };

    Metric {
        name,
        description,
        unit,
        metadata,
        data: new_data,
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Kind {
    Gauge,
    Sum {
        aggregation_temporality: i32,
        is_monotonic: bool,
    },
    Histogram {
        aggregation_temporality: i32,
    },
    ExponentialHistogram {
        aggregation_temporality: i32,
        scale: i32,
    },
    Summary,
}

#[derive(Clone, Debug)]
pub(crate) struct ScopeTemplateGenerator {
    metrics_per_scope: ConfRange<u8>,
    metric_generator: MetricTemplateGenerator,
    str_pool: Rc<strings::RandomStringPool>,
    tags: TagGenerator,
    attributes_per_scope: ConfRange<u8>,
}

impl ScopeTemplateGenerator {
    pub(crate) fn new<R>(
        config: &Config,
        str_pool: &Rc<strings::RandomStringPool>,
        rng: &mut R,
    ) -> Result<Self, Error>
    where
        R: Rng + ?Sized,
    {
        let tags = TagGenerator::new(
            rng.random(),
            config.contexts.attributes_per_scope,
            ConfRange::Inclusive { min: 3, max: 32 },
            config.contexts.total_contexts.end() as usize,
            str_pool,
            UNIQUE_TAG_RATIO,
        )?;

        Ok(Self {
            metrics_per_scope: config.contexts.metrics_per_scope,
            metric_generator: MetricTemplateGenerator::new(config, str_pool, rng)?,
            str_pool: Rc::clone(str_pool),
            tags,
            attributes_per_scope: config.contexts.attributes_per_scope,
        })
    }
}

impl<'a> crate::SizedGenerator<'a> for ScopeTemplateGenerator {
    type Output = ScopeMetrics;
    type Error = GeneratorError;

    fn generate<R>(
        &'a mut self,
        rng: &mut R,
        budget: &mut usize,
    ) -> Result<Self::Output, Self::Error>
    where
        R: Rng + ?Sized,
    {
        // We record the original budget because if we bail out on generation we
        // are obligated by trait semantics to NOT alter the passed budget.
        let original_budget = *budget;
        let mut inner_budget = *budget;

        let scope = if self.attributes_per_scope.start() == 0 {
            None
        } else {
            let attributes = match self.tags.generate(rng, &mut inner_budget) {
                Ok(md) => md,
                Err(GeneratorError::SizeExhausted) => {
                    debug!("Tag generator unable to satify request for {inner_budget} size");
                    Vec::new()
                }
                Err(e) => Err(e)?,
            };
            Some(InstrumentationScope {
                name: self
                    .str_pool
                    .of_size_range(rng, 1_u8..16)
                    .ok_or(Self::Error::StringGenerate)?
                    .to_owned(),
                version: String::new(),
                attributes: attributes.as_slice().to_owned(),
                dropped_attributes_count: 0,
            })
        };

        let total_metrics = self.metrics_per_scope.sample(rng);
        let mut metrics: Vec<Metric> = Vec::with_capacity(total_metrics as usize);
        // Search for the most metrics we can fit. If the metric_generator
        // returns SizeExhausted we check to see if metrics was populated at all
        // and if it was not we signal SizeExhausted.
        for _ in 0..total_metrics {
            match self.metric_generator.generate(rng, &mut inner_budget) {
                Ok(m) => metrics.push(m),
                Err(GeneratorError::SizeExhausted) => break,
                Err(e) => return Err(e),
            }
        }
        if metrics.is_empty() {
            debug!(
                "ScopeTemplateGenerator unable to populate metrics with budget {original_budget}"
            );
            return Err(GeneratorError::SizeExhausted);
        }

        let mut scope_metrics = ScopeMetrics {
            scope,
            metrics,
            schema_url: String::new(),
        };
        loop {
            let required_bytes = scope_metrics.encoded_len();
            match original_budget.cmp(&required_bytes) {
                cmp::Ordering::Equal | cmp::Ordering::Greater => {
                    *budget -= required_bytes;
                    return Ok(scope_metrics);
                }
                cmp::Ordering::Less => {
                    if scope_metrics.metrics.pop().is_some() {
                        continue;
                    }
                    return Err(GeneratorError::SizeExhausted);
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ResourceTemplateGenerator {
    scopes_per_resource: ConfRange<u8>,
    attributes_per_resource: ConfRange<u8>,
    scope_generator: ScopeTemplateGenerator,
    tags: TagGenerator,
}

impl ResourceTemplateGenerator {
    pub(crate) fn new<R>(
        config: &Config,
        str_pool: &Rc<strings::RandomStringPool>,
        rng: &mut R,
    ) -> Result<Self, Error>
    where
        R: Rng + ?Sized,
    {
        let tags = TagGenerator::new(
            rng.random(),
            config.contexts.attributes_per_resource,
            ConfRange::Inclusive { min: 3, max: 32 },
            config.contexts.total_contexts.end() as usize,
            &Rc::clone(str_pool),
            UNIQUE_TAG_RATIO,
        )?;

        Ok(Self {
            scopes_per_resource: config.contexts.scopes_per_resource,
            attributes_per_resource: config.contexts.attributes_per_resource,
            scope_generator: ScopeTemplateGenerator::new(config, str_pool, rng)?,
            tags,
        })
    }
}

impl<'a> crate::SizedGenerator<'a> for ResourceTemplateGenerator {
    type Output = ResourceMetrics;
    type Error = GeneratorError;

    fn generate<R>(
        &'a mut self,
        rng: &mut R,
        budget: &mut usize,
    ) -> Result<Self::Output, Self::Error>
    where
        R: Rng + ?Sized,
    {
        // We record the original budget because if we bail out on generation we
        // are obligated by trait semantics to NOT alter the passed budget.
        let original_budget = *budget;
        let mut inner_budget = *budget;

        let resource = if self.attributes_per_resource.end() == 0 {
            None
        } else {
            match self.tags.generate(rng, &mut inner_budget) {
                Ok(attributes) => {
                    let res = resource::v1::Resource {
                        attributes: attributes.as_slice().to_owned(),
                        dropped_attributes_count: 0,
                        entity_refs: vec![],
                    };
                    Some(res)
                }
                Err(GeneratorError::SizeExhausted) => None,
                Err(e) => return Err(e),
            }
        };

        // Search for the most scopes we can fit. If the scope_generator
        // returns SizeExhausted we check to see if metrics was populated at all
        // and if it was not we signal SizeExhausted.
        let total_scopes = self.scopes_per_resource.sample(rng);
        let mut scopes = Vec::with_capacity(total_scopes as usize);
        for _ in 0..total_scopes {
            match self.scope_generator.generate(rng, &mut inner_budget) {
                Ok(s) => scopes.push(s),
                Err(GeneratorError::SizeExhausted) => break,
                Err(e) => return Err(e),
            }
        }
        if scopes.is_empty() {
            debug!(
                "ResourceTemplateGenerator unable to populate metrics with budget {original_budget}"
            );
            return Err(GeneratorError::SizeExhausted);
        }

        let mut resource_metrics = ResourceMetrics {
            resource,
            scope_metrics: scopes,
            schema_url: String::new(),
        };

        loop {
            let required_bytes = resource_metrics.encoded_len();

            match original_budget.cmp(&required_bytes) {
                cmp::Ordering::Equal | cmp::Ordering::Greater => {
                    *budget -= required_bytes;
                    return Ok(resource_metrics);
                }
                cmp::Ordering::Less => {
                    if resource_metrics.scope_metrics.pop().is_some() {
                        continue;
                    }
                    return Err(Self::Error::SizeExhausted);
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::SizedGenerator;
    use proptest::prelude::*;
    use rand::{SeedableRng, rngs::SmallRng};

    proptest! {
        #[test]
        fn metric_template_generator_generate(
            seed: u64,
            gauge in 0..2_u8,
            sum_delta in 0..2_u8,
            sum_cumulative in 0..2_u8,
        ) {
            if gauge == 0 && sum_delta == 0 && sum_cumulative == 0 {
                return Ok(());
            }

            let mut config = Config::default();
            config.metric_weights.gauge = gauge;
            config.metric_weights.sum_delta = sum_delta;
            config.metric_weights.sum_cumulative = sum_cumulative;

            let mut rng = SmallRng::seed_from_u64(seed);

            let generator_result = MetricTemplateGenerator::new(
                &config,
                &Rc::new(strings::RandomStringPool::with_size(&mut rng, 1024)),
                &mut rng,
            );
            assert!(generator_result.is_ok());
            let mut generator = generator_result.unwrap();

            for _ in 0..100 {
                let result = generator.generate(&mut rng, &mut 1024);
                assert!(result.is_ok());
                let metric = result.unwrap();
                assert!(metric.data.is_some());
                match metric.data.unwrap() {
                    Data::Gauge(_) => assert!(gauge >= 1),
                    Data::Sum(sum) => {
                        match sum.aggregation_temporality {
                            1 => assert!(sum_delta >= 1),
                            2 => assert!(sum_cumulative >= 1),
                            _ => panic!("invalid aggregation temporality"),
                        }
                    }
                    Data::Histogram(_) | Data::ExponentialHistogram(_) | Data::Summary(_) => {}
                }
            }
        }
    }
}
