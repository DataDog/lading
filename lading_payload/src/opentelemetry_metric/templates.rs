use std::{collections::BTreeMap, rc::Rc};

use opentelemetry_proto::tonic::{
    common::v1::InstrumentationScope,
    metrics::{
        self,
        v1::{Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric::Data},
    },
    resource,
};
use prost::Message;
use rand::{
    Rng,
    distr::{Distribution, StandardUniform, weighted::WeightedIndex},
    seq::{IndexedRandom, IteratorRandom},
};
use tracing::error;

use super::{Config, UnitGenerator, tags::TagGenerator};
use crate::{Error, Generator, SizedGenerator, common::config::ConfRange, common::strings};

const UNIQUE_TAG_RATIO: f32 = 0.75;

/// Errors related to template generators
#[derive(thiserror::Error, Debug, Clone, Copy)]
pub enum PoolError {
    #[error("Choice could not be made on empty container.")]
    EmptyChoice,
    #[error("Generation error: {0}")]
    Generator(#[from] GeneratorError),
}

#[derive(Debug, Clone)]
pub(crate) struct Pool {
    context_cap: u32,
    /// key: encoded size; val: templates with that size
    by_size: BTreeMap<usize, Vec<ResourceMetrics>>,
    generator: ResourceTemplateGenerator,
    len: u32,
}

impl Pool {
    /// Build an empty pool that can hold at most `context_cap` templates.
    pub(crate) fn new(context_cap: u32, generator: ResourceTemplateGenerator) -> Self {
        Self {
            context_cap,
            by_size: BTreeMap::new(),
            generator,
            len: 0,
        }
    }

    /// Return a `ResourceMetrics` from the pool.
    ///
    /// Instances of `ResourceMetrics` returned by this function are guaranteed
    /// to be of an encoded size no greater than budget. No greater than
    /// `context_cap` instances of `ResourceMetrics` will ever be stored in this
    /// structure.
    pub(crate) fn fetch<R>(
        &mut self,
        rng: &mut R,
        budget: &mut usize,
    ) -> Result<&ResourceMetrics, PoolError>
    where
        R: rand::Rng + ?Sized,
    {
        // If we are at context cap, search by_size for templates <= budget and
        // return a random choice. If we are not at context cap, call
        // ResourceMetrics::generator with the budget and then store the result
        // for future use in `by_size`.
        //
        // Size search is in the interval (0, budget].

        let upper = *budget;
        let mut limit = *budget;

        // Generate new instances until either context_cap is hit or the
        // remaining space drops below our lookup interval.
        if self.len < self.context_cap {
            match self.generator.generate(rng, &mut limit) {
                Ok(rm) => {
                    let sz = rm.encoded_len();
                    self.by_size.entry(sz).or_default().push(rm);
                    self.len += 1;
                }
                Err(e) => return Err(PoolError::Generator(e)),
            }
        }

        let (choice_sz, choices) = self
            .by_size
            .range(..=upper)
            .choose(rng)
            .ok_or(PoolError::EmptyChoice)?;

        let choice = choices.choose(rng).ok_or(PoolError::EmptyChoice)?;
        *budget = budget.saturating_sub(*choice_sz);

        Ok(choice)
    }
}

/// Errors related to template generators
#[derive(thiserror::Error, Debug, Clone, Copy)]
pub enum GeneratorError {
    #[error("Generator exhausted bytes budget prematurely")]
    SizeExhausted,
    /// failed to generate string
    #[error("Failed to generate string")]
    StringGenerate,
}

struct Ndp(NumberDataPoint);
impl Distribution<Ndp> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> Ndp
    where
        R: Rng + ?Sized,
    {
        let value = match rng.random_range(0..=1) {
            0 => metrics::v1::number_data_point::Value::AsDouble(rng.random()),
            1 => metrics::v1::number_data_point::Value::AsInt(rng.random()),
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
    str_pool: Rc<strings::Pool>,
    tags: TagGenerator,
}

impl MetricTemplateGenerator {
    pub(crate) fn new<R>(
        config: &Config,
        str_pool: &Rc<strings::Pool>,
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
            Rc::clone(str_pool),
            UNIQUE_TAG_RATIO,
        )?;

        Ok(Self {
            kind_dist: WeightedIndex::new([
                u16::from(config.metric_weights.gauge),
                u16::from(config.metric_weights.sum),
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
        let original_budget = *budget;
        let mut inner_budget = *budget;

        let metadata = match self.tags.generate(rng, &mut inner_budget) {
            Ok(md) => md,
            Err(GeneratorError::SizeExhausted) => {
                error!("Tag generator unable to satify request for {inner_budget} size");
                Err(GeneratorError::SizeExhausted)?
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
                aggregation_temporality: *[1, 2].choose(rng).expect("cannot fail"),
                is_monotonic: rng.random_bool(0.5),
            },
            _ => unreachable!(),
        };

        let total_data_points = rng.random_range(1..60);
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
        };
        let metric = Metric {
            name,
            description,
            unit,
            data: Some(data),
            metadata,
        };

        let required_bytes = metric.encoded_len();
        let diff = original_budget.saturating_sub(required_bytes);

        if diff > 0 {
            *budget = diff;
            Ok(metric)
        } else {
            error!(
                "MetricTemplateGenerator unable to satisfy request for {original_budget} bytes."
            );
            Err(Self::Error::SizeExhausted)
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Kind {
    Gauge,
    Sum {
        aggregation_temporality: i32,
        is_monotonic: bool,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct ScopeTemplateGenerator {
    metrics_per_scope: ConfRange<u8>,
    metric_generator: MetricTemplateGenerator,
    str_pool: Rc<strings::Pool>,
    tags: TagGenerator,
    attributes_per_scope: ConfRange<u8>,
}

impl ScopeTemplateGenerator {
    pub(crate) fn new<R>(
        config: &Config,
        str_pool: &Rc<strings::Pool>,
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
            Rc::clone(str_pool),
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

    #[tracing::instrument(skip(self, rng))]
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
                    error!("Tag generator unable to satify request for {inner_budget} size");
                    Err(GeneratorError::SizeExhausted)?
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

        let mut total_metrics = self.metrics_per_scope.sample(rng);
        let mut metrics: Vec<Metric> = Vec::with_capacity(total_metrics as usize);
        // Search for the most metrics we can fit. If the metric_generator
        // returns SizeExhausted we cut total_metrics by 1/4 and try again until
        // we hit total_metrics == 0, at which point this generator signals
        // SizeExhausted.
        'outer: while total_metrics > 0 {
            for _ in 0..total_metrics {
                match self.metric_generator.generate(rng, &mut inner_budget) {
                    Ok(m) => metrics.push(m),
                    Err(GeneratorError::SizeExhausted) => {
                        total_metrics /= 4;
                        if total_metrics == 0 {
                            error!(
                                "ScopeTemplateGenerator unable to satisfy erquest for {original_budget} bytes"
                            );
                            return Err(GeneratorError::SizeExhausted);
                        }
                        continue 'outer;
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        let scope_metrics = ScopeMetrics {
            scope,
            metrics,
            schema_url: String::new(),
        };

        let required_bytes = scope_metrics.encoded_len();
        let diff = original_budget.saturating_sub(required_bytes);

        if diff > 0 {
            *budget = diff;
            Ok(scope_metrics)
        } else {
            Err(Self::Error::SizeExhausted)
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
        str_pool: &Rc<strings::Pool>,
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
            Rc::clone(str_pool),
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
        let resource = if self.attributes_per_resource.start() == 0 {
            None
        } else {
            match self.tags.generate(rng, budget) {
                Ok(attributes) => {
                    let res = resource::v1::Resource {
                        attributes: attributes.as_slice().to_owned(),
                        dropped_attributes_count: 0,
                    };
                    Some(res)
                }
                Err(GeneratorError::SizeExhausted) => None,
                Err(e) => return Err(e),
            }
        };

        // NOTE we are aware that this may under-generate unique contexts. In
        // particular, imagine that the user has configured things such that the
        // natural entropy of the pool is very low, meaning most scopes produced
        // are "equal". We will produce total_scopes scopes that have less
        // uniqueness than the amount generated.
        //
        // This is a chronic issue with our current approach across multiple
        // payloads and will require a deep think to repair.
        let total_scopes = self.scopes_per_resource.sample(rng);
        let mut scopes = Vec::with_capacity(total_scopes as usize);
        for _ in 0..total_scopes {
            match self.scope_generator.generate(rng, budget) {
                Ok(s) => scopes.push(s),
                Err(GeneratorError::SizeExhausted) => break,
                Err(e) => return Err(e),
            }
        }

        let resource_metrics = ResourceMetrics {
            resource,
            scope_metrics: scopes,
            schema_url: String::new(),
        };

        let required_bytes = resource_metrics.encoded_len();
        let diff = budget.saturating_sub(required_bytes);

        if diff > 0 {
            *budget = diff;
            Ok(resource_metrics)
        } else {
            Err(Self::Error::SizeExhausted)
        }
    }
}
