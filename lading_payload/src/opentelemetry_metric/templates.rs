use std::rc::Rc;

use opentelemetry_proto::tonic::{
    common::v1::{self, InstrumentationScope},
    metrics::{
        self,
        v1::{Metric, NumberDataPoint, metric::Data},
    },
    resource,
};
use rand::{
    Rng,
    distr::{Distribution, StandardUniform, weighted::WeightedIndex},
    seq::IndexedRandom,
};

use super::{Config, UnitGenerator, tags::TagGenerator};
use crate::{Error, common::config::ConfRange, common::strings};

const UNIQUE_TAG_RATIO: f32 = 0.75;

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

/// Static description of a Metric (everything that defines a context).
#[derive(Debug, Clone)]
pub(crate) struct MetricTemplate {
    pub name: String,
    pub description: String,
    pub unit: String,
    pub metadata: Vec<v1::KeyValue>,
    pub kind: Kind,
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

impl<'a> crate::Generator<'a> for MetricTemplateGenerator {
    type Output = MetricTemplate;
    type Error = Error;

    fn generate<R>(&'a self, rng: &mut R) -> Result<Self::Output, Error>
    where
        R: Rng + ?Sized,
    {
        let metadata = self.tags.generate(rng)?;

        let name = self
            .str_pool
            .of_size_range(rng, 1_u8..16)
            .ok_or(Error::StringGenerate)?
            .to_owned();
        let description = if rng.random_bool(0.1) {
            self.str_pool
                .of_size_range(rng, 1_u8..16)
                .ok_or(Error::StringGenerate)?
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

        Ok(MetricTemplate {
            name,
            description,
            unit,
            metadata,
            kind,
        })
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

impl MetricTemplate {
    /// Instantiate the template into a concrete `v1::Metric`
    pub(crate) fn instantiate<R: Rng + ?Sized>(&self, rng: &mut R) -> Metric {
        let total_data_points = rng.random_range(0..60);
        let data_points = (0..total_data_points)
            .map(|_| rng.random::<Ndp>().0)
            .collect();
        let data = match self.kind {
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
        Metric {
            name: self.name.clone(),
            description: self.description.clone(),
            unit: self.unit.clone(),
            data: Some(data),
            metadata: self.metadata.clone(),
        }
    }
}

/// Static description of a Scope and its metrics.
#[derive(Debug, Clone)]
pub(crate) struct ScopeTemplate {
    pub scope: Option<InstrumentationScope>,
    pub metrics: Vec<MetricTemplate>,
}

#[derive(Clone)]
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

impl<'a> crate::Generator<'a> for ScopeTemplateGenerator {
    type Output = ScopeTemplate;
    type Error = Error;

    fn generate<R>(&'a self, rng: &mut R) -> Result<Self::Output, Error>
    where
        R: Rng + ?Sized,
    {
        let scope = if self.attributes_per_scope.start() == 0 {
            None
        } else {
            let attributes = self.tags.generate(rng)?;
            Some(InstrumentationScope {
                name: self
                    .str_pool
                    .of_size_range(rng, 1_u8..16)
                    .ok_or(Error::StringGenerate)?
                    .to_owned(),
                version: String::new(),
                attributes,
                dropped_attributes_count: 0,
            })
        };

        let total_metrics = self.metrics_per_scope.sample(rng);
        let mut metrics: Vec<MetricTemplate> = Vec::with_capacity(total_metrics as usize);
        for _ in 0..total_metrics {
            let m = self.metric_generator.generate(rng)?;
            metrics.push(m);
        }

        Ok(ScopeTemplate { scope, metrics })
    }
}

/// Static description of an OTLP Resource (context top-level).
#[derive(Debug, Clone)]
pub(crate) struct ResourceTemplate {
    pub resource: Option<resource::v1::Resource>,
    pub scopes: Vec<ScopeTemplate>,
}

#[derive(Clone)]
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

impl<'a> crate::Generator<'a> for ResourceTemplateGenerator {
    type Output = ResourceTemplate;
    type Error = Error;

    fn generate<R>(&'a self, rng: &mut R) -> Result<Self::Output, Error>
    where
        R: Rng + ?Sized,
    {
        let resource = if self.attributes_per_resource.start() == 0 {
            None
        } else {
            let attributes = self.tags.generate(rng)?;
            let res = resource::v1::Resource {
                attributes,
                dropped_attributes_count: 0,
            };
            Some(res)
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
            let s = self.scope_generator.generate(rng)?;
            scopes.push(s);
        }

        Ok(ResourceTemplate { resource, scopes })
    }
}
