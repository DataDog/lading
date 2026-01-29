//! `DogStatsD` metric.
use std::fmt;

use rand::{
    Rng,
    distr::{OpenClosed01, weighted::WeightedIndex},
    prelude::Distribution,
    seq::IteratorRandom,
};

use crate::{Error, Generator, common::strings, dogstatsd::metric::template::Template};
use tracing::debug;

use self::strings::{Pool, choose_or_not_ref};

use super::{
    ConfRange, StringPools, ValueConf,
    common::{self, NumValueGenerator},
};

mod template;

#[derive(Clone, Debug)]
pub(crate) struct MetricGenerator {
    pub(crate) container_ids: Vec<String>,
    pub(crate) templates: Vec<template::Template>,
    pub(crate) multivalue_count: ConfRange<u16>,
    pub(crate) multivalue_pack_probability: f32,
    pub(crate) sampling: ConfRange<f32>,
    pub(crate) sampling_probability: f32,
    pub(crate) num_value_generator: NumValueGenerator,
    pub(crate) pools: StringPools,
    /// Tags for each template. Each position in this Vec corresponds to the
    /// same position in the templates Vec. The handles are resolved to strings
    /// via `str_pool` during serialization.
    pub(crate) tags: Vec<common::tags::Tagset>,
}

impl MetricGenerator {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new<R>(
        num_contexts: usize,
        name_length: ConfRange<u16>,
        multivalue_count: ConfRange<u16>,
        multivalue_pack_probability: f32,
        sampling: ConfRange<f32>,
        sampling_probability: f32,
        metric_weights: &WeightedIndex<u16>,
        container_ids: Vec<String>,
        tags_generator: &mut common::tags::Generator,
        pools: &StringPools,
        value_conf: ValueConf,
        mut rng: &mut R,
    ) -> Result<Self, Error>
    where
        R: Rng + ?Sized,
    {
        let mut templates = Vec::with_capacity(num_contexts);
        let mut tags = Vec::with_capacity(num_contexts);

        debug!("Generating metric templates for {num_contexts} contexts.",);
        for _ in 0..num_contexts {
            let template_tags = tags_generator.generate(&mut rng)?;
            let name_sz = name_length.sample(&mut rng) as usize;
            let (_, name_handle) = pools
                .name_pool
                .of_size_with_handle(&mut rng, name_sz)
                .ok_or(Error::StringGenerate)?;
            tags.push(template_tags);

            let res = match metric_weights.sample(rng) {
                0 => Template::Count(template::Count { name: name_handle }),
                1 => Template::Gauge(template::Gauge { name: name_handle }),
                2 => Template::Timer(template::Timer { name: name_handle }),
                3 => Template::Distribution(template::Dist { name: name_handle }),
                4 => Template::Set(template::Set { name: name_handle }),
                5 => Template::Histogram(template::Histogram { name: name_handle }),
                _ => unreachable!(),
            };
            templates.push(res);
        }

        Ok(MetricGenerator {
            container_ids,
            templates,
            multivalue_count,
            multivalue_pack_probability,
            sampling,
            sampling_probability,
            num_value_generator: NumValueGenerator::new(value_conf),
            pools: pools.clone(),
            tags,
        })
    }
}

impl<'a> Generator<'a> for MetricGenerator {
    type Output = Metric<'a>;
    type Error = Error;

    #[allow(clippy::too_many_lines)]
    fn generate<R>(&'a self, mut rng: &mut R) -> Result<Self::Output, Self::Error>
    where
        R: rand::Rng + ?Sized,
    {
        // SAFETY: If `self.templates` is ever empty this is a serious logic bug
        // and the program should crash prior to this point.
        let template_idx = (0..self.templates.len())
            .choose(&mut rng)
            .expect("failed to choose template index");
        let template = &self.templates[template_idx];
        let tags = &self.tags[template_idx];

        let container_id = choose_or_not_ref(&mut rng, &self.container_ids).map(String::as_str);
        // https://docs.datadoghq.com/metrics/custom_metrics/dogstatsd_metrics_submission/#sample-rates
        let prob: f32 = OpenClosed01.sample(&mut rng);
        let sample_rate = if prob < self.sampling_probability {
            let sample_rate = self.sampling.sample(&mut rng).clamp(0.0, 1.0);
            let sample_rate = common::ZeroToOne::try_from(sample_rate)
                .expect("failed to convert sample rate to ZeroToOne");
            Some(sample_rate)
        } else {
            None
        };

        let mut values = Vec::with_capacity(self.multivalue_count.end() as usize);
        let value: common::NumValue = self.num_value_generator.generate(&mut rng)?;
        values.push(value);

        let prob: f32 = OpenClosed01.sample(&mut rng);
        if prob < self.multivalue_pack_probability {
            let num_desired_values = self.multivalue_count.sample(&mut rng) as usize;
            for _ in 1..num_desired_values {
                values.push(self.num_value_generator.generate(&mut rng)?);
            }
        }

        match template {
            Template::Count(count) => {
                let name = self
                    .pools
                    .name_pool
                    .using_handle(count.name)
                    .ok_or(Error::StringGenerate)?;
                Ok(Metric::Count(Count {
                    name,
                    values,
                    sample_rate,
                    tags,
                    pools: &self.pools,
                    container_id,
                }))
            }
            Template::Gauge(gauge) => {
                let name = self
                    .pools
                    .name_pool
                    .using_handle(gauge.name)
                    .ok_or(Error::StringGenerate)?;
                Ok(Metric::Gauge(Gauge {
                    name,
                    values,
                    tags,
                    pools: &self.pools,
                    container_id,
                }))
            }
            Template::Distribution(dist) => {
                let name = self
                    .pools
                    .name_pool
                    .using_handle(dist.name)
                    .ok_or(Error::StringGenerate)?;
                Ok(Metric::Distribution(Dist {
                    name,
                    values,
                    sample_rate,
                    tags,
                    pools: &self.pools,
                    container_id,
                }))
            }
            Template::Histogram(hist) => {
                let name = self
                    .pools
                    .name_pool
                    .using_handle(hist.name)
                    .ok_or(Error::StringGenerate)?;
                Ok(Metric::Histogram(Histogram {
                    name,
                    values,
                    sample_rate,
                    tags,
                    pools: &self.pools,
                    container_id,
                }))
            }
            Template::Timer(timer) => {
                let name = self
                    .pools
                    .name_pool
                    .using_handle(timer.name)
                    .ok_or(Error::StringGenerate)?;
                Ok(Metric::Timer(Timer {
                    name,
                    values,
                    sample_rate,
                    tags,
                    pools: &self.pools,
                    container_id,
                }))
            }
            Template::Set(set) => {
                let name = self
                    .pools
                    .name_pool
                    .using_handle(set.name)
                    .ok_or(Error::StringGenerate)?;
                Ok(Metric::Set(Set {
                    name,
                    value: values.pop().expect("failed to pop value from Vec"),
                    tags,
                    pools: &self.pools,
                    container_id,
                }))
            }
        }
    }
}

/// Representation of a dogstatsd Metric
#[derive(Clone)]
pub enum Metric<'a> {
    /// Dogstatsd 'count' metric type.
    Count(Count<'a>),
    /// Dogstatsd 'gauge' metric type.
    Gauge(Gauge<'a>),
    /// Dogstatsd 'timer' metric type.
    Timer(Timer<'a>),
    /// Dogstatsd 'histogram' metric type.
    Histogram(Histogram<'a>),
    /// Dogstatsd 'set' metric type.
    Set(Set<'a>),
    /// Dogstatsd 'distribution' metric type.
    Distribution(Dist<'a>),
}

impl fmt::Display for Metric<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Count(count) => write!(f, "{count}"),
            Self::Gauge(gauge) => write!(f, "{gauge}"),
            Self::Timer(timer) => write!(f, "{timer}"),
            Self::Histogram(histogram) => write!(f, "{histogram}"),
            Self::Set(set) => write!(f, "{set}"),
            Self::Distribution(distribution) => write!(f, "{distribution}"),
        }
    }
}

impl std::fmt::Debug for Metric<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Count(count) => write!(f, "{count}"),
            Self::Gauge(gauge) => write!(f, "{gauge}"),
            Self::Timer(timer) => write!(f, "{timer}"),
            Self::Histogram(histogram) => write!(f, "{histogram}"),
            Self::Set(set) => write!(f, "{set}"),
            Self::Distribution(distribution) => write!(f, "{distribution}"),
        }
    }
}

#[derive(Clone, Debug)]
/// The count type in `DogStatsD` metric format. Monotonically increasing value.
pub struct Count<'a> {
    /// Name of the metric.
    pub name: &'a str,
    /// Values of the metric.
    pub values: Vec<common::NumValue>,
    /// Sample rate of the metric.
    pub sample_rate: Option<common::ZeroToOne>,
    /// Tags of the metric.
    pub(crate) tags: &'a common::tags::Tagset,
    /// String pools for tag handle lookups during serialization.
    pub(crate) pools: &'a StringPools,
    /// Container ID of the metric.
    pub container_id: Option<&'a str>,
}

impl fmt::Display for Count<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // <METRIC_NAME>:<VALUE>|d|#<TAG_KEY_1>:<TAGVALUE_1>,<TAG_2>|c:<CONTAINER_ID>
        // <METRIC_NAME>:<VALUE1>:<VALUE2>:<VALUE3>|d|@<SAMPLE_RATE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
        write!(f, "{name}", name = self.name)?;
        for val in &self.values {
            write!(f, ":{val}")?;
        }
        write!(f, "|c")?;
        if let Some(ref sample_rate) = self.sample_rate {
            write!(f, "|@{sample_rate}")?;
        }
        if !self.tags.is_empty() {
            write!(f, "|#")?;
            let mut commas_remaining = self.tags.len() - 1;
            for tag in self.tags {
                // Format tag from handles: "key:value"
                let key = self
                    .pools
                    .tag_pool
                    .using_handle(tag.key)
                    .expect("invalid tag key handle");
                let value = self
                    .pools
                    .str_pool
                    .using_handle(tag.value)
                    .expect("invalid tag value handle");
                write!(f, "{key}:{value}")?;
                if commas_remaining != 0 {
                    write!(f, ",")?;
                    commas_remaining -= 1;
                }
            }
        }
        if let Some(container_id) = self.container_id {
            write!(f, "|c:{container_id}")?;
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
/// The gauge type in `DogStatsD` format.
pub struct Gauge<'a> {
    /// Name of the metric.
    pub name: &'a str,
    /// Values of the metric.
    pub values: Vec<common::NumValue>,
    /// Tags of the metric.
    pub(crate) tags: &'a common::tags::Tagset,
    /// String pools for tag handle lookups during serialization.
    pub(crate) pools: &'a StringPools,
    /// Container ID of the metric.
    pub container_id: Option<&'a str>,
}

impl fmt::Display for Gauge<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // <METRIC_NAME>:<VALUE>|d|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|c:<CONTAINER_ID>
        // <METRIC_NAME>:<VALUE1>:<VALUE2>:<VALUE3>|d|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
        write!(f, "{name}", name = self.name)?;
        for val in &self.values {
            write!(f, ":{val}")?;
        }
        write!(f, "|g")?;
        if !self.tags.is_empty() {
            write!(f, "|#")?;
            let mut commas_remaining = self.tags.len() - 1;
            for tag in self.tags {
                let key = self
                    .pools
                    .tag_pool
                    .using_handle(tag.key)
                    .expect("invalid tag key handle");
                let value = self
                    .pools
                    .str_pool
                    .using_handle(tag.value)
                    .expect("invalid tag value handle");
                write!(f, "{key}:{value}")?;
                if commas_remaining != 0 {
                    write!(f, ",")?;
                    commas_remaining -= 1;
                }
            }
        }
        if let Some(container_id) = self.container_id {
            write!(f, "|c:{container_id}")?;
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
/// The timer type in `DogStatsD` format.
pub struct Timer<'a> {
    /// Name of the metric.
    pub name: &'a str,
    /// Values of the metric.
    pub values: Vec<common::NumValue>,
    /// Sample rate of the metric.
    pub sample_rate: Option<common::ZeroToOne>,
    /// Tags of the metric.
    pub(crate) tags: &'a common::tags::Tagset,
    /// String pools for tag handle lookups during serialization.
    pub(crate) pools: &'a StringPools,
    /// Container ID of the metric.
    pub container_id: Option<&'a str>,
}

impl fmt::Display for Timer<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // <METRIC_NAME>:<VALUE>|d|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|c:<CONTAINER_ID>
        // <METRIC_NAME>:<VALUE1>:<VALUE2>:<VALUE3>|d|@<SAMPLE_RATE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
        write!(f, "{name}", name = self.name)?;
        for val in &self.values {
            write!(f, ":{val}")?;
        }
        write!(f, "|ms")?;
        if let Some(ref sample_rate) = self.sample_rate {
            write!(f, "|@{sample_rate}")?;
        }
        if !self.tags.is_empty() {
            write!(f, "|#")?;
            let mut commas_remaining = self.tags.len() - 1;
            for tag in self.tags {
                let key = self
                    .pools
                    .tag_pool
                    .using_handle(tag.key)
                    .expect("invalid tag key handle");
                let value = self
                    .pools
                    .str_pool
                    .using_handle(tag.value)
                    .expect("invalid tag value handle");
                write!(f, "{key}:{value}")?;
                if commas_remaining != 0 {
                    write!(f, ",")?;
                    commas_remaining -= 1;
                }
            }
        }
        if let Some(container_id) = self.container_id {
            write!(f, "|c:{container_id}")?;
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
/// The distribution type in `DogStatsD` format.
pub struct Dist<'a> {
    /// Name of the metric.
    pub name: &'a str,
    /// Values of the metric.
    pub values: Vec<common::NumValue>,
    /// Sample rate of the metric.
    pub sample_rate: Option<common::ZeroToOne>,
    /// Tags of the metric.
    pub(crate) tags: &'a common::tags::Tagset,
    /// String pools for tag handle lookups during serialization.
    pub(crate) pools: &'a StringPools,
    /// Container ID of the metric.
    pub container_id: Option<&'a str>,
}

impl fmt::Display for Dist<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // <METRIC_NAME>:<VALUE>|d|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|c:<CONTAINER_ID>
        // <METRIC_NAME>:<VALUE1>:<VALUE2>:<VALUE3>|d|@<SAMPLE_RATE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
        write!(f, "{name}", name = self.name)?;
        for val in &self.values {
            write!(f, ":{val}")?;
        }
        write!(f, "|d")?;
        if let Some(ref sample_rate) = self.sample_rate {
            write!(f, "|@{sample_rate}")?;
        }
        if !self.tags.is_empty() {
            write!(f, "|#")?;
            let mut commas_remaining = self.tags.len() - 1;
            for tag in self.tags {
                let key = self
                    .pools
                    .tag_pool
                    .using_handle(tag.key)
                    .expect("invalid tag key handle");
                let value = self
                    .pools
                    .str_pool
                    .using_handle(tag.value)
                    .expect("invalid tag value handle");
                write!(f, "{key}:{value}")?;
                if commas_remaining != 0 {
                    write!(f, ",")?;
                    commas_remaining -= 1;
                }
            }
        }
        if let Some(container_id) = self.container_id {
            write!(f, "|c:{container_id}")?;
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
/// The set type in `DogStatsD` format.
pub struct Set<'a> {
    /// Name of the metric.
    pub name: &'a str,
    /// Value of the metric.
    pub value: common::NumValue,
    /// Tags of the metric.
    pub(crate) tags: &'a common::tags::Tagset,
    /// String pools for tag handle lookups during serialization.
    pub(crate) pools: &'a StringPools,
    /// Container ID of the metric.
    pub container_id: Option<&'a str>,
}

impl fmt::Display for Set<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // <METRIC_NAME>:<VALUE>|s|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|c:<CONTAINER_ID>
        let name = &self.name;
        write!(f, "{name}:{value}|s", value = self.value)?;
        if !self.tags.is_empty() {
            write!(f, "|#")?;
            let mut commas_remaining = self.tags.len() - 1;
            for tag in self.tags {
                let key = self
                    .pools
                    .tag_pool
                    .using_handle(tag.key)
                    .expect("invalid tag key handle");
                let value = self
                    .pools
                    .str_pool
                    .using_handle(tag.value)
                    .expect("invalid tag value handle");
                write!(f, "{key}:{value}")?;
                if commas_remaining != 0 {
                    write!(f, ",")?;
                    commas_remaining -= 1;
                }
            }
        }
        if let Some(container_id) = self.container_id {
            write!(f, "|c:{container_id}")?;
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
/// The histogram type in `DogStatsD` format.
pub struct Histogram<'a> {
    /// Name of the metric.
    pub name: &'a str,
    /// Values of the metric.
    pub values: Vec<common::NumValue>,
    /// Sample rate of the metric.
    pub sample_rate: Option<common::ZeroToOne>,
    /// Tags of the metric.
    pub(crate) tags: &'a common::tags::Tagset,
    /// String pools for tag handle lookups during serialization.
    pub(crate) pools: &'a StringPools,
    /// Container ID of the metric.
    pub container_id: Option<&'a str>,
}

impl fmt::Display for Histogram<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // <METRIC_NAME>:<VALUE>|h|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|c:<CONTAINER_ID>
        // <METRIC_NAME>:<VALUE1>:<VALUE2>:<VALUE3>|h|@<SAMPLE_RATE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
        write!(f, "{name}", name = self.name)?;
        for val in &self.values {
            write!(f, ":{val}")?;
        }
        write!(f, "|h")?;
        if let Some(ref sample_rate) = self.sample_rate {
            write!(f, "|@{sample_rate}")?;
        }
        if !self.tags.is_empty() {
            write!(f, "|#")?;
            let mut commas_remaining = self.tags.len() - 1;
            for tag in self.tags {
                let key = self
                    .pools
                    .tag_pool
                    .using_handle(tag.key)
                    .expect("invalid tag key handle");
                let value = self
                    .pools
                    .str_pool
                    .using_handle(tag.value)
                    .expect("invalid tag value handle");
                write!(f, "{key}:{value}")?;
                if commas_remaining != 0 {
                    write!(f, ",")?;
                    commas_remaining -= 1;
                }
            }
        }
        if let Some(container_id) = self.container_id {
            write!(f, "|c:{container_id}")?;
        }

        Ok(())
    }
}
