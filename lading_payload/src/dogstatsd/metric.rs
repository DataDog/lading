//! `DogStatsD` metric.
use std::{fmt, rc::Rc};

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
    ConfRange, ValueConf,
    common::{self, NumValueGenerator},
};

mod template;

#[derive(Clone, Debug)]
pub(crate) struct MetricGenerator<NP, KP, VP>
where
    NP: Pool,
    KP: Pool,
    VP: Pool,
{
    pub(crate) container_ids: Vec<String>,
    pub(crate) templates: Vec<template::Template<NP::Handle>>,
    pub(crate) multivalue_count: ConfRange<u16>,
    pub(crate) multivalue_pack_probability: f32,
    pub(crate) sampling: ConfRange<f32>,
    pub(crate) sampling_probability: f32,
    pub(crate) num_value_generator: NumValueGenerator,
    pub(crate) str_pool: Rc<strings::RandomStringPool>,
    pub(crate) name_pool: Rc<NP>,
    pub(crate) tag_pool: Rc<KP>,
    /// Tags for each template. Each position in this Vec corresponds to the
    /// same position in the templates Vec. The handles are resolved to strings
    /// via `str_pool` during serialization.
    pub(crate) tags: Vec<common::tags::Tagset<KP::Handle, VP::Handle>>,
}

impl<NP, KP, VP> MetricGenerator<NP, KP, VP>
where
    NP: Pool,
    KP: Pool,
    VP: Pool,
{
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
        tags_generator: &mut common::tags::Generator<KP, VP>,
        str_pool: &Rc<strings::RandomStringPool>,
        value_conf: ValueConf,
        name_pool: &Rc<NP>,
        tag_pool: &Rc<KP>,
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
            let (_, name_handle) = name_pool
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
            str_pool: Rc::clone(str_pool),
            name_pool: Rc::clone(name_pool),
            tag_pool: Rc::clone(tag_pool),
            tags,
        })
    }
}

impl<'a, NP, KP, VP> Generator<'a> for MetricGenerator<NP, KP, VP>
where
    NP: Pool + 'a,
    KP: Pool + 'a,
    VP: Pool,
    NP::Handle: 'a,
    KP::Handle: 'a,
    VP::Handle: 'a,
{
    type Output = Metric<'a, KP::Handle, VP::Handle, KP>;
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
                    .name_pool
                    .using_handle(count.name)
                    .ok_or(Error::StringGenerate)?;
                Ok(Metric::Count(Count {
                    name,
                    values,
                    sample_rate,
                    tags,
                    str_pool: &self.str_pool,
                    tag_pool: Rc::clone(&self.tag_pool),
                    container_id,
                }))
            }
            Template::Gauge(gauge) => {
                let name = self
                    .name_pool
                    .using_handle(gauge.name)
                    .ok_or(Error::StringGenerate)?;
                Ok(Metric::Gauge(Gauge {
                    name,
                    values,
                    tags,
                    str_pool: &self.str_pool,
                    tag_pool: Rc::clone(&self.tag_pool),
                    container_id,
                }))
            }
            Template::Distribution(dist) => {
                let name = self
                    .name_pool
                    .using_handle(dist.name)
                    .ok_or(Error::StringGenerate)?;
                Ok(Metric::Distribution(Dist {
                    name,
                    values,
                    sample_rate,
                    tags,
                    str_pool: &self.str_pool,
                    tag_pool: Rc::clone(&self.tag_pool),
                    container_id,
                }))
            }
            Template::Histogram(hist) => {
                let name = self
                    .name_pool
                    .using_handle(hist.name)
                    .ok_or(Error::StringGenerate)?;
                Ok(Metric::Histogram(Histogram {
                    name,
                    values,
                    sample_rate,
                    tags,
                    str_pool: &self.str_pool,
                    tag_pool: Rc::clone(&self.tag_pool),
                    container_id,
                }))
            }
            Template::Timer(timer) => {
                let name = self
                    .name_pool
                    .using_handle(timer.name)
                    .ok_or(Error::StringGenerate)?;
                Ok(Metric::Timer(Timer {
                    name,
                    values,
                    sample_rate,
                    tags,
                    str_pool: &self.str_pool,
                    tag_pool: Rc::clone(&self.tag_pool),
                    container_id,
                }))
            }
            Template::Set(set) => {
                let name = self
                    .name_pool
                    .using_handle(set.name)
                    .ok_or(Error::StringGenerate)?;
                Ok(Metric::Set(Set {
                    name,
                    value: values.pop().expect("failed to pop value from Vec"),
                    tags,
                    str_pool: &self.str_pool,
                    tag_pool: Rc::clone(&self.tag_pool),
                    container_id,
                }))
            }
        }
    }
}

/// Representation of a dogstatsd Metric
#[derive(Clone)]
#[allow(private_bounds)]
pub enum Metric<'a, KH, VH, KP>
where
    KP: strings::Pool<Handle = KH>,
{
    /// Dogstatsd 'count' metric type.
    Count(Count<'a, KH, VH, KP>),
    /// Dogstatsd 'gauge' metric type.
    Gauge(Gauge<'a, KH, VH, KP>),
    /// Dogstatsd 'timer' metric type.
    Timer(Timer<'a, KH, VH, KP>),
    /// Dogstatsd 'histogram' metric type.
    Histogram(Histogram<'a, KH, VH, KP>),
    /// Dogstatsd 'set' metric type.
    Set(Set<'a, KH, VH, KP>),
    /// Dogstatsd 'distribution' metric type.
    Distribution(Dist<'a, KH, VH, KP>),
}

impl<KP> fmt::Display for Metric<'_, strings::PosAndLengthHandle, strings::PosAndLengthHandle, KP>
where
    KP: strings::Pool<Handle = strings::PosAndLengthHandle>,
{
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

impl<KP> std::fmt::Debug for Metric<'_, strings::PosAndLengthHandle, strings::PosAndLengthHandle, KP>
where
    KP: strings::Pool<Handle = strings::PosAndLengthHandle>,
{
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
#[allow(private_bounds)]
pub struct Count<'a, KH, VH, KP>
where
    KP: strings::Pool<Handle = KH>,
{
    /// Name of the metric.
    pub name: &'a str,
    /// Values of the metric.
    pub values: Vec<common::NumValue>,
    /// Sample rate of the metric.
    pub sample_rate: Option<common::ZeroToOne>,
    /// Tags of the metric.
    pub(crate) tags: &'a common::tags::Tagset<KH, VH>,
    /// String pool for tag handle lookups during serialization.
    pub(crate) str_pool: &'a strings::RandomStringPool,
    /// The tag pool
    pub(crate) tag_pool: Rc<KP>,
    /// Container ID of the metric.
    pub container_id: Option<&'a str>,
}

impl<KP> fmt::Display for Count<'_, strings::PosAndLengthHandle, strings::PosAndLengthHandle, KP>
where
    KP: strings::Pool<Handle = strings::PosAndLengthHandle>,
{
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
                    .tag_pool
                    .using_handle(tag.key)
                    .expect("invalid tag key handle");
                let value = self
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
#[allow(private_bounds)]
pub struct Gauge<'a, KH, VH, KP>
where
    KP: strings::Pool<Handle = KH>,
{
    /// Name of the metric.
    pub name: &'a str,
    /// Values of the metric.
    pub values: Vec<common::NumValue>,
    /// Tags of the metric.
    pub(crate) tags: &'a common::tags::Tagset<KH, VH>,
    /// String pool for tag handle lookups during serialization.
    pub(crate) str_pool: &'a strings::RandomStringPool,
    /// The tag pool
    pub(crate) tag_pool: Rc<KP>,
    /// Container ID of the metric.
    pub container_id: Option<&'a str>,
}

impl<KP> fmt::Display for Gauge<'_, strings::PosAndLengthHandle, strings::PosAndLengthHandle, KP>
where
    KP: strings::Pool<Handle = strings::PosAndLengthHandle>,
{
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
                    .tag_pool
                    .using_handle(tag.key)
                    .expect("invalid tag key handle");
                let value = self
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
#[allow(private_bounds)]
pub struct Timer<'a, KH, VH, KP>
where
    KP: strings::Pool<Handle = KH>,
{
    /// Name of the metric.
    pub name: &'a str,
    /// Values of the metric.
    pub values: Vec<common::NumValue>,
    /// Sample rate of the metric.
    pub sample_rate: Option<common::ZeroToOne>,
    /// Tags of the metric.
    pub(crate) tags: &'a common::tags::Tagset<KH, VH>,
    /// String pool for tag handle lookups during serialization.
    pub(crate) str_pool: &'a strings::RandomStringPool,
    /// The tag pool
    pub(crate) tag_pool: Rc<KP>,
    /// Container ID of the metric.
    pub container_id: Option<&'a str>,
}

impl<KP> fmt::Display for Timer<'_, strings::PosAndLengthHandle, strings::PosAndLengthHandle, KP>
where
    KP: strings::Pool<Handle = strings::PosAndLengthHandle>,
{
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
                    .tag_pool
                    .using_handle(tag.key)
                    .expect("invalid tag key handle");
                let value = self
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
#[allow(private_bounds)]
pub struct Dist<'a, KH, VH, KP>
where
    KP: strings::Pool<Handle = KH>,
{
    /// Name of the metric.
    pub name: &'a str,
    /// Values of the metric.
    pub values: Vec<common::NumValue>,
    /// Sample rate of the metric.
    pub sample_rate: Option<common::ZeroToOne>,
    /// Tags of the metric.
    pub(crate) tags: &'a common::tags::Tagset<KH, VH>,
    /// String pool for tag handle lookups during serialization.
    pub(crate) str_pool: &'a strings::RandomStringPool,
    /// The tag pool
    pub(crate) tag_pool: Rc<KP>,
    /// Container ID of the metric.
    pub container_id: Option<&'a str>,
}

impl<KP> fmt::Display for Dist<'_, strings::PosAndLengthHandle, strings::PosAndLengthHandle, KP>
where
    KP: strings::Pool<Handle = strings::PosAndLengthHandle>,
{
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
                    .tag_pool
                    .using_handle(tag.key)
                    .expect("invalid tag key handle");
                let value = self
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
#[allow(private_bounds)]
pub struct Set<'a, KH, VH, KP>
where
    KP: strings::Pool<Handle = KH>,
{
    /// Name of the metric.
    pub name: &'a str,
    /// Value of the metric.
    pub value: common::NumValue,
    /// Tags of the metric.
    pub(crate) tags: &'a common::tags::Tagset<KH, VH>,
    /// String pool for tag handle lookups during serialization.
    pub(crate) str_pool: &'a strings::RandomStringPool,
    /// The tag pool
    pub(crate) tag_pool: Rc<KP>,
    /// Container ID of the metric.
    pub container_id: Option<&'a str>,
}

impl<KP> fmt::Display for Set<'_, strings::PosAndLengthHandle, strings::PosAndLengthHandle, KP>
where
    KP: strings::Pool<Handle = strings::PosAndLengthHandle>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // <METRIC_NAME>:<VALUE>|s|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|c:<CONTAINER_ID>
        let name = &self.name;
        write!(f, "{name}:{value}|s", value = self.value)?;
        if !self.tags.is_empty() {
            write!(f, "|#")?;
            let mut commas_remaining = self.tags.len() - 1;
            for tag in self.tags {
                let key = self
                    .tag_pool
                    .using_handle(tag.key)
                    .expect("invalid tag key handle");
                let value = self
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
#[allow(private_bounds)]
pub struct Histogram<'a, KH, VH, KP>
where
    KP: strings::Pool<Handle = KH>,
{
    /// Name of the metric.
    pub name: &'a str,
    /// Values of the metric.
    pub values: Vec<common::NumValue>,
    /// Sample rate of the metric.
    pub sample_rate: Option<common::ZeroToOne>,
    /// Tags of the metric.
    pub(crate) tags: &'a common::tags::Tagset<KH, VH>,
    /// String pool for tag handle lookups during serialization.
    pub(crate) str_pool: &'a strings::RandomStringPool,
    /// The tag pool
    pub(crate) tag_pool: Rc<KP>,
    /// Container ID of the metric.
    pub container_id: Option<&'a str>,
}

impl<KP> fmt::Display for Histogram<'_, strings::PosAndLengthHandle, strings::PosAndLengthHandle, KP>
where
    KP: strings::Pool<Handle = strings::PosAndLengthHandle>,
{
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
                    .tag_pool
                    .using_handle(tag.key)
                    .expect("invalid tag key handle");
                let value = self
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
