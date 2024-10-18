//! `DogStatsD` metric.
use std::fmt;

use rand::{
    distributions::{OpenClosed01, WeightedIndex},
    prelude::Distribution,
    Rng,
};

use crate::{common::strings, dogstatsd::metric::template::Template, Error, Generator};
use tracing::info;

use super::{
    choose_or_not_ref,
    common::{self, NumValueGenerator},
    ConfRange, ValueConf,
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
    pub(crate) ctx_idx: std::cell::Cell<usize>,
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
        str_pool: &strings::Pool,
        value_conf: ValueConf,
        metric_name_prefix: &'static str,
        mut rng: &mut R,
    ) -> Result<Self, Error>
    where
        R: Rng + ?Sized,
    {
        let mut templates = Vec::with_capacity(num_contexts);

        info!("Generating metric templates for {} contexts.", num_contexts);
        for _ in 0..num_contexts {
            let tags = tags_generator.generate(&mut rng);
            let name_sz = name_length.sample(&mut rng) as usize;
            let strpool_name = String::from(
                str_pool
                    .of_size(&mut rng, name_sz)
                    .ok_or(Error::StringGenerate)?,
            );
            let name = if metric_name_prefix.is_empty() {
                strpool_name
            } else {
                format!("{metric_name_prefix}{strpool_name}")
            };

            let res = match metric_weights.sample(rng) {
                0 => Template::Count(template::Count { name, tags: tags? }),
                1 => Template::Gauge(template::Gauge { name, tags: tags? }),
                2 => Template::Timer(template::Timer { name, tags: tags? }),
                3 => Template::Distribution(template::Dist { name, tags: tags? }),
                4 => Template::Set(template::Set { name, tags: tags? }),
                5 => Template::Histogram(template::Histogram { name, tags: tags? }),
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
            ctx_idx: std::cell::Cell::new(0),
        })
    }
}

impl<'a> Generator<'a> for MetricGenerator {
    type Output = Metric<'a>;
    type Error = Error;

    fn generate<R>(&'a self, mut rng: &mut R) -> Result<Self::Output, Self::Error>
    where
        R: rand::Rng + ?Sized,
    {
        // SAFETY: If `self.templates` is ever empty this is a serious logic bug
        // and the program should crash prior to this point.
        let template: &Template = self
            .templates
            .get(self.ctx_idx.get())
            .expect("failed to get template");
        self.ctx_idx
            .set((self.ctx_idx.get() + 1) % self.templates.len());

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
            Template::Count(ref count) => Ok(Metric::Count(Count {
                name: &count.name,
                values,
                sample_rate,
                tags: &count.tags,
                container_id,
            })),
            Template::Gauge(ref gauge) => Ok(Metric::Gauge(Gauge {
                name: &gauge.name,
                values,
                tags: &gauge.tags,
                container_id,
            })),
            Template::Distribution(ref dist) => Ok(Metric::Distribution(Dist {
                name: &dist.name,
                values,
                sample_rate,
                tags: &dist.tags,
                container_id,
            })),
            Template::Histogram(ref hist) => Ok(Metric::Histogram(Histogram {
                name: &hist.name,
                values,
                sample_rate,
                tags: &hist.tags,
                container_id,
            })),
            Template::Timer(ref timer) => Ok(Metric::Timer(Timer {
                name: &timer.name,
                values,
                sample_rate,
                tags: &timer.tags,
                container_id,
            })),
            Template::Set(ref set) => Ok(Metric::Set(Set {
                name: &set.name,
                value: values.pop().expect("failed to pop value from Vec"),
                tags: &set.tags,
                container_id,
            })),
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

impl<'a> fmt::Display for Metric<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Count(ref count) => write!(f, "{count}"),
            Self::Gauge(ref gauge) => write!(f, "{gauge}"),
            Self::Timer(ref timer) => write!(f, "{timer}"),
            Self::Histogram(ref histogram) => write!(f, "{histogram}"),
            Self::Set(ref set) => write!(f, "{set}"),
            Self::Distribution(ref distribution) => write!(f, "{distribution}"),
        }
    }
}

impl<'a> std::fmt::Debug for Metric<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Count(ref count) => write!(f, "{count}"),
            Self::Gauge(ref gauge) => write!(f, "{gauge}"),
            Self::Timer(ref timer) => write!(f, "{timer}"),
            Self::Histogram(ref histogram) => write!(f, "{histogram}"),
            Self::Set(ref set) => write!(f, "{set}"),
            Self::Distribution(ref distribution) => write!(f, "{distribution}"),
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
    pub tags: &'a common::tags::Tagset,
    /// Container ID of the metric.
    pub container_id: Option<&'a str>,
}

impl<'a> fmt::Display for Count<'a> {
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
                write!(f, "{tag}")?;
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
    pub tags: &'a common::tags::Tagset,
    /// Container ID of the metric.
    pub container_id: Option<&'a str>,
}

impl<'a> fmt::Display for Gauge<'a> {
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
                write!(f, "{tag}")?;
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
    pub tags: &'a common::tags::Tagset,
    /// Container ID of the metric.
    pub container_id: Option<&'a str>,
}

impl<'a> fmt::Display for Timer<'a> {
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
                write!(f, "{tag}")?;
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
    pub tags: &'a common::tags::Tagset,
    /// Container ID of the metric.
    pub container_id: Option<&'a str>,
}

impl<'a> fmt::Display for Dist<'a> {
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
                write!(f, "{tag}")?;
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
    pub tags: &'a common::tags::Tagset,
    /// Container ID of the metric.
    pub container_id: Option<&'a str>,
}

impl<'a> fmt::Display for Set<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // <METRIC_NAME>:<VALUE>|s|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|c:<CONTAINER_ID>
        let name = &self.name;
        write!(f, "{name}:{value}|s", value = self.value)?;
        if !self.tags.is_empty() {
            write!(f, "|#")?;
            let mut commas_remaining = self.tags.len() - 1;
            for tag in self.tags {
                write!(f, "{tag}")?;
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
    pub tags: &'a common::tags::Tagset,
    /// Container ID of the metric.
    pub container_id: Option<&'a str>,
}

impl<'a> fmt::Display for Histogram<'a> {
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
                write!(f, "{tag}")?;
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
