use std::{fmt, ops::Range};

use rand::{
    distributions::{OpenClosed01, Standard, WeightedIndex},
    prelude::{Distribution, SliceRandom},
    Rng,
};

use crate::{common::AsciiString, Generator};

use super::{
    choose_or_not,
    common::{self},
};

#[derive(Clone, Debug)]
pub(crate) struct MetricGenerator {
    pub(crate) container_ids: Vec<String>,
    // A "metric_template" is a Metric that has a type, name, and tag set, but no values or container id.
    pub(crate) metric_templates: Vec<Metric>,
    pub(crate) multivalue_cnt_range: Range<usize>,
    pub(crate) multivalue_pack_probability: f32,
}

impl MetricGenerator {
    pub(crate) fn new<R>(
        num_contexts: usize,
        multivalue_cnt_range: Range<usize>,
        multivalue_pack_probability: f32,
        metric_weights: &WeightedIndex<u8>,
        container_ids: Vec<String>,
        tagsets: common::tags::Tagsets,
        rng: &mut R,
    ) -> Self
    where
        R: Rng + ?Sized,
    {
        // TODO find some ground truth for a good default here.
        // I remember reading that we have a recommended max metric name length,
        // but I can't find it in our public docs anymore...
        let max_name_length = 120;
        // TODO -- somewhere we should track if the requested number of contexts
        // does not fit into the block_cache_size. This is a footgun
        // If you specify 10k contexts but only give 5kb of cache size
        // then that is not possible and ideally we'd issue a WARN

        let mut buf = Vec::with_capacity(num_contexts);

        assert!(tagsets.len() >= num_contexts);
        for tagset in tagsets {
            let name = AsciiString::with_maximum_length(max_name_length).generate(rng);

            let res = match metric_weights.sample(rng) {
                0 => Metric::Count(Count {
                    name,
                    values: None,
                    sample_rate: None,
                    tags: tagset,
                    container_id: None,
                }),
                1 => Metric::Gauge(Gauge {
                    name,
                    values: None,
                    tags: tagset,
                    container_id: None,
                }),
                2 => Metric::Timer(Timer {
                    name,
                    values: None,
                    sample_rate: None,
                    tags: tagset,
                    container_id: None,
                }),
                3 => Metric::Distribution(Dist {
                    name,
                    values: None,
                    sample_rate: None,
                    tags: tagset,
                    container_id: None,
                }),
                4 => Metric::Set(Set {
                    name,
                    value: None,
                    tags: tagset,
                    container_id: None,
                }),
                5 => Metric::Histogram(Histogram {
                    name,
                    values: None,
                    sample_rate: None,
                    tags: tagset,
                    container_id: None,
                }),
                _ => unreachable!(),
            };
            buf.push(res);
        }

        MetricGenerator {
            metric_templates: buf,
            container_ids,
            multivalue_cnt_range,
            multivalue_pack_probability,
        }
    }
}

impl Generator<Metric> for MetricGenerator {
    fn generate<R>(&self, mut rng: &mut R) -> Metric
    where
        R: rand::Rng + ?Sized,
    {
        let mut new_metric = self.metric_templates.choose(&mut rng).unwrap().clone();

        let multivalue_cnt_range: Range<usize> =
            self.multivalue_cnt_range.start..self.multivalue_cnt_range.end;

        let container_id = choose_or_not(&mut rng, &self.container_ids);
        // TODO sample_rate should be option and have a probability that determines if its present
        // Mostly inconsequential for the Agent, for certain metric types the Agent
        // applies some correction based on this value. Affects count and histogram computation.
        // https://docs.datadoghq.com/metrics/custom_metrics/dogstatsd_metrics_submission/#sample-rates
        let sample_rate = rng.gen();

        let value: common::NumValue = Standard.sample(&mut rng);
        let mut values = vec![value.clone()];

        let prob: f32 = OpenClosed01.sample(&mut rng);
        if prob < self.multivalue_pack_probability {
            let num_desired_values = rng.gen_range(multivalue_cnt_range);
            for _ in 1..num_desired_values {
                values.push(Standard.sample(&mut rng));
            }
        }

        match &mut new_metric {
            Metric::Count(count) => {
                count.container_id = container_id;
                count.sample_rate = sample_rate;
                count.values = Some(values);
            }
            Metric::Gauge(gauge) => {
                gauge.container_id = container_id;
                gauge.values = Some(values);
            }
            Metric::Distribution(count) => {
                count.container_id = container_id;
                count.sample_rate = sample_rate;
                count.values = Some(values);
            }
            Metric::Histogram(ref mut count) => {
                count.container_id = container_id;
                count.sample_rate = sample_rate;
                count.values = Some(values);
            }
            Metric::Timer(ref mut count) => {
                count.container_id = container_id;
                count.sample_rate = sample_rate;
                count.values = Some(values);
            }
            Metric::Set(ref mut count) => {
                count.container_id = container_id;
                count.value = Some(value);
            }
        }

        new_metric
    }
}

#[derive(Clone)]
pub enum Metric {
    Count(Count),
    Gauge(Gauge),
    Timer(Timer),
    Histogram(Histogram),
    Set(Set),
    Distribution(Dist),
}

impl fmt::Display for Metric {
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

impl std::fmt::Debug for Metric {
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
pub struct Count {
    name: String,
    values: Option<Vec<common::NumValue>>,
    sample_rate: Option<common::ZeroToOne>,
    tags: common::tags::Tagset,
    container_id: Option<String>,
}

impl fmt::Display for Count {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // <METRIC_NAME>:<VALUE>|d|#<TAG_KEY_1>:<TAGVALUE_1>,<TAG_2>|c:<CONTAINER_ID>
        // <METRIC_NAME>:<VALUE1>:<VALUE2>:<VALUE3>|d|@<SAMPLE_RATE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
        write!(f, "{name}", name = self.name)?;
        if let Some(values) = &self.values {
            for val in values {
                write!(f, ":{val}")?;
            }
        }
        write!(f, "|c")?;
        if let Some(ref sample_rate) = self.sample_rate {
            write!(f, "|@{sample_rate}")?;
        }
        if !self.tags.is_empty() {
            write!(f, "|#")?;
            let mut commas_remaining = self.tags.len() - 1;
            for tag in &self.tags {
                write!(f, "{tag}")?;
                if commas_remaining != 0 {
                    write!(f, ",")?;
                    commas_remaining -= 1;
                }
            }
        }
        if let Some(ref container_id) = self.container_id {
            write!(f, "|c:{container_id}")?;
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
/// The gauge type in `DogStatsD` format.
pub struct Gauge {
    name: String,
    values: Option<Vec<common::NumValue>>,
    tags: common::tags::Tagset,
    container_id: Option<String>,
}

impl fmt::Display for Gauge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // <METRIC_NAME>:<VALUE>|d|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|c:<CONTAINER_ID>
        // <METRIC_NAME>:<VALUE1>:<VALUE2>:<VALUE3>|d|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
        write!(f, "{name}", name = self.name)?;
        if let Some(values) = &self.values {
            for val in values {
                write!(f, ":{val}")?;
            }
        }
        write!(f, "|g")?;
        if !self.tags.is_empty() {
            write!(f, "|#")?;
            let mut commas_remaining = self.tags.len() - 1;
            for tag in &self.tags {
                write!(f, "{tag}")?;
                if commas_remaining != 0 {
                    write!(f, ",")?;
                    commas_remaining -= 1;
                }
            }
        }
        if let Some(ref container_id) = self.container_id {
            write!(f, "|c:{container_id}")?;
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
/// The timer type in `DogStatsD` format.
pub struct Timer {
    name: String,
    values: Option<Vec<common::NumValue>>,
    sample_rate: Option<common::ZeroToOne>,
    tags: common::tags::Tagset,
    container_id: Option<String>,
}

impl fmt::Display for Timer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // <METRIC_NAME>:<VALUE>|d|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|c:<CONTAINER_ID>
        // <METRIC_NAME>:<VALUE1>:<VALUE2>:<VALUE3>|d|@<SAMPLE_RATE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
        write!(f, "{name}", name = self.name)?;
        if let Some(values) = &self.values {
            for val in values {
                write!(f, ":{val}")?;
            }
        }
        write!(f, "|ms")?;
        if let Some(ref sample_rate) = self.sample_rate {
            write!(f, "|@{sample_rate}")?;
        }
        if !self.tags.is_empty() {
            write!(f, "|#")?;
            let mut commas_remaining = self.tags.len() - 1;
            for tag in &self.tags {
                write!(f, "{tag}")?;
                if commas_remaining != 0 {
                    write!(f, ",")?;
                    commas_remaining -= 1;
                }
            }
        }
        if let Some(ref container_id) = self.container_id {
            write!(f, "|c:{container_id}")?;
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
/// The distribution type in `DogStatsD` format.
pub struct Dist {
    name: String,
    values: Option<Vec<common::NumValue>>,
    sample_rate: Option<common::ZeroToOne>,
    tags: common::tags::Tagset,
    container_id: Option<String>,
}

impl fmt::Display for Dist {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // <METRIC_NAME>:<VALUE>|d|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|c:<CONTAINER_ID>
        // <METRIC_NAME>:<VALUE1>:<VALUE2>:<VALUE3>|d|@<SAMPLE_RATE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
        write!(f, "{name}", name = self.name)?;
        if let Some(values) = &self.values {
            for val in values {
                write!(f, ":{val}")?;
            }
        }
        write!(f, "|d")?;
        if let Some(ref sample_rate) = self.sample_rate {
            write!(f, "|@{sample_rate}")?;
        }
        if !self.tags.is_empty() {
            write!(f, "|#")?;
            let mut commas_remaining = self.tags.len() - 1;
            for tag in &self.tags {
                write!(f, "{tag}")?;
                if commas_remaining != 0 {
                    write!(f, ",")?;
                    commas_remaining -= 1;
                }
            }
        }
        if let Some(ref container_id) = self.container_id {
            write!(f, "|c:{container_id}")?;
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
/// The set type in `DogStatsD` format.
pub struct Set {
    name: String,
    value: Option<common::NumValue>,
    tags: common::tags::Tagset,
    container_id: Option<String>,
}

impl fmt::Display for Set {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // <METRIC_NAME>:<VALUE>|s|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|c:<CONTAINER_ID>
        let name = &self.name;
        write!(f, "{name}")?;
        if let Some(value) = &self.value {
            write!(f, ":{value}")?;
        }
        write!(f, "|s")?;
        if !self.tags.is_empty() {
            write!(f, "|#")?;
            let mut commas_remaining = self.tags.len() - 1;
            for tag in &self.tags {
                write!(f, "{tag}")?;
                if commas_remaining != 0 {
                    write!(f, ",")?;
                    commas_remaining -= 1;
                }
            }
        }
        if let Some(ref container_id) = self.container_id {
            write!(f, "|c:{container_id}")?;
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
/// The histogram type in `DogStatsD` format.
pub struct Histogram {
    name: String,
    values: Option<Vec<common::NumValue>>,
    sample_rate: Option<common::ZeroToOne>,
    tags: common::tags::Tagset,
    container_id: Option<String>,
}

impl fmt::Display for Histogram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // <METRIC_NAME>:<VALUE>|h|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|c:<CONTAINER_ID>
        // <METRIC_NAME>:<VALUE1>:<VALUE2>:<VALUE3>|h|@<SAMPLE_RATE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
        write!(f, "{name}", name = self.name)?;
        if let Some(values) = &self.values {
            for val in values {
                write!(f, ":{val}")?;
            }
        }
        write!(f, "|h")?;
        if let Some(ref sample_rate) = self.sample_rate {
            write!(f, "|@{sample_rate}")?;
        }
        if !self.tags.is_empty() {
            write!(f, "|#")?;
            let mut commas_remaining = self.tags.len() - 1;
            for tag in &self.tags {
                write!(f, "{tag}")?;
                if commas_remaining != 0 {
                    write!(f, ",")?;
                    commas_remaining -= 1;
                }
            }
        }
        if let Some(ref container_id) = self.container_id {
            write!(f, "|c:{container_id}")?;
        }

        Ok(())
    }
}
