use std::fmt;

use rand::{
    distributions::{Standard, WeightedIndex},
    prelude::Distribution,
    seq::SliceRandom,
};

use crate::payload::Generator;

use super::{choose_or_not, common};

#[derive(Debug, Clone)]
pub(crate) struct MetricGenerator {
    pub(crate) metric_weights: WeightedIndex<u8>,
    pub(crate) names: Vec<String>,
    pub(crate) container_ids: Vec<String>,
    pub(crate) tags: Vec<common::tags::Tags>,
}

impl Generator<Metric> for MetricGenerator {
    fn generate<R>(&self, mut rng: &mut R) -> Metric
    where
        R: rand::Rng + ?Sized,
    {
        let container_id = choose_or_not(&mut rng, &self.container_ids);
        let name = self.names.choose(&mut rng).unwrap().clone();
        let tags = choose_or_not(&mut rng, &self.tags);
        let sample_rate = rng.gen();
        let total_values = rng.gen_range(1..32);
        let value: Vec<common::NumValue> =
            Standard.sample_iter(&mut rng).take(total_values).collect();

        match self.metric_weights.sample(rng) {
            0 => Metric::Count(Count {
                name,
                value,
                sample_rate,
                tags,
                container_id,
            }),
            1 => Metric::Gauge(Gauge {
                name,
                value,
                tags,
                container_id,
            }),
            2 => Metric::Timer(Timer {
                name,
                value,
                sample_rate,
                tags,
                container_id,
            }),
            3 => Metric::Distribution(Dist {
                name,
                value,
                sample_rate,
                tags,
                container_id,
            }),
            4 => Metric::Set(Set {
                name,
                value,
                tags,
                container_id,
            }),
            5 => Metric::Histogram(Histogram {
                name,
                value,
                sample_rate,
                tags,
                container_id,
            }),
            _ => unreachable!(),
        }
    }
}

pub(crate) enum Metric {
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

pub(crate) struct Count {
    name: String,
    value: Vec<common::NumValue>,
    sample_rate: Option<common::ZeroToOne>,
    tags: Option<common::tags::Tags>,
    container_id: Option<String>,
}

impl fmt::Display for Count {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // <METRIC_NAME>:<VALUE>|<TYPE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|c:<CONTAINER_ID>
        // <METRIC_NAME>:<VALUE1>:<VALUE2>:<VALUE3>|<TYPE>|@<SAMPLE_RATE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
        write!(f, "{name}", name = self.name)?;
        for val in &self.value {
            write!(f, ":{val}")?;
        }
        write!(f, "|c")?;
        if let Some(ref sample_rate) = self.sample_rate {
            write!(f, "|@{sample_rate}")?;
        }
        if let Some(ref tags) = self.tags {
            if !tags.is_empty() {
                write!(f, "|#")?;
                let mut commas_remaining = tags.len() - 1;
                for (k, v) in tags.iter() {
                    write!(f, "{k}:{v}")?;
                    if commas_remaining != 0 {
                        write!(f, ",")?;
                        commas_remaining -= 1;
                    }
                }
            }
        }
        if let Some(ref container_id) = self.container_id {
            write!(f, "|c:{container_id}")?;
        }

        Ok(())
    }
}

pub(crate) struct Gauge {
    name: String,
    value: Vec<common::NumValue>,
    tags: Option<common::tags::Tags>,
    container_id: Option<String>,
}

impl fmt::Display for Gauge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // <METRIC_NAME>:<VALUE>|<TYPE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|c:<CONTAINER_ID>
        // <METRIC_NAME>:<VALUE1>:<VALUE2>:<VALUE3>|<TYPE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
        write!(f, "{name}", name = self.name)?;
        for val in &self.value {
            write!(f, ":{val}")?;
        }
        write!(f, "|g")?;
        if let Some(ref tags) = self.tags {
            if !tags.is_empty() {
                write!(f, "|#")?;
                let mut commas_remaining = tags.len() - 1;
                for (k, v) in tags.iter() {
                    write!(f, "{k}:{v}")?;
                    if commas_remaining != 0 {
                        write!(f, ",")?;
                        commas_remaining -= 1;
                    }
                }
            }
        }
        if let Some(ref container_id) = self.container_id {
            write!(f, "|c:{container_id}")?;
        }

        Ok(())
    }
}

pub(crate) struct Timer {
    name: String,
    value: Vec<common::NumValue>,
    sample_rate: Option<common::ZeroToOne>,
    tags: Option<common::tags::Tags>,
    container_id: Option<String>,
}

impl fmt::Display for Timer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // <METRIC_NAME>:<VALUE>|<TYPE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|c:<CONTAINER_ID>
        // <METRIC_NAME>:<VALUE1>:<VALUE2>:<VALUE3>|<TYPE>|@<SAMPLE_RATE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
        write!(f, "{name}", name = self.name)?;
        for val in &self.value {
            write!(f, ":{val}")?;
        }
        write!(f, "|ms")?;
        if let Some(ref sample_rate) = self.sample_rate {
            write!(f, "|@{sample_rate}")?;
        }
        if let Some(ref tags) = self.tags {
            if !tags.is_empty() {
                write!(f, "|#")?;
                let mut commas_remaining = tags.len() - 1;
                for (k, v) in tags.iter() {
                    write!(f, "{k}:{v}")?;
                    if commas_remaining != 0 {
                        write!(f, ",")?;
                        commas_remaining -= 1;
                    }
                }
            }
        }
        if let Some(ref container_id) = self.container_id {
            write!(f, "|c:{container_id}")?;
        }

        Ok(())
    }
}

pub(crate) struct Dist {
    name: String,
    value: Vec<common::NumValue>,
    sample_rate: Option<common::ZeroToOne>,
    tags: Option<common::tags::Tags>,
    container_id: Option<String>,
}

impl fmt::Display for Dist {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // <METRIC_NAME>:<VALUE>|<TYPE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|c:<CONTAINER_ID>
        // <METRIC_NAME>:<VALUE1>:<VALUE2>:<VALUE3>|<TYPE>|@<SAMPLE_RATE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
        write!(f, "{name}", name = self.name)?;
        for val in &self.value {
            write!(f, ":{val}")?;
        }
        write!(f, "|d")?;
        if let Some(ref sample_rate) = self.sample_rate {
            write!(f, "|@{sample_rate}")?;
        }
        if let Some(ref tags) = self.tags {
            if !tags.is_empty() {
                write!(f, "|#")?;
                let mut commas_remaining = tags.len() - 1;
                for (k, v) in tags.iter() {
                    write!(f, "{k}:{v}")?;
                    if commas_remaining != 0 {
                        write!(f, ",")?;
                        commas_remaining -= 1;
                    }
                }
            }
        }
        if let Some(ref container_id) = self.container_id {
            write!(f, "|c:{container_id}")?;
        }

        Ok(())
    }
}

pub(crate) struct Set {
    name: String,
    value: Vec<common::NumValue>,
    tags: Option<common::tags::Tags>,
    container_id: Option<String>,
}

impl fmt::Display for Set {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // <METRIC_NAME>:<VALUE>|<TYPE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|c:<CONTAINER_ID>
        // <METRIC_NAME>:<VALUE1>:<VALUE2>:<VALUE3>|<TYPE>|@<SAMPLE_RATE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
        write!(f, "{name}", name = self.name)?;
        for val in &self.value {
            write!(f, ":{val}")?;
        }
        write!(f, "|s")?;
        if let Some(ref tags) = self.tags {
            if !tags.is_empty() {
                write!(f, "|#")?;
                let mut commas_remaining = tags.len() - 1;
                for (k, v) in tags.iter() {
                    write!(f, "{k}:{v}")?;
                    if commas_remaining != 0 {
                        write!(f, ",")?;
                        commas_remaining -= 1;
                    }
                }
            }
        }
        if let Some(ref container_id) = self.container_id {
            write!(f, "|c:{container_id}")?;
        }

        Ok(())
    }
}

pub(crate) struct Histogram {
    name: String,
    value: Vec<common::NumValue>,
    sample_rate: Option<common::ZeroToOne>,
    tags: Option<common::tags::Tags>,
    container_id: Option<String>,
}

impl fmt::Display for Histogram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // <METRIC_NAME>:<VALUE>|<TYPE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|c:<CONTAINER_ID>
        // <METRIC_NAME>:<VALUE1>:<VALUE2>:<VALUE3>|<TYPE>|@<SAMPLE_RATE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
        write!(f, "{name}", name = self.name)?;
        for val in &self.value {
            write!(f, ":{val}")?;
        }
        write!(f, "|h")?;
        if let Some(ref sample_rate) = self.sample_rate {
            write!(f, "|@{sample_rate}")?;
        }
        if let Some(ref tags) = self.tags {
            if !tags.is_empty() {
                write!(f, "|#")?;
                let mut commas_remaining = tags.len() - 1;
                for (k, v) in tags.iter() {
                    write!(f, "{k}:{v}")?;
                    if commas_remaining != 0 {
                        write!(f, ",")?;
                        commas_remaining -= 1;
                    }
                }
            }
        }
        if let Some(ref container_id) = self.container_id {
            write!(f, "|c:{container_id}")?;
        }

        Ok(())
    }
}
