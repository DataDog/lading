use std::fmt;

use rand::{distributions::Standard, prelude, Rng};

use super::common;

pub(crate) enum Metric {
    Count(Count),
    Gauge(Gauge),
    Timer(Timer),
    Histogram(Histogram),
    Set(Set),
    Distribution(Distribution),
}

impl prelude::Distribution<Metric> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Metric
    where
        R: Rng + ?Sized,
    {
        match rng.gen_range(0..6) {
            0 => Metric::Count(rng.gen()),
            1 => Metric::Gauge(rng.gen()),
            2 => Metric::Timer(rng.gen()),
            3 => Metric::Histogram(rng.gen()),
            4 => Metric::Set(rng.gen()),
            5 => Metric::Distribution(rng.gen()),
            _ => unreachable!(),
        }
    }
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
    tags: Option<common::Tags>,
    container_id: Option<String>,
}

impl prelude::Distribution<Count> for Standard {
    fn sample<R>(&self, mut rng: &mut R) -> Count
    where
        R: Rng + ?Sized,
    {
        let container_id = if rng.gen() {
            Some(format!("{}", rng.gen_range(0..32)))
        } else {
            None
        };
        let total_values = rng.gen_range(0..32);
        let value: Vec<common::NumValue> =
            Standard.sample_iter(&mut rng).take(total_values).collect();

        Count {
            name: format!("{}", rng.gen::<u8>()),
            value,
            sample_rate: rng.gen(),
            tags: rng.gen(),
            container_id,
        }
    }
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
            if !tags.inner.is_empty() {
                write!(f, "|#")?;
                let mut commas_remaining = tags.inner.len() - 1;
                for (k, v) in &tags.inner {
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
    tags: Option<common::Tags>,
    container_id: Option<String>,
}

impl prelude::Distribution<Gauge> for Standard {
    fn sample<R>(&self, mut rng: &mut R) -> Gauge
    where
        R: Rng + ?Sized,
    {
        let container_id = if rng.gen() {
            Some(format!("{}", rng.gen_range(0..32)))
        } else {
            None
        };
        let total_values = rng.gen_range(0..32);
        let value: Vec<common::NumValue> =
            Standard.sample_iter(&mut rng).take(total_values).collect();

        Gauge {
            name: format!("{}", rng.gen::<u8>()),
            value,
            tags: rng.gen(),
            container_id,
        }
    }
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
            if !tags.inner.is_empty() {
                write!(f, "|#")?;
                let mut commas_remaining = tags.inner.len() - 1;
                for (k, v) in &tags.inner {
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
    tags: Option<common::Tags>,
    container_id: Option<String>,
}

impl prelude::Distribution<Timer> for Standard {
    fn sample<R>(&self, mut rng: &mut R) -> Timer
    where
        R: Rng + ?Sized,
    {
        let container_id = if rng.gen() {
            Some(format!("{}", rng.gen_range(0..32)))
        } else {
            None
        };
        let total_values = rng.gen_range(0..32);
        let value: Vec<common::NumValue> =
            Standard.sample_iter(&mut rng).take(total_values).collect();

        Timer {
            name: format!("{}", rng.gen::<u8>()),
            value,
            sample_rate: rng.gen(),
            tags: rng.gen(),
            container_id,
        }
    }
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
            if !tags.inner.is_empty() {
                write!(f, "|#")?;
                let mut commas_remaining = tags.inner.len() - 1;
                for (k, v) in &tags.inner {
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

pub(crate) struct Distribution {
    name: String,
    value: Vec<common::NumValue>,
    sample_rate: Option<common::ZeroToOne>,
    tags: Option<common::Tags>,
    container_id: Option<String>,
}

impl prelude::Distribution<Distribution> for Standard {
    fn sample<R>(&self, mut rng: &mut R) -> Distribution
    where
        R: Rng + ?Sized,
    {
        let container_id = if rng.gen() {
            Some(format!("{}", rng.gen_range(0..32)))
        } else {
            None
        };
        let total_values = rng.gen_range(0..32);
        let value: Vec<common::NumValue> =
            Standard.sample_iter(&mut rng).take(total_values).collect();

        Distribution {
            name: format!("{}", rng.gen::<u8>()),
            value,
            sample_rate: rng.gen(),
            tags: rng.gen(),
            container_id,
        }
    }
}

impl fmt::Display for Distribution {
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
            if !tags.inner.is_empty() {
                write!(f, "|#")?;
                let mut commas_remaining = tags.inner.len() - 1;
                for (k, v) in &tags.inner {
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
    tags: Option<common::Tags>,
    container_id: Option<String>,
}

impl prelude::Distribution<Set> for Standard {
    fn sample<R>(&self, mut rng: &mut R) -> Set
    where
        R: Rng + ?Sized,
    {
        let container_id = if rng.gen() {
            Some(format!("{}", rng.gen_range(0..32)))
        } else {
            None
        };
        let total_values = rng.gen_range(0..32);
        let value: Vec<common::NumValue> =
            Standard.sample_iter(&mut rng).take(total_values).collect();

        Set {
            name: format!("{}", rng.gen::<u8>()),
            value,
            tags: rng.gen(),
            container_id,
        }
    }
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
            if !tags.inner.is_empty() {
                write!(f, "|#")?;
                let mut commas_remaining = tags.inner.len() - 1;
                for (k, v) in &tags.inner {
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
    tags: Option<common::Tags>,
    container_id: Option<String>,
}

impl prelude::Distribution<Histogram> for Standard {
    fn sample<R>(&self, mut rng: &mut R) -> Histogram
    where
        R: Rng + ?Sized,
    {
        let container_id = if rng.gen() {
            Some(format!("{}", rng.gen_range(0..32)))
        } else {
            None
        };
        let total_values = rng.gen_range(0..32);
        let value: Vec<common::NumValue> =
            Standard.sample_iter(&mut rng).take(total_values).collect();

        Histogram {
            name: format!("{}", rng.gen::<u8>()),
            value,
            sample_rate: rng.gen(),
            tags: rng.gen(),
            container_id,
        }
    }
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
            if !tags.inner.is_empty() {
                write!(f, "|#")?;
                let mut commas_remaining = tags.inner.len() - 1;
                for (k, v) in &tags.inner {
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
