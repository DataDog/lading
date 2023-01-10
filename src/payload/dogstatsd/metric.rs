use std::fmt;

use arbitrary::{size_hint::and_all, Arbitrary, Unstructured};

use super::common::{self, NonEmptyVec};

const MAX_VALUES: usize = 8;

#[derive(Arbitrary)]
pub(crate) enum Metric {
    Count(Count),
    Gauge(Gauge),
    Timer(Timer),
    Histogram(Histogram),
    Set(Set),
    Distribution(Distribution),
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
    name: common::MetricTagStr,
    value: NonEmptyVec<common::NumValue>,
    sample_rate: Option<common::ZeroToOne>,
    tags: Option<common::Tags>,
    container_id: Option<common::MetricTagStr>,
}

impl fmt::Display for Count {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // <METRIC_NAME>:<VALUE>|<TYPE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|c:<CONTAINER_ID>
        // <METRIC_NAME>:<VALUE1>:<VALUE2>:<VALUE3>|<TYPE>|@<SAMPLE_RATE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
        write!(f, "{name}", name = self.name)?;
        let mut colons_needed = self.value.inner.len() - 1;
        for val in &self.value.inner {
            write!(f, "{val}")?;
            if colons_needed != 0 {
                write!(f, ":")?;
                colons_needed -= 1;
            }
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

impl<'a> arbitrary::Arbitrary<'a> for Count {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self {
            name: u.arbitrary()?,
            value: u.arbitrary()?,
            sample_rate: u.arbitrary()?,
            tags: u.arbitrary()?,
            container_id: u.arbitrary()?,
        })
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        let name_sz = common::MetricTagStr::size_hint(depth);
        let value_sz = {
            let (low, upper) = common::NumValue::size_hint(depth);
            (low * MAX_VALUES, upper.map(|u| u * MAX_VALUES))
        };
        let sample_sz = common::ZeroToOne::size_hint(depth);
        let tags_sz = common::Tags::size_hint(depth);
        let container_id_sz = common::MetricTagStr::size_hint(depth);
        and_all(&[name_sz, value_sz, sample_sz, tags_sz, container_id_sz])
    }
}

pub(crate) struct Gauge {
    name: common::MetricTagStr,
    value: NonEmptyVec<common::NumValue>,
    tags: Option<common::Tags>,
    container_id: Option<common::MetricTagStr>,
}

impl fmt::Display for Gauge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // <METRIC_NAME>:<VALUE>|<TYPE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|c:<CONTAINER_ID>
        // <METRIC_NAME>:<VALUE1>:<VALUE2>:<VALUE3>|<TYPE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
        write!(f, "{name}", name = self.name)?;
        let mut colons_needed = self.value.inner.len() - 1;
        for val in &self.value.inner {
            write!(f, "{val}")?;
            if colons_needed != 0 {
                write!(f, ":")?;
                colons_needed -= 1;
            }
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

impl<'a> arbitrary::Arbitrary<'a> for Gauge {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self {
            name: u.arbitrary()?,
            value: u.arbitrary()?,
            tags: u.arbitrary()?,
            container_id: u.arbitrary()?,
        })
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        let name_sz = common::MetricTagStr::size_hint(depth);
        let value_sz = {
            let (low, upper) = common::NumValue::size_hint(depth);
            (low * MAX_VALUES, upper.map(|u| u * MAX_VALUES))
        };
        let tags_sz = common::Tags::size_hint(depth);
        let container_id_sz = common::MetricTagStr::size_hint(depth);
        and_all(&[name_sz, value_sz, tags_sz, container_id_sz])
    }
}

pub(crate) struct Timer {
    name: common::MetricTagStr,
    value: NonEmptyVec<common::NumValue>,
    sample_rate: Option<common::ZeroToOne>,
    tags: Option<common::Tags>,
    container_id: Option<common::MetricTagStr>,
}

impl fmt::Display for Timer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // <METRIC_NAME>:<VALUE>|<TYPE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|c:<CONTAINER_ID>
        // <METRIC_NAME>:<VALUE1>:<VALUE2>:<VALUE3>|<TYPE>|@<SAMPLE_RATE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
        write!(f, "{name}", name = self.name)?;
        let mut colons_needed = self.value.inner.len() - 1;
        for val in &self.value.inner {
            write!(f, "{val}")?;
            if colons_needed != 0 {
                write!(f, ":")?;
                colons_needed -= 1;
            }
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

impl<'a> arbitrary::Arbitrary<'a> for Timer {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self {
            name: u.arbitrary()?,
            value: u.arbitrary()?,
            sample_rate: u.arbitrary()?,
            tags: u.arbitrary()?,
            container_id: u.arbitrary()?,
        })
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        let name_sz = common::MetricTagStr::size_hint(depth);
        let value_sz = {
            let (low, upper) = common::NumValue::size_hint(depth);
            (low * MAX_VALUES, upper.map(|u| u * MAX_VALUES))
        };
        let sample_sz = common::ZeroToOne::size_hint(depth);
        let tags_sz = common::Tags::size_hint(depth);
        let container_id_sz = common::MetricTagStr::size_hint(depth);
        and_all(&[name_sz, value_sz, sample_sz, tags_sz, container_id_sz])
    }
}

pub(crate) struct Distribution {
    name: common::MetricTagStr,
    value: NonEmptyVec<common::NumValue>,
    sample_rate: Option<common::ZeroToOne>,
    tags: Option<common::Tags>,
    container_id: Option<common::MetricTagStr>,
}

impl fmt::Display for Distribution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // <METRIC_NAME>:<VALUE>|<TYPE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|c:<CONTAINER_ID>
        // <METRIC_NAME>:<VALUE1>:<VALUE2>:<VALUE3>|<TYPE>|@<SAMPLE_RATE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
        write!(f, "{name}", name = self.name)?;
        let mut colons_needed = self.value.inner.len() - 1;
        for val in &self.value.inner {
            write!(f, "{val}")?;
            if colons_needed != 0 {
                write!(f, ":")?;
                colons_needed -= 1;
            }
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

impl<'a> arbitrary::Arbitrary<'a> for Distribution {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self {
            name: u.arbitrary()?,
            value: u.arbitrary()?,
            sample_rate: u.arbitrary()?,
            tags: u.arbitrary()?,
            container_id: u.arbitrary()?,
        })
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        let name_sz = common::MetricTagStr::size_hint(depth);
        let value_sz = {
            let (low, upper) = common::NumValue::size_hint(depth);
            (low * MAX_VALUES, upper.map(|u| u * MAX_VALUES))
        };
        let sample_sz = common::ZeroToOne::size_hint(depth);
        let tags_sz = common::Tags::size_hint(depth);
        let container_id_sz = common::MetricTagStr::size_hint(depth);
        and_all(&[name_sz, value_sz, sample_sz, tags_sz, container_id_sz])
    }
}

pub(crate) struct Set {
    name: common::MetricTagStr,
    value: NonEmptyVec<common::NumValue>,
    tags: Option<common::Tags>,
    container_id: Option<common::MetricTagStr>,
}

impl fmt::Display for Set {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // <METRIC_NAME>:<VALUE>|<TYPE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|c:<CONTAINER_ID>
        // <METRIC_NAME>:<VALUE1>:<VALUE2>:<VALUE3>|<TYPE>|@<SAMPLE_RATE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
        write!(f, "{name}", name = self.name)?;
        let mut colons_needed = self.value.inner.len() - 1;
        for val in &self.value.inner {
            write!(f, "{val}")?;
            if colons_needed != 0 {
                write!(f, ":")?;
                colons_needed -= 1;
            }
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

impl<'a> arbitrary::Arbitrary<'a> for Set {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self {
            name: u.arbitrary()?,
            value: u.arbitrary()?,
            tags: u.arbitrary()?,
            container_id: u.arbitrary()?,
        })
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        let name_sz = common::MetricTagStr::size_hint(depth);
        let value_sz = {
            let (low, upper) = common::NumValue::size_hint(depth);
            (low * MAX_VALUES, upper.map(|u| u * MAX_VALUES))
        };
        let tags_sz = common::Tags::size_hint(depth);
        let container_id_sz = common::MetricTagStr::size_hint(depth);
        and_all(&[name_sz, value_sz, tags_sz, container_id_sz])
    }
}

pub(crate) struct Histogram {
    name: common::MetricTagStr,
    value: NonEmptyVec<common::NumValue>,
    sample_rate: Option<common::ZeroToOne>,
    tags: Option<common::Tags>,
    container_id: Option<common::MetricTagStr>,
}

impl fmt::Display for Histogram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // <METRIC_NAME>:<VALUE>|<TYPE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|c:<CONTAINER_ID>
        // <METRIC_NAME>:<VALUE1>:<VALUE2>:<VALUE3>|<TYPE>|@<SAMPLE_RATE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
        write!(f, "{name}", name = self.name)?;
        let mut colons_needed = self.value.inner.len() - 1;
        for val in &self.value.inner {
            write!(f, "{val}")?;
            if colons_needed != 0 {
                write!(f, ":")?;
                colons_needed -= 1;
            }
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

impl<'a> arbitrary::Arbitrary<'a> for Histogram {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self {
            name: u.arbitrary()?,
            value: u.arbitrary()?,
            sample_rate: u.arbitrary()?,
            tags: u.arbitrary()?,
            container_id: u.arbitrary()?,
        })
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        let name_sz = common::MetricTagStr::size_hint(depth);
        let value_sz = {
            let (low, upper) = common::NumValue::size_hint(depth);
            (low * MAX_VALUES, upper.map(|u| u * MAX_VALUES))
        };
        let sample_sz = common::ZeroToOne::size_hint(depth);
        let tags_sz = common::Tags::size_hint(depth);
        let container_id_sz = common::MetricTagStr::size_hint(depth);
        and_all(&[name_sz, value_sz, sample_sz, tags_sz, container_id_sz])
    }
}
