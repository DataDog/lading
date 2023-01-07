use arbitrary::{size_hint::and_all, Unstructured};

use super::common;

const MAX_VALUES: usize = 8;

pub(crate) enum Metric {
    Count(Count),
    Gauge(Gauge),
    Timer(Timer),
    Histogram(Histogram),
    Set(Set),
    Distribution(Distribution),
}

impl ToString for Metric {
    fn to_string(&self) -> String {
        unimplemented!()
    }
}

pub(crate) struct Count {
    name: common::MetricTagStr,
    value: Vec<common::NumValue>,
    sample_rate: Option<common::ZeroToOne>,
    tags: Option<common::Tags>,
    container_id: Option<common::MetricTagStr>,
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
    value: Vec<common::NumValue>,
    tags: Option<common::Tags>,
    container_id: Option<common::MetricTagStr>,
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
    value: Vec<common::NumValue>,
    sample_rate: Option<common::ZeroToOne>,
    tags: Option<common::Tags>,
    container_id: Option<common::MetricTagStr>,
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
    value: Vec<common::NumValue>,
    sample_rate: Option<common::ZeroToOne>,
    tags: Option<common::Tags>,
    container_id: Option<common::MetricTagStr>,
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
    value: Vec<common::NumValue>,
    tags: Option<common::Tags>,
    container_id: Option<common::MetricTagStr>,
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
    value: Vec<common::NumValue>,
    sample_rate: Option<common::ZeroToOne>,
    tags: Option<common::Tags>,
    container_id: Option<common::MetricTagStr>,
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
