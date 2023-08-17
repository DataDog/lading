use crate::dogstatsd::common;

#[derive(Clone, Debug)]
/// A metric `Template` is a `super::Metric` that lacks values and has no
/// container ID but has every other attribute of a `super::Metric`.
pub(crate) enum Template {
    Count(Count),
    Gauge(Gauge),
    Timer(Timer),
    Histogram(Histogram),
    Set(Set),
    Distribution(Dist),
}

#[derive(Clone, Debug)]
/// The count type in `DogStatsD` metric format. Monotonically increasing value.
pub(crate) struct Count {
    pub(crate) name: String,
    pub(crate) tags: common::tags::Tagset,
}

#[derive(Clone, Debug)]
/// The gauge type in `DogStatsD` format.
pub(crate) struct Gauge {
    pub(crate) name: String,
    pub(crate) tags: common::tags::Tagset,
}

#[derive(Clone, Debug)]
/// The timer type in `DogStatsD` format.
pub(crate) struct Timer {
    pub(crate) name: String,
    pub(crate) tags: common::tags::Tagset,
}

#[derive(Clone, Debug)]
/// The distribution type in `DogStatsD` format.
pub(crate) struct Dist {
    pub(crate) name: String,
    pub(crate) tags: common::tags::Tagset,
}

#[derive(Clone, Debug)]
/// The set type in `DogStatsD` format.
pub(crate) struct Set {
    pub(crate) name: String,
    pub(crate) tags: common::tags::Tagset,
}

#[derive(Clone, Debug)]
/// The histogram type in `DogStatsD` format.
pub(crate) struct Histogram {
    pub(crate) name: String,
    pub(crate) tags: common::tags::Tagset,
}
