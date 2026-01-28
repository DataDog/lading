#[derive(Clone, Debug)]
/// A metric `Template` is a `super::Metric` that lacks values and has no
/// container ID but has every other attribute of a `super::Metric`.
pub(crate) enum Template<H> {
    Count(Count<H>),
    Gauge(Gauge<H>),
    Timer(Timer<H>),
    Histogram(Histogram<H>),
    Set(Set<H>),
    Distribution(Dist<H>),
}

#[derive(Clone, Debug)]
/// The count type in `DogStatsD` metric format. Monotonically increasing value.
pub(crate) struct Count<H> {
    pub(crate) name: H,
}

#[derive(Clone, Debug)]
/// The gauge type in `DogStatsD` format.
pub(crate) struct Gauge<H> {
    pub(crate) name: H,
}

#[derive(Clone, Debug)]
/// The timer type in `DogStatsD` format.
pub(crate) struct Timer<H> {
    pub(crate) name: H,
}

#[derive(Clone, Debug)]
/// The distribution type in `DogStatsD` format.
pub(crate) struct Dist<H> {
    pub(crate) name: H,
}

#[derive(Clone, Debug)]
/// The set type in `DogStatsD` format.
pub(crate) struct Set<H> {
    pub(crate) name: H,
}

#[derive(Clone, Debug)]
/// The histogram type in `DogStatsD` format.
pub(crate) struct Histogram<H> {
    pub(crate) name: H,
}
