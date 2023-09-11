//! `DogStatsD` payload.

use std::{fmt, io::Write, ops::Range, rc::Rc};

use rand::{distributions::WeightedIndex, prelude::Distribution, seq::SliceRandom, Rng};
use serde::Deserialize;

use crate::{common::strings, Error, Serialize};

use self::{
    common::tags, event::EventGenerator, metric::MetricGenerator,
    service_check::ServiceCheckGenerator,
};

use super::Generator;

mod common;
mod event;
mod metric;
mod service_check;

fn contexts_minimum() -> u16 {
    5000
}

fn contexts_maximum() -> u16 {
    10_000
}

fn value_minimum() -> f64 {
    f64::MIN
}

fn value_maximum() -> f64 {
    f64::MAX
}

// https://docs.datadoghq.com/developers/guide/what-best-practices-are-recommended-for-naming-metrics-and-tags/#rules-and-best-practices-for-naming-metrics
fn name_length_minimum() -> u16 {
    1
}

fn name_length_maximum() -> u16 {
    200
}

fn tag_key_length_minimum() -> u16 {
    1
}

fn tag_key_length_maximum() -> u16 {
    100
}

fn tag_value_length_minimum() -> u16 {
    1
}

fn tag_value_length_maximum() -> u16 {
    100
}

fn tags_per_msg_minimum() -> u16 {
    2
}

fn tags_per_msg_maximum() -> u16 {
    50
}

fn multivalue_pack_probability() -> f32 {
    0.08
}

fn multivalue_count_minimum() -> u16 {
    2
}

fn multivalue_count_maximum() -> u16 {
    32
}

/// Weights for `DogStatsD` kinds: metrics, events, service checks
///
/// Defines the relative probability of each kind of `DogStatsD` datagram.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct KindWeights {
    metric: u8,
    event: u8,
    service_check: u8,
}

impl Default for KindWeights {
    fn default() -> Self {
        KindWeights {
            metric: 80,        // 80%
            event: 10,         // 10%
            service_check: 10, // 10%
        }
    }
}

/// Weights for `DogStatsD` metrics: gauges, counters, etc
#[derive(Debug, Deserialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct MetricWeights {
    count: u8,
    gauge: u8,
    timer: u8,
    distribution: u8,
    set: u8,
    histogram: u8,
}

impl Default for MetricWeights {
    fn default() -> Self {
        MetricWeights {
            count: 34,       // 34%
            gauge: 34,       // 34%
            timer: 5,        // 5%
            distribution: 1, // 1%
            set: 1,          // 1%
            histogram: 25,   // 25%
        }
    }
}

/// Configure the `DogStatsD` payload.
#[derive(Debug, Deserialize, Clone, PartialEq, Copy)]
pub struct Config {
    /// Minimum number of unique metric contexts to generate
    /// A context is a set of unique metric name + tags
    #[serde(default = "contexts_minimum")]
    pub contexts_minimum: u16,

    /// Maximum number of unique metric contexts to generate
    /// A context is a set of unique metric name + tags
    #[serde(default = "contexts_maximum")]
    pub contexts_maximum: u16,

    /// Minimum length for a dogstatsd message name
    #[serde(default = "name_length_minimum")]
    pub name_length_minimum: u16,

    /// Maximum length for a dogstatsd message name
    #[serde(default = "name_length_maximum")]
    pub name_length_maximum: u16,

    /// Minimum length for the 'key' part of a dogstatsd tag
    #[serde(default = "tag_key_length_minimum")]
    pub tag_key_length_minimum: u16,

    /// Maximum length for the 'key' part of a dogstatsd tag
    #[serde(default = "tag_key_length_maximum")]
    pub tag_key_length_maximum: u16,

    /// Minimum length for the 'value' part of a dogstatsd tag
    #[serde(default = "tag_value_length_minimum")]
    pub tag_value_length_minimum: u16,

    /// Maximum length for the 'value' part of a dogstatsd tag
    #[serde(default = "tag_value_length_maximum")]
    pub tag_value_length_maximum: u16,

    /// Maximum number of tags per individual dogstatsd msg
    /// a tag is a key-value pair separated by a :
    #[serde(default = "tags_per_msg_maximum")]
    pub tags_per_msg_maximum: u16,

    /// Minimum number of tags per individual dogstatsd msg
    /// a tag is a key-value pair separated by a :
    #[serde(default = "tags_per_msg_minimum")]
    pub tags_per_msg_minimum: u16,

    /// Probability between 0 and 1 that a given dogstatsd msg
    /// contains multiple values
    #[serde(default = "multivalue_pack_probability")]
    pub multivalue_pack_probability: f32,

    /// The minimum count of values that will be generated if
    /// multi-value is chosen to be generated
    #[serde(default = "multivalue_count_minimum")]
    pub multivalue_count_minimum: u16,

    /// The maximum count of values that will be generated if
    /// multi-value is chosen to be generated
    #[serde(default = "multivalue_count_maximum")]
    pub multivalue_count_maximum: u16,

    /// Defines the relative probability of each kind of DogStatsD kinds of
    /// payload.
    #[serde(default)]
    pub kind_weights: KindWeights,

    /// Defines the relative probability of each kind of DogStatsD metric.
    #[serde(default)]
    pub metric_weights: MetricWeights,

    /// The minimum value to appear in metrics.
    #[serde(default = "value_minimum")]
    pub value_minimum: f64,

    /// The maximum value to appear in metrics.
    #[serde(default = "value_maximum")]
    pub value_maximum: f64,
}

fn choose_or_not_ref<'a, R, T>(mut rng: &mut R, pool: &'a [T]) -> Option<&'a T>
where
    R: rand::Rng + ?Sized,
{
    if rng.gen() {
        pool.choose(&mut rng)
    } else {
        None
    }
}

fn choose_or_not_fn<R, T, F>(rng: &mut R, func: F) -> Option<T>
where
    T: Clone,
    R: rand::Rng + ?Sized,
    F: FnOnce(&mut R) -> Option<T>,
{
    if rng.gen() {
        func(rng)
    } else {
        None
    }
}

#[inline]
/// Generate a total number of strings randomly chosen from the range `min_max` with a maximum length
/// per string of `max_length`.
fn random_strings_with_length<R>(
    pool: &strings::Pool,
    min_max: Range<usize>,
    max_length: u16,
    rng: &mut R,
) -> Vec<String>
where
    R: Rng + ?Sized,
{
    let total = rng.gen_range(min_max);
    let length_range = 1..max_length;

    random_strings_with_length_range(pool, total, length_range, rng)
}

#[inline]
/// Generate a `total` number of strings with a maximum length per string of
/// `max_length`.
fn random_strings_with_length_range<R>(
    pool: &strings::Pool,
    total: usize,
    length_range: Range<u16>,
    mut rng: &mut R,
) -> Vec<String>
where
    R: Rng + ?Sized,
{
    let mut buf = Vec::with_capacity(total);
    for _ in 0..total {
        buf.push(String::from(
            pool.of_size_range(&mut rng, length_range.clone()).unwrap(),
        ));
    }
    buf
}

#[derive(Debug, Clone)]
struct MemberGenerator {
    kind_weights: WeightedIndex<u8>,
    event_generator: EventGenerator,
    service_check_generator: ServiceCheckGenerator,
    metric_generator: MetricGenerator,
}

impl MemberGenerator {
    #[allow(clippy::too_many_arguments)]
    fn new<R>(
        context_range: Range<u16>,
        name_length_range: Range<u16>,
        tag_key_length_range: Range<u16>,
        tag_value_length_range: Range<u16>,
        tags_per_msg_range: Range<u16>,
        multivalue_count_range: Range<u16>,
        multivalue_pack_probability: f32,
        kind_weights: KindWeights,
        metric_weights: MetricWeights,
        num_value_range: Range<f64>,
        mut rng: &mut R,
    ) -> Self
    where
        R: Rng + ?Sized,
    {
        let pool = Rc::new(strings::Pool::with_size(&mut rng, 8_000_000));

        let context_range: Range<usize> =
            context_range.start.try_into().unwrap()..context_range.end.try_into().unwrap();

        let tags_per_msg_range: Range<usize> = tags_per_msg_range.start.try_into().unwrap()
            ..tags_per_msg_range.end.try_into().unwrap();

        let num_contexts = rng.gen_range(context_range);

        let tags_generator = tags::Generator {
            num_tagsets: num_contexts,
            tags_per_msg_range,
            tag_key_length_range,
            tag_value_length_range,
            str_pool: Rc::clone(&pool),
        };

        let service_event_titles = random_strings_with_length_range(
            pool.as_ref(),
            num_contexts,
            name_length_range.clone(),
            &mut rng,
        );
        let tagsets = tags_generator.generate(&mut rng);
        drop(tags_generator); // now unused, clear up its allocation s

        let texts_or_messages = random_strings_with_length(pool.as_ref(), 4..128, 1024, &mut rng);
        let small_strings = random_strings_with_length(pool.as_ref(), 16..1024, 8, &mut rng);

        // NOTE the ordering here of `metric_choices` is very important! If you
        // change it here you MUST also change it in `Generator<Metric> for
        // MetricGenerator`.
        let metric_choices = [
            metric_weights.count,
            metric_weights.gauge,
            metric_weights.timer,
            metric_weights.distribution,
            metric_weights.set,
            metric_weights.histogram,
        ];

        let event_generator = EventGenerator {
            str_pool: Rc::clone(&pool),
            title_length_range: name_length_range.clone(),
            texts_or_messages_length_range: 1..1024,
            small_strings_length_range: 1..8,
            tagsets: tagsets.clone(),
        };

        let service_check_generator = ServiceCheckGenerator {
            names: service_event_titles.clone(),
            small_strings: small_strings.clone(),
            texts_or_messages,
            tagsets: tagsets.clone(),
        };

        let metric_generator = MetricGenerator::new(
            num_contexts,
            name_length_range.clone(),
            multivalue_count_range.clone(),
            multivalue_pack_probability,
            &WeightedIndex::new(metric_choices).unwrap(),
            small_strings,
            tagsets.clone(),
            pool.as_ref(),
            num_value_range,
            &mut rng,
        );

        // NOTE the ordering here of `member_choices` is very important! If you
        // change it here you MUST also change it in `Generator<Member> for
        // MemberGenerator`.
        let member_choices = [
            kind_weights.metric,
            kind_weights.event,
            kind_weights.service_check,
        ];
        MemberGenerator {
            kind_weights: WeightedIndex::new(member_choices).unwrap(),
            event_generator,
            service_check_generator,
            metric_generator,
        }
    }
}

impl MemberGenerator {
    fn generate<'a, R>(&'a self, rng: &mut R) -> Member<'a>
    where
        R: rand::Rng + ?Sized,
    {
        match self.kind_weights.sample(rng) {
            0 => Member::Metric(self.metric_generator.generate(rng)),
            1 => {
                let event = self.event_generator.generate(rng);
                Member::Event(event)
            }
            2 => Member::ServiceCheck(self.service_check_generator.generate(rng)),
            _ => unreachable!(),
        }
    }
}

// https://docs.datadoghq.com/developers/dogstatsd/datagram_shell/
#[derive(Debug)]
/// Supra-type for all dogstatsd variants
pub enum Member<'a> {
    /// Metrics
    Metric(metric::Metric<'a>),
    /// Events, think syslog.
    Event(event::Event<'a>),
    /// Services, checked.
    ServiceCheck(service_check::ServiceCheck<'a>),
}

impl<'a> fmt::Display for Member<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Metric(ref m) => write!(f, "{m}"),
            Self::Event(ref e) => write!(f, "{e}"),
            Self::ServiceCheck(ref sc) => write!(f, "{sc}"),
        }
    }
}

#[derive(Debug, Clone)]
#[allow(clippy::module_name_repetitions)]
/// A generator for `DogStatsD` payloads
pub struct DogStatsD {
    member_generator: MemberGenerator,
}

impl DogStatsD {
    /// Create a new default instance of `DogStatsD` with reasonable settings.
    pub fn default<R>(rng: &mut R) -> Self
    where
        R: rand::Rng + ?Sized,
    {
        Self::new(
            contexts_minimum()..contexts_maximum(),
            name_length_minimum()..name_length_maximum(),
            tag_key_length_minimum()..tag_key_length_maximum(),
            tag_value_length_minimum()..tag_value_length_maximum(),
            tags_per_msg_minimum()..tags_per_msg_maximum(),
            multivalue_count_minimum()..multivalue_count_maximum(),
            multivalue_pack_probability(),
            KindWeights::default(),
            MetricWeights::default(),
            value_minimum()..value_maximum(),
            rng,
        )
    }

    #[cfg(feature = "dogstatsd_perf")]
    /// Call the internal member generator and count the in-memory byte
    /// size. This is not useful except in a loop to track how quickly we can do
    /// this operation. It's meant to be a proxy by which we can determine how
    /// quickly members are able to be generated and then serialized. An
    /// approximation.
    pub fn generate<R>(&self, rng: &mut R) -> Member
    where
        R: rand::Rng + ?Sized,
    {
        self.member_generator.generate(rng)
    }

    /// Create a new instance of `DogStatsD`.
    #[allow(clippy::too_many_arguments)]
    pub fn new<R>(
        context_range: Range<u16>,
        name_length_range: Range<u16>,
        tag_key_length_range: Range<u16>,
        tag_value_length_range: Range<u16>,
        tags_per_msg_range: Range<u16>,
        multivalue_count_range: Range<u16>,
        multivalue_pack_probability: f32,
        kind_weights: KindWeights,
        metric_weights: MetricWeights,
        num_value_range: Range<f64>,
        rng: &mut R,
    ) -> Self
    where
        R: rand::Rng + ?Sized,
    {
        let member_generator = MemberGenerator::new(
            context_range,
            name_length_range,
            tag_key_length_range,
            tag_value_length_range,
            tags_per_msg_range,
            multivalue_count_range,
            multivalue_pack_probability,
            kind_weights,
            metric_weights,
            num_value_range,
            rng,
        );

        Self { member_generator }
    }
}

impl Serialize for DogStatsD {
    fn to_bytes<W, R>(&self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        let mut bytes_remaining = max_bytes;
        loop {
            let member: Member = self.member_generator.generate(&mut rng);
            let encoding = format!("{member}");
            let line_length = encoding.len() + 1; // add one for the newline
            match bytes_remaining.checked_sub(line_length) {
                Some(remainder) => {
                    writeln!(writer, "{encoding}")?;
                    bytes_remaining = remainder;
                }
                None => break,
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;
    use rand::{rngs::SmallRng, SeedableRng};

    use crate::{
        dogstatsd::{
            contexts_maximum, contexts_minimum, multivalue_count_maximum, multivalue_count_minimum,
            multivalue_pack_probability, name_length_maximum, name_length_minimum,
            tag_key_length_maximum, tag_key_length_minimum, tag_value_length_maximum,
            tag_value_length_minimum, tags_per_msg_maximum, tags_per_msg_minimum, value_maximum,
            value_minimum, KindWeights, MetricWeights,
        },
        DogStatsD, Serialize,
    };

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let context_range = contexts_minimum()..contexts_maximum();
            let name_length_range = name_length_minimum()..name_length_maximum();
            let tag_key_length_range = tag_key_length_minimum()..tag_key_length_maximum();
            let tag_value_length_range = tag_value_length_minimum()..tag_value_length_maximum();
            let tags_per_msg_range = tags_per_msg_minimum()..tags_per_msg_maximum();
            let multivalue_count_range = multivalue_count_minimum()..multivalue_count_maximum();
            let multivalue_pack_probability = multivalue_pack_probability();
            let num_value_range = value_minimum()..value_maximum();

            let kind_weights = KindWeights::default();
            let metric_weights = MetricWeights::default();
            let dogstatsd = DogStatsD::new(context_range, name_length_range, tag_key_length_range,
                                           tag_value_length_range, tags_per_msg_range,
                                           multivalue_count_range, multivalue_pack_probability, kind_weights,
                                           metric_weights, num_value_range, &mut rng);

            let mut bytes = Vec::with_capacity(max_bytes);
            dogstatsd.to_bytes(rng, max_bytes, &mut bytes).unwrap();
            debug_assert!(
                bytes.len() <= max_bytes,
                "{:?}",
                std::str::from_utf8(&bytes).unwrap()
            );
        }
    }
}
