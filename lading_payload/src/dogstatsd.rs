//! `DogStatsD` payload.

use std::{fmt, io::Write, num::NonZeroUsize, ops::Range};

use rand::{distributions::WeightedIndex, prelude::Distribution, seq::SliceRandom, Rng};
use serde::Deserialize;

use crate::Error;

use self::{
    common::tags, event::EventGenerator, metric::MetricGenerator,
    service_check::ServiceCheckGenerator,
};

use super::{common::AsciiString, Generator};

mod common;
mod event;
mod metric;
mod service_check;

fn default_metric_names_minimum() -> NonZeroUsize {
    NonZeroUsize::new(1).unwrap()
}

fn default_metric_names_maximum() -> NonZeroUsize {
    NonZeroUsize::new(64).unwrap()
}

fn default_tag_keys_minimum() -> usize {
    0
}

fn default_tag_keys_maximum() -> usize {
    64
}

fn default_metric_multivalue() -> Vec<MetricValueWeight> {
    let mut weights = Vec::with_capacity(32);
    for idx in 1..32 {
        weights.push(MetricValueWeight {
            weight: 1,
            value: idx,
        });
    }
    weights
}

/// Weight for a `DogStatsD` multivalue message.
///
/// Defines the relative weight of a certain number of 'values' packed into a
/// single metric message.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct MetricValueWeight {
    value: u8,
    weight: u8,
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
#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct Config {
    /// Defines the minimum number of metric names allowed in a payload.
    #[serde(default = "default_metric_names_minimum")]
    pub metric_names_minimum: NonZeroUsize,
    /// Defines the maximum number of metric names allowed in a
    /// payload. Must be greater or equal to minimum.
    #[serde(default = "default_metric_names_maximum")]
    pub metric_names_maximum: NonZeroUsize,
    /// Defines the minimum number of metric names allowed in a payload.
    #[serde(default = "default_tag_keys_minimum")]
    pub tag_keys_minimum: usize,
    /// Defines the maximum number of metric names allowed in a
    /// payload. Must be greater or equal to minimum.
    #[serde(default = "default_tag_keys_maximum")]
    pub tag_keys_maximum: usize,
    /// Defines the relative probability of each kind of DogStatsD kinds of
    /// payload.
    #[serde(default)]
    pub kind_weights: KindWeights,
    /// Defines the relative probability of each kind of DogStatsD metric.
    #[serde(default)]
    pub metric_weights: MetricWeights,
    /// Defines the relative probability of a dogstatsd message having multiple values.
    /// Choices are weighted according to the specified weight.
    #[serde(default = "default_metric_multivalue")]
    pub metric_multivalue: Vec<MetricValueWeight>,
}

fn choose_or_not<R, T>(mut rng: &mut R, pool: &[T]) -> Option<T>
where
    T: Clone,
    R: rand::Rng + ?Sized,
{
    if rng.gen() {
        pool.choose(&mut rng).cloned()
    } else {
        None
    }
}

#[derive(Debug, Clone)]
struct MemberGenerator {
    kind_weights: WeightedIndex<u8>,
    event_generator: EventGenerator,
    service_check_generator: ServiceCheckGenerator,
    metric_generator: MetricGenerator,
}

#[inline]
fn random_strings_with_length<R>(min_max: Range<usize>, max_length: u16, rng: &mut R) -> Vec<String>
where
    R: Rng + ?Sized,
{
    let mut buf = Vec::with_capacity(min_max.end);
    for _ in 0..rng.gen_range(min_max) {
        buf.push(AsciiString::with_maximum_length(max_length).generate(rng));
    }
    buf
}

impl MemberGenerator {
    fn new<R>(
        metric_range: Range<NonZeroUsize>,
        key_range: Range<usize>,
        kind_weights: KindWeights,
        metric_weights: MetricWeights,
        metric_multivalue: &[MetricValueWeight],
        mut rng: &mut R,
    ) -> Self
    where
        R: Rng + ?Sized,
    {
        let metric_range = metric_range.start.get()..metric_range.end.get();

        let titles = random_strings_with_length(metric_range, 64, &mut rng);
        let texts_or_messages = random_strings_with_length(4..128, 1024, &mut rng);
        let small_strings = random_strings_with_length(16..1024, 8, &mut rng);

        let total_tag_sets = 512;
        let max_values_per_tag_set = 512;

        let mut tags = Vec::with_capacity(total_tag_sets);
        let tags_generator = tags::Generator::new(key_range, max_values_per_tag_set);
        for _ in 0..total_tag_sets {
            tags.push(tags_generator.generate(&mut rng));
        }

        let event_generator = EventGenerator {
            titles: titles.clone(),
            texts_or_messages: texts_or_messages.clone(),
            small_strings: small_strings.clone(),
            tags: tags.clone(),
        };

        let service_check_generator = ServiceCheckGenerator {
            names: titles.clone(),
            small_strings: small_strings.clone(),
            texts_or_messages,
            tags: tags.clone(),
        };

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
        let metric_generator = MetricGenerator {
            metric_weights: WeightedIndex::new(metric_choices).unwrap(),
            metric_multivalue_weights: WeightedIndex::new(
                metric_multivalue
                    .iter()
                    .map(|x| x.weight)
                    .collect::<Vec<u8>>(),
            )
            .unwrap(),
            metric_multivalue_choices: metric_multivalue.iter().map(|x| x.value).collect(),
            names: titles,
            container_ids: small_strings,
            tags,
        };

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

impl Generator<Member> for MemberGenerator {
    fn generate<R>(&self, rng: &mut R) -> Member
    where
        R: rand::Rng + ?Sized,
    {
        match self.kind_weights.sample(rng) {
            0 => Member::Metric(self.metric_generator.generate(rng)),
            1 => Member::Event(self.event_generator.generate(rng)),
            2 => Member::ServiceCheck(self.service_check_generator.generate(rng)),
            _ => unreachable!(),
        }
    }
}

// https://docs.datadoghq.com/developers/dogstatsd/datagram_shell/
enum Member {
    Metric(metric::Metric),
    Event(event::Event),
    ServiceCheck(service_check::ServiceCheck),
}

impl fmt::Display for Member {
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
/// `DogStatsD` payload
pub struct DogStatsD {
    member_generator: MemberGenerator,
}

impl DogStatsD {
    /// Create a new, default instance of `DogStatsD` with reasonable settings.
    ///
    /// # Panics
    ///
    /// Panics indicate a serious internal failure that has nothing to do with
    /// user input.
    pub fn default<R>(rng: &mut R) -> Self
    where
        R: rand::Rng + ?Sized,
    {
        Self::new(
            NonZeroUsize::new(1).unwrap()..NonZeroUsize::new(64).unwrap(),
            0..32,
            KindWeights::default(),
            MetricWeights::default(),
            &default_metric_multivalue(),
            rng,
        )
    }

    /// Create a new instance of `DogStatsD`
    pub fn new<R>(
        metric_names_range: Range<NonZeroUsize>,
        tag_keys_range: Range<usize>,
        kind_weights: KindWeights,
        metric_weights: MetricWeights,
        metric_multivalue: &[MetricValueWeight],
        rng: &mut R,
    ) -> Self
    where
        R: rand::Rng + ?Sized,
    {
        let member_generator = MemberGenerator::new(
            metric_names_range,
            tag_keys_range,
            kind_weights,
            metric_weights,
            metric_multivalue,
            rng,
        );

        Self { member_generator }
    }
}

impl crate::Serialize for DogStatsD {
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
    use std::num::NonZeroUsize;

    use proptest::prelude::*;
    use rand::{rngs::SmallRng, SeedableRng};

    use crate::{
        dogstatsd::{default_metric_multivalue, KindWeights, MetricWeights},
        DogStatsD, Serialize,
    };

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let metric_names_range =  NonZeroUsize::new(1).unwrap()..NonZeroUsize::new(64).unwrap();
            let tag_keys_range =  0..32;
            let kind_weights = KindWeights::default();
            let metric_weights = MetricWeights::default();
            let metric_multivalue_weights = default_metric_multivalue();
            let dogstatsd = DogStatsD::new(metric_names_range, tag_keys_range, kind_weights,
                                           metric_weights, &metric_multivalue_weights, &mut rng);

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
