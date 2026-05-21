//! `DogStatsD` payload.

use std::{fmt, io::Write, rc::Rc};

use rand::{Rng, RngExt, distr::weighted::WeightedIndex, prelude::Distribution};
use serde::Deserialize;
use tracing::{debug, warn};

use crate::{
    Serialize,
    common::{
        config::{AtLeastOneHundredth, ConfRange, Probability},
        strings,
        strings::{random_strings_with_length, random_strings_with_length_range},
    },
};

use self::{
    common::tags, event::EventGenerator, metric::MetricGenerator,
    service_check::ServiceCheckGenerator,
};

use super::Generator;

mod common;
pub mod event;
pub mod metric;
pub mod service_check;

const MAX_CONTEXTS: u32 = 10_000_000;
const MAX_NAME_LENGTH: u16 = 4_096;
const LENGTH_PREFIX_SIZE: usize = std::mem::size_of::<u32>();

/// Weights for `DogStatsD` kinds: metrics, events, service checks
///
/// Defines the relative probability of each kind of `DogStatsD` datagram.
#[derive(Debug, Deserialize, serde::Serialize, Clone, Copy, PartialEq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct KindWeights {
    metric: u8,
    event: u8,
    service_check: u8,
}

impl KindWeights {
    /// Create a new instance of `KindWeights` according to the args
    #[must_use]
    pub fn new(metric: u8, event: u8, service_check: u8) -> Self {
        Self {
            metric,
            event,
            service_check,
        }
    }
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
#[derive(Debug, Deserialize, serde::Serialize, Clone, Copy, PartialEq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct MetricWeights {
    count: u8,
    gauge: u8,
    timer: u8,
    distribution: u8,
    set: u8,
    histogram: u8,
}

impl MetricWeights {
    /// Create a new instance of `MetricWeights` according to the args
    #[must_use]
    pub fn new(count: u8, gauge: u8, timer: u8, distribution: u8, set: u8, histogram: u8) -> Self {
        Self {
            count,
            gauge,
            timer,
            distribution,
            set,
            histogram,
        }
    }
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

/// Configuration for the values of a metric.
#[derive(Debug, Deserialize, serde::Serialize, Clone, PartialEq, Copy)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct ValueConf {
    /// Probability that the value will be a float and not an integer.
    float_probability: Probability,
    range: ConfRange<i64>,
}

impl ValueConf {
    /// Create a new instance of `ValueConf` according to the args
    #[must_use]
    pub fn new(float_probability: Probability, range: ConfRange<i64>) -> Self {
        Self {
            float_probability,
            range,
        }
    }

    fn valid(&self) -> (bool, &'static str) {
        self.range.valid()
    }
}

impl Default for ValueConf {
    fn default() -> Self {
        Self {
            float_probability: Probability::try_new(0.5).expect("0.5 is in [0.0, 1.0]"),
            range: ConfRange::Inclusive {
                min: i64::MIN,
                max: i64::MAX,
            },
        }
    }
}

/// Configure the `DogStatsD` payload.
#[derive(Debug, Deserialize, serde::Serialize, Clone, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// The unique metric contexts to generate. A context is a set of unique
    /// metric name + tags
    pub contexts: ConfRange<u32>,
    /// The range of unique service check names.
    pub service_check_names: ConfRange<u16>,
    /// Length for a dogstatsd message name
    pub name_length: ConfRange<u16>,
    /// Length for a dogstatsd tag
    pub tag_length: ConfRange<u16>,
    /// Number of tags per individual dogstatsd msg a tag is a key-value pair
    /// separated by a :
    pub tags_per_msg: ConfRange<u8>,
    /// Probability that a given dogstatsd msg contains multiple values
    pub multivalue_pack_probability: Probability,
    /// The count of values that will be generated if multi-value is chosen to
    /// be generated
    pub multivalue_count: ConfRange<u16>,
    /// Range of possible values for the sampling rate sent in dogstatsd messages
    pub sampling_range: ConfRange<f32>,
    /// Probability that a given dogstatsd msg will specify a sampling rate.
    /// The sampling rate is chosen from `sampling_range`
    pub sampling_probability: Probability,
    /// Defines the relative probability of each kind of `DogStatsD` kinds of
    /// payload.
    pub kind_weights: KindWeights,
    /// Defines the relative probability of each kind of `DogStatsD` metric.
    pub metric_weights: MetricWeights,
    /// The configuration of values that appear in all metrics.
    pub value: ValueConf,
    /// Whether completed blocks should use length-prefix framing.
    ///
    /// If enabled, each block emitted from this generator will have
    /// a 4-byte header that is a little-endian u32 representing the
    /// total length of the data block.
    pub length_prefix_framed: bool,
    /// This is a ratio that determines how many individual tags are unique vs
    /// re-used tags. If this is 1, then every single tag will be unique. If
    /// this is 0.10, then most of the tags (90%) will be re-used from existing
    /// tags. The type enforces the closed range `[0.01, 1.0]` at deserialize
    /// time.
    pub unique_tag_ratio: AtLeastOneHundredth,
    /// A list of possible metric names to generate
    pub metric_names: Vec<String>,
    /// A list of possible tag names to generate
    pub tag_names: Vec<String>,
    /// A list of possible tag values to generate
    pub tag_values: Vec<String>,
    /// Pool of possible container ID values for the `|c:` origin detection extension field.
    ///
    /// When non-empty, each generated metric randomly selects from this pool (or omits the field).
    /// When empty, a random value is used, which matches the behavior prior to this field existing.
    pub container_ids: Vec<String>,
    /// Pool of possible External Data strings for the `|e:` origin detection extension field.
    ///
    /// When non-empty, each generated metric randomly selects from this pool (or omits the field).
    /// When empty, the field is never emitted.
    pub external_data: Vec<String>,
    /// Pool of possible cardinality values for the `|card:` origin detection extension field.
    ///
    /// When non-empty, each generated metric randomly selects from this pool (or omits the field).
    /// When empty, the field is never emitted.
    pub cardinality: Vec<String>,
    /// Configuration for the optional `|T` `DogStatsD` protocol v1.3 timestamp field.
    pub timestamp: Box<TimestampConfig>,
}

/// Configuration for the optional `|T` `DogStatsD` protocol v1.3 timestamp field.
#[derive(Debug, Deserialize, serde::Serialize, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields, default)]
pub struct TimestampConfig {
    /// Range of possible Unix timestamps for the `|T` `DogStatsD` protocol v1.3 field.
    ///
    /// The `DogStatsD` protocol only supports this field for count and gauge metrics.
    pub range: ConfRange<u32>,
    /// Probability that a generated count or gauge metric includes `|T`.
    pub probability: Probability,
}

impl Default for TimestampConfig {
    fn default() -> Self {
        Self {
            range: ConfRange::Constant(1),
            probability: Probability::try_new(0.0).expect("0.0 is in [0.0, 1.0]"),
        }
    }
}

impl TimestampConfig {
    fn valid(&self) -> Result<(), String> {
        let (range_valid, reason) = self.range.valid();
        if !range_valid {
            return Result::Err(format!("Timestamp range is invalid: {reason}"));
        }
        if self.probability.get() > 0.0 && self.range.start() == 0 {
            return Result::Err(
                "Timestamp range start value cannot be 0 when timestamps are enabled".to_string(),
            );
        }

        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            contexts: ConfRange::Inclusive {
                min: 5_000,
                max: 10_000,
            },
            service_check_names: ConfRange::Inclusive {
                min: 1,
                max: 10_000,
            },
            // https://docs.datadoghq.com/developers/guide/what-best-practices-are-recommended-for-naming-metrics-and-tags/#rules-and-best-practices-for-naming-metrics
            name_length: ConfRange::Inclusive { min: 1, max: 200 },
            tag_length: ConfRange::Inclusive { min: 3, max: 100 },
            tags_per_msg: ConfRange::Inclusive { min: 2, max: 50 },
            multivalue_count: ConfRange::Inclusive { min: 2, max: 32 },
            multivalue_pack_probability: Probability::try_new(0.08).expect("0.08 is in [0.0, 1.0]"),
            sampling_range: ConfRange::Inclusive { min: 0.1, max: 1.0 },
            sampling_probability: Probability::try_new(0.5).expect("0.5 is in [0.0, 1.0]"),
            kind_weights: KindWeights::default(),
            metric_weights: MetricWeights::default(),
            value: ValueConf::default(),
            // This should be enabled for UDS-streams, but not for UDS-datagram nor UDP
            length_prefix_framed: false,
            unique_tag_ratio: AtLeastOneHundredth::try_new(0.11)
                .expect("0.11 is in [0.01, 1.0]"),
            metric_names: Vec::default(),
            tag_names: Vec::default(),
            tag_values: Vec::default(),
            container_ids: Vec::default(),
            external_data: Vec::default(),
            cardinality: Vec::default(),
            timestamp: Box::default(),
        }
    }
}

impl Config {
    /// Determine whether the passed configuration obeys validation criteria
    /// # Errors
    /// Function will error if the configuration is invalid
    pub fn valid(&self) -> Result<(), String> {
        let (contexts_valid, reason) = self.contexts.valid();
        if !contexts_valid {
            return Result::Err(format!("Contexts value is invalid: {reason}"));
        }
        if self.contexts.start() == 0 {
            return Result::Err("Contexts start value cannot be 0".to_string());
        }
        if let ConfRange::Constant(0) = self.contexts {
            return Result::Err("Contexts cannot be 0".to_string());
        }
        if self.contexts.end() > MAX_CONTEXTS {
            return Result::Err(format!(
                "Contexts end value is greater than the maximum allowed value of {MAX_CONTEXTS}"
            ));
        }
        let (service_check_names_valid, reason) = self.service_check_names.valid();
        if !service_check_names_valid {
            return Result::Err(format!("Service check names value is invalid: {reason}"));
        }
        if self.service_check_names.start() == 0 {
            return Result::Err("Service check names start value cannot be 0".to_string());
        }

        let (tag_length_valid, reason) = self.tag_length.valid();
        if !tag_length_valid {
            return Result::Err(format!("Tag length value is invalid: {reason}"));
        }
        if self.tag_length.start() == 0 {
            return Result::Err("Tag length start value cannot be 0".to_string());
        }
        let (name_length_valid, reason) = self.name_length.valid();
        if !name_length_valid {
            return Result::Err(format!("Name length value is invalid: {reason}"));
        }
        if self.name_length.start() == 0 {
            return Result::Err("Name length start value cannot be 0".to_string());
        }
        if self.name_length.end() > MAX_NAME_LENGTH {
            return Result::Err("Name length end value cannot be 0".to_string());
        }
        let (tags_per_msg_valid, reason) = self.tags_per_msg.valid();
        if !tags_per_msg_valid {
            return Result::Err(format!("Tags per msg value is invalid: {reason}"));
        }
        let (multivalue_count_valid, reason) = self.multivalue_count.valid();
        if !multivalue_count_valid {
            return Result::Err(format!("Multivalue count value is invalid: {reason}"));
        }
        let (value_valid, reason) = self.value.valid();
        if !value_valid {
            return Result::Err(format!("Value configuration is invalid: {reason}"));
        }

        let (sampling_range_valid, reason) = self.sampling_range.valid();
        if !sampling_range_valid {
            return Result::Err(format!("Sampling range is invalid: {reason}"));
        }
        match self.sampling_range {
            ConfRange::Constant(v) => {
                if !v.is_finite() || !(0.0..=1.0).contains(&v) {
                    return Result::Err(format!(
                        "Sampling range constant {v} must be finite and in range [0.0, 1.0]"
                    ));
                }
            }
            ConfRange::Inclusive { min, max } => {
                if !min.is_finite() || !max.is_finite() {
                    return Result::Err("Sampling range values must be finite".to_string());
                }
                if min < 0.0 || max > 1.0 {
                    return Result::Err("Sampling range must be within [0.0, 1.0]".to_string());
                }
            }
        }

        self.timestamp.valid()?;

        if self.kind_weights.metric == 0
            && self.kind_weights.event == 0
            && self.kind_weights.service_check == 0
        {
            return Err("KindWeights cannot all be 0".to_string());
        }

        if self.metric_weights.gauge == 0
            && self.metric_weights.count == 0
            && self.metric_weights.histogram == 0
            && self.metric_weights.set == 0
            && self.metric_weights.distribution == 0
        {
            return Err("MetricWeights cannot all be 0".to_string());
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
struct MemberGenerator {
    kind_weights: WeightedIndex<u16>,
    event_generator: EventGenerator,
    service_check_generator: ServiceCheckGenerator,
    metric_generator: MetricGenerator,
}

/// The collection of pools from which to choose our strings
/// Cheap to clone.
#[derive(Clone, Debug)]
pub(crate) struct StringPools {
    tag_name: Rc<strings::PoolKind>,
    tag_value: Rc<strings::PoolKind>,
    name: Rc<strings::PoolKind>,
    randomstring: Rc<strings::RandomStringPool>,
}

impl MemberGenerator {
    #[expect(clippy::too_many_arguments, clippy::too_many_lines)]
    fn new<R>(
        contexts: ConfRange<u32>,
        service_check_names: ConfRange<u16>,
        name_length: ConfRange<u16>,
        tag_length: ConfRange<u16>,
        tags_per_msg: ConfRange<u8>,
        multivalue_count: ConfRange<u16>,
        multivalue_pack_probability: Probability,
        sampling: ConfRange<f32>,
        sampling_probability: Probability,
        kind_weights: KindWeights,
        metric_weights: MetricWeights,
        value_conf: ValueConf,
        unique_tag_ratio: AtLeastOneHundredth,
        metric_names: &[String],
        tag_names: &[String],
        tag_values: &[String],
        container_ids: Vec<String>,
        external_data: Vec<String>,
        cardinality: Vec<String>,
        timestamp_range: ConfRange<u32>,
        timestamp_probability: Probability,
        mut rng: &mut R,
    ) -> Result<Self, crate::Error>
    where
        R: Rng + ?Sized,
    {
        // TODO only create the Generators if they're needed per the kind weights
        let pool = strings::RandomStringPool::with_size(&mut rng, 8_000_000);

        // BUG: Resulting size is contexts * name_length, so 2**32 * 2**16.
        let service_event_titles = random_strings_with_length_range(
            &pool,
            service_check_names.sample(rng) as usize,
            name_length,
            &mut rng,
        );

        let texts_or_messages = random_strings_with_length(&pool, 4..128, 1024, &mut rng);
        let small_strings = random_strings_with_length(&pool, 16..1024, 8, &mut rng);
        // Use caller-provided container IDs when available; fall back to random small strings.
        let container_ids = if container_ids.is_empty() {
            small_strings.clone()
        } else {
            container_ids
        };

        let str_pool = Rc::new(pool.clone());
        let pool = Rc::new(strings::PoolKind::RandomStringPool(pool));

        let tag_name_pool = if tag_names.is_empty() {
            Rc::clone(&pool)
        } else {
            Rc::new(strings::PoolKind::StringListPool(
                strings::StringListPool::new(tag_names, 15_000)?,
            ))
        };

        let tag_value_pool = if tag_values.is_empty() {
            Rc::clone(&pool)
        } else {
            Rc::new(strings::PoolKind::StringListPool(
                strings::StringListPool::new(tag_values, 15_000)?,
            ))
        };

        let name_pool = if metric_names.is_empty() {
            Rc::clone(&pool)
        } else {
            Rc::new(strings::PoolKind::StringListPool(
                strings::StringListPool::new(metric_names, 15_000)?,
            ))
        };

        let pools = StringPools {
            tag_name: tag_name_pool,
            tag_value: tag_value_pool,
            name: name_pool,
            randomstring: str_pool,
        };

        let num_contexts = contexts.sample(rng);

        let mut tags_generator = match tags::Generator::new(
            rng.random(),
            tags_per_msg,
            tag_length,
            num_contexts as usize,
            Rc::clone(&pools.tag_value),
            Rc::clone(&pools.tag_name),
            unique_tag_ratio,
        ) {
            Ok(tg) => tg,
            Err(e) => {
                warn!("Encountered error while constructing tag generator: {e}");
                return Err(crate::Error::StringGenerate);
            }
        };

        // NOTE the ordering here of `metric_choices` is very important! If you
        // change it here you MUST also change it in `Generator<Metric> for
        // MetricGenerator`.
        let metric_choices = [
            u16::from(metric_weights.count),
            u16::from(metric_weights.gauge),
            u16::from(metric_weights.timer),
            u16::from(metric_weights.distribution),
            u16::from(metric_weights.set),
            u16::from(metric_weights.histogram),
        ];

        let event_generator = EventGenerator {
            pools: pools.clone(),
            title_length: name_length,
            texts_or_messages_length_range: 1..1024,
            small_strings_length_range: 1..8,
            tags_generator: tags_generator.clone(),
        };

        let service_check_generator = ServiceCheckGenerator {
            names: service_event_titles.clone(),
            small_strings: small_strings.clone(),
            texts_or_messages,
            tags_generator: tags_generator.clone(),
            pools: pools.clone(),
        };

        let metric_generator = MetricGenerator::new(
            num_contexts as usize,
            name_length,
            multivalue_count,
            multivalue_pack_probability,
            sampling,
            sampling_probability,
            &WeightedIndex::new(metric_choices)?,
            container_ids,
            external_data,
            cardinality,
            timestamp_range,
            timestamp_probability,
            &mut tags_generator,
            &pools,
            value_conf,
            &mut rng,
        );

        // NOTE the ordering here of `member_choices` is very important! If you
        // change it here you MUST also change it in `Generator<Member> for
        // MemberGenerator`.
        let member_choices = [
            u16::from(kind_weights.metric),
            u16::from(kind_weights.event),
            u16::from(kind_weights.service_check),
        ];
        Ok(MemberGenerator {
            kind_weights: WeightedIndex::new(member_choices)?,
            event_generator,
            service_check_generator,
            metric_generator: metric_generator?,
        })
    }
}

impl MemberGenerator {
    fn generate<'a, R>(&'a self, rng: &mut R) -> Result<Member<'a>, crate::Error>
    where
        R: rand::Rng + ?Sized,
    {
        match self.kind_weights.sample(rng) {
            0 => Ok(Member::Metric(self.metric_generator.generate(rng)?)),
            1 => {
                let event = self.event_generator.generate(rng)?;
                Ok(Member::Event(event))
            }
            2 => Ok(Member::ServiceCheck(
                self.service_check_generator.generate(rng)?,
            )),
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

impl fmt::Display for Member<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Metric(m) => write!(f, "{m}"),
            Self::Event(e) => write!(f, "{e}"),
            Self::ServiceCheck(sc) => write!(f, "{sc}"),
        }
    }
}

#[derive(Debug, Clone)]
#[allow(clippy::module_name_repetitions)]
/// A generator for `DogStatsD` payloads
pub struct DogStatsD {
    member_generator: MemberGenerator,
    length_prefix_framed: bool,
}

impl DogStatsD {
    /// Create a new default instance of `DogStatsD` with reasonable settings.
    ///
    /// # Errors
    /// Function will error if dogstatd could not be created
    pub fn default<R>(rng: &mut R) -> Result<Self, crate::Error>
    where
        R: rand::Rng + ?Sized,
    {
        let config = Config::default();
        let dogstatd = Self::new(&config, rng)?;
        Ok(dogstatd)
    }

    /// Generate a single `Member`.
    /// Prefer using the Serialize implementation for `DogStatsD` which
    /// generates a stream of `Member`s according to some constraints.
    ///
    /// # Errors
    ///
    /// Function will error if a member could not be generated
    pub fn generate<R>(&self, rng: &mut R) -> Result<Member<'_>, crate::Error>
    where
        R: rand::Rng + ?Sized,
    {
        self.member_generator.generate(rng)
    }

    /// Create a new instance of `DogStatsD`.
    ///
    /// # Errors
    ///
    /// See documentation for [`Error`]
    #[allow(clippy::too_many_arguments)]
    pub fn new<R>(config: &Config, rng: &mut R) -> Result<Self, crate::Error>
    where
        R: rand::Rng + ?Sized,
    {
        let member_generator = MemberGenerator::new(
            config.contexts,
            config.service_check_names,
            config.name_length,
            config.tag_length,
            config.tags_per_msg,
            config.multivalue_count,
            config.multivalue_pack_probability,
            config.sampling_range,
            config.sampling_probability,
            config.kind_weights,
            config.metric_weights,
            config.value,
            config.unique_tag_ratio,
            &config.metric_names,
            &config.tag_names,
            &config.tag_values,
            config.container_ids.clone(),
            config.external_data.clone(),
            config.cardinality.clone(),
            config.timestamp.range,
            config.timestamp.probability,
            rng,
        )?;

        Ok(Self {
            member_generator,
            length_prefix_framed: config.length_prefix_framed,
        })
    }
}

impl Serialize for DogStatsD {
    fn to_bytes<W, R>(
        &mut self,
        rng: R,
        max_bytes: usize,
        writer: &mut W,
    ) -> Result<(), crate::Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        if self.length_prefix_framed {
            self.to_bytes_length_prefix_framed(rng, max_bytes, writer)
        } else {
            self.to_bytes_unframed(rng, max_bytes, writer)
        }
    }
}
impl DogStatsD {
    fn to_bytes_length_prefix_framed<W, R>(
        &self,
        mut rng: R,
        max_bytes: usize,
        writer: &mut W,
    ) -> Result<(), crate::Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        // If max_bytes is less than the length prefix size, we can't write anything
        if max_bytes < LENGTH_PREFIX_SIZE {
            return Ok(());
        }

        let content_budget = max_bytes.saturating_sub(LENGTH_PREFIX_SIZE);
        // Accumulate all content into a single buffer instead of Vec<String>.
        // This eliminates per-member String allocations.
        let mut content_buffer: Vec<u8> = Vec::with_capacity(content_budget);
        // Temporary buffer for formatting each member before measuring/appending
        let mut member_buffer: Vec<u8> = Vec::with_capacity(256);

        loop {
            let member: Member = self.member_generator.generate(&mut rng)?;
            member_buffer.clear();
            // Format into the temporary buffer - write! on Vec<u8> is infallible
            write!(&mut member_buffer, "{member}").expect("formatting to Vec<u8> cannot fail");
            let line_length = member_buffer.len() + 1; // add one for the newline
            if content_buffer.len() + line_length > content_budget {
                break;
            }
            content_buffer.extend_from_slice(&member_buffer);
            content_buffer.push(b'\n');
        }

        if content_buffer.is_empty() {
            return Ok(());
        }

        let length: u32 = content_buffer.len().try_into().unwrap_or_else(|_| {
            panic!(
                "Could not convert content length to a u32, are you using a block size greater than {}?",
                u32::MAX
            );
        });

        // write prefix
        writer.write_all(&length.to_le_bytes())?;
        // LENGTH_PREFIX_SIZE is always 4 (size_of::<u32>()), safe to cast
        #[allow(clippy::cast_possible_truncation)]
        let total_bytes = length + LENGTH_PREFIX_SIZE as u32;
        debug!("Filling block. Requested: {max_bytes} bytes. Actual: {total_bytes} bytes.",);

        // write contents
        writer.write_all(&content_buffer)?;

        Ok(())
    }

    fn to_bytes_unframed<W, R>(
        &self,
        mut rng: R,
        max_bytes: usize,
        writer: &mut W,
    ) -> Result<(), crate::Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        let mut bytes_remaining = max_bytes;
        // Reuse a single buffer across iterations to avoid repeated allocations.
        // Each member is formatted here, measured, then written to the output.
        let mut buffer: Vec<u8> = Vec::with_capacity(256);
        loop {
            let member: Member = self.member_generator.generate(&mut rng)?;
            buffer.clear();
            // Format into the reusable buffer - write! on Vec<u8> is infallible
            write!(&mut buffer, "{member}").expect("formatting to Vec<u8> cannot fail");
            let line_length = buffer.len() + 1; // add one for the newline
            match bytes_remaining.checked_sub(line_length) {
                Some(remainder) => {
                    writer.write_all(&buffer)?;
                    writer.write_all(b"\n")?;
                    bytes_remaining = remainder;
                }
                None => break,
            }
        }
        if bytes_remaining == max_bytes {
            return Ok(());
        }

        let length = max_bytes - bytes_remaining;
        debug!(
            "Filling block. Requested: {max_bytes} bytes. Actual: {} bytes.",
            length
        );
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{ConfRange, Config, KindWeights, MetricWeights, Probability, TimestampConfig};
    use proptest::prelude::*;
    use rand::{SeedableRng, rngs::SmallRng};

    use crate::{DogStatsD, Serialize};

    fn prob(value: f32) -> Probability {
        Probability::try_new(value).expect("value must be in [0.0, 1.0]")
    }

    // Generate a batch of raw DogStatsD bytes using the given config and seed.
    fn generate_bytes(config: &Config, seed: u64) -> Vec<u8> {
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut dogstatsd = DogStatsD::new(config, &mut rng).expect("failed to create DogStatsD");
        let mut bytes = Vec::new();
        dogstatsd
            .to_bytes(rng, 65536, &mut bytes)
            .expect("failed to generate bytes");
        bytes
    }

    // With a non-empty pool, the extension field must appear in at least one metric across a
    // set of differently-seeded runs. Each metric has a 50% chance of including the field, so
    // the probability of never seeing it across N independent runs is 0.5^N (~10^-30 for N=100).
    fn assert_field_emitted_at_least_once(config: &Config, field_prefix: &[u8]) {
        let seen = (0..100).any(|seed| {
            let bytes = generate_bytes(config, seed);
            bytes.windows(field_prefix.len()).any(|w| w == field_prefix)
        });
        assert!(
            seen,
            "expected '{}' to appear in generated output with a non-empty pool, but it never did",
            std::str::from_utf8(field_prefix).unwrap_or("<non-utf8>")
        );
    }

    #[test]
    fn external_data_emitted_when_pool_nonempty() {
        let config = Config {
            external_data: vec!["pu-abc123,cn-mycontainer,it-false".to_string()],
            ..Default::default()
        };
        assert_field_emitted_at_least_once(&config, b"|e:");
    }

    #[test]
    fn external_data_absent_when_pool_empty() {
        let bytes = generate_bytes(&Config::default(), 0);
        assert!(
            !bytes.windows(3).any(|w| w == b"|e:"),
            "expected no |e: field when external_data pool is empty"
        );
    }

    #[test]
    fn cardinality_emitted_when_pool_nonempty() {
        let config = Config {
            cardinality: vec!["low".to_string(), "orchestrator".to_string()],
            ..Default::default()
        };
        assert_field_emitted_at_least_once(&config, b"|card:");
    }

    #[test]
    fn cardinality_absent_when_pool_empty() {
        let bytes = generate_bytes(&Config::default(), 0);
        assert!(
            !bytes.windows(6).any(|w| w == b"|card:"),
            "expected no |card: field when cardinality pool is empty"
        );
    }

    #[test]
    fn container_id_uses_configured_pool() {
        // No absence test for container_ids: an empty pool falls back to random strings rather
        // than suppressing the field, so there is no "absent when empty" condition to assert.
        let id = "abc123def456";
        let config = Config {
            container_ids: vec![id.to_string()],
            ..Default::default()
        };
        let needle = format!("|c:{id}");
        assert_field_emitted_at_least_once(&config, needle.as_bytes());
    }

    #[test]
    fn cardinality_values_come_from_pool() {
        // When a pool is non-empty, only values from that pool should appear after the prefix.
        let config = Config {
            cardinality: vec!["low".to_string()],
            ..Default::default()
        };
        for seed in 0..10 {
            let bytes = generate_bytes(&config, seed);
            let text = std::str::from_utf8(&bytes).expect("output should be valid utf-8");
            for segment in text.split("|card:").skip(1) {
                // Each metric is newline-terminated, so split on both | and \n to isolate the value.
                let value = segment.split(['|', '\n']).next().unwrap_or("");
                assert_eq!(
                    value, "low",
                    "|card: value {value:?} was not from the configured pool"
                );
            }
        }
    }

    #[test]
    fn timestamp_emitted_when_probability_is_one() {
        let config = Config {
            kind_weights: KindWeights::new(100, 0, 0),
            metric_weights: MetricWeights::new(100, 100, 0, 0, 0, 0),
            timestamp: Box::new(TimestampConfig {
                range: ConfRange::Constant(1_656_581_400),
                probability: prob(1.0),
            }),
            ..Default::default()
        };
        assert_field_emitted_at_least_once(&config, b"|T1656581400");
    }

    #[test]
    fn timestamp_absent_when_probability_is_zero() {
        let config = Config {
            kind_weights: KindWeights::new(100, 0, 0),
            metric_weights: MetricWeights::new(100, 100, 0, 0, 0, 0),
            timestamp: Box::new(TimestampConfig {
                range: ConfRange::Constant(1_656_581_400),
                probability: prob(0.0),
            }),
            ..Default::default()
        };
        let bytes = generate_bytes(&config, 0);
        assert!(
            !bytes.windows(2).any(|w| w == b"|T"),
            "expected no |T field when timestamp_probability is 0"
        );
    }

    #[test]
    fn timestamp_values_come_from_range() {
        let config = Config {
            kind_weights: KindWeights::new(100, 0, 0),
            metric_weights: MetricWeights::new(100, 100, 0, 0, 0, 0),
            timestamp: Box::new(TimestampConfig {
                range: ConfRange::Inclusive { min: 10, max: 20 },
                probability: prob(1.0),
            }),
            ..Default::default()
        };
        let bytes = generate_bytes(&config, 0);
        let text = std::str::from_utf8(&bytes).expect("output should be valid utf-8");
        for segment in text.split("|T").skip(1) {
            let value = segment
                .split(['|', '\n'])
                .next()
                .unwrap_or("")
                .parse::<u32>()
                .expect("timestamp should parse as u64");
            assert!(
                (10..=20).contains(&value),
                "|T value {value} was not inside the configured timestamp range"
            );
        }
    }

    #[test]
    fn timestamp_only_emitted_for_count_and_gauge() {
        let config = Config {
            kind_weights: KindWeights::new(100, 0, 0),
            metric_weights: MetricWeights::new(0, 0, 100, 0, 0, 0),
            timestamp: Box::new(TimestampConfig {
                range: ConfRange::Constant(1_656_581_400),
                probability: prob(1.0),
            }),
            ..Default::default()
        };
        let bytes = generate_bytes(&config, 0);
        assert!(
            !bytes.windows(2).any(|w| w == b"|T"),
            "expected no |T field for metric types outside count and gauge"
        );
    }

    #[test]
    fn timestamp_probability_rejects_out_of_range_on_deserialize() {
        let yaml = "range: !constant 1\nprobability: 2.0\n";
        let err = serde_yaml::from_str::<TimestampConfig>(yaml)
            .expect_err("probability 2.0 must be rejected at deserialize time");
        assert!(
            err.to_string().contains("exceeds 1.0"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn timestamp_range_must_be_positive_when_enabled() {
        let config = Config {
            timestamp: Box::new(TimestampConfig {
                range: ConfRange::Constant(0),
                probability: prob(1.0),
            }),
            ..Default::default()
        };
        assert!(config.valid().is_err());
    }

    #[test]
    fn zero_contexts_should_be_rejected() {
        let config = Config {
            contexts: ConfRange::Constant(0),
            ..Default::default()
        };

        // Validation should reject 0 contexts
        let validation_result = config.valid();
        assert!(validation_result.is_err());
        // The error message could be either one, both indicate 0 contexts is invalid
        let err = validation_result.expect_err("expected validation to fail");
        assert!(
            err == "Contexts start value cannot be 0" || err == "Contexts cannot be 0",
            "Expected error about 0 contexts, got: {err}"
        );
    }

    // For all seeds, generation with the same timestamp configuration and same seed
    // produces byte-identical output.
    proptest! {
        #[test]
        fn timestamp_generation_is_deterministic(seed: u64) {
            let config = Config {
                kind_weights: KindWeights::new(100, 0, 0),
                metric_weights: MetricWeights::new(100, 100, 0, 0, 0, 0),
                timestamp: Box::new(TimestampConfig {
                    range: ConfRange::Inclusive { min: 10, max: 20 },
                    probability: prob(0.5),
                }),
                ..Default::default()
            };
            prop_assert_eq!(generate_bytes(&config, seed), generate_bytes(&config, seed));
        }
    }

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);

            let dogstatsd_config = Config::default();
            let mut dogstatsd = match DogStatsD::new(&dogstatsd_config, &mut rng) {
                Ok(d) => d,
                Err(e) => {
                    eprintln!("Failed to create DogStatsD with error: {e:?}");
                    return Err(TestCaseError::fail(format!("Failed to create DogStatsD: {e:?}")));
                }
            };

            let mut bytes = Vec::with_capacity(max_bytes);
            dogstatsd.to_bytes(rng, max_bytes, &mut bytes)?;
            prop_assert!(
                bytes.len() <= max_bytes,
                "{:?}",
                std::str::from_utf8(&bytes).expect("failed to convert from utf-8 to str")
            );
        }
    }

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes` if framing is enabled
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes_with_length_prefix_frames(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);

            let dogstatsd_config = Config { length_prefix_framed: true, ..Default::default() };
            let mut dogstatsd = DogStatsD::new(&dogstatsd_config, &mut rng).expect("failed to create DogStatsD");

            let mut bytes = Vec::with_capacity(max_bytes);
            dogstatsd.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");
            prop_assert!(
                bytes.len() <= max_bytes,
                "{l} <= {max_bytes}, {pyld:?}",
                l = bytes.len(),
                pyld = std::str::from_utf8(&bytes).expect("failed to convert from utf-8 to str")
            );
        }
    }
}
