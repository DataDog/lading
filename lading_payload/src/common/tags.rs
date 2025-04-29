//! Tag generation for dogstatsd payloads
use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::rc::Rc;

use rand::Rng;
use rand::{SeedableRng, rngs::SmallRng};
use rand::{
    distr::{Distribution, OpenClosed01},
    seq::IndexedRandom,
};
use tracing::warn;

use crate::common::{
    config::ConfRange,
    strings::{Handle, Pool},
};

pub(crate) const MIN_UNIQUE_TAG_RATIO: f32 = 0.01;
pub(crate) const MAX_UNIQUE_TAG_RATIO: f32 = 1.00;
pub(crate) const WARN_UNIQUE_TAG_RATIO: f32 = 0.10;
pub(crate) const MIN_TAG_LENGTH: u16 = 3;

/// List of tags that will be present on a dogstatsd message
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct Tag {
    pub(crate) key: Handle,
    pub(crate) value: Handle,
}
pub(crate) type Tagset = Vec<Tag>;

/// Generator for individual tags
#[derive(Debug, Clone)]
pub(crate) struct TagGenerator {
    str_pool: Rc<Pool>,
    tag_length: ConfRange<u16>,
}

impl TagGenerator {
    pub(crate) fn new(str_pool: Rc<Pool>, tag_length: ConfRange<u16>) -> Self {
        Self {
            str_pool,
            tag_length,
        }
    }

    pub(crate) fn generate<R>(&self, rng: &mut R) -> Result<Tag, crate::Error>
    where
        R: rand::Rng + ?Sized,
    {
        let desired_size = self.tag_length.sample(rng) as usize;
        // Ensure we have at least 1 character for both key and value
        let max_key_size = desired_size - 1;
        let min_key_size = 1;
        let key_size = if max_key_size <= min_key_size {
            min_key_size
        } else {
            rng.random_range(min_key_size..=max_key_size)
        };
        let value_size = desired_size - key_size;

        let key_handle = self.str_pool.of_size_with_handle(rng, key_size);
        let value_handle = self.str_pool.of_size_with_handle(rng, value_size);

        match (key_handle, value_handle) {
            (Some((_, key_handle)), Some((_, value_handle))) => Ok(Tag {
                key: key_handle,
                value: value_handle,
            }),
            _ => Err(crate::Error::StringGenerate),
        }
    }
}

/// Generator for tags
///
/// This is an unusual generator. Unlike our others this maintains its own RNG
/// and will reseed it when counter reaches `num_tagsets`. The goal is to
/// produce only a limited, deterministic set of tags while avoiding needing to
/// allocate them all in one shot.
///
/// The `unique_tag_ratio` is a value between 0.10 and 1.0. It represents the
/// ratio of new tags to existing tags. If the value is 1.0, then all tags will
/// be new. If the value 0.0 were allowed, it would conceptually mean "always
/// use an existing tag", however this is a degenerate case as there would never
/// be a new tag generated. Therefore a minimum value is enforced.  Despite this
/// minimum, if the configuration is overly restrictive, it may result in
/// non-unique tagsets.
///
/// As an example:
/// `unique_tag_probability`: 0.10
/// `tags_per_msg`: 3
/// `num_tagsets`: 1000
///
/// With 3 tags per message, and a 10% chance of generating a new tag, 1000
/// calls to generate will result in 3000 "get new tag" decisions.  300 of these
/// will result in new tags and 2700 of these will re-use an existing tag. With
/// only 300 chances for new entropy, each individual tagset will likely
/// over-sample the existing tags and therefore UNDER-generate the desired
/// number of unique tagsets.
#[derive(Debug, Clone)]
pub(crate) struct Generator {
    seed: Cell<u64>,
    internal_rng: RefCell<SmallRng>,
    tagsets_produced: Cell<usize>,
    num_tagsets: usize, // Maximum number of unique tagsets that will ever be generated
    tags_per_msg: ConfRange<u8>, // Maximum number of tags per individually generated tagset
    tags: TagGenerator,
    unique_tag_probability: f32,
    unique_tags: RefCell<HashSet<Tag>>, // The unique tags that have been generated
}

/// Error type for `TagGenerator`
#[derive(thiserror::Error, Debug)]
pub(crate) enum Error {
    /// Invalid construction
    #[error("Invalid construction: {0}")]
    InvalidConstruction(String),
}

impl Generator {
    /// Creates a new tag generator
    ///
    /// # Errors
    /// - If `tags_per_msg` is invalid or exceeds the maximum
    /// - If `tag_length` is invalid or has minimum value less than 3
    /// - If `unique_tag_probability` is not between 0.10 and 1.0
    pub(crate) fn new(
        seed: u64,
        tags_per_msg: ConfRange<u8>,
        tag_length: ConfRange<u16>,
        num_tagsets: usize,
        str_pool: Rc<Pool>,
        unique_tag_probability: f32,
    ) -> Result<Self, Error> {
        let (tag_length_valid, tag_length_valid_msg) = tag_length.valid();
        if !tag_length_valid {
            return Err(Error::InvalidConstruction(format!(
                "Invalid tag length: {tag_length_valid_msg}"
            )));
        }
        if tag_length.start() < MIN_TAG_LENGTH {
            return Err(Error::InvalidConstruction(format!(
                "Tag length must be at least {MIN_TAG_LENGTH}, found {start}",
                start = tag_length.start()
            )));
        }

        if !(MIN_UNIQUE_TAG_RATIO..=MAX_UNIQUE_TAG_RATIO).contains(&unique_tag_probability) {
            return Err(Error::InvalidConstruction(format!(
                "Unique tag ratio must be between {MIN_UNIQUE_TAG_RATIO} and {MAX_UNIQUE_TAG_RATIO}"
            )));
        }

        if (MIN_UNIQUE_TAG_RATIO..=WARN_UNIQUE_TAG_RATIO).contains(&unique_tag_probability) {
            warn!(
                "unique_tag_probability is less than {WARN_UNIQUE_TAG_RATIO}. This may result in non-unique tagsets"
            );
        }

        let rng = SmallRng::seed_from_u64(seed);
        let tags = TagGenerator::new(str_pool, tag_length);

        Ok(Generator {
            seed: Cell::new(seed),
            internal_rng: RefCell::new(rng),
            tags_per_msg,
            tags,
            tagsets_produced: Cell::new(0),
            num_tagsets,
            unique_tag_probability,
            unique_tags: RefCell::new(HashSet::new()),
        })
    }

    /// Given an opaque handle returned from `generate`, return the &str it
    /// represents. None if handle is not valid.
    #[must_use]
    #[inline]
    pub(crate) fn using_handle(&self, handle: Handle) -> Option<&str> {
        self.tags.str_pool.using_handle(handle)
    }
}

impl<'a> crate::Generator<'a> for Generator {
    type Output = Tagset;
    type Error = crate::Error;

    /// Return a tagset -- a list of tags
    ///
    /// This function concerns itself with two general concepts: tags and
    /// tagsets. A "tag" is a key-value pair that obeys some length parameters
    /// taken as a whole. A tagset is a collection of tags. We generate a
    /// globally unique number of tagsets -- corresponding to `num_tagsets` in
    /// the constructor -- and each tagset is guaranteed to have a bounded
    /// number of tags -- corresponding to `tags_per_msg` in the constructor.
    /// `unique_tag_probability` in the constructor controls how often a unique
    /// tag will be a part of any individual tagset.
    ///
    /// Note that after `num_tagsets` have been produced, the tagsets will loop
    /// and produce identical tagsets. Each tagset is randomly chosen. No more
    /// than `num_tagsets` unique tagsets will ever be generated.
    fn generate<R>(&'a self, _rng: &mut R) -> Result<Self::Output, Self::Error>
    where
        R: rand::Rng + ?Sized,
    {
        // If we have produced the number of tagsets we are supposed to produce,
        // then reseed the internal RNG this ensures that we generate the same
        // tags in a loop
        if self.tagsets_produced.get() >= self.num_tagsets {
            // Reseed internal RNG with initial seed
            self.internal_rng
                .replace(SmallRng::seed_from_u64(self.seed.get()));
            self.tagsets_produced.set(0);
        }

        let mut tagset: Tagset = Vec::new();
        let mut rng = self.internal_rng.borrow_mut();
        let tags_count = self.tags_per_msg.sample(&mut *rng) as usize;
        let mut unique_tags = self.unique_tags.borrow_mut();

        // If we have no unique tags yet, we must generate at least one
        if unique_tags.is_empty() {
            let tag = self.tags.generate(&mut *rng)?;
            unique_tags.insert(tag);
            tagset.push(tag);
        }

        // For remaining tags, decide whether to reuse existing tags or generate new ones
        while tagset.len() < tags_count {
            let choose_existing_prob: f32 = OpenClosed01.sample(&mut *rng);
            let should_reuse = choose_existing_prob > self.unique_tag_probability;

            if should_reuse && !unique_tags.is_empty() {
                // Reuse an existing tag
                if let Some(tag) = unique_tags.choose(&mut *rng) {
                    tagset.push(tag);
                }
            } else {
                // Generate a new unique tag
                let tag = self.tags.generate(&mut *rng)?;
                unique_tags.insert(tag);
                tagset.push(tag);
            }
        }

        self.tagsets_produced.set(self.tagsets_produced.get() + 1);
        Ok(tagset)
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::rc::Rc;

    use proptest::prelude::*;
    use rand::{SeedableRng, rngs::SmallRng};

    use super::{MAX_UNIQUE_TAG_RATIO, WARN_UNIQUE_TAG_RATIO};
    use crate::Generator;
    use crate::common::config::ConfRange;
    use crate::common::strings::Pool;

    proptest! {
        #[test]
        fn generator_yields_valid_tags(
            seed: u64,
            num_tagsets in 1..100_usize,
            tags_per_msg_max in 1..u8::MAX,
            tag_size_min in 3..64_u16,
            tag_size_max in 64..128_u16,
            pool_size in 1_024..10_000_usize
        ) {
            let mut rng = SmallRng::seed_from_u64(seed);

            let str_pool = Rc::new(Pool::with_size(&mut rng, pool_size));
            let tags_per_msg_range = ConfRange::Inclusive{min: 0, max: tags_per_msg_max};
            let tag_size_range = ConfRange::Inclusive{min: tag_size_min, max: tag_size_max};
            let generator = super::Generator::new(
                seed,
                tags_per_msg_range,
                tag_size_range,
                num_tagsets,
                str_pool,
                1.0
            ).expect("Tag generator to be valid");

            for _ in 0..num_tagsets {
                let tagset = generator.generate(&mut rng)?;
                for tag in &tagset {
                    let key = generator.using_handle(tag.key).expect("invalid handle");
                    let value = generator.using_handle(tag.value).expect("invalid handle");
                    let total_size = key.len() + value.len();
                    debug_assert!(total_size <= tag_size_range.end() as usize, "tag len: {}, tag_size_range end: {end}", total_size, end = tag_size_range.end());
                    debug_assert!(total_size >= tag_size_range.start() as usize, "tag len: {}, tag_size_range start: {start}", total_size, start = tag_size_range.start());
                }
                debug_assert!(tagset.len() <= tags_per_msg_range.end() as usize, "tagset len: {}, tags_per_msg_range end: {end}", tagset.len(), end = tags_per_msg_range.end());
                debug_assert!(tagset.len() >= tags_per_msg_range.start() as usize, "tagset len: {}, tags_per_msg_range start: {start}", tagset.len(), start = tags_per_msg_range.start());
            }
        }

        #[test]
        fn tagsets_repeat_after_reaching_tagset_max(
            seed: u64,
            num_tagsets in 1..100_usize,
            tags_per_msg_max in 1..u8::MAX,
            tag_size_min in 3..64_u16,
            tag_size_max in 64..128_u16,
            pool_size in 1_024..10_000_usize
        ) {
            let mut rng = SmallRng::seed_from_u64(seed);

            let str_pool = Rc::new(Pool::with_size(&mut rng, pool_size));
            let tags_per_msg_range = ConfRange::Inclusive { min: 0, max: tags_per_msg_max };
            let tag_size_range = ConfRange::Inclusive { min: tag_size_min, max: tag_size_max };
            let generator = super::Generator::new(
                seed,
                tags_per_msg_range,
                tag_size_range,
                num_tagsets,
                str_pool,
                1.0
            ).expect("Tag generator to be valid");

            let first_batch = (0..num_tagsets)
                .map(|_| generator.generate(&mut rng).expect("failed to generate tagset"))
                .collect::<Vec<_>>();

            let second_batch = (0..num_tagsets)
                .map(|_| generator.generate(&mut rng).expect("failed to generate tagset"))
                .collect::<Vec<_>>();

            debug_assert_eq!(first_batch.len(), second_batch.len(), "batch lengths differ: first={}, second={}", first_batch.len(), second_batch.len());
            for i in 0..first_batch.len() {
                let first = &first_batch[i];
                let second = &second_batch[i];
                debug_assert_eq!(first, second, "tagsets at index {i} differ");
            }
        }

        #[test]
        fn unique_tagsets_respected_always_unique_tags(
            seed: u64,
            desired_num_tagsets in 1..100_usize,
            tags_per_msg_max in 1..u8::MAX,
            tag_size_min in 3..64_u16,
            tag_size_max in 64..128_u16,
            pool_size in 1_024..10_000_usize
        ) {
            let tags_per_msg_range = ConfRange::Inclusive { min: 2, max: tags_per_msg_max };
            let tag_size_range = ConfRange::Inclusive { min: tag_size_min, max: tag_size_max };
            let mut rng = SmallRng::seed_from_u64(seed);

            let str_pool = Rc::new(Pool::with_size(&mut rng, pool_size));
            let generator = super::Generator::new(
                seed,
                tags_per_msg_range,
                tag_size_range,
                desired_num_tagsets,
                str_pool,
                1.0
            ).expect("Tag generator to be valid");

            let mut unique_tags = HashSet::new();
            for _ in 0..desired_num_tagsets {
                let tagset = generator.generate(&mut rng).expect("failed to generate tagset");
                for tag in tagset {
                    unique_tags.insert((tag.key, tag.value));
                }
            }
            assert!(unique_tags.len() >= desired_num_tagsets);
        }

        #[test]
        fn unique_tagsets_respected_with_varying_ratio(
            seed: u64,
            desired_num_tagsets in 5..100_usize,
            unique_tag_ratio in WARN_UNIQUE_TAG_RATIO..MAX_UNIQUE_TAG_RATIO,
            tags_per_msg_max in 1..u8::MAX,
            tag_size_min in 3..64_u16,
            tag_size_max in 64..128_u16,
            pool_size in 1_024..10_000_usize
        ) {
            let tags_per_msg_range = ConfRange::Inclusive { min: 2, max: tags_per_msg_max };
            let tag_size_range = ConfRange::Inclusive { min: tag_size_min, max: tag_size_max };
            let mut rng = SmallRng::seed_from_u64(seed);

            let str_pool = Rc::new(Pool::with_size(&mut rng, pool_size));
            let generator = super::Generator::new(
                seed,
                tags_per_msg_range,
                tag_size_range,
                desired_num_tagsets,
                str_pool,
                unique_tag_ratio
            ).expect("Tag generator to be valid");

            let mut unique_tags = HashSet::new();
            for _ in 0..desired_num_tagsets {
                let tagset = generator.generate(&mut rng).expect("failed to generate tagset");
                for tag in tagset {
                    unique_tags.insert((tag.key, tag.value));
                }
            }
            debug_assert_eq!(unique_tags.len(), desired_num_tagsets, "Expected exactly {desired_num_tagsets} unique tags, got {}", unique_tags.len());
        }
    }
}
