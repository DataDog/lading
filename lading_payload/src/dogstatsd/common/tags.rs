//! Tag generation for dogstatsd payloads
use std::cell::{Cell, RefCell};
use std::rc::Rc;

use rand::Rng;
use rand::distr::Distribution;
use rand::seq::IndexedRandom;
use rand::{SeedableRng, rngs::SmallRng};
use tracing::warn;

use crate::dogstatsd::common::OpenClosed01;
use crate::{
    common::strings::{Handle, Pool},
    dogstatsd::ConfRange,
};

const MIN_UNIQUE_TAG_RATIO: f32 = 0.01;
const MAX_UNIQUE_TAG_RATIO: f32 = 1.00;
const WARN_UNIQUE_TAG_RATIO: f32 = 0.10;
const MIN_TAG_LENGTH: u16 = 3;

/// List of tags that will be present on a dogstatsd message
pub(crate) type Tagset = Vec<String>;

/// Generator for tags
///
/// This is an unusual generator. Unlike our others this maintains its own RNG
/// and will reseed it when counter reaches `num_tagsets`. The goal is to
/// produce only a limited, deterministic set of tags while avoiding needing to
/// allocate them all in one shot.
///
/// The `unique_tag_ratio` is a value between 0.10 and 1.0. It represents the
/// ratio of new tags to existing tags. If the value is 1.0, then all tags will
/// be new. If the value 0.0 were allowed,
/// it would conceptually mean "always use an existing tag", however this is a
/// degenerate case as there would never be a new tag generated.
/// therefore a minimum value is enforced.
/// Despite this minimum, if the configuration is overly restrictive, it may
/// result in non-unique tagsets.
/// As an example:
/// `unique_tag_probability`: 0.10
/// `tags_per_msg`: 3
/// `num_tagsets`: 1000
///
/// With 3 tags per message, and a 10% chance of generating a new tag, 1000 calls
/// to generate will result in 3000 "get new tag" decisions.
/// 300 of these will result in new tags and 2700 of these will re-use an existing tag.
/// With only 300 chances for new entropy, each individual tagset will likely over-sample
/// the existing tags and therefore UNDER-generate the desired number of unique tagsets.
#[derive(Debug, Clone)]
pub(crate) struct Generator {
    seed: Cell<u64>,
    internal_rng: RefCell<SmallRng>,
    tagsets_produced: Cell<usize>,
    num_tagsets: usize,
    tags_per_msg: ConfRange<u8>,
    tag_length: ConfRange<u16>,
    str_pool: Rc<Pool>,
    unique_tag_probability: f32,
    unique_tags: RefCell<Vec<(Handle, Handle)>>,
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

        Ok(Generator {
            seed: Cell::new(seed),
            internal_rng: RefCell::new(rng),
            tags_per_msg,
            tag_length,
            str_pool,
            tagsets_produced: Cell::new(0),
            num_tagsets,
            unique_tag_probability,
            unique_tags: RefCell::new(Vec::new()),
        })
    }

    fn get_existing_tag<R>(&self, unique_tags: &[(Handle, Handle)], rng: &mut R) -> Option<String>
    where
        R: rand::Rng + ?Sized,
    {
        let handles = unique_tags.choose(rng);
        match handles {
            Some((key_handle, value_handle)) => {
                match (
                    self.str_pool.using_handle(*key_handle),
                    self.str_pool.using_handle(*value_handle),
                ) {
                    (Some(key), Some(value)) => Some(format!("{key}:{value}")),
                    _ => None,
                }
            }
            None => None,
        }
    }
}

// https://docs.datadoghq.com/getting_started/tagging/#define-tags
impl<'a> crate::Generator<'a> for Generator {
    type Output = Tagset;
    type Error = crate::Error;

    /// Return a tagset -- a list of tags, each tag having the format `key:value`
    /// Note that after `num_tagsets` have been produced, the tagsets will loop and produce
    /// identical tagsets.
    /// Each tagset is randomly chosen. There is a very high probability that each tagset
    /// will be unique, however see the note in the component documentation.
    fn generate<R>(&'a self, _rng: &mut R) -> Result<Self::Output, Self::Error>
    where
        R: rand::Rng + ?Sized,
    {
        // if we have produced the number of tagsets we are supposed to produce, then reseed the internal RNG
        // this ensures that we generate the same tags in a loop
        if self.tagsets_produced.get() >= self.num_tagsets {
            // Reseed internal RNG with initial seed
            self.internal_rng
                .replace(SmallRng::seed_from_u64(self.seed.get()));
            self.tagsets_produced.set(0);
        }

        // a tagset is a list of tags that will be put on a single dogstatsd message.
        // this is the return value from this function
        let mut tagset: Tagset = Vec::new();

        let mut rng = self.internal_rng.borrow_mut();
        let tags_count = self.tags_per_msg.sample(&mut *rng) as usize;
        for _ in 0..tags_count {
            let choose_existing_prob: f32 = OpenClosed01.sample(&mut *rng);

            let mut unique_tags = self.unique_tags.borrow_mut();
            let attempted_tag = if choose_existing_prob > self.unique_tag_probability {
                self.get_existing_tag(&unique_tags, &mut *rng)
            } else {
                None
            };

            // if a tag was chosen from the existing set, no need to grab a new tag.
            if let Some(tag) = attempted_tag {
                tagset.push(tag.to_string());
            } else {
                // subtract 1 to account for delimiter
                let desired_size = self.tag_length.sample(&mut *rng) as usize - 1;
                // randomly split this between two strings
                let key_size = rng.random_range(1..desired_size);
                let value_size = desired_size - key_size;

                let key_handle = self.str_pool.of_size_with_handle(&mut *rng, key_size);
                let value_handle = self.str_pool.of_size_with_handle(&mut *rng, value_size);

                match (key_handle, value_handle) {
                    (Some((key, key_handle)), Some((value, value_handle))) => {
                        unique_tags.push((key_handle, value_handle));
                        tagset.push(format!("{key}:{value}"));
                    }
                    _ => panic!("failed to generate tag"),
                }
            };
        }

        self.tagsets_produced.set(self.tagsets_produced.get() + 1);
        Ok(tagset)
    }
}

#[cfg(test)]
mod test {
    use std::collections::{BTreeSet, HashMap, hash_map::RandomState};
    use std::hash::BuildHasher;
    use std::hash::Hasher;
    use std::rc::Rc;

    use proptest::prelude::*;
    use rand::{SeedableRng, rngs::SmallRng};

    use crate::Generator;
    use crate::common::strings::Pool;
    use crate::dogstatsd::common::tags::{MAX_UNIQUE_TAG_RATIO, WARN_UNIQUE_TAG_RATIO};
    use crate::dogstatsd::{ConfRange, tags};

    /// given a list of tagsets, this returns the number of unique timeseries that they represent
    /// in dogstatsd terms, if we assume a constant metric name,
    fn count_num_contexts(tagsets: &Vec<tags::Tagset>) -> usize {
        let mut context_map: HashMap<u64, u64> = HashMap::new();
        let hash_builder = RandomState::new();

        for tagset in tagsets {
            let mut sorted_tags: BTreeSet<String> = BTreeSet::new();
            for tag in tagset {
                sorted_tags.insert(tag.to_string());
            }

            let mut context_key = hash_builder.build_hasher();
            for tag in &sorted_tags {
                context_key.write_usize(tag.len());
                context_key.write(tag.as_bytes());
            }
            let entry = context_map.entry(context_key.finish()).or_default();
            *entry += 1;
        }
        context_map.len()
    }

    #[test]
    fn count_contexts_works() {
        let tagsets = vec![
            vec!["a:1".to_string(), "b:2".to_string()],
            vec!["a:1".to_string(), "b:2".to_string()],
            vec!["a:1".to_string(), "b:2".to_string()],
            vec!["a:1".to_string(), "b:2".to_string()],
        ];
        let num_contexts = count_num_contexts(&tagsets);
        assert_eq!(num_contexts, 1);

        let tagsets = vec![
            vec!["a:3".to_string(), "b:2".to_string()],
            vec!["a:4".to_string(), "b:2".to_string()],
            vec!["a:5".to_string(), "b:2".to_string()],
            vec!["a:1".to_string(), "b:2".to_string()],
        ];
        let num_contexts = count_num_contexts(&tagsets);
        assert_eq!(num_contexts, 4);
    }

    proptest! {
        #[test]
        fn tagsets_repeat_after_reaching_tagset_max(seed: u64, num_tagsets in 1..100_000_usize) {
            let mut rng = SmallRng::seed_from_u64(seed);

            let str_pool = Rc::new(Pool::with_size(&mut rng, 8_000_000));
            let tags_per_msg_range = ConfRange::Inclusive { min: 0, max: 25 };
            let tag_size_range = ConfRange::Inclusive { min: 3, max: 128 };
            let generator =
                tags::Generator::new(seed, tags_per_msg_range, tag_size_range, num_tagsets, str_pool, 1.0)
                    .expect("Tag generator to be valid");

            let first_batch = (0..num_tagsets)
                .map(|_| {
                    generator
                        .generate(&mut rng)
                        .expect("failed to generate tagset")
                })
                .collect::<Vec<_>>();

            let second_batch = (0..num_tagsets)
                .map(|_| {
                    generator
                        .generate(&mut rng)
                        .expect("failed to generate tagset")
                })
                .collect::<Vec<_>>();

            assert_eq!(first_batch.len(), second_batch.len());
            for i in 0..first_batch.len() {
                let first = &first_batch[i];
                let second = &second_batch[i];
                assert_eq!(first, second);
            }
        }
    }

    proptest! {
        #[test]
        fn generator_yields_valid_tagsets(seed: u64, num_tagsets in 1..100_000_usize, tags_per_msg_max in 1..u8::MAX) {
            let mut rng = SmallRng::seed_from_u64(seed);

            let str_pool = Rc::new(Pool::with_size(&mut rng, 8_000_000));
            let tags_per_msg_range = ConfRange::Inclusive{min: 0, max: tags_per_msg_max};
            let tag_size_range = ConfRange::Inclusive{min: 3, max: 128};
            let generator = tags::Generator::new(
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
                    assert!(tag.len() <= tag_size_range.end().into());
                    assert!(tag.len() >= tag_size_range.start().into());
                    let num_delimiters = tag.chars().filter(|c| *c == ':').count();
                    assert_eq!(num_delimiters, 1);
                }
                assert!(tagset.len() <= tags_per_msg_range.end() as usize);
                assert!(tagset.len() >= tags_per_msg_range.start() as usize);
            }
        }
    }

    proptest! {
        /// This test asserts that when the  is 1.0, we always are able to hit
        /// the desired number of unique tagsets no matter what.
        #[test]
        fn uniquetagsets_respected_always_unique_tags(seed: u64, desired_num_tagsets in 1..100_000_usize) {
            let tags_per_msg_range = ConfRange::Inclusive { min: 2, max: 50 };
            let tag_size_range = ConfRange::Inclusive { min: 3, max: 128 };
            let mut rng = SmallRng::seed_from_u64(seed);

            let str_pool = Rc::new(Pool::with_size(&mut rng, 8_000_000));
            let generator = tags::Generator::new(
                seed,
                tags_per_msg_range,
                tag_size_range,
                desired_num_tagsets,
                str_pool,
                1.0,
            )
            .expect("Tag generator to be valid");

            // need guarantee that calling generate N times will generate N unique tagsets
            let tagsets = (0..desired_num_tagsets)
                .map(|_| {
                    generator
                        .generate(&mut rng)
                        .expect("failed to generate tagset")
                })
                .collect::<Vec<_>>();

            let num_contexts = count_num_contexts(&tagsets);
            assert_eq!(num_contexts, desired_num_tagsets);
        }
    }

    proptest! {
        /// This test varies the unique_tag_probability. This config option makes it possible
        /// to specify inputs that will force the tagsets to repeat
        /// A concern of the dogstatsd consumer is that the tagsets yielded have a cardinality of
        /// `num_tagsets`
        /// The goal of this test is to vary the unique_tag_probability between the WARN and MAX
        /// levels and ensure that we are always able to generate the desired number of unique tagsets
        #[test]
        fn uniquetagsets_respected_with_varying_ratio(seed: u64, desired_num_tagsets in 5..100_000_usize, unique_tag_ratio in WARN_UNIQUE_TAG_RATIO..MAX_UNIQUE_TAG_RATIO) {
            let tags_per_msg_range = ConfRange::Inclusive { min: 2, max: 50 };
            let tag_size_range = ConfRange::Inclusive { min: 3, max: 128 };
            let mut rng = SmallRng::seed_from_u64(seed);

            let str_pool = Rc::new(Pool::with_size(&mut rng, 8_000_000));
            let generator = tags::Generator::new(
                seed,
                tags_per_msg_range,
                tag_size_range,
                desired_num_tagsets,
                str_pool,
                unique_tag_ratio
            )
            .expect("Tag generator to be valid");

            let tagsets = (0..desired_num_tagsets)
                .map(|_| {
                    generator
                        .generate(&mut rng)
                        .expect("failed to generate tagset")
                })
                .collect::<Vec<_>>();

            let margin_of_error = 3;
            let num_contexts = count_num_contexts(&tagsets);
            assert!(num_contexts >= desired_num_tagsets - margin_of_error || num_contexts <= desired_num_tagsets + margin_of_error);
        }
    }
}
