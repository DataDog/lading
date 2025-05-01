//! Tag generation for dogstatsd payloads
use crate::{common::strings::Pool, dogstatsd::ConfRange};
use std::rc::Rc;

pub(crate) type Tagset = Vec<String>;

#[derive(Debug, Clone)]
pub(crate) struct Generator {
    inner: crate::common::tags::Generator,
}

/// Error type for `TagGenerator`
#[derive(thiserror::Error, Debug)]
pub(crate) enum Error {
    /// Invalid construction
    #[error("Invalid construction: {0}")]
    InvalidConstruction(#[from] crate::common::tags::Error),
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
        // Adjust tag_length range to account for the colon separator
        let adjusted_tag_length = ConfRange::Inclusive {
            min: tag_length.start(),
            max: tag_length.end().saturating_sub(1),
        };

        let inner = crate::common::tags::Generator::new(
            seed,
            tags_per_msg,
            adjusted_tag_length,
            num_tagsets,
            str_pool,
            unique_tag_probability,
        )?;
        Ok(Generator { inner })
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
    fn generate<R>(&'a self, rng: &mut R) -> Result<Self::Output, Self::Error>
    where
        R: rand::Rng + ?Sized,
    {
        let mut tag_handles = self.inner.generate(rng)?;
        let mut tagset = Vec::with_capacity(tag_handles.len());
        for crate::common::tags::Tag { key, value } in tag_handles.drain(..) {
            let key_s = self
                .inner
                .using_handle(key)
                .expect("invalid handle, catastrophic bug");
            let val_s = self
                .inner
                .using_handle(value)
                .expect("invalid handle, catastrophic bug");
            let mut s = String::with_capacity(key_s.len() + 1 + val_s.len());
            s.push_str(key_s);
            s.push(':');
            s.push_str(val_s);
            tagset.push(s);
        }
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
    use crate::common::tags::{MAX_UNIQUE_TAG_RATIO, WARN_UNIQUE_TAG_RATIO};
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
        fn tagsets_repeat_after_reaching_tagset_max(seed: u64, num_tagsets in 1..10_000_usize) {
            let mut rng = SmallRng::seed_from_u64(seed);

            let str_pool = Rc::new(Pool::with_size(&mut rng, 1_000_000));
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

            prop_assert_eq!(first_batch.len(), second_batch.len());
            for i in 0..first_batch.len() {
                let first = &first_batch[i];
                let second = &second_batch[i];
                prop_assert_eq!(first, second);
            }
        }
    }

    proptest! {
        #[test]
        fn generator_yields_valid_tagsets(seed: u64, num_tagsets in 1..10_000_usize, tags_per_msg_max in 1..u8::MAX) {
            let mut rng = SmallRng::seed_from_u64(seed);

            let str_pool = Rc::new(Pool::with_size(&mut rng, 1_000_000));
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
                    let start = tag_size_range.start().into();
                    let end = tag_size_range.end().into();
                    prop_assert!(tag.len() <= end, "tag len: {}, tag_size_range end: {end}", tag.len());
                    prop_assert!(tag.len() >= start, "tag len: {}, tag_size_range start: {start}", tag.len());
                    let num_delimiters = tag.chars().filter(|c| *c == ':').count();
                    prop_assert_eq!(num_delimiters, 1);
                }
                prop_assert!(tagset.len() <= tags_per_msg_range.end() as usize);
                prop_assert!(tagset.len() >= tags_per_msg_range.start() as usize);
            }
        }
    }

    proptest! {
        /// This test asserts that when the  is 1.0, we always are able to hit
        /// the desired number of unique tagsets no matter what.
        #[test]
        fn unique_tagsets_respected_always_unique_tags(seed: u64, desired_num_tagsets in 1..10_000_usize) {
            let tags_per_msg_range = ConfRange::Inclusive { min: 2, max: 25 };
            let tag_size_range = ConfRange::Inclusive { min: 3, max: 128 };
            let mut rng = SmallRng::seed_from_u64(seed);

            let str_pool = Rc::new(Pool::with_size(&mut rng, 1_000_000));
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
            prop_assert_eq!(num_contexts, desired_num_tagsets);
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
        fn unique_tagsets_respected_with_varying_ratio(seed: u64, desired_num_tagsets in 5..10_000_usize, unique_tag_ratio in WARN_UNIQUE_TAG_RATIO..MAX_UNIQUE_TAG_RATIO) {
            let tags_per_msg_range = ConfRange::Inclusive { min: 2, max: 25 };
            let tag_size_range = ConfRange::Inclusive { min: 3, max: 128 };
            let mut rng = SmallRng::seed_from_u64(seed);

            let str_pool = Rc::new(Pool::with_size(&mut rng, 1_000_000));
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

            let num_contexts = count_num_contexts(&tagsets);
            prop_assert_eq!(num_contexts, desired_num_tagsets);
        }
    }
}
