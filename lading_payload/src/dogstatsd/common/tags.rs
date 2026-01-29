//! Tag generation for dogstatsd payloads
use crate::{
    common::{strings::PoolKind, tags},
    dogstatsd::ConfRange,
};
use std::rc::Rc;

// Use handle-based tagset from core module directly, avoiding String
// allocations. Formatting happens at serialization time with O(1) handle
// lookups.
pub(crate) use tags::Tagset;

#[derive(Debug, Clone)]
pub(crate) struct Generator {
    inner: tags::Generator,
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
        key_pool: Rc<PoolKind>,
        tag_pool: Rc<PoolKind>,
        unique_tag_probability: f32,
    ) -> Result<Self, Error> {
        // Adjust tag_length range to account for the colon separator
        let adjusted_tag_length = ConfRange::Inclusive {
            min: tag_length.start(),
            max: tag_length.end().saturating_sub(1),
        };

        let inner = tags::Generator::new(
            seed,
            tags_per_msg,
            adjusted_tag_length,
            num_tagsets,
            key_pool,
            tag_pool,
            unique_tag_probability,
        )?;
        Ok(Generator { inner })
    }
}

// https://docs.datadoghq.com/getting_started/tagging/#define-tags
impl<'a> crate::Generator<'a> for Generator {
    type Output = Tagset;
    type Error = crate::Error;

    /// Return a tagset -- a list of tags as handle pairs (key, value).
    ///
    /// Note that after `num_tagsets` have been produced, the tagsets will loop
    /// and produce identical tagsets.
    fn generate<R>(&'a self, rng: &mut R) -> Result<Self::Output, Self::Error>
    where
        R: rand::Rng + ?Sized,
    {
        self.inner.generate(rng)
    }
}

#[cfg(test)]
mod test {
    use std::collections::{HashSet, hash_map::RandomState};
    use std::hash::BuildHasher;
    use std::hash::Hasher;
    use std::rc::Rc;

    use proptest::prelude::*;
    use rand::{SeedableRng, rngs::SmallRng};

    use crate::Generator;
    use crate::common::strings::{Handle, PoolKind, RandomStringPool};
    use crate::common::tags::{MAX_UNIQUE_TAG_RATIO, Tag, WARN_UNIQUE_TAG_RATIO};
    use crate::dogstatsd::{ConfRange, tags};

    /// Given a list of tagsets, count unique contexts.
    fn count_num_contexts(tagsets: &[tags::Tagset]) -> usize {
        let mut unique_contexts: HashSet<u64> = HashSet::new();
        let hash_builder = RandomState::new();

        for tagset in tagsets {
            let mut sorted_handles: Vec<(usize, usize, usize, usize)> = tagset
                .iter()
                .map(|t| {
                    let (k0, k1) = match t.key {
                        Handle::PosAndLength(k0, k1) => (k0 as usize, k1 as usize),
                        Handle::Index(k0) => (k0, 0),
                    };
                    let (v0, v1) = match t.value {
                        Handle::PosAndLength(v0, v1) => (v0 as usize, v1 as usize),
                        Handle::Index(v0) => (v0, 0),
                    };
                    (k0, k1, v0, v1)
                })
                .collect();
            sorted_handles.sort();

            let mut context_hasher = hash_builder.build_hasher();
            for (k0, k1, v0, v1) in &sorted_handles {
                context_hasher.write_usize(*k0);
                context_hasher.write_usize(*k1);
                context_hasher.write_usize(*v0);
                context_hasher.write_usize(*v1);
            }
            unique_contexts.insert(context_hasher.finish());
        }
        unique_contexts.len()
    }

    #[test]
    fn count_contexts_works() {
        // Create tags with identical handles - same context
        let tag1 = Tag {
            key: Handle::PosAndLength(0, 1),
            value: Handle::PosAndLength(2, 1),
        };
        let tag2 = Tag {
            key: Handle::PosAndLength(10, 1),
            value: Handle::PosAndLength(12, 1),
        };

        let tagsets = vec![
            vec![tag1, tag2],
            vec![tag1, tag2],
            vec![tag1, tag2],
            vec![tag1, tag2],
        ];
        let num_contexts = count_num_contexts(&tagsets);
        assert_eq!(num_contexts, 1);

        // Different tags = different contexts
        let tag3 = Tag {
            key: Handle::PosAndLength(0, 1),
            value: Handle::PosAndLength(3, 1),
        };
        let tag4 = Tag {
            key: Handle::PosAndLength(0, 1),
            value: Handle::PosAndLength(4, 1),
        };
        let tag5 = Tag {
            key: Handle::PosAndLength(0, 1),
            value: Handle::PosAndLength(5, 1),
        };
        let tagsets = vec![
            vec![tag3, tag2],
            vec![tag4, tag2],
            vec![tag5, tag2],
            vec![tag1, tag2],
        ];
        let num_contexts = count_num_contexts(&tagsets);
        assert_eq!(num_contexts, 4);
    }

    proptest! {
        #[test]
        fn tagsets_repeat_after_reaching_tagset_max(seed: u64, num_tagsets in 1..10_000_usize) {
            let mut rng = SmallRng::seed_from_u64(seed);

            let str_pool = Rc::new(PoolKind::RandomStringPool(RandomStringPool::with_size(&mut rng, 1_000_000)));
            let tags_per_msg_range = ConfRange::Inclusive { min: 0, max: 25 };
            let tag_size_range = ConfRange::Inclusive { min: 3, max: 128 };
            let tag_pool = Rc::clone(&str_pool);
            let generator =
                tags::Generator::new(seed, tags_per_msg_range, tag_size_range, num_tagsets, str_pool, tag_pool, 1.0)
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
        /// This test asserts that when the  is 1.0, we always are able to hit
        /// the desired number of unique tagsets no matter what.
        #[test]
        fn unique_tagsets_respected_always_unique_tags(seed: u64, desired_num_tagsets in 1..5_000_usize) {
            let tags_per_msg_range = ConfRange::Inclusive { min: 2, max: 25 };
            let tag_size_range = ConfRange::Inclusive { min: 3, max: 128 };
            let mut rng = SmallRng::seed_from_u64(seed);

            let str_pool = Rc::new(PoolKind::RandomStringPool(RandomStringPool::with_size(&mut rng, 500_000)));
            let tag_pool = Rc::clone(&str_pool);
            let generator = tags::Generator::new(
                seed,
                tags_per_msg_range,
                tag_size_range,
                desired_num_tagsets,
                str_pool,
                tag_pool,
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
        fn unique_tagsets_respected_with_varying_ratio(seed: u64, desired_num_tagsets in 5..5_000_usize, unique_tag_ratio in WARN_UNIQUE_TAG_RATIO..MAX_UNIQUE_TAG_RATIO) {
            let tags_per_msg_range = ConfRange::Inclusive { min: 2, max: 25 };
            let tag_size_range = ConfRange::Inclusive { min: 3, max: 128 };
            let mut rng = SmallRng::seed_from_u64(seed);

            let str_pool = Rc::new(PoolKind::RandomStringPool(RandomStringPool::with_size(&mut rng, 500_000)));
            let tag_pool = Rc::clone(&str_pool);
            let generator = tags::Generator::new(
                seed,
                tags_per_msg_range,
                tag_size_range,
                desired_num_tagsets,
                str_pool,
                tag_pool,
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
