//! Tag generation for dogstatsd payloads
use crate::{
    common::{
        strings::PoolKind,
        tags::{self, MIN_TAG_LENGTH},
    },
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

    /// The on-wire `tag_length` range cannot accommodate the `:`
    /// separator. On-wire dogstatsd tags are `key:value`, so this
    /// wrapper reserves one byte for the separator -- forwarding
    /// `Inclusive { min: start, max: end - 1 }` to the inner
    /// generator, which requires a non-empty range whose max is at
    /// least `MIN_TAG_LENGTH`. The adjusted range collapses to empty
    /// or sub-minimum whenever `end` is not strictly greater than
    /// both `start` and `MIN_TAG_LENGTH` -- e.g. any constant or
    /// single-value range such as `Constant(3)`, `Constant(4)`, or
    /// `Inclusive { min: 100, max: 100 }`. Caught here so the failure
    /// names the configured range rather than the adjusted one.
    #[error(
        "tag_length end ({end}) must be strictly greater than both start ({start}) and \
         MIN_TAG_LENGTH ({min}) so the range remains valid after reserving one byte for the \
         ':' separator"
    )]
    TagLengthRangeTooNarrow { start: u16, end: u16, min: u16 },
}

impl Generator {
    /// Creates a new tag generator
    ///
    /// # Errors
    /// - If `tags_per_msg` is invalid or exceeds the maximum
    /// - If `tag_length.start()` is less than `MIN_TAG_LENGTH` (forwarded
    ///   from the inner generator's `Error::InvalidConstruction`)
    /// - If `tag_length.end()` is not strictly greater than both
    ///   `tag_length.start()` and `MIN_TAG_LENGTH`, returned as
    ///   `Error::TagLengthRangeTooNarrow`. This catches every range that
    ///   would collapse after the colon-separator subtract on `max` -- any
    ///   constant or single-value range such as `ConfRange::Constant(N)`
    ///   or `Inclusive { min: N, max: N }`, as well as `end` values at or
    ///   below `MIN_TAG_LENGTH` -- which would otherwise yield an inner
    ///   range with `min > max` and surface as a misleading inner
    ///   construction error.
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
        if tag_length.end() <= MIN_TAG_LENGTH || tag_length.end() <= tag_length.start() {
            return Err(Error::TagLengthRangeTooNarrow {
                start: tag_length.start(),
                end: tag_length.end(),
                min: MIN_TAG_LENGTH,
            });
        }

        // Reserve one byte for the `:` separator before forwarding to the
        // inner generator. The checks above guarantee the adjusted range is
        // non-empty (`start <= end - 1`) and that its `max` stays at or above
        // `MIN_TAG_LENGTH`. The inner generator still validates that the
        // adjusted `min` (`start`) is itself at least `MIN_TAG_LENGTH`.
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
    use crate::common::strings::{Handle, PoolKind, RandomStringPool, StringListPool};
    use crate::common::tags::{MAX_UNIQUE_TAG_RATIO, MIN_TAG_LENGTH, Tag, WARN_UNIQUE_TAG_RATIO};
    use crate::dogstatsd::common::tags::Error;
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
            sorted_handles.sort_unstable();

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
        #[test]
        fn tagsets_repeat_after_reaching_tagset_max_string_list_pool(seed: u64, num_tagsets in 1..10_000_usize) {
            let mut rng = SmallRng::seed_from_u64(seed);

            // Create a string list pool with enough strings to generate unique tagsets
            let string_list: Vec<String> = (0..1_000)
                .map(|i| format!("string_{i}"))
                .collect();
            let str_pool = Rc::new(PoolKind::StringListPool(StringListPool::new(&string_list, 10_000).expect("valid patterns")));
            let tags_per_msg_range = ConfRange::Inclusive { min: 0, max: 25 };
            let tag_size_range = ConfRange::Inclusive { min: 3, max: 128 };

            let tag_list: Vec<String> = (0..1_000)
                .map(|i| format!("string_{i}"))
                .collect();
            let tag_pool = Rc::new(PoolKind::StringListPool(StringListPool::new(&tag_list, 10_000).expect("valid patterns")));
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

    proptest! {
        /// This test asserts that when the unique_tag_probability is 1.0, we always are able to hit
        /// the desired number of unique tagsets no matter what, using StringListPool.
        #[test]
        fn unique_tagsets_respected_always_unique_tags_string_list_pool(seed: u64, desired_num_tagsets in 1..5_000_usize) {
            let tags_per_msg_range = ConfRange::Inclusive { min: 2, max: 25 };
            let tag_size_range = ConfRange::Inclusive { min: 3, max: 128 };
            let mut rng = SmallRng::seed_from_u64(seed);

            // Create a string list pool with enough strings to generate unique tagsets
            let string_list: Vec<String> = (0..10_000)
                .map(|i| format!("key_{i}"))
                .collect();
            let str_pool = Rc::new(PoolKind::StringListPool(StringListPool::new(&string_list, 10_000).expect("valid patterns")));

            let tag_list: Vec<String> = (0..10_000)
                .map(|i| format!("value_{i}"))
                .collect();
            let tag_pool = Rc::new(PoolKind::StringListPool(StringListPool::new(&tag_list, 10_000).expect("valid patterns")));

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
        /// This test varies the unique_tag_probability with StringListPool. This config option makes it possible
        /// to specify inputs that will force the tagsets to repeat
        /// A concern of the dogstatsd consumer is that the tagsets yielded have a cardinality of
        /// `num_tagsets`
        /// The goal of this test is to vary the unique_tag_probability between the WARN and MAX
        /// levels and ensure that we are always able to generate the desired number of unique tagsets
        #[test]
        fn unique_tagsets_respected_with_varying_ratio_string_list_pool(seed: u64, desired_num_tagsets in 5..5_000_usize, unique_tag_ratio in WARN_UNIQUE_TAG_RATIO..MAX_UNIQUE_TAG_RATIO) {
            let tags_per_msg_range = ConfRange::Inclusive { min: 2, max: 25 };
            let tag_size_range = ConfRange::Inclusive { min: 3, max: 128 };
            let mut rng = SmallRng::seed_from_u64(seed);

            // Create a string list pool with enough strings to generate unique tagsets
            let string_list: Vec<String> = (0..10_000)
                .map(|i| format!("key_{i}"))
                .collect();
            let str_pool = Rc::new(PoolKind::StringListPool(StringListPool::new(&string_list, 10_000).expect("valid patterns")));

            let tag_list: Vec<String> = (0..10_000)
                .map(|i| format!("value_{i}"))
                .collect();
            let tag_pool = Rc::new(PoolKind::StringListPool(StringListPool::new(&tag_list, 10_000).expect("valid patterns")));

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

    /// Construct a wrapper `Generator` for the given `tag_length`, holding
    /// every other argument fixed. Used by the `tag_length` boundary tests.
    fn generator_with_tag_length(tag_length: ConfRange<u16>) -> Result<tags::Generator, Error> {
        let mut rng = SmallRng::seed_from_u64(0);
        let pool = Rc::new(PoolKind::RandomStringPool(RandomStringPool::with_size(
            &mut rng, 1024,
        )));
        tags::Generator::new(
            0,
            ConfRange::Inclusive { min: 0, max: 2 },
            tag_length,
            10,
            Rc::clone(&pool),
            pool,
            1.0,
        )
    }

    /// `tag_length.end() == MIN_TAG_LENGTH` would, prior to the
    /// upfront validation, produce an adjusted range with `min >
    /// max` and surface as a misleading inner construction error.
    /// The wrapper now rejects this with `TagLengthRangeTooNarrow`.
    #[test]
    fn tag_length_constant_at_min_rejected() {
        match generator_with_tag_length(ConfRange::Constant(MIN_TAG_LENGTH)) {
            Err(Error::TagLengthRangeTooNarrow { start, end, min }) => {
                assert_eq!(start, MIN_TAG_LENGTH);
                assert_eq!(end, MIN_TAG_LENGTH);
                assert_eq!(min, MIN_TAG_LENGTH);
            }
            other => panic!("expected TagLengthRangeTooNarrow, got {other:?}"),
        }
    }

    /// `Inclusive { min: MIN_TAG_LENGTH, max: MIN_TAG_LENGTH }` is
    /// the same problematic shape as the constant case and must
    /// also be rejected by the upfront check.
    #[test]
    fn tag_length_inclusive_min_equals_max_at_min_rejected() {
        let result = generator_with_tag_length(ConfRange::Inclusive {
            min: MIN_TAG_LENGTH,
            max: MIN_TAG_LENGTH,
        });
        assert!(matches!(result, Err(Error::TagLengthRangeTooNarrow { .. })));
    }

    /// A constant range *above* the minimum still collapses: after
    /// reserving the separator byte the adjusted range is
    /// `Inclusive { min: N, max: N - 1 }`. It must be rejected with
    /// `TagLengthRangeTooNarrow`, not fall through to the inner
    /// generator's misleading construction error.
    #[test]
    fn tag_length_constant_above_min_rejected() {
        match generator_with_tag_length(ConfRange::Constant(MIN_TAG_LENGTH + 1)) {
            Err(Error::TagLengthRangeTooNarrow { start, end, min }) => {
                assert_eq!(start, MIN_TAG_LENGTH + 1);
                assert_eq!(end, MIN_TAG_LENGTH + 1);
                assert_eq!(min, MIN_TAG_LENGTH);
            }
            other => panic!("expected TagLengthRangeTooNarrow, got {other:?}"),
        }
    }

    /// A single-value `Inclusive` range well above the minimum
    /// (`min == max`) collapses for the same reason and must also be
    /// rejected.
    #[test]
    fn tag_length_inclusive_single_value_above_min_rejected() {
        let result = generator_with_tag_length(ConfRange::Inclusive { min: 100, max: 100 });
        assert!(matches!(result, Err(Error::TagLengthRangeTooNarrow { .. })));
    }

    /// `tag_length.end() == MIN_TAG_LENGTH + 1` is the smallest
    /// accepted end value. The adjusted inner range becomes
    /// `Inclusive { min: MIN_TAG_LENGTH, max: MIN_TAG_LENGTH }`,
    /// which the inner generator accepts.
    #[test]
    fn tag_length_at_min_plus_one_accepted() {
        let result = generator_with_tag_length(ConfRange::Inclusive {
            min: MIN_TAG_LENGTH,
            max: MIN_TAG_LENGTH + 1,
        });
        assert!(result.is_ok(), "{:?}", result.err());
    }
}
