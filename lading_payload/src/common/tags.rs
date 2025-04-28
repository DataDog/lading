//! Tag generation for dogstatsd payloads
use std::cell::{Cell, RefCell};
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
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Tag {
    pub(crate) key: Handle,
    pub(crate) value: Handle,
}
pub(crate) type Tagset = Vec<Tag>;

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
    num_tagsets: usize,
    tags_per_msg: ConfRange<u8>,
    tag_length: ConfRange<u16>,
    str_pool: Rc<Pool>,
    unique_tag_probability: f32,
    unique_tags: RefCell<Vec<Tag>>,
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

    /// Given an opaque handle returned from `generate`, return the &str it
    /// represents. None if handle is not valid.
    #[must_use]
    #[inline]
    pub(crate) fn using_handle(&self, handle: Handle) -> Option<&str> {
        self.str_pool.using_handle(handle)
    }
}

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
        for _ in 0..tags_count {
            let choose_existing_prob: f32 = OpenClosed01.sample(&mut *rng);

            let mut unique_tags = self.unique_tags.borrow_mut();
            let attempted_tag = if choose_existing_prob > self.unique_tag_probability {
                unique_tags.choose(&mut rng).copied()
            } else {
                None
            };

            // If a tag was chosen from the existing set, no need to grab a new tag.
            if let Some(tag) = attempted_tag {
                tagset.push(tag);
            } else {
                let desired_size = self.tag_length.sample(&mut *rng) as usize;
                // randomly split this between two strings
                let key_size = rng.random_range(1..desired_size);
                let value_size = desired_size - key_size;

                let key_handle = self.str_pool.of_size_with_handle(&mut *rng, key_size);
                let value_handle = self.str_pool.of_size_with_handle(&mut *rng, value_size);

                match (key_handle, value_handle) {
                    (Some((_, key_handle)), Some((_, value_handle))) => {
                        let tag: Tag = Tag {
                            key: key_handle,
                            value: value_handle,
                        };
                        unique_tags.push(tag);
                        tagset.push(tag);
                    }
                    _ => panic!("failed to generate tag"),
                }
            };
        }

        self.tagsets_produced.set(self.tagsets_produced.get() + 1);
        Ok(tagset)
    }
}
