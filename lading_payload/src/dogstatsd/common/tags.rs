//! Tag generation for dogstatsd payloads
use std::cell::{Cell, RefCell, RefMut};
use std::{collections::HashSet, rc::Rc};

use rand::distributions::Distribution;
use rand::Rng;
use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};

use crate::dogstatsd::common::OpenClosed01;
use crate::{
    common::strings::{Handle, Pool},
    dogstatsd::ConfRange,
};

// This represents a list of tags that will be present on a single
// dogstatsd message.
pub(crate) type Tagset = Vec<String>;

/// Generator for tags
///
/// This is an unusual generator. Unlike our others this maintains its own RNG
/// and will reseed it when counter reaches `num_tagsets`. The goal is to
/// produce only a limited, deterministic set of tags while avoiding needing to
/// allocate them all in one shot.
#[derive(Debug, Clone)]
pub(crate) struct Generator {
    seed: Cell<u64>,
    internal_rng: RefCell<SmallRng>,
    tagsets_produced: Cell<usize>,
    num_contexts: usize,
    tags_per_msg: ConfRange<u8>,
    tag_length: ConfRange<u16>,
    pool: Pool,
    tag_context_ratio: f32,
    unique_tags: RefCell<Vec<(Handle, Handle)>>,
}

/// Error type for `TagGenerator`
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Invalid construction
    #[error("Invalid construction: {0}")]
    InvalidConstruction(String),
}

impl Generator {
    #[allow(unreachable_pub)]
    /// Creates a new tag generator
    ///
    /// # Errors
    /// - If `tags_per_msg` is invalid or exceeds the maximum
    /// - If `tag_length` is invalid or has minimum value less than 3
    pub(crate) fn new(
        seed: u64,
        tags_per_msg: ConfRange<u8>,
        tag_length: ConfRange<u16>,
        num_contexts: usize,
        tag_context_ratio: f32,
    ) -> Result<Self, Error> {
        let mut rng = SmallRng::seed_from_u64(seed);
        let pool = Pool::with_size(&mut rng, 1_000_000);
        let (tag_length_valid, tag_length_valid_msg) = tag_length.valid();
        if !tag_length_valid {
            return Err(Error::InvalidConstruction(format!(
                "Invalid tag length: {tag_length_valid_msg}"
            )));
        }
        if tag_length.start() < 3 {
            return Err(Error::InvalidConstruction(
                "Tag length must be at least 3".to_string(),
            ));
        }

        Ok(Generator {
            seed: Cell::new(seed),
            internal_rng: RefCell::new(rng),
            tags_per_msg,
            tag_length,
            pool,
            tagsets_produced: Cell::new(0),
            num_contexts,
            tag_context_ratio,
            unique_tags: RefCell::new(Vec::new()),
        })
    }

    fn get_existing_tag<R>(
        &self,
        unique_tags: &RefMut<Vec<(Handle, Handle)>>,
        mut rng: RefMut<R>,
    ) -> Option<String>
    where
        R: rand::Rng + ?Sized,
    {
        let handles = unique_tags.choose(&mut *rng);
        match handles {
            Some((key_handle, value_handle)) => {
                match (
                    self.pool.from_handle(*key_handle),
                    self.pool.from_handle(*value_handle),
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

    /// Output here is a list of tags that will be put on a single dogstatsd message
    fn generate<R>(&'a self, _rng: &mut R) -> Result<Self::Output, crate::Error>
    where
        R: rand::Rng + ?Sized,
    {
        // if we have produced the number of tagsets we are supposed to produce, then reseed the internal RNG
        // this ensures that we generate the same tags in a loop
        if self.tagsets_produced.get() >= self.num_contexts {
            // Reseed internal RNG with initial seed
            self.internal_rng
                .replace(SmallRng::seed_from_u64(self.seed.get()));
            self.tagsets_produced.set(0);
        }

        // a single tagset is a list of tags that will be put on a single dogstatsd message
        // this is the return value from this function
        let mut tagset: Tagset = Vec::new();

        let tags_count = self
            .tags_per_msg
            .sample(&mut *self.internal_rng.borrow_mut()) as usize;
        for _ in 0..tags_count {
            // if ratio is 1.0, then we always generate a brand-new tag
            // if the ratio is 0.5, then we generate a new tag 50% of the time
            // if the ratio is 0.0, then we never generate a new tag
            // so we generate a new tag if the ratio is greater than a random number between 0 and 1
            let choose_existing_prob: f32 =
                OpenClosed01.sample(&mut *(self.internal_rng.borrow_mut()));

            let attempted_tag = if choose_existing_prob < self.tag_context_ratio {
                self.get_existing_tag(
                    &self.unique_tags.borrow_mut(),
                    self.internal_rng.borrow_mut(),
                )
            } else {
                None
            };

            let mut unique_tags = self.unique_tags.borrow_mut();
            let mut rng = self.internal_rng.borrow_mut();
            // if a tag was chosen from the existing set, no need to grab a new tag.
            if let Some(tag) = attempted_tag {
                tagset.push(tag.to_string());
            } else {
                // subtract 1 to account for delimiter
                let desired_size = self.tag_length.sample(&mut *rng) as usize - 1;
                // randomly split this between two strings
                let key_size = rng.gen_range(1..desired_size);
                let value_size = desired_size - key_size;

                let key_handle = self.pool.of_size_with_handle(&mut *rng, key_size);
                let value_handle = self.pool.of_size_with_handle(&mut *rng, value_size);

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
    use proptest::prelude::*;
    use rand::{rngs::SmallRng, SeedableRng};

    use crate::dogstatsd::{tags, ConfRange};
    use crate::Generator;

    proptest! {
        #[test]
        fn generator_yields_valid_tagsets(seed: u64, num_contexts in 1..100_000_usize, tags_per_msg_max in 1..u8::MAX) {
            let mut rng = SmallRng::seed_from_u64(seed);

            let tags_per_msg_range = ConfRange::Inclusive{min: 0, max: tags_per_msg_max};
            let tag_size_range = ConfRange::Inclusive{min: 3, max: 128};
            let generator = tags::Generator::new(
                seed,
                tags_per_msg_range,
                tag_size_range,
                num_contexts,
                1.0
            ).expect("Tag generator to be valid");

            for _ in 0..100 {
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
}
