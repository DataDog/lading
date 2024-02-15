use std::{
    cell::{Cell, RefCell},
    collections::HashSet,
    rc::Rc,
};

use rand::distributions::Distribution;
use rand::Rng;
use rand::{rngs::SmallRng, SeedableRng};
use tracing::info;

use crate::dogstatsd::common::OpenClosed01;
use crate::{common::strings, dogstatsd::ConfRange, Error};

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
    num_tagsets: usize,
    tags_per_msg: ConfRange<u8>,
    tag_key_length: ConfRange<u8>,
    tag_value_length: ConfRange<u8>,
    str_pool: Rc<strings::Pool>,
    tag_context_ratio: f32,
    unique_tags: RefCell<HashSet<String>>,
}

impl Generator {
    pub(crate) fn new(
        seed: u64,
        tags_per_msg: ConfRange<u8>,
        tag_key_length: ConfRange<u8>,
        tag_value_length: ConfRange<u8>,
        str_pool: Rc<strings::Pool>,
        num_tagsets: usize,
        tag_context_ratio: f32,
    ) -> Self {
        Generator {
            seed: Cell::new(seed),
            internal_rng: RefCell::new(SmallRng::seed_from_u64(seed)),
            tags_per_msg,
            tag_key_length,
            tag_value_length,
            str_pool,
            tagsets_produced: Cell::new(0),
            num_tagsets,
            tag_context_ratio,
            unique_tags: RefCell::new(HashSet::new()),
        }
    }
}

impl Generator {
    fn generate_single_tag(&self, rng: &mut SmallRng) -> String {
        let key_sz = self.tag_key_length.sample(&mut *rng) as usize;
        let key = self.str_pool.of_size(&mut *rng, key_sz).unwrap_or_default();
        let value_sz = self.tag_value_length.sample(&mut *rng) as usize;
        let value = self
            .str_pool
            .of_size(&mut *rng, value_sz)
            .unwrap_or_default();
        format!("{key}:{value}")
    }
}

// https://docs.datadoghq.com/getting_started/tagging/#define-tags
impl<'a> crate::Generator<'a> for Generator {
    type Output = Tagset;
    type Error = Error;

    /// Output here is a list of tags that will be put on a single dogstatsd message
    fn generate<R>(&'a self, _rng: &mut R) -> Result<Self::Output, Error>
    where
        R: rand::Rng + ?Sized,
    {
        info!("Producing a tagset");
        // if we have produced the number of tagsets we are supposed to produce, then reseed the internal RNG
        // this ensures that we generate the same tags in a loop
        if self.tagsets_produced.get() >= self.num_tagsets {
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
            let mut unique_tags = self.unique_tags.borrow_mut();
            let mut rng = self.internal_rng.borrow_mut();
            // if ratio is 1.0, then we always generate a brand-new tag
            // if the ratio is 0.5, then we generate a new tag 50% of the time
            // if the ratio is 0.0, then we never generate a new tag
            // so we generate a new tag if the ratio is greater than a random number between 0 and 1
            let choose_existing_prob: f32 = OpenClosed01.sample(&mut *rng);

            let attempted_tag = if choose_existing_prob < self.tag_context_ratio {
                unique_tags
                    .iter()
                    .nth((*rng).gen_range(0..unique_tags.len()))
            } else {
                None
            };
            if let Some(tag) = attempted_tag {
                tagset.push(tag.clone());
            } else {
                let new_tag = self.generate_single_tag(&mut rng);
                unique_tags.insert(new_tag.clone());
                tagset.push(new_tag);
            };
        }

        self.tagsets_produced.set(self.tagsets_produced.get() + 1);
        info!("Produced a tagset of {} tags", tagset.len());
        Ok(tagset)
    }
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;
    use rand::{rngs::SmallRng, SeedableRng};

    use crate::dogstatsd::{strings, tags, ConfRange};
    use crate::Generator;
    use std::rc::Rc;

    proptest! {
        #[test]
        fn generator_not_exceed_tagset_max(seed: u64, num_tagsets in 0..1_000_usize) {
            let mut rng = SmallRng::seed_from_u64(seed);
            let pool = Rc::new(strings::Pool::with_size(&mut rng, 8_000_000));

            let tags_per_msg_max = 255;
            let generator = tags::Generator::new(
                seed,
                ConfRange::Inclusive{min: 0, max: tags_per_msg_max},
                ConfRange::Inclusive{min: 1, max: 64},
                ConfRange::Inclusive{min: 1, max: 64},
                pool.clone(),
                num_tagsets,
                1.0
            );
            let tagset = generator.generate(&mut rng)?;
            assert!(tagset.len() <= tags_per_msg_max as usize);
        }
    }
}
