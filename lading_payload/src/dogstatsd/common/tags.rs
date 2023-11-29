use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};

use crate::{common::strings, dogstatsd::ConfRange};

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
    default_tags: Vec<String>,
    tags_per_msg: ConfRange<u8>,
    tag_key_length: ConfRange<u8>,
    tag_value_length: ConfRange<u8>,
    str_pool: Rc<strings::Pool>,
}

impl Generator {
    pub(crate) fn new(
        seed: u64,
        tags_per_msg: ConfRange<u8>,
        default_tags: Vec<String>,
        tag_key_length: ConfRange<u8>,
        tag_value_length: ConfRange<u8>,
        str_pool: Rc<strings::Pool>,
        num_tagsets: usize,
    ) -> Self {
        Generator {
            seed: Cell::new(seed),
            internal_rng: RefCell::new(SmallRng::seed_from_u64(seed)),
            default_tags,
            tags_per_msg,
            tag_key_length,
            tag_value_length,
            str_pool,
            tagsets_produced: Cell::new(0),
            num_tagsets,
        }
    }
}

// https://docs.datadoghq.com/getting_started/tagging/#define-tags
impl<'a> crate::Generator<'a> for Generator {
    type Output = Tagset;

    fn generate<R>(&'a self, _rng: &mut R) -> Self::Output
    where
        R: rand::Rng + ?Sized,
    {
        let mut tagset = Vec::new();

        if let Some(default_tag) = self
            .default_tags
            .choose(&mut *self.internal_rng.borrow_mut())
        {
            // The user is allowed to pass an empty string in as a default
            // tagset. Save ourselves a little allocation and avoid putting ""
            // in tagset. This also means we don't have to cope with empty
            // strings when it comes time to serialize to text.
            if !default_tag.is_empty() {
                tagset.push(default_tag.clone());
            }
        }

        let tags_count = self
            .tags_per_msg
            .sample(&mut *self.internal_rng.borrow_mut()) as usize;
        for _ in 0..tags_count {
            if self.tagsets_produced.get() >= self.num_tagsets {
                // Reseed internal RNG with initial seed
                self.internal_rng
                    .replace(SmallRng::seed_from_u64(self.seed.get()));
                self.tagsets_produced.set(0);
            }

            let mut rng = self.internal_rng.borrow_mut();
            let key_sz = self.tag_key_length.sample(&mut *rng) as usize;
            let key = self.str_pool.of_size(&mut *rng, key_sz).unwrap_or_default();
            let value_sz = self.tag_value_length.sample(&mut *rng) as usize;
            let value = self
                .str_pool
                .of_size(&mut *rng, value_sz)
                .unwrap_or_default();

            self.tagsets_produced.set(self.tagsets_produced.get() + 1);
            tagset.push(format!("{key}:{value}"));
        }

        tagset
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
        fn generator_not_exceed_tagset_max(seed: u64, num_tagsets in 0..1_000) {
            let mut rng = SmallRng::seed_from_u64(seed);
            let num_tagsets = num_tagsets as usize;
            let pool = Rc::new(strings::Pool::with_size(&mut rng, 8_000_000));

            let tags_per_msg_max = 255;
            let generator = tags::Generator::new(
                seed,
                ConfRange::Inclusive{min: 0, max: tags_per_msg_max},
                Vec::new(),
                ConfRange::Inclusive{min: 1, max: 64},
                ConfRange::Inclusive{min: 1, max: 64},
                pool.clone(),
                num_tagsets,
            );
            let tagset = generator.generate(&mut rng);
            assert!(tagset.len() <= tags_per_msg_max as usize);
        }
    }
}
