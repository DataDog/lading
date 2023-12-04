use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

use rand::{
    distributions::{Distribution, OpenClosed01},
    rngs::SmallRng,
    seq::SliceRandom,
    SeedableRng,
};

use crate::{common::strings, dogstatsd::ConfRange};

// This represents a list of tags that will be present on a single
// dogstatsd message.
pub(crate) type Tagset = Vec<String>;

/// Generator for tagsets
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
    common_tag_pool: Vec<String>,
    tags_per_msg: ConfRange<u8>,
    tag_key_length: ConfRange<u8>,
    tag_value_length: ConfRange<u8>,
    str_pool: Rc<strings::Pool>,
}

fn generate_single_tag(
    rng: &mut SmallRng,
    tag_key_length: ConfRange<u8>,
    tag_value_length: ConfRange<u8>,
    str_pool: &strings::Pool,
) -> String {
    let key_sz = tag_key_length.sample(&mut *rng) as usize;
    let key = str_pool.of_size(&mut *rng, key_sz).unwrap_or_default();
    let value_sz = tag_value_length.sample(&mut *rng) as usize;
    let value = str_pool.of_size(&mut *rng, value_sz).unwrap_or_default();
    format!("{}:{}", key, value)
}

impl Generator {
    pub(crate) fn new(
        seed: u64,
        common_tag_pool_size: ConfRange<u8>,
        tags_per_msg: ConfRange<u8>,
        tag_key_length: ConfRange<u8>,
        tag_value_length: ConfRange<u8>,
        str_pool: Rc<strings::Pool>,
        num_tagsets: usize,
    ) -> Self {
        let internal_rng = RefCell::new(SmallRng::seed_from_u64(seed));
        let mut rng = internal_rng.borrow_mut();
        let num_common_tags = common_tag_pool_size.sample(&mut *rng);
        let common_tag_pool = (0..num_common_tags)
            .map(|_| generate_single_tag(&mut *rng, tag_key_length, tag_value_length, &str_pool))
            .collect();

        drop(rng);
        Generator {
            seed: Cell::new(seed),
            internal_rng,
            common_tag_pool,
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
        let mut tagset: Vec<String> = Vec::new();

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
            let prob: f32 = OpenClosed01.sample(&mut *rng);
            let tag = if prob < 0.1 {
                self.common_tag_pool
                    .choose(&mut *rng)
                    .expect("empty pool")
                    .to_string()
            } else {
                generate_single_tag(
                    &mut *rng,
                    self.tag_key_length,
                    self.tag_value_length,
                    &self.str_pool,
                )
            };

            tagset.push(tag);
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
                ConfRange::Inclusive{min: 1, max: 20},
                ConfRange::Inclusive{min: 0, max: tags_per_msg_max},
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
