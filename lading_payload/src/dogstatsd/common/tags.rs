use std::rc::Rc;

use crate::{common::strings, dogstatsd::ConfRange};

// This represents a list of tags that will be present on a single
// dogstatsd message.
pub(crate) type Tagset = Vec<String>;
// Multiple tagsets. Useful to generate as a batch (is it??)
pub(crate) type Tagsets = Vec<Tagset>;

pub(crate) struct Generator {
    pub(crate) num_tagsets: usize,
    pub(crate) tags_per_msg: ConfRange<u8>,
    pub(crate) tag_key_length: ConfRange<u8>,
    pub(crate) tag_value_length: ConfRange<u8>,
    pub(crate) str_pool: Rc<strings::Pool>,
}

// https://docs.datadoghq.com/getting_started/tagging/#define-tags
impl<'a> crate::Generator<'a> for Generator {
    type Output = Tagsets;

    fn generate<R>(&'a self, mut rng: &mut R) -> Self::Output
    where
        R: rand::Rng + ?Sized,
    {
        let mut tagsets: Vec<Tagset> = Vec::with_capacity(self.num_tagsets);
        for _ in 0..self.num_tagsets {
            let num_tags_for_this_msg = self.tags_per_msg.sample(rng) as usize;
            let mut tagset = Vec::with_capacity(num_tags_for_this_msg);
            for _ in 0..num_tags_for_this_msg {
                let mut tag = String::new();
                tag.reserve(512); // a guess, big-ish but not too big
                let key_sz = self.tag_key_length.sample(&mut rng) as usize;
                let key = self.str_pool.of_size(&mut rng, key_sz).unwrap();
                let value_sz = self.tag_value_length.sample(&mut rng) as usize;
                let value = self.str_pool.of_size(&mut rng, value_sz).unwrap();
                tag.push_str(key);
                tag.push(':');
                tag.push_str(value);
                tagset.push(tag);
            }
            tagsets.push(tagset);
        }
        tagsets
    }
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;
    use rand::{rngs::SmallRng, SeedableRng};

    use crate::dogstatsd::{strings, tags, ConfRange};
    use crate::Generator;
    use std::rc::Rc;

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    proptest! {
        #[test]
        fn generator_not_exceed_tagset_max(seed: u64, num_tagsets in 0..1_000) {
            let mut rng = SmallRng::seed_from_u64(seed);
            let num_tagsets = num_tagsets as usize;
            let pool = Rc::new(strings::Pool::with_size(&mut rng, 8_000_000));

            let generator = tags::Generator {
                num_tagsets,
                tags_per_msg: ConfRange::Inclusive{min: 0, max: 1_000},
                tag_key_length: ConfRange::Inclusive{min: 1, max: 64 },
                tag_value_length: ConfRange::Inclusive{min: 1, max: 64 },
                str_pool: pool,
            };
            let tagsets = generator.generate(&mut rng);
            assert!(tagsets.len() == num_tagsets);
        }
    }
}
