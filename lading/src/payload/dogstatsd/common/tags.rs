use std::{collections::HashMap, ops::Range};

use rand::seq::SliceRandom;

use crate::payload::{self, dogstatsd::random_strings_with_length};

pub(crate) type Tags = HashMap<String, String>;

pub(crate) struct Generator {
    pub(crate) key_range: Range<usize>,
    pub(crate) max_values: usize,
}

impl Generator {
    // key_range is tag_keys_min..tag_keys_max
    // max_values is static. Value is 'max_values_per_tag_set' == 512
    pub(crate) fn new(key_range: Range<usize>, max_values: usize) -> Self {
        Self {
            key_range,
            max_values,
        }
    }
}

impl payload::Generator<Tags> for Generator {
    fn generate<R>(&self, mut rng: &mut R) -> Tags
    where
        R: rand::Rng + ?Sized,
    {
        // generates key_range number of tag names/keys with max string length of 64
        let tag_keys = random_strings_with_length(self.key_range.clone(), 64, &mut rng);
        // Generates max_values (512) strings with max string length of 32
        let tag_values = random_strings_with_length(0..self.max_values, 32, &mut rng);

        // the number of tags for this metric will be a random value chosen from
        // tag_keys_min..tag_keys_max
        let total_keys = rng.gen_range(self.key_range.clone());
        let mut tags = HashMap::new();
        for k in tag_keys.choose_multiple(&mut rng, total_keys) {
            let key = k.clone();
            // tag_values.choose will return None if tag_values has been exhausted.
            // This implies that if tag_keys_max is greater than 512, we'll see repeated tag values
            // repeated values are not really a big deal.

            if let Some(val) = tag_values.choose(&mut rng) {
                let val = val.clone();
                tags.insert(key, val);
            }
        }
        // final result is a map with size set to a random value between `tag_keys_min` and `tag_keys_max`
        tags
    }
}
