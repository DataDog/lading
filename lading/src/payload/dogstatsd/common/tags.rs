use std::{collections::HashMap, ops::Range};

use rand::seq::SliceRandom;

use crate::payload::{self, dogstatsd::random_strings_with_length};

pub(crate) type Tags = HashMap<String, String>;

pub(crate) struct Generator {
    pub(crate) key_range: Range<usize>,
    tag_keys: Vec<String>,
    tag_values: Vec<String>,
}

impl Generator {
    pub(crate) fn new<R>(
        key_range: Range<usize>,
        value_range: Range<usize>,
        mut rng: &mut R,
    ) -> Self
    where
        R: rand::Rng + ?Sized,
    {
        let tag_keys = random_strings_with_length(key_range.clone(), 64, &mut rng);
        let tag_values = random_strings_with_length(value_range.clone(), 32, &mut rng);
        Self {
            tag_keys,
            tag_values,
            key_range,
        }
    }
}

impl payload::Generator<Tags> for Generator {
    fn generate<R>(&self, mut rng: &mut R) -> Tags
    where
        R: rand::Rng + ?Sized,
    {
        let total_keys = rng.gen_range(self.key_range.clone());
        let mut tags = HashMap::new();
        for k in self.tag_keys.choose_multiple(&mut rng, total_keys) {
            let key = k.clone();

            if let Some(val) = self.tag_values.choose(&mut rng) {
                let val = val.clone();
                tags.insert(key, val);
            }
        }
        // final result is a map with size set to a random value between `tag_keys_min` and `tag_keys_max`
        tags
    }
}
