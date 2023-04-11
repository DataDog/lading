use std::{collections::HashMap, ops::Range};

use rand::seq::SliceRandom;

use crate::payload::{self, dogstatsd::random_strings};

pub(crate) type Tags = HashMap<String, String>;

pub(crate) struct Generator {
    pub(crate) key_range: Range<usize>,
    pub(crate) max_values: usize,
}

impl Generator {
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
        let max_keys = self.key_range.end;

        let tag_keys = random_strings(max_keys, &mut rng);
        let tag_values = random_strings(self.max_values, &mut rng);

        let total_keys = rng.gen_range(self.key_range.clone());
        let mut tags = HashMap::new();
        for k in tag_keys.choose_multiple(&mut rng, total_keys) {
            let key = k.clone();
            if let Some(val) = tag_values.choose(&mut rng) {
                let val = val.clone();
                tags.insert(key, val);
            }
        }
        tags
    }
}
