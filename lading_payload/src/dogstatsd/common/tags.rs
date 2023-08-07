use std::ops::Range;

use crate::payload::{self, dogstatsd::AsciiString};

use crate::dogstatsd::random_strings_with_length;

pub(crate) type Tags = HashMap<String, String>;

pub(crate) struct Generator {
    pub(crate) num_tagsets: usize,
    pub(crate) tags_per_msg_range: Range<usize>,
    pub(crate) max_length: u16,
}

impl Generator {
    pub(crate) fn new(key_range: Range<usize>, max_values: usize) -> Self {
        Self {
            key_range,
            max_values,
        }
    }
}

impl crate::Generator<Tags> for Generator {
    fn generate<R>(&self, mut rng: &mut R) -> Tags
    where
        R: rand::Rng + ?Sized,
    {
        let tags_per_msg_range: Range<usize> = self.tags_per_msg_range.start.try_into().unwrap()
            ..self.tags_per_msg_range.end.try_into().unwrap();

        let mut tagsets: Vec<Tagset> = Vec::with_capacity(self.num_tagsets);
        for _ in 0..self.num_tagsets {
            let tags_per_msg_range: Range<usize> = tags_per_msg_range.start.try_into().unwrap()
                ..tags_per_msg_range.end.try_into().unwrap();

            let num_tags_for_this_msg = rng.gen_range(tags_per_msg_range);
            let mut tagset = Vec::with_capacity(num_tags_for_this_msg);
            for _ in 0..num_tags_for_this_msg {
                let mut tag = AsciiString::with_maximum_length(self.max_length).generate(rng);
                let tag_value = AsciiString::with_maximum_length(self.max_length).generate(rng);
                tag.push_str(":");
                tag.push_str(&tag_value);
                tagset.push(tag)
            }
            tagsets.push(tagset)
        }
        tagsets
    }
}
