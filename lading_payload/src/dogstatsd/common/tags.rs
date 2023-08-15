use std::ops::Range;

use crate::common::AsciiString;

// This represents a list of tags that will be present on a single
// dogstatsd message.
pub(crate) type Tagset = Vec<String>;
// Multiple tagsets. Useful to generate as a batch (is it??)
pub(crate) type Tagsets = Vec<Tagset>;

pub(crate) struct Generator {
    pub(crate) num_tagsets: usize,
    pub(crate) tags_per_msg_range: Range<usize>,
    pub(crate) tag_key_length_range: Range<u16>,
    pub(crate) tag_value_length_range: Range<u16>,
}

// https://docs.datadoghq.com/getting_started/tagging/#define-tags
impl crate::Generator<Tagsets> for Generator {
    fn generate<R>(&self, rng: &mut R) -> Tagsets
    where
        R: rand::Rng + ?Sized,
    {
        let tags_per_msg_range = self.tags_per_msg_range.clone();

        let tag_key_generator = AsciiString::with_length_range(self.tag_key_length_range.clone());
        let tag_value_generator =
            AsciiString::with_length_range(self.tag_value_length_range.clone());

        let mut tagsets: Vec<Tagset> = Vec::with_capacity(self.num_tagsets);
        for _ in 0..self.num_tagsets {
            let tags_per_msg_range = tags_per_msg_range.clone();

            let num_tags_for_this_msg = rng.gen_range(tags_per_msg_range);
            let mut tagset = Vec::with_capacity(num_tags_for_this_msg);
            for _ in 0..num_tags_for_this_msg {
                let mut tag = tag_key_generator.generate(rng);
                let tag_value = tag_value_generator.generate(rng);
                tag.push(':');
                tag.push_str(&tag_value);
                tagset.push(tag);
            }
            tagsets.push(tagset);
        }
        tagsets
    }
}
