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
    pub(crate) max_length: u16,
}

// https://docs.datadoghq.com/getting_started/tagging/#define-tags
impl crate::Generator<Tagsets> for Generator {
    fn generate<R>(&self, rng: &mut R) -> Tagsets
    where
        R: rand::Rng + ?Sized,
    {
        let tags_per_msg_range: Range<usize> =
            self.tags_per_msg_range.start..self.tags_per_msg_range.end;

        let mut tagsets: Vec<Tagset> = Vec::with_capacity(self.num_tagsets);
        for _ in 0..self.num_tagsets {
            let tags_per_msg_range: Range<usize> = tags_per_msg_range.start..tags_per_msg_range.end;

            let num_tags_for_this_msg = rng.gen_range(tags_per_msg_range);
            let mut tagset = Vec::with_capacity(num_tags_for_this_msg);
            for _ in 0..num_tags_for_this_msg {
                let mut tag = AsciiString::with_maximum_length(self.max_length).generate(rng);
                let tag_value = AsciiString::with_maximum_length(self.max_length).generate(rng);
                tag.push(':');
                tag.push_str(&tag_value);
                tagset.push(tag);
            }
            tagsets.push(tagset);
        }
        tagsets
    }
}
