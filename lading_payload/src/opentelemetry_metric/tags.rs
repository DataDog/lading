//! Tag generation for OpenTelemetry metric payloads
use std::rc::Rc;

use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};

use crate::{
    Error, common::config::ConfRange, common::strings::Pool,
    common::tags::Generator as InnerGenerator,
};

#[derive(Debug, Clone)]
pub(crate) struct TagGenerator {
    inner: InnerGenerator,
}

impl TagGenerator {
    /// Creates a new tag generator
    ///
    /// # Errors
    /// - If `tags_per_msg` is invalid or exceeds the maximum
    /// - If `tag_length` is invalid or has minimum value less than 3
    /// - If `unique_tag_probability` is not between 0.10 and 1.0
    pub(crate) fn new(
        seed: u64,
        tags_per_msg: ConfRange<u8>,
        tag_length: ConfRange<u16>,
        num_tagsets: usize,
        str_pool: Rc<Pool>,
        unique_tag_probability: f32,
    ) -> Result<Self, Error> {
        let inner = InnerGenerator::new(
            seed,
            tags_per_msg,
            tag_length,
            num_tagsets,
            str_pool,
            unique_tag_probability,
        )
        .map_err(|_| Error::StringGenerate)?;
        Ok(TagGenerator { inner })
    }
}

impl<'a> crate::Generator<'a> for TagGenerator {
    type Output = Vec<KeyValue>;
    type Error = Error;

    fn generate<R>(&'a self, rng: &mut R) -> Result<Self::Output, Self::Error>
    where
        R: rand::Rng + ?Sized,
    {
        let tagset = self
            .inner
            .generate(rng)
            .map_err(|_| Error::StringGenerate)?;
        let mut attributes = Vec::with_capacity(tagset.len());

        for tag in tagset {
            let key = self
                .inner
                .using_handle(tag.key)
                .ok_or(Error::StringGenerate)?;
            let val = self
                .inner
                .using_handle(tag.value)
                .ok_or(Error::StringGenerate)?;

            attributes.push(KeyValue {
                key: String::from(key),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(String::from(val))),
                }),
            });
        }

        Ok(attributes)
    }
}
