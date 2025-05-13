//! Tag generation for OpenTelemetry metric payloads
use std::rc::Rc;

use super::templates::GeneratorError;
use crate::{Error, Generator, common::config::ConfRange, common::strings::Pool};
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use prost::Message;

#[derive(Debug, Clone)]
pub(crate) struct TagGenerator {
    inner: crate::common::tags::Generator,
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
        let inner = crate::common::tags::Generator::new(
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

impl<'a> crate::SizedGenerator<'a> for TagGenerator {
    type Output = Vec<KeyValue>;
    type Error = GeneratorError;

    fn generate<R>(
        &'a mut self,
        rng: &mut R,
        budget: &mut usize,
    ) -> Result<Self::Output, Self::Error>
    where
        R: rand::Rng + ?Sized,
    {
        // NOTE that the rng must be passed to the inner generator but is not
        // used by that generator: the generator maintains its own rng. It's a
        // quirk of the trait.
        //
        // However DO NOT use this function's `rng` argument in any regard.
        // Arguably this is a code smell and we need two traits.
        let tagset = self
            .inner
            .generate(rng)
            .map_err(|_| Self::Error::StringGenerate)?;
        let mut attributes = Vec::<KeyValue>::with_capacity(tagset.len());

        for tag in tagset {
            let key = self
                .inner
                .using_handle(tag.key)
                .ok_or(Self::Error::StringGenerate)?;
            let val = self
                .inner
                .using_handle(tag.value)
                .ok_or(Self::Error::StringGenerate)?;

            let kv = KeyValue {
                key: String::from(key),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(String::from(val))),
                }),
            };

            // NOTE when this is encoded by protobuf the Vec will have a
            // variable length associated with it which we DO NOT consider here.
            let required_bytes = kv.encoded_len();
            let diff = budget.saturating_sub(required_bytes);

            if diff > 0 {
                attributes.push(kv);
                *budget = diff;
            } else if attributes.is_empty() {
                return Err(Self::Error::SizeExhausted);
            }
        }

        Ok(attributes)
    }
}
