//! Tag generation for OpenTelemetry metric payloads
use std::{cmp, rc::Rc};

use super::templates::GeneratorError;
use crate::{Error, Generator, common::config::ConfRange, common::strings::Pool};
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use prost::Message;

#[derive(Debug, Clone)]
pub(crate) struct TagGenerator {
    inner: crate::common::tags::Generator,
}

// smallest useful protobuf, determined by experimentation and enforced in
// smallest_kv_protobuf test
const SMALLEST_KV_PROTOBUF: usize = 10;

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

fn varint_len(v: usize) -> usize {
    let mut v = v;
    let mut n = 1;
    while v > 0x7f {
        v >>= 7;
        n += 1;
    }
    n
}

fn overhead(v: usize) -> usize {
    // overhead in a repeated field is per-item, so:
    //
    // [tag-byte] [varint-length] [kv-bytes…]
    varint_len(v) + 1 + v
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
        let mut inner_budget = *budget;

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
            if inner_budget < SMALLEST_KV_PROTOBUF {
                break;
            }

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

            let required_bytes = overhead(kv.encoded_len());

            match inner_budget.cmp(&required_bytes) {
                cmp::Ordering::Equal | cmp::Ordering::Greater => {
                    attributes.push(kv);
                    inner_budget -= required_bytes;
                }
                cmp::Ordering::Less => {
                    if attributes.is_empty() {
                        return Err(Self::Error::SizeExhausted);
                    }
                    break;
                }
            }
        }

        *budget = inner_budget;
        Ok(attributes)
    }
}

#[cfg(test)]
mod test {
    use super::{SMALLEST_KV_PROTOBUF, overhead};
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use prost::Message;

    #[test]
    fn smallest_kv_protobuf() {
        let kv = KeyValue {
            key: String::from("k"),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(String::from("v"))),
            }),
        };

        let encoded_size = overhead(kv.encoded_len());

        assert!(
            encoded_size == SMALLEST_KV_PROTOBUF,
            "Minimal useful request size ({encoded_size}) should be == SMALLEST_KV_PROTOBUF ({SMALLEST_KV_PROTOBUF})"
        );
    }
}
