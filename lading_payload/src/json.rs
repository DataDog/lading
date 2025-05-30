//! JSON payload.

use std::io::Write;

use rand::{Rng, distr::StandardUniform, prelude::Distribution, seq::IndexedRandom};

use crate::Error;

use super::Generator;

const SIZES: [usize; 13] = [0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048];

/// A simplistic 'Payload' structure without self-reference
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Member {
    /// A u64. Its name has no meaning.
    pub(crate) id: u64,
    /// A u64. Its name has no meaning.
    pub(crate) name: u64,
    /// A u16. Its name has no meaning.
    pub(crate) seed: u16,
    /// A variable length array of bytes. Its name has no meaning.
    pub(crate) byte_parade: Vec<u8>,
}

impl Distribution<Member> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> Member
    where
        R: Rng + ?Sized,
    {
        let max = SIZES.choose(rng).expect("failed to choose size");

        Member {
            id: rng.random(),
            name: rng.random(),
            seed: rng.random(),
            byte_parade: rng.sample_iter(StandardUniform).take(*max).collect(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
/// A JSON payload
pub struct Json;

impl<'a> Generator<'a> for Json {
    type Output = Member;
    type Error = Error;

    fn generate<R>(&'a self, rng: &mut R) -> Result<Self::Output, Error>
    where
        R: rand::Rng + ?Sized,
    {
        Ok(rng.random())
    }
}

impl crate::Serialize for Json {
    fn to_bytes<W, R>(&mut self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        let mut bytes_remaining = max_bytes;

        loop {
            let member = self.generate(&mut rng);
            let encoding = serde_json::to_string(&member?)?;
            let line_length = encoding.len() + 1; // add one for the newline

            match bytes_remaining.checked_sub(line_length) {
                Some(remainder) => {
                    writeln!(writer, "{encoding}")?;
                    bytes_remaining = remainder;
                }
                None => break,
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;
    use rand::{SeedableRng, rngs::SmallRng};

    use super::Member;
    use crate::{Json, Serialize};

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let rng = SmallRng::seed_from_u64(seed);
            let mut json = Json;

            let mut bytes = Vec::with_capacity(max_bytes);
            json.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");
            assert!(bytes.len() <= max_bytes);
        }
    }

    // We want to know that every payload produced by this type actually
    // deserializes as json, is not truncated etc.
    proptest! {
        #[test]
        fn every_payload_deserializes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let rng = SmallRng::seed_from_u64(seed);
            let mut json = Json;

            let mut bytes: Vec<u8> = Vec::with_capacity(max_bytes);
            json.to_bytes(rng, max_bytes, &mut bytes).expect("failed to convert to bytes");

            let payload = std::str::from_utf8(&bytes).expect("failed to convert from utf-8 to str");
            for msg in payload.lines() {
                let _members: Member = serde_json::from_str(msg).expect("failed to deserialize from str");
            }
        }
    }
}
