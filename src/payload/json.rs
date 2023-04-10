use std::io::Write;

use rand::{distributions::Standard, prelude::Distribution, seq::SliceRandom, Rng};

use crate::payload::{Error, Serialize};

use super::Generator;

const SIZES: [usize; 13] = [0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048];

/// A simplistic 'Payload' structure without self-reference
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Member {
    /// A u64. Its name has no meaning.
    pub(crate) id: u64,
    /// A u64. Its name has no meaning.
    pub(crate) name: u64,
    /// A u16. Its name has no meaning.
    pub(crate) seed: u16,
    /// A variable length array of bytes. Its name has no meaning.
    pub(crate) byte_parade: Vec<u8>,
}

impl Distribution<Member> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Member
    where
        R: Rng + ?Sized,
    {
        let max = SIZES.choose(rng).unwrap();

        Member {
            id: rng.gen(),
            name: rng.gen(),
            seed: rng.gen(),
            byte_parade: rng.sample_iter(Standard).take(*max).collect(),
        }
    }
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub(crate) struct Json;

impl Generator<Member> for Json {
    fn generate<R>(&self, rng: &mut R) -> Member
    where
        R: rand::Rng + ?Sized,
    {
        rng.gen()
    }
}

impl Serialize for Json {
    fn to_bytes<W, R>(&self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        let mut bytes_remaining = max_bytes;

        loop {
            let member = self.generate(&mut rng);
            let encoding = serde_json::to_string(&member)?;
            let line_length = encoding.len() + 1; // add one for the newline

            println!("[{max_bytes}] {line_length} {bytes_remaining}");
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
    use rand::{rngs::SmallRng, SeedableRng};

    use super::Member;
    use crate::payload::{Json, Serialize};

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let rng = SmallRng::seed_from_u64(seed);
            let json = Json::default();

            let mut bytes = Vec::with_capacity(max_bytes);
            json.to_bytes(rng, max_bytes, &mut bytes).unwrap();
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
            let json = Json::default();

            let mut bytes: Vec<u8> = Vec::with_capacity(max_bytes);
            json.to_bytes(rng, max_bytes, &mut bytes).unwrap();

            let payload = std::str::from_utf8(&bytes).unwrap();
            for msg in payload.lines() {
                let _members: Member = serde_json::from_str(msg).unwrap();
            }
        }
    }
}
