use arbitrary::Unstructured;
use rand::{distributions::DistString, seq::SliceRandom, Rng};

use super::Generator;

const SIZES: [usize; 12] = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048];
const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789().,";
#[allow(clippy::cast_possible_truncation)]
const CHARSET_LEN: u8 = CHARSET.len() as u8;

impl DistString for AsciiStr {
    fn append_string<R: Rng + ?Sized>(&self, rng: &mut R, s: &mut String, len: usize) {
        s.reserve(4 * len); // max size of an `AsciiChar`
        s.extend(
            CHARSET
                .choose_multiple(rng, len)
                .map(|c| unsafe { char::from_u32_unchecked(u32::from(*c)) }),
        );
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct AsciiStr {
    bytes: Vec<u8>,
}

#[derive(Debug, PartialEq, Default)]
pub(crate) struct AsciiString {
    max_length: u16,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum Error {}

impl Generator<String, Error> for AsciiString {
    fn generate<R>(&self, rng: &mut R) -> Result<String, Error>
    where
        R: rand::Rng + ?Sized,
    {
        let len: usize = rng.gen_range(1..self.max_length) as usize; // todo
        let total_bytes = 4 * len; // max size of an `char` times length
        let mut s = String::with_capacity(total_bytes);
        s.reserve(total_bytes);
        s.extend(
            CHARSET
                .choose_multiple(rng, len)
                .map(|c| unsafe { char::from_u32_unchecked(u32::from(*c)) }),
        );
        Ok(s)
    }
}

/// -----

impl AsciiStr {
    pub(crate) fn as_str(&self) -> &str {
        // Safety: given that CHARSET is where we derive members from
        // `self.bytes` is always valid UTF-8.
        unsafe { std::str::from_utf8_unchecked(&self.bytes) }
    }
}

impl<'a> arbitrary::Arbitrary<'a> for AsciiStr {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let choice: u8 = u.arbitrary()?;
        let size = SIZES[(choice as usize) % SIZES.len()];
        let mut bytes: Vec<u8> = vec![0; size];
        u.fill_buffer(&mut bytes)?;
        bytes
            .iter_mut()
            .for_each(|item| *item = CHARSET[(*item % CHARSET_LEN) as usize]);
        Ok(Self { bytes })
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        (1, Some(2048))
    }
}
