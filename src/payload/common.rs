use rand::seq::SliceRandom;

use super::Generator;

const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789().,";

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
