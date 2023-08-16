pub(crate) mod strings;

use crate::Generator;

const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

#[derive(Debug, PartialEq)]
pub(crate) struct AsciiString {
    min_length: u16,
    max_length: u16,
}

impl Default for AsciiString {
    fn default() -> Self {
        Self {
            min_length: 1,
            max_length: 16,
        }
    }
}

impl Generator<String> for AsciiString {
    fn generate<R>(&self, rng: &mut R) -> String
    where
        R: rand::Rng + ?Sized,
    {
        let len: usize = rng.gen_range(self.min_length..self.max_length) as usize;
        let mut s = String::new();
        s.reserve(len);
        for _ in 0..len {
            let idx = rng.gen_range(0..CHARSET.len());
            s.push(unsafe { char::from_u32_unchecked(u32::from(CHARSET[idx])) });
        }
        s
    }
}
