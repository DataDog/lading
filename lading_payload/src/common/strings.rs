//! Code for the quick creation of randomize strings

use std::ops::Range;

use rand::{distributions::uniform::SampleUniform, seq::SliceRandom};

const ALPHANUM: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

/// A pool of strings
///
/// Our payloads need to create a number of small strings. This structures holds
/// those strings, created at `Pool` initialization. We differ from a slab or
/// `typed_arena` in that there is no insertion, only creation, and the
/// structure hands out `&str`, randomly.
#[derive(Debug, Clone)]
pub(crate) struct Pool {
    // The approach for the pool is simple. The user provides an alphabet and a
    // maximum size in memory and we stuff a `String` until that size is
    // met. The user calls for a `&str` of a certain size less than the maximum
    // size and we make a slice of that size in `inner` at a random offset.
    inner: String,
}

impl Pool {
    /// Create a new instance of `Pool` with the default alpha-numeric character
    /// set of size `bytes`.
    pub(crate) fn with_size<R>(rng: &mut R, bytes: usize) -> Self
    where
        R: rand::Rng + ?Sized,
    {
        Self::with_size_and_alphabet(rng, bytes, ALPHANUM)
    }

    /// Create a new instance of `Pool` with the provided `alphabet` and set of
    /// size `bytes`.
    ///
    /// User should supply an alphabet of ASCII characters.
    pub(crate) fn with_size_and_alphabet<R>(rng: &mut R, bytes: usize, alphabet: &[u8]) -> Self
    where
        R: rand::Rng + ?Sized,
    {
        let mut inner = String::new();

        let mut idx: usize = rng.gen();
        let cap = alphabet.len();

        if !alphabet.is_empty() {
            inner.reserve(bytes);
            for _ in 0..bytes {
                inner.push(unsafe {
                    let c = alphabet[idx % cap];
                    idx = idx.wrapping_add(rng.gen());
                    // Safety: `chars` is not empty so choose will never return
                    // None and the values passed in `alphabet` will always be
                    // valid.
                    char::from_u32_unchecked(u32::from(c))
                });
            }
        }

        Self { inner }
    }

    /// Return a `&str` from the interior storage with size `bytes`. Result will
    /// be `None` if the request cannot be satisfied.
    pub(crate) fn of_size<'a, R>(&'a self, rng: &mut R, bytes: usize) -> Option<&'a str>
    where
        R: rand::Rng + ?Sized,
    {
        if bytes >= self.inner.len() {
            return None;
        }

        let max_lower_idx = self.inner.len() - bytes;
        let lower_idx = rng.gen_range(0..max_lower_idx);
        let upper_idx = lower_idx + bytes;

        Some(&self.inner[lower_idx..upper_idx])
    }

    /// Return a `&str` from the interior storage with size selected from `bytes_range`. Result will
    /// be `None` if the request cannot be satisfied.
    pub(crate) fn of_size_range<'a, R, T>(
        &'a self,
        rng: &mut R,
        bytes_range: Range<T>,
    ) -> Option<&'a str>
    where
        R: rand::Rng + ?Sized,
        T: Into<usize> + Copy + PartialOrd + SampleUniform,
    {
        let bytes: usize = rng.gen_range(bytes_range).into();
        self.of_size(rng, bytes)
    }
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;

    use super::{Pool, ALPHANUM};
    use rand::{rngs::SmallRng, SeedableRng};

    // Ensure that no returned string ever has a non-alphabet character.
    proptest! {
        #[test]
        fn no_nonalphabet_char(seed: u64, max_bytes: u16, of_size_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let of_size_bytes = of_size_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);

            let pool = Pool::with_size_and_alphabet(&mut rng, max_bytes, ALPHANUM);
            if let Some(s) = pool.of_size(&mut rng, of_size_bytes) {
                for c in s.bytes() {
                    assert!(ALPHANUM.contains(&c));
                }
            }
        }
    }

    // Ensure that no returned string is ever larger or smaller than of_size_bytes.
    proptest! {
        #[test]
        fn no_size_mismatch(seed: u64, max_bytes: u16, of_size_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let of_size_bytes = of_size_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);

            let pool = Pool::with_size_and_alphabet(&mut rng, max_bytes, ALPHANUM);
            if let Some(s) = pool.of_size(&mut rng, of_size_bytes) {
                assert!(s.len() == of_size_bytes);
            }
        }
    }

    // Ensure that of_size only returns None if the request is greater than or
    // equal to the interior size.
    proptest! {
        #[test]
        fn return_none_condition(seed: u64, max_bytes: u16, of_size_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let of_size_bytes = of_size_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);

            let pool = Pool::with_size_and_alphabet(&mut rng, max_bytes, ALPHANUM);
            if pool.of_size(&mut rng, of_size_bytes).is_none() {
                assert!(of_size_bytes >= max_bytes);
            }
        }
    }
}
