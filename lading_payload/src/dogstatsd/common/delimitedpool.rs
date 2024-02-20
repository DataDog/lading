//! Code for the quick creation of randomize strings

const ALPHANUM: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

/// A pool of strings with a specific delimeter spaced throughout the stream.
/// `&str` yielded will always contain a single delimiter.
#[derive(Debug, Clone)]
pub struct DelimitedPool {
    delimiter: char,
    spacing: usize,
    inner: String,
}

/// Opaque 'str' handle for the pool
pub type Handle = (usize, usize);

impl DelimitedPool {
    /// Create a new instance of `Pool` with the underlying pool being of size `bytes`
    /// and the `delimiter` being present at least once every `spacing` bytes. // todo spacing should be a confrange probs
    /// The `delimiter` is a single ASCII character.
    pub fn new<R>(rng: &mut R, bytes: usize, delimiter: char, spacing: usize) -> Self
    where
        R: rand::Rng + ?Sized,
    {
        let mut inner = String::new();

        let mut idx: usize = rng.gen();
        let cap = ALPHANUM.len();

        if !ALPHANUM.is_empty() {
            inner.reserve(bytes);
            for i in 0..bytes {
                if i % spacing == 0 {
                    inner.push(delimiter);
                } else {
                    inner.push(unsafe {
                        let c = ALPHANUM[idx % cap];
                        idx = idx.wrapping_add(rng.gen());
                        // Safety: `chars` is not empty so choose will never return
                        // None and the values passed in `alphabet` will always be
                        // valid.
                        char::from_u32_unchecked(u32::from(c))
                    });
                }
            }
        }

        Self {
            delimiter,
            spacing,
            inner,
        }
    }

    /// Return a `&str` from the interior storage containing delimiter. Result will
    /// be `None` if the request cannot be satisfied.
    pub fn of<'a, R>(&'a self, rng: &mut R) -> Option<(&'a str, Handle)>
    where
        R: rand::Rng + ?Sized,
    {
        let max_lower_idx = self.inner.len() - self.spacing;
        let lower_idx = rng.gen_range(0..max_lower_idx);
        let is_delimiter = |idx: usize| self.inner.as_bytes()[idx] == self.delimiter as u8;
        // start at random index, if its a delimiter, move to the next non-delimiter
        let mut str_start = lower_idx;
        while is_delimiter(str_start) {
            str_start += 1;
        }
        // end at the next delimiter for simplicity
        let mut str_end = str_start + 1;
        while !is_delimiter(str_end) {
            str_end += 1;
        }
        if str_end + self.spacing < self.inner.len() {
            str_end += self.spacing;
        }

        let str = &self.inner[str_start..str_end];
        Some((str, (str_start, str.len())))
    }

    /// Given an opaque handle returned from `of`, return the &str it represents
    #[must_use]
    pub fn from_handle(&self, handle: Handle) -> Option<&str> {
        let (offset, length) = handle;
        if offset + length < self.inner.len() {
            let str = &self.inner[offset..offset + length];
            Some(str)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;

    use super::DelimitedPool;
    use rand::{rngs::SmallRng, SeedableRng};

    #[test]
    fn simple_delimited_pool() {
        let max_bytes = 10_000_usize;
        let mut rng = SmallRng::seed_from_u64(123_456_u64);

        let pool = DelimitedPool::new(&mut rng, max_bytes, ':', 10);
        if let Some(s) = pool.of(&mut rng) {
            let (s, handle) = s;
            let delimiter_count = s.chars().filter(|c| *c == ':').count();
            assert_eq!(delimiter_count, 1);
        }
    }

    // Ensure that delimited string always returns a string with exactly 1 delimiter
    proptest! {
        #[test]
        fn delimited_pool(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);

            let pool = DelimitedPool::new(&mut rng, max_bytes, ':', 10);
            if let Some(s) = pool.of(&mut rng) {
                let (s, handle) = s;
                let delimiter_count = s.chars().filter(|c| *c == ':').count();
                assert_eq!(delimiter_count, 1);
            }
        }
    }
}
