//! Code for the quick creation of randomize strings

use crate::dogstatsd::ConfRange;
use std::cell::Cell;

const ALPHANUM: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
const MAX_POOL_SIZE: usize = 100_000_000_usize;

/// A pool of strings with a specific delimeter spaced throughout the stream.
/// `&str` yielded will always contain a single delimiter.
#[derive(Debug, Clone)]
pub struct DelimitedPool {
    delimiter: char,
    desired_str_size: ConfRange<u16>,
    inner: String,
    ptr: Cell<usize>,
}

/// Error type for `DelimitedPool`
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Invalid construction
    #[error("Invalid construction: {0}")]
    InvalidConstruction(String),
}

/// Opaque 'str' handle for the pool
pub type Handle = (usize, usize);

impl DelimitedPool {
    /// Create a new instance of `Pool` with the underlying pool being of size `bytes`.
    /// The `delimiter` will be spaced throughout the pool such that all requests for
    /// a string will contain a single delimiter.
    /// This requires that the length of returned strings be specified up-front in `string_size`
    /// The `delimiter` is a single ASCII character.
    /// # Errors
    /// Will return an error if `bytes` is less than the length of the alphabet.
    pub fn new<R>(
        rng: &mut R,
        bytes: usize,
        delimiter: char,
        desired_str_size: ConfRange<u16>,
    ) -> Result<Self, Error>
    where
        R: rand::Rng + ?Sized,
    {
        let (desired_str_size_valid, reason) = desired_str_size.valid();
        if !desired_str_size_valid {
            let mut err = String::from("desired_str_size must be valid, invalid due to: ");
            err.push_str(reason);
            return Err(Error::InvalidConstruction(err));
        }
        let spacing_max = desired_str_size.end();
        if desired_str_size.start() < 3 || spacing_max < 3 {
            return Err(Error::InvalidConstruction(String::from(
                "spacing must be at least 3",
            )));
        }
        if bytes < ALPHANUM.len() || bytes > MAX_POOL_SIZE {
            return Err(Error::InvalidConstruction(String::from(
                "bytes must be at least the length of the alphabet",
            )));
        }
        if bytes <= (spacing_max as usize * 2) {
            return Err(Error::InvalidConstruction(String::from(
                "total bytes must be at least twice as large as the maximum spacing",
            )));
        }

        let mut inner = String::new();

        let mut idx: usize = rng.gen();
        let cap = ALPHANUM.len();

        let mut till_next_delimiter = desired_str_size.sample(rng);
        if !ALPHANUM.is_empty() {
            inner.reserve(bytes);
            for _ in 0..bytes {
                if till_next_delimiter == 0 {
                    inner.push(delimiter);
                    till_next_delimiter = desired_str_size.sample(rng);
                } else {
                    inner.push(unsafe {
                        let c = ALPHANUM[idx % cap];
                        idx = idx.wrapping_add(rng.gen());
                        // Safety: `chars` is not empty so choose will never return
                        // None and the values passed in `alphabet` will always be
                        // valid.
                        char::from_u32_unchecked(u32::from(c))
                    });
                    till_next_delimiter -= 1;
                }
            }
        }

        Ok(Self {
            delimiter,
            desired_str_size,
            ptr: Cell::new(0),
            inner,
        })
    }
    /// Return a `&str` from the interior storage containing delimiter.
    /// Guaranteed to have at least 1 character before and 1 character after the delimiter.
    /// Result will be `None` if the request cannot be satisfied.
    pub fn of<'a, R>(&'a self, rng: &mut R) -> Option<(&'a str, Handle)>
    where
        R: rand::Rng + ?Sized,
    {
        let starting_point = self.ptr.get();
        // size may be too small to include a delimiter

        let mut delimiter_pointer = starting_point;

        let is_delimiter = |idx: usize| self.inner.as_bytes()[idx] == self.delimiter as u8;

        // First, find the next delimiter to the right
        while !is_delimiter(delimiter_pointer) {
            delimiter_pointer += 1;
        }

        let pre_delimiter = delimiter_pointer - starting_point;
        if pre_delimiter < 1 {
            // we need at least 1 char before the delimiter
            let new_idx = rng.gen_range(0..self.inner.len());
            self.ptr.set(new_idx);
            return self.of(rng);
        }
        // given a desired size, what guarantees do we currently have about pre-delimiter?
        // pre_delimiter is greater than or equal to 1
        // desired_size - pre_delimiter - 1 is the max post_delimiter len
        let size = self.desired_str_size.sample(rng) as usize;
        if size.saturating_sub(pre_delimiter).saturating_sub(1) < 2 {
            // we can't satisfy the request because we've already used up all the space allowed by 'size'
            // lets reset the pointer and try again
            // TODO is this safe? Could I enter into an infinite loop somehow?
            let new_idx = rng.gen_range(0..self.inner.len());
            self.ptr.set(new_idx);
            return self.of(rng);
        }
        let post_delimiter = size - pre_delimiter - 1;

        // End point is ideally just pre_delimiter + post_delimiter
        // however we could encounter either the end of the string or the next delimiter
        // If we encounter either of those before we reach the desired size, we should stop
        let mut ending_point = delimiter_pointer + 1;
        while !is_delimiter(ending_point)
            && ending_point < self.inner.len()
            && ending_point.saturating_sub(delimiter_pointer.saturating_add(1)) < post_delimiter
        {
            ending_point += 1;
        }

        if ending_point + 1 >= self.inner.len() {
            // we've looped, just reset
            self.ptr.set(0);
            // and try from the beginning
            return self.of(rng);
        }
        self.ptr.set(ending_point + 1);

        let ret_str = &self.inner[starting_point..ending_point];
        println!(
            "str_start: {}, delimiter_pointer: {}, str_end: {}, str: {}",
            starting_point, delimiter_pointer, ending_point, ret_str
        );
        Some((ret_str, (starting_point, ret_str.len())))
    }

    fn get_random_delimiter<R>(&self, rng: &mut R) -> usize
    where
        R: rand::Rng + ?Sized,
    {
        let max_spacing = self.desired_str_size.end() as usize;
        let max_lower_idx = self.inner.len() - max_spacing;
        let min_lower_idx = max_spacing;

        let lower_idx = rng.gen_range(min_lower_idx..max_lower_idx);
        let is_delimiter = |idx: usize| {
            idx < self.inner.len() && self.inner.as_bytes()[idx] == self.delimiter as u8
        };

        let mut delimiter_pointer = lower_idx;

        while !is_delimiter(delimiter_pointer) {
            delimiter_pointer += 1;
        }

        delimiter_pointer
    }

    fn expand_from_delimiter(&self, delimiter_pointer: usize) -> (&str, Handle) {
        // given a delimiter
        // expand left until other delimiter or start of string
        // expand right until other delimiter or end of string
        let delimiter = self.delimiter as u8;

        let mut str_start = delimiter_pointer.saturating_sub(1);
        while str_start > 0 {
            let next_str_start = str_start.saturating_sub(1);
            if self.inner.as_bytes()[next_str_start] == delimiter {
                break;
            }
            str_start = next_str_start;
        }

        let mut str_end = delimiter_pointer.saturating_add(1);
        while str_end < self.inner.len() {
            let next_str_end = str_end.saturating_add(1);
            if self.inner.as_bytes()[next_str_end] == delimiter {
                break;
            }
            str_end = next_str_end;
        }

        let str = &self.inner[str_start..str_end];
        (str, (str_start, str.len()))
    }

    /// Return a `&str` from the interior storage containing delimiter. Result will
    /// be `None` if the request cannot be satisfied.
    pub fn of_newnew<'a, R>(&'a self, rng: &mut R) -> Option<(&'a str, Handle)>
    where
        R: rand::Rng + ?Sized,
    {
        let delimiter_pointer = self.get_random_delimiter(rng);
        let (str, handle) = self.expand_from_delimiter(delimiter_pointer);
        Some((str, handle))
    }

    /// Return a `&str` from the interior storage containing delimiter. Result will
    /// be `None` if the request cannot be satisfied.
    pub fn of_old<'a, R>(&'a self, rng: &mut R) -> Option<(&'a str, Handle)>
    where
        R: rand::Rng + ?Sized,
    {
        let max_spacing = self.desired_str_size.end() as usize;
        let max_lower_idx = self.inner.len() - max_spacing;
        let min_lower_idx = max_spacing;

        let lower_idx = rng.gen_range(min_lower_idx..max_lower_idx);
        let is_delimiter = |idx: usize| {
            idx < self.inner.len() && self.inner.as_bytes()[idx] == self.delimiter as u8
        };

        let mut delimiter_pointer = lower_idx;

        // First, find the next delimiter to the right of the random index
        while !is_delimiter(delimiter_pointer) {
            delimiter_pointer += 1;
        }

        // subtract 1 to account for the fact that the delimiter is included in the string
        let current_desired_str_size = self.desired_str_size.sample(rng) as usize - 1;
        // Second, once a delimiter is found, take N chars before and M chars after
        // while respecting the bounds of the string and the next delimiter
        let n = current_desired_str_size / 2;
        // TODO i think there's an off-by-one somewhere around this delimiter crawling
        let mut str_start = delimiter_pointer - 1;
        for _ in 0..n {
            let candidate_str_start = str_start.saturating_sub(1);
            if candidate_str_start == 0 || is_delimiter(candidate_str_start) {
                // we don't want to go back this far
                // stop early.
                break;
            }
            str_start = candidate_str_start;
        }
        let m = current_desired_str_size / 2;
        let mut str_end = delimiter_pointer + 1;
        for _ in 0..m {
            let candidate_str_end = str_end.saturating_add(1);
            if candidate_str_end == self.inner.len() || is_delimiter(candidate_str_end) {
                // we don't want to go forward this far
                // stop early.
                break;
            }
            str_end = candidate_str_end;
        }

        let str = &self.inner[str_start..str_end];
        /*
        println!(
            "str_start: {}, delimiter_pointer: {}, str_end: {}, str: {}, str with 10 extra chars surrounding: {}",
            str_start, delimiter_pointer, str_end, str, &self.inner[str_start - 10..str_end + 10]
        ); */
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

    use super::{DelimitedPool, ALPHANUM, MAX_POOL_SIZE};
    use crate::dogstatsd::ConfRange;
    use rand::{rngs::SmallRng, SeedableRng};

    macro_rules! assert_str {
        ($s:expr, $pool:expr, $spacing:expr) => {
            let (s, handle) = $s;
            let s2 = $pool.from_handle(handle).expect("handle should be valid");
            assert_eq!(s, s2);

            let delimiter_indices = s
                .char_indices()
                .filter(|(_, c)| *c == ':')
                .map(|(idx, _)| idx)
                .collect::<Vec<_>>();
            assert_eq!(1, delimiter_indices.len());

            let delimiter_idx = delimiter_indices[0];

            let chars_after_delimiter = s.len() - delimiter_idx - 1;
            assert!(chars_after_delimiter >= 1);

            let chars_before_delimeter = delimiter_idx;
            assert!(chars_before_delimeter >= 1);

            assert!(s.len() >= $spacing.start() as usize);
            assert!(s.len() <= $spacing.end() as usize);
        };
    }

    #[test]
    fn simple_delimited_pool() {
        let max_bytes = 10_000_usize;
        let mut rng = SmallRng::seed_from_u64(123_456_u64);

        let spacing = ConfRange::Constant(10);
        let pool = DelimitedPool::new(&mut rng, max_bytes, ':', spacing).expect("valid pool");
        if let Some(s) = pool.of_newnew(&mut rng) {
            assert_str!(s, pool, spacing);
        }
    }

    #[test]
    fn proptest_found_case() {
        let mut rng = SmallRng::seed_from_u64(0);

        let spacing = ConfRange::Inclusive { min: 3, max: 30 };
        let pool = DelimitedPool::new(&mut rng, 62, ':', spacing).expect("valid pool");
        if let Some(s) = pool.of_newnew(&mut rng) {
            assert_str!(s, pool, spacing);
        }
    }

    #[test]
    fn proptest_found_case_2() {
        let seed = 9_659_552_839_220_940_646;
        let mut rng = SmallRng::seed_from_u64(seed);
        let max_bytes = 67_809_160;
        let min_spacing = 3_086;
        let max_spacing = 32_295;

        let spacing = ConfRange::Inclusive {
            min: min_spacing,
            max: max_spacing,
        };
        let pool = DelimitedPool::new(&mut rng, max_bytes, ':', spacing).expect("valid pool");
        if let Some(s) = pool.of_newnew(&mut rng) {
            assert_str!(s, pool, spacing);
        }
    }

    #[test]
    fn proptest_found_case_3() {
        let seed = 0;
        let mut rng = SmallRng::seed_from_u64(seed);
        let max_bytes = 57_852_848;
        let min_spacing = 3;
        let max_spacing = 4;

        let spacing = ConfRange::Inclusive {
            min: min_spacing,
            max: max_spacing,
        };
        let pool = DelimitedPool::new(&mut rng, max_bytes, ':', spacing).expect("valid pool");
        let s = pool
            .of_newnew(&mut rng)
            .expect("should have found a string");
        assert_str!(s, pool, spacing);
    }

    #[test]
    fn proptest_found_case_4() {
        let seed = 0;
        let mut rng = SmallRng::seed_from_u64(seed);
        let max_bytes = 27811;

        let spacing = ConfRange::Constant(10697);
        let pool = DelimitedPool::new(&mut rng, max_bytes, ':', spacing).expect("valid pool");
        let s = pool
            .of_newnew(&mut rng)
            .expect("should have found a string");
        assert_str!(s, pool, spacing);
    }

    #[test]
    fn proptest_found_case_5() {
        let seed = 18_403_561_312_680_147_070_u64;
        let mut rng = SmallRng::seed_from_u64(seed);
        let max_bytes = 31_427_160;
        let min_spacing = 56435;
        let max_spacing = 63199;

        let spacing = ConfRange::Inclusive {
            min: min_spacing,
            max: max_spacing,
        };
        let pool = DelimitedPool::new(&mut rng, max_bytes, ':', spacing).expect("valid pool");
        let s = pool
            .of_newnew(&mut rng)
            .expect("should have found a string");
        assert_str!(s, pool, spacing);
    }

    proptest! {
        #[test]
        fn get_random_delimiter_works(seed: u64, max_bytes in ALPHANUM.len()..MAX_POOL_SIZE, spacing in 3..u16::MAX) {
            let spacing = ConfRange::Constant(spacing);
            let mut rng = SmallRng::seed_from_u64(seed);

            if let Ok(pool) = DelimitedPool::new(&mut rng, max_bytes, ':', spacing) {
                let delimiter_idx = pool.get_random_delimiter(&mut rng);
                assert_eq!(pool.delimiter as u8, pool.inner.as_bytes()[delimiter_idx]);
            }
        }
    }

    // Ensure that delimited string always returns a string with exactly 1 delimiter
    proptest! {
        #[test]
        fn delimited_pool_constant_spacing(seed: u64, max_bytes in ALPHANUM.len()..MAX_POOL_SIZE, spacing in 3..u16::MAX) {
            let spacing = ConfRange::Constant(spacing);
            let mut rng = SmallRng::seed_from_u64(seed);

            if let Ok(pool) = DelimitedPool::new(&mut rng, max_bytes, ':', spacing) {
                if let Some(s) = pool.of_newnew(&mut rng) {
                    assert_str!(s, pool, spacing);
                }
            }
        }
    }

    // Ensure that delimited string always returns a string with exactly 1 delimiter
    proptest! {
        #[test]
        fn delimited_pool_variable_spacing(seed: u64, max_bytes in ALPHANUM.len()..MAX_POOL_SIZE, min_spacing in 3..u16::MAX, max_spacing in 3..u16::MAX) {
            let spacing = ConfRange::Inclusive{min: min_spacing, max: max_spacing};
            let mut rng = SmallRng::seed_from_u64(seed);

            if let Ok(pool) = DelimitedPool::new(&mut rng, max_bytes, ':', spacing) {
                let mut nones_found = 0;
                for _ in 0..100 {
                    match pool.of_newnew(&mut rng) {
                        Some(s) => {
                            assert_str!(s, pool, spacing);
                        },
                        None => {
                            nones_found += 1;
                        }
                    }
                }
                assert!(nones_found < 100);
            }
        }
    }
}
