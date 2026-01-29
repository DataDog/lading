use rand::Rng;
use rand::distr::{Distribution, Uniform};
use rand::{RngCore, distr::uniform::SampleUniform};
use std::ops::Range;
use std::rc::Rc;

use crate::common::config::ConfRange;

use super::{Handle, Pool};

pub(crate) const ALPHANUM: &[u8] =
    b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

/// A pool of strings
///
/// Our payloads need to create a number of small strings. This structures holds
/// those strings, created at `Pool` initialization. We differ from a slab or
/// `typed_arena` in that there is no insertion, only creation, and the
/// structure hands out `&str`, randomly.
#[derive(Debug, Clone)]
pub(crate) struct RandomStringPool {
    // The approach for the pool is simple. The user provides an alphabet and a
    // maximum size in memory and we stuff a `String` until that size is
    // met. The user calls for a `&str` of a certain size less than the maximum
    // size and we make a slice of that size in `inner` at a random offset.
    inner: Rc<String>,
}

impl RandomStringPool {
    /// Create a new instance of `Pool` with the default alpha-numeric character
    /// set of size `bytes`.
    pub(crate) fn with_size<R>(rng: &mut R, bytes: usize) -> Self
    where
        R: rand::Rng + ?Sized,
    {
        assert!(u32::try_from(bytes).is_ok());
        Self::with_size_and_alphabet(rng, bytes, ALPHANUM)
    }

    /// Create a new instance of `RandomStringPool` with the provided `alphabet` and set of
    /// size `bytes`.
    ///
    /// User should supply an alphabet of ASCII characters.
    #[inline]
    pub(crate) fn with_size_and_alphabet<R>(rng: &mut R, bytes: usize, alphabet: &[u8]) -> Self
    where
        R: rand::Rng + ?Sized,
    {
        let mut inner = String::new();

        let mut idx: usize = rng.random::<u32>() as usize;
        let cap = alphabet.len();

        if !alphabet.is_empty() {
            inner.reserve(bytes);
            for _ in 0..bytes {
                inner.push(unsafe {
                    let c = alphabet[idx % cap];
                    idx = idx.wrapping_add(rng.random::<u32>() as usize);
                    // Safety: `chars` is not empty so choose will never return
                    // None and the values passed in `alphabet` will always be
                    // valid.
                    char::from_u32_unchecked(u32::from(c))
                });
            }
        }

        Self {
            inner: Rc::new(inner),
        }
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
        let lower_idx = rng.random_range(0..max_lower_idx);
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
        let bytes: usize = rng.random_range(bytes_range).into();
        self.of_size(rng, bytes)
    }
}

impl Pool for RandomStringPool {
    /// Return a `&str` from the interior storage with size `bytes`. Result will
    /// be `None` if the request cannot be satisfied.
    fn of_size_with_handle<'a>(
        &'a self,
        rng: &mut dyn RngCore,
        bytes: usize,
    ) -> Option<(&'a str, Handle)> {
        if bytes >= self.inner.len() {
            return None;
        }

        let max_lower_idx = self.inner.len() - bytes;
        let dist = Uniform::new(0, max_lower_idx).expect("failed to create uniform distribution");
        let lower_idx: usize = dist.sample(rng);
        let upper_idx: usize = lower_idx + bytes;

        Some((
            &self.inner[lower_idx..upper_idx],
            Handle::PosAndLength(
                lower_idx
                    .try_into()
                    .expect("must fit into u32 by construction"),
                bytes.try_into().expect("must fit in u32 by construction"),
            ),
        ))
    }

    #[inline]
    fn using_handle(&self, handle: Handle) -> Option<&str> {
        let (offset, length) = handle
            .as_pos_and_length()
            .expect("handle for random string pool should be a random string pool handle");
        let offset = offset as usize;
        let length = length as usize;
        if offset + length <= self.inner.len() {
            let str = &self.inner[offset..offset + length];
            return Some(str);
        }

        None
    }
}

#[inline]
/// Generate a total number of strings randomly chosen from the range `min_max` with a maximum length
/// per string of `max_length`.
pub(crate) fn random_strings_with_length<R>(
    pool: &RandomStringPool,
    min_max: Range<usize>,
    max_length: u16,
    rng: &mut R,
) -> Vec<String>
where
    R: Rng + ?Sized,
{
    let total = rng.random_range(min_max);
    let length_range = ConfRange::Inclusive {
        min: 1,
        max: max_length,
    };

    random_strings_with_length_range(pool, total, length_range, rng)
}

#[inline]
/// Generate a `total` number of strings with a maximum length per string of
/// `max_length`.
pub(crate) fn random_strings_with_length_range<R>(
    pool: &RandomStringPool,
    total: usize,
    length_range: ConfRange<u16>,
    mut rng: &mut R,
) -> Vec<String>
where
    R: Rng + ?Sized,
{
    let mut buf = Vec::with_capacity(total);
    for _ in 0..total {
        let sz = length_range.sample(&mut rng) as usize;
        buf.push(String::from(
            pool.of_size(&mut rng, sz)
                .expect("failed to generate string"),
        ));
    }
    buf
}
