//! Code for the quick creation of randomize strings

use enum_dispatch::enum_dispatch;
use rand::seq::IndexedRandom;

mod random_string_pool;
mod string_list_pool;

pub(crate) use random_string_pool::{
    RandomStringPool, random_strings_with_length, random_strings_with_length_range,
};
pub(crate) use string_list_pool::StringListPool;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Handle {
    /// Handle type for Random String Pool - stores position and length
    PosAndLength(u32, u32),
    /// Handle type for String List Pool - stores the index into the string list
    Index(usize),
}

impl Handle {
    pub(crate) fn as_pos_and_length(self) -> Option<(u32, u32)> {
        match self {
            Self::PosAndLength(pos, len) => Some((pos, len)),
            _ => None,
        }
    }

    pub(crate) fn as_index(self) -> Option<usize> {
        match self {
            Self::Index(idx) => Some(idx),
            _ => None,
        }
    }
}

impl Default for Handle {
    fn default() -> Self {
        Self::PosAndLength(0, 0)
    }
}

#[enum_dispatch]
pub(crate) trait Pool {
    fn of_size_with_handle<'a, R>(&'a self, rng: &mut R, bytes: usize) -> Option<(&'a str, Handle)>
    where
        R: rand::Rng + ?Sized;

    fn using_handle(&self, handle: Handle) -> Option<&str>;
}

#[enum_dispatch(Pool)]
#[derive(Debug, Clone)]
pub(crate) enum PoolKind {
    RandomStringPool(RandomStringPool),
    StringListPool(StringListPool),
}

pub(crate) fn choose_or_not_ref<'a, R, T>(mut rng: &mut R, pool: &'a [T]) -> Option<&'a T>
where
    R: rand::Rng + ?Sized,
{
    if rng.random() {
        pool.choose(&mut rng)
    } else {
        None
    }
}

pub(crate) fn choose_or_not_fn<R, T, F>(rng: &mut R, func: F) -> Option<T>
where
    T: Clone,
    R: rand::Rng + ?Sized,
    F: FnOnce(&mut R) -> Option<T>,
{
    if rng.random() { func(rng) } else { None }
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;

    use super::RandomStringPool;
    use super::random_string_pool::ALPHANUM;
    use rand::{SeedableRng, rngs::SmallRng};

    // Ensure that no returned string ever has a non-alphabet character.
    proptest! {
        #[test]
        fn no_nonalphabet_char(seed: u64, max_bytes: u16, of_size_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let of_size_bytes = of_size_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);

            let pool = RandomStringPool::with_size_and_alphabet(&mut rng, max_bytes, ALPHANUM);
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

            let pool = RandomStringPool::with_size_and_alphabet(&mut rng, max_bytes, ALPHANUM);
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

            let pool = RandomStringPool::with_size_and_alphabet(&mut rng, max_bytes, ALPHANUM);
            if pool.of_size(&mut rng, of_size_bytes).is_none() {
                assert!(of_size_bytes >= max_bytes);
            }
        }
    }
}
