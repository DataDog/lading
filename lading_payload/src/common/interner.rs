//! String interner for compact storage of repeated strings
//!
//! This module provides a string interner that deduplicates strings and returns
//! compact handles that can be used to retrieve the original strings.

use rustc_hash::{FxHashMap, FxHasher};
use std::hash::{Hash, Hasher};
use std::{hash::BuildHasherDefault, str};

/// Handle to an interned string
pub(crate) type Handle = u32;

/// Initial capacity for string handles
const INITIAL_STRING_CAP: usize = 256;

/// Initial capacity for byte storage
const INITIAL_BYTE_CAP: usize = 16_384; // 16KB

/// Error types for interner operations
#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    /// String too large for interner
    #[error("String too large: {0} bytes exceeds maximum")]
    StringTooLarge(usize),
    /// Integer overflow in size calculation
    #[error("Integer overflow in size calculation")]
    IntegerOverflow,
}

/// String interner for deduplication
///
/// This structure is intended to be used as a pair to our string pool. If the
/// payload has repetitious strings -- `DogStatsD` tags, `OTel` attributes -- at
/// various levels of an in-memory representation it is better to intern the
/// string and refer to it only by handle. That's what this does.
#[derive(Debug, Clone)]
pub(crate) struct StringInterner {
    /// Concatenated string bytes
    bytes: Vec<u8>,
    /// (`start_offset`, `length`) for each interned string
    strings: Vec<(u32, u32)>,
    /// String hash -> indicies in `strings`
    lookup: FxHashMap<u64, Vec<u32>>,
}

impl StringInterner {
    /// Create a new interner
    pub(crate) fn new() -> Self {
        Self {
            bytes: Vec::with_capacity(INITIAL_BYTE_CAP),
            strings: Vec::with_capacity(INITIAL_STRING_CAP),
            lookup: FxHashMap::with_capacity_and_hasher(
                INITIAL_STRING_CAP,
                BuildHasherDefault::<FxHasher>::default(),
            ),
        }
    }

    /// Intern a string and return its handle
    ///
    /// If the string is already interned, returns the existing handle.
    ///
    /// # Errors
    ///
    /// Returns error if string exceeeds `u32::MAX` length.
    pub(crate) fn intern(&mut self, s: &str) -> Result<Handle, Error> {
        // This function performs deduplication by hashing input strings,
        // stuffing them into a lookup table. If the string is present in the
        // lookup table, we return its handle. Otherwise, we append it to our
        // byte buffer and create a new handle.
        let hash = {
            let mut hasher = FxHasher::default();
            s.hash(&mut hasher);
            hasher.finish()
        };

        // Check if we have any handles with this hash, then check for collisions.
        if let Some(handles) = self.lookup.get(&hash) {
            for &handle in handles {
                if let Some(existing) = self.resolve(handle)
                    && existing == s
                {
                    return Ok(handle);
                }
            }
        }

        // Assert we haven't grown too big for our u32 handle or that `s` isn't
        // so large we can't fit into a handle.
        let str_len = s.len();
        if str_len > u32::MAX as usize {
            return Err(Error::StringTooLarge(str_len));
        }
        if self.strings.len() >= u32::MAX as usize {
            return Err(Error::IntegerOverflow);
        }
        let start_offset = self.bytes.len();
        if start_offset > u32::MAX as usize {
            return Err(Error::IntegerOverflow);
        }

        self.bytes.extend_from_slice(s.as_bytes());
        // Safety: we've already verified these fit in u32
        let handle = u32::try_from(self.strings.len()).expect("catastrophic programming error");
        self.strings.push((
            u32::try_from(start_offset).expect("catastrophic programming error"),
            u32::try_from(str_len).expect("catastrophic programming error"),
        ));

        self.lookup.entry(hash).or_default().push(handle);
        Ok(handle)
    }

    /// Resolve a handle to its string
    ///
    /// Returns None if handle is invalid
    pub(crate) fn resolve(&self, handle: Handle) -> Option<&str> {
        let (start, len) = *self.strings.get(handle as usize)?;
        let start = start as usize;
        let len = len as usize;
        let end = start + len;

        if end > self.bytes.len() {
            return None;
        }

        // Safety: We only store valid UTF-8 strings via intern(), which takes &str
        // Therefore the bytes are guaranteed to be valid UTF-8
        Some(unsafe { str::from_utf8_unchecked(&self.bytes[start..end]) })
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.strings.len()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.strings.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn byte_size(&self) -> usize {
        self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // Test that hash collisions are handled correctly
    #[test]
    fn hash_collision_handling() {
        let mut interner = StringInterner::new();

        // Verify deduplication works
        let h1 = interner.intern("hello").unwrap();
        let h2 = interner.intern("world").unwrap();
        let h3 = interner.intern("hello").unwrap(); // duplicate
        assert_eq!(h1, h3);
        assert_ne!(h1, h2);

        // Verify resolution works correctly
        assert_eq!(interner.resolve(h1), Some("hello"));
        assert_eq!(interner.resolve(h2), Some("world"));
        assert_eq!(interner.resolve(h3), Some("hello"));

        // Now with many strings
        let mut handles = Vec::new();
        let strings = [
            "foo", "bar", "baz", "qux", "quux", "corge", "grault", "garply",
        ];
        for s in &strings {
            handles.push((interner.intern(s).unwrap(), *s));
        }
        for (handle, expected) in handles {
            assert_eq!(interner.resolve(handle), Some(expected));
        }
    }

    proptest! {
        // Idempotence: interning the same string multiple times always returns
        // the same handle.
        #[test]
        fn intern_idempotent(s: String) {
            let mut interner = StringInterner::new();

            if let Ok(h1) = interner.intern(&s) {
                let h2 = interner.intern(&s).unwrap();
                prop_assert_eq!(h1, h2);
            }
        }

        // Round-trips are valid: interning and then resolving a string returns
        // the original string.
        #[test]
        fn round_trip(s: String) {
            let mut interner = StringInterner::new();

            if let Ok(handle) = interner.intern(&s) {
                prop_assert_eq!(interner.resolve(handle), Some(s.as_str()));
            }
        }

        // Assert that interned strings can be resolved through its handle
        // regardless of how many other strings we've interned, that is, we're
        // not confusing handles as the number of strings increase in
        // StringInterner.
        #[test]
        fn unique_handles(strings: Vec<String>) {
            let mut interner = StringInterner::new();
            let mut handles = Vec::new();

            for s in &strings {
                if let Ok(h) = interner.intern(s) {
                    handles.push(h);
                }
            }

            for (s, &h) in strings.iter().zip(handles.iter()) {
                if let Some(resolved) = interner.resolve(h) {
                    prop_assert_eq!(resolved, s);
                }
            }
        }

        // Assert that interning actually saves space, that a string pushed into
        // the interner only increases the internal length by one and only by
        // the size of the string.
        #[test]
        fn deduplication_saves_space(s: String, repeat_count in 1usize..100) {
            let mut interner = StringInterner::new();

            if s.len() > 0 && s.len() < 1000 {
                let mut handles = Vec::new();
                for _ in 0..repeat_count {
                    if let Ok(h) = interner.intern(&s) {
                        handles.push(h);
                    }
                }

                // All handles should be the same
                if !handles.is_empty() {
                    let first = handles[0];
                    for h in &handles {
                        prop_assert_eq!(*h, first);
                    }

                    // Should only store string once
                    prop_assert_eq!(interner.len(), 1);
                    prop_assert_eq!(interner.byte_size(), s.len());
                }
            }
        }
    }
}
