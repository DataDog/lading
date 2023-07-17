//! ASCII payload.

use std::io::Write;

use rand::Rng;

use crate::payload::{Error, Serialize};

use super::{common::AsciiString, Generator};

const MAX_LENGTH: u16 = 6_144; // 6 KiB

#[derive(Debug, Default, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub(crate) struct Ascii {}

impl Serialize for Ascii {
    fn to_bytes<W, R>(&self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        let mut bytes_remaining = max_bytes;
        loop {
            let encoding: String = AsciiString::with_maximum_length(MAX_LENGTH).generate(&mut rng);
            let line_length = encoding.len() + 1; // add one for the newline
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

    use crate::payload::{Ascii, Serialize};

    // The serialized size of the payload must not exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let rng = SmallRng::seed_from_u64(seed);
            let ascii = Ascii::default();

            let mut bytes = Vec::with_capacity(max_bytes);
            ascii.to_bytes(rng, max_bytes, &mut bytes).unwrap();
            prop_assert!(bytes.len() <= max_bytes);
        }
    }
}
