//! ASCII payload.

use std::io::Write;

use rand::Rng;

use crate::{common::strings, Error};

const MAX_LENGTH: u16 = 6_144; // 6 KiB

#[derive(Debug, Clone)]
/// ASCII text payload
pub struct Ascii {
    pool: strings::Pool,
}

impl Ascii {
    /// Construct a new instance of `Ascii`
    pub fn new<R>(rng: &mut R) -> Self
    where
        R: rand::Rng + ?Sized,
    {
        Self {
            // SAFETY: Do not adjust this downward below MAX_LENGTH without also
            // adjusting the input to `self.pool.of_size` below.
            pool: strings::Pool::with_size(rng, usize::from(MAX_LENGTH * 4)),
        }
    }
}

impl crate::Serialize for Ascii {
    fn to_bytes<W, R>(&self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        let mut bytes_remaining = max_bytes;
        loop {
            let bytes = rng.gen_range(1..MAX_LENGTH);
            // SAFETY: the maximum request is always less than the size of the
            // pool, per our constructor.
            let encoding: &str = self
                .pool
                .of_size(&mut rng, usize::from(bytes))
                .ok_or(Error::StringGenerate)?;
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

    use crate::{Ascii, Serialize};

    // The serialized size of the payload must not exceed `max_bytes`.
    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let ascii = Ascii::new(&mut rng);

            let mut bytes = Vec::with_capacity(max_bytes);
            ascii.to_bytes(rng, max_bytes, &mut bytes)?;
            prop_assert!(bytes.len() <= max_bytes);
        }
    }
}
