use crate::payload::{common::AsciiStr, Error, Serialize};
use arbitrary::{self, Unstructured};
use rand::Rng;
use std::io::Write;

#[derive(Debug, Default)]
pub struct Ascii {}

impl Serialize for Ascii {
    fn to_bytes<W, R>(&self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        let mut entropy: Vec<u8> = vec![0; max_bytes];
        rng.fill_bytes(&mut entropy);
        let mut unstructured = Unstructured::new(&entropy);

        let mut bytes_remaining = max_bytes;
        while let Ok(member) = unstructured.arbitrary::<AsciiStr>() {
            let encoding = member.as_str();
            let line_length = encoding.len() + 1; // add one for the newline
            match bytes_remaining.checked_sub(line_length) {
                Some(remainder) => {
                    writeln!(writer, "{}", encoding)?;
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
    use crate::payload::{Ascii, Serialize};
    use quickcheck::{QuickCheck, TestResult};
    use rand::{rngs::SmallRng, SeedableRng};

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    #[test]
    fn payload_not_exceed_max_bytes() {
        fn inner(seed: u64, max_bytes: u16) -> TestResult {
            let max_bytes = max_bytes as usize;
            let rng = SmallRng::seed_from_u64(seed);
            let ascii = Ascii::default();

            let mut bytes = Vec::with_capacity(max_bytes);
            ascii.to_bytes(rng, max_bytes, &mut bytes).unwrap();
            assert!(bytes.len() <= max_bytes);

            TestResult::passed()
        }
        QuickCheck::new()
            .tests(1_000)
            .quickcheck(inner as fn(u64, u16) -> TestResult);
    }
}
