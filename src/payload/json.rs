use crate::payload::{Error, Serialize};
use arbitrary::{size_hint, Arbitrary, Unstructured};
use rand::Rng;
use std::io::Write;

const SIZES: [usize; 13] = [0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048];

/// A simplistic 'Payload' structure without self-reference
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Member {
    /// A u64. Its name has no meaning.
    pub id: u64,
    /// A u64. Its name has no meaning.
    pub name: u64,
    /// A u16. Its name has no meaning.
    pub seed: u16,
    /// A variable length array of bytes. Its name has no meaning.
    pub byte_parade: Vec<u8>,
}

impl<'a> Arbitrary<'a> for Member {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let choice: u8 = u.arbitrary()?;
        let size = SIZES[(choice as usize) % SIZES.len()];
        let mut byte_parade: Vec<u8> = vec![0; size];
        u.fill_buffer(&mut byte_parade)?;

        let member = Member {
            id: u.arbitrary()?,
            name: u.arbitrary()?,
            seed: u.arbitrary()?,
            byte_parade,
        };
        Ok(member)
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        let byte_parade_hint = (SIZES[0], Some(SIZES[SIZES.len() - 1]));

        size_hint::recursion_guard(depth, |depth| {
            size_hint::and_all(&[
                <u64 as Arbitrary>::size_hint(depth),
                <u64 as Arbitrary>::size_hint(depth),
                <u16 as Arbitrary>::size_hint(depth),
                byte_parade_hint,
            ])
        })
    }
}

#[derive(Debug, Default)]
pub struct Json {}

impl Serialize for Json {
    fn to_bytes<W, R>(&self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        let mut entropy: Vec<u8> = vec![0; max_bytes];
        rng.fill_bytes(&mut entropy);
        let mut unstructured = Unstructured::new(&entropy);

        let mut bytes_remaining = max_bytes;
        while let Ok(member) = unstructured.arbitrary::<Member>() {
            let encoding = serde_json::to_string(&member)?;
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
    use quickcheck::{QuickCheck, TestResult};
    use rand::rngs::SmallRng;
    use rand::SeedableRng;

    use super::Member;
    use crate::payload::{Json, Serialize};

    // We want to be sure that the serialized size of the payload does not
    // exceed `max_bytes`.
    #[test]
    fn payload_not_exceed_max_bytes() {
        fn inner(seed: u64, max_bytes: u16) -> TestResult {
            let max_bytes = max_bytes as usize;
            let rng = SmallRng::seed_from_u64(seed);
            let json = Json::default();

            let mut bytes = Vec::with_capacity(max_bytes);
            json.to_bytes(rng, max_bytes, &mut bytes).unwrap();
            assert!(bytes.len() <= max_bytes);

            TestResult::passed()
        }
        QuickCheck::new()
            .tests(1_000)
            .quickcheck(inner as fn(u64, u16) -> TestResult);
    }

    // We want to know that every payload produced by this type actually
    // deserializes as json, is not truncated etc.
    #[test]
    fn every_payload_deserializes() {
        fn inner(seed: u64, max_bytes: u16) -> TestResult {
            let max_bytes = max_bytes as usize;
            let rng = SmallRng::seed_from_u64(seed);
            let json = Json::default();

            let mut bytes: Vec<u8> = Vec::with_capacity(max_bytes);
            json.to_bytes(rng, max_bytes, &mut bytes).unwrap();

            let payload = std::str::from_utf8(&bytes).unwrap();
            for msg in payload.lines() {
                let _members: Member = serde_json::from_str(msg).unwrap();
            }

            TestResult::passed()
        }
        QuickCheck::new()
            .tests(1_000_000)
            .quickcheck(inner as fn(u64, u16) -> TestResult);
    }
}
