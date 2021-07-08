use crate::payload::{Error, Serialize};
use arbitrary::{self, Unstructured};
use rand::{thread_rng, RngCore};
use std::io::Write;

const SIZES: [usize; 8] = [16, 32, 64, 128, 256, 512, 1024, 2048];
const CHARSET: &[u8] =
    b"abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789().,/\\{}[];:'\"";
#[allow(clippy::cast_possible_truncation)]
const CHARSET_LEN: u8 = CHARSET.len() as u8;

#[derive(Debug)]
struct Member {
    bytes: Vec<u8>,
}

impl<'a> arbitrary::Arbitrary<'a> for Member {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let choice: u8 = u.arbitrary()?;
        let size = SIZES[(choice as usize) % SIZES.len()];
        let mut bytes: Vec<u8> = vec![0; size];
        u.fill_buffer(&mut bytes)?;
        bytes
            .iter_mut()
            .for_each(|item| *item = CHARSET[(*item % CHARSET_LEN) as usize]);
        Ok(Member { bytes })
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        (128, Some(8192))
    }
}

#[derive(Debug, Default)]
pub struct Ascii {}

impl Serialize for Ascii {
    fn to_bytes<W>(&self, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        W: Write,
    {
        let mut rng = thread_rng();
        let mut entropy: Vec<u8> = vec![0; max_bytes];
        rng.fill_bytes(&mut entropy);
        let mut unstructured = Unstructured::new(&entropy);

        let mut bytes_remaining = max_bytes;
        while let Ok(member) = unstructured.arbitrary::<Member>() {
            let encoding = std::str::from_utf8(&member.bytes).unwrap();
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
