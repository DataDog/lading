use crate::payload::{common::AsciiStr, Error, Serialize};
use arbitrary::{self, Unstructured};
use rand::{thread_rng, RngCore};
use std::io::Write;

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
