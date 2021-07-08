use crate::payload::common::AsciiStr;
use crate::payload::{Error, Serialize};
use arbitrary::{Arbitrary, Unstructured};
use rand::{thread_rng, RngCore};
use std::io::Write;

#[derive(Arbitrary, Debug, serde::Serialize)]
struct Member {
    /// The message is a short ascii string, without newlines for now
    pub message: AsciiStr,
    /// The timestamp is a simple integer value since epoch, presumably
    pub timestamp: u32,
}

#[derive(Debug, Default)]
pub struct DatadogLog {}

impl Serialize for DatadogLog {
    fn to_bytes<W>(&self, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        W: Write,
    {
        let mut rng = thread_rng();
        let mut entropy: Vec<u8> = vec![0; max_bytes];
        rng.fill_bytes(&mut entropy);
        let mut unstructured = Unstructured::new(&entropy);

        let mut bytes_remaining = max_bytes;
        while let Ok(member) = unstructured.arbitrary::<Vec<Member>>() {
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
