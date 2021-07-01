use crate::payload::{Error, Serialize};
use arbitrary::Arbitrary;
use std::io::Write;

/// A simplistic 'Payload' structure without self-reference
#[derive(Arbitrary, Debug, serde::Serialize)]
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

#[derive(Arbitrary)]
pub struct Json {
    members: Vec<Member>,
}

impl Serialize for Json {
    fn to_bytes<W>(&self, writer: &mut W) -> Result<(), Error>
    where
        W: Write,
    {
        for member in &self.members {
            serde_json::to_writer(&mut *writer, member)?;
            writeln!(writer)?;
        }
        Ok(())
    }
}
