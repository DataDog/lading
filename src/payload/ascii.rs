use crate::payload::{Error, Serialize};
use arbitrary::{self, Arbitrary, Unstructured};
use std::io::Write;

const CHARSET: &[u8] =
    b"abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789().,/\\{}[];:'\"";
#[allow(clippy::cast_possible_truncation)]
const CHARSET_LEN: u8 = CHARSET.len() as u8;

#[derive(Debug)]
struct Member {
    bytes: Vec<u8>,
}

impl<'a> Arbitrary<'a> for Member {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let mut bytes: Vec<u8> = u.arbitrary()?;
        u.fill_buffer(&mut bytes)?;
        bytes
            .iter_mut()
            .for_each(|item| *item = CHARSET[(*item % CHARSET_LEN) as usize]);
        Ok(Member { bytes })
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        (0, Some(6144)) // 100B to 6KiB
    }
}

#[derive(Arbitrary, Debug)]
pub struct Ascii {
    members: Vec<Member>,
}

impl Serialize for Ascii {
    fn to_bytes<W>(&self, writer: &mut W) -> Result<(), Error>
    where
        W: Write,
    {
        for member in &self.members {
            writeln!(writer, "{}", std::str::from_utf8(&member.bytes).unwrap())?;
        }
        Ok(())
    }
}
