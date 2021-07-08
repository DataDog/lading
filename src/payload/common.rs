use arbitrary::Unstructured;

const SIZES: [usize; 8] = [16, 32, 64, 128, 256, 512, 1024, 2048];
const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789().,";
#[allow(clippy::cast_possible_truncation)]
const CHARSET_LEN: u8 = CHARSET.len() as u8;

#[derive(Debug)]
pub struct AsciiStr {
    bytes: Vec<u8>,
}

impl serde::Serialize for AsciiStr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl AsciiStr {
    pub fn as_str(&self) -> &str {
        // Safety: given that CHARSET is where we derive members from
        // `self.bytes` is always valid UTF-8.
        unsafe { std::str::from_utf8_unchecked(&self.bytes) }
    }
}

impl<'a> arbitrary::Arbitrary<'a> for AsciiStr {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let choice: u8 = u.arbitrary()?;
        let size = SIZES[(choice as usize) % SIZES.len()];
        let mut bytes: Vec<u8> = vec![0; size];
        u.fill_buffer(&mut bytes)?;
        bytes
            .iter_mut()
            .for_each(|item| *item = CHARSET[(*item % CHARSET_LEN) as usize]);
        Ok(Self { bytes })
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        (128, Some(8192))
    }
}
