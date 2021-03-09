//! Functions to fill buffers with random data.
use rand::Rng;
use serde::Serialize;
use serde_json::to_writer;
use std::io::{self, Cursor, Write};

const CHARSET: &[u8] =
    b"abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789().,/\\{}[];:'\"";
#[allow(clippy::cast_possible_truncation)]
const CHARSET_LEN: u8 = CHARSET.len() as u8;

/// Errors related to buffer filling
#[derive(Debug)]
pub enum Error {
    /// Json payload could not be encoded
    Json(serde_json::Error),
    /// IO operation failed
    Io(io::Error),
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Error::Json(error)
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::Io(error)
    }
}

/// The json payload produced for the json variant line
#[derive(Debug, Serialize)]
pub struct JsonPayload {
    /// A u64. Its name has no meaning.
    pub id: u64,
    /// A u64. Its name has no meaning.
    pub name: u64,
    /// A u16. Its name has no meaning.
    pub seed: u16,
    /// A variable length array of bytes. Its name has no meaning.
    pub byte_parade: Vec<u8>,
}

const fn max_string_u64() -> usize {
    let base = 20;
    let quotes = 2;
    let comma = 1;

    base + quotes + comma
}

const fn max_string_u16() -> usize {
    let base = 5;
    let quotes = 2;
    let comma = 1;

    base + quotes + comma
}

const fn max_string_u8() -> usize {
    let base = 3;
    let quotes = 2;
    let comma = 1;

    base + quotes + comma
}

/// Fill a buffer with a json payload
///
/// This function fills a buffer with a json payload, terminating the buffer in
/// a newline. The exact payload is guaranteed to be an instance of
/// [`JsonPayload`].
///
/// We measure this function as filling a buffer at ~450Mb/s, +/- 3%.
///
/// # Errors
///
/// This function will never produce an error.
#[inline]
pub fn fill_json<R>(rng: &mut R, buffer: &mut [u8]) -> Result<usize, Error>
where
    R: Rng + Sized,
{
    // The final payload, being stringly encoded, will have a bunch of '"' and
    // ',' characters populating it. To control for this we
    let brackets = 2;
    let curley_brackets = 2;
    let payload_overhead = max_string_u64() * 2 + max_string_u16() + brackets + curley_brackets;
    let remain = buffer.len().saturating_sub(payload_overhead);
    let parade_bytes_available = remain / max_string_u8();

    let mut buf = Cursor::new(buffer);
    let mut byte_parade = vec![0; parade_bytes_available];
    rng.fill_bytes(&mut byte_parade);

    let payload = JsonPayload {
        id: rng.gen(),
        name: rng.gen(),
        seed: rng.gen(),
        byte_parade,
    };

    to_writer(&mut buf, &payload)?;
    buf.write_all(b"\n")?;

    // On 32-bit and lower platforms this may truncate away valuable bytes. We
    // consider it unlikely that this program will be run on anything other than
    // a 64-bit computer.
    #[allow(clippy::cast_possible_truncation)]
    Ok(buf.position() as usize)
}

/// Fill a buffer with a constant char
///
/// This function fills a buffer with a constant ascii char, terminating the
/// buffer in a newline. The exact char is not specified and you should not rely
/// on it being consistent across versions of this program.
///
/// We measure this function as filling a buffer at ~125Gb/s, +/- 5%.
///
/// # Errors
///
/// This function will never produce an error.
#[inline]
pub fn fill_constant<R>(_: &mut R, buffer: &mut [u8]) -> Result<usize, Error>
where
    R: Rng + Sized,
{
    buffer.iter_mut().for_each(|c| *c = b'A');
    buffer[buffer.len() - 1] = b'\n';
    Ok(buffer.len())
}

/// Fill a buffer with ascii printable characters
///
/// This function fills a buffer with ascii printable characters, terminating
/// the buffer in a newline. There is no structure to the line and not all
/// printable characters are guaranteed to appear in the line.
///
/// We measure this function as filling a buffer at ~1.2Gb/s, +/- 1%.
///
/// # Errors
///
/// This function will never produce an error.
#[inline]
pub fn fill_ascii<R>(rng: &mut R, buffer: &mut [u8]) -> Result<usize, Error>
where
    R: Rng + Sized,
{
    rng.fill_bytes(buffer);
    buffer
        .iter_mut()
        .for_each(|item| *item = CHARSET[(*item % CHARSET_LEN) as usize]);
    buffer[buffer.len() - 1] = b'\n';
    Ok(buffer.len())
}
