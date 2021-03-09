use rand::Rng;
use serde::Serialize;
use serde_json::to_writer;
use std::io::{self, Cursor, Write};

const CHARSET: &[u8] =
    b"abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789().,/\\{}[];:'\"";
const CHARSET_LEN: u8 = CHARSET.len() as u8;

#[derive(Debug)]
pub enum Error {
    InsufficientSpace,
    Json(serde_json::Error),
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

#[derive(Debug, Serialize)]
struct JsonPayload {
    id: u64,
    name: u64,
    seed: u16,
    byte_parade: Vec<u8>,
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

/// Performance: ~250Mb/s
#[inline]
pub fn fill_json_buffer<R>(rng: &mut R, buffer: &mut [u8]) -> Result<usize, Error>
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
    buf.write(b"\n")?;

    Ok(buf.position() as usize)
}

/// Performance: ~100Gb/s
#[inline]
pub fn fill_constant_buffer<R>(_: &mut R, buffer: &mut [u8]) -> Result<usize, Error>
where
    R: Rng + Sized,
{
    buffer.iter_mut().for_each(|c| *c = b'A');
    buffer[buffer.len() - 1] = b'\n';
    Ok(buffer.len())
}

/// Performance: ~1.2Gb/s
#[inline]
pub fn fill_ascii_buffer<R>(rng: &mut R, buffer: &mut [u8]) -> Result<usize, Error>
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
