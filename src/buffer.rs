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
}

/// Performance: ~400Mb/s
#[inline]
pub fn fill_json_buffer<R>(rng: &mut R, buffer: &mut [u8]) -> Result<usize, Error>
where
    R: Rng + Sized,
{
    let mut buf = Cursor::new(buffer);
    let bytes_written;
    loop {
        let payload = JsonPayload {
            id: rng.gen(),
            name: rng.gen(),
            seed: rng.gen(),
        };

        let pos = buf.position();
        if to_writer(&mut buf, &payload).is_err() {
            buf.set_position(pos);
            buf.write(b"\n")?;
            bytes_written = buf.position();
            break;
        } else {
            buf.write(b"\n")?;
        }
    }

    Ok(bytes_written as usize)
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
