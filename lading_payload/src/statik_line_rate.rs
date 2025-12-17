//! Static file payload that replays a limited number of lines per block.

use std::{
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Write},
    num::NonZeroU32,
    path::Path,
};

use rand::{Rng, seq::IndexedMutRandom};
use tracing::debug;

#[derive(Debug)]
struct Source {
    lines: Vec<Vec<u8>>,
    next_idx: usize,
}

#[derive(Debug)]
/// Static payload that emits a fixed number of lines each time it is asked to
/// serialize.
pub struct StaticLinesPerSecond {
    sources: Vec<Source>,
    lines_per_block: NonZeroU32,
    last_lines_generated: u64,
}

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`StaticLinesPerSecond`].
pub enum Error {
    /// IO error
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// No lines were discovered in the provided path
    #[error("No lines found in static path")]
    NoLines,
    /// The provided lines_per_second value was zero
    #[error("lines_per_second must be greater than zero")]
    ZeroLinesPerSecond,
}

impl StaticLinesPerSecond {
    /// Create a new instance of `StaticLinesPerSecond`
    ///
    /// # Errors
    ///
    /// See documentation for [`Error`]
    pub fn new(path: &Path, lines_per_second: u32) -> Result<Self, Error> {
        let lines_per_block = NonZeroU32::new(lines_per_second).ok_or(Error::ZeroLinesPerSecond)?;

        let mut sources = Vec::with_capacity(16);

        let metadata = fs::metadata(path)?;
        if metadata.is_file() {
            debug!("Static path {} is a file.", path.display());
            let lines = read_lines(path)?;
            sources.push(Source { next_idx: 0, lines });
        } else if metadata.is_dir() {
            debug!("Static path {} is a directory.", path.display());
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                let entry_pth = entry.path();
                debug!("Attempting to open {} as file.", entry_pth.display());
                if let Ok(file) = OpenOptions::new().read(true).open(&entry_pth) {
                    let lines = read_lines_from_reader(file)?;
                    sources.push(Source { next_idx: 0, lines });
                }
            }
        }

        if sources.iter().all(|s| s.lines.is_empty()) {
            return Err(Error::NoLines);
        }

        Ok(Self {
            sources,
            lines_per_block,
            last_lines_generated: 0,
        })
    }
}

impl crate::Serialize for StaticLinesPerSecond {
    fn to_bytes<W, R>(
        &mut self,
        mut rng: R,
        max_bytes: usize,
        writer: &mut W,
    ) -> Result<(), crate::Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        self.last_lines_generated = 0;

        let Some(source) = self.sources.choose_mut(&mut rng) else {
            return Ok(());
        };
        if source.lines.is_empty() {
            return Ok(());
        }

        let mut bytes_written = 0usize;
        for _ in 0..self.lines_per_block.get() {
            let line = &source.lines[source.next_idx % source.lines.len()];
            let needed = line.len() + 1; // newline
            if bytes_written + needed > max_bytes {
                break;
            }

            writer.write_all(line)?;
            writer.write_all(b"\n")?;
            bytes_written += needed;
            self.last_lines_generated += 1;
            source.next_idx = (source.next_idx + 1) % source.lines.len();
        }

        Ok(())
    }

    fn data_points_generated(&self) -> Option<u64> {
        Some(self.last_lines_generated)
    }
}

fn read_lines(path: &Path) -> Result<Vec<Vec<u8>>, std::io::Error> {
    let file = OpenOptions::new().read(true).open(path)?;
    read_lines_from_reader(file)
}

fn read_lines_from_reader<R: std::io::Read>(reader: R) -> Result<Vec<Vec<u8>>, std::io::Error> {
    let mut out = Vec::new();
    let mut reader = BufReader::new(reader);
    let mut buf = String::new();
    while {
        buf.clear();
        reader.read_line(&mut buf)?
    } != 0
    {
        if buf.ends_with('\n') {
            buf.pop();
            if buf.ends_with('\r') {
                buf.pop();
            }
        }
        out.push(buf.as_bytes().to_vec());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Serialize;
    use rand::{SeedableRng, rngs::StdRng};
    use std::{env, fs::File, io::Write as IoWrite};

    #[test]
    fn writes_requested_number_of_lines() {
        let mut path = env::temp_dir();
        path.push("static_line_rate_test.txt");
        {
            let mut f = File::create(&path).unwrap();
            writeln!(f, "alpha").unwrap();
            writeln!(f, "beta").unwrap();
            writeln!(f, "gamma").unwrap();
        }

        let mut serializer = StaticLinesPerSecond::new(&path, 2).unwrap();
        let mut buf = Vec::new();
        let mut rng = StdRng::seed_from_u64(42);

        serializer.to_bytes(&mut rng, 1024, &mut buf).unwrap();
        assert_eq!(buf, b"alpha\nbeta\n");
        // Clean up
        let _ = std::fs::remove_file(&path);
    }
}
