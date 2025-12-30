//! Static file payload that emits one second of log lines per block, based on
//! parsing a timestamp at the start of each line. The parsed timestamp is
//! stripped from emitted lines; only the message body is replayed.

use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use chrono::{NaiveDateTime, TimeZone, Utc};
use rand::Rng;
use tracing::{debug, info};

#[derive(Debug)]
struct BlockLines {
    lines: Vec<Vec<u8>>,
}

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`StaticSecond`].
pub enum Error {
    /// IO error
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// No lines were discovered in the provided path
    #[error("No lines found in static path")]
    NoLines,
    /// Timestamp parsing failed for a line
    #[error("Failed to parse timestamp from line: {0}")]
    Timestamp(String),
}

#[derive(Debug)]
/// Static payload grouped by second boundaries.
pub struct StaticSecond {
    path: PathBuf,
    timestamp_format: String,
    emit_placeholder: bool,
    initial_offset: u64,
    lines_to_skip_remaining: u64,
    offset_consumed: bool,
    reader: BufReader<File>,
    carry_first_line: Option<(i64, Vec<u8>)>,
    pending_gap: u64,
    next_block: Option<BlockLines>,
    last_lines_generated: u64,
}

impl StaticSecond {
    /// Create a new instance of `StaticSecond`
    ///
    /// Lines are grouped into blocks by the second of their timestamp. The
    /// timestamp is parsed from the start of the line up to the first
    /// whitespace, using `timestamp_format` (chrono strftime syntax). The
    /// parsed timestamp is removed from the emitted line, leaving only the
    /// remainder of the message. `start_line_index`, when provided, skips that
    /// many lines (modulo the total number of available lines) before
    /// returning payloads.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read, contains no lines, or a
    /// timestamp fails to parse.
    pub fn new(
        path: &Path,
        timestamp_format: &str,
        emit_placeholder: bool,
        start_line_index: Option<u64>,
    ) -> Result<Self, Error> {
        let file_size_bytes = File::open(path)
            .ok()
            .and_then(|f| f.metadata().ok())
            .map(|m| m.len())
            .unwrap_or(0);

        // Validation + counting pass; keeps memory usage low while preserving eager error detection.
        let mut validation_reader = BufReader::new(File::open(path)?);
        let mut total_lines: u64 = 0;
        let mut buf = String::new();
        loop {
            buf.clear();
            let bytes = validation_reader.read_line(&mut buf)?;
            if bytes == 0 {
                break;
            }
            if buf.trim().is_empty() {
                continue;
            }
            Self::parse_line_with_format(&buf, timestamp_format)?;
            total_lines += 1;
        }

        if total_lines == 0 {
            return Err(Error::NoLines);
        }

        let initial_offset = start_line_index.unwrap_or(0) % total_lines;
        let reader = BufReader::new(File::open(path)?);

        info!(
            "StaticSecond streaming from {} ({} bytes, {} total lines, emit_placeholder={}, start_line_index={})",
            path.display(),
            file_size_bytes,
            total_lines,
            emit_placeholder,
            initial_offset
        );

        let mut this = Self {
            path: path.to_path_buf(),
            timestamp_format: timestamp_format.to_owned(),
            emit_placeholder,
            initial_offset,
            lines_to_skip_remaining: initial_offset,
            offset_consumed: initial_offset == 0,
            reader,
            carry_first_line: None,
            pending_gap: 0,
            next_block: None,
            last_lines_generated: 0,
        };

        // Preload first block so we can fail fast on empty/invalid content.
        this.fill_next_block()?;
        if this.next_block.is_none() {
            return Err(Error::NoLines);
        }

        Ok(this)
    }

    fn parse_line_with_format(line: &str, timestamp_format: &str) -> Result<(i64, Vec<u8>), Error> {
        let mut parts = line.splitn(2, char::is_whitespace);
        let ts_token = parts.next().unwrap_or("");
        let payload = parts
            .next()
            .unwrap_or("")
            .trim_start()
            // Strip trailing newlines so we don't double-append in `to_bytes`.
            .trim_end_matches(['\r', '\n'])
            .as_bytes()
            .to_vec();
        let ts = NaiveDateTime::parse_from_str(ts_token, timestamp_format)
            .map_err(|_| Error::Timestamp(line.to_string()))?;
        let sec = Utc.from_utc_datetime(&ts).timestamp();
        Ok((sec, payload))
    }

    fn reset_reader(&mut self) -> Result<(), Error> {
        let file = File::open(&self.path)?;
        self.reader = BufReader::new(file);
        self.carry_first_line = None;
        self.pending_gap = 0;
        self.next_block = None;
        self.lines_to_skip_remaining = if self.offset_consumed {
            0
        } else {
            self.initial_offset
        };
        Ok(())
    }

    fn fill_next_block(&mut self) -> Result<(), Error> {
        if self.next_block.is_none() {
            self.next_block = self.read_next_block()?;
        }
        Ok(())
    }

    fn read_next_block(&mut self) -> Result<Option<BlockLines>, Error> {
        if self.pending_gap > 0 {
            self.pending_gap -= 1;
            return Ok(Some(BlockLines { lines: Vec::new() }));
        }

        // Pre-allocate for typical log density (reduces reallocation during push)
        let mut lines: Vec<Vec<u8>> = Vec::with_capacity(256);
        let mut sec: Option<i64> = None;

        if let Some((carry_sec, payload)) = self.carry_first_line.take() {
            sec = Some(carry_sec);
            lines.push(payload);
        }

        let mut buf = String::new();
        loop {
            buf.clear();
            let bytes = self.reader.read_line(&mut buf)?;
            if bytes == 0 {
                break;
            }
            if buf.trim().is_empty() {
                continue;
            }

            if self.lines_to_skip_remaining > 0 {
                self.lines_to_skip_remaining -= 1;
                continue;
            }

            let (line_sec, payload) = Self::parse_line_with_format(&buf, &self.timestamp_format)?;

            match sec {
                None => {
                    sec = Some(line_sec);
                    lines.push(payload);
                    self.offset_consumed = true;
                }
                Some(s) if s == line_sec => {
                    lines.push(payload);
                }
                Some(s) if s < line_sec => {
                    if self.emit_placeholder && line_sec > s + 1 {
                        self.pending_gap = (line_sec - s - 1) as u64;
                    }
                    self.carry_first_line = Some((line_sec, payload));
                    break;
                }
                Some(s) => {
                    debug!("Encountered out-of-order timestamp: current {s}, new {line_sec}");
                    self.carry_first_line = Some((line_sec, payload));
                    break;
                }
            }
        }

        if sec.is_none() && lines.is_empty() {
            return Ok(None);
        }

        Ok(Some(BlockLines { lines }))
    }
}

impl crate::Serialize for StaticSecond {
    fn to_bytes<W, R>(
        &mut self,
        _rng: R,
        max_bytes: usize,
        writer: &mut W,
    ) -> Result<(), crate::Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        self.last_lines_generated = 0;

        self.fill_next_block()?;
        if self.next_block.is_none() {
            self.reset_reader()?;
            self.fill_next_block()?;
        }

        let Some(block) = self.next_block.take() else {
            return Ok(());
        };

        let mut bytes_written = 0usize;
        if block.lines.is_empty() {
            // When requested, emit a minimal placeholder (one newline) for
            // empty seconds to preserve timing gaps without breaking the
            // non-zero block invariant.
            if self.emit_placeholder && max_bytes > 0 {
                writer.write_all(b"\n")?;
            }
        } else {
            for line in &block.lines {
                let needed = line.len() + 1; // newline
                if bytes_written + needed > max_bytes {
                    break;
                }
                writer.write_all(line)?;
                writer.write_all(b"\n")?;
                bytes_written += needed;
                self.last_lines_generated += 1;
            }
        }
        Ok(())
    }

    fn data_points_generated(&self) -> Option<u64> {
        Some(self.last_lines_generated)
    }
}

impl From<Error> for crate::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Io(e) => crate::Error::Io(e),
            Error::NoLines | Error::Timestamp(_) => crate::Error::Serialize,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Serialize;
    use rand::{SeedableRng, rngs::StdRng};
    use std::{fs::File, io::Write};
    use tempfile::tempdir;

    #[test]
    fn removes_timestamp_from_output() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("static_second_test.log");
        {
            let mut f = File::create(&path).unwrap();
            writeln!(f, "2024-01-01T00:00:00 first").unwrap();
            writeln!(f, "2024-01-01T00:00:00 second").unwrap();
            writeln!(f, "2024-01-01T00:00:01 third").unwrap();
        }

        let mut serializer = StaticSecond::new(
            &path,
            "%Y-%m-%dT%H:%M:%S",
            /* emit_placeholder */ false,
            None,
        )
        .unwrap();
        let mut rng = StdRng::seed_from_u64(7);
        let mut buf = Vec::new();

        serializer.to_bytes(&mut rng, 1024, &mut buf).unwrap();
        assert_eq!(buf, b"first\nsecond\n");

        buf.clear();
        serializer.to_bytes(&mut rng, 1024, &mut buf).unwrap();
        assert_eq!(buf, b"third\n");
    }

    #[test]
    fn emits_placeholders_for_missing_seconds() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("placeholder_test.log");
        {
            let mut f = File::create(&path).unwrap();
            writeln!(f, "2024-01-01T00:00:00 first").unwrap();
            // Intentionally skip 00:00:01
            writeln!(f, "2024-01-01T00:00:02 third").unwrap();
        }

        let mut serializer = StaticSecond::new(&path, "%Y-%m-%dT%H:%M:%S", true, None).unwrap();
        let mut rng = StdRng::seed_from_u64(7);

        let mut buf = Vec::new();
        serializer.to_bytes(&mut rng, 1024, &mut buf).unwrap();
        assert_eq!(buf, b"first\n");

        buf.clear();
        serializer.to_bytes(&mut rng, 1024, &mut buf).unwrap();
        // Placeholder newline for the missing second
        assert_eq!(buf, b"\n");

        buf.clear();
        serializer.to_bytes(&mut rng, 1024, &mut buf).unwrap();
        assert_eq!(buf, b"third\n");
    }

    #[test]
    fn honors_start_line_index_with_wraparound() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("start_index_test.log");
        {
            let mut f = File::create(&path).unwrap();
            // Two lines in the first second, one in the second second.
            writeln!(f, "2024-01-01T00:00:00 first").unwrap();
            writeln!(f, "2024-01-01T00:00:00 second").unwrap();
            writeln!(f, "2024-01-01T00:00:01 third").unwrap();
        }

        // Skip the first two lines; the stream should begin with "third".
        let mut serializer = StaticSecond::new(&path, "%Y-%m-%dT%H:%M:%S", false, Some(2)).unwrap();
        let mut rng = StdRng::seed_from_u64(7);

        let mut buf = Vec::new();
        serializer.to_bytes(&mut rng, 1024, &mut buf).unwrap();
        assert_eq!(buf, b"third\n");

        buf.clear();
        serializer.to_bytes(&mut rng, 1024, &mut buf).unwrap();
        // After wrapping, we return to the beginning of the stream.
        assert_eq!(buf, b"first\nsecond\n");
    }
}
