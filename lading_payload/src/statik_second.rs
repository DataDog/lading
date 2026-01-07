//! Static file payload that emits one second of log lines per block, based on
//! parsing a timestamp at the start of each line. The parsed timestamp is
//! stripped from emitted lines; only the message body is replayed.

use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
    path::Path,
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
    blocks: Vec<BlockLines>,
    idx: usize,
    last_lines_generated: u64,
    emit_placeholder: bool,
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
        let file = File::open(path)?;
        let file_size_bytes = file.metadata().map(|m| m.len()).unwrap_or(0);
        let reader = BufReader::new(file);

        let mut blocks: Vec<BlockLines> = Vec::new();
        let mut current_sec: Option<i64> = None;
        let mut current_lines: Vec<Vec<u8>> = Vec::new();

        for line_res in reader.lines() {
            let line = line_res?;
            if line.trim().is_empty() {
                continue;
            }

            // Take prefix until first whitespace as the timestamp segment and
            // drop it from the payload we store.
            let mut parts = line.splitn(2, char::is_whitespace);
            let ts_token = parts.next().unwrap_or("");
            let payload = parts.next().unwrap_or("").trim_start().as_bytes().to_vec();
            let ts = NaiveDateTime::parse_from_str(ts_token, timestamp_format)
                .map_err(|_| Error::Timestamp(line.clone()))?;
            let sec = Utc.from_utc_datetime(&ts).timestamp();

            match current_sec {
                Some(s) if s == sec => {
                    current_lines.push(payload);
                }
                Some(s) if s < sec => {
                    // Close out the previous second.
                    blocks.push(BlockLines {
                        lines: current_lines,
                    });
                    // Fill missing seconds with empty buckets when placeholders
                    // are requested.
                    if emit_placeholder {
                        let mut missing = s + 1;
                        while missing < sec {
                            blocks.push(BlockLines { lines: Vec::new() });
                            missing += 1;
                        }
                    }
                    current_lines = vec![payload];
                    current_sec = Some(sec);
                }
                Some(s) => {
                    // Unexpected time travel backwards; treat as new bucket to
                    // preserve ordering.
                    blocks.push(BlockLines {
                        lines: current_lines,
                    });
                    current_lines = vec![payload];
                    current_sec = Some(sec);
                    debug!("Encountered out-of-order timestamp: current {s}, new {sec}");
                }
                None => {
                    current_sec = Some(sec);
                    current_lines.push(payload);
                }
            }
        }

        if !current_lines.is_empty() {
            blocks.push(BlockLines {
                lines: current_lines,
            });
        } else if emit_placeholder && current_sec.is_some() {
            // If the file ended right after emitting placeholders, ensure the
            // last bucket is represented.
            blocks.push(BlockLines { lines: Vec::new() });
        }

        if blocks.is_empty() {
            return Err(Error::NoLines);
        }

        let start_line_index = start_line_index.unwrap_or(0);
        // Apply starting line offset by trimming leading lines across buckets.
        let total_lines: u64 = blocks.iter().map(|b| b.lines.len() as u64).sum();
        let mut start_idx = 0usize;
        if total_lines > 0 && start_line_index > 0 {
            let mut remaining = start_line_index % total_lines;
            if remaining > 0 {
                for (idx, block) in blocks.iter_mut().enumerate() {
                    let len = block.lines.len() as u64;
                    if len == 0 {
                        continue;
                    }
                    if remaining >= len {
                        remaining -= len;
                    } else {
                        let cut = usize::try_from(remaining).unwrap_or(block.lines.len());
                        block.lines.drain(0..cut);
                        start_idx = idx;
                        break;
                    }
                }
            }
        }

        info!(
            "StaticSecond loaded {} second-buckets ({} total lines) from {} ({} bytes, emit_placeholder={})",
            blocks.len(),
            total_lines,
            path.display(),
            file_size_bytes,
            emit_placeholder
        );

        Ok(Self {
            blocks,
            idx: start_idx,
            last_lines_generated: 0,
            emit_placeholder,
        })
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
        if self.blocks.is_empty() {
            return Ok(());
        }

        // Choose blocks strictly sequentially to preserve chronological replay (no rng based on seed)
        let block = &self.blocks[self.idx];

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

        self.idx = (self.idx + 1) % self.blocks.len();
        Ok(())
    }

    fn data_points_generated(&self) -> Option<u64> {
        Some(self.last_lines_generated)
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
