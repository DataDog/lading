//! Static file payload that emits one second of data per block, calculated by
//! parsing a timestamp at the start of each line. The parsed timestamp is
//! stripped from emitted lines; only the message body is replayed.

use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use chrono::{NaiveDateTime, TimeZone, Utc};
use rand::Rng;
use tracing::info;

#[derive(Debug)]
struct BlockLines {
    lines: Vec<Vec<u8>>,
}

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`StaticTimestamped`].
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
    /// Timestamps are not in ascending order
    #[error(
        "Out-of-order timestamp at line {line_number}: expected >= {previous_timestamp}, got {current_timestamp}"
    )]
    OutOfOrder {
        /// The 0-indexed line number where the out-of-order timestamp was found
        line_number: u64,
        /// The timestamp of the previous line
        previous_timestamp: i64,
        /// The timestamp of the current (out-of-order) line
        current_timestamp: i64,
    },
}

#[derive(Debug)]
/// Static payload grouped by second boundaries.
pub struct StaticTimestamped {
    /// The path to the static file.
    path: PathBuf,
    /// The timestamp format to use.
    timestamp_format: String,
    /// Whether to emit a placeholder for empty seconds.
    emit_placeholder: bool,
    /// The initial line to start reading from (0-indexed). None means a random offset will be chosen on first use.
    initial_offset: Option<u64>,

    total_lines: u64,
    reader: BufReader<File>,

    /// Line (and its parsed timestamp) "carried" over while reading the previous block.
    carry_first_line: Option<(i64, Vec<u8>)>,
    /// The number of seconds to skip before reading the next block. E.g. if `pending_gap` is 1,
    /// we will skip the first second before reading the second block.
    pending_gap: u64,
    /// The next block to read.
    next_block: Option<BlockLines>,
}

impl StaticTimestamped {
    /// Create a new instance of `StaticTimestamped`
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
            .map_or(0, |m| m.len());

        // Validation pass: verify this file has lines with valid, ascending timestamps.
        let mut validation_reader = BufReader::new(File::open(path)?);
        let mut total_lines: u64 = 0;
        let mut prev_timestamp: Option<i64> = None;
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
            let (ts, _) = Self::parse_line_with_format(&buf, timestamp_format)?;
            if let Some(prev_ts) = prev_timestamp
                && ts < prev_ts
            {
                return Err(Error::OutOfOrder {
                    line_number: total_lines,
                    previous_timestamp: prev_ts,
                    current_timestamp: ts,
                });
            }
            prev_timestamp = Some(ts);
            total_lines += 1;
        }
        if total_lines == 0 {
            return Err(Error::NoLines);
        }

        let initial_offset = start_line_index.map(|idx| idx % total_lines);
        let reader = BufReader::new(File::open(path)?);

        info!(
            "StaticTimestamped streaming from {} ({} bytes, {} total lines) with options emit_placeholder={}, start_line_index={:?}",
            path.display(),
            file_size_bytes,
            total_lines,
            emit_placeholder,
            initial_offset
        );

        let mut instance = Self {
            path: path.to_path_buf(),
            timestamp_format: timestamp_format.to_owned(),
            emit_placeholder,
            initial_offset,
            total_lines,
            reader,
            carry_first_line: None,
            pending_gap: 0,
            next_block: None,
        };

        // Reset reader state to the initial offset if specified.
        if let Some(offset) = initial_offset {
            instance.reset_reader(offset)?;
        }

        Ok(instance)
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

    fn reset_reader(&mut self, offset: u64) -> Result<(), Error> {
        let file = File::open(&self.path)?;
        self.reader = BufReader::new(file);
        self.carry_first_line = None;
        self.pending_gap = 0;
        self.next_block = None;

        // Skip to the specified offset.
        let mut buf = String::new();
        let mut lines_skipped = 0u64;
        while lines_skipped < offset {
            buf.clear();
            let bytes = self.reader.read_line(&mut buf)?;
            if bytes == 0 {
                break;
            }
            if buf.trim().is_empty() {
                continue;
            }
            lines_skipped += 1;
        }
        Ok(())
    }

    /// Given a target line offset, find the start of the timestamp group it belongs to.
    /// This ensures we always begin reading from the first line of a timestamp group.
    fn find_group_start(&self, target_offset: u64) -> Result<u64, Error> {
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        let mut buf = String::new();

        let mut line_index: u64 = 0;
        let mut group_start: u64 = 0;
        let mut current_timestamp: Option<i64> = None;

        loop {
            buf.clear();
            let bytes = reader.read_line(&mut buf)?;
            if bytes == 0 {
                break;
            }
            if buf.trim().is_empty() {
                continue;
            }

            let (ts, _) = Self::parse_line_with_format(&buf, &self.timestamp_format)?;

            match current_timestamp {
                None => {
                    current_timestamp = Some(ts);
                    group_start = line_index;
                }
                Some(prev_ts) if prev_ts != ts => {
                    current_timestamp = Some(ts);
                    group_start = line_index;
                }
                _ => {}
            }

            if line_index == target_offset {
                break;
            }

            line_index += 1;
        }

        Ok(group_start)
    }

    fn fill_next_block(&mut self) -> Result<(), Error> {
        if self.next_block.is_some() {
            return Ok(());
        }

        // Try to read a block, wrapping around if we hit EOF
        loop {
            self.next_block = self.read_next_block()?;
            if self.next_block.is_some() {
                break;
            }
            // Hit EOF - wrap around.
            self.reset_reader(0)?;
        }
        Ok(())
    }

    fn read_next_block(&mut self) -> Result<Option<BlockLines>, Error> {
        // While reading with pending gap, emit an empty block. This occurs with files don't populate every second with logs e.g.:
        // 2024-01-01T00:00:00 file line 0
        // <Pending gap of 10>
        // 2024-01-01T00:00:10 file line 1
        if self.pending_gap > 0 {
            self.pending_gap -= 1;
            return Ok(Some(BlockLines { lines: Vec::new() }));
        }

        let mut lines: Vec<Vec<u8>> = Vec::with_capacity(256);
        let mut current_second: Option<i64> = None;

        // If we have a carry-over line (found when reading the previous block), use it to start the block.
        if let Some((carry_sec, payload)) = self.carry_first_line.take() {
            current_second = Some(carry_sec);
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

            let (line_sec, payload) = Self::parse_line_with_format(&buf, &self.timestamp_format)?;

            match current_second {
                None => {
                    // No carry over - start a new block
                    current_second = Some(line_sec);
                    lines.push(payload);
                }
                Some(s) if s == line_sec => {
                    // This line is in the same second as the current block add to it.
                    lines.push(payload);
                }
                Some(s) if s < line_sec => {
                    // This line is in the future - set the pending gap and carry over the line.
                    if self.emit_placeholder && line_sec > s + 1 {
                        // The condition above guarantees line_sec - s - 1 > 0
                        self.pending_gap = u64::try_from(line_sec - s - 1).unwrap_or(0);
                    }
                    self.carry_first_line = Some((line_sec, payload));
                    break;
                }
                Some(s) => {
                    // This should be unreachable due to validation in `new()`, but return
                    // an error defensively
                    return Err(Error::OutOfOrder {
                        line_number: 0, // Line number not tracked at runtime
                        previous_timestamp: s,
                        current_timestamp: line_sec,
                    });
                }
            }
        }
        if current_second.is_none() && lines.is_empty() {
            // EOF reached return None and let fill_next_block handle the wrap around.
            return Ok(None);
        }
        Ok(Some(BlockLines { lines }))
    }
}

impl crate::Serialize for StaticTimestamped {
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
        if self.initial_offset.is_none() {
            let random_offset = rng.random_range(0..self.total_lines);
            // Backtrack to the start of the timestamp group containing this line
            let adjusted_offset = self.find_group_start(random_offset)?;
            self.initial_offset = Some(adjusted_offset);
            // Reset reader to apply the adjusted offset
            self.reset_reader(adjusted_offset)?;
        }

        self.fill_next_block()?;
        let block = self
            .next_block
            .take()
            .expect("fill_next_block guarantees a block");

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
            }
        }
        Ok(())
    }
}

impl From<Error> for crate::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Io(e) => crate::Error::Io(e),
            Error::NoLines | Error::Timestamp(_) | Error::OutOfOrder { .. } => {
                crate::Error::Serialize
            }
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

        let mut serializer = StaticTimestamped::new(
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

        let mut serializer =
            StaticTimestamped::new(&path, "%Y-%m-%dT%H:%M:%S", true, None).unwrap();
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
        let mut serializer =
            StaticTimestamped::new(&path, "%Y-%m-%dT%H:%M:%S", false, Some(2)).unwrap();
        let mut rng = StdRng::seed_from_u64(7);

        let mut buf = Vec::new();
        serializer.to_bytes(&mut rng, 1024, &mut buf).unwrap();
        assert_eq!(buf, b"third\n");

        buf.clear();
        serializer.to_bytes(&mut rng, 1024, &mut buf).unwrap();
        // After wrapping, we return to the beginning of the stream.
        assert_eq!(buf, b"first\nsecond\n");
    }

    #[test]
    fn random_offset_backtracks_to_group_start() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("backtrack_test.log");
        {
            let mut f = File::create(&path).unwrap();
            // Three lines in the first second (indices 0, 1, 2)
            writeln!(f, "2024-01-01T00:00:00 ts 1 line 0").unwrap();
            writeln!(f, "2024-01-01T00:00:00 ts 1 line 1").unwrap();
            writeln!(f, "2024-01-01T00:00:00 ts 1 line 2").unwrap();
            // Two lines in the second second (indices 3, 4)
            writeln!(f, "2024-01-01T00:00:01 ts 2 line 3").unwrap();
            writeln!(f, "2024-01-01T00:00:01 ts 2 line 4").unwrap();
        }

        // Test find_group_start directly
        let serializer =
            StaticTimestamped::new(&path, "%Y-%m-%dT%H:%M:%S", false, Some(0)).unwrap();

        // Offset 0 is at start of first group -> should return 0
        assert_eq!(serializer.find_group_start(0).unwrap(), 0);
        // Offset 1 is in first group -> should backtrack to 0
        assert_eq!(serializer.find_group_start(1).unwrap(), 0);
        // Offset 2 is in first group -> should backtrack to 0
        assert_eq!(serializer.find_group_start(2).unwrap(), 0);
        // Offset 3 is at start of second group -> should return 3
        assert_eq!(serializer.find_group_start(3).unwrap(), 3);
        // Offset 4 is in second group -> should backtrack to 3
        assert_eq!(serializer.find_group_start(4).unwrap(), 3);
    }
}
