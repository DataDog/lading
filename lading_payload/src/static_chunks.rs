//! Static file/directory payload, that auto-chunks files into the maximum bytes per block.

use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use rand::Rng;
use tracing::debug;

#[derive(Debug)]
/// Static payload
pub struct StaticChunks {
    files: Vec<PathBuf>,
    current_file_idx: usize,
    current_reader: Option<BufReader<File>>,
    pending_line: Option<Vec<u8>>,
    initialized: bool,
}

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`StaticChunks`].
pub enum Error {
    /// IO error
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// No lines were discovered in the provided path
    #[error("No lines found in static path")]
    NoLines,
}

impl StaticChunks {
    /// Create a new instance of `StaticChunks`
    ///
    /// # Errors
    ///
    /// See documentation for [`Error`]
    pub fn new(path: &Path) -> Result<Self, Error> {
        let metadata = fs::metadata(path)?;
        let mut files = Vec::new();

        if metadata.is_file() {
            debug!("Static path {} is a file.", path.display());
            files.push(path.to_path_buf());
        } else if metadata.is_dir() {
            debug!("Static path {} is a directory.", path.display());
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                let entry_pth: PathBuf = entry.path();
                debug!("Attempting to open {} as file.", entry_pth.display());
                if entry_pth.is_file() {
                    files.push(entry_pth);
                }
            }
        }

        if files.is_empty() {
            return Err(Error::NoLines);
        }

        // Build instance and grab the first line so we can error early if all files are empty
        let mut this = Self {
            files,
            current_file_idx: 0,
            current_reader: None,
            pending_line: None,
            initialized: false,
        };

        let mut buf = String::new();
        if let Some(line) = this.read_next_line(&mut buf)? {
            this.pending_line = Some(line);
        } else {
            return Err(Error::NoLines);
        }

        Ok(this)
    }

    fn initialize_start_position<R: Rng + ?Sized>(&mut self, rng: &mut R) -> std::io::Result<()> {
        if self.files.is_empty() {
            return Ok(());
        }
        self.current_file_idx = rng.random_range(0..self.files.len());
        let path = &self.files[self.current_file_idx];
        let file = OpenOptions::new().read(true).open(path)?;
        let len = file.metadata()?.len();
        let mut reader = BufReader::new(file);

        if len > 0 {
            let offset = rng.random_range(0..len);
            if offset > 0 {
                reader.seek(SeekFrom::Start(offset))?;
                // Discard the partial line we likely landed in.
                let mut discard = String::new();
                let _ = reader.read_line(&mut discard)?;
            }
        }

        // Capture the first line from this randomized position; if none, leave
        // pending_line empty and let normal iteration advance.
        let mut first = String::new();
        loop {
            first.clear();
            let read = reader.read_line(&mut first)?;
            if read == 0 {
                if len == 0 {
                    break;
                }
                // Wrapped to start; try again from beginning.
                reader.seek(SeekFrom::Start(0))?;
                continue;
            }
            if first.ends_with('\n') {
                first.pop();
                if first.ends_with('\r') {
                    first.pop();
                }
            }
            self.pending_line = Some(first.as_bytes().to_vec());
            break;
        }

        self.current_reader = Some(reader);
        self.initialized = true;
        Ok(())
    }

    fn ensure_reader(&mut self) -> std::io::Result<()> {
        if self.current_reader.is_none() {
            let file = OpenOptions::new()
                .read(true)
                .open(&self.files[self.current_file_idx])?;
            self.current_reader = Some(BufReader::new(file));
        }
        Ok(())
    }

    fn advance_file(&mut self) -> std::io::Result<()> {
        self.current_file_idx = (self.current_file_idx + 1) % self.files.len();
        let file = OpenOptions::new()
            .read(true)
            .open(&self.files[self.current_file_idx])?;
        self.current_reader = Some(BufReader::new(file));
        Ok(())
    }

    // Returns the next line (without trailing newline) or None if every file is empty.
    fn read_next_line(&mut self, buf: &mut String) -> std::io::Result<Option<Vec<u8>>> {
        // Try each file at most once; if we fail to read a line from all of them,
        // we consider the dataset empty.
        let files_len = self.files.len();
        let mut attempts = 0;
        while attempts < files_len {
            self.ensure_reader()?;
            let reader = self
                .current_reader
                .as_mut()
                .expect("reader should be present after ensure_reader");

            buf.clear();
            let read = reader.read_line(buf)?;
            if read == 0 {
                // EOF: move to next file; if we've wrapped around without data, bail out
                self.advance_file()?;
                attempts += 1;
                continue;
            }

            if buf.ends_with('\n') {
                buf.pop();
                if buf.ends_with('\r') {
                    buf.pop();
                }
            }
            return Ok(Some(buf.as_bytes().to_vec()));
        }
        Ok(None)
    }
}

impl crate::Serialize for StaticChunks {
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
        if !self.initialized {
            self.initialize_start_position(&mut rng)?;
        }

        let mut bytes_written = 0usize;
        let mut buf = String::new();

        while bytes_written < max_bytes {
            let line = if let Some(pending) = self.pending_line.take() {
                Some(pending)
            } else {
                self.read_next_line(&mut buf)?
            };

            let Some(line) = line else {
                break;
            };

            let needed = line.len() + 1; // newline

            if needed > max_bytes && bytes_written == 0 {
                // Skip oversized line and move on.
                debug!(
                    "Skipping oversized line ({} bytes incl. newline) for max_bytes={}",
                    needed, max_bytes
                );
                continue;
            }

            if bytes_written + needed > max_bytes {
                break;
            }

            writer.write_all(&line)?;
            writer.write_all(b"\n")?;
            bytes_written += needed;
        }

        Ok(())
    }
}
