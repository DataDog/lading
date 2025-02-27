//! Static file/directory payload.

use std::{
    fs,
    io::{BufRead, Write},
    path::{Path, PathBuf},
};

use rand::{Rng, prelude::IteratorRandom};
use tracing::debug;

#[derive(Debug)]
struct Source {
    byte_size: u64,
    path: PathBuf,
}

#[derive(Debug)]
/// Static payload
pub struct Static {
    sources: Vec<Source>,
}

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`Static`].
pub enum Error {
    /// IO error
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl Static {
    /// Create a new instance of `Static`
    ///
    /// # Errors
    ///
    /// See documentation for [`Error`]
    pub fn new(path: &Path) -> Result<Self, Error> {
        let mut sources = Vec::with_capacity(16);

        // Attempt to open the path, if this fails we assume that it is a directory.
        let metadata = fs::metadata(path)?;
        if metadata.is_file() {
            debug!("Static path {} is a file.", path.display());
            let byte_size = metadata.len();
            sources.push(Source {
                byte_size,
                path: path.to_owned(),
            });
        } else if metadata.is_dir() {
            debug!("Static path {} is a directory.", path.display());
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                let entry_pth = entry.path();
                debug!("Attempting to open {} as file.", entry_pth.display());
                if let Ok(file) = std::fs::OpenOptions::new().read(true).open(&entry_pth) {
                    let byte_size = file.metadata()?.len();
                    sources.push(Source {
                        byte_size,
                        path: entry_pth.clone(),
                    });
                }
            }
        }

        Ok(Self { sources })
    }
}

impl crate::Serialize for Static {
    fn to_bytes<W, R>(
        &self,
        mut rng: R,
        max_bytes: usize,
        writer: &mut W,
    ) -> Result<(), crate::Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        // Filter available static files to those with size less than
        // max_bytes. Of the remaining, randomly choose one and write it out. We
        // do not change the structure of the file in any respect; it is
        // faithfully transmitted.

        let subset = self
            .sources
            .iter()
            .filter(|src| src.byte_size < max_bytes as u64);
        if let Some(source) = subset.choose(&mut rng) {
            debug!("Opening {} static file.", &source.path.display());
            let file = std::fs::OpenOptions::new().read(true).open(&source.path)?;

            let mut reader = std::io::BufReader::new(file);
            let buffer = reader.fill_buf()?;
            let buffer_length = buffer.len();
            writer.write_all(buffer)?;
            reader.consume(buffer_length);
        }

        Ok(())
    }
}
