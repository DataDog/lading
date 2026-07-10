//! Static file/directory payload.

use std::{
    fs::{self, OpenOptions},
    io::{self, Write},
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
                if let Ok(file) = OpenOptions::new().read(true).open(&entry_pth) {
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
        &mut self,
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
            let mut file = OpenOptions::new().read(true).open(&source.path)?;
            io::copy(&mut file, writer)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use rand::{SeedableRng, rngs::StdRng};

    use super::Static;
    use crate::Serialize;

    proptest! {
        /// For every non-empty file smaller than the maximum block size, serialization
        /// returns every source byte in its original order.
        #[test]
        fn transmits_complete_binary_file(bytes in prop::collection::vec(any::<u8>(), 8_193..131_072)) {
            let directory = tempfile::tempdir()?;
            let path = directory.path().join("payload.bin");
            std::fs::write(&path, &bytes)?;
            let mut payload = Static::new(&path)?;
            let mut output = Vec::new();
            let rng = StdRng::seed_from_u64(1);

            payload.to_bytes(rng, bytes.len() + 1, &mut output)?;

            prop_assert_eq!(output, bytes);
        }
    }
}
