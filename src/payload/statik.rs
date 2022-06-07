use std::{
    fs,
    io::{BufRead, Write},
    path::{Path, PathBuf},
};

use rand::{prelude::IteratorRandom, Rng};

use crate::payload::{Error, Serialize};

#[derive(Debug)]
struct Source {
    byte_size: u64,
    path: PathBuf,
}

#[derive(Debug)]
pub(crate) struct Static {
    sources: Vec<Source>,
}

impl Static {
    #[must_use]
    pub(crate) fn new(path: &Path) -> Self {
        let mut sources = Vec::with_capacity(16);

        if path.is_file() {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .open(path)
                .expect("could not open file");
            let byte_size = file.metadata().expect("could not read file metadata").len();
            sources.push(Source {
                byte_size,
                path: path.to_owned(),
            });
        } else if path.is_dir() {
            for entry in fs::read_dir(path).expect("could not read directory") {
                let entry = entry.unwrap();
                let entry_pth = entry.path();
                if entry_pth.is_dir() {
                    // intentionally skip sub-directories
                    continue;
                } else {
                    let file = std::fs::OpenOptions::new()
                        .read(true)
                        .open(&entry_pth)
                        .expect("could not open file");
                    let byte_size = file.metadata().expect("could not read file metadata").len();
                    sources.push(Source {
                        byte_size,
                        path: entry_pth.to_owned(),
                    });
                }
            }
        } else {
            panic!("discovered a path to something that is neither file nor directory");
        }

        Self { sources }
    }
}

impl Serialize for Static {
    fn to_bytes<W, R>(&self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
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
            // Read lines from `static_path` until such time as the total byte
            // length of the lines read exceeds `bytes_max`. If the path contains
            // more bytes than `bytes_max` the tail of the file will be chopped off.
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
