use std::{
    io::{BufRead, Write},
    path::Path,
};

use rand::Rng;

use crate::payload::{Error, Serialize};

#[derive(Debug)]
pub(crate) struct Static<'a> {
    path: &'a Path,
}

impl<'a> Static<'a> {
    #[must_use]
    pub(crate) fn new(path: &'a Path) -> Self {
        Self { path }
    }
}

impl<'a> Serialize for Static<'a> {
    fn to_bytes<W, R>(&self, _rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        // Read lines from `static_path` until such time as the total byte
        // length of the lines read exceeds `bytes_max`. If the path contains
        // more bytes than `bytes_max` the tail of the file will be chopped off.
        let file = std::fs::OpenOptions::new().read(true).open(self.path)?;
        let mut reader = std::io::BufReader::new(file);

        let mut bytes_remaining = max_bytes;
        let mut line = String::new();
        while bytes_remaining > 0 {
            let len = reader.read_line(&mut line)?;
            if len > bytes_remaining {
                break;
            }

            writer.write_all(line.as_bytes())?;
            bytes_remaining = bytes_remaining.saturating_sub(line.len());
        }

        Ok(())
    }
}
