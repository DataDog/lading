//! Decode a capture with the frozen reader in an independent process.

mod model;
mod reader;

use std::{
    env, fs,
    io::{self, Write},
};

use crate::reader::read;

fn main() -> io::Result<()> {
    let path = env::args_os()
        .nth(1)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "expected a capture path"))?;
    let records = read(&fs::read(path)?)?;
    let output = serde_json::to_vec(&records).map_err(io::Error::other)?;
    io::stdout().write_all(&output)
}
