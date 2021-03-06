use byte_unit::{Byte, ByteError};
use serde::Deserialize;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub random_seed: u64,
    pub targets: HashMap<String, LogTarget>,
}

#[derive(Debug, Deserialize)]
pub struct LogTarget {
    pub path: PathBuf,
    /// Sets the **soft** maximum bytes to be written into the `LogTarget`. This
    /// limit is soft, meaning a burst may go beyond this limit by no more than
    /// `maximum_token_burst`.
    ///
    /// After this limit is breached the target is closed and deleted. A new
    /// target with the same name is created to be written to.
    maximum_bytes_per: String,
    /// Defines the number of bytes that are added into the `LogTarget`'s rate
    /// limiting mechanism per second. This sets the maximum bytes that can be
    /// written _continuously_ per second from this target. Higher bursts are
    /// possible as the internal governor accumulates, up to
    /// `maximum_bytes_burst`.
    bytes_per_second: String,
    maximum_bytes_burst: String,
}

#[derive(Debug)]
pub enum Error {
    ByteError(ByteError),
}

impl From<ByteError> for Error {
    fn from(error: ByteError) -> Self {
        Error::ByteError(error)
    }
}

impl LogTarget {
    pub fn maximum_bytes_burst(&self) -> Result<NonZeroU32, Error> {
        let bytes = Byte::from_str(&self.maximum_bytes_burst)?;
        Ok(NonZeroU32::new(bytes.get_bytes() as u32).expect("maximum_bytes_burst must not be 0"))
    }

    pub fn bytes_per_second(&self) -> Result<NonZeroU32, Error> {
        let bytes = Byte::from_str(&self.bytes_per_second)?;
        Ok(NonZeroU32::new(bytes.get_bytes() as u32).expect("bytes_per_second must not be 0"))
    }

    pub fn maximum_bytes_per(&self) -> Result<NonZeroU32, Error> {
        let bytes = Byte::from_str(&self.maximum_bytes_per)?;
        Ok(NonZeroU32::new(bytes.get_bytes() as u32).expect("maximum_bytes_per must not be 0"))
    }
}
