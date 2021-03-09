use byte_unit::{Byte, ByteError};
use serde::Deserialize;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::num::{NonZeroU32, TryFromIntError};
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub random_seed: u64,
    pub worker_threads: u16,
    pub targets: HashMap<String, LogTarget>,
}

#[derive(Debug, Deserialize)]
pub enum Variant {
    Constant,
    Ascii,
    Json,
}

#[derive(Debug, Deserialize)]
pub struct LogTarget {
    pub path: PathBuf,
    pub variant: Variant,
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
    maximum_line_size_bytes: String,
}

#[derive(Debug)]
pub enum Error {
    ByteError(ByteError),
    TryFromInt(TryFromIntError),
}

impl From<ByteError> for Error {
    fn from(error: ByteError) -> Self {
        Error::ByteError(error)
    }
}

impl From<TryFromIntError> for Error {
    fn from(error: TryFromIntError) -> Self {
        Error::TryFromInt(error)
    }
}

impl LogTarget {
    /// Determine the `maximum_line_size_bytes` for this [`LogTarget`]
    ///
    /// Parses the user's supplied stringy number into a non-zero u32 of bytes.
    ///
    /// # Errors
    ///
    /// If the users supplies anything other than a stringy number plus some
    /// recognizable unit this function will return an error. Likewise if the
    /// user supplies a number that is larger than `u32::MAX` bytes this
    /// function will return an error.
    pub fn maximum_line_size_bytes(&self) -> Result<NonZeroU32, Error> {
        let bytes = Byte::from_str(&self.maximum_line_size_bytes)?;
        Ok(NonZeroU32::new(u32::try_from(bytes.get_bytes())?)
            .expect("maximum_line_size_bytes must not be 0"))
    }

    /// Determine the `bytes_per_second` for this [`LogTarget`]
    ///
    /// Parses the user's supplied stringy number into a non-zero u32 of bytes.
    ///
    /// # Errors
    ///
    /// If the users supplies anything other than a stringy number plus some
    /// recognizable unit this function will return an error. Likewise if the
    /// user supplies a number that is larger than `u32::MAX` bytes this
    /// function will return an error.
    pub fn bytes_per_second(&self) -> Result<NonZeroU32, Error> {
        let bytes = Byte::from_str(&self.bytes_per_second)?;
        Ok(NonZeroU32::new(u32::try_from(bytes.get_bytes())?)
            .expect("bytes_per_second must not be 0"))
    }

    /// Determine the `maximum_bytes_per` for this [`LogTarget`]
    ///
    /// Parses the user's supplied stringy number into a non-zero u32 of bytes.
    ///
    /// # Errors
    ///
    /// If the users supplies anything other than a stringy number plus some
    /// recognizable unit this function will return an error. Likewise if the
    /// user supplies a number that is larger than `u32::MAX` bytes this
    /// function will return an error.
    pub fn maximum_bytes_per(&self) -> Result<NonZeroU32, Error> {
        let bytes = Byte::from_str(&self.maximum_bytes_per)?;
        Ok(NonZeroU32::new(u32::try_from(bytes.get_bytes())?)
            .expect("maximum_bytes_per must not be 0"))
    }
}
