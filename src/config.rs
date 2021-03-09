use byte_unit::{Byte, ByteError};
use serde::Deserialize;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::iter::Iterator;
use std::num::{NonZeroU32, TryFromIntError};
use std::path::PathBuf;
use std::str;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub random_seed: u64,
    pub worker_threads: u16,
    pub targets: HashMap<String, LogTargetTemplate>,
}

#[derive(Debug, Deserialize, Copy, Clone)]
pub enum Variant {
    Constant,
    Ascii,
    Json,
}

#[derive(Debug, Deserialize)]
pub struct LogTargetTemplate {
    pub path_template: String,
    pub duplicates: u8,
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
pub struct LogTarget {
    pub path: PathBuf,
    pub variant: Variant,
    /// Sets the **soft** maximum bytes to be written into the `LogTarget`. This
    /// limit is soft, meaning a burst may go beyond this limit by no more than
    /// `maximum_token_burst`.
    ///
    /// After this limit is breached the target is closed and deleted. A new
    /// target with the same name is created to be written to.
    pub maximum_bytes_per: NonZeroU32,
    /// Defines the number of bytes that are added into the `LogTarget`'s rate
    /// limiting mechanism per second. This sets the maximum bytes that can be
    /// written _continuously_ per second from this target. Higher bursts are
    /// possible as the internal governor accumulates, up to
    /// `maximum_bytes_burst`.
    pub bytes_per_second: NonZeroU32,
    pub maximum_line_size_bytes: NonZeroU32,
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

impl Iterator for LogTargetTemplateIter {
    type Item = LogTarget;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_duplicate >= self.duplicates {
            return None;
        }

        let duplicate = format!("{}", self.current_duplicate);
        let full_path = self.path_template.replace("%NNN%", &duplicate);

        let path = PathBuf::from(full_path);

        self.current_duplicate += 1;

        Some(LogTarget {
            path,
            variant: self.variant,
            maximum_bytes_per: self.maximum_bytes_per,
            bytes_per_second: self.bytes_per_second,
            maximum_line_size_bytes: self.maximum_line_size_bytes,
        })
    }
}

#[derive(Debug)]
pub struct LogTargetTemplateIter {
    path_template: String,
    duplicates: usize,
    current_duplicate: usize,
    variant: Variant,
    maximum_bytes_per: NonZeroU32,
    bytes_per_second: NonZeroU32,
    maximum_line_size_bytes: NonZeroU32,
}

impl LogTargetTemplate {
    pub fn iter(self) -> Result<LogTargetTemplateIter, Error> {
        Ok(LogTargetTemplateIter {
            path_template: self.path_template.clone(),
            current_duplicate: 0,
            duplicates: self.duplicates as usize,
            variant: self.variant,
            maximum_bytes_per: self.maximum_bytes_per()?,
            bytes_per_second: self.bytes_per_second()?,
            maximum_line_size_bytes: self.maximum_line_size_bytes()?,
        })
    }

    /// Determine the `maximum_line_size_bytes` for this [`LogTargetTemplate`]
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

    /// Determine the `bytes_per_second` for this [`LogTargetTemplate`]
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

    /// Determine the `maximum_bytes_per` for this [`LogTargetTemplate`]
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
