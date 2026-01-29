//! `DogStatsD` service check.
use std::fmt;

use rand::{Rng, distr::StandardUniform, prelude::Distribution, seq::IndexedRandom};

use crate::{
    Error, Generator,
    common::strings::{Pool, choose_or_not_ref},
};

use super::{
    StringPools,
    common::{self, tags::Tagset},
};

#[derive(Debug, Clone)]
pub(crate) struct ServiceCheckGenerator {
    pub(crate) names: Vec<String>,
    pub(crate) small_strings: Vec<String>,
    pub(crate) texts_or_messages: Vec<String>,
    pub(crate) tags_generator: common::tags::Generator,
    pub(crate) pools: StringPools,
}

impl<'a> Generator<'a> for ServiceCheckGenerator {
    type Output = ServiceCheck<'a>;
    type Error = Error;

    fn generate<R>(&'a self, mut rng: &mut R) -> Result<Self::Output, Error>
    where
        R: rand::Rng + ?Sized,
    {
        let name = self.names.choose(&mut rng).expect("failed to choose name");
        let hostname = choose_or_not_ref(&mut rng, &self.small_strings).map(String::as_str);
        let message = choose_or_not_ref(&mut rng, &self.texts_or_messages).map(String::as_str);
        let tags = if rng.random() {
            Some(self.tags_generator.generate(&mut rng)?)
        } else {
            None
        };

        Ok(ServiceCheck {
            name,
            status: rng.random(),
            timestamp_second: rng.random_bool(0.5).then(|| rng.random()),
            hostname,
            tags,
            message,
            pools: &self.pools,
        })
    }
}

/// Check of a service.
#[derive(Debug)]
pub struct ServiceCheck<'a> {
    /// Name of the service check.
    pub name: &'a str,
    /// Status of the service check.
    pub status: Status,
    /// Timestamp of the service check.
    pub timestamp_second: Option<u32>,
    /// Hostname of the service check.
    pub hostname: Option<&'a str>,
    /// Tags of the service check.
    pub(crate) tags: Option<Tagset>,
    /// Message of the service check.
    pub message: Option<&'a str>,
    /// String pool for tag handle lookups during serialization.
    pub(crate) pools: &'a StringPools,
}

impl fmt::Display for ServiceCheck<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // _sc|<NAME>|<STATUS>|d:<TIMESTAMP>|h:<HOSTNAME>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|m:<SERVICE_CHECK_MESSAGE>
        write!(
            f,
            "_sc|{name}|{status}",
            name = self.name,
            status = self.status
        )?;
        if let Some(timestamp) = self.timestamp_second {
            write!(f, "|d:{timestamp}")?;
        }
        if let Some(hostname) = self.hostname {
            write!(f, "|h:{hostname}")?;
        }
        if let Some(tags) = &self.tags
            && !tags.is_empty()
        {
            write!(f, "|#")?;
            let mut commas_remaining = tags.len() - 1;
            for tag in tags {
                let key = self
                    .pools
                    .randomstring
                    .using_handle(tag.key)
                    .expect("invalid tag key handle");
                let value = self
                    .pools
                    .tag_name
                    .using_handle(tag.value)
                    .expect("invalid tag value handle");
                write!(f, "{key}:{value}")?;
                if commas_remaining != 0 {
                    write!(f, ",")?;
                    commas_remaining -= 1;
                }
            }
        }
        if let Some(msg) = self.message {
            write!(f, "|m:{msg}")?;
        }
        Ok(())
    }
}

/// Status of a service check.
#[derive(Clone, Copy, Debug)]
pub enum Status {
    /// 'OK' status. Corresponds to 0
    Ok,
    /// 'Warning' status. Corresponds to 1
    Warning,
    /// 'Critical' status. Corresponds to 2
    Critical,
    /// 'Unknown' status. Corresponds to 3
    Unknown,
}

impl Distribution<Status> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> Status
    where
        R: Rng + ?Sized,
    {
        match rng.random_range(0..4) {
            0 => Status::Ok,
            1 => Status::Warning,
            2 => Status::Critical,
            3 => Status::Unknown,
            _ => unreachable!(),
        }
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ok => {
                write!(f, "0")
            }
            Self::Warning => {
                write!(f, "1")
            }
            Self::Critical => {
                write!(f, "2")
            }
            Self::Unknown => {
                write!(f, "3")
            }
        }
    }
}
