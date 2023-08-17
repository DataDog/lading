use std::fmt;

use rand::{distributions::Standard, prelude::Distribution, seq::SliceRandom, Rng};

use crate::Generator;

use super::{choose_or_not_ref, common};

#[derive(Debug, Clone)]
pub(crate) struct ServiceCheckGenerator {
    pub(crate) names: Vec<String>,
    pub(crate) small_strings: Vec<String>,
    pub(crate) texts_or_messages: Vec<String>,
    pub(crate) tagsets: common::tags::Tagsets,
}

impl<'a> Generator<'a> for ServiceCheckGenerator {
    type Output = ServiceCheck<'a>;

    fn generate<R>(&'a self, mut rng: &mut R) -> Self::Output
    where
        R: rand::Rng + ?Sized,
    {
        let name = self.names.choose(&mut rng).unwrap();
        let hostname = choose_or_not_ref(&mut rng, &self.small_strings).map(String::as_str);
        let message = choose_or_not_ref(&mut rng, &self.texts_or_messages).map(String::as_str);
        let tags = choose_or_not_ref(&mut rng, &self.tagsets);

        ServiceCheck {
            name,
            status: rng.gen(),
            timestamp_second: rng.gen(),
            hostname,
            tags,
            message,
        }
    }
}

/// Check of a service.
#[derive(Debug)]
pub struct ServiceCheck<'a> {
    name: &'a str,
    status: Status,
    timestamp_second: Option<u32>,
    hostname: Option<&'a str>,
    tags: Option<&'a Vec<String>>,
    message: Option<&'a str>,
}

impl<'a> fmt::Display for ServiceCheck<'a> {
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
        if let Some(tags) = self.tags {
            if !tags.is_empty() {
                write!(f, "|#")?;
                let mut commas_remaining = tags.len() - 1;
                for tag in tags.iter() {
                    write!(f, "{tag}")?;
                    if commas_remaining != 0 {
                        write!(f, ",")?;
                        commas_remaining -= 1;
                    }
                }
            }
        }
        if let Some(msg) = self.message {
            write!(f, "|m:{msg}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
enum Status {
    Ok,
    Warning,
    Critical,
    Unknown,
}

impl Distribution<Status> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Status
    where
        R: Rng + ?Sized,
    {
        match rng.gen_range(0..4) {
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
