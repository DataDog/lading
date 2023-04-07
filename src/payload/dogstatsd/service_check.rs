use std::fmt;

use rand::{distributions::Standard, prelude::Distribution, Rng};

use crate::payload::{common::AsciiString, Generator};

use super::common;

pub(crate) struct ServiceCheck {
    name: String,
    status: Status,
    timestamp_second: Option<u32>,
    hostname: Option<String>,
    tags: Option<common::Tags>,
    message: Option<String>,
}

impl Distribution<ServiceCheck> for Standard {
    fn sample<R>(&self, rng: &mut R) -> ServiceCheck
    where
        R: Rng + ?Sized,
    {
        let name = format!("{}", rng.gen::<u64>());
        let hostname = if rng.gen() {
            Some(format!("{}", rng.gen_range(0..32)))
        } else {
            None
        };
        let message = if rng.gen() {
            Some(AsciiString::default().generate(rng).unwrap())
        } else {
            None
        };

        ServiceCheck {
            name,
            status: rng.gen(),
            timestamp_second: rng.gen(),
            hostname,
            tags: rng.gen(),
            message,
        }
    }
}

impl fmt::Display for ServiceCheck {
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
        if let Some(ref hostname) = self.hostname {
            write!(f, "|h:{hostname}")?;
        }
        if let Some(ref tags) = self.tags {
            if !tags.inner.is_empty() {
                write!(f, "|#")?;
                let mut commas_remaining = tags.inner.len() - 1;
                for (k, v) in &tags.inner {
                    write!(f, "{k}:{v}")?;
                    if commas_remaining != 0 {
                        write!(f, ",")?;
                        commas_remaining -= 1;
                    }
                }
            }
        }
        if let Some(ref msg) = self.message {
            write!(f, "|m:{msg}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
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
