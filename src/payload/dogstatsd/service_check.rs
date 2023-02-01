use std::fmt;

use arbitrary::{size_hint::and_all, Arbitrary, Unstructured};

use super::common;

pub(crate) struct ServiceCheck {
    name: common::MetricTagKey,
    status: Status,
    timestamp_second: Option<u32>,
    hostname: Option<common::MetricTagKey>,
    tags: Option<common::Tags>,
    message: Option<common::MetricTagStr>,
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

impl<'a> arbitrary::Arbitrary<'a> for ServiceCheck {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self {
            name: u.arbitrary()?,
            status: u.arbitrary()?,
            timestamp_second: u.arbitrary()?,
            hostname: u.arbitrary()?,
            tags: u.arbitrary()?,
            message: u.arbitrary()?,
        })
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        let name_sz = common::MetricTagStr::size_hint(depth);
        let status_sz = Status::size_hint(depth);
        let timestamp_sz = u32::size_hint(depth);
        let hostname_sz = common::MetricTagStr::size_hint(depth);
        let tags_sz = common::Tags::size_hint(depth);
        let message_sz = common::MetricTagStr::size_hint(depth);

        and_all(&[
            name_sz,
            status_sz,
            timestamp_sz,
            hostname_sz,
            tags_sz,
            message_sz,
        ])
    }
}

#[derive(Clone, Copy, Arbitrary)]
enum Status {
    Ok,
    Warning,
    Critical,
    Unknown,
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
