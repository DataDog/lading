use std::mem;

use arbitrary::{
    size_hint::{self, and_all},
    Unstructured,
};

use super::common;

pub(crate) struct ServiceCheck {
    name: common::MetricTagStr,
    status: Status,
    timestamp_second: Option<u32>,
    hostname: Option<common::MetricTagStr>,
    tags: Option<common::Tags>,
    message: Option<common::MetricTagStr>,
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

#[derive(Clone, Copy)]
enum Status {
    Ok,
    Warning,
    Critical,
    Unknown,
}

impl<'a> arbitrary::Arbitrary<'a> for Status {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let options = [
            Status::Ok,
            Status::Warning,
            Status::Critical,
            Status::Unknown,
        ];
        Ok(*u.choose(&options)?)
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        (mem::size_of::<Self>(), Some(mem::size_of::<Self>()))
    }
}
