use std::mem;

use arbitrary::{size_hint::and_all, Unstructured};

use super::common;

pub(crate) struct Event {
    title: common::MetricTagStr,
    text: common::MetricTagStr,
    title_utf8_length: usize,
    text_utf8_length: usize,
    timestamp_second: Option<u32>,
    hostname: Option<common::MetricTagStr>,
    aggregation_key: Option<common::MetricTagStr>,
    priority: Option<Priority>,
    source_type_name: Option<common::MetricTagStr>,
    alert_type: Option<Alert>,
    tags: Option<common::Tags>,
}

impl<'a> arbitrary::Arbitrary<'a> for Event {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let title: common::MetricTagStr = u.arbitrary()?;
        let text: common::MetricTagStr = u.arbitrary()?;
        Ok(Self {
            title_utf8_length: title.len(),
            text_utf8_length: text.len(),
            title,
            text,
            timestamp_second: u.arbitrary()?,
            hostname: u.arbitrary()?,
            aggregation_key: u.arbitrary()?,
            priority: u.arbitrary()?,
            source_type_name: u.arbitrary()?,
            alert_type: u.arbitrary()?,
            tags: u.arbitrary()?,
        })
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        let title_sz = common::MetricTagStr::size_hint(depth);
        let text_sz = common::MetricTagStr::size_hint(depth);
        let title_len_sz = usize::size_hint(depth);
        let text_len_sz = usize::size_hint(depth);
        let timestamp_sz = u32::size_hint(depth);
        let hostname_sz = common::MetricTagStr::size_hint(depth);
        let aggregation_sz = common::MetricTagStr::size_hint(depth);
        let priority_sz = Priority::size_hint(depth);
        let source_type_sz = common::MetricTagStr::size_hint(depth);
        let alert_sz = Alert::size_hint(depth);
        let tags = common::Tags::size_hint(depth);

        and_all(&[
            title_sz,
            text_sz,
            title_len_sz,
            text_len_sz,
            timestamp_sz,
            hostname_sz,
            aggregation_sz,
            priority_sz,
            source_type_sz,
            alert_sz,
            tags,
        ])
    }
}

enum Priority {
    Normal,
    Low,
}

impl<'a> arbitrary::Arbitrary<'a> for Priority {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let is_normal = u.arbitrary()?;
        if is_normal {
            Ok(Self::Normal)
        } else {
            Ok(Self::Low)
        }
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        (mem::size_of::<Self>(), Some(mem::size_of::<Self>()))
    }
}

#[derive(Clone, Copy)]
enum Alert {
    Error,
    Warning,
    Info,
    Success,
}

impl<'a> arbitrary::Arbitrary<'a> for Alert {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let options = [Alert::Error, Alert::Warning, Alert::Info, Alert::Success];
        Ok(*u.choose(&options)?)
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        (mem::size_of::<Self>(), Some(mem::size_of::<Self>()))
    }
}
