use std::fmt;

use arbitrary::{size_hint::and_all, Arbitrary, Unstructured};

use super::common;

pub(crate) struct Event {
    title: common::MetricTagStr,
    text: common::MetricTagStr,
    title_utf8_length: usize,
    text_utf8_length: usize,
    timestamp_second: Option<u32>,
    hostname: Option<common::MetricTagKey>,
    aggregation_key: Option<common::MetricTagKey>,
    priority: Option<Priority>,
    source_type_name: Option<common::MetricTagKey>,
    alert_type: Option<Alert>,
    tags: Option<common::Tags>,
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // _e{<TITLE_UTF8_LENGTH>,<TEXT_UTF8_LENGTH>}:<TITLE>|<TEXT>|d:<TIMESTAMP>|h:<HOSTNAME>|p:<PRIORITY>|t:<ALERT_TYPE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
        write!(
            f,
            "_e{{{title_utf8_length},{text_utf8_length}}}:{title}|{text}",
            title_utf8_length = self.title_utf8_length,
            text_utf8_length = self.text_utf8_length,
            title = self.title,
            text = self.text,
        )?;
        if let Some(timestamp) = self.timestamp_second {
            write!(f, "|d:{timestamp}")?;
        }
        if let Some(ref hostname) = self.hostname {
            write!(f, "|h:{hostname}")?;
        }
        if let Some(ref priority) = self.priority {
            write!(f, "|p:{priority}")?;
        }
        if let Some(ref alert_type) = self.alert_type {
            write!(f, "|t:{alert_type}")?;
        }
        if let Some(ref aggregation_key) = self.aggregation_key {
            write!(f, "|k:{aggregation_key}")?;
        }
        if let Some(ref source_type_name) = self.source_type_name {
            write!(f, "|s:{source_type_name}")?;
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
        Ok(())
    }
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

#[derive(Arbitrary)]
enum Priority {
    Normal,
    Low,
}

impl fmt::Display for Priority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Normal => write!(f, "normal"),
            Self::Low => write!(f, "low"),
        }
    }
}

#[derive(Clone, Copy, Arbitrary)]
enum Alert {
    Error,
    Warning,
    Info,
    Success,
}

impl fmt::Display for Alert {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Error => write!(f, "error"),
            Self::Warning => write!(f, "warning"),
            Self::Info => write!(f, "info"),
            Self::Success => write!(f, "success"),
        }
    }
}
