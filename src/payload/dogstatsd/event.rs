use std::fmt;

use rand::{distributions::Standard, prelude::Distribution, Rng};

use super::common;

pub(crate) struct Event {
    title: String,
    text: String,
    title_utf8_length: usize,
    text_utf8_length: usize,
    timestamp_second: Option<u32>,
    hostname: Option<String>,
    aggregation_key: Option<String>,
    priority: Option<Priority>,
    source_type_name: Option<String>,
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

impl Distribution<Event> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Event
    where
        R: Rng + ?Sized,
    {
        let title = format!("{}", rng.gen::<u64>());
        let text = format!("{}", rng.gen::<u64>());

        let source_type_name = if rng.gen() {
            Some(format!("{}", rng.gen_range(0..32)))
        } else {
            None
        };
        let aggregation_key = if rng.gen() {
            Some(format!("{}", rng.gen_range(0..32)))
        } else {
            None
        };
        let hostname = if rng.gen() {
            Some(format!("{}", rng.gen_range(0..32)))
        } else {
            None
        };

        Event {
            title_utf8_length: title.len(),
            text_utf8_length: text.len(),
            title,
            text,
            timestamp_second: rng.gen(),
            hostname,
            aggregation_key,
            priority: rng.gen(),
            source_type_name,
            alert_type: rng.gen(),
            tags: rng.gen(),
        }
    }
}

enum Priority {
    Normal,
    Low,
}

impl Distribution<Priority> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Priority
    where
        R: Rng + ?Sized,
    {
        match rng.gen_range(0..2) {
            0 => Priority::Low,
            1 => Priority::Normal,
            _ => unreachable!(),
        }
    }
}

impl fmt::Display for Priority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Normal => write!(f, "normal"),
            Self::Low => write!(f, "low"),
        }
    }
}

#[derive(Clone, Copy)]
enum Alert {
    Error,
    Warning,
    Info,
    Success,
}

impl Distribution<Alert> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Alert
    where
        R: Rng + ?Sized,
    {
        match rng.gen_range(0..4) {
            0 => Alert::Error,
            1 => Alert::Warning,
            2 => Alert::Info,
            3 => Alert::Success,
            _ => unreachable!(),
        }
    }
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
