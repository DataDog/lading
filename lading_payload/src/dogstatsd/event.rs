use std::fmt;

use rand::{distributions::Standard, prelude::Distribution, seq::SliceRandom, Rng};

use crate::Generator;

use super::{choose_or_not, common};

#[derive(Debug, Clone)]
pub(crate) struct EventGenerator {
    pub(crate) titles: Vec<String>,
    pub(crate) texts_or_messages: Vec<String>,
    pub(crate) small_strings: Vec<String>,
    pub(crate) tagsets: common::tags::Tagsets,
}

impl Generator<Event> for EventGenerator {
    fn generate<R>(&self, mut rng: &mut R) -> Event
    where
        R: rand::Rng + ?Sized,
    {
        let title = self.titles.choose(&mut rng).unwrap().clone();
        let text = self.texts_or_messages.choose(&mut rng).unwrap().clone();
        let tags = choose_or_not(&mut rng, &self.tagsets);

        Event {
            title_utf8_length: title.len(),
            text_utf8_length: text.len(),
            title,
            text,
            timestamp_second: rng.gen(),
            hostname: choose_or_not(&mut rng, &self.small_strings),
            aggregation_key: choose_or_not(&mut rng, &self.small_strings),
            priority: rng.gen(),
            source_type_name: choose_or_not(&mut rng, &self.small_strings),
            alert_type: rng.gen(),
            tags,
        }
    }
}

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
    tags: Option<common::tags::Tagset>,
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
        Ok(())
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
