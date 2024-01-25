//! `DogStatsD` event.
use std::{fmt, ops::Range, rc::Rc};

use rand::{distributions::Standard, prelude::Distribution, Rng};

use crate::{common::strings, Generator};

use super::{choose_or_not_fn, common, ConfRange};

#[derive(Debug, Clone)]
pub(crate) struct EventGenerator {
    pub(crate) title_length: ConfRange<u16>,
    pub(crate) texts_or_messages_length_range: Range<u16>,
    pub(crate) small_strings_length_range: Range<u16>,
    pub(crate) str_pool: Rc<strings::Pool>,
    pub(crate) tags_generator: common::tags::Generator,
}

impl<'a> Generator<'a> for EventGenerator {
    type Output = Event<'a>;

    fn generate<R>(&'a self, mut rng: &mut R) -> Self::Output
    where
        R: rand::Rng + ?Sized,
    {
        let title_sz = self.title_length.sample(&mut rng) as usize;
        let title = self
            .str_pool
            .of_size(&mut rng, title_sz)
            .expect("failed to generate string");
        let text = self
            .str_pool
            .of_size_range(&mut rng, self.texts_or_messages_length_range.clone())
            .expect("failed to generate string");
        let tags = if rng.gen() {
            Some(self.tags_generator.generate(&mut rng))
        } else {
            None
        };

        Event {
            title_utf8_length: title.len(),
            text_utf8_length: text.len(),
            title,
            text,
            timestamp_second: rng.gen(),
            hostname: choose_or_not_fn(&mut rng, |r| {
                self.str_pool
                    .of_size_range(r, self.small_strings_length_range.clone())
            }),
            aggregation_key: choose_or_not_fn(&mut rng, |r| {
                self.str_pool
                    .of_size_range(r, self.small_strings_length_range.clone())
            }),
            priority: rng.gen(),
            source_type_name: choose_or_not_fn(&mut rng, |r| {
                self.str_pool
                    .of_size_range(r, self.small_strings_length_range.clone())
            }),
            alert_type: rng.gen(),
            tags,
        }
    }
}

/// An event, like a syslog kind of.
#[derive(Debug)]
pub struct Event<'a> {
    /// Title of the event.
    pub title: &'a str,
    /// Text of the event.
    pub text: &'a str,
    /// Length of the title in UTF-8 bytes.
    pub title_utf8_length: usize,
    /// Length of the text in UTF-8 bytes.
    pub text_utf8_length: usize,
    /// Timestamp of the event (in seconds from unix epoch)
    pub timestamp_second: Option<u32>,
    /// Hostname of the event.
    pub hostname: Option<&'a str>,
    /// Aggregation key of the event.
    pub aggregation_key: Option<&'a str>,
    /// Priority of the event.
    pub priority: Option<Priority>,
    /// Source type name of the event.
    pub source_type_name: Option<&'a str>,
    /// Alert type of the event.
    pub alert_type: Option<Alert>,
    /// Tags of the event.
    pub tags: Option<common::tags::Tagset>,
}

impl<'a> fmt::Display for Event<'a> {
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
        if let Some(hostname) = self.hostname {
            write!(f, "|h:{hostname}")?;
        }
        if let Some(priority) = self.priority {
            write!(f, "|p:{priority}")?;
        }
        if let Some(alert_type) = self.alert_type {
            write!(f, "|t:{alert_type}")?;
        }
        if let Some(aggregation_key) = self.aggregation_key {
            write!(f, "|k:{aggregation_key}")?;
        }
        if let Some(source_type_name) = self.source_type_name {
            write!(f, "|s:{source_type_name}")?;
        }
        if let Some(tags) = &self.tags {
            if !tags.is_empty() {
                write!(f, "|#")?;
                let mut commas_remaining = tags.len() - 1;
                for tag in tags {
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

/// Priority of an event.
#[derive(Clone, Copy, Debug)]
pub enum Priority {
    /// Normal priority. (default)
    Normal,
    /// Low priority.
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

/// Alert Type associated with an event
#[derive(Clone, Copy, Debug)]
pub enum Alert {
    /// Error alert type.
    Error,
    /// Warning alert type.
    Warning,
    /// Info alert type.
    Info,
    /// Success alert type.
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
