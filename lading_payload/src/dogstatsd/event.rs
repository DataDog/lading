//! `DogStatsD` event.
use std::{fmt, ops::Range};

use rand::{Rng, distr::StandardUniform, prelude::Distribution};

use crate::{Error, Generator, common::strings};

use self::strings::{Pool, choose_or_not_fn};

use super::{ConfRange, StringPools, common};

#[derive(Debug, Clone)]
pub(crate) struct EventGenerator {
    pub(crate) title_length: ConfRange<u16>,
    pub(crate) texts_or_messages_length_range: Range<u16>,
    pub(crate) small_strings_length_range: Range<u16>,
    pub(crate) pools: StringPools,
    pub(crate) tags_generator: common::tags::Generator,
}

impl<'a> Generator<'a> for EventGenerator {
    type Output = Event<'a>;
    type Error = Error;

    fn generate<R>(&'a self, mut rng: &mut R) -> Result<Self::Output, Error>
    where
        R: rand::Rng + ?Sized,
    {
        let title_sz = self.title_length.sample(&mut rng) as usize;
        let title = self
            .pools
            .str_pool
            .of_size(&mut rng, title_sz)
            .ok_or(Error::StringGenerate)?;
        let text = self
            .pools
            .str_pool
            .of_size_range(&mut rng, self.texts_or_messages_length_range.clone())
            .ok_or(Error::StringGenerate)?;
        let tags = if rng.random() {
            Some(self.tags_generator.generate(&mut rng)?)
        } else {
            None
        };

        Ok(Event {
            title_utf8_length: title.len(),
            text_utf8_length: text.len(),
            title,
            text,
            timestamp_second: rng.random_bool(0.5).then(|| rng.random()),
            hostname: choose_or_not_fn(&mut rng, |r| {
                self.pools
                    .str_pool
                    .of_size_range(r, self.small_strings_length_range.clone())
            }),
            aggregation_key: choose_or_not_fn(&mut rng, |r| {
                self.pools
                    .str_pool
                    .of_size_range(r, self.small_strings_length_range.clone())
            }),
            priority: rng.random_bool(0.5).then(|| rng.random()),
            source_type_name: choose_or_not_fn(&mut rng, |r| {
                self.pools
                    .str_pool
                    .of_size_range(r, self.small_strings_length_range.clone())
            }),
            alert_type: rng.random_bool(0.5).then(|| rng.random()),
            tags,
            pools: &self.pools,
        })
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
    /// Tags of the event
    pub(crate) tags: Option<common::tags::Tagset>,
    /// String pool for tag handle lookups during serialization.
    pub(crate) pools: &'a StringPools,
}

impl fmt::Display for Event<'_> {
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
        if let Some(tags) = &self.tags
            && !tags.is_empty()
        {
            write!(f, "|#")?;
            let mut commas_remaining = tags.len() - 1;
            for tag in tags {
                let key = self
                    .pools
                    .tag_name_pool
                    .using_handle(tag.key)
                    .expect("invalid tag key handle");
                let value = self
                    .pools
                    .str_pool
                    .using_handle(tag.value)
                    .expect("invalid tag value handle");
                write!(f, "{key}:{value}")?;
                if commas_remaining != 0 {
                    write!(f, ",")?;
                    commas_remaining -= 1;
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

impl Distribution<Priority> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> Priority
    where
        R: Rng + ?Sized,
    {
        match rng.random_range(0..2) {
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

impl Distribution<Alert> for StandardUniform {
    fn sample<R>(&self, rng: &mut R) -> Alert
    where
        R: Rng + ?Sized,
    {
        match rng.random_range(0..4) {
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
