//! The `DogStatsD` protocol speaking blackhole.
//!
//! This blackhole receives `DogStatsD` metrics over Unix Domain Sockets (UDS)
//! and emits metrics about what it observes. Strings are interned and live for
//! the lifetime of the program.
//!
//! Known limitations:
//!
//! 1. Only Counter and Gauge metrics are supported.
//! 2. Maximum input packet size of 65 KiB with silent truncation beyond.
//! 3. Maximum tag limit per line of 16, with silent truncation beyond.
//!
//! ## Metrics
//!
//! `bytes_received`: Total bytes received

use std::{io, path::PathBuf, str};

use metrics::{counter, gauge};
use rustc_hash::FxHashSet;
use serde::{Deserialize, Serialize};
use tokio::{fs, net};
use tracing::{error, info, warn};

use super::General;

const MAX_TAGS: usize = 16;

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`Dogstatsd`].
pub enum Error {
    /// Wrapper for [`std::io::Error`].
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
/// Configuration for [`Dogstatsd`].
pub struct Config {
    /// The path of the Unix Domain Socket to read from.
    pub path: PathBuf,
}

#[derive(Debug)]
/// The `Dogstatsd` blackhole. Intended to support the same payloads created in
/// `lading_payload`.
pub struct Dogstatsd {
    path: PathBuf,
    shutdown: lading_signal::Watcher,
    metric_labels: Vec<(String, String)>,
    interner: Interner,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetricKind {
    Counter,
    Gauge,
}

/// String interner for deduplicating metric names and tag strings.
#[derive(Debug)]
struct Interner {
    interned: FxHashSet<&'static str>,
}

impl Interner {
    fn new() -> Self {
        Self {
            interned: FxHashSet::default(),
        }
    }

    fn intern(&mut self, s: &str) -> &'static str {
        if let Some(&interned) = self.interned.get(s) {
            return interned;
        }
        let leaked: &'static str = Box::leak(s.to_string().into_boxed_str());
        self.interned.insert(leaked);
        leaked
    }
}

/// A parsed `DogStatsD` metric.
#[derive(Debug)]
pub struct Metric<'a> {
    name: &'a str,
    kind: MetricKind,
    // technically this should be u64|f64 depending on kind but we accept some
    // loss of accuracy
    value: f64,
    tags: [(&'a str, &'a str); MAX_TAGS],
    tag_count: usize,
}

impl Metric<'_> {
    /// Intern all strings in this metric, converting to `Metric<'static>`.
    fn freeze(self, interner: &mut Interner) -> Metric<'static> {
        let name = interner.intern(self.name);
        let mut tags: [(&'static str, &'static str); MAX_TAGS] = [("", ""); MAX_TAGS];
        for (i, (key, val)) in self.tags.iter().take(self.tag_count).enumerate() {
            tags[i] = (interner.intern(key), interner.intern(val));
        }
        Metric {
            name,
            kind: self.kind,
            value: self.value,
            tags,
            tag_count: self.tag_count,
        }
    }
}

impl Dogstatsd {
    /// Create a new [`Dogstatsd`] server instance
    #[must_use]
    pub fn new(general: General, config: Config, shutdown: lading_signal::Watcher) -> Self {
        let mut metric_labels = vec![
            ("component".to_string(), "blackhole".to_string()),
            ("component_name".to_string(), "dogstatsd".to_string()),
        ];
        if let Some(id) = general.id {
            metric_labels.push(("id".to_string(), id));
        }

        Self {
            path: config.path,
            shutdown,
            metric_labels,
            interner: Interner::new(),
        }
    }

    /// Run [`Dogstatsd`] to completion
    ///
    /// This function runs the UDS server forever, unless a shutdown signal is
    /// received or an unrecoverable error is encountered.
    ///
    /// # Errors
    ///
    /// Function will return an error if receiving a packet fails.
    pub async fn run(mut self) -> Result<(), Error> {
        // Sockets can't be rebound if they existed previously. Delete the
        // socket if it exists before binding.
        let _res = fs::remove_file(&self.path);
        let socket = net::UnixDatagram::bind(&self.path)?;
        let mut buf = vec![0; 65_536];

        let metric_labels = self.metric_labels;
        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);
        loop {
            tokio::select! {
                res = socket.recv(&mut buf) => {
                    let n: usize = res?;
                    counter!("bytes_received", &metric_labels).increment(n as u64);
                    let data = &buf[..n];
                    for metric in Parser::new(data) {
                        let frozen = metric.freeze(&mut self.interner);
                        emit(&frozen);
                    }
                }
                () = &mut shutdown_wait => {
                    info!("shutdown signal received");
                    return Ok(())
                }
            }
        }
    }
}

/// Emit a parsed and frozen metric via metrics-rs.
fn emit(metric: &Metric<'static>) {
    let tags = &metric.tags[..metric.tag_count];
    match metric.kind {
        MetricKind::Counter => {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let value_u64 = metric.value.round() as u64;
            counter!(metric.name, tags).increment(value_u64);
        }
        MetricKind::Gauge => {
            gauge!(metric.name, tags).set(metric.value);
        }
    }
}

/// Iterator over parsed `DogStatsD` metrics from a buffer.
///
/// Only yields Counter and Gauge metrics. If a multi-value Gauge is present
/// only the last value will be emitted. Likewise multi-value Counter instances
/// are summed before being emitted.
#[derive(Debug)]
pub struct Parser<'a> {
    remaining: &'a [u8],
}

impl<'a> Parser<'a> {
    /// Create a new parser for the given data buffer.
    #[must_use]
    pub fn new(data: &'a [u8]) -> Self {
        Self { remaining: data }
    }

    /// Parse a single line into a Metric.
    ///
    /// Returns None if the line is unparsable or unsupported type.
    fn parse_line(line: &'a [u8]) -> Option<Metric<'a>> {
        if line.is_empty() {
            return None;
        }

        let pipe_pos = line.iter().position(|&b| b == b'|')?;
        let name_values = &line[..pipe_pos];

        let colon_pos = name_values.iter().position(|&b| b == b':')?;
        let name_bytes = &name_values[..colon_pos];
        let name = std::str::from_utf8(name_bytes).ok()?;
        let values_bytes = &name_values[colon_pos + 1..];

        let rest = &line[pipe_pos + 1..];
        let mut parts = rest.split(|&b| b == b'|');
        let kind = parse_kind(parts.next()?)?;

        // Parse and aggregate values. Counters are summed up, the last value of
        // the gauge is taken.
        let value = if let Ok(values_str) = str::from_utf8(values_bytes) {
            let values = values_str.split(':');
            match kind {
                MetricKind::Counter => {
                    let mut sum = 0.0;
                    for value_str in values {
                        if let Ok(v) = value_str.parse::<f64>() {
                            sum += v;
                        } else {
                            warn!("Failed to parse counter value: {value_str}");
                        }
                    }
                    sum
                }
                MetricKind::Gauge => {
                    let mut last = 0.0;
                    for value_str in values {
                        if let Ok(v) = value_str.parse::<f64>() {
                            last = v;
                        } else {
                            warn!("Failed to parse gauge value: {value_str}");
                        }
                    }
                    last
                }
            }
        } else {
            0.0
        };

        // Parse tags section if present.
        let mut tags: [(&'a str, &'a str); MAX_TAGS] = [("", ""); MAX_TAGS];
        let mut tag_count = 0;

        for part in parts {
            if !part.is_empty() && part[0] == b'#' {
                let tags_section = &part[1..];
                for tag_bytes in tags_section.split(|&b| b == b',') {
                    if tag_bytes.is_empty() || tag_count >= MAX_TAGS {
                        continue;
                    }
                    if let Ok(tag_str) = std::str::from_utf8(tag_bytes) {
                        if let Some(colon_idx) = tag_str.find(':') {
                            tags[tag_count] = (&tag_str[..colon_idx], &tag_str[colon_idx + 1..]);
                        } else {
                            tags[tag_count] = (tag_str, "");
                        }
                        tag_count += 1;
                    }
                }
                break;
            }
        }

        Some(Metric {
            name,
            kind,
            value,
            tags,
            tag_count,
        })
    }
}

impl<'a> Iterator for Parser<'a> {
    type Item = Metric<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.remaining.is_empty() {
                return None;
            }

            let line_end = self
                .remaining
                .iter()
                .position(|&b| b == b'\n')
                .unwrap_or(self.remaining.len());

            let line = &self.remaining[..line_end];
            self.remaining = if line_end < self.remaining.len() {
                &self.remaining[line_end + 1..]
            } else {
                &[]
            };

            if let Some(metric) = Self::parse_line(line) {
                return Some(metric);
            }
        }
    }
}

/// Parse the metric type from the type bytes.
///
/// Returns None for unsupported types (timer, distribution, set, histogram,
/// unknown).
#[inline]
fn parse_kind(bytes: &[u8]) -> Option<MetricKind> {
    match bytes {
        b"c" => Some(MetricKind::Counter),
        b"g" => Some(MetricKind::Gauge),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_counter() {
        let line = b"metric.name:1|c";
        let mut parser = Parser::new(line);
        let result = parser.next().expect("parse failed");
        assert_eq!(result.name, "metric.name");
        assert_eq!(result.kind, MetricKind::Counter);
        assert_eq!(result.value, 1.0);
    }

    #[test]
    fn parse_gauge() {
        let line = b"gauge.name:42.5|g";
        let mut parser = Parser::new(line);
        let result = parser.next().expect("parse failed");
        assert_eq!(result.name, "gauge.name");
        assert_eq!(result.kind, MetricKind::Gauge);
        assert_eq!(result.value, 42.5);
    }

    #[test]
    fn parse_with_tags() {
        let line = b"metric.name:1|c|#tag1:value1,tag2:value2";
        let mut parser = Parser::new(line);
        let result = parser.next().expect("parse failed");
        assert_eq!(result.name, "metric.name");
        assert_eq!(result.tag_count, 2);
        assert_eq!(result.tags[0], ("tag1", "value1"));
        assert_eq!(result.tags[1], ("tag2", "value2"));
    }

    #[test]
    fn parse_multi_value_counter_sums() {
        let line = b"metric.name:1:2:3|c";
        let mut parser = Parser::new(line);
        let result = parser.next().expect("parse failed");
        assert_eq!(result.value, 6.0);
    }

    #[test]
    fn parse_multi_value_gauge_takes_last() {
        let line = b"metric.name:1:2:3|g";
        let mut parser = Parser::new(line);
        let result = parser.next().expect("parse failed");
        assert_eq!(result.value, 3.0);
    }

    #[test]
    fn parse_invalid_missing_pipe() {
        let line = b"metric.name:1";
        let mut parser = Parser::new(line);
        assert!(parser.next().is_none());
    }

    #[test]
    fn parse_invalid_unknown_type() {
        let line = b"metric.name:1|unknown";
        let mut parser = Parser::new(line);
        assert!(parser.next().is_none());
    }

    #[test]
    fn parser_skips_unsupported_types() {
        let data = b"metric1:1|ms\nmetric2:2|c\nmetric3:3|d\nmetric4:4|g";
        let metrics: Vec<_> = Parser::new(data).collect();
        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0].name, "metric2");
        assert_eq!(metrics[0].kind, MetricKind::Counter);
        assert_eq!(metrics[1].name, "metric4");
        assert_eq!(metrics[1].kind, MetricKind::Gauge);
    }

    #[test]
    fn parse_tag_without_value() {
        let line = b"metric.name:1|c|#region:us-east,important";
        let mut parser = Parser::new(line);
        let result = parser.next().expect("parse failed");
        assert_eq!(result.tag_count, 2);
        assert_eq!(result.tags[0], ("region", "us-east"));
        assert_eq!(result.tags[1], ("important", ""));
    }

    #[test]
    fn parse_exceeds_max_tags() {
        let tags = (0..20)
            .map(|i| format!("tag{i}:val{i}"))
            .collect::<Vec<_>>()
            .join(",");
        let line = format!("metric:1|c|#{tags}");
        let mut parser = Parser::new(line.as_bytes());
        let result = parser.next().expect("parse failed");
        assert_eq!(result.tag_count, MAX_TAGS);
    }

    #[test]
    fn parse_multiple_metrics_in_buffer() {
        let data = b"counter:10|c\ngauge:3.14|g\nignored:1|ms\nanother:5|c|#env:test";
        let metrics: Vec<_> = Parser::new(data).collect();
        assert_eq!(metrics.len(), 3);

        assert_eq!(metrics[0].name, "counter");
        assert_eq!(metrics[0].value, 10.0);
        assert_eq!(metrics[0].kind, MetricKind::Counter);

        assert_eq!(metrics[1].name, "gauge");
        assert_eq!(metrics[1].value, 3.14);
        assert_eq!(metrics[1].kind, MetricKind::Gauge);

        assert_eq!(metrics[2].name, "another");
        assert_eq!(metrics[2].value, 5.0);
        assert_eq!(metrics[2].tag_count, 1);
        assert_eq!(metrics[2].tags[0], ("env", "test"));
    }

    #[test]
    fn parse_empty_buffer() {
        let data = b"";
        let metrics: Vec<_> = Parser::new(data).collect();
        assert_eq!(metrics.len(), 0);
    }

    #[test]
    fn parse_buffer_with_empty_lines() {
        let data = b"\n\nmetric:1|c\n\n";
        let metrics: Vec<_> = Parser::new(data).collect();
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].name, "metric");
    }

    #[test]
    fn parse_negative_values() {
        let line = b"metric:-5.5|g";
        let mut parser = Parser::new(line);
        let result = parser.next().expect("parse failed");
        assert_eq!(result.value, -5.5);
    }

    #[test]
    fn parse_realistic_multi_metric_packet() {
        let data = b"api.request.duration:23.5|g|#endpoint:/users,method:GET\n\
            api.request.count:1|c|#endpoint:/users,method:GET,status:200\n\
            memory.usage:1024.768|g|#host:web-01\n\
            cache.hits:100:150:200|c|#cache:redis\n\
            unsupported:1|h\n\
            error.rate:0.05|g|#severity:warning";

        let metrics: Vec<_> = Parser::new(data).collect();
        assert_eq!(metrics.len(), 5); // 5 valid metrics, 1 skipped

        assert_eq!(metrics[0].name, "api.request.duration");
        assert_eq!(metrics[0].kind, MetricKind::Gauge);
        assert_eq!(metrics[0].value, 23.5);
        assert_eq!(metrics[0].tag_count, 2);
        assert_eq!(metrics[1].name, "api.request.count");
        assert_eq!(metrics[1].kind, MetricKind::Counter);
        assert_eq!(metrics[1].value, 1.0);
        assert_eq!(metrics[1].tag_count, 3);
        assert_eq!(metrics[2].name, "memory.usage");
        assert_eq!(metrics[2].value, 1024.768);
        assert_eq!(metrics[3].name, "cache.hits");
        assert_eq!(metrics[3].value, 450.0); // 100+150+200
        assert_eq!(metrics[4].name, "error.rate");
        assert_eq!(metrics[4].value, 0.05);
    }
}
