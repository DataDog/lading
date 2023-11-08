//! Prometheus target metrics fetcher
//!
//! This module scrapes Prometheus/OpenMetrics formatted metrics from the target
//! software.
//!

use std::{str::FromStr, time::Duration};

use futures::TryStreamExt;
use metrics::{absolute_counter, gauge, register_histogram};
use rustc_hash::FxHashMap;
use serde::Deserialize;
use tokio::io::AsyncBufReadExt;
use tokio_util::io::StreamReader;
use tracing::{error, info, trace, warn};

use crate::signals::Shutdown;

#[derive(Debug, thiserror::Error)]
/// Errors produced by [`Prometheus`]
pub enum Error {
    /// Prometheus scraper shut down unexpectedly
    #[error("Unexpected shutdown")]
    EarlyShutdown,
    /// IO Error
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
/// Configuration for collecting Prometheus based target metrics
pub struct Config {
    /// URI to scrape
    uri: String,
    /// Metric names to scrape. Leave unset to scrape all metrics.
    metrics: Option<Vec<String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetricType {
    Gauge,
    Counter,
    Histogram,
    Summary,
}

#[derive(Debug)]
enum MetricTypeParseError {
    UnknownType,
}

impl FromStr for MetricType {
    type Err = MetricTypeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "counter" => Ok(Self::Counter),
            "gauge" => Ok(Self::Gauge),
            "histogram" => Ok(Self::Histogram),
            "summary" => Ok(Self::Summary),
            _ => Err(MetricTypeParseError::UnknownType),
        }
    }
}

/// Intentionally dump the reqwest error but log it out.
#[inline]
#[allow(clippy::needless_pass_by_value)]
fn convert_err(err: reqwest::Error) -> std::io::Error {
    error!("reqwest error: {err}");
    std::io::Error::new(std::io::ErrorKind::Other, "reqwest error")
}

/// The `Prometheus` target metrics implementation.
#[derive(Debug)]
pub struct Prometheus {
    config: Config,
    shutdown: Shutdown,
}

impl Prometheus {
    /// Create a new [`Prometheus`] instance
    ///
    /// This is responsible for scraping metrics from the target process in the
    /// Prometheus format.
    ///
    pub(crate) fn new(config: Config, shutdown: Shutdown) -> Self {
        Self { config, shutdown }
    }

    /// Run this [`Server`] to completion
    ///
    /// Scrape metrics from the target at 1Hz.
    ///
    /// # Errors
    ///
    /// None are known.
    ///
    /// # Panics
    ///
    /// None are known.
    #[allow(clippy::cast_sign_loss)]
    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::too_many_lines)]
    pub(crate) async fn run(mut self) -> Result<(), Error> {
        info!("Prometheus target metrics scraper running");
        let client = reqwest::Client::new();

        let mut timer = tokio::time::interval(Duration::from_secs(1));

        let content_length =
            register_histogram!("target_metrics_content_length", "source" => "prometheus");

        loop {
            tokio::select! {
                _ = timer.tick() => {
                    let Ok(resp) = client.get(&self.config.uri).timeout(Duration::from_secs(1)).send().await else {
                        info!("failed to get Prometheus uri");
                        continue;
                    };
                    if let Some(cl) = resp.content_length() {
                    content_length.record(cl as f64);
                    }

                    let reader = StreamReader::new(resp.bytes_stream().map_err(convert_err));
                    let mut lines = reader.lines();

                    // Used to preserve types across prometheus lines. We could
                    // potentially elide this if we adjusted our parser to have
                    // look-ahead, rather than being line by line. Or
                    // constrained the lines to require types just prior to the
                    // values.
                    let mut typemap = FxHashMap::default();
                    while let Some(line) = lines.next_line().await? {
                        // Process the line immediately
                        self.process_line(&line, &mut typemap);
                    }
                    gauge!("total_client_prometheus_metrics", typemap.len() as f64);
                }
                _ = self.shutdown.recv() => {
                    info!("shutdown signal received");
                    break;
                }
            }
        }

        Ok(())
    }

    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_sign_loss)]
    fn process_line(&self, line: &str, typemap: &mut FxHashMap<String, MetricType>) {
        // this deserves a real parser, but this will do for now.
        // Format doc: https://github.com/prometheus/docs/blob/main/content/docs/instrumenting/exposition_formats.md
        if line.starts_with("# HELP") {
            return;
        }

        if line.starts_with("# TYPE") {
            let mut parts = line.split_ascii_whitespace().skip(2);
            let name = parts.next().unwrap();
            let metric_type = parts.next().unwrap();
            let metric_type: MetricType = metric_type.parse().unwrap();
            // summary and histogram metrics additionally report names suffixed with _sum, _count, _bucket
            if matches!(metric_type, MetricType::Histogram | MetricType::Summary) {
                typemap.insert(format!("{name}_sum"), metric_type);
                typemap.insert(format!("{name}_count"), metric_type);
                typemap.insert(format!("{name}_bucket"), metric_type);
            }
            typemap.insert(name.to_owned(), metric_type);
            return;
        }

        let mut parts = line.split_ascii_whitespace();
        let name_and_labels = parts.next().unwrap();
        let value = parts.next().unwrap();

        if value.contains('#') {
            trace!("Unknown format: {value}");
            return;
        }

        let (name, labels) = {
            if let Some((name, labels)) = name_and_labels.split_once('{') {
                let labels = labels.trim_end_matches('}');
                let labels = labels.split(',').map(|label| {
                    let (label_name, label_value) = label.split_once('=').unwrap();
                    let label_value = label_value.trim_matches('\"');
                    (label_name.to_owned(), label_value.to_owned())
                });
                let labels = labels.collect::<Vec<_>>();
                (name, Some(labels))
            } else {
                (name_and_labels, None)
            }
        };

        let metric_type = typemap.get(name);
        let name = name.replace("__", ".");

        if let Some(metrics) = &self.config.metrics {
            if !metrics.contains(&name) {
                return;
            }
        }

        match metric_type {
            Some(MetricType::Gauge) => {
                let Ok(value): Result<f64, _> = value.parse() else {
                        let e = value.parse::<f64>().unwrap_err();
                        warn!("{e}: {name} = {value}");
                        return;
                    };

                trace!("gauge: {name} = {value}");
                gauge!(format!("target/{name}"), value, &labels.unwrap_or_default());
            }
            Some(MetricType::Counter) => {
                let Ok(value): Result<f64, _> = value.parse() else {
                        let e = value.parse::<f64>().unwrap_err();
                        warn!("{e}: {name} = {value}");
                        return;
                    };

                let value = if value < 0.0 {
                    warn!("Negative counter value unhandled");
                    return;
                } else {
                    // clippy shows "error: casting `f64` to `u64` may lose the sign of the value". This is
                    // guarded by the sign check above.
                    if value > u64::MAX as f64 {
                        warn!("Counter value above maximum limit");
                        return;
                    }
                    value as u64
                };

                trace!("counter: {name} = {value}");
                absolute_counter!(format!("target/{name}"), value, &labels.unwrap_or_default());
            }
            Some(_) | None => {
                trace!("unsupported metric type: {name} = {value}");
            }
        }
    }
}
