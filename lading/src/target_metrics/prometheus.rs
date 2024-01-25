//! Prometheus target metrics fetcher
//!
//! This module scrapes Prometheus/OpenMetrics formatted metrics from the target
//! software.
//!

use std::{str::FromStr, time::Duration};

use metrics::{absolute_counter, gauge};
use rustc_hash::FxHashMap;
use serde::Deserialize;
use tracing::{error, info, trace, warn};

use crate::signals::Phase;

#[derive(Debug, Clone, Copy, thiserror::Error)]
/// Errors produced by [`Prometheus`]
pub enum Error {
    /// Prometheus scraper shut down unexpectedly
    #[error("Unexpected shutdown")]
    EarlyShutdown,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
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

/// The `Prometheus` target metrics implementation.
#[derive(Debug)]
pub struct Prometheus {
    config: Config,
    shutdown: Phase,
    experiment_started: Phase,
}

impl Prometheus {
    /// Create a new [`Prometheus`] instance
    ///
    /// This is responsible for scraping metrics from the target process in the
    /// Prometheus format.
    ///
    pub(crate) fn new(config: Config, shutdown: Phase, experiment_started: Phase) -> Self {
        Self {
            config,
            shutdown,
            experiment_started,
        }
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
        let client = reqwest::Client::new();

        let server = async move {
            info!("Prometheus target metrics scraper running, but waiting for warmup to complete");
            self.experiment_started.recv().await; // block until experimental phase entered
            info!("Prometheus target metrics scraper starting collection");
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let Ok(resp) = client
                    .get(&self.config.uri)
                    .timeout(Duration::from_secs(1))
                    .send()
                    .await
                else {
                    info!("failed to get Prometheus uri");
                    continue;
                };

                let Ok(text) = resp.text().await else {
                    info!("failed to read Prometheus response");
                    continue;
                };

                // remember the type for each metric across lines
                let mut typemap = FxHashMap::default();

                // this deserves a real parser, but this will do for now.
                // Format doc: https://github.com/prometheus/docs/blob/main/content/docs/instrumenting/exposition_formats.md
                for line in text.lines() {
                    if line.starts_with("# HELP") {
                        continue;
                    }

                    if line.starts_with("# TYPE") {
                        let mut parts = line.split_ascii_whitespace().skip(2);
                        let name = parts.next().expect("Error: parts iterator is missing name");
                        let metric_type = parts
                            .next()
                            .expect("Error: parts iterator is missing metric type");
                        let metric_type: MetricType = metric_type
                            .parse()
                            .expect("Error: failed to parse metric type");
                        // summary and histogram metrics additionally report names suffixed with _sum, _count, _bucket
                        if matches!(metric_type, MetricType::Histogram | MetricType::Summary) {
                            typemap.insert(format!("{name}_sum"), metric_type);
                            typemap.insert(format!("{name}_count"), metric_type);
                            typemap.insert(format!("{name}_bucket"), metric_type);
                        }
                        typemap.insert(name.to_owned(), metric_type);
                        continue;
                    }

                    let mut parts = line.split_ascii_whitespace();
                    let name_and_labels = parts
                        .next()
                        .expect("Error: parts iterator is missing name and labels");
                    let value = parts
                        .next()
                        .expect("Error: parts iterator is missing value");

                    if value.contains('#') {
                        trace!("Unknown format: {value}");
                        continue;
                    }

                    let (name, labels) = {
                        if let Some((name, labels)) = name_and_labels.split_once('{') {
                            let labels = labels.trim_end_matches('}');
                            let labels = labels.split(',').map(|label| {
                                let (label_name, label_value) = label
                                    .split_once('=')
                                    .expect("Error: label failed to split on first =");
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
                            continue;
                        }
                    }

                    match metric_type {
                        Some(MetricType::Gauge) => {
                            let Ok(value): Result<f64, _> = value.parse() else {
                                let e = value.parse::<f64>().unwrap_err();
                                warn!("{e}: {name} = {value}");
                                continue;
                            };

                            trace!("gauge: {name} = {value}");
                            gauge!(format!("target/{name}"), value, &labels.unwrap_or_default());
                        }
                        Some(MetricType::Counter) => {
                            let Ok(value): Result<f64, _> = value.parse() else {
                                let e = value.parse::<f64>().unwrap_err();
                                warn!("{e}: {name} = {value}");
                                continue;
                            };

                            let value = if value < 0.0 {
                                warn!("Negative counter value unhandled");
                                continue;
                            } else {
                                // clippy shows "error: casting `f64` to `u64` may lose the sign of the value". This is
                                // guarded by the sign check above.
                                if value > u64::MAX as f64 {
                                    warn!("Counter value above maximum limit");
                                    continue;
                                }
                                value as u64
                            };

                            trace!("counter: {name} = {value}");
                            absolute_counter!(
                                format!("target/{name}"),
                                value,
                                &labels.unwrap_or_default()
                            );
                        }
                        Some(_) | None => {
                            trace!("unsupported metric type: {name} = {value}");
                        }
                    }
                }
            }
        };

        tokio::select! {
            _res = server => {
                error!("server shutdown unexpectedly");
                 Err(Error::EarlyShutdown)
            }
            () = self.shutdown.recv() => {
                info!("shutdown signal received");
                 Ok(())
            }
        }
    }
}
