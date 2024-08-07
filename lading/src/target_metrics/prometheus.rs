//! Prometheus target metrics fetcher
//!
//! This module scrapes Prometheus/OpenMetrics formatted metrics from the target
//! software.
//!

use std::{str::FromStr, time::Duration};

use metrics::{counter, gauge};
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
    client: reqwest::Client,
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
        let client = reqwest::Client::new();
        Self {
            config,
            client,
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
        let mut shutdown = self.shutdown.clone();
        let server = async {
            info!("Prometheus target metrics scraper running, but waiting for warmup to complete");
            self.experiment_started.recv().await; // block until experimental phase entered
            info!("Prometheus target metrics scraper starting collection");
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                self.scrape_metrics().await;
            }
        };

        tokio::select! {
            _res = server => {
                error!("server shutdown unexpectedly");
                 Err(Error::EarlyShutdown)
            }
            () = shutdown.recv() => {
                info!("shutdown signal received");
                 Ok(())
            }
        }
    }

    #[allow(
        clippy::too_many_lines,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    async fn scrape_metrics(&self) {
        let Ok(resp) = self
            .client
            .get(&self.config.uri)
            .timeout(Duration::from_secs(1))
            .send()
            .await
        else {
            info!("failed to get Prometheus uri");
            return;
        };

        let Ok(text) = resp.text().await else {
            info!("failed to read Prometheus response");
            return;
        };

        // remember the type for each metric across lines
        let mut typemap = FxHashMap::default();

        // this deserves a real parser, but this will do for now.
        // Format doc: https://github.com/prometheus/docs/blob/main/content/docs/instrumenting/exposition_formats.md
        for line in text.lines().filter_map(|l| {
            let line = l.trim();
            if line.is_empty() {
                None
            } else {
                Some(line)
            }
        }) {
            if line.starts_with("# HELP") {
                continue;
            }

            if line.starts_with("# TYPE") {
                let mut parts = line.split_ascii_whitespace().skip(2);
                let name = parts.next().expect("parts iterator is missing name");
                let metric_type = parts.next().expect("parts iterator is missing metric type");
                let metric_type: MetricType =
                    metric_type.parse().expect("failed to parse metric type");
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
                .expect("parts iterator is missing name and labels");
            let value = parts.next().expect("parts iterator is missing value");

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
                            .expect("label failed to split on first =");
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
                        let e = value.parse::<f64>().expect("failed to parse gauge value");
                        warn!("{e}: {name} = {value}");
                        continue;
                    };

                    let handle = gauge!(format!("target/{name}"), &labels.unwrap_or_default());
                    handle.set(value);
                }
                Some(MetricType::Counter) => {
                    let Ok(value): Result<f64, _> = value.parse() else {
                        let e = value.parse::<f64>().expect("failed to parse counter value");
                        warn!("{e}: {name} = {value}");
                        continue;
                    };

                    let value = if value < 0.0 {
                        warn!("Negative counter value unhandled");
                        continue;
                    } else {
                        if value > u64::MAX as f64 {
                            warn!("Counter value above maximum limit");
                            continue;
                        }
                        // clippy shows "error: casting `f64` to `u64` may lose the sign of the value".
                        // This is guarded by the sign check above.
                        value as u64
                    };

                    trace!("counter: {name} = {value}");
                    let handle = counter!(format!("target/{name}"), &labels.unwrap_or_default());
                    handle.absolute(value);
                }
                Some(_) => {
                    trace!("unsupported metric type: {name} = {value}");
                }
                None => {
                    warn!("Couldn't find metric type for {name}");
                }
            }
        }
    }
}

#[allow(clippy::needless_raw_string_hashes)]
#[allow(clippy::mutable_key_type)]
#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use metrics::{Key, Label};
    use metrics_util::{CompositeKey, MetricKind};
    use warp;
    use warp::Filter;

    const SINGLE_GAUGE_TWO_SERIES: &str = r#"
    # HELP workloadmeta_stored_entities Number of entities in the store.
    # TYPE workloadmeta_stored_entities gauge
    workloadmeta_stored_entities{kind="container",source="node_orchestrator"} 35
    workloadmeta_stored_entities{kind="container",source="runtime"} 36
    "#;

    const COUNT_ZERO_LABELS: &str = r#"
    # TYPE request_count counter
    request_count 1027
    "#;

    const GAUGE_ONE_LABEL: &str = r#"
    # TYPE memory_usage_bytes gauge
    memory_usage_bytes{process="test"} 5264384
    "#;

    async fn run_scrape_and_parse_metrics(
        s: &str,
    ) -> HashMap<
        CompositeKey,
        (
            Option<metrics::Unit>,
            Option<metrics::SharedString>,
            metrics_util::debugging::DebugValue,
        ),
    > {
        let s = s.to_string();
        let server = warp::serve(
            warp::path("metrics")
                .map(move || warp::reply::with_status(s.clone(), warp::http::StatusCode::OK)),
        );

        let (addr, serve_fut) = server.bind_ephemeral(([127, 0, 0, 1], 0));
        let _server_handle = tokio::spawn(serve_fut);

        let server_uri = format!("http://{addr}/metrics");

        let shutdown = Phase::new();
        let experiment_started = Phase::new();
        let p = Prometheus::new(
            Config {
                uri: server_uri,
                metrics: None,
            },
            shutdown.clone(),
            experiment_started.clone(),
        );

        let dr = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = dr.snapshotter();
        dr.install().expect("failed to install recorder");

        experiment_started.signal();

        p.scrape_metrics().await;

        snapshotter.snapshot().into_hashmap()
    }

    #[tokio::test]
    async fn test_gauge_with_two_series() {
        let snapshot = run_scrape_and_parse_metrics(SINGLE_GAUGE_TWO_SERIES).await;

        assert_eq!(snapshot.len(), 2);

        let metric_one = snapshot
            .get(&CompositeKey::new(
                MetricKind::Gauge,
                Key::from_parts(
                    "target/workloadmeta_stored_entities",
                    vec![
                        Label::new("kind", "container"),
                        Label::new("source", "node_orchestrator"),
                    ],
                ),
            ))
            .expect("metric not found");
        match metric_one.2 {
            metrics_util::debugging::DebugValue::Gauge(ordered_float) => {
                assert_eq!(ordered_float, 35.0);
            }
            _ => panic!("unexpected metric type"),
        }

        let metric_two = snapshot
            .get(&CompositeKey::new(
                MetricKind::Gauge,
                Key::from_parts(
                    "target/workloadmeta_stored_entities",
                    vec![
                        Label::new("kind", "container"),
                        Label::new("source", "runtime"),
                    ],
                ),
            ))
            .expect("metric not found");
        match metric_two.2 {
            metrics_util::debugging::DebugValue::Gauge(ordered_float) => {
                assert_eq!(ordered_float, 36.0);
            }
            _ => panic!("unexpected metric type"),
        }
    }

    #[tokio::test]
    async fn test_count_zero_labels() {
        let snapshot = run_scrape_and_parse_metrics(COUNT_ZERO_LABELS).await;

        assert_eq!(snapshot.len(), 1);

        let metric_one = snapshot
            .get(&CompositeKey::new(
                MetricKind::Counter,
                Key::from_parts("target/request_count", vec![]),
            ))
            .expect("metric not found");
        match metric_one.2 {
            metrics_util::debugging::DebugValue::Counter(v) => {
                assert_eq!(v, 1027);
            }
            _ => panic!("unexpected metric type"),
        }
    }
    #[tokio::test]
    async fn test_count_one_labels() {
        let snapshot = run_scrape_and_parse_metrics(GAUGE_ONE_LABEL).await;

        assert_eq!(snapshot.len(), 1);

        let metric_one = snapshot
            .get(&CompositeKey::new(
                MetricKind::Gauge,
                Key::from_parts(
                    "target/memory_usage_bytes",
                    vec![Label::new("process", "test")],
                ),
            ))
            .expect("metric not found");
        match metric_one.2 {
            metrics_util::debugging::DebugValue::Gauge(v) => {
                assert_eq!(v, 5_264_384_f64);
            }
            _ => panic!("unexpected metric type"),
        }
    }
}
