//! Prometheus target metrics fetcher
//!
//! This module scrapes Prometheus/OpenMetrics formatted metrics from the target
//! software.
//!

pub mod parser;

use std::time::Duration;

use metrics::{counter, gauge};
use rustc_hash::FxHashMap;
use serde::Deserialize;
use tracing::{error, info, trace, warn};

use self::parser::{MetricType, Parser};

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
    /// Optional additional tags to label target metrics
    tags: Option<FxHashMap<String, String>>,
}

/// The `Prometheus` target metrics implementation.
#[derive(Debug)]
pub struct Prometheus {
    config: Config,
    client: reqwest::Client,
    shutdown: lading_signal::Watcher,
    experiment_started: lading_signal::Watcher,
    sample_period: Duration,
}

impl Prometheus {
    /// Create a new [`Prometheus`] instance
    ///
    /// This is responsible for scraping metrics from the target process in the
    /// Prometheus format.
    ///
    pub(crate) fn new(
        config: Config,
        shutdown: lading_signal::Watcher,
        experiment_started: lading_signal::Watcher,
        sample_period: Duration,
    ) -> Self {
        let client = reqwest::Client::new();
        Self {
            config,
            client,
            shutdown,
            experiment_started,
            sample_period,
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
    pub(crate) async fn run(self) -> Result<(), Error> {
        info!("Prometheus target metrics scraper running, but waiting for warmup to complete");
        self.experiment_started.recv().await;
        info!(
            "Prometheus target metrics scraper starting collection at {:?} interval",
            self.sample_period
        );

        let client = self.client;
        let uri = self.config.uri;
        let tags = self.config.tags;
        let metrics = self.config.metrics;

        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);

        let mut poll = tokio::time::interval(self.sample_period);

        loop {
            tokio::select! {
                _ = poll.tick() => {
                    scrape_metrics(&client, &uri, tags.as_ref(), metrics.as_ref()).await;
                }
                () = &mut shutdown_wait => {
                    info!("shutdown signal received");
                    return Ok(());
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn scrape_metrics(&self) {
        scrape_metrics(
            &self.client,
            &self.config.uri,
            self.config.tags.as_ref(),
            self.config.metrics.as_ref(),
        )
        .await;
    }
}

#[allow(
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
pub(crate) async fn scrape_metrics(
    client: &reqwest::Client,
    uri: &str,
    tags: Option<&FxHashMap<String, String>>,
    metrics: Option<&Vec<String>>,
) {
    let Ok(resp) = client.get(uri).timeout(Duration::from_secs(1)).send().await else {
        info!("failed to get Prometheus {uri}");
        return;
    };

    let Ok(text) = resp.text().await else {
        info!("failed to read Prometheus response from {uri}");
        return;
    };

    let mut parser = Parser::new();
    let parsed_metrics = parser.parse_text(&text);

    for metric_result in parsed_metrics {
        let parsed_metric = match metric_result {
            Ok(m) => m,
            Err(e) => {
                trace!("Failed to parse metric: {:?}", e);
                continue;
            }
        };

        let name = parsed_metric.name.replace("__", ".");

        if let Some(metrics) = metrics {
            if !metrics.contains(&name) {
                continue;
            }
        }

        // Add lading labels including user defined tags for this endpoint
        let all_labels: Option<Vec<(String, String)>>;
        if let Some(tags) = tags {
            let mut additional_labels = Vec::new();
            for (tag_name, tag_val) in tags {
                additional_labels.push((tag_name.clone(), tag_val.clone()));
            }
            if let Some(labels) = parsed_metric.labels {
                // Convert borrowed label data to owned
                let owned_labels: Vec<(String, String)> = labels
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.into_owned()))
                    .collect();
                all_labels = Some([owned_labels, additional_labels].concat());
            } else {
                all_labels = Some(additional_labels);
            }
        } else {
            // Convert borrowed label data to owned if present
            all_labels = parsed_metric.labels.map(|labels| {
                labels
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.into_owned()))
                    .collect()
            });
        }

        let value = parsed_metric.value;

        match parsed_metric.metric_type {
            Some(MetricType::Gauge) => {
                if value.is_nan() {
                    warn!("Skipping NaN gauge value");
                    continue;
                }

                gauge!(format!("target/{name}"), &all_labels.unwrap_or_default()).set(value);
            }
            Some(MetricType::Counter) => {
                if value.is_nan() {
                    warn!("Skipping NaN counter value");
                    continue;
                }

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
                counter!(format!("target/{name}"), &all_labels.unwrap_or_default()).absolute(value);
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

#[allow(clippy::needless_raw_string_hashes)] // Test data is more readable with consistent raw string format
#[allow(clippy::mutable_key_type)] // CompositeKey has interior mutability
#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use metrics::{Key, Label};
    use metrics_util::{CompositeKey, MetricKind};
    use rustc_hash::FxHasher;
    use std::hash::BuildHasherDefault;
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

    const GAUGE_LABEL_WITH_SPACES: &str = r#"
    # TYPE vector_build_info gauge
    vector_build_info{arch="aarch64",debug="false",host="d0cf527728fe",revision="745babd 2024-09-11 14:55:36.802851761",rust_version="1.78",version="0.41.1"} 1 1729113558073
    "#;

    const GAUGE_INVALID_VALUE: &str = r#"
    # TYPE memory_usage_bytes gauge
    memory_usage_bytes{process="test"} foobar
    "#;

    const COUNTER_INVALID_VALUE: &str = r#"
    # TYPE memory_usage_bytes counter
    memory_usage_bytes{process="test"} foobar
    "#;

    async fn run_scrape_and_parse_metrics(
        s: &str,
        tags: Option<HashMap<String, String, BuildHasherDefault<FxHasher>>>,
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

        let (shutdown_watcher, _) = lading_signal::signal();
        let (experiment_started_watcher, experiment_started_broadcaster) = lading_signal::signal();
        let sample_period = Duration::from_secs(1);
        let p = Prometheus::new(
            Config {
                uri: server_uri,
                metrics: None,
                tags,
            },
            shutdown_watcher,
            experiment_started_watcher,
            sample_period,
        );

        let dr = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = dr.snapshotter();
        dr.install().expect("failed to install recorder");

        experiment_started_broadcaster.signal();

        p.scrape_metrics().await;

        snapshotter.snapshot().into_hashmap()
    }

    #[tokio::test]
    async fn test_gauge_with_two_series() {
        let tags = None;
        let snapshot = run_scrape_and_parse_metrics(SINGLE_GAUGE_TWO_SERIES, tags).await;

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
        let tags = None;
        let snapshot = run_scrape_and_parse_metrics(COUNT_ZERO_LABELS, tags).await;

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
        let tags = None;
        let snapshot = run_scrape_and_parse_metrics(GAUGE_ONE_LABEL, tags).await;

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

    #[tokio::test]
    async fn test_count_one_labels_with_sub_agent_label() {
        let mut tags: FxHashMap<String, String> = FxHashMap::default();
        tags.insert("sub-agent".to_string(), "testing-agent".to_string());
        let snapshot = run_scrape_and_parse_metrics(GAUGE_ONE_LABEL, Some(tags)).await;

        assert_eq!(snapshot.len(), 1);

        let metric_one = snapshot
            .get(&CompositeKey::new(
                MetricKind::Gauge,
                Key::from_parts(
                    "target/memory_usage_bytes",
                    vec![
                        Label::new("process", "test"),
                        Label::new("sub-agent", "testing-agent"),
                    ],
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

    #[tokio::test]
    async fn test_gauge_label_with_spaces() {
        let tags = None;
        let snapshot = run_scrape_and_parse_metrics(GAUGE_LABEL_WITH_SPACES, tags).await;

        assert_eq!(snapshot.len(), 1);

        let metric = snapshot
            .get(&CompositeKey::new(
                MetricKind::Gauge,
                Key::from_parts(
                    "target/vector_build_info",
                    vec![
                        Label::new("arch", "aarch64"),
                        Label::new("debug", "false"),
                        Label::new("host", "d0cf527728fe"),
                        Label::new("revision", "745babd 2024-09-11 14:55:36.802851761"),
                        Label::new("rust_version", "1.78"),
                        Label::new("version", "0.41.1"),
                    ],
                ),
            ))
            .expect("metric not found");
        match metric.2 {
            metrics_util::debugging::DebugValue::Gauge(ordered_float) => {
                assert_eq!(ordered_float, 1.0);
            }
            _ => panic!("unexpected metric type"),
        }
    }

    #[tokio::test]
    async fn test_gauge_invalid_value() {
        let snapshot = run_scrape_and_parse_metrics(GAUGE_INVALID_VALUE, None).await;

        assert_eq!(snapshot.len(), 0);
    }

    #[tokio::test]
    async fn test_counter_invalid_value() {
        let snapshot = run_scrape_and_parse_metrics(COUNTER_INVALID_VALUE, None).await;

        assert_eq!(snapshot.len(), 0);
    }
}
