//! Prometheus target metrics fetcher
//!
//! This module scrapes Prometheus/OpenMetrics formatted metrics from the target
//! software.
//!

use std::{str::FromStr, time::Duration};

use metrics::{counter, gauge};
use once_cell::sync::Lazy;
use regex::Regex;
use rustc_hash::FxHashMap;
use serde::Deserialize;
use tracing::{error, info, trace, warn};

// Regex to match Prometheus label pairs: label_name="label_value"
// The value can be empty (e.g., label="")
static LABEL_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"(\w+)="([^"]*)""#).expect("Failed to compile label regex"));

#[derive(Debug, Clone, Copy, thiserror::Error)]
/// Errors produced by [`Prometheus`]
pub enum Error {
    /// Prometheus scraper shut down unexpectedly
    #[error("Unexpected shutdown")]
    EarlyShutdown,
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
/// Configuration for collecting Prometheus based target metrics
pub struct Config {
    /// URI to scrape
    pub(crate) uri: String,
    /// Metric names to scrape. Leave unset to scrape all metrics.
    pub(crate) metrics: Option<Vec<String>>,
    /// Optional additional tags to label target metrics
    pub(crate) tags: Option<FxHashMap<String, String>>,
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

    parse_prometheus_metrics(&text, tags, metrics);
}

#[allow(
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
pub(crate) fn parse_prometheus_metrics(
    text: &str,
    tags: Option<&FxHashMap<String, String>>,
    metrics: Option<&Vec<String>>,
) {
    // remember the type for each metric across lines
    let mut typemap = FxHashMap::default();

    // this deserves a real parser, but this will do for now.
    // Format doc: https://github.com/prometheus/docs/blob/main/content/docs/instrumenting/exposition_formats.md
    for line in text.lines().filter_map(|l| {
        let line = l.trim();
        if line.is_empty() { None } else { Some(line) }
    }) {
        if line.starts_with("# HELP") {
            continue;
        }

        if line.starts_with("# TYPE") {
            let mut parts = line.split_ascii_whitespace().skip(2);
            let name = parts.next().expect("parts iterator is missing name");
            let metric_type = parts.next().expect("parts iterator is missing metric type");
            let metric_type: MetricType = metric_type.parse().expect("failed to parse metric type");
            // summary and histogram metrics additionally report names suffixed with _sum, _count, _bucket
            if matches!(metric_type, MetricType::Histogram | MetricType::Summary) {
                typemap.insert(format!("{name}_sum"), metric_type);
                typemap.insert(format!("{name}_count"), metric_type);
                typemap.insert(format!("{name}_bucket"), metric_type);
            }
            typemap.insert(name.to_owned(), metric_type);
            continue;
        }

        let mut parts = if line.contains('}') {
            line.split_inclusive('}').collect::<Vec<&str>>()
        } else {
            // line contains no labels
            line.split_ascii_whitespace().collect::<Vec<&str>>()
        }
        .into_iter();

        let name_and_labels = parts
            .next()
            .expect("parts iterator is missing name and labels");
        let value = parts
            .next()
            .expect("parts iterator is missing value")
            .split_ascii_whitespace()
            .next()
            .expect("parts iterator is missing value");

        if value.contains('#') {
            trace!("Unknown format: {value}");
            continue;
        }

        let (name, labels) = {
            if let Some((name, labels_str)) = name_and_labels.split_once('{') {
                let labels_str = labels_str.trim_end_matches('}');
                let labels: Vec<(String, String)> = LABEL_REGEX
                    .captures_iter(labels_str)
                    .map(|cap| {
                        let label_name = cap
                            .get(1)
                            .expect("regex should have label name capture group")
                            .as_str();
                        let label_value = cap
                            .get(2)
                            .expect("regex should have label value capture group")
                            .as_str();
                        (label_name.to_owned(), label_value.to_owned())
                    })
                    .collect();
                (name, Some(labels))
            } else {
                (name_and_labels, None)
            }
        };

        // Add lading labels including user defined tags for this endpoint
        let all_labels: Option<Vec<(String, String)>>;
        if let Some(tags) = tags {
            let mut additional_labels = Vec::new();
            for (tag_name, tag_val) in tags {
                additional_labels.push((tag_name.clone(), tag_val.clone()));
            }
            if let Some(labels) = labels {
                all_labels = Some([labels, additional_labels].concat());
            } else {
                all_labels = Some(additional_labels);
            }
        } else {
            all_labels = labels;
        }

        let metric_type = typemap.get(name);
        let name = name.replace("__", ".");

        if let Some(metrics) = metrics
            && !metrics.contains(&name)
        {
            continue;
        }

        match metric_type {
            Some(MetricType::Gauge) => {
                let value: f64 = match value.parse() {
                    Ok(v) => v,
                    Err(e) => {
                        warn!("failed to parse gauge value {value} for metric {name}: {e}");
                        continue;
                    }
                };

                if value.is_nan() {
                    warn!("Skipping NaN gauge value");
                    continue;
                }

                gauge!(format!("target/{name}"), &all_labels.unwrap_or_default()).set(value);
            }
            Some(MetricType::Counter) => {
                let value: f64 = match value.parse() {
                    Ok(v) => v,
                    Err(e) => {
                        warn!("failed to parse counter value {value} for metric {name}: {e}");
                        continue;
                    }
                };

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

#[allow(clippy::needless_raw_string_hashes)]
#[allow(clippy::mutable_key_type)]
#[cfg(test)]
mod tests {
    use std::{collections::HashMap, net};

    use super::*;
    use bytes::Bytes;
    use http_body_util::Full;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Request, Response, StatusCode};
    use hyper_util::rt::TokioIo;
    use metrics::{Key, Label};
    use metrics_util::{CompositeKey, MetricKind, debugging};
    use rustc_hash::FxHasher;
    use std::hash::BuildHasherDefault;
    use tokio::{net::TcpListener, sync::oneshot};

    fn parse_and_get_metrics(
        s: &str,
        tags: Option<HashMap<String, String, BuildHasherDefault<FxHasher>>>,
    ) -> HashMap<
        CompositeKey,
        (
            Option<metrics::Unit>,
            Option<metrics::SharedString>,
            debugging::DebugValue,
        ),
    > {
        let dr = debugging::DebuggingRecorder::new();
        let snapshotter = dr.snapshotter();

        metrics::with_local_recorder(&dr, || {
            parse_prometheus_metrics(s, tags.as_ref(), None);
        });

        snapshotter.snapshot().into_hashmap()
    }

    async fn run_scrape_and_parse_metrics(
        s: &str,
        tags: Option<HashMap<String, String, BuildHasherDefault<FxHasher>>>,
    ) -> HashMap<
        CompositeKey,
        (
            Option<metrics::Unit>,
            Option<metrics::SharedString>,
            debugging::DebugValue,
        ),
    > {
        let s = s.to_string();

        // Avoid a race with the underlying server by signaling with this
        // oneshot, underlying server will serve as a test /metrics endpoint.
        let (tx, rx) = oneshot::channel();

        let _server_handle = tokio::spawn(async move {
            // Bind an ephemeral port
            let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            listener.set_nonblocking(true).unwrap();

            let listener = TcpListener::from_std(listener).unwrap();
            let _ = tx.send(addr);
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let io = TokioIo::new(stream);
                let s = s.clone();

                tokio::spawn(async move {
                    let service = service_fn(move |req: Request<hyper::body::Incoming>| {
                        let s = s.clone();
                        async move {
                            if req.uri().path() == "/metrics" {
                                Ok::<_, hyper::Error>(
                                    Response::builder()
                                        .status(StatusCode::OK)
                                        .body(Full::new(Bytes::from(s)))
                                        .unwrap(),
                                )
                            } else {
                                Ok(Response::builder()
                                    .status(StatusCode::NOT_FOUND)
                                    .body(Full::new(Bytes::new()))
                                    .unwrap())
                            }
                        }
                    });

                    let _ = http1::Builder::new().serve_connection(io, service).await;
                });
            }
        });

        let addr = rx.await.unwrap();
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

        let dr = debugging::DebuggingRecorder::new();
        let snapshotter = dr.snapshotter();
        let _guard = metrics::set_default_local_recorder(&dr);

        experiment_started_broadcaster.signal();

        p.scrape_metrics().await;

        snapshotter.snapshot().into_hashmap()
    }

    #[test]
    fn test_gauge_with_two_series() {
        let prom_text = r#"
        # HELP workloadmeta_stored_entities Number of entities in the store.
        # TYPE workloadmeta_stored_entities gauge
        workloadmeta_stored_entities{kind="container",source="node_orchestrator"} 35
        workloadmeta_stored_entities{kind="container",source="runtime"} 36
        "#;

        let tags = None;
        let snapshot = parse_and_get_metrics(prom_text, tags);

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
            debugging::DebugValue::Gauge(ordered_float) => {
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
            debugging::DebugValue::Gauge(ordered_float) => {
                assert_eq!(ordered_float, 36.0);
            }
            _ => panic!("unexpected metric type"),
        }
    }

    #[test]
    fn test_count_zero_labels() {
        let prom_text = r#"
        # TYPE request_count counter
        request_count 1027
        "#;

        let tags = None;
        let snapshot = parse_and_get_metrics(prom_text, tags);

        assert_eq!(snapshot.len(), 1);

        let metric_one = snapshot
            .get(&CompositeKey::new(
                MetricKind::Counter,
                Key::from_parts("target/request_count", vec![]),
            ))
            .expect("metric not found");
        match metric_one.2 {
            debugging::DebugValue::Counter(v) => {
                assert_eq!(v, 1027);
            }
            _ => panic!("unexpected metric type"),
        }
    }

    #[test]
    fn test_count_one_labels() {
        let prom_text = r#"
        # TYPE memory_usage_bytes gauge
        memory_usage_bytes{process="test"} 5264384
        "#;

        let tags = None;
        let snapshot = parse_and_get_metrics(prom_text, tags);

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
            debugging::DebugValue::Gauge(v) => {
                assert_eq!(v, 5_264_384_f64);
            }
            _ => panic!("unexpected metric type"),
        }
    }

    #[test]
    fn test_count_one_labels_with_sub_agent_label() {
        let prom_text = r#"
        # TYPE memory_usage_bytes gauge
        memory_usage_bytes{process="test"} 5264384
        "#;

        let mut tags: FxHashMap<String, String> = FxHashMap::default();
        tags.insert("sub-agent".to_string(), "testing-agent".to_string());
        let snapshot = parse_and_get_metrics(prom_text, Some(tags));

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
            debugging::DebugValue::Gauge(v) => {
                assert_eq!(v, 5_264_384_f64);
            }
            _ => panic!("unexpected metric type"),
        }
    }

    #[test]
    fn test_gauge_label_with_spaces() {
        let prom_text = r#"
        # TYPE vector_build_info gauge
        vector_build_info{arch="aarch64",debug="false",host="d0cf527728fe",revision="745babd 2024-09-11 14:55:36.802851761",rust_version="1.78",version="0.41.1"} 1 1729113558073
        "#;

        let tags = None;
        let snapshot = parse_and_get_metrics(prom_text, tags);

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
            debugging::DebugValue::Gauge(ordered_float) => {
                assert_eq!(ordered_float, 1.0);
            }
            _ => panic!("unexpected metric type"),
        }
    }

    #[test]
    fn test_gauge_invalid_value() {
        let prom_text = r#"
        # TYPE memory_usage_bytes gauge
        memory_usage_bytes{process="test"} foobar
        "#;

        let snapshot = parse_and_get_metrics(prom_text, None);

        assert_eq!(snapshot.len(), 0);
    }

    #[test]
    fn test_counter_invalid_value() {
        let prom_text = r#"
        # TYPE memory_usage_bytes counter
        memory_usage_bytes{process="test"} foobar
        "#;

        let snapshot = parse_and_get_metrics(prom_text, None);

        assert_eq!(snapshot.len(), 0);
    }

    #[tokio::test]
    async fn test_http_scraping_integration() {
        let prom_text = r#"
        # HELP workloadmeta_stored_entities Number of entities in the store.
        # TYPE workloadmeta_stored_entities gauge
        workloadmeta_stored_entities{kind="container",source="node_orchestrator"} 35
        workloadmeta_stored_entities{kind="container",source="runtime"} 36
        "#;

        let tags = None;
        let snapshot = run_scrape_and_parse_metrics(prom_text, tags).await;

        assert_eq!(snapshot.len(), 2);
    }

    #[test]
    fn test_gauge_label_value_with_comma() {
        let prom_text = r#"
        # HELP go_info Information about the Go environment.
        # TYPE go_info gauge
        go_info{version="go1.25rc1 X:jsonv2,greenteagc"} 1
        "#;

        let tags = None;
        let snapshot = parse_and_get_metrics(prom_text, tags);

        assert_eq!(snapshot.len(), 1);

        let metric = snapshot
            .get(&CompositeKey::new(
                MetricKind::Gauge,
                Key::from_parts(
                    "target/go_info",
                    vec![Label::new("version", "go1.25rc1 X:jsonv2,greenteagc")],
                ),
            ))
            .expect("metric not found");
        match metric.2 {
            debugging::DebugValue::Gauge(ordered_float) => {
                assert_eq!(ordered_float, 1.0);
            }
            _ => panic!("unexpected metric type"),
        }
    }

    #[test]
    fn test_gauge_multiple_labels_with_special_chars() {
        let prom_text = r#"
        # TYPE test_metric gauge
        test_metric{label1="value1",label2="value,with,commas",label3="value=with=equals",label4="normal"} 42
        "#;

        let tags = None;
        let snapshot = parse_and_get_metrics(prom_text, tags);

        assert_eq!(snapshot.len(), 1);

        let metric = snapshot
            .get(&CompositeKey::new(
                MetricKind::Gauge,
                Key::from_parts(
                    "target/test_metric",
                    vec![
                        Label::new("label1", "value1"),
                        Label::new("label2", "value,with,commas"),
                        Label::new("label3", "value=with=equals"),
                        Label::new("label4", "normal"),
                    ],
                ),
            ))
            .expect("metric not found");
        match metric.2 {
            debugging::DebugValue::Gauge(ordered_float) => {
                assert_eq!(ordered_float, 42.0);
            }
            _ => panic!("unexpected metric type"),
        }
    }

    #[test]
    fn test_gauge_edge_case_labels() {
        let prom_text = r#"
        # TYPE edge_cases gauge
        edge_cases{empty="",spaces="value with spaces",underscore_123="test",path="/var/log/app.log",url="https://example.com/path?query=1"} 1
        "#;

        let tags = None;
        let snapshot = parse_and_get_metrics(prom_text, tags);

        assert_eq!(snapshot.len(), 1);

        let metric = snapshot
            .get(&CompositeKey::new(
                MetricKind::Gauge,
                Key::from_parts(
                    "target/edge_cases",
                    vec![
                        Label::new("empty", ""),
                        Label::new("spaces", "value with spaces"),
                        Label::new("underscore_123", "test"),
                        Label::new("path", "/var/log/app.log"),
                        Label::new("url", "https://example.com/path?query=1"),
                    ],
                ),
            ))
            .expect("metric not found");
        match metric.2 {
            debugging::DebugValue::Gauge(ordered_float) => {
                assert_eq!(ordered_float, 1.0);
            }
            _ => panic!("unexpected metric type"),
        }
    }

    #[test]
    fn test_gauge_unicode_labels() {
        let prom_text = r#"
        # TYPE unicode_test gauge
        unicode_test{emoji="🚀",chinese="你好",mixed="hello-世界"} 1
        "#;

        let tags = None;
        let snapshot = parse_and_get_metrics(prom_text, tags);

        assert_eq!(snapshot.len(), 1);

        let metric = snapshot
            .get(&CompositeKey::new(
                MetricKind::Gauge,
                Key::from_parts(
                    "target/unicode_test",
                    vec![
                        Label::new("emoji", "🚀"),
                        Label::new("chinese", "你好"),
                        Label::new("mixed", "hello-世界"),
                    ],
                ),
            ))
            .expect("metric not found");
        match metric.2 {
            debugging::DebugValue::Gauge(ordered_float) => {
                assert_eq!(ordered_float, 1.0);
            }
            _ => panic!("unexpected metric type"),
        }
    }

    #[test]
    fn test_gauge_numeric_label_names() {
        let prom_text = r#"
        # TYPE numeric_names gauge
        numeric_names{label_123="value",_underscore="test",ALL_CAPS="VALUE",mixedCase_123="test"} 1
        "#;

        let tags = None;
        let snapshot = parse_and_get_metrics(prom_text, tags);

        assert_eq!(snapshot.len(), 1);

        let metric = snapshot
            .get(&CompositeKey::new(
                MetricKind::Gauge,
                Key::from_parts(
                    "target/numeric_names",
                    vec![
                        Label::new("label_123", "value"),
                        Label::new("_underscore", "test"),
                        Label::new("ALL_CAPS", "VALUE"),
                        Label::new("mixedCase_123", "test"),
                    ],
                ),
            ))
            .expect("metric not found");
        match metric.2 {
            debugging::DebugValue::Gauge(ordered_float) => {
                assert_eq!(ordered_float, 1.0);
            }
            _ => panic!("unexpected metric type"),
        }
    }

    #[test]
    fn test_gauge_no_labels() {
        let prom_text = r#"
        # TYPE no_labels gauge
        no_labels 42
        "#;

        let tags = None;
        let snapshot = parse_and_get_metrics(prom_text, tags);

        assert_eq!(snapshot.len(), 1);

        let metric = snapshot
            .get(&CompositeKey::new(
                MetricKind::Gauge,
                Key::from_parts("target/no_labels", vec![]),
            ))
            .expect("metric not found");
        match metric.2 {
            debugging::DebugValue::Gauge(ordered_float) => {
                assert_eq!(ordered_float, 42.0);
            }
            _ => panic!("unexpected metric type"),
        }
    }

    #[test]
    fn test_gauge_mixed_quotes() {
        let prom_text = r#"
        # TYPE mixed_quotes gauge
        mixed_quotes{quoted="value",unquoted=novalue,another="test"} 1
        "#;

        let tags = None;
        let snapshot = parse_and_get_metrics(prom_text, tags);

        assert_eq!(snapshot.len(), 1);

        // The regex should only capture properly quoted labels
        let metric = snapshot
            .get(&CompositeKey::new(
                MetricKind::Gauge,
                Key::from_parts(
                    "target/mixed_quotes",
                    vec![
                        Label::new("quoted", "value"),
                        Label::new("another", "test"),
                        // Note: "unquoted" label is skipped because it doesn't match the regex
                    ],
                ),
            ))
            .expect("metric not found");
        match metric.2 {
            debugging::DebugValue::Gauge(ordered_float) => {
                assert_eq!(ordered_float, 1.0);
            }
            _ => panic!("unexpected metric type"),
        }
    }
}
