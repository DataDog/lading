//! `OpenMetrics` text exposition payload.
//!
//! This generator builds a deterministic Prometheus text exposition body for
//! scrape-oriented tests. The body is fully precomputed before serving so that
//! lading does no payload generation work on request hot paths.

use std::io::{self, Write};

use serde::Deserialize;

const DEFAULT_METRIC_NAME_PREFIX: &str = "lading_openmetrics";
const DEFAULT_ROUTE_COUNT: u32 = 60;
const DEFAULT_BUCKETS: &[f64] = &[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0];
const DEFAULT_QUANTILES: &[f64] = &[0.5, 0.75, 0.9, 0.95, 0.99];

/// Errors produced by `OpenMetrics` payload generation.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// IO error writing generated exposition text.
    #[error(transparent)]
    Io(#[from] io::Error),
    /// Invalid configuration.
    #[error("Invalid OpenMetrics configuration: {0}")]
    InvalidConfig(String),
}

fn default_metric_name_prefix() -> String {
    DEFAULT_METRIC_NAME_PREFIX.to_string()
}

fn default_include_help() -> bool {
    true
}

fn default_include_type() -> bool {
    true
}

fn default_route_count() -> u32 {
    DEFAULT_ROUTE_COUNT
}

fn default_services() -> Vec<String> {
    vec![
        "checkout".to_string(),
        "catalog".to_string(),
        "payments".to_string(),
        "fulfillment".to_string(),
        "search".to_string(),
    ]
}

fn default_regions() -> Vec<String> {
    vec![
        "us-east-1".to_string(),
        "us-west-2".to_string(),
        "eu-central-1".to_string(),
        "ap-southeast-1".to_string(),
    ]
}

fn default_methods() -> Vec<String> {
    vec![
        "GET".to_string(),
        "POST".to_string(),
        "PUT".to_string(),
        "DELETE".to_string(),
    ]
}

fn default_status_classes() -> Vec<String> {
    vec![
        "2xx".to_string(),
        "3xx".to_string(),
        "4xx".to_string(),
        "5xx".to_string(),
    ]
}

fn default_consumers() -> Vec<String> {
    (0..12)
        .map(|index| format!("consumer-{index:02}"))
        .collect()
}

fn default_buckets() -> Vec<String> {
    DEFAULT_BUCKETS.iter().map(ToString::to_string).collect()
}

fn default_quantiles() -> Vec<String> {
    DEFAULT_QUANTILES.iter().map(ToString::to_string).collect()
}

/// Configure gauge family generation.
#[derive(Debug, Deserialize, serde::Serialize, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields, default)]
pub struct GaugeConfig {
    /// Number of gauge samples to emit.
    pub count: u32,
}

impl Default for GaugeConfig {
    fn default() -> Self {
        Self { count: 180 }
    }
}

/// Configure counter family generation.
#[derive(Debug, Deserialize, serde::Serialize, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields, default)]
pub struct CounterConfig {
    /// Number of counter samples to emit.
    pub count: u32,
}

impl Default for CounterConfig {
    fn default() -> Self {
        Self { count: 240 }
    }
}

/// Configure histogram family generation.
#[derive(Debug, Deserialize, serde::Serialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields, default)]
pub struct HistogramConfig {
    /// Number of histogram contexts to emit.
    pub count: u32,
    /// Bucket upper bounds, excluding the required +Inf bucket.
    #[serde(default = "default_buckets")]
    pub buckets: Vec<String>,
}

impl Default for HistogramConfig {
    fn default() -> Self {
        Self {
            count: 20,
            buckets: default_buckets(),
        }
    }
}

/// Configure summary family generation.
#[derive(Debug, Deserialize, serde::Serialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields, default)]
pub struct SummaryConfig {
    /// Number of summary contexts to emit.
    pub count: u32,
    /// Quantiles to emit for each summary context.
    #[serde(default = "default_quantiles")]
    pub quantiles: Vec<String>,
}

impl Default for SummaryConfig {
    fn default() -> Self {
        Self {
            count: 40,
            quantiles: default_quantiles(),
        }
    }
}

/// Configure label value pools used by generated samples.
#[derive(Debug, Deserialize, serde::Serialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields, default)]
pub struct LabelConfig {
    /// Service label values.
    #[serde(default = "default_services")]
    pub services: Vec<String>,
    /// Region label values.
    #[serde(default = "default_regions")]
    pub regions: Vec<String>,
    /// HTTP method label values.
    #[serde(default = "default_methods")]
    pub methods: Vec<String>,
    /// Status class label values.
    #[serde(default = "default_status_classes")]
    pub status_classes: Vec<String>,
    /// Consumer label values.
    #[serde(default = "default_consumers")]
    pub consumers: Vec<String>,
    /// Number of distinct route label values to synthesize.
    #[serde(default = "default_route_count")]
    pub route_count: u32,
}

impl Default for LabelConfig {
    fn default() -> Self {
        Self {
            services: default_services(),
            regions: default_regions(),
            methods: default_methods(),
            status_classes: default_status_classes(),
            consumers: default_consumers(),
            route_count: default_route_count(),
        }
    }
}

/// Configure `OpenMetrics` text exposition generation.
#[derive(Debug, Deserialize, serde::Serialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// Prefix prepended to all generated metric names.
    #[serde(default = "default_metric_name_prefix")]
    pub metric_name_prefix: String,
    /// Whether to emit HELP lines.
    #[serde(default = "default_include_help")]
    pub include_help: bool,
    /// Whether to emit TYPE lines.
    #[serde(default = "default_include_type")]
    pub include_type: bool,
    /// Gauge family configuration.
    pub gauges: GaugeConfig,
    /// Counter family configuration.
    pub counters: CounterConfig,
    /// Histogram family configuration.
    pub histograms: HistogramConfig,
    /// Summary family configuration.
    pub summaries: SummaryConfig,
    /// Label value pools.
    pub labels: LabelConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            metric_name_prefix: default_metric_name_prefix(),
            include_help: default_include_help(),
            include_type: default_include_type(),
            gauges: GaugeConfig::default(),
            counters: CounterConfig::default(),
            histograms: HistogramConfig::default(),
            summaries: SummaryConfig::default(),
            labels: LabelConfig::default(),
        }
    }
}

impl Config {
    /// Validate this configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if this configuration cannot produce valid exposition text.
    pub fn valid(&self) -> Result<(), String> {
        validate_metric_prefix(&self.metric_name_prefix)?;
        validate_non_empty("labels.services", &self.labels.services)?;
        validate_non_empty("labels.regions", &self.labels.regions)?;
        validate_non_empty("labels.methods", &self.labels.methods)?;
        validate_non_empty("labels.status_classes", &self.labels.status_classes)?;
        validate_non_empty("labels.consumers", &self.labels.consumers)?;
        if self.labels.route_count == 0 {
            return Err("labels.route_count cannot be zero".to_string());
        }
        validate_finite_non_negative("histograms.buckets", &self.histograms.buckets)?;
        let buckets = parsed_floats("histograms.buckets", &self.histograms.buckets)?;
        if buckets.windows(2).any(|window| window[0] >= window[1]) {
            return Err("histograms.buckets must be strictly increasing".to_string());
        }
        validate_finite_non_negative("summaries.quantiles", &self.summaries.quantiles)?;
        let quantiles = parsed_floats("summaries.quantiles", &self.summaries.quantiles)?;
        if quantiles.iter().any(|quantile| *quantile > 1.0) {
            return Err("summaries.quantiles must be in the range [0.0, 1.0]".to_string());
        }
        Ok(())
    }
}

/// Precomputed `OpenMetrics` exposition body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenMetrics {
    body: Vec<u8>,
    sample_count: u64,
}

impl OpenMetrics {
    /// Create a new precomputed `OpenMetrics` exposition body.
    ///
    /// # Errors
    ///
    /// Returns an error when configuration validation or writing fails.
    pub fn new(config: &Config) -> Result<Self, Error> {
        config.valid().map_err(Error::InvalidConfig)?;
        let mut body = Vec::new();
        let mut sample_count = 0;
        write_target_info(config, &mut body)?;
        sample_count += 1;
        write_counters(config, &mut body, &mut sample_count)?;
        write_gauges(config, &mut body, &mut sample_count)?;
        write_histograms(config, &mut body, &mut sample_count)?;
        write_summaries(config, &mut body, &mut sample_count)?;
        write_build_info(config, &mut body)?;
        sample_count += 1;
        Ok(Self { body, sample_count })
    }

    /// Return the precomputed body bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.body
    }

    /// Return the number of non-comment samples in the body.
    #[must_use]
    pub fn sample_count(&self) -> u64 {
        self.sample_count
    }

    /// Consume this payload into body bytes.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.body
    }
}

fn validate_metric_prefix(prefix: &str) -> Result<(), String> {
    if prefix.is_empty() {
        return Err("metric_name_prefix cannot be empty".to_string());
    }
    if !prefix
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == ':')
    {
        return Err("metric_name_prefix contains invalid characters".to_string());
    }
    Ok(())
}

fn validate_non_empty(name: &str, values: &[String]) -> Result<(), String> {
    if values.is_empty() {
        return Err(format!("{name} cannot be empty"));
    }
    if values.iter().any(String::is_empty) {
        return Err(format!("{name} cannot contain empty values"));
    }
    if values.iter().any(|value| value.contains('"')) {
        return Err(format!("{name} cannot contain quote characters"));
    }
    Ok(())
}

fn validate_finite_non_negative(name: &str, values: &[String]) -> Result<(), String> {
    if values.is_empty() {
        return Err(format!("{name} cannot be empty"));
    }
    parsed_floats(name, values)?;
    Ok(())
}

fn parsed_floats(name: &str, values: &[String]) -> Result<Vec<f64>, String> {
    let mut parsed = Vec::with_capacity(values.len());
    for value in values {
        let parsed_value = value
            .parse::<f64>()
            .map_err(|_| format!("{name} values must parse as floating point numbers"))?;
        if !parsed_value.is_finite() || parsed_value.is_sign_negative() {
            return Err(format!("{name} values must be finite and non-negative"));
        }
        parsed.push(parsed_value);
    }
    Ok(parsed)
}

fn write_family_header<W: Write>(
    config: &Config,
    writer: &mut W,
    name: &str,
    kind: &str,
    help: &str,
) -> Result<(), Error> {
    let full_name = metric_name(config, name);
    if config.include_help {
        writeln!(writer, "# HELP {full_name} {help}")?;
    }
    if config.include_type {
        writeln!(writer, "# TYPE {full_name} {kind}")?;
    }
    Ok(())
}

fn write_target_info<W: Write>(config: &Config, writer: &mut W) -> Result<(), Error> {
    write_family_header(
        config,
        writer,
        "target_info",
        "gauge",
        "Synthetic OpenMetrics target identity.",
    )?;
    writeln!(
        writer,
        "{}{{service=\"checkout\",region=\"us-east-1\",shard=\"control\"}} 1",
        metric_name(config, "target_info")
    )?;
    Ok(())
}

fn write_counters<W: Write>(
    config: &Config,
    writer: &mut W,
    sample_count: &mut u64,
) -> Result<(), Error> {
    if config.counters.count == 0 {
        return Ok(());
    }
    write_family_header(
        config,
        writer,
        "requests_total",
        "counter",
        "Synthetic monotonic request counter.",
    )?;
    for index in 0..config.counters.count {
        writeln!(
            writer,
            "{}{{service=\"{}\",region=\"{}\",method=\"{}\",status_class=\"{}\",route=\"{}\"}} {}",
            metric_name(config, "requests_total"),
            select(&config.labels.services, index),
            select(&config.labels.regions, index),
            select(&config.labels.methods, index),
            select(&config.labels.status_classes, index),
            route(config, index),
            100_000 + u64::from(index) * 17
        )?;
        *sample_count += 1;
    }
    Ok(())
}

fn write_gauges<W: Write>(
    config: &Config,
    writer: &mut W,
    sample_count: &mut u64,
) -> Result<(), Error> {
    if config.gauges.count == 0 {
        return Ok(());
    }
    write_family_header(
        config,
        writer,
        "queue_depth",
        "gauge",
        "Synthetic queue depth gauge.",
    )?;
    for index in 0..config.gauges.count {
        let priority = if index % 7 == 0 { "high" } else { "normal" };
        writeln!(
            writer,
            "{}{{service=\"{}\",region=\"{}\",queue=\"queue-{:02}\",priority=\"{}\"}} {:.1}",
            metric_name(config, "queue_depth"),
            select(&config.labels.services, index),
            select(&config.labels.regions, index / 5),
            index % 36,
            priority,
            f64::from((index * 13) % 997) / 10.0
        )?;
        *sample_count += 1;
    }
    Ok(())
}

fn write_histograms<W: Write>(
    config: &Config,
    writer: &mut W,
    sample_count: &mut u64,
) -> Result<(), Error> {
    if config.histograms.count == 0 {
        return Ok(());
    }
    write_family_header(
        config,
        writer,
        "request_duration_seconds",
        "histogram",
        "Synthetic request duration histogram.",
    )?;
    for index in 0..config.histograms.count {
        let mut cumulative = 0_u64;
        for (bucket_index, bucket) in config.histograms.buckets.iter().enumerate() {
            cumulative += 50 + u64::from(index) + bucket_index as u64 * 3;
            write_histogram_bucket(config, writer, index, bucket, cumulative)?;
            *sample_count += 1;
        }
        write_histogram_bucket(config, writer, index, "+Inf", cumulative + 25)?;
        *sample_count += 1;
        writeln!(
            writer,
            "{}_sum{{service=\"{}\",region=\"{}\",route=\"{}\"}} {:.3}",
            metric_name(config, "request_duration_seconds"),
            select(&config.labels.services, index),
            select(&config.labels.regions, index),
            route(config, index),
            f64::from(index + 1) * 123.456
        )?;
        *sample_count += 1;
        writeln!(
            writer,
            "{}_count{{service=\"{}\",region=\"{}\",route=\"{}\"}} {}",
            metric_name(config, "request_duration_seconds"),
            select(&config.labels.services, index),
            select(&config.labels.regions, index),
            route(config, index),
            cumulative + 25
        )?;
        *sample_count += 1;
    }
    Ok(())
}

fn write_histogram_bucket<W: Write>(
    config: &Config,
    writer: &mut W,
    index: u32,
    le: &str,
    value: u64,
) -> Result<(), Error> {
    writeln!(
        writer,
        "{}_bucket{{service=\"{}\",region=\"{}\",route=\"{}\",le=\"{}\"}} {}",
        metric_name(config, "request_duration_seconds"),
        select(&config.labels.services, index),
        select(&config.labels.regions, index),
        route(config, index),
        le,
        value
    )?;
    Ok(())
}

fn write_summaries<W: Write>(
    config: &Config,
    writer: &mut W,
    sample_count: &mut u64,
) -> Result<(), Error> {
    if config.summaries.count == 0 {
        return Ok(());
    }
    write_family_header(
        config,
        writer,
        "payload_bytes",
        "summary",
        "Synthetic payload size summary.",
    )?;
    for index in 0..config.summaries.count {
        for quantile in &config.summaries.quantiles {
            let quantile_value = quantile.parse::<f64>().map_err(|_| {
                Error::InvalidConfig("summaries.quantiles values must parse".to_string())
            })?;
            writeln!(
                writer,
                "{}{{service=\"{}\",region=\"{}\",consumer=\"{}\",quantile=\"{}\"}} {:.1}",
                metric_name(config, "payload_bytes"),
                select(&config.labels.services, index),
                select(&config.labels.regions, index / 3),
                select(&config.labels.consumers, index),
                quantile,
                512.0 + f64::from(index) * 11.0 + quantile_value * 100.0
            )?;
            *sample_count += 1;
        }
        writeln!(
            writer,
            "{}_sum{{service=\"{}\",region=\"{}\",consumer=\"{}\"}} {}",
            metric_name(config, "payload_bytes"),
            select(&config.labels.services, index),
            select(&config.labels.regions, index / 3),
            select(&config.labels.consumers, index),
            500_000 + u64::from(index) * 997
        )?;
        *sample_count += 1;
        writeln!(
            writer,
            "{}_count{{service=\"{}\",region=\"{}\",consumer=\"{}\"}} {}",
            metric_name(config, "payload_bytes"),
            select(&config.labels.services, index),
            select(&config.labels.regions, index / 3),
            select(&config.labels.consumers, index),
            1_000 + u64::from(index) * 31
        )?;
        *sample_count += 1;
    }
    Ok(())
}

fn write_build_info<W: Write>(config: &Config, writer: &mut W) -> Result<(), Error> {
    write_family_header(
        config,
        writer,
        "build_info",
        "gauge",
        "Synthetic build metadata gauge.",
    )?;
    writeln!(
        writer,
        "{}{{version=\"1.2.3\",revision=\"deadbeef\",runtime=\"lading-http-blackhole\"}} 1",
        metric_name(config, "build_info")
    )?;
    Ok(())
}

fn metric_name(config: &Config, suffix: &str) -> String {
    format!("{}_{}", config.metric_name_prefix, suffix)
}

fn select(values: &[String], index: u32) -> &str {
    values[index as usize % values.len()].as_str()
}

fn route(config: &Config, index: u32) -> String {
    format!("/api/v1/resource/{:02}", index % config.labels.route_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn non_comment_line_count(body: &str) -> u64 {
        body.lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .count() as u64
    }

    #[test]
    fn default_payload_is_non_empty_and_counted() {
        let payload = OpenMetrics::new(&Config::default()).expect("payload should build");
        let body = std::str::from_utf8(payload.as_bytes()).expect("body should be utf8");
        assert!(body.contains("# HELP lading_openmetrics_requests_total"));
        assert!(body.contains("# TYPE lading_openmetrics_queue_depth gauge"));
        assert!(body.ends_with('\n'));
        assert_eq!(payload.sample_count(), non_comment_line_count(body));
    }

    #[test]
    fn default_payload_matches_expected_sample_count() {
        let config = Config::default();
        let payload = OpenMetrics::new(&config).expect("payload should build");
        let expected = 2
            + u64::from(config.counters.count)
            + u64::from(config.gauges.count)
            + u64::from(config.histograms.count) * (config.histograms.buckets.len() as u64 + 3)
            + u64::from(config.summaries.count) * (config.summaries.quantiles.len() as u64 + 2);
        assert_eq!(payload.sample_count(), expected);
    }

    #[test]
    fn histogram_and_summary_shapes_are_emitted() {
        let config = Config {
            metric_name_prefix: "om_test".to_string(),
            counters: CounterConfig { count: 0 },
            gauges: GaugeConfig { count: 0 },
            histograms: HistogramConfig {
                count: 1,
                buckets: vec!["0.5".to_string(), "1".to_string()],
            },
            summaries: SummaryConfig {
                count: 1,
                quantiles: vec!["0.5".to_string(), "0.99".to_string()],
            },
            ..Config::default()
        };
        let payload = OpenMetrics::new(&config).expect("payload should build");
        let body = std::str::from_utf8(payload.as_bytes()).expect("body should be utf8");
        assert!(body.contains("om_test_request_duration_seconds_bucket"));
        assert!(body.contains("le=\"+Inf\""));
        assert!(body.contains("om_test_request_duration_seconds_sum"));
        assert!(body.contains("om_test_request_duration_seconds_count"));
        assert!(body.contains("om_test_payload_bytes{service="));
        assert!(body.contains("quantile=\"0.99\""));
        assert!(body.contains("om_test_payload_bytes_sum"));
        assert!(body.contains("om_test_payload_bytes_count"));
        assert_eq!(payload.sample_count(), non_comment_line_count(body));
    }

    #[test]
    fn generation_is_deterministic() {
        let config = Config::default();
        let first = OpenMetrics::new(&config).expect("first payload should build");
        let second = OpenMetrics::new(&config).expect("second payload should build");
        assert_eq!(first, second);
    }

    #[test]
    fn validation_rejects_bad_config() {
        let config = Config {
            metric_name_prefix: "bad-name".to_string(),
            ..Config::default()
        };
        assert!(config.valid().is_err());

        let config = Config {
            labels: LabelConfig {
                services: Vec::new(),
                ..LabelConfig::default()
            },
            ..Config::default()
        };
        assert!(config.valid().is_err());

        let config = Config {
            histograms: HistogramConfig {
                count: 1,
                buckets: vec!["1".to_string(), "0.5".to_string()],
            },
            ..Config::default()
        };
        assert!(config.valid().is_err());
    }

    proptest! {
        #[test]
        fn generated_payload_counts_non_comment_lines(
            counters in 0_u32..20,
            gauges in 0_u32..20,
            histograms in 0_u32..10,
            summaries in 0_u32..10,
            bucket_count in 1_usize..8,
            quantile_count in 1_usize..6,
        ) {
            let buckets = (1..=bucket_count)
                .map(|index| format!("{index}"))
                .collect::<Vec<_>>();
            let quantiles = (1..=quantile_count)
                .map(|index| format!("0.{index}"))
                .collect::<Vec<_>>();
            let config = Config {
                counters: CounterConfig { count: counters },
                gauges: GaugeConfig { count: gauges },
                histograms: HistogramConfig { count: histograms, buckets },
                summaries: SummaryConfig { count: summaries, quantiles },
                ..Config::default()
            };
            let payload = OpenMetrics::new(&config).expect("payload should build");
            let body = std::str::from_utf8(payload.as_bytes()).expect("body should be utf8");
            prop_assert_eq!(payload.sample_count(), non_comment_line_count(body));
        }
    }
    #[test]
    fn config_deserializes_from_yaml() {
        let yaml = r#"
metric_name_prefix: om_scale
include_help: false
counters:
  count: 2
gauges:
  count: 3
histograms:
  count: 1
  buckets: ["0.1", "1"]
summaries:
  count: 1
  quantiles: ["0.5", "0.95"]
labels:
  services: [checkout]
  regions: [us-east-1]
  methods: [GET]
  status_classes: [2xx]
  consumers: [consumer-00]
  route_count: 4
"#;
        let config: Config = serde_yaml::from_str(yaml).expect("config should parse");
        let payload = OpenMetrics::new(&config).expect("payload should build");
        let body = std::str::from_utf8(payload.as_bytes()).expect("body should be utf8");
        assert!(!body.contains("# HELP"));
        assert!(body.contains("om_scale_requests_total"));
        assert_eq!(payload.sample_count(), non_comment_line_count(body));
    }
}
