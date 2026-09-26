//! The HTTP protocol speaking blackhole.
//!
//! ## Metrics
//!
//! `bytes_received`: Total bytes received
//! `total_bytes_received`: Aggregated bytes received across all blackhole types
//! `decoded_bytes_received`: Total decoded bytes received
//! `requests_received`: Total requests received
//!

use bytes::Bytes;
use http::{HeaderMap, header::InvalidHeaderValue, status::InvalidStatusCode};
use http_body_util::{BodyExt, combinators::BoxBody};
use hyper::{Request, Response, StatusCode, header};
use lading_payload::openmetrics;
use metrics::counter;
use serde::{Deserialize, Deserializer, Serialize, de};
use serde_yaml::with::singleton_map_recursive;
use std::{
    io::Write,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc::{SyncSender, sync_channel},
    },
    time::{Duration, Instant},
};
use tracing::{error, info};

use super::General;
use crate::blackhole::common;

fn default_concurrent_requests_max() -> usize {
    100
}

/// Errors produced by [`Http`].
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Wrapper for [`hyper::Error`].
    #[error("HTTP server error: {0}")]
    Hyper(hyper::Error),
    /// The configured content type value was not valid.
    #[error("The configured content type value was not valid: {0}")]
    InvalidContentType(InvalidHeaderValue),
    /// The configured status code was not valid.
    #[error("The configured status code was not valid: {0}")]
    InvalidStatusCode(InvalidStatusCode),
    /// Failed to deserialize the configuration.
    #[error("Failed to deserialize the configuration: {0}")]
    Serde(#[from] serde_json::Error),
    /// `OpenMetrics` payload generation failed.
    #[error("OpenMetrics payload error: {0}")]
    OpenMetrics(#[from] lading_payload::openmetrics::Error),
    /// Wrapper for [`crate::blackhole::common::Error`].
    #[error(transparent)]
    Common(#[from] crate::blackhole::common::Error),
    /// Error binding HTTP server
    #[error("Failed to bind HTTP server to {addr}: {source}")]
    BindServer {
        /// Binding address
        addr: SocketAddr,
        /// Underlying error
        #[source]
        source: Box<crate::blackhole::common::Error>,
    },
    /// HTTP server encountered error
    #[error("HTTP server on {addr} encountered error: {source}")]
    ServerError {
        /// Server address
        addr: SocketAddr,
        /// Underlying error
        #[source]
        source: Box<hyper::Error>,
    },
}

/// Body variant supported by this blackhole.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum BodyVariant {
    /// All response bodies will be empty.
    Nothing,
    /// All response bodies will mimic AWS Kinesis.
    AwsKinesis,
    /// Respond with a hardcoded byte slice value
    RawBytes,
    /// Respond with a hardcoded string value
    Static(String),
    /// Respond with a generated `OpenMetrics` text exposition body.
    #[serde(rename = "openmetrics")]
    OpenMetrics(Box<openmetrics::Config>),
}

fn default_body_variant() -> BodyVariant {
    BodyVariant::Nothing
}

fn default_response_delay_millis() -> u64 {
    0
}

fn default_status_code() -> u16 {
    StatusCode::OK.as_u16()
}

fn default_headers() -> HeaderMap {
    let mut map = HeaderMap::new();
    map.insert(
        header::CONTENT_TYPE,
        "application/json"
            .parse()
            .expect("Not possible to parse into HeaderMap"),
    );
    map
}

fn openmetrics_content_type() -> http::HeaderValue {
    "application/openmetrics-text; version=1.0.0; charset=utf-8"
        .parse()
        .expect("Not possible to parse into HeaderValue")
}

fn response_headers(config: &Config) -> HeaderMap {
    let mut headers = config.headers.clone();
    if matches!(config.body_variant, BodyVariant::OpenMetrics(_)) {
        // OpenMetrics scrapers may reject responses before parsing if the
        // content type does not advertise the OpenMetrics text format. This
        // body variant owns that semantic header while preserving all other
        // user-provided response headers.
        headers.insert(header::CONTENT_TYPE, openmetrics_content_type());
    }
    headers
}

fn deserialize_body_variant<'de, D>(deserializer: D) -> Result<BodyVariant, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_yaml::Value::deserialize(deserializer)?;
    BodyVariant::deserialize(value.clone()).or_else(|original| {
        singleton_map_recursive::deserialize(value)
            .map_err(|_| de::Error::custom(original.to_string()))
    })
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
/// Configuration for [`Http`]
pub struct Config {
    /// number of concurrent HTTP connections to allow
    #[serde(default = "default_concurrent_requests_max")]
    pub concurrent_requests_max: usize,
    /// address -- IP plus port -- to bind to
    pub binding_addr: SocketAddr,
    /// the body variant to respond with, default nothing
    #[serde(default = "default_body_variant")]
    #[serde(deserialize_with = "deserialize_body_variant")]
    pub body_variant: BodyVariant,
    /// Headers to include in the response; default is `Content-Type: application/json`
    #[serde(with = "http_serde::header_map", default = "default_headers")]
    pub headers: HeaderMap,
    /// the content-type header to respond with, defaults to 200
    #[serde(default = "default_status_code")]
    pub status: u16,
    /// raw array of bytes if the `raw_bytes` body variant is selected
    #[serde(default)]
    pub raw_bytes: Vec<u8>,
    /// delay to add before making a response
    #[serde(default = "default_response_delay_millis")]
    pub response_delay_millis: u64,
    /// Optional payload capture. When set, decoded HTTP request bodies are
    /// written as JSONL to `path`, up to `max_payloads` records. Additional
    /// requests are served normally but not captured.
    #[serde(default)]
    pub capture: Option<CaptureConfig>,
}

/// Configuration for blackhole HTTP payload capture.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CaptureConfig {
    /// File path to write JSONL records to. Overwritten on startup.
    pub path: PathBuf,
    /// Maximum number of payloads to capture. Once reached, subsequent
    /// requests skip all capture work with a single relaxed atomic load.
    pub max_payloads: usize,
    /// Optional cap on the number of payload bytes retained per record.
    /// When set, decoded payloads are truncated to this many bytes before
    /// being enqueued for the writer. Bounds memory held in the channel.
    /// Truncation is zero-copy (`Bytes::slice`).
    #[serde(default)]
    pub max_payload_bytes: Option<usize>,
}

/// A single captured blackhole payload. Holds `Bytes` (a refcounted view
/// into the decoded body) so cloning into the channel does not memcpy the
/// payload; proto encoding happens on the writer thread, off the request
/// path.
struct CaptureRecord {
    /// Milliseconds since the blackhole server started.
    relative_ms: u64,
    /// Size of the compressed (on-wire) body in bytes.
    compressed_bytes: u64,
    /// Value of the request's Content-Type header. Empty if absent.
    content_type: String,
    /// Path portion of the request URI.
    request_path: String,
    /// Decoded body.
    payload: Bytes,
}

/// Bounded channel capacity between request handlers and the writer thread.
/// Sized to absorb write bursts for full-size (5 MiB) payloads while
/// bounding memory retention to a predictable ceiling. Each slot holds a
/// refcounted `Bytes` view of a decoded payload; with 128 slots the ceiling
/// is `128 * max_payload_size` (~640 MiB at 5 MiB payloads). If
/// `max_payload_bytes` is set the ceiling drops accordingly.
const CAPTURE_CHANNEL_CAPACITY: usize = 128;

/// File format magic: `LBHC` = Lading blackhole capture.
const CAPTURE_FILE_MAGIC: &[u8; 4] = b"LBHC";

/// File format version. Increment on wire-incompatible changes.
const CAPTURE_FILE_VERSION: u16 = 1;

/// Write the 8-byte file prologue. See `proto/blackhole_capture.proto` for
/// the full on-disk format documentation.
fn write_capture_prologue<W: Write>(writer: &mut W) -> std::io::Result<()> {
    writer.write_all(CAPTURE_FILE_MAGIC)?;
    writer.write_all(&CAPTURE_FILE_VERSION.to_le_bytes())?;
    writer.write_all(&0u16.to_le_bytes())?; // reserved
    Ok(())
}

/// Spawn a dedicated OS thread that drains capture records and writes a
/// length-delimited proto stream to disk. Deliberately not a tokio task:
/// file I/O is synchronous, and running it on the tokio runtime would pin
/// a worker thread on every flush and back-pressure the request handlers
/// through the runtime. A plain `std::thread` isolates disk stalls from
/// the executor.
///
/// Flushes after every record so captures survive abrupt shutdown and can
/// be live-tailed.
fn spawn_capture_writer(rx: std::sync::mpsc::Receiver<CaptureRecord>, path: PathBuf) {
    std::thread::Builder::new()
        .name("blackhole-capture-writer".to_string())
        .spawn(move || {
            let file = match std::fs::File::create(&path) {
                Ok(f) => f,
                Err(e) => {
                    error!(
                        "Failed to create blackhole capture file {}: {e}",
                        path.display()
                    );
                    return;
                }
            };
            let mut writer = std::io::BufWriter::new(file);
            if let Err(e) = write_capture_prologue(&mut writer) {
                error!("Failed to write blackhole capture prologue: {e}");
                return;
            }
            // Reusable encode buffer: sized to grow to the largest record
            // seen and then stay there, so per-record cost is a memcpy
            // (unavoidable at proto encode time) with no reallocation.
            let mut encode_buf: Vec<u8> = Vec::with_capacity(64 * 1024);
            while let Ok(record) = rx.recv() {
                let proto_record = crate::proto::blackhole::v1::BlackholeCaptureRecord {
                    relative_ms: record.relative_ms,
                    compressed_bytes: record.compressed_bytes,
                    content_type: record.content_type,
                    request_path: record.request_path,
                    payload: record.payload,
                };
                encode_buf.clear();
                if let Err(e) = prost::Message::encode_length_delimited(
                    &proto_record,
                    &mut encode_buf,
                ) {
                    error!("Failed to encode blackhole capture record: {e}");
                    continue;
                }
                if let Err(e) = writer
                    .write_all(&encode_buf)
                    .and_then(|()| writer.flush())
                {
                    error!("Blackhole capture write failed: {e}");
                    return;
                }
            }
            info!("Blackhole capture writer finished for {}", path.display());
        })
        .expect("failed to spawn blackhole capture writer thread");
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
struct KinesisPutRecordBatchResponseEntry {
    error_code: Option<String>,
    error_message: Option<String>,
    record_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
struct KinesisPutRecordBatchResponse {
    encrypted: Option<bool>,
    failed_put_count: u32,
    request_responses: Vec<KinesisPutRecordBatchResponseEntry>,
}

#[allow(clippy::borrow_interior_mutable_const)]
#[allow(clippy::too_many_arguments)]
async fn srv(
    status: StatusCode,
    metric_labels: Vec<(String, String)>,
    body_bytes: Vec<u8>,
    req: Request<hyper::body::Incoming>,
    headers: HeaderMap,
    response_delay: Duration,
    capture: Option<CaptureState>,
) -> Result<hyper::Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
    counter!("requests_received", &metric_labels).increment(1);

    // Split into parts
    let (parts, body) = req.into_parts();

    // Convert the `Body` into `Bytes`
    let body: Bytes = body.boxed().collect().await?.to_bytes();

    let body_len = body.len() as u64;
    counter!("bytes_received", &metric_labels).increment(body_len);
    counter!("total_bytes_received").increment(body_len);

    match crate::codec::decode(parts.headers.get(hyper::header::CONTENT_ENCODING), body) {
        Err(response) => Ok(*response),
        Ok(body) => {
            counter!("decoded_bytes_received", &metric_labels).increment(body.len() as u64);

            // Capture path is deliberately fire-and-forget on a spawned
            // task so no capture work — atomic RMWs, timestamp reads,
            // channel sends, metric emission — is on the response path.
            //
            // When capture is disabled the branch is a `None` check.
            // When capture is enabled but the payload cap has been reached,
            // we pay only a single `Relaxed` load and skip the spawn.
            // Under cap we spawn a task that owns the decoded `body`
            // (moved, not cloned) and does the rest of the work off-path.
            if let Some(cap) = capture.as_ref()
                && cap.captured.load(Ordering::Relaxed) < cap.max_payloads
            {
                let cap = cap.clone();
                let metric_labels = metric_labels.clone();
                // Extract request metadata inline. These are `&str`
                // conversions from small headers and are effectively free;
                // doing them here avoids moving `parts` into the spawned
                // task.
                let content_type = parts
                    .headers
                    .get(hyper::header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                let request_path = parts.uri.path().to_string();
                tokio::spawn(async move {
                    // Reserve a slot. A second thread may have raced
                    // past the cap between our load and this RMW, so
                    // re-check and bail without touching the channel.
                    let seq = cap.captured.fetch_add(1, Ordering::Relaxed);
                    if seq >= cap.max_payloads {
                        return;
                    }
                    let payload = match cap.max_payload_bytes {
                        Some(n) if body.len() > n => body.slice(..n),
                        _ => body,
                    };
                    let record = CaptureRecord {
                        relative_ms: u64::try_from(cap.epoch.elapsed().as_millis())
                            .unwrap_or(u64::MAX),
                        compressed_bytes: body_len,
                        content_type,
                        request_path,
                        payload,
                    };
                    // `SyncSender::try_send` returns only `Full` or
                    // `Disconnected`; both are drops for our purposes.
                    if cap.tx.try_send(record).is_err() {
                        counter!("blackhole_capture_dropped", &metric_labels).increment(1);
                    }
                });
            }

            tokio::time::sleep(response_delay).await;

            let mut okay = Response::default();
            *okay.status_mut() = status;
            *okay.headers_mut() = headers;
            *okay.body_mut() = crate::full(body_bytes);
            Ok(okay)
        }
    }
}

/// Shared per-request capture state.
#[derive(Debug, Clone)]
struct CaptureState {
    tx: SyncSender<CaptureRecord>,
    captured: Arc<AtomicUsize>,
    max_payloads: usize,
    max_payload_bytes: Option<usize>,
    epoch: Instant,
}

#[derive(Debug)]
/// The HTTP blackhole.
pub struct Http {
    httpd_addr: SocketAddr,
    body_bytes: Vec<u8>,
    concurrency_limit: usize,
    shutdown: lading_signal::Watcher,
    headers: HeaderMap,
    status: StatusCode,
    metric_labels: Vec<(String, String)>,
    response_delay: Duration,
    capture: Option<CaptureState>,
}


impl Http {
    /// Create a new [`Http`] server instance
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid.
    ///
    /// # Panics
    ///
    /// None known.
    pub fn new(
        general: General,
        config: &Config,
        shutdown: lading_signal::Watcher,
    ) -> Result<Self, Error> {
        let status = StatusCode::from_u16(config.status).map_err(Error::InvalidStatusCode)?;

        let mut metric_labels = vec![
            ("component".to_string(), "blackhole".to_string()),
            ("component_name".to_string(), "http".to_string()),
        ];
        if let Some(id) = general.id {
            metric_labels.push(("id".to_string(), id));
        }

        let body_bytes = match &config.body_variant {
            BodyVariant::AwsKinesis => {
                let response = KinesisPutRecordBatchResponse {
                    encrypted: None,
                    failed_put_count: 0,
                    request_responses: vec![KinesisPutRecordBatchResponseEntry {
                        error_code: None,
                        error_message: None,
                        record_id: "foobar".to_string(),
                    }],
                };
                serde_json::to_vec(&response)?
            }
            BodyVariant::Nothing => vec![],
            BodyVariant::RawBytes => config.raw_bytes.clone(),
            BodyVariant::Static(val) => val.as_bytes().to_vec(),
            BodyVariant::OpenMetrics(conf) => openmetrics::OpenMetrics::new(conf)?.into_bytes(),
        };

        let capture = config.capture.as_ref().map(|cap| {
            let (tx, rx) = sync_channel::<CaptureRecord>(CAPTURE_CHANNEL_CAPACITY);
            spawn_capture_writer(rx, cap.path.clone());
            if let Some(n) = cap.max_payload_bytes {
                info!(
                    "Blackhole HTTP capture enabled, writing up to {} payloads (truncated to {} bytes) to {}",
                    cap.max_payloads,
                    n,
                    cap.path.display()
                );
            } else {
                info!(
                    "Blackhole HTTP capture enabled, writing up to {} payloads to {}",
                    cap.max_payloads,
                    cap.path.display()
                );
            }
            CaptureState {
                tx,
                captured: Arc::new(AtomicUsize::new(0)),
                max_payloads: cap.max_payloads,
                max_payload_bytes: cap.max_payload_bytes,
                epoch: Instant::now(),
            }
        });

        Ok(Self {
            httpd_addr: config.binding_addr,
            body_bytes,
            concurrency_limit: config.concurrent_requests_max,
            headers: response_headers(config),
            status,
            shutdown,
            metric_labels,
            response_delay: Duration::from_millis(config.response_delay_millis),
            capture,
        })
    }

    /// Run [`Http`] to completion
    ///
    /// This function runs the HTTP server forever, unless a shutdown signal is
    /// received or an unrecoverable error is encountered.
    ///
    /// # Errors
    ///
    /// Function will return an error if the configuration is invalid or if
    /// receiving a packet fails.
    pub async fn run(self) -> Result<(), Error> {
        common::run_httpd(
            self.httpd_addr,
            self.concurrency_limit,
            self.shutdown,
            self.metric_labels.clone(),
            move || {
                let metric_labels = self.metric_labels.clone();
                let body_bytes = self.body_bytes.clone();
                let headers = self.headers.clone();
                let status = self.status;
                let response_delay = self.response_delay;
                let capture = self.capture.clone();

                hyper::service::service_fn(move |req| {
                    srv(
                        status,
                        metric_labels.clone(),
                        body_bytes.clone(),
                        req,
                        headers.clone(),
                        response_delay,
                        capture.clone(),
                    )
                })
            },
        )
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn config_deserializes_variant_nothing() {
        let contents = r#"
binding_addr: "127.0.0.1:1000"
body_variant: "nothing"
"#;
        let config: Config =
            serde_yaml::from_str(contents).expect("Contents do not match the structure expected");
        assert_eq!(
            config,
            Config {
                concurrent_requests_max: default_concurrent_requests_max(),
                response_delay_millis: default_response_delay_millis(),
                binding_addr: SocketAddr::from_str("127.0.0.1:1000")
                    .expect("Not possible to parse into SocketAddr"),
                body_variant: BodyVariant::Nothing,
                headers: default_headers(),
                status: default_status_code(),
                raw_bytes: vec![],
                capture: None,
            },
        );
    }

    #[test]
    fn config_deserializes_raw_bytes() {
        let contents = r#"
binding_addr: "127.0.0.1:1000"
body_variant: "raw_bytes"
raw_bytes: [0x01, 0x02, 0x10]
"#;
        let config: Config =
            serde_yaml::from_str(contents).expect("Contents do not match the structure expected");
        assert_eq!(
            config,
            Config {
                concurrent_requests_max: default_concurrent_requests_max(),
                response_delay_millis: default_response_delay_millis(),
                binding_addr: SocketAddr::from_str("127.0.0.1:1000")
                    .expect("Not possible to parse into SocketAddr"),
                body_variant: BodyVariant::RawBytes,
                headers: default_headers(),
                status: default_status_code(),
                raw_bytes: vec![0x01, 0x02, 0x10],
                capture: None,
            },
        );
    }

    #[test]
    fn config_deserializes_capture() {
        let contents = r#"
binding_addr: "127.0.0.1:1000"
capture:
  path: /tmp/blackhole.jsonl
  max_payloads: 100
"#;
        let config: Config =
            serde_yaml::from_str(contents).expect("Contents do not match the structure expected");
        let capture = config.capture.expect("capture should be set");
        assert_eq!(capture.path, PathBuf::from("/tmp/blackhole.jsonl"));
        assert_eq!(capture.max_payloads, 100);
        assert_eq!(capture.max_payload_bytes, None);
    }

    #[test]
    fn config_deserializes_capture_with_truncation() {
        let contents = r#"
binding_addr: "127.0.0.1:1000"
capture:
  path: /tmp/blackhole.jsonl
  max_payloads: 100
  max_payload_bytes: 4096
"#;
        let config: Config =
            serde_yaml::from_str(contents).expect("Contents do not match the structure expected");
        let capture = config.capture.expect("capture should be set");
        assert_eq!(capture.max_payload_bytes, Some(4096));
    }

    #[test]
    fn config_deserializes_tagged_static_variant() {
        let contents = r#"
binding_addr: "127.0.0.1:1000"
body_variant: !static ok
"#;
        let config: Config =
            serde_yaml::from_str(contents).expect("Contents do not match the structure expected");
        assert_eq!(config.body_variant, BodyVariant::Static("ok".to_string()));
    }

    #[test]
    fn config_deserializes_singleton_static_variant() {
        let contents = r#"
binding_addr: "127.0.0.1:1000"
body_variant:
  static: ok
"#;
        let config: Config =
            serde_yaml::from_str(contents).expect("Contents do not match the structure expected");
        assert_eq!(config.body_variant, BodyVariant::Static("ok".to_string()));
    }

    #[test]
    fn openmetrics_body_variant_sets_openmetrics_content_type() {
        let contents = r#"
binding_addr: "127.0.0.1:1000"
headers:
  x-test-header: "still here"
body_variant:
  openmetrics: {}
"#;
        let config: Config =
            serde_yaml::from_str(contents).expect("Contents do not match the structure expected");
        let (shutdown, _) = lading_signal::signal();
        let http = Http::new(General { id: None }, &config, shutdown).expect("http should build");
        assert_eq!(
            http.headers.get(header::CONTENT_TYPE),
            Some(&openmetrics_content_type())
        );
        assert_eq!(
            http.headers
                .get("x-test-header")
                .and_then(|value| value.to_str().ok()),
            Some("still here")
        );
    }

    #[test]
    fn config_deserializes_openmetrics() {
        let contents = r#"
binding_addr: "127.0.0.1:1000"
body_variant:
  openmetrics:
    metric_name_prefix: "om_test"
    counters:
      count: 2
    gauges:
      count: 3
    histograms:
      count: 1
      buckets: ["0.5", "1"]
    summaries:
      count: 1
      quantiles: ["0.5", "0.99"]
"#;
        let config: Config =
            serde_yaml::from_str(contents).expect("Contents do not match the structure expected");
        match config.body_variant {
            BodyVariant::OpenMetrics(openmetrics) => {
                assert_eq!(openmetrics.metric_name_prefix, "om_test");
                assert_eq!(openmetrics.counters.count, 2);
                assert_eq!(openmetrics.gauges.count, 3);
                assert_eq!(openmetrics.histograms.count, 1);
                assert_eq!(openmetrics.summaries.count, 1);
            }
            other => panic!("expected openmetrics body variant, got {other:?}"),
        }
    }
}
