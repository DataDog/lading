//! Datadog Intake API target metrics receiver
//!
//! This module mimics the Datadog agent V2 metrics intake API, accepting
//! protobuf-encoded metric payloads. Only POST is supported.
//!
//! All other endpoints return `202 Accepted` without processing.
//!
//! # Payload
//!
//! The V2 protobuf format is defined in `proto/agent_payload.proto`.
//!
//! # Historical Metrics
//!
//! This module records metrics with their original timestamps from the agent
//! using lading's historical metrics capture system. Metrics are recorded at
//! the time indicated by their timestamp field, not when the payload arrives.

use bytes::Bytes;
use flate2::read::{GzDecoder, ZlibDecoder};
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, Response, StatusCode, body::Incoming, header, service::service_fn};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use lading_capture::{counter_incr, gauge_set};
use metrics::counter;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    io,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::net::TcpListener;
use tracing::{debug, error, info, trace, warn};

use crate::proto::datadog::intake::metrics::MetricPayload;

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`Datadog`].
pub enum Error {
    /// Wrapper for [`std::io::Error`].
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Wrapper for [`hyper::Error`].
    #[error(transparent)]
    Hyper(#[from] hyper::Error),
    /// Wrapper for [`http::Error`].
    #[error(transparent)]
    Http(#[from] http::Error),
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
/// Datadog intake API variant
pub enum Variant {
    /// V2 metrics API (protobuf only, JSON not supported)
    V2 {
        /// The binding address for the HTTP server
        binding_addr: SocketAddr,
    },
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
/// Configuration for [`Datadog`].
pub struct Config {
    /// The Datadog API variant to use
    #[serde(flatten)]
    pub variant: Variant,
}

#[derive(Clone)]
struct AppState {
    metric_labels: Arc<[(String, String)]>,
}

/// The `Datadog` intake target metrics receiver.
#[derive(Debug)]
pub struct Datadog {
    binding_addr: SocketAddr,
    shutdown: lading_signal::Watcher,
    metric_labels: Vec<(String, String)>,
}

impl Datadog {
    /// Create a new [`Datadog`] instance
    ///
    /// This is responsible for accepting metrics from the target process in the
    /// Datadog V2 protobuf format.
    ///
    pub(crate) fn new(
        config: Config,
        shutdown: lading_signal::Watcher,
        _sample_period: Duration,
    ) -> Self {
        let metric_labels = vec![
            ("component".to_string(), "target_metrics".to_string()),
            ("component_name".to_string(), "datadog".to_string()),
        ];

        let binding_addr = match config.variant {
            Variant::V2 { binding_addr } => binding_addr,
        };

        Self {
            binding_addr,
            shutdown,
            metric_labels,
        }
    }

    /// Run [`Datadog`] to completion
    ///
    /// This function runs the intake server forever, unless a shutdown signal
    /// is received or an unrecoverable error is encountered.
    ///
    /// # Errors
    ///
    /// Function will return an error if the server fails to start.
    pub(crate) async fn run(self) -> Result<(), Error> {
        let metric_labels: Arc<[(String, String)]> = Arc::from(self.metric_labels);
        let state = Arc::new(AppState { metric_labels });

        let listener = TcpListener::bind(self.binding_addr).await?;
        info!("Datadog intake server listening on {}", self.binding_addr);

        let shutdown_wait = self.shutdown.recv();
        tokio::pin!(shutdown_wait);

        loop {
            tokio::select! {
                Ok((stream, _addr)) = listener.accept() => {
                    let io = TokioIo::new(stream);
                    let state = Arc::clone(&state);

                    tokio::spawn(async move {
                        let service = service_fn(move |req| {
                            let state = Arc::clone(&state);
                            async move { handle_request(state, req).await }
                        });

                        if let Err(err) = auto::Builder::new(TokioExecutor::new())
                            .serve_connection(io, service)
                            .await
                        {
                            error!("Error serving connection: {:?}", err);
                        }
                    });
                }
                () = &mut shutdown_wait => {
                    info!("Shutdown signal received");
                    return Ok(());
                }
            }
        }
    }
}

async fn handle_request(
    state: Arc<AppState>,
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, http::Error> {
    let path = req.uri().path();
    let method = req.method();
    let headers = req.headers();

    let path = path.to_string();
    let method = method.clone();
    let headers = headers.clone();

    if method != Method::POST {
        warn!("Received other than POST method: {method}");
        return Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Full::new(Bytes::new()));
    }

    let labels = state.metric_labels.as_ref();
    counter!("requests_received", labels).increment(1);

    let whole_body = match req.collect().await {
        Ok(body) => body.to_bytes(),
        Err(e) => {
            warn!("Failed to read request body for path={path}: {e}");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Full::new(Bytes::new()));
        }
    };

    counter!("bytes_received", labels).increment(whole_body.len() as u64);

    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unspecified");
    let content_encoding = headers
        .get(header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unspecified");

    debug!(
        path = ?path,
        content_type = ?content_type,
        content_encoding = ?content_encoding,
        body_size = ?whole_body.len(),
        "received request",
    );

    let status = match (path.as_str(), content_type) {
        ("/api/v2/series", "application/x-protobuf") => {
            handle_v2_protobuf(&whole_body, content_encoding, &path, labels).await
        }
        _ => StatusCode::ACCEPTED,
    };

    Response::builder()
        .status(status)
        .body(Full::new(Bytes::new()))
}

fn decompress_if_needed<'a>(
    body: &'a [u8],
    content_encoding: &str,
) -> Result<Cow<'a, [u8]>, io::Error> {
    match content_encoding {
        "gzip" => {
            let mut decoder = GzDecoder::new(body);
            let mut decompressed = Vec::new();
            io::Read::read_to_end(&mut decoder, &mut decompressed)?;
            Ok(Cow::Owned(decompressed))
        }
        "deflate" => {
            let mut decoder = ZlibDecoder::new(body);
            let mut decompressed = Vec::new();
            io::Read::read_to_end(&mut decoder, &mut decompressed)?;
            Ok(Cow::Owned(decompressed))
        }
        "zstd" => {
            let decompressed = zstd::decode_all(body)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(Cow::Owned(decompressed))
        }
        _ => {
            // No compression or unknown, avoid allocation
            Ok(Cow::Borrowed(body))
        }
    }
}

#[inline]
fn unix_to_instant(timestamp_secs: i64) -> Instant {
    let now_system = SystemTime::now();
    let now_instant = Instant::now();

    #[allow(clippy::cast_sign_loss)]
    let point_time = UNIX_EPOCH + Duration::from_secs(timestamp_secs.max(0) as u64);

    match now_system.duration_since(point_time) {
        Ok(duration_ago) => now_instant.checked_sub(duration_ago).unwrap_or(now_instant),
        Err(_) => now_instant,
    }
}

async fn handle_v2_protobuf(
    body: &[u8],
    content_encoding: &str,
    path: &str,
    labels: &[(String, String)],
) -> StatusCode {
    let decompressed = match decompress_if_needed(body, content_encoding) {
        Ok(data) => data,
        Err(e) => {
            warn!(
                "Failed to decompress body for path={path}, encoding={content_encoding}, body_size={}: {e}",
                body.len()
            );
            return StatusCode::BAD_REQUEST;
        }
    };

    match MetricPayload::decode(&decompressed[..]) {
        Ok(payload) => {
            trace!(
                "Parsed protobuf MetricPayload: {} series",
                payload.series.len()
            );

            for series in &payload.series {
                if series.points.is_empty() {
                    continue;
                }

                // Parse Datadog tags (format: "key:value" or "key") into label pairs.
                // Key-only tags are represented with an empty value.
                let tag_pairs: Vec<(&str, &str)> = series
                    .tags
                    .iter()
                    .map(|tag| tag.split_once(':').unwrap_or((tag.as_str(), "")))
                    .collect();

                // Metric types from the agent_payload.proto:
                //
                // - COUNT (1): Delta count over the interval
                // - RATE (2): Per-second rate, converted into a counter by
                //   multiplication with the interval.
                // - GAUGE (3): Point-in-time value
                //
                // For COUNT/RATE we use counter_incr, for GAUGE we use
                // gauge_set. Timestamps are Unix epoch.
                for point in &series.points {
                    let timestamp = unix_to_instant(point.timestamp);

                    let metrics_res = match series.r#type {
                        1 => {
                            // COUNT
                            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                            let value = point.value.round() as u64;
                            counter_incr(&series.metric, &tag_pairs, value, timestamp).await
                        }
                        2 => {
                            // RATE
                            let interval = series.interval;
                            if interval <= 0 {
                                warn!(
                                    "RATE with non-positive interval for {metric}, ignoring",
                                    metric = series.metric
                                );
                                continue;
                            }
                            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                            let val = (point.value * interval as f64).round() as u64;
                            counter_incr(&series.metric, &tag_pairs, val, timestamp).await
                        }
                        3 => {
                            // GAUGE
                            gauge_set(&series.metric, &tag_pairs, point.value, timestamp).await
                        }
                        i => {
                            warn!("Unknown metric type, skipping: {i}");
                            Ok(())
                        }
                    };
                    if let Err(e) = metrics_res {
                        warn!(
                            "Failed to record metric {metric} at timestamp {ts}: {e}",
                            metric = series.metric,
                            ts = point.timestamp
                        );
                    }
                }
            }

            counter!("datadog_intake_payloads_parsed", labels).increment(1);
        }
        Err(e) => {
            warn!(
                "Failed to parse protobuf for path={path}, decompressed_size={}: {e}",
                decompressed.len()
            );
            counter!("datadog_intake_parse_failures", labels).increment(1);
        }
    }
    StatusCode::ACCEPTED
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_deserializes() {
        let yaml = r#"
v2:
  binding_addr: "127.0.0.1:9091"
"#;
        let _config: Config = serde_yaml::from_str(yaml).unwrap();
    }
}
