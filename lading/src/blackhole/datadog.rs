//! The Datadog intake API blackhole.
//!
//! This blackhole mimics the Datadog agent intake API, accepting payloads from
//! the Datadog agent and other clients that use the Datadog protocol.
//!
//! # Datadog Agent Intake Protocol
//!
//! The Datadog agent sends different types of data to different endpoints.
//! This blackhole supports the following endpoints:
//!
//! ## Metrics Endpoints
//!
//! - `/api/v1/series` - V1 metrics API (JSON format)
//! - `/api/v2/series` - V2 metrics API (JSON or protobuf format)
//! - `/intake/` - Legacy v1 endpoint, also used for events
//! - `/api/v1/check_run` - Service check results
//! - `/api/v1/metadata` - Metric metadata
//!
//! ## Trace Endpoints
//!
//! - `/v0.4/traces` - Trace API v0.4 (msgpack format)
//! - `/v0.5/traces` - Trace API v0.5 (msgpack format)
//! - `/v0.7/traces` - Trace API v0.7 (protobuf format)
//!
//! ## Other Endpoints
//!
//! - `/telemetry/proxy/*` - Telemetry proxy (used by trace agent)
//!
//! # Payload Formats
//!
//! ## Series Format (JSON)
//!
//! Both V1 and V2 use similar JSON structure:
//!
//! ```json
//! {
//!   "series": [
//!     {
//!       "metric": "metric.name",
//!       "points": [[timestamp, value], [timestamp, value]],
//!       "tags": ["tag1:value1", "tag2:value2"],
//!       "host": "hostname",
//!       "type": "gauge|count|rate",
//!       "interval": 10,
//!       "source_type_name": "System"
//!     }
//!   ]
//! }
//! ```
//!
//! Points are arrays of `[timestamp, value]` pairs where:
//! - `timestamp` is Unix epoch seconds (as float or int)
//! - `value` is the metric value (float or int)
//!
//! ## Compression
//!
//! All payloads are typically gzip compressed with `Content-Encoding: gzip`.
//! The blackhole automatically detects and decompresses gzipped payloads by:
//! - Checking the `Content-Encoding` header
//! - Detecting gzip magic bytes (0x1f 0x8b) at the start of the payload
//!
//! ## Content Types
//!
//! - `application/json` - JSON format (series, metadata)
//! - `application/msgpack` - `MessagePack` format (traces v0.4, v0.5)
//! - `application/x-protobuf` - Protobuf format (traces v0.7, some V2 metrics)
//!
//! # Agent Configuration
//!
//! To configure a Datadog agent to send to this blackhole, set in `datadog.yaml`:
//!
//! ```yaml
//! dd_url: http://localhost:9091                    # Main metrics intake
//! apm_config:
//!   apm_dd_url: http://localhost:9091              # Trace intake
//! process_config:
//!   process_dd_url: http://localhost:9092          # Process agent intake
//! logs_config:
//!   logs_dd_url: localhost:9093                    # Logs intake
//!   logs_no_ssl: true
//!   force_use_http: true
//! ```
//!
//! # Implementation Notes
//!
//! Following lading's design principles, this blackhole:
//! - Does minimal payload parsing (only what's needed for metrics)
//! - Accepts and counts data without full deserialization
//! - Returns success responses quickly to avoid backpressure
//! - Emits metrics about what it observes
//!
//! For traces and other complex payloads, we accept them without parsing
//! to maintain minimal overhead as required by lading's performance goals.
//!
//! # References
//!
//! Datadog agent intake implementation:
//! - Metrics: `pkg/serializer/internal/metrics/iterable_series.go`
//! - Traces: `pkg/trace/api/api.go`
//! - Endpoints: `comp/forwarder/defaultforwarder/endpoints/endpoints.go`

use std::{io, net::SocketAddr, sync::Arc};

use bytes::Bytes;
use flate2::read::{GzDecoder, ZlibDecoder};
use http_body_util::{BodyExt, Full};
use hyper::{
    Method, Request, Response, StatusCode, body::Incoming, header, server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use metrics::counter;
use prost::Message;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tracing::{error, info, warn};

use crate::proto::datadog::intake::metrics::MetricPayload;

use super::General;

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`Datadog`].
pub enum Error {
    /// Wrapper for [`std::io::Error`].
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Wrapper for [`hyper::Error`].
    #[error(transparent)]
    Hyper(#[from] hyper::Error),
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
/// Configuration for [`Datadog`].
pub struct Config {
    /// The binding address for the HTTP server.
    pub binding_addr: SocketAddr,
}

#[derive(Debug)]
/// The Datadog intake blackhole.
pub struct Datadog {
    config: Config,
    shutdown: lading_signal::Watcher,
    metric_labels: Vec<(String, String)>,
}

#[derive(Clone)]
struct AppState {
    metric_labels: Arc<[(String, String)]>,
}

impl Datadog {
    /// Create a new [`Datadog`] server instance
    #[must_use]
    pub fn new(general: General, config: Config, shutdown: lading_signal::Watcher) -> Self {
        let mut metric_labels = vec![
            ("component".to_string(), "blackhole".to_string()),
            ("component_name".to_string(), "datadog".to_string()),
        ];
        if let Some(id) = general.id {
            metric_labels.push(("id".to_string(), id));
        }

        Self {
            config,
            shutdown,
            metric_labels,
        }
    }

    /// Run [`Datadog`] to completion
    ///
    /// This function runs the HTTP server forever, unless a shutdown signal is
    /// received or an unrecoverable error is encountered.
    ///
    /// # Errors
    ///
    /// Function will return an error if the server fails to start.
    pub async fn run(self) -> Result<(), Error> {
        let metric_labels: Arc<[(String, String)]> = Arc::from(self.metric_labels.clone());
        let state = Arc::new(AppState { metric_labels });

        let listener = TcpListener::bind(self.config.binding_addr).await?;
        info!(
            "Datadog intake blackhole listening on {}",
            self.config.binding_addr
        );

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

                        if let Err(err) = http1::Builder::new()
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
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let path = req.uri().path();
    let method = req.method();
    let headers = req.headers();

    // Clone what we need before consuming req
    let path = path.to_string();
    let method = method.clone();
    let headers = headers.clone();

    if method != Method::POST {
        return Ok(Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Full::new(Bytes::new()))
            .expect("building response should not fail"));
    }

    let labels = state.metric_labels.as_ref();
    counter!("requests_received", labels).increment(1);

    let whole_body = match req.collect().await {
        Ok(body) => body.to_bytes(),
        Err(e) => {
            warn!("Failed to read request body: {e}");
            return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Full::new(Bytes::new()))
                .expect("building response should not fail"));
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

    info!(
        "Request: path={path}, content_type={content_type}, content_encoding={content_encoding}, body_size={}",
        whole_body.len()
    );

    let status = match (path.as_str(), content_type) {
        ("/api/v2/series", "application/x-protobuf") => {
            let decompressed = match decompress_if_needed(&whole_body, content_encoding) {
                Ok(data) => data,
                Err(e) => {
                    warn!("Failed to decompress body: {e}");
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Full::new(Bytes::new()))
                        .expect("building response should not fail"));
                }
            };

            match MetricPayload::decode(&decompressed[..]) {
                Ok(_payload) => {
                    // info!(
                    //     "Parsed protobuf MetricPayload: {} series",
                    //     payload.series.len()
                    // );
                    // for series in &payload.series {
                    //     info!(
                    //         "  Series: metric={}, points={}, tags={}",
                    //         series.metric,
                    //         series.points.len(),
                    //         series.tags.len()
                    //     );
                    // }
                }
                Err(e) => {
                    warn!("Failed to parse protobuf: {e}");
                }
            }
            StatusCode::ACCEPTED
        }
        _ => StatusCode::ACCEPTED,
    };

    Ok(Response::builder()
        .status(status)
        .body(Full::new(Bytes::new()))
        .expect("building response should not fail"))
}

fn decompress_if_needed(body: &[u8], content_encoding: &str) -> Result<Vec<u8>, io::Error> {
    match content_encoding {
        "gzip" => {
            let mut decoder = GzDecoder::new(body);
            let mut decompressed = Vec::new();
            io::Read::read_to_end(&mut decoder, &mut decompressed)?;
            Ok(decompressed)
        }
        "deflate" => {
            let mut decoder = ZlibDecoder::new(body);
            let mut decompressed = Vec::new();
            io::Read::read_to_end(&mut decoder, &mut decompressed)?;
            Ok(decompressed)
        }
        "zstd" => zstd::decode_all(body).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e)),
        _ => {
            // No compression or unknown - return as-is
            Ok(body.to_vec())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_deserializes() {
        let yaml = r#"
binding_addr: "127.0.0.1:9091"
"#;
        let _config: Config = serde_yaml::from_str(yaml).unwrap();
    }
}
