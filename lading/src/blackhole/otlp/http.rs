//! HTTP implementation of the OTLP blackhole.

use bytes::Bytes;
use http::StatusCode;
use http_body_util::{BodyExt, combinators::BoxBody};
use hyper::{Request, Response};
use metrics::counter;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use prost::Message;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::blackhole::common;

/// Run the HTTP server for OTLP
pub(crate) fn run_server(
    addr: SocketAddr,
    response_delay: Duration,
    base_labels: &[(String, String)],
    concurrency_limit: usize,
    shutdown: lading_signal::Watcher,
) -> JoinHandle<()> {
    info!("Starting OTLP HTTP server on {addr}");
    let mut labels = Vec::with_capacity(base_labels.len() + 1);
    labels.push(("protocol".to_string(), "http".to_string()));
    labels.extend_from_slice(base_labels);

    tokio::spawn(async move {
        let handler = OtlpHttpHandler::new(response_delay, &labels);

        let result = common::run_httpd(addr, concurrency_limit, shutdown, labels, move || {
            let handler_clone = handler.clone();
            hyper::service::service_fn(move |req| {
                let request_handler = handler_clone.clone();
                request_handler.handle_request(req)
            })
        })
        .await;

        if let Err(e) = result {
            error!("HTTP server error: {e}");
        }
    })
}

/// Request format for OTLP HTTP
#[derive(Clone, Copy, Debug)]
enum RequestFormat {
    Proto,
    Json,
}

/// Response format for OTLP HTTP
#[derive(Clone, Copy, Debug)]
enum ResponseFormat {
    Proto,
    Json,
}

/// Handler for OTLP HTTP requests
#[derive(Clone, Debug)]
struct OtlpHttpHandler {
    labels: Vec<(String, String)>,
    empty_metrics_response_proto: Bytes,
    empty_traces_response_proto: Bytes,
    empty_logs_response_proto: Bytes,
    empty_metrics_response_json: Bytes,
    empty_traces_response_json: Bytes,
    empty_logs_response_json: Bytes,
    content_type_proto: http::HeaderValue,
    content_type_json: http::HeaderValue,
    response_delay: Duration,
}

impl OtlpHttpHandler {
    fn new(response_delay: Duration, labels: &[(String, String)]) -> Self {
        // Pre-compute empty responses for both formats
        let empty_metrics = ExportMetricsServiceResponse::default();
        let empty_traces = ExportTraceServiceResponse::default();
        let empty_logs = ExportLogsServiceResponse::default();

        let empty_metrics_response_proto = Bytes::from(empty_metrics.encode_to_vec());
        let empty_traces_response_proto = Bytes::from(empty_traces.encode_to_vec());
        let empty_logs_response_proto = Bytes::from(empty_logs.encode_to_vec());
        let empty_metrics_response_json = Bytes::from(
            serde_json::to_vec(&empty_metrics).expect("Failed to serialize empty metrics response"),
        );
        let empty_traces_response_json = Bytes::from(
            serde_json::to_vec(&empty_traces).expect("Failed to serialize empty traces response"),
        );
        let empty_logs_response_json = Bytes::from(
            serde_json::to_vec(&empty_logs).expect("Failed to serialize empty logs response"),
        );

        let content_type_proto = "application/x-protobuf"
            .parse()
            .expect("application/x-protobuf is a valid MIME type");
        let content_type_json = "application/json"
            .parse()
            .expect("application/json is a valid MIME type");

        Self {
            labels: labels.to_vec(),
            empty_metrics_response_proto,
            empty_traces_response_proto,
            empty_logs_response_proto,
            empty_metrics_response_json,
            empty_traces_response_json,
            empty_logs_response_json,
            content_type_proto,
            content_type_json,
            response_delay,
        }
    }

    #[inline]
    async fn build_response(
        &self,
        response_bytes: Bytes,
        content_type: &http::HeaderValue,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
        if self.response_delay.as_micros() > 0 {
            tokio::time::sleep(self.response_delay).await;
        }

        let mut response = Response::builder().status(StatusCode::OK);
        let headers = response
            .headers_mut()
            .expect("Response builder should always provide headers_mut");
        headers.insert(hyper::header::CONTENT_TYPE, content_type.clone());
        Ok(response
            .body(crate::full(response_bytes))
            .expect("Creating HTTP response should not fail"))
    }

    #[allow(clippy::too_many_lines)]
    async fn handle_request(
        self,
        req: Request<hyper::body::Incoming>,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
        // Fast-path for invalid paths
        let path_ref = req.uri().path();
        if path_ref != "/v1/metrics" && path_ref != "/v1/traces" && path_ref != "/v1/logs" {
            static NOT_FOUND: &[u8] = b"Not found";
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(crate::full(Bytes::from_static(NOT_FOUND)))
                .expect("Creating HTTP response should not fail"));
        }

        counter!("requests_received", &self.labels).increment(1);

        let request_format: RequestFormat = req
            .headers()
            .get(hyper::header::CONTENT_TYPE)
            .and_then(|ct| ct.to_str().ok())
            .map_or(RequestFormat::Proto, |ct| {
                if ct.contains("application/json") {
                    RequestFormat::Json
                } else {
                    RequestFormat::Proto
                }
            });
        let response_format: ResponseFormat = req
            .headers()
            .get(hyper::header::ACCEPT)
            .and_then(|accept| accept.to_str().ok())
            .map_or_else(
                || match request_format {
                    RequestFormat::Json => ResponseFormat::Json,
                    RequestFormat::Proto => ResponseFormat::Proto,
                },
                |accept| {
                    if accept.contains("application/json") {
                        ResponseFormat::Json
                    } else {
                        ResponseFormat::Proto
                    }
                },
            );

        // Check for empty bodies using Content-Length when available
        if let Some(content_length) = req.headers().get(hyper::header::CONTENT_LENGTH)
            && let Ok(length) = content_length.to_str()
            && let Ok(length) = length.parse::<u64>()
            && length == 0
        {
            counter!("bytes_received", &self.labels).increment(0);

            let (response_bytes, content_type) = match (path_ref, response_format) {
                ("/v1/metrics", ResponseFormat::Json) => (
                    self.empty_metrics_response_json.clone(),
                    &self.content_type_json,
                ),
                ("/v1/metrics", ResponseFormat::Proto) => (
                    self.empty_metrics_response_proto.clone(),
                    &self.content_type_proto,
                ),
                ("/v1/traces", ResponseFormat::Json) => (
                    self.empty_traces_response_json.clone(),
                    &self.content_type_json,
                ),
                ("/v1/traces", ResponseFormat::Proto) => (
                    self.empty_traces_response_proto.clone(),
                    &self.content_type_proto,
                ),
                ("/v1/logs", ResponseFormat::Json) => (
                    self.empty_logs_response_json.clone(),
                    &self.content_type_json,
                ),
                ("/v1/logs", ResponseFormat::Proto) => (
                    self.empty_logs_response_proto.clone(),
                    &self.content_type_proto,
                ),
                _ => unreachable!(), // path already validated
            };

            return self.build_response(response_bytes, content_type).await;
        }

        // Non-empty body, implies a little more CPU work
        let content_encoding = req.headers().get(hyper::header::CONTENT_ENCODING).cloned();
        let path = path_ref.to_string();
        let (_, body) = req.into_parts();

        let body_bytes = body.collect().await?.to_bytes();

        counter!("bytes_received", &self.labels).increment(body_bytes.len() as u64);

        let response_bytes =
            match crate::codec::decode(content_encoding.as_ref(), body_bytes.clone()) {
                Ok(decoded) => {
                    counter!("decoded_bytes_received", &self.labels)
                        .increment(decoded.len() as u64);

                    match path.as_str() {
                        "/v1/metrics" => {
                            self.process_metrics(&decoded, request_format, response_format)
                        }
                        "/v1/traces" => {
                            self.process_traces(&decoded, request_format, response_format)
                        }
                        "/v1/logs" => self.process_logs(&decoded, request_format, response_format),
                        _ => unreachable!("path already validated"),
                    }
                }
                Err(response) => return Ok(*response),
            };

        let content_type = match response_format {
            ResponseFormat::Json => &self.content_type_json,
            ResponseFormat::Proto => &self.content_type_proto,
        };

        self.build_response(response_bytes, content_type).await
    }

    fn process_metrics(
        &self,
        body_bytes: &[u8],
        request_format: RequestFormat,
        response_format: ResponseFormat,
    ) -> Bytes {
        if body_bytes.is_empty() {
            return match response_format {
                ResponseFormat::Json => self.empty_metrics_response_json.clone(),
                ResponseFormat::Proto => self.empty_metrics_response_proto.clone(),
            };
        }

        let mut total_points: u64 = 0;
        let request_opt = match request_format {
            RequestFormat::Json => {
                serde_json::from_slice::<ExportMetricsServiceRequest>(body_bytes).ok()
            }
            RequestFormat::Proto => ExportMetricsServiceRequest::decode(body_bytes).ok(),
        };

        if let Some(request) = request_opt {
            for rm in &request.resource_metrics {
                for sm in &rm.scope_metrics {
                    for m in &sm.metrics {
                        if let Some(data) = &m.data {
                            let points_count = match data {
                                MetricData::Gauge(g) => g.data_points.len(),
                                MetricData::Sum(s) => s.data_points.len(),
                                MetricData::Histogram(h) => h.data_points.len(),
                                MetricData::ExponentialHistogram(eh) => eh.data_points.len(),
                                MetricData::Summary(s) => s.data_points.len(),
                            };
                            total_points += points_count as u64;
                        }
                    }
                }
            }
        }

        if total_points > 0 {
            counter!("data_points_received", &self.labels).increment(total_points);
        }

        match response_format {
            ResponseFormat::Json => self.empty_metrics_response_json.clone(),
            ResponseFormat::Proto => self.empty_metrics_response_proto.clone(),
        }
    }

    fn process_traces(
        &self,
        body_bytes: &[u8],
        request_format: RequestFormat,
        response_format: ResponseFormat,
    ) -> Bytes {
        if body_bytes.is_empty() {
            return match response_format {
                ResponseFormat::Json => self.empty_traces_response_json.clone(),
                ResponseFormat::Proto => self.empty_traces_response_proto.clone(),
            };
        }

        let mut total_spans: u64 = 0;
        let request_opt = match request_format {
            RequestFormat::Json => {
                serde_json::from_slice::<ExportTraceServiceRequest>(body_bytes).ok()
            }
            RequestFormat::Proto => ExportTraceServiceRequest::decode(body_bytes).ok(),
        };

        if let Some(request) = request_opt {
            for rs in &request.resource_spans {
                for ss in &rs.scope_spans {
                    let spans_count = ss.spans.len() as u64;
                    total_spans += spans_count;
                }
            }
        }

        if total_spans > 0 {
            counter!("data_points_received", &self.labels).increment(total_spans);
        }

        match response_format {
            ResponseFormat::Json => self.empty_traces_response_json.clone(),
            ResponseFormat::Proto => self.empty_traces_response_proto.clone(),
        }
    }

    fn process_logs(
        &self,
        body_bytes: &[u8],
        request_format: RequestFormat,
        response_format: ResponseFormat,
    ) -> Bytes {
        if body_bytes.is_empty() {
            return match response_format {
                ResponseFormat::Json => self.empty_logs_response_json.clone(),
                ResponseFormat::Proto => self.empty_logs_response_proto.clone(),
            };
        }

        let mut total_logs: u64 = 0;
        let request_opt = match request_format {
            RequestFormat::Json => {
                serde_json::from_slice::<ExportLogsServiceRequest>(body_bytes).ok()
            }
            RequestFormat::Proto => ExportLogsServiceRequest::decode(body_bytes).ok(),
        };

        if let Some(request) = request_opt {
            for rl in &request.resource_logs {
                for sl in &rl.scope_logs {
                    let logs_count = sl.log_records.len() as u64;
                    total_logs += logs_count;
                }
            }
        }

        if total_logs > 0 {
            counter!("data_points_received", &self.labels).increment(total_logs);
        }

        match response_format {
            ResponseFormat::Json => self.empty_logs_response_json.clone(),
            ResponseFormat::Proto => self.empty_logs_response_proto.clone(),
        }
    }
}
