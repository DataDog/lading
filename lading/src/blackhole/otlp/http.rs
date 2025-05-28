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

/// Handler for OTLP HTTP requests
#[derive(Clone, Debug)]
struct OtlpHttpHandler {
    labels: Vec<(String, String)>,
    empty_metrics_response: Bytes,
    empty_traces_response: Bytes,
    empty_logs_response: Bytes,
    content_type_header_value: http::HeaderValue,
    response_delay: Duration,
}

impl OtlpHttpHandler {
    fn new(response_delay: Duration, labels: &[(String, String)]) -> Self {
        // Pre-compute empty responses
        let empty_metrics_response =
            Bytes::from(ExportMetricsServiceResponse::default().encode_to_vec());
        let empty_traces_response =
            Bytes::from(ExportTraceServiceResponse::default().encode_to_vec());
        let empty_logs_response = Bytes::from(ExportLogsServiceResponse::default().encode_to_vec());

        let content_type_header_value = "application/x-protobuf"
            .parse()
            .expect("application/x-protobuf is a valid MIME type");

        Self {
            labels: labels.to_vec(),
            empty_metrics_response,
            empty_traces_response,
            empty_logs_response,
            content_type_header_value,
            response_delay,
        }
    }

    #[inline]
    async fn build_response(
        &self,
        response_bytes: Bytes,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
        if self.response_delay.as_micros() > 0 {
            tokio::time::sleep(self.response_delay).await;
        }

        let mut response = Response::builder().status(StatusCode::OK);
        let headers = response
            .headers_mut()
            .expect("Response builder should always provide headers_mut");
        headers.insert(
            hyper::header::CONTENT_TYPE,
            self.content_type_header_value.clone(),
        );
        Ok(response
            .body(crate::full(response_bytes))
            .expect("Creating HTTP response should not fail"))
    }

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

        // Check for empty bodies using Content-Length when available
        if let Some(content_length) = req.headers().get(hyper::header::CONTENT_LENGTH) {
            if let Ok(length) = content_length.to_str() {
                if let Ok(length) = length.parse::<u64>() {
                    if length == 0 {
                        counter!("bytes_received", &self.labels).increment(0);

                        let response_bytes = match path_ref {
                            "/v1/metrics" => self.empty_metrics_response.clone(),
                            "/v1/traces" => self.empty_traces_response.clone(),
                            "/v1/logs" => self.empty_logs_response.clone(),
                            _ => unreachable!(), // checked earlier
                        };

                        return self.build_response(response_bytes).await;
                    }
                }
            }
        }

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
                        "/v1/metrics" => self.process_metrics(&decoded),
                        "/v1/traces" => self.process_traces(&decoded),
                        "/v1/logs" => self.process_logs(&decoded),
                        _ => unreachable!(
                            "path previously checked, catastrophic programming mistake"
                        ),
                    }
                }
                Err(response) => return Ok(response),
            };

        self.build_response(response_bytes).await
    }

    fn process_metrics(&self, body_bytes: &[u8]) -> Bytes {
        if body_bytes.is_empty() {
            return self.empty_metrics_response.clone();
        }

        let mut total_points: u64 = 0;
        if let Ok(request) = ExportMetricsServiceRequest::decode(body_bytes) {
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

        self.empty_metrics_response.clone()
    }

    fn process_traces(&self, body_bytes: &[u8]) -> Bytes {
        if body_bytes.is_empty() {
            return self.empty_traces_response.clone();
        }

        let mut total_spans: u64 = 0;
        if let Ok(request) = ExportTraceServiceRequest::decode(body_bytes) {
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

        self.empty_traces_response.clone()
    }

    fn process_logs(&self, body_bytes: &[u8]) -> Bytes {
        if body_bytes.is_empty() {
            return self.empty_logs_response.clone();
        }

        let mut total_logs: u64 = 0;
        if let Ok(request) = ExportLogsServiceRequest::decode(body_bytes) {
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

        self.empty_logs_response.clone()
    }
}
