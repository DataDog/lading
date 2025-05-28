//! gRPC implementation of the OTLP blackhole.

use metrics::counter;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
    logs_service_server::{LogsService, LogsServiceServer},
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    metrics_service_server::{MetricsService, MetricsServiceServer},
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
    trace_service_server::{TraceService, TraceServiceServer},
};
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use prost::Message;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::task::JoinHandle;
use tonic::{Request as TonicRequest, Response as TonicResponse, Status};
use tracing::{error, info};

pub(crate) fn run_server(
    addr: SocketAddr,
    response_delay: Duration,
    base_labels: &[(String, String)],
    concurrency_limit: usize,
) -> JoinHandle<()> {
    let mut labels = Vec::with_capacity(base_labels.len() + 1);
    labels.push(("protocol".to_string(), "grpc".to_string()));
    labels.extend_from_slice(base_labels);

    // Pre-compute empty responses
    let empty_metrics_response = ExportMetricsServiceResponse::default();
    let empty_traces_response = ExportTraceServiceResponse::default();
    let empty_logs_response = ExportLogsServiceResponse::default();

    let metrics_service = OtlpMetricsService {
        labels: labels.clone(),
        response_delay,
        empty_response: empty_metrics_response,
    };

    let traces_service = OtlpTracesService {
        labels: labels.clone(),
        response_delay,
        empty_response: empty_traces_response,
    };

    let logs_service = OtlpLogsService {
        labels,
        response_delay,
        empty_response: empty_logs_response,
    };

    info!("Starting OTLP gRPC service (all signal types) on {addr}");
    let router = tonic::transport::Server::builder()
        .concurrency_limit_per_connection(concurrency_limit)
        .add_service(MetricsServiceServer::new(metrics_service))
        .add_service(TraceServiceServer::new(traces_service))
        .add_service(LogsServiceServer::new(logs_service));

    tokio::spawn(async move {
        if let Err(e) = router.serve(addr).await {
            error!("gRPC server error: {}", e);
        }
    })
}

/// Metrics Service implementation
#[derive(Debug)]
struct OtlpMetricsService {
    labels: Vec<(String, String)>,
    response_delay: Duration,
    empty_response: ExportMetricsServiceResponse,
}

#[tonic::async_trait]
impl MetricsService for OtlpMetricsService {
    async fn export(
        &self,
        request: TonicRequest<ExportMetricsServiceRequest>,
    ) -> Result<TonicResponse<ExportMetricsServiceResponse>, Status> {
        let request = request.into_inner();
        let size = request.encoded_len();

        counter!("bytes_received", &self.labels).increment(size as u64);
        counter!("requests_received", &self.labels).increment(1);

        let mut total_points: u64 = 0;
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

        if total_points > 0 {
            counter!("data_points_received", &self.labels).increment(total_points);
        }

        if self.response_delay.as_micros() > 0 {
            tokio::time::sleep(self.response_delay).await;
        }
        Ok(TonicResponse::new(self.empty_response.clone()))
    }
}

/// Traces Service implementation
#[derive(Debug)]
struct OtlpTracesService {
    labels: Vec<(String, String)>,
    response_delay: Duration,
    empty_response: ExportTraceServiceResponse,
}

#[tonic::async_trait]
impl TraceService for OtlpTracesService {
    async fn export(
        &self,
        request: TonicRequest<ExportTraceServiceRequest>,
    ) -> Result<TonicResponse<ExportTraceServiceResponse>, Status> {
        let request = request.into_inner();
        let size = request.encoded_len();

        counter!("bytes_received", &self.labels).increment(size as u64);
        counter!("requests_received", &self.labels).increment(1);

        let mut total_spans: u64 = 0;
        for rs in &request.resource_spans {
            for ss in &rs.scope_spans {
                let spans_count = ss.spans.len() as u64;
                total_spans += spans_count;
            }
        }

        if total_spans > 0 {
            counter!("data_points_received", &self.labels).increment(total_spans);
        }

        if self.response_delay.as_micros() > 0 {
            tokio::time::sleep(self.response_delay).await;
        }
        Ok(TonicResponse::new(self.empty_response.clone()))
    }
}

/// Logs Service implementation
#[derive(Debug)]
struct OtlpLogsService {
    labels: Vec<(String, String)>,
    response_delay: Duration,
    empty_response: ExportLogsServiceResponse,
}

#[tonic::async_trait]
impl LogsService for OtlpLogsService {
    async fn export(
        &self,
        request: TonicRequest<ExportLogsServiceRequest>,
    ) -> Result<TonicResponse<ExportLogsServiceResponse>, Status> {
        let request = request.into_inner();
        let size = request.encoded_len();

        counter!("bytes_received", &self.labels).increment(size as u64);
        counter!("requests_received", &self.labels).increment(1);

        let mut total_logs: u64 = 0;
        for rl in &request.resource_logs {
            for sl in &rl.scope_logs {
                let logs_count = sl.log_records.len() as u64;
                total_logs += logs_count;
            }
        }

        if total_logs > 0 {
            counter!("data_points_received", &self.labels).increment(total_logs);
        }

        if self.response_delay.as_micros() > 0 {
            tokio::time::sleep(self.response_delay).await;
        }
        Ok(TonicResponse::new(self.empty_response.clone()))
    }
}
