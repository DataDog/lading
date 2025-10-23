//! gRPC implementation for Datadog stateful logs blackhole.

use crate::proto::datadog::intake::stateful::{
    BatchStatus, StatefulBatch,
    stateful_logs_service_server::{StatefulLogsService, StatefulLogsServiceServer},
};
use metrics::counter;
use prost::Message;
use std::net::SocketAddr;
use std::pin::Pin;
use tokio::task::JoinHandle;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, transport};
use tracing::{error, info};

/// Run the stateful logs gRPC server
pub(crate) fn run_server(addr: SocketAddr, base_labels: &[(String, String)]) -> JoinHandle<()> {
    let mut labels = Vec::with_capacity(base_labels.len() + 1);
    labels.push(("protocol".to_string(), "grpc".to_string()));
    labels.push(("variant".to_string(), "stateful_logs".to_string()));
    labels.extend_from_slice(base_labels);

    let service = StatefulLogsServiceImpl { labels };

    info!("Starting Datadog stateful logs gRPC service on {addr}");

    let router = transport::Server::builder().add_service(StatefulLogsServiceServer::new(service));

    tokio::spawn(async move {
        if let Err(e) = router.serve(addr).await {
            error!("gRPC server error: {}", e);
        }
    })
}

/// Implementation of the StatefulLogsService
#[derive(Debug, Clone)]
struct StatefulLogsServiceImpl {
    labels: Vec<(String, String)>,
}

#[tonic::async_trait]
impl StatefulLogsService for StatefulLogsServiceImpl {
    type LogsStreamStream = Pin<Box<dyn Stream<Item = Result<BatchStatus, Status>> + Send>>;

    async fn logs_stream(
        &self,
        request: Request<tonic::Streaming<StatefulBatch>>,
    ) -> Result<Response<Self::LogsStreamStream>, Status> {
        let mut stream = request.into_inner();
        let labels = self.labels.clone();

        // Create output stream
        let output = async_stream::stream! {
            while let Some(result) = stream.next().await {
                match result {
                    Ok(batch) => {
                        let batch_id = batch.batch_id;
                        let size = batch.encoded_len();

                        counter!("bytes_received", &labels).increment(size as u64);
                        counter!("requests_received", &labels).increment(1);
                        counter!("batches_received", &labels).increment(1);

                        // Count data items in the batch
                        let data_count = batch.data.len();
                        if data_count > 0 {
                            counter!("data_items_received", &labels)
                                .increment(data_count as u64);
                        }

                        // Send back OK status
                        yield Ok(BatchStatus {
                            batch_id: batch_id as i32,
                            status: crate::proto::datadog::intake::stateful::batch_status::Status::Ok as i32,
                        });
                    }
                    Err(e) => {
                        error!("Error receiving batch: {}", e);
                        counter!("stream_errors", &labels).increment(1);
                        yield Err(e);
                        break;
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(output)))
    }
}
