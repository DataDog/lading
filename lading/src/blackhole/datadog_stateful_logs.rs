//! Datadog stateful logs gRPC blackhole.
//!
//! This blackhole implements the Datadog stateful logs protocol via bidirectional
//! streaming gRPC.
//!
//! ## Metrics
//!
//! - `bytes_received`: Total bytes received
//! - `streams_received`: Total streams received
//! - `batches_received`: Total batches received
//! - `data_items_received`: Total data items in batches
//! - `stream_errors`: Errors encountered while processing streams

use crate::proto::datadog::intake::stateful_encoding::{
    BatchStatus, StatefulBatch, batch_status,
    stateful_logs_service_server::{StatefulLogsService, StatefulLogsServiceServer},
};
use metrics::counter;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::{JoinError, JoinHandle};
use tokio::time::sleep;
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, transport};
use tracing::{error, info};

use super::General;

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`DatadogStatefulLogs`].
pub enum Error {
    /// Wrapper for [`std::io::Error`].
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Wrapper for gRPC transport error.
    #[error(transparent)]
    Grpc(#[from] tonic::transport::Error),
    /// The server task failed internally (panic or unexpected cancellation).
    #[error("internal server task failure: {0}")]
    Internal(#[from] JoinError),
    /// The gRPC server task exited unexpectedly without reporting an error.
    #[error("gRPC server exited unexpectedly")]
    UnexpectedShutdown,
    /// Error binding gRPC server
    #[error("Failed to bind Datadog Stateful Logs gRPC server to {addr}: {source}")]
    BindServer {
        /// Binding address
        addr: SocketAddr,
        /// Underlying transport error
        #[source]
        source: Box<tonic::transport::Error>,
    },
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
/// Configuration for [`DatadogStatefulLogs`].
pub struct Config {
    /// The binding address for the gRPC server
    pub grpc_addr: SocketAddr,
    /// Delay to add before sending responses (in milliseconds)
    #[serde(default = "default_response_delay_millis")]
    pub response_delay_millis: u64,
}

fn default_response_delay_millis() -> u64 {
    0
}

#[derive(Debug)]
/// The Datadog stateful logs gRPC blackhole.
pub struct DatadogStatefulLogs {
    grpc_addr: SocketAddr,
    shutdown: lading_signal::Watcher,
    metric_labels: Vec<(String, String)>,
    response_delay: Duration,
}

impl DatadogStatefulLogs {
    /// Create a new [`DatadogStatefulLogs`] server instance
    #[must_use]
    pub fn new(general: General, config: Config, shutdown: lading_signal::Watcher) -> Self {
        let mut metric_labels = vec![
            ("component".to_string(), "blackhole".to_string()),
            (
                "component_name".to_string(),
                "datadog_stateful_logs".to_string(),
            ),
        ];
        if let Some(id) = general.id {
            metric_labels.push(("id".to_string(), id));
        }

        Self {
            grpc_addr: config.grpc_addr,
            shutdown,
            metric_labels,
            response_delay: Duration::from_millis(config.response_delay_millis),
        }
    }

    /// Run [`DatadogStatefulLogs`] to completion
    ///
    /// This function runs the server forever, unless a shutdown signal is
    /// received or an unrecoverable error is encountered.
    ///
    /// # Errors
    ///
    /// Function will return an error if the server fails to start.
    pub async fn run(self) -> Result<(), Error> {
        let server_task = run_server(self.grpc_addr, self.response_delay, &self.metric_labels);

        let shutdown_signal = self.shutdown.recv();
        tokio::pin!(shutdown_signal);
        tokio::pin!(server_task);

        tokio::select! {
            res = &mut server_task => match res {
                Ok(Ok(())) => Err(Error::UnexpectedShutdown),
                Ok(Err(e)) => Err(Error::Grpc(e)),
                Err(e) => Err(Error::Internal(e)),
            },
            () = &mut shutdown_signal => {
                info!("Shutdown signal received, stopping Datadog stateful logs server");
                server_task.abort();
                info!("Datadog stateful logs server has completed");
                Ok(())
            }
        }
    }
}

fn run_server(
    addr: SocketAddr,
    response_delay: Duration,
    base_labels: &[(String, String)],
) -> JoinHandle<Result<(), transport::Error>> {
    let mut labels = Vec::with_capacity(base_labels.len() + 1);
    labels.push(("protocol".to_string(), "grpc".to_string()));
    labels.extend_from_slice(base_labels);

    let service = StatefulLogsServiceImpl {
        labels,
        response_delay,
    };

    info!("Starting Datadog stateful logs gRPC service on {addr}");

    let router = transport::Server::builder().add_service(StatefulLogsServiceServer::new(service));
    tokio::spawn(async move { router.serve(addr).await })
}

/// Implementation of the `StatefulLogsService`
#[derive(Debug, Clone)]
struct StatefulLogsServiceImpl {
    labels: Vec<(String, String)>,
    response_delay: Duration,
}

#[tonic::async_trait]
impl StatefulLogsService for StatefulLogsServiceImpl {
    type LogsStreamStream = Pin<Box<dyn Stream<Item = Result<BatchStatus, Status>> + Send>>;

    async fn logs_stream(
        &self,
        request: Request<tonic::Streaming<StatefulBatch>>,
    ) -> Result<Response<Self::LogsStreamStream>, Status> {
        let mut req_stream = request.into_inner();
        let labels = self.labels.clone();
        let response_delay = self.response_delay;

        counter!("streams_received", &labels).increment(1);

        let (tx, rx) = mpsc::channel::<Result<BatchStatus, Status>>(16);

        tokio::spawn(async move {
            let labels: Vec<(String, String)> = labels.clone();
            while let Some(result) = req_stream.next().await {
                match result {
                    Ok(batch) => {
                        let batch_id = batch.batch_id;
                        let size = batch.encoded_len();

                        counter!("bytes_received", &labels).increment(size as u64);
                        counter!("batches_received", &labels).increment(1);

                        // Count data items in the batch
                        let data_count = batch.data.len();
                        if data_count > 0 {
                            counter!("data_items_received", &labels).increment(data_count as u64);
                        }

                        if !response_delay.is_zero() {
                            sleep(response_delay).await;
                        }

                        if let Err(send_err) = tx
                            .send(Ok(BatchStatus {
                                batch_id,
                                status: batch_status::Status::Ok.into(),
                            }))
                            .await
                        {
                            // Once tx.send() fails further calls will not succeed so record an error and end stream.
                            error!("Error sending back OK status: {}", send_err);
                            counter!("stream_errors", &labels).increment(1);
                            break;
                        }
                    }
                    Err(e) => {
                        // Once we receive an error there will be no further data on the stream. Record an erro and end the stream.
                        error!("Error receiving batch: {}", e);
                        counter!("stream_errors", &labels).increment(1);
                        let _ = tx.send(Err(e)).await;
                        break;
                    }
                }
            }
        });

        let output = ReceiverStream::new(rx);

        Ok(Response::new(Box::pin(output)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_deserializes() {
        let yaml = r#"
grpc_addr: "127.0.0.1:9092"
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.response_delay_millis, 0);
    }
}
