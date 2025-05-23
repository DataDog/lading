//! OpenTelemetry protocol (OTLP) speaking blackhole.
//!
//! ## Metrics
//!
//! `bytes_received`: Total bytes received
//! `requests_received`: Total requests received
//! `metrics_received`: Number of metric data points received
//! `spans_received`: Number of spans received
//! `logs_received`: Number of log records received
//!

mod grpc;
mod http;

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::task::JoinSet;
use tracing::{error, info};

use crate::blackhole::General;

fn default_concurrent_requests_max() -> usize {
    100
}

fn default_response_delay_millis() -> u64 {
    0
}

/// Errors produced by [`Otlp`].
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Wrapper for IO errors.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// HTTP server error.
    #[error("HTTP server error: {0}")]
    Http(#[from] hyper::Error),
    /// gRPC server error.
    #[error("gRPC server error: {0}")]
    Grpc(#[from] tonic::transport::Error),
    /// Underlying server error.
    #[error("Server error: {0}")]
    Server(#[from] crate::blackhole::common::Error),
    /// No protocol was enabled.
    #[error("No protocols enabled - must enable at least HTTP or gRPC")]
    NoProtocolEnabled,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Copy)]
#[serde(deny_unknown_fields)]
/// Configuration for [`Otlp`]
pub struct Config {
    /// Number of concurrent connections to allow
    #[serde(default = "default_concurrent_requests_max")]
    pub concurrent_requests_max: usize,
    /// Address for gRPC endpoint
    pub grpc_addr: Option<SocketAddr>,
    /// Address for HTTP endpoint
    pub http_addr: Option<SocketAddr>,
    /// Delay to add before making a response
    #[serde(default = "default_response_delay_millis")]
    pub response_delay_millis: u64,
}

/// The OTLP blackhole, supporting both gRPC and HTTP protocols.
#[derive(Debug)]
pub struct Otlp {
    grpc_addr: Option<SocketAddr>,
    http_addr: Option<SocketAddr>,
    concurrency_limit: usize,
    shutdown: lading_signal::Watcher,
    response_delay: Duration,
    labels: Vec<(String, String)>,
}

impl Otlp {
    /// Create a new [`Otlp`] server instance
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid.
    pub fn new(
        general: &General,
        config: &Config,
        shutdown: &lading_signal::Watcher,
    ) -> Result<Self, Error> {
        // Ensure at least one protocol is enabled
        if config.grpc_addr.is_none() && config.http_addr.is_none() {
            return Err(Error::NoProtocolEnabled);
        };

        let mut labels = vec![
            ("component".to_string(), "blackhole".to_string()),
            ("component_name".to_string(), "otlp".to_string()),
        ];
        if let Some(id) = &general.id {
            labels.push(("id".to_string(), id.clone()));
        }

        Ok(Self {
            grpc_addr: config.grpc_addr,
            http_addr: config.http_addr,
            concurrency_limit: config.concurrent_requests_max,
            shutdown: shutdown.clone(),
            response_delay: Duration::from_millis(config.response_delay_millis),
            labels,
        })
    }

    /// Run [`Otlp`] to completion
    ///
    /// This function runs the OTLP server(s) until a shutdown signal is
    /// received or an unrecoverable error is encountered.
    ///
    /// # Errors
    ///
    /// Function will return an error if server initialization fails.
    pub async fn run(self) -> Result<(), Error> {
        let mut join_set = JoinSet::new();

        if let Some(addr) = self.grpc_addr {
            let grpc_task = grpc::run_server(
                addr,
                self.response_delay,
                &self.labels,
                self.concurrency_limit,
            );
            join_set.spawn(async move {
                grpc_task.await.unwrap_or_else(|e| {
                    error!("gRPC task failed: {}", e);
                });
            });
        }

        if let Some(addr) = self.http_addr {
            let response_delay = self.response_delay;
            let labels = self.labels.clone();
            let concurrency_limit = self.concurrency_limit;
            let shutdown = self.shutdown.clone();

            let http_task =
                http::run_server(addr, response_delay, &labels, concurrency_limit, shutdown);
            join_set.spawn(async move {
                http_task.await.unwrap_or_else(|e| {
                    error!("HTTP task failed: {}", e);
                });
            });
        }

        let shutdown_future = self.shutdown.recv();
        tokio::pin!(shutdown_future);

        tokio::select! {
            () = shutdown_future => {
                info!("Shutdown signal received, stopping OTLP server(s)");
                join_set.abort_all();
            }
            () = async { while join_set.join_next().await.is_some() {} } => {
                info!("All OTLP servers have completed");
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    #[test]
    fn otlp_config() {
        let config = Config {
            concurrent_requests_max: 100,
            grpc_addr: Some("127.0.0.1:4317".parse().unwrap()),
            http_addr: Some("127.0.0.1:4318".parse().unwrap()),
            response_delay_millis: 0,
        };

        assert_eq!(config.concurrent_requests_max, 100);
        assert_eq!(
            config.grpc_addr,
            Some(SocketAddr::from(([127, 0, 0, 1], 4317)))
        );
        assert_eq!(
            config.http_addr,
            Some(SocketAddr::from(([127, 0, 0, 1], 4318)))
        );
        assert_eq!(config.response_delay_millis, 0);
    }

    // OtlpMetrics test removed since we now use metrics counters directly
}
