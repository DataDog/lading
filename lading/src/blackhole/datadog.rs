//! The Datadog blackhole.
//!
//! This blackhole supports multiple Datadog intake protocols:
//! - HTTP V2 metrics API (protobuf-encoded)
//! - gRPC stateful logs API (bidirectional streaming)

mod grpc;
mod v2_http;

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::task::JoinSet;
use tracing::{error, info};

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
    /// Wrapper for HTTP error.
    #[error(transparent)]
    HttpError(#[from] http::Error),
    /// No variant was configured.
    #[error("No variants enabled - must enable at least one Datadog intake variant")]
    NoVariantEnabled,
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
    /// Stateful logs gRPC API (bidirectional streaming)
    StatefulLogs {
        /// The binding address for the gRPC server
        grpc_addr: SocketAddr,
    },
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
/// Configuration for [`Datadog`].
pub struct Config {
    /// The Datadog API variant(s) to use
    #[serde(flatten)]
    pub variant: Variant,
}

#[derive(Debug)]
/// The Datadog blackhole.
pub struct Datadog {
    variant: Variant,
    shutdown: lading_signal::Watcher,
    metric_labels: Vec<(String, String)>,
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
            variant: config.variant,
            shutdown,
            metric_labels,
        }
    }

    /// Run [`Datadog`] to completion
    ///
    /// This function runs the server(s) forever, unless a shutdown signal is
    /// received or an unrecoverable error is encountered.
    ///
    /// # Errors
    ///
    /// Function will return an error if the server fails to start.
    pub async fn run(self) -> Result<(), Error> {
        let mut join_set = JoinSet::new();

        match self.variant {
            Variant::V2 { binding_addr } => {
                // Run HTTP V2 metrics server
                let http_server = v2_http::run_v2_server(
                    binding_addr,
                    self.metric_labels.clone(),
                    self.shutdown.clone(),
                );
                join_set.spawn(async move {
                    if let Err(e) = http_server.await {
                        error!("HTTP V2 server error: {:?}", e);
                    }
                });
            }
            Variant::StatefulLogs { grpc_addr } => {
                // Run gRPC stateful logs server
                let grpc_task = grpc::run_server(grpc_addr, &self.metric_labels);
                join_set.spawn(async move {
                    grpc_task.await.unwrap_or_else(|e| {
                        error!("gRPC task failed: {}", e);
                    });
                });
            }
        }

        let shutdown = self.shutdown.recv();
        tokio::pin!(shutdown);

        tokio::select! {
            () = shutdown => {
                info!("Shutdown signal received, stopping Datadog server(s)");
                join_set.shutdown().await;
                while join_set.join_next().await.is_some() {}
                info!("All Datadog servers have completed");
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_v2_deserializes() {
        let yaml = r#"
v2:
  binding_addr: "127.0.0.1:9091"
"#;
        let _config: Config = serde_yaml::from_str(yaml).unwrap();
    }

    #[test]
    fn config_stateful_logs_deserializes() {
        let yaml = r#"
stateful_logs:
  grpc_addr: "127.0.0.1:9092"
"#;
        let _config: Config = serde_yaml::from_str(yaml).unwrap();
    }
}
