//! TCP request/response (`tcp_rr`) blackhole — the server side.
//! Based on <https://github.com/google/neper>
//!
//! Listens for incoming connections and, for each flow, reads a fixed-size
//! request then writes a fixed-size response, repeating until the flow closes
//! or lading shuts down.
//!
//! The event-loop machinery lives in [`crate::neper::rr`]; this module is a
//! thin wrapper that supplies configuration.
//!
//! ## Metrics
//!
//! `connections_accepted`: Incoming connections accepted
//! `requests_received`: Completed request reads
//! `responses_sent`: Completed response writes
//! `bytes_received`: Request bytes read
//! `bytes_written`: Response bytes sent

use std::net::{IpAddr, SocketAddr};
use std::num::{NonZeroU16, NonZeroUsize};

use serde::{Deserialize, Serialize};

use super::General;
use crate::neper::rr::{self, ServerParams};

fn default_nonzero_u16() -> NonZeroU16 {
    NonZeroU16::new(1).expect("1 is nonzero")
}

fn default_nonzero_usize() -> NonZeroUsize {
    NonZeroUsize::new(1).expect("1 is nonzero")
}

fn default_control_port() -> u16 {
    12866
}

fn default_data_port() -> u16 {
    12867
}

fn default_backlog() -> i32 {
    1024
}

const fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
/// Configuration for the `tcp_rr` blackhole.
pub struct Config {
    /// IP address to bind on.
    pub addr: IpAddr,
    /// Data port for flow connections. Default 12867.
    #[serde(default = "default_data_port")]
    pub data_port: u16,
    /// Control port for startup synchronization with the generator. Default 12866.
    #[serde(default = "default_control_port")]
    pub control_port: u16,
    /// Number of OS server threads. Default 1. When > 1, uses `SO_REUSEPORT`
    /// with an eBPF program for load balancing.
    #[serde(default = "default_nonzero_u16")]
    pub threads: NonZeroU16,
    /// Total number of TCP flows the generator should open.
    /// Default 1. Sent to the generator over the control connection at
    /// startup; the generator does not configure this independently.
    #[serde(default = "default_nonzero_u16")]
    pub flows: NonZeroU16,
    /// Bytes to read per request. Default 1.
    #[serde(default = "default_nonzero_usize")]
    pub request_size: NonZeroUsize,
    /// Bytes to send per response. Default 1.
    #[serde(default = "default_nonzero_usize")]
    pub response_size: NonZeroUsize,
    /// Whether to set `TCP_NODELAY` on accepted connections. Default true.
    #[serde(default = "default_true")]
    pub no_delay: bool,
    /// Listener backlog (pending-connection queue length) passed to `listen(2)`.
    /// Default 1024.
    #[serde(default = "default_backlog")]
    pub backlog: i32,
}

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`TcpRr`].
pub enum Error {
    /// Shared neper-style request/response error.
    #[error(transparent)]
    Rr(#[from] rr::Error),
}

#[derive(Debug)]
/// The `tcp_rr` blackhole (server side).
pub struct TcpRr {
    config: Config,
    metric_labels: Vec<(String, String)>,
    shutdown: lading_signal::Watcher,
}

impl TcpRr {
    /// Create a new [`TcpRr`] blackhole instance.
    #[must_use]
    pub fn new(general: General, config: &Config, shutdown: lading_signal::Watcher) -> Self {
        let mut metric_labels = vec![
            ("component".to_string(), "blackhole".to_string()),
            ("component_name".to_string(), "tcp_rr".to_string()),
        ];
        if let Some(id) = general.id {
            metric_labels.push(("id".to_string(), id));
        }
        Self {
            config: *config,
            metric_labels,
            shutdown,
        }
    }

    /// Run the blackhole to completion or until a shutdown signal is received.
    ///
    /// # Errors
    ///
    /// Returns an error if binding fails or a worker thread panics.
    pub async fn run(self) -> Result<(), Error> {
        let params = ServerParams {
            data_addr: SocketAddr::new(self.config.addr, self.config.data_port),
            control_addr: SocketAddr::new(self.config.addr, self.config.control_port),
            threads: self.config.threads.get(),
            flows: self.config.flows.get(),
            request_size: self.config.request_size.get(),
            response_size: self.config.response_size.get(),
            no_delay: self.config.no_delay,
            backlog: self.config.backlog,
        };
        rr::run_server(params, self.metric_labels, self.shutdown, "tcp_rr").await?;
        Ok(())
    }
}
