//! TCP connect/request/response (`tcp_crr`) generator — the client side.
//! Based on <https://github.com/google/neper>
//!
//! Implements neper's `tcp_crr` protocol: each flow connects, sends a
//! fixed-size request, reads a fixed-size response, closes the connection,
//! then reconnects and repeats. This measures connection-establishment rate
//! end-to-end, including kernel and TCP-handshake overhead.
//!
//! The event-loop machinery lives in [`crate::neper::rr`]; this module is a
//! thin wrapper that supplies configuration and selects [`Mode::Crr`].
//!
//! ## Ephemeral port / `TIME_WAIT`
//!
//! Each transaction consumes one client-side ephemeral port for the duration
//! of `TIME_WAIT` (~60s by default on Linux). At even a few thousand
//! transactions/sec the ~28k usable port range exhausts in seconds. Widen
//! `net.ipv4.ip_local_port_range` and consider lowering `tcp_fin_timeout`
//! before running sustained CRR workloads.
//!
//! ## Metrics
//!
//! `connections_initiated`: Successful client-side connect completions
//! `requests_sent`: Completed request writes
//! `responses_received`: Completed response reads
//! `bytes_written`: Request bytes sent
//! `bytes_read`: Response bytes received
//! `connections_failed`: Failed connection attempts

use std::net::{IpAddr, SocketAddr};
use std::num::{NonZeroU16, NonZeroUsize};

use serde::{Deserialize, Serialize};

use super::General;
use crate::generator::common::MetricsBuilder;
use crate::neper::rr::{self, ClientParams, Mode};

fn default_nonzero_u16() -> NonZeroU16 {
    NonZeroU16::new(1).expect("1 is nonzero")
}

fn default_nonzero_usize() -> NonZeroUsize {
    NonZeroUsize::new(1).expect("1 is nonzero")
}

const fn default_true() -> bool {
    true
}

fn default_control_port() -> u16 {
    12866
}

fn default_data_port() -> u16 {
    12867
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
/// Configuration for the `tcp_crr` generator.
pub struct Config {
    /// The IP address of the `tcp_crr` server.
    pub addr: String,
    /// Data port for flow connections. Default 12867.
    #[serde(default = "default_data_port")]
    pub data_port: u16,
    /// Control port for startup synchronization with the blackhole. Default 12866.
    #[serde(default = "default_control_port")]
    pub control_port: u16,
    /// Number of OS threads (neper -T). Default 1.
    #[serde(default = "default_nonzero_u16")]
    pub threads: NonZeroU16,
    /// Total number of TCP flows (neper -F). Default 1.
    ///
    /// Each flow continuously reconnects after every transaction.
    #[serde(default = "default_nonzero_u16")]
    pub flows: NonZeroU16,
    /// Bytes per request. Default 1.
    #[serde(default = "default_nonzero_usize")]
    pub request_size: NonZeroUsize,
    /// Bytes per response to read back. Default 1.
    #[serde(default = "default_nonzero_usize")]
    pub response_size: NonZeroUsize,
    /// Whether to set `TCP_NODELAY` on connections. Default true.
    #[serde(default = "default_true")]
    pub no_delay: bool,
}

#[derive(thiserror::Error, Debug)]
/// Errors produced by [`TcpCrr`].
pub enum Error {
    /// Shared neper-style request/response error.
    #[error(transparent)]
    Rr(#[from] rr::Error),
}

#[derive(Debug)]
/// The `tcp_crr` generator (client side).
pub struct TcpCrr {
    config: Config,
    metric_labels: Vec<(String, String)>,
    shutdown: lading_signal::Watcher,
}

impl TcpCrr {
    /// Create a new [`TcpCrr`] generator instance.
    #[must_use]
    pub fn new(general: General, config: &Config, shutdown: lading_signal::Watcher) -> Self {
        let metric_labels = MetricsBuilder::new("tcp_crr").with_id(general.id).build();
        Self {
            config: config.clone(),
            metric_labels,
            shutdown,
        }
    }

    /// Run the generator to completion or until a shutdown signal is received.
    ///
    /// # Errors
    ///
    /// Returns an error if a worker thread panics or configuration is invalid.
    ///
    /// # Panics
    ///
    /// Panics if `addr` cannot be parsed as an IP address.
    pub async fn spin(self) -> Result<(), Error> {
        let ip: IpAddr = self.config.addr.parse().expect("invalid addr");
        let params = ClientParams {
            data_addr: SocketAddr::new(ip, self.config.data_port),
            control_addr: SocketAddr::new(ip, self.config.control_port),
            threads: self.config.threads.get(),
            flows: self.config.flows.get(),
            request_size: self.config.request_size.get(),
            response_size: self.config.response_size.get(),
            no_delay: self.config.no_delay,
            mode: Mode::Crr,
        };
        rr::run_client(params, self.metric_labels, self.shutdown, "tcp_crr").await?;
        Ok(())
    }
}
