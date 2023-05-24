//! Lading blackholes
//!
//! For targets that need to push bytes themselves lading has b'blackhole'
//! support. These are listening servers that catch payloads from the target, do
//! as little as possible with them and respond as minimally as possible in
//! order to avoid overhead.

use serde::Deserialize;

use crate::signals::Shutdown;

pub mod http;
pub mod splunk_hec;
pub mod sqs;
pub mod tcp;
pub mod udp;
pub mod unix_datagram;
pub mod unix_stream;

#[derive(Debug)]
/// Errors produced by [`Server`].
pub enum Error {
    /// See [`crate::blackhole::tcp::Error`] for details.
    Tcp(tcp::Error),
    /// See [`crate::blackhole::http::Error`] for details.
    Http(http::Error),
    /// See [`crate::blackhole::splunk_hec::Error`] for details.
    SplunkHec(splunk_hec::Error),
    /// See [`crate::blackhole::udp::Error`] for details.
    Udp(udp::Error),
    /// See [`crate::blackhole::unix_stream::Error`] for details.
    UnixStream(unix_stream::Error),
    /// See [`crate::blackhole::unix_datagram::Error`] for details.
    UnixDatagram(unix_datagram::Error),
    /// See [`crate::blackhole::sqs::Error`] for details.
    Sqs(sqs::Error),
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
/// Configuration for [`Server`]
pub struct NamedConfig {
    /// The name assigned to this blackhole
    pub name: Option<String>,
    /// The blackhole config
    #[serde(flatten)]
    pub config: Config,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
/// Configuration for [`Server`]
pub enum Config {
    /// See [`crate::blackhole::tcp::Config`] for details.
    Tcp(tcp::Config),
    /// See [`crate::blackhole::http::Config`] for details.
    Http(http::Config),
    /// See [`crate::blackhole::splunk_hec::Config`] for details.
    SplunkHec(splunk_hec::Config),
    /// See [`crate::blackhole::udp::Config`] for details.
    Udp(udp::Config),
    /// See [`crate::blackhole::unix_stream::Config`] for details.
    UnixStream(unix_stream::Config),
    /// See [`crate::blackhole::unix_datagram::Config`] for details.
    UnixDatagram(unix_datagram::Config),
    /// See [`crate::blackhole::sqs::Config`] for details.
    Sqs(sqs::Config),
}

#[derive(Debug)]
/// The blackhole server.
///
/// All blackholes supported by lading are a variant of this enum. Please see
/// variant documentation for details.
pub enum Server {
    /// See [`crate::blackhole::tcp::Tcp`] for details.
    Tcp(tcp::Tcp),
    /// See [`crate::blackhole::http::Http`] for details.
    Http(http::Http),
    /// See [`crate::blackhole::splunk_hec::SplunkHec`] for details.
    SplunkHec(splunk_hec::SplunkHec),
    /// See [`crate::blackhole::udp::Udp`] for details.
    Udp(udp::Udp),
    /// See [`crate::blackhole::unix_stream::UnixStream`] for details.
    UnixStream(unix_stream::UnixStream),
    /// See [`crate::blackhole::unix_datagram::UnixDatagram`] for details.
    UnixDatagram(unix_datagram::UnixDatagram),
    /// See [`crate::blackhole::sqs::Sqs`] for details.
    Sqs(sqs::Sqs),
}

impl Server {
    /// Create a new [`Server`]
    ///
    /// This function creates a new [`Server`] instance, deferring to the
    /// underlying sub-server.
    ///
    /// # Errors
    ///
    /// Function will return an error if the underlying sub-server creation
    /// signals error.
    pub fn new(config: Config, shutdown: Shutdown) -> Result<Self, Error> {
        let server = match config {
            Config::Tcp(conf) => Self::Tcp(tcp::Tcp::new(&conf, shutdown)),
            Config::Http(conf) => {
                Self::Http(http::Http::new(&conf, shutdown).map_err(Error::Http)?)
            }
            Config::Udp(conf) => Self::Udp(udp::Udp::new(&conf, shutdown)),
            Config::UnixStream(conf) => {
                Self::UnixStream(unix_stream::UnixStream::new(conf, shutdown))
            }
            Config::UnixDatagram(conf) => {
                Self::UnixDatagram(unix_datagram::UnixDatagram::new(conf, shutdown))
            }
            Config::Sqs(conf) => Self::Sqs(sqs::Sqs::new(&conf, shutdown)),
            Config::SplunkHec(conf) => Self::SplunkHec(splunk_hec::SplunkHec::new(&conf, shutdown)),
        };
        Ok(server)
    }

    /// Runs this [`Server`] to completion
    ///
    /// This function runs the user supplied process to its completion, or until
    /// a shutdown signal is received.
    ///
    /// # Errors
    ///
    /// Function will return an error if the underlying sub-server signals
    /// error.
    pub async fn run(self) -> Result<(), Error> {
        match self {
            Server::Tcp(inner) => inner.run().await.map_err(Error::Tcp),
            Server::Http(inner) => inner.run().await.map_err(Error::Http),
            Server::Udp(inner) => inner.run().await.map_err(Error::Udp),
            Server::UnixStream(inner) => inner.run().await.map_err(Error::UnixStream),
            Server::UnixDatagram(inner) => inner.run().await.map_err(Error::UnixDatagram),
            Server::Sqs(inner) => inner.run().await.map_err(Error::Sqs),
            Server::SplunkHec(inner) => inner.run().await.map_err(Error::SplunkHec),
        }
    }
}
