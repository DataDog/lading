use crate::signals::Shutdown;
use serde::Deserialize;

pub mod http;
pub mod splunk_hec;
pub mod sqs;
pub mod tcp;
pub mod udp;

pub enum Error {
    Tcp(tcp::Error),
    Http(http::Error),
    SplunkHec(splunk_hec::Error),
    Udp(udp::Error),
    Sqs(sqs::Error),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Config {
    Tcp(tcp::Config),
    Http(http::Config),
    SplunkHec(splunk_hec::Config),
    Udp(udp::Config),
    Sqs(sqs::Config),
}

pub enum Server {
    Tcp(tcp::Tcp),
    Http(http::Http),
    SplunkHec(splunk_hec::SplunkHec),
    Udp(udp::Udp),
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
    #[must_use]
    pub fn new(config: Config, shutdown: Shutdown) -> Self {
        match config {
            Config::Tcp(conf) => Self::Tcp(tcp::Tcp::new(&conf, shutdown)),
            Config::Http(conf) => Self::Http(http::Http::new(&conf, shutdown)),
            Config::Udp(conf) => Self::Udp(udp::Udp::new(&conf, shutdown)),
            Config::Sqs(conf) => Self::Sqs(sqs::Sqs::new(&conf, shutdown)),
            Config::SplunkHec(conf) => Self::SplunkHec(splunk_hec::SplunkHec::new(&conf, shutdown)),
        }
    }

    /// Runs this [`Server`] to completion
    ///
    /// This function runs the user supplied process to its completion, or until
    /// a shutdown signal is received.
    ///
    /// # Errors
    ///
    /// Function will return an error if the underlying sub-server signals error.
    pub async fn run(self) -> Result<(), Error> {
        match self {
            Server::Tcp(inner) => inner.run().await.map_err(Error::Tcp),
            Server::Http(inner) => inner.run().await.map_err(Error::Http),
            Server::Udp(inner) => inner.run().await.map_err(Error::Udp),
            Server::Sqs(inner) => inner.run().await.map_err(Error::Sqs),
            Server::SplunkHec(inner) => inner.run().await.map_err(Error::SplunkHec),
        }
    }
}
