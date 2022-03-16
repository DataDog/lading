use crate::signals::Shutdown;
use serde::Deserialize;

pub mod file_gen;
pub mod http;
pub mod kafka;
pub mod splunk_hec;
pub mod tcp;

pub enum Error {
    Tcp(tcp::Error),
    Http(http::Error),
    SplunkHec(splunk_hec::Error),
    Kafka(kafka::Error),
    FileGen(file_gen::Error),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Config {
    Tcp(tcp::Config),
    Http(http::Config),
    SplunkHec(splunk_hec::Config),
    Kafka(kafka::Config),
    FileGen(file_gen::Config),
}

pub enum Server {
    Tcp(tcp::Tcp),
    Http(http::Http),
    SplunkHec(splunk_hec::SplunkHec),
    Kafka(kafka::Kafka),
    FileGen(file_gen::FileGen),
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
        let srv = match config {
            Config::Tcp(conf) => Self::Tcp(tcp::Tcp::new(&conf, shutdown).map_err(Error::Tcp)?),
            Config::Http(conf) => Self::Http(http::Http::new(conf, shutdown).map_err(Error::Http)?),
            Config::SplunkHec(conf) => Self::SplunkHec(
                splunk_hec::SplunkHec::new(conf, shutdown).map_err(Error::SplunkHec)?,
            ),
            Config::Kafka(conf) => {
                Self::Kafka(kafka::Kafka::new(conf, shutdown).map_err(Error::Kafka)?)
            }
            Config::FileGen(conf) => {
                Self::FileGen(file_gen::FileGen::new(conf, shutdown).map_err(Error::FileGen)?)
            }
        };
        Ok(srv)
    }

    /// Run this [`Server`] to completion
    ///
    /// This function runs the user supplied process to its completion, or until
    /// a shutdown signal is received.
    ///
    /// # Errors
    ///
    /// Function will return an error if the underlying sub-server signals error.
    pub async fn run(self) -> Result<(), Error> {
        match self {
            Server::Tcp(inner) => inner.spin().await.map_err(Error::Tcp),
            Server::Http(inner) => inner.spin().await.map_err(Error::Http),
            Server::SplunkHec(inner) => inner.spin().await.map_err(Error::SplunkHec),
            Server::Kafka(inner) => inner.spin().await.map_err(Error::Kafka),
            Server::FileGen(inner) => inner.spin().await.map_err(Error::FileGen),
        }
    }
}
